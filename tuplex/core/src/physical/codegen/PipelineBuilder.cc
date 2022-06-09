//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/codegen/PipelineBuilder.h>
#include <CSVUtils.h>
#include "visitors/BlockGeneratorVisitor.h"

namespace tuplex {
    namespace codegen {


        // cache structtype here
        static std::unordered_map<llvm::LLVMContext*, llvm::StructType*> g_cached_types;
        llvm::StructType* PipelineBuilder::resultStructType(llvm::LLVMContext& ctx) {
            using namespace llvm;

            auto i32_type = Type::getInt32Ty(ctx);
            return llvm::StructType::get(ctx, {i32_type, i32_type, i32_type});

            //// old
            //// check if entry is already there
            //auto it = g_cached_types.find(&ctx);
            //if(it == g_cached_types.end()) {
            //    auto i32_type = Type::getInt32Ty(ctx);
            //    g_cached_types[&ctx] = llvm::StructType::create(ctx, {i32_type, i32_type, i32_type}, "struct.result", false);
            //}
            //return g_cached_types[&ctx];
        }

        // reusable function b.c. needs to be done in resolver too.
        FlattenedTuple castRow(llvm::IRBuilder<>& builder, const FlattenedTuple& row, const python::Type& target_type) {
            Logger::instance().defaultLogger().warn("using deprecated function, please change in code.");
            return row.upcastTo(builder, target_type);
        }

        void PipelineBuilder::addVariable(llvm::IRBuilder<> &builder, const std::string name, llvm::Type *type,
                                                   llvm::Value *initialValue) {
            _variables[name] = builder.CreateAlloca(type, 0, nullptr, name);

            if(initialValue)
                builder.CreateStore(initialValue, _variables[name]);
        }

        llvm::Value* PipelineBuilder::getVariable(llvm::IRBuilder<> &builder, const std::string name) {
            assert(_variables.find(name) != _variables.end());
            return builder.CreateLoad(_variables[name]);
        }

        llvm::Value* PipelineBuilder::getPointerToVariable(llvm::IRBuilder<> &builder, const std::string name) {
            assert(_variables.find(name) != _variables.end());
            return _variables[name];
        }

        void PipelineBuilder::assignToVariable(llvm::IRBuilder<> &builder, const std::string name,
                                                        llvm::Value *newValue) {
            assert(_variables.find(name) != _variables.end());
            builder.CreateStore(newValue, _variables[name]);
        }

        void PipelineBuilder::createFunction(const std::string& Name, const python::Type& intermediateOutputType) {
            using namespace llvm;
            using namespace std;

            // create (internal) llvm function to be inlined with all contents
#ifndef NDEBUG
            Logger::instance().defaultLogger().debug("DEBUG PRINT: creating func for " + _lastSchemaType.desc());
#endif
            auto& context = env().getContext();

            // signature is basically i8* userData, ?* row
            FlattenedTuple ft(_env.get());
            ft.init(_lastSchemaType);

            vector<Type*> argsType{resultStructType()->getPointerTo(0),
                                   env().i8ptrType(),
                                   ft.getLLVMType()->getPointerTo(),
                                   env().i64Type()};
            vector<string> argNames{"result", "userData", "row", "rowNumber"};
            if(intermediateOutputType != python::Type::UNKNOWN) {
                argsType.emplace_back(env().pythonToLLVMType(intermediateOutputType)->getPointerTo());
                argNames.emplace_back("intermediate");
            }

            FunctionType* func_type = FunctionType::get(Type::getVoidTy(env().getContext()), argsType, false);
            _func = Function::Create(func_type, Function::InternalLinkage, Name, env().getModule().get());

            // TEST: disable attributes to prevent wrong use...
            // // set inline attributes
            // AttrBuilder ab;
            // ab.addAttribute(Attribute::AlwaysInline);
            // _func->addAttributes(llvm::AttributeList::FunctionIndex, ab);

            _args = mapLLVMFunctionArgs(_func, argNames);
            auto argRow = llvm::dyn_cast<llvm::Argument>(_args["row"]);

            // make result noalias + sret
            llvm::dyn_cast<llvm::Argument>(_args["result"])->addAttr(Attribute::StructRet);
            llvm::dyn_cast<llvm::Argument>(_args["result"])->addAttr(Attribute::NoAlias);

            if(intermediateOutputType != python::Type::UNKNOWN) {
                // set nocapture
                //llvm::dyn_cast<llvm::Argument>(_args["intermediate"])->addAttr(Attribute::NoCapture);
            }

            // save args
            _argUserData = _args["userData"];
            _argRowNumber = _args["rowNumber"];

            // make c'tor block the first one to be created
            _constructorBlock = BasicBlock::Create(context, "constructors", _func);
            _destructorBlock = BasicBlock::Create(context, "destructors", _func);
            _entryBlock = _lastBlock = BasicBlock::Create(context, "entry", _func);

            // initialize variables
            IRBuilder<> builder(_constructorBlock);
            addVariable(builder, "exceptionCode", env().i64Type(),env().i64Const(0));
            addVariable(builder, "exceptionOperatorID", env().i64Type());
            addVariable(builder, "numOutputRows", env().i64Type());

            // assign 0, writers have to explicitly call it
            assignToVariable(builder, "numOutputRows", env().i64Const(0));

            // load the tuple1
            _lastRowResult = _lastRowInput = FlattenedTuple::fromLLVMStructVal(&env(), builder, argRow, ft.getTupleType());

            // store in var
            _lastTupleResultVar = _lastRowResult.alloc(builder);
            _lastRowResult.storeTo(builder, _lastTupleResultVar);

            // assign argInput
            _argInputRow = _lastRowResult;

            // first leave block is destructor
            _leaveBlocks.push_back(_destructorBlock);
        }

        bool PipelineBuilder::addIgnore(const ExceptionCode &ec, const int64_t operatorID) {
            using namespace std;
            using namespace llvm;
            auto& logger = Logger::instance().logger("PipelineBuilder");
            auto& context = env().getContext();

            assert(!_exceptionBlocks.empty());

            // current exception block
            IRBuilder<> builder(_exceptionBlocks.back());


            // logger.debug("name of last exception block: " + _exceptionBlocks.back()->getName().str());
            // logger.debug("name of last block: " + _lastBlock->getName().str());

            // remove block from the ones to be connected with the end!
            _exceptionBlocks.erase(_exceptionBlocks.end() - 1);

            // compare last exception code
            Value* lastExceptionCode = getVariable(builder, "exceptionCode");
            Value* lastOperatorID = getVariable(builder, "exceptionOperatorID");
            Value* ignoreCond = _env->matchExceptionHierarchy(builder, lastExceptionCode, ec);

            // use same approach as when adding a filter operation:
            // i.e. a keep block and then just exit (without writing the row)
            auto func = _constructorBlock->getParent(); assert(func);
            BasicBlock* bbException = createExceptionBlock();

            // branch according to ignore
            builder.CreateCondBr(ignoreCond, leaveBlock(), bbException);

            // logger.debug("name of last exception block: " + _exceptionBlocks.back()->getName().str());
            // logger.debug("name of last block: " + _lastBlock->getName().str());

            return true;
        }

        bool PipelineBuilder::addResolver(const tuplex::ExceptionCode &ec, const int64_t operatorID,
                                          const tuplex::UDF &udf, double normalCaseThreshold, bool allowUndefinedBehavior, bool sharedObjectPropagation) {
            using namespace std;
            using namespace llvm;
            auto& logger = Logger::instance().logger("PipelineBuilder");
            auto& context = env().getContext();

            assert(!_exceptionBlocks.empty());

            BasicBlock* lastNormalBlock = _lastBlock; // last block might be modified by filter & Co.

            // create new tupleVal
            IRBuilder<> variableBuilder(_constructorBlock);

            // current exception block
            IRBuilder<> builder(_exceptionBlocks.back());

            // remove block from the ones to be connected with the end!
            _exceptionBlocks.erase(_exceptionBlocks.end() - 1);

            // compare last exception code
            Value* lastExceptionCode = getVariable(builder, "exceptionCode");
            Value* lastOperatorID = getVariable(builder, "exceptionOperatorID");
            // old:
            // Value* resolveCond = builder.CreateICmpEQ(lastExceptionCode, env().i64Const(ecToI32(ec)));
            // new:
            Value* resolveCond = _env->matchExceptionHierarchy(builder, lastExceptionCode, ec);

            // create two blocks:
            // 1) block where to resolve exception
            // 2) new except block
            auto func = _constructorBlock->getParent(); assert(func);
            BasicBlock* bbResolverMatch = BasicBlock::Create(context, "resolver_match", func);
            BasicBlock* bbException = createExceptionBlock();
            // create continuation block (i.e. when exception was successfully resolved)
            BasicBlock* bbNextBlock = BasicBlock::Create(context, "resolved_block", func);

            // branch according to resolver...
            builder.CreateCondBr(resolveCond, bbResolverMatch, bbException);


            // --- apply to resolver
            builder.SetInsertPoint(bbResolverMatch);

            // call function, if fails => go to new exception block
            // if succeeds, go to continuation block
            // ==> call depends on operation...


            // compile dependent on udf
            auto cf = udf.isCompiled() ? const_cast<UDF&>(udf).compile(env()) :
                      const_cast<UDF&>(udf).compileFallback(env(), _constructorBlock, _destructorBlock);

            // stop if compilation didn't succeed
            if(!cf.good())
                return false;

            switch(_lastOperatorType) {
                case LogicalOperatorType::MAP: {

                    // _env->debugPrint(builder, "resolving exception for MAP operator");

                    // store operatorID (i.e., resolver can also throw exceptions!)
                    assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));
                    // simply call function
                    auto resVal = _lastTupleResultVar; // i.e. the output of the last tuple (just overwrite it)
                    FlattenedTuple resultRow = cf.callWithExceptionHandler(builder, _lastRowInput, resVal, bbException, getPointerToVariable(builder, "exceptionCode"));

                    // check that the output type is the same as the expected one!
                    if(resultRow.getTupleType() != _lastRowResult.getTupleType())
                        throw std::runtime_error("result type " + resultRow.getTupleType().desc() + "of resolver does not match type of previous operator " + _lastRowResult.getTupleType().desc());

                    // store result into var
                    resultRow.storeTo(builder, _lastTupleResultVar);

                    // branch to continuation block
                    builder.CreateBr(bbNextBlock);
                    break;
                }
                case LogicalOperatorType::FILTER: {
                    // check filtering
                    // store operatorID (i.e., resolver can also throw exceptions!)
                    assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));
                    // simply call function
                    auto resVal = variableBuilder.CreateAlloca(cf.getLLVMResultType(env()), 0, nullptr);// i.e. a single bool
                    FlattenedTuple ft = cf.callWithExceptionHandler(builder, _lastRowInput, resVal, bbException, getPointerToVariable(builder, "exceptionCode"));

                    // check type is correct
                    auto expectedType = python::Type::BOOLEAN;
                    if(ft.getTupleType() != expectedType) {
                        logger.error("wrong output type. Expected " + expectedType.desc() + " got " + ft.getTupleType().desc());
                        return false;
                    }

                    // filter specific code:
                    // now the filter check happens:
                    // decide whether to skip row or not
                    assert(ft.numElements() ==  1);
                    auto compareVal = ft.get(0);

                    assert(compareVal->getType() == env().getBooleanType());

                    auto filterCond = builder.CreateICmpEQ(compareVal, env().boolConst(true));

                    // create block which alters number of output rows:
                    BasicBlock* bbFilter = BasicBlock::Create(env().getContext(), "filtered_out", func);

                    // if tuple is filtered away, simply go to destructor block, else to continuation block
                    builder.CreateCondBr(filterCond, bbNextBlock, bbFilter);

                    // alter numOutputRows to 0, and go to destructor block
                    builder.SetInsertPoint(bbFilter);
                    assignToVariable(builder, "numOutputRows", env().i64Const(0));
                    builder.CreateBr(_destructorBlock);

                    // last tuple result var remains unchanged!
                    break;
                }
                case LogicalOperatorType::MAPCOLUMN: {
                    // store operatorID (i.e., resolver can also throw exceptions!)
                    assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));
                    // simply call function
                    auto resVal = variableBuilder.CreateAlloca(cf.getLLVMResultType(env()), 0, nullptr);
                    auto elementType = _lastInputSchemaType.parameters()[_lastOperatorColumnIndex]; // this might lead to an error...
                    auto load = _lastRowInput.getLoad(builder, {_lastOperatorColumnIndex});
                    // check what type element loaded is (tuple? primitive?)
                    FlattenedTuple inVal(&env());
                    if(load.val && load.val->getType()->isStructTy()) {
                        EXCEPTION("struct type for map column not yet implemented...");
                        inVal = FlattenedTuple::fromLLVMStructVal(&env(), builder, load.val, elementType);
                    } else {
                        inVal.init(elementType);
                        // simple element
                        inVal.assign(0, load.val, load.size, load.is_null);
                    }

                    auto outVal = cf.callWithExceptionHandler(builder, inVal, resVal, bbException, getPointerToVariable(builder, "exceptionCode"));

                    if(outVal.getTupleType().isTupleType())
                        EXCEPTION("tuples not yet supported...");
                    auto resLoad = codegen::SerializableValue(outVal.get(0), outVal.getSize(0));

                    // copy to new vals
                    FlattenedTuple ftOut(&env());

                    auto params = _lastInputSchemaType.parameters();
                    // update with result of UDF
                    params[_lastOperatorColumnIndex] = outVal.getTupleType();
                    auto finalType = python::Type::makeTupleType(params);

                    ftOut.init(finalType);

                    for(int i = 0; i < finalType.parameters().size(); ++i) {
                        auto elLoad = i != _lastOperatorColumnIndex ? _lastRowInput.getLoad(builder, {i}) : resLoad;

                        // store in output
                        ftOut.set(builder, {i}, elLoad.val, elLoad.size, elLoad.is_null);
                    }

                    auto resultRow = ftOut;

                    // store final output to variable
                    // check that the output type is the same as the expected one!
                    if(resultRow.getTupleType() != _lastRowResult.getTupleType())
                        throw std::runtime_error("result type " + resultRow.getTupleType().desc() + "of resolver does not match type of previous operator " + _lastRowResult.getTupleType().desc());

                    // store result into var
                    resultRow.storeTo(builder, _lastTupleResultVar);

                    builder.CreateBr(bbNextBlock);
                    break;
                }
                case LogicalOperatorType::WITHCOLUMN: {
                    // similar to mapColumn block...
                    // store in what operator called here (needed for exception handler)
                    assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));
                    // as stated in the map operation, the result type needs to be allocated within the entry block
                    auto resVal = variableBuilder.CreateAlloca(cf.getLLVMResultType(env()), 0, nullptr);
                    auto outVal = cf.callWithExceptionHandler(builder, _lastRowInput, resVal, bbException, getPointerToVariable(builder, "exceptionCode"));
                    if(outVal.getTupleType().isTupleType())
                        EXCEPTION("tuples not yet supported...");
                    // assign to input vals
#warning "untested code for tuples etc...."
                    auto resLoad = codegen::SerializableValue(outVal.get(0), outVal.getSize(0));

                    // copy to new vals
                    FlattenedTuple ftOut(&env());

                    auto params = _lastInputSchemaType.parameters();
                    // update with result of UDF
                    if(_lastOperatorColumnIndex < params.size())
                        params[_lastOperatorColumnIndex] = outVal.getTupleType();
                    else {
                        assert(_lastOperatorColumnIndex == params.size()); // append!
                        params.emplace_back(outVal.getTupleType());
                    }
                    auto finalType = python::Type::makeTupleType(params);

                    ftOut.init(finalType);

                    for(int i = 0; i < finalType.parameters().size(); ++i) {
                        auto elLoad = i != _lastOperatorColumnIndex ? _lastRowInput.getLoad(builder, {i}) : resLoad;
                        // store in output
                        ftOut.set(builder, {i}, elLoad.val, elLoad.size, elLoad.is_null);
                    }

                    auto resultRow = ftOut;

                    // store final output to variable
                    // check that the output type is the same as the expected one!
                    if(resultRow.getTupleType() != _lastRowResult.getTupleType())
                        throw std::runtime_error("result type " + resultRow.getTupleType().desc() + "of resolver does not match type of previous operator " + _lastRowResult.getTupleType().desc());

                    // store result into var
                    resultRow.storeTo(builder, _lastTupleResultVar);

                    builder.CreateBr(bbNextBlock);
                    break;
                }
                default: {
                    logger.error("unknown operator type " + to_string((int)_lastOperatorType) + " found before resolver, can't generate code for it.");
                    throw std::runtime_error("unknown operator in addResolver");
                }
            }

            // ----

            // link last (normal) block to continuation block
            builder.SetInsertPoint(lastNormalBlock);
            builder.CreateBr(bbNextBlock);

            // fetch last row in next block
            builder.SetInsertPoint(bbNextBlock);
            // update wrapper (through variable load)
            _lastRowResult = FlattenedTuple::fromLLVMStructVal(&env(), builder,
                                                               _lastTupleResultVar,
                                                               _lastRowResult.getTupleType());

            // last block is continuation block!
            _lastBlock = bbNextBlock;

            return true;
        }

        // simple building, i.e. mapOperation
        bool PipelineBuilder::mapOperation(const int64_t operatorID, const tuplex::UDF &udf,
                                                    double normalCaseThreshold,
                                                    bool allowUndefinedBehavior, bool sharedObjectPropagation) {
            using namespace std;
            using namespace llvm;
            auto& logger = Logger::instance().logger("PipelineBuilder");

            // empty udf? i.e. renaming only? ==> skip!
            if(udf.empty())
                return true;

            // compile dependent on udf
            auto cf = udf.isCompiled() ? const_cast<UDF&>(udf).compile(env()) :
                      const_cast<UDF&>(udf).compileFallback(env(), _constructorBlock, _destructorBlock);

            // stop if compilation didn't succeed
            if(!cf.good())
                return false;

            IRBuilder<> builder(_lastBlock);

            // store in what operator called here (needed for exception handler)
            assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));
            // as stated in the map operation, the result type needs to be allocated within the entry block
            IRBuilder<> variableBuilder(_constructorBlock);
            _lastTupleResultVar = variableBuilder.CreateAlloca(cf.getLLVMResultType(env()), 0, nullptr);
            _lastRowInput = _lastRowResult;

            // debug info
            logger.debug("Compiled function takes input row type: " + cf.input_type.desc());
            logger.debug("Last LLVM input row is: " + _lastRowResult.getTupleType().desc());

            // check compatibility of types -> they HAVE to be compatible on a flattened type basis
            if(flattenedType(cf.input_type) != flattenedType(_lastRowResult.getTupleType())) {
                std::stringstream err;
                err<<"last result and expected UDF input type are not compatible\n"
                     <<"Compiled function takes input row type: " + cf.input_type.desc() + "\n"
                     <<"Last LLVM input row is: " + _lastRowResult.getTupleType().desc();
                logger.warn(err.str());
            }

            auto exceptionBlock = createExceptionBlock();
            _lastRowResult = cf.callWithExceptionHandler(builder, _lastRowResult, _lastTupleResultVar, exceptionBlock, getPointerToVariable(builder, "exceptionCode"));

            if(_lastRowResult.getTupleType() != cf.output_type) {
                logger.error("wrong output type. Expected " + cf.output_type.desc() + " got " + _lastRowResult.getTupleType().desc());
                return false;
            }

            _lastBlock = builder.GetInsertBlock();
            assert(_lastBlock);

            // update what the last schema type is
            _lastInputSchemaType = _lastSchemaType;
            _lastSchemaType = cf.output_type;

            _lastOperatorType = LogicalOperatorType::MAP;
            _lastOperatorColumnIndex = -1;
            return true;
        }


        // filter is super simple too, just call branching to go to destructor block if nothing is to do
        bool PipelineBuilder::filterOperation(const int64_t operatorID, const tuplex::UDF &udf,
                                              double normalCaseThreshold,
                                                       bool allowUndefinedBehavior, bool sharedObjectPropagation) {
            using namespace std;
            using namespace llvm;
            auto& logger = Logger::instance().logger("PipelineBuilder");

            // compile dependent on udf
            auto cf = udf.isCompiled() ? const_cast<UDF&>(udf).compile(env()) :
                      const_cast<UDF&>(udf).compileFallback(env(), _constructorBlock, _destructorBlock);

            // stop if compilation didn't succeed
            if(!cf.good())
                return false;
            // the return type of the UDF MUST BE BOOLEAN! this is the definition for filter operator
            // hence, if it's not boolean need to add a truth test.
            // for some types, we can statically decide this...

            if(python::Type::BOOLEAN != cf.output_type) {
                // check if it's something that's always false
                // e.g., // empty sequences and collections: '', (), [], {}, set(), range(0)
                // cf. LLVMEnvironment.cc truthValueTest for this...
                if(cf.output_type == python::Type::EMPTYTUPLE ||
                cf.output_type == python::Type::EMPTYLIST ||
                cf.output_type == python::Type::EMPTYDICT) {
                    logger.warn("filter operation will filter out all rows and yield therefore an empty dataset.");

                    IRBuilder<> builder(_lastBlock);
                    BasicBlock *keepBlock = BasicBlock::Create(env().getContext(), "filter_keep", builder.GetInsertBlock()->getParent());

                    // if tuple is filtered away, simply go to destructor block
                    // to continue codegen, create a dummy here
                    builder.CreateCondBr(env().i1Const(false), keepBlock, leaveBlock());
                    _lastBlock = keepBlock; // update this

                    _lastOperatorType = LogicalOperatorType::FILTER;
                    _lastOperatorColumnIndex = -1;
                    return true;
                }
            }

            IRBuilder<> builder(_lastBlock);

            // store in what operator called here (needed for exception handler)
            assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));
            // as stated in the map operation, the result type needs to be allocated within the entry block
            IRBuilder<> variableBuilder(_constructorBlock);
            // for filter, do not update row
            auto resVal = variableBuilder.CreateAlloca(cf.getLLVMResultType(env()), 0, nullptr);
            _lastRowInput = _lastRowResult;

            auto exceptionBlock = createExceptionBlock();
            auto ft = cf.callWithExceptionHandler(builder, _lastRowResult, resVal, exceptionBlock, getPointerToVariable(builder, "exceptionCode"));
            // check type is correct
            auto expectedType = python::Type::BOOLEAN;
            llvm::Value* filterCond = nullptr; // i1 filter condition
            if(ft.getTupleType() != expectedType) {
                // perform truthValueTest on result
                // is it a tuple type?
                // keep everything!
                if(ft.numElements() > 1)
                    filterCond = env().i1Const(true);
                else{
                    auto ret_val = SerializableValue(ft.get(0), ft.getSize(0), ft.getIsNull(0)); // single value?
                    filterCond = env().truthValueTest(builder, ret_val, ft.getTupleType());
                }
            } else {
                assert(ft.numElements() ==  1);
                auto compareVal = ft.get(0);
                assert(compareVal->getType() == env().getBooleanType());
                filterCond = builder.CreateICmpEQ(compareVal, env().boolConst(true));
            }

            _lastBlock = builder.GetInsertBlock();
            assert(_lastBlock);

            // now the filter check happens:
            // decide whether to skip row or not
            BasicBlock *keepBlock = BasicBlock::Create(env().getContext(),
                                                       "filter_keep", builder.GetInsertBlock()->getParent());

            // if tuple is filtered away, simply go to destructor block
            builder.CreateCondBr(filterCond, keepBlock, leaveBlock());
            _lastBlock = keepBlock; // update this

            _lastOperatorType = LogicalOperatorType::FILTER;
            _lastOperatorColumnIndex = -1;

            return true;
        }

        bool PipelineBuilder::mapColumnOperation(const int64_t operatorID, int columnToMapIndex,
                                                          const tuplex::UDF &udf, double normalCaseThreshold, bool allowUndefinedBehavior, bool sharedObjectPropagation) {
            using namespace std;
            using namespace llvm;
            auto& logger = Logger::instance().logger("PipelineBuilder");

            // compile dependent on udf
            auto cf = udf.isCompiled() ? const_cast<UDF&>(udf).compile(env()) :
                      const_cast<UDF&>(udf).compileFallback(env(), _constructorBlock, _destructorBlock);

            // stop if compilation didn't succeed
            if(!cf.good())
                return false;

            IRBuilder<> builder(_lastBlock);

            // store in what operator called here (needed for exception handler)
            assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));
            // as stated in the map operation, the result type needs to be allocated within the entry block
            IRBuilder<> variableBuilder(_constructorBlock);
            auto resVal = variableBuilder.CreateAlloca(cf.getLLVMResultType(env()), 0, nullptr);

            // get input for this UDF
            assert((columnToMapIndex == 0 && !_lastSchemaType.isTupleType()) || columnToMapIndex < _lastSchemaType.parameters().size());

            // index here is already the optimized one, aka when selectionPushDown is used...
            auto elementType = _lastSchemaType.isTupleType() ? _lastSchemaType.parameters()[columnToMapIndex] : _lastSchemaType;
            assert(elementType.withoutOptions().isPrimitiveType()); // only primitives yet supported!
            // convert to Flattened Tuple

            _lastRowInput = _lastRowResult;
            auto load = _lastRowResult.getLoad(builder, {columnToMapIndex});

            // check what type element loaded is (tuple? primitive?)
            FlattenedTuple inVal(&env());
            if(load.val && load.val->getType()->isStructTy()) { // Null value goes to the other branch...
                EXCEPTION("struct type for map column not yet implemented...");
                inVal = FlattenedTuple::fromLLVMStructVal(&env(), builder, load.val, elementType);
            } else {
                inVal.init(elementType);
                // simple element
                inVal.assign(0, load.val, load.size, load.is_null);
            }

            auto exceptionBlock = createExceptionBlock();
            auto outVal = cf.callWithExceptionHandler(builder, inVal, resVal, exceptionBlock, getPointerToVariable(builder, "exceptionCode"));

            if(outVal.getTupleType().isTupleType())
                EXCEPTION("tuples not yet supported...");

            // assign to input vals
#warning "untested code for tuples etc...."
            auto resLoad = codegen::SerializableValue(outVal.get(0), outVal.getSize(0), outVal.getIsNull(0));

            // copy to new vals
            FlattenedTuple ftOut(&env());

            auto params = _lastSchemaType.isTupleType() ? _lastSchemaType.parameters() : std::vector<python::Type>({_lastSchemaType});
            // update with result of UDF
            params[columnToMapIndex] = outVal.getTupleType();
            auto finalType = python::Type::makeTupleType(params);

            ftOut.init(finalType);

            for(int i = 0; i < finalType.parameters().size(); ++i) {
                auto elLoad = i != columnToMapIndex ? _lastRowResult.getLoad(builder, {i}) : resLoad;

                // store in output
                ftOut.set(builder, {i}, elLoad.val, elLoad.size, elLoad.is_null);
            }

            _lastRowResult = ftOut;

            // store final output to variable
            _lastTupleResultVar = ftOut.alloc(variableBuilder);
            ftOut.storeTo(builder, _lastTupleResultVar);

            // check return type..
            if(_lastRowResult.getTupleType().isTupleType()) {
                if(_lastRowResult.getTupleType().parameters()[columnToMapIndex] != cf.output_type) {
                    logger.error("wrong output type. Expected " + finalType.desc() + " got " + _lastRowResult.getTupleType().desc());
                    return false;
                }
            } else {
                if(_lastRowResult.getTupleType() != cf.output_type) {
                    logger.error("wrong output type. Expected " + cf.output_type.desc() + " got " + _lastRowResult.getTupleType().desc());
                    return false;
                }
            }

            _lastBlock = builder.GetInsertBlock();
            assert(_lastBlock);

            // update what the last schema type is
            _lastInputSchemaType = _lastSchemaType;
            _lastSchemaType = finalType;

            _lastOperatorType = LogicalOperatorType::MAPCOLUMN;
            _lastOperatorColumnIndex = columnToMapIndex;
            return true;
        }

        bool PipelineBuilder::withColumnOperation(const int64_t operatorID, int columnToMapIndex,
                                                           const tuplex::UDF &udf, double normalCaseThreshold, bool allowUndefinedBehavior, bool sharedObjectPropagation) {
            using namespace std;
            using namespace llvm;
            auto& logger = Logger::instance().logger("PipelineBuilder");

            // compile dependent on udf
            auto cf = udf.isCompiled() ? const_cast<UDF&>(udf).compile(env()) :
                      const_cast<UDF&>(udf).compileFallback(env(), _constructorBlock, _destructorBlock);

            // stop if compilation didn't succeed
            if(!cf.good())
                return false;

            IRBuilder<> builder(_lastBlock);

            // store in what operator called here (needed for exception handler)
            assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));
            // as stated in the map operation, the result type needs to be allocated within the entry block
            IRBuilder<> variableBuilder(_constructorBlock);
            auto resVal = variableBuilder.CreateAlloca(cf.getLLVMResultType(env()), 0, nullptr);

            // // print out input vals/params
            // for(int i = 0; i < _lastRowResult.numElements(); ++i) {
            //     auto param = codegen::SerializableValue(_lastRowResult.get(i), _lastRowResult.getSize(i), _lastRowResult.getIsNull(i));
            //     assert(param.val);
            //     assert(param.size);
            //     assert(param.is_null);
            //     _env->debugPrint(builder, "input value x" + std::to_string(i) + " of " + cf.name() + ": ", param.val);
            //     _env->debugPrint(builder, "input size of x" + std::to_string(i) + "of " + cf.name() + ": ", param.size);
            //     _env->debugPrint(builder, "input isnull of x" + std::to_string(i) + " " + cf.name() + ": ", param.is_null);
            // }

            auto exceptionBlock = createExceptionBlock();
            auto outVal = cf.callWithExceptionHandler(builder, _lastRowResult, resVal, exceptionBlock, getPointerToVariable(builder, "exceptionCode"));

            if(outVal.getTupleType().isTupleType())
                EXCEPTION("tuples not yet supported...");

            // assign to input vals
            auto resLoad = codegen::SerializableValue(outVal.get(0), outVal.getSize(0), outVal.getIsNull(0));

            // // debug
            // assert(resLoad.val);
            // assert(resLoad.size);
            // assert(resLoad.is_null);
            // _env->debugPrint(builder, "result value of " + cf.name() + ": ", resLoad.val);
            // _env->debugPrint(builder, "result size of " + cf.name() + ": ", resLoad.size);
            // _env->debugPrint(builder, "result isnull " + cf.name() + ": ", resLoad.is_null);

            // copy to new vals
            FlattenedTuple ftOut(&env());

            auto params = _lastSchemaType.isTupleType() ? _lastSchemaType.parameters() : std::vector<python::Type>({_lastSchemaType});
            // update with result of UDF
            if(columnToMapIndex < params.size())
                params[columnToMapIndex] = outVal.getTupleType();
            else {
                assert(columnToMapIndex == params.size()); // append!
                params.emplace_back(outVal.getTupleType());
            }
            auto finalType = python::Type::makeTupleType(params);

            ftOut.init(finalType);

            for(int i = 0; i < finalType.parameters().size(); ++i) {
                auto elLoad = i != columnToMapIndex ? _lastRowResult.getLoad(builder, {i}) : resLoad;

                // store in output
                ftOut.set(builder, {i}, elLoad.val, elLoad.size, elLoad.is_null);
            }

            // create in contructor block a new variable to store tuple result
            _lastRowInput = _lastRowResult;
            _lastRowResult = ftOut;
            // store final output to variable
            _lastTupleResultVar = ftOut.alloc(variableBuilder);
            ftOut.storeTo(builder, _lastTupleResultVar);

            // check return type..
            if(_lastRowResult.getTupleType().isTupleType()) {
                if(_lastRowResult.getTupleType().parameters()[columnToMapIndex] != cf.output_type) {
                    logger.error("wrong output type. Expected " + finalType.desc() + " got " + _lastRowResult.getTupleType().desc());
                    return false;
                }
            } else {
                if(_lastRowResult.getTupleType() != cf.output_type) {
                    logger.error("wrong output type. Expected " + cf.output_type.desc() + " got " + _lastRowResult.getTupleType().desc());
                    return false;
                }
            }

            _lastBlock = builder.GetInsertBlock();
            assert(_lastBlock);

            // update what the last schema type is
            _lastInputSchemaType = _lastSchemaType;
            _lastSchemaType = finalType;
            _lastOperatorType = LogicalOperatorType::WITHCOLUMN;
            _lastOperatorColumnIndex = columnToMapIndex;
            return true;
        }


        llvm::Function* PipelineBuilder::build() {

            auto& logger = Logger::instance().logger("codegen");

            // create ret of void function
            llvm::IRBuilder<> builder(_lastBlock);

            // link blocks
            builder.CreateBr(leaveBlock());


            // create destructor block.
            builder.SetInsertPoint(_destructorBlock);
            // destruct whatever is necessary...

#warning "flatMap not yet implemented, hence return 0 or 1 row exactly"
            auto ec = getVariable(builder, "exceptionCode");
            auto ecOpID = getVariable(builder, "exceptionOperatorID");
            auto numOutputRows = getVariable(builder, "numOutputRows");
            createRet(builder, ec, ecOpID, numOutputRows);

            builder.SetInsertPoint(_constructorBlock);
            builder.CreateBr(_entryBlock);

            // connect all exception blocks with destructor
            // ==> these are the ones without any successor!
            for(auto eBlock : _exceptionBlocks) {
                builder.SetInsertPoint(eBlock);
                // put debug print into exception block
#ifndef NDEBUG
                 // env().debugPrint(builder, "exception occurred with code: ", getVariable(builder, "exceptionCode"));
                 // env().debugPrint(builder, "exception operator ID: ", getVariable(builder, "exceptionOperatorID"));
#endif

                // make sure no successors yet!
                assert(codegen::successorBlockCount(eBlock) == 0);

                // because of exception handling, always assume one row is written.
                // => if more, need to reflect in writer etc.
                assignToVariable(builder, "numOutputRows", env().i64Const(1)); // per default, assign one.

                // if exception handler exists, add call to it
                if(!_exceptionCallbackName.empty()) {
                    using namespace llvm;

                    auto eh_func = exception_handler_prototype(env().getContext(), env().getModule().get(), _exceptionCallbackName);

                    // output original input row
                    // => serialize to runtime memory!
                    logger.debug("Warning: need to make sure types are here correct when using exception handler directly from PipelineBuilder.cc");
                    logger.info("generating exception handler with input data of type " + _argInputRow.getTupleType().desc());
#warning "probably would need to upcast to general case here."
                    auto serialized_row = _argInputRow.serializeToMemory(builder);

                    // call handler
#warning "for flat map, make sure correct row number is used..."
                    Value *userData = _argUserData;
                    Value *ecCode = getVariable(builder, "exceptionCode");
                    Value *ecOpID = getVariable(builder, "exceptionOperatorID");
                    Value *rowNumber = _argRowNumber;
                    Value *badDataPtr = serialized_row.val;
                    Value *badDataLength = serialized_row.size;
                    builder.CreateCall(eh_func, {userData, ecCode, ecOpID, rowNumber, badDataPtr, badDataLength});
                }

                builder.CreateBr(_destructorBlock); // note: is this correct? => prob. yes, reject row at start!
            }

            return _func;
        }

        llvm::Function* PipelineBuilder::buildWithTuplexWriter(const std::string &callbackName,
                                                               int64_t operatorID,
                                                               bool returnCallbackResult) {
            using namespace llvm;
            using namespace std;

            // use last Row as row to serialize, change here if desired
            // @NOTE: ==> when using flatmap, call multipe times
            auto row = _lastRowResult;
            IRBuilder<> builder(_lastBlock);
            const auto& writeCallbackFnName = callbackName;
            auto userData = _argUserData;


            // // debug print
            // for(int i = 0; i < _lastRowResult.numElements(); ++i) {
            //     auto param = codegen::SerializableValue(_lastRowResult.get(i), _lastRowResult.getSize(i), _lastRowResult.getIsNull(i));
            //     assert(param.val);
            //     assert(param.size);
            //     assert(param.is_null);
            //     _env->debugPrint(builder, "final value x" + std::to_string(i) + " of pipeline: ", param.val);
            //     _env->debugPrint(builder, "final size of x" + std::to_string(i) + " of pipeline: ", param.size);
            //     _env->debugPrint(builder, "final isnull of x" + std::to_string(i) + " of pipeline: ", param.is_null);
            // }


            auto serialized_row = row.serializeToMemory(builder);

            // call callback
            // typedef int64_t(*write_row_f)(void*, uint8_t*, int64_t);
            auto& ctx = env().getContext();
            FunctionType *writeCallback_type = FunctionType::get(ctypeToLLVM<int64_t>(ctx), {ctypeToLLVM<void*>(ctx), ctypeToLLVM<uint8_t*>(ctx), ctypeToLLVM<int64_t>(ctx)}, false);
            auto callback_func = env().getModule()->getOrInsertFunction(writeCallbackFnName, writeCallback_type);
            auto callbackECVal = builder.CreateCall(callback_func, {userData, serialized_row.val, serialized_row.size});

            if(returnCallbackResult)
                assignWriteCallbackReturnValue(builder, operatorID, callbackECVal);

            // assign output var!
            assignToVariable(builder, "numOutputRows", env().i64Const(1));

            _lastBlock = builder.GetInsertBlock();
            assert(_lastBlock);
            // @Todo: exception code value for callback function!!!

            // connect blocks together
            return build();
        }

        void PipelineBuilder::assignWriteCallbackReturnValue(llvm::IRBuilder<> &builder, int64_t operatorID,
                                                        llvm::CallInst *callbackECVal) {
            // check result of callback, if not 0 then return exception
            assert(builder.GetInsertBlock());
            auto& ctx = builder.GetInsertBlock()->getContext();
            auto callbackSuccessful = builder.CreateICmpEQ(callbackECVal, env().i64Const(ecToI64(ExceptionCode::SUCCESS)));
            auto bbCallbackDone = llvm::BasicBlock::Create(ctx, "callback_done", builder.GetInsertBlock()->getParent());
            auto bbCallbackFailed = llvm::BasicBlock::Create(ctx, "callback_failed", builder.GetInsertBlock()->getParent());
            builder.CreateCondBr(callbackSuccessful, bbCallbackDone, bbCallbackFailed);
            builder.SetInsertPoint(bbCallbackFailed);

            // store exception info
            assignToVariable(builder, "exceptionCode", callbackECVal);
            assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));

            builder.CreateBr(bbCallbackDone);
            builder.SetInsertPoint(bbCallbackDone);
        }

        SerializableValue PipelineBuilder::makeKey(llvm::IRBuilder<> &builder,
                                                   const tuplex::codegen::SerializableValue &key,
                                                   bool persist) {
            using namespace llvm;

            // isnull?
            if(key.is_null) {

                // are val,size set?
                if(key.val && key.size) {
                    // string valid, simply perform memcpy!
                    assert(key.val->getType() == _env->i8ptrType()); // only string keys supported yet...
                    // nullptr, size 0 if null else malloc & copy
                    BasicBlock* bbCopyKey = BasicBlock::Create(builder.getContext(), "copy_key", builder.GetInsertBlock()->getParent());
                    BasicBlock* bbKeyDone = BasicBlock::Create(builder.getContext(), "key_done", builder.GetInsertBlock()->getParent());
                    BasicBlock* curBlock = builder.GetInsertBlock(); assert(curBlock);

                    builder.CreateCondBr(key.is_null, bbKeyDone, bbCopyKey);

                    builder.SetInsertPoint(bbCopyKey);
                    // copy key? else simply reuse...
                    auto k = key.val;
                    if(persist) {
                        // cmalloc + copy
                        k = _env->cmalloc(builder, key.size);
                        builder.CreateMemCpy(k, 0, key.val, 0, key.size);
                    }
                    builder.CreateBr(bbKeyDone);

                    // Phi nodes
                    builder.SetInsertPoint(bbKeyDone);
                    auto phiVal = builder.CreatePHI(_env->i8ptrType(), 2);
                    auto phiSize = builder.CreatePHI(_env->i64Type(), 2);
                    phiVal->addIncoming(k, bbCopyKey);
                    phiVal->addIncoming(_env->i8nullptr(), curBlock);
                    phiSize->addIncoming(key.size, bbCopyKey);
                    phiSize->addIncoming(_env->i64Const(0), curBlock);
                    return SerializableValue(phiVal, phiSize, key.is_null);
                } else {
                    assert(!key.val && !key.size);
                    // None case
                    return SerializableValue(_env->i8nullptr(), _env->i64Const(0), key.is_null);
                }
            } else {
                // string valid, simply perform memcpy!
                assert(key.val->getType() == _env->i8ptrType()); // only string keys supported yet...
                assert(!key.is_null);

                if(persist) {
                    // simply copy key & return
                    // cmalloc + copy
                    auto k = _env->cmalloc(builder, key.size);
                    builder.CreateMemCpy(k, 0, key.val, 0, key.size);

                    return SerializableValue(k, key.size, _env->i1Const(false));
                } else {
                    return SerializableValue(key.val, key.size, _env->i1Const(false));
                }
            }
        }

        // TODO: make an empty vector mean a null key (so that aggregate can be the same backend as aggregateByKey) -> change unique() pipeline to pass all the columns rather than empty
        llvm::Function * PipelineBuilder::buildWithHashmapWriter(const std::string &callbackName,
                                                                 const std::vector<size_t> &keyCols,
                                                                 const int hashtableWidth,
                                                                 bool bucketize,
                                                                 bool isAggregateByKey) {
            assert(!bucketize || (bucketize && !keyCols.empty() && !isAggregateByKey));
            assert((isAggregateByKey && !bucketize) || !isAggregateByKey);
            assert(hashtableWidth == 8 || hashtableWidth == 0);
            using namespace llvm;
            using namespace std;

            // check colKey and whether it works.
            auto lastType = _lastRowResult.getTupleType();
            python::Type keyType;

            if(!keyCols.empty()) {
                if (!lastType.isTupleType()) { // single column -> keyCols must be {0}
                    if(!(keyCols.size() == 1 && keyCols.front() == 0)) {
                        std::string cols = "[";
                        for (const auto v: keyCols) cols += std::to_string(v) + ", ";
                        cols += "]";
                        throw std::runtime_error(
                                "simple type '" + lastType.desc() + "' given for row type, can't extract key columns " +
                                cols);
                    } else {
                        keyType = lastType;
                    }
                } else { // tuple
                    // build the type
                    std::vector<python::Type> keyTypes;
                    for (const auto col: keyCols) {
                        if (col >= lastType.parameters().size())
                            throw std::runtime_error("column to use as key outside of available columns");
                        keyTypes.push_back(lastType.parameters()[col]);
                    }
                    keyType = python::Type::makeTupleType(keyTypes);
                    if (keyType.parameters().size() == 1) keyType = keyType.parameters().front();
                }
            } else { // no keyCols given -> hash the whole row (e.g. unique)
                if (!lastType.isTupleType() && lastType.withoutOptions() == python::Type::I64)
                    keyType = lastType; // whole row is just an i64 or Option[i64]
                else if(lastType.isTupleType() && lastType.parameters().size() == 1 &&
                        lastType.parameters()[0].withoutOptions() == python::Type::I64)
                    keyType = lastType.parameters()[0]; // whole row is (i64,) or (Option[i64],)
                else
                    keyType = python::Type::STRING; // otherwise, we need to serialize the full row and use a string hashmap
            }

            // perform type conversions: general types become strings (serialize to hash)
            if(keyType.isTupleType()) {
                if(keyType.isOptionType()) throw std::runtime_error("This shouldn't happen.");
                keyType = python::Type::STRING;
            }
            // TODO: eventually, make boolean/f64 into int (potentially with options)

            // what is the key Type?
            auto needsNullBucket = keyType.isOptionType();

            keyType = keyType.withoutOptions();

            // what keyType do we have?
            // => makes sense to hash
            // bool, int64_t, double => uint64_t => bucket hashmap
            if(keyType == python::Type::BOOLEAN || keyType == python::Type::F64) {
                throw std::runtime_error("bool, f64 hashmap not yet implemented");
            }

            // use the C hashmap provided for string, nullvalue, i64
            // => note: that excludeds unique on non-string rows!
            if((keyType != python::Type::STRING && keyType != python::Type::NULLVALUE) // None is ok, b.c. of option
               && (keyType != python::Type::I64))
                throw std::runtime_error("no support for " + keyType.desc() + " yet");

            // start codegen here...
            IRBuilder<> builder(_lastBlock);
            auto &ctx = env().getContext();

            // logic is quite easy
            // simply call the hash callback function
            // with the key value (in cmalloc!) and bucket value (also cmalloc!) => receiving function has to free them
            Value *bucket = _env->i8nullptr();
            Value *bucketSize = _env->i64Const(0);
            Value *key = _env->i8nullptr();
            Value *keySize = _env->i64Const(0);
            Value *keyNull = _env->boolConst(false);
            SerializableValue keyVal;

            if(!keyCols.empty()) {
                if(keyCols.size() == 1) {
                    keyVal = _lastRowResult.getLoad(builder, {static_cast<int>(keyCols.front())});
                } else {
                    // get the values and types
                    std::vector<python::Type> keyTypes;
                    std::vector<SerializableValue> keyValues;
                    for (const auto col: keyCols) {
                        keyTypes.push_back(lastType.parameters()[col]);
                        keyValues.push_back(_lastRowResult.getLoad(builder, {static_cast<int>(col)}));
                    }
                    auto tt = python::Type::makeTupleType(keyTypes);

                    // construct the flattened tuple
                    FlattenedTuple ft(_env.get());
                    ft.init(tt);
                    for (int i = 0; i < keyValues.size(); i++) {
                        ft.set(builder, {i}, keyValues[i].val, keyValues[i].size, keyValues[i].is_null);
                    }

                    keyVal = ft.serializeToMemory(builder); // serialize the value
                }
            } else { // get the whole row -> unique
                if(keyType == python::Type::I64 || keyType == python::Type::STRING)
                    keyVal = _lastRowResult.getLoad(builder, vector<int>{(int) 0});
                else
                    keyVal = _lastRowResult.serializeToMemory(builder);
            }

            if(bucketize) {
                std::set<int> keyColSet(keyCols.begin(), keyCols.end());
                vector<python::Type> py_types;
                for(int i = 0; i < lastType.parameters().size(); ++i)
                    if(keyColSet.count(i) == 0)
                        py_types.push_back(lastType.parameters()[i]);

                // empty? nullptr + bucket size (initialized above)
                if(!py_types.empty()) {
                    auto bucketType = python::Type::makeTupleType(py_types);

                    FlattenedTuple ft(_env.get());
                    ft.init(bucketType);

                    // assign from last tuple & serialize to bucket
                    int pos = 0;
                    for(int i = 0; i < lastType.parameters().size(); ++i) {
                        if(keyColSet.count(i) == 0) {
                            ft.assign(pos++, _lastRowResult.get(i), _lastRowResult.getSize(i), _lastRowResult.getIsNull(i));
                        }
                    }

                    bucketSize = ft.getSize(builder);
                    bucket = _env->cmalloc(builder, bucketSize); // TODO: I think this memory is being leaked! (check TransformTask::writeRowToHashtable and similar functions - I don't think anyone frees the bucket)
                    ft.serialize(builder, bucket);
                }
            }
            if(isAggregateByKey) { // need to pass the entire row to the aggregate function
                bucketSize = _lastRowResult.getSize(builder);
                bucket = _env->cmalloc(builder, bucketSize);
                _lastRowResult.serialize(builder, bucket);
            }

            // convert key...
            // => malloc!
            BasicBlock* bbNullDone = nullptr;
            BasicBlock* curBlock = nullptr;
            if(needsNullBucket) {
                // if check on null on whether to use nullptr as key for null string
                curBlock = builder.GetInsertBlock();

                bbNullDone = BasicBlock::Create(ctx, "null_bucket_done", curBlock->getParent());
                BasicBlock* bbNullBucket = BasicBlock::Create(ctx, "null_bucket", curBlock->getParent());

                builder.CreateCondBr(keyVal.is_null, bbNullDone, bbNullBucket);
                builder.SetInsertPoint(bbNullBucket);
            }

            if(keyType == python::Type::STRING) {
                auto k = makeKey(builder, keyVal, true);
                key = k.val;
                keySize = k.size;
            } else if(keyType == python::Type::I64) { // don't need to copy -> can directly use the value
                if(keyVal.size && keyVal.val) {
                    key = keyVal.val;
                    keySize = keyVal.size;
                } else { // None case? -> shouldn't really happen because of the null_bucket_done branch above
                    assert(false);
                    assert(keyVal.is_null);
                    assert(!keyVal.size && !keyVal.val);
                    key = _env->i64Const(0);
                    keySize = _env->i64Const(0);
                }
            } else if(keyType == python::Type::NULLVALUE) {
                if(hashtableWidth == 0) key = _env->i8nullptr();
                else if(hashtableWidth == 8) key = _env->i64Const(0);
                keySize = _env->i64Const(0);
                keyNull = _env->boolConst(true);
            }

            // if end for null bucket
            if(needsNullBucket) {
                BasicBlock* bbNullBucket = builder.GetInsertBlock();
                builder.CreateBr(bbNullDone);

                builder.SetInsertPoint(bbNullDone);
                // create phi nodes

                llvm::PHINode *phi_key;
                if(keyType == python::Type::I64) {
                    phi_key = builder.CreatePHI(_env->i64Type(), 2);
                    phi_key->addIncoming(_env->i64Const(0), curBlock);
                } else {
                    phi_key = builder.CreatePHI(_env->i8ptrType(), 2);
                    phi_key->addIncoming(_env->i8nullptr(), curBlock);
                }
                phi_key->addIncoming(key, bbNullBucket);

                auto phi_size = builder.CreatePHI(_env->i64Type(), 2);
                phi_size->addIncoming(_env->i64Const(0), curBlock);
                phi_size->addIncoming(keySize, bbNullBucket);

                auto phi_null = builder.CreatePHI(_env->getBooleanType(), 2);
                phi_null->addIncoming(_env->boolConst(true), curBlock); // branch directly
                phi_null->addIncoming(_env->boolConst(false), bbNullBucket);

                key = phi_key;
                keySize = phi_size;
                keyNull = phi_null;
            }

            // call hash callback! see i64_hash_row_f/str_hash_row_f in CodeDefs.h for signature
            if(hashtableWidth == 8) {
                FunctionType *hashCallback_type = FunctionType::get(Type::getVoidTy(ctx),
                                                                    {ctypeToLLVM<void *>(ctx),
                                                                     ctypeToLLVM<int64_t>(ctx), ctypeToLLVM<bool>(ctx),
                                                                     ctypeToLLVM<bool>(ctx), ctypeToLLVM<uint8_t *>(ctx),
                                                                     ctypeToLLVM<int64_t>(ctx)}, false);
                auto callback_func = env().getModule()->getOrInsertFunction(callbackName, hashCallback_type);
                builder.CreateCall(callback_func,
                                   {_argUserData, key, keyNull, _env->boolConst(bucketize), bucket, bucketSize});
            } else {
                FunctionType *hashCallback_type = FunctionType::get(Type::getVoidTy(ctx),
                                                                    {ctypeToLLVM<void *>(ctx),
                                                                     ctypeToLLVM<uint8_t *>(ctx), ctypeToLLVM<int64_t>(ctx),
                                                                     ctypeToLLVM<bool>(ctx), ctypeToLLVM<uint8_t *>(ctx),
                                                                     ctypeToLLVM<int64_t>(ctx)}, false);
                auto callback_func = env().getModule()->getOrInsertFunction(callbackName, hashCallback_type);
                builder.CreateCall(callback_func,
                                   {_argUserData, key, keySize, _env->boolConst(bucketize), bucket, bucketSize});
                // NEW: hashmap handles key dup
                // call free on the key
                _env->cfree(builder, key); // should be NULL safe.
            }
            _env->cfree(builder, bucket); // should be NULL safe.

            // connect blocks
            _lastBlock = builder.GetInsertBlock();
            return build();
        }

        SerializableValue sprintf_csvwriter(llvm::IRBuilder<>& builder, LLVMEnvironment& env, const FlattenedTuple& row, std::string null_value, bool newLineDelimited, char delimiter, char quotechar) {
            using namespace std;
            using namespace llvm;

            // basically write serialization code for data
            string separator = char2str(delimiter);
            null_value = quote(null_value);

            // for strings: need to escape!
            // i.e. escaped size
            // call helper function from runtime for this!
            auto types = row.getFieldTypes();
            string fmtString = "";
            vector<Value*> args;
            // 3 dummy args to be filled later
            args.emplace_back(nullptr);
            args.emplace_back(nullptr);
            args.emplace_back(nullptr);

            Value* fmtSize = env.i64Const(1);
            for(int i = 0; i < row.numElements(); ++i) {
                auto val = row.get(i);
                auto size = row.getSize(i);
                auto type = types[i];

                // make sure no weird types encountered!!
                if(python::Type::BOOLEAN == type) {
                    fmtString += "%s";
                    auto boolCond = builder.CreateICmpNE(env.boolConst(false), val);
                    // select
                    val = builder.CreateSelect(boolCond, env.strConst(builder, "True"), env.strConst(builder, "False"));
                    fmtSize = builder.CreateAdd(fmtSize, env.i64Const(5));

                } else if(python::Type::I64 == type) {
                    fmtString += "%lld";
                    fmtSize = builder.CreateAdd(fmtSize, env.i64Const(20)); // roughly estimate formatted size with 20 bytes
                } else if(python::Type::F64 == type) {
                    fmtString += "%f";
                    fmtSize = builder.CreateAdd(fmtSize, env.i64Const(20)); // roughly estimate formatted size with 20 bytes
                } else if(python::Type::STRING == type) {
                    // call quote
                    auto quotedSize = builder.CreateAlloca(env.i64Type(), 0, nullptr);
                    auto func = quoteForCSV_prototype(env.getContext(), env.getModule().get());
                    val = builder.CreateCall(func, {val, size, quotedSize, env.i8Const(','), env.i8Const('"')});
                    fmtString += "%s";
                    fmtSize = builder.CreateAdd(fmtSize, builder.CreateLoad(quotedSize));
                } else if(type.isOptionType()) {

                    // check element type & call string conversion function with convert
                    auto is_null = row.getIsNull(i);
                    assert(is_null);

                    // check element type
                    auto elementType = type.withoutOptions();
                    if(python::Type::BOOLEAN == elementType) {
                        // check that val is valid
                        if (!val)
                            val = env.boolConst(true);

                        // create arg
                        fmtString += "%s";
                        auto boolCond = builder.CreateICmpNE(env.boolConst(false), val);
                        // select
                        val = builder.CreateSelect(is_null, env.strConst(builder, null_value),
                                                   builder.CreateSelect(boolCond, env.strConst(builder, "True"),
                                                                        env.strConst(builder, "False")));
                        fmtSize = builder.CreateAdd(fmtSize, env.i64Const(std::max(5ul, null_value.length() + 1)));
                    } else if(python::Type::I64 == elementType) {
                        // convert to string
                        // if val is nullptr, append dummy
                        if(!val)
                            val = env.i64Const(0);

                        // call conversion function
                        auto conv_val = env.i64ToString(builder, val);

                        fmtString += "%s";
                        val = builder.CreateSelect(is_null, env.strConst(builder, null_value), conv_val.val);
                        fmtSize = builder.CreateAdd(fmtSize, conv_val.size);
                    } else if(python::Type::F64 == elementType) {
                        // convert to string
                        // if val is nullptr, append dummy
                        if(!val)
                            val = env.f64Const(0.0);

                        // call conversion function
                        auto conv_val = env.f64ToString(builder, val);

                        fmtString += "%s";
                        val = builder.CreateSelect(is_null, env.strConst(builder, null_value), conv_val.val);
                        fmtSize = builder.CreateAdd(fmtSize, conv_val.size);
                    } else if(python::Type::STRING == elementType) {
                        // simple, just None check together with Max intrinsic for size

                        // if val is nullptr, append dummy
                        if(!val)
                            val = env.strConst(builder, "");

                        if(!size)
                            size = env.i64Const(1);

                        fmtString += "%s";
                        val = builder.CreateSelect(is_null, env.strConst(builder, null_value), val);

                        // need to quote
                        auto quotedSize = builder.CreateAlloca(env.i64Type(), 0, nullptr);
                        auto func = quoteForCSV_prototype(env.getContext(), env.getModule().get());
                        val = builder.CreateCall(func, {val, size, quotedSize, env.i8Const(delimiter), env.i8Const(quotechar)});
                        fmtSize = builder.CreateAdd(fmtSize, env.CreateMaximum(builder, size, env.i64Const(null_value.length() + 1)));
                    } else throw std::runtime_error("Option type " + type.desc() + " not yet supported in csv output.");

                } else {
                    // throw exception!
                    throw std::runtime_error("no support for CSV output for type " + type.desc());
                }

                args.emplace_back(val);

                // add separator
                if(i != row.numElements() - 1)
                    fmtString += separator;
            }

            // if newline delimited, add to fmt string!
            if(newLineDelimited)
                fmtString += "\n";

            // use sprintf and speculate a bit on size upfront!
            // then do logic to extend buffer if necessary
            BasicBlock *bbCallback = BasicBlock::Create(env.getContext(), "csvWriteCallback_block", builder.GetInsertBlock()->getParent());
            BasicBlock *bbLargerBuf = BasicBlock::Create(env.getContext(), "strformat_realloc", builder.GetInsertBlock()->getParent());

            auto bufVar = builder.CreateAlloca(env.i8ptrType());
            builder.CreateStore(env.malloc(builder, fmtSize), bufVar);
            auto snprintf_func = snprintf_prototype(env.getContext(), env.getModule().get());

            //{csvRow, fmtSize, env().strConst(builder, fmtString), ...}
            args[0] = builder.CreateLoad(bufVar); args[1] = fmtSize; args[2] = env.strConst(builder, fmtString);
            auto charsRequired = builder.CreateCall(snprintf_func, args);
            auto sizeWritten = builder.CreateAdd(builder.CreateZExt(charsRequired, env.i64Type()), env.i64Const(1));

            // now condition, is this greater than allocSize + 1?
            auto notEnoughSpaceCond = builder.CreateICmpSGT(sizeWritten, fmtSize);

            // two checks: if size is too small, alloc larger buffer!!!
            builder.CreateCondBr(notEnoughSpaceCond, bbLargerBuf, bbCallback);

            // -- block begin --
            builder.SetInsertPoint(bbLargerBuf);
            // realloc with sizeWritten
            // store new malloc in bufVar
            builder.CreateStore(env.malloc(builder, sizeWritten), bufVar);
            args[0] = builder.CreateLoad(bufVar);
            args[1] = sizeWritten;
            builder.CreateCall(snprintf_func, args);

            builder.CreateBr(bbCallback);
            // -- block end --


            // -- block begin --
            builder.SetInsertPoint(bbCallback);



            // then, call writeRow
            auto buf = builder.CreateLoad(bufVar);
            // use string length instead of size, because else writer will copy '\0' too!
            auto length = builder.CreateSub(sizeWritten, env.i64Const(1));

            return SerializableValue(buf, length);
        }


        SerializableValue fast_csvwriter(llvm::IRBuilder<>& builder, LLVMEnvironment& env, const FlattenedTuple& row, std::string null_value, bool newLineDelimited, char delimiter, char quotechar) {
            using namespace std;
            using namespace llvm;

            string trueValue = "true";
            string falseValue = "false";

            // optimized & fast writer using Ryu + branchlut for itoa
            auto num_columns = row.numElements();
            auto types = row.getFieldTypes();

            // quote null value if necessary
            null_value = quote(null_value);

            // not 8 is to have identical results like spark
            // 6 is the precision sprintf uses by default...
            auto max_float_precision = 8; // change if desired, for now fixed

            // reserve space according to quoting + quote etc.
            size_t space_needed = num_columns; // , and \n ( overestimate a bit)
            // add to space estimate fixed types
            bool foundOption = false;
            Value* varSizeRequired = env.i64Const(0);
            for(unsigned i = 0; i < types.size(); ++i) {
                auto t = types[i];

                // short circuit constant valued opt
                if(t.isConstantValued()) {

                    t = simplifyConstantType(t); // could be NULL or so, therefore check again...

                    // TODO: float format, for now ignore
                    if(t.isConstantValued() && t.underlying() == python::Type::F64)
                        Logger::instance().defaultLogger().debug("should do something with float format here.");

                   if(t.isConstantValued() && python::Type::STRING == t.underlying()) {
                       // dequoted constant value!
                       space_needed += quoteForCSV(str_value_from_python_raw_value(t.constant()), delimiter, quotechar).length();
                       continue;
                   }

                   if(t.isConstantValued() && python::Type::I64 == t.underlying()) {
                       space_needed += t.constant().size();
                       continue;
                   }

                   // else, use underlying!
                    if(t.isConstantValued())
                        t = t.underlying();
                }

                if(t.isOptionType())
                    foundOption = true;

                t = t.withoutOptions(); // option is easy, just use null_value
                if(t == python::Type::BOOLEAN) {
                    // saved as true or false
                    space_needed += std::max(strlen("false"), strlen("true"));
                } else if(t == python::Type::I64) {
                    // int64_t needs a maximum of 20 chars
                    space_needed += 20;
                } else if(t == python::Type::F64) {
                    // float is more complicated, do here super conservative estimate.
                    // restrict precision to 8 digits
                    space_needed += 310 + max_float_precision;
                } else if(t == python::Type::NULLVALUE) {
                    space_needed += null_value.length();
                } else if(t == python::Type::STRING) {
                    // fetch size and add, times 2 + 2 because of quoting
                    varSizeRequired = builder.CreateAdd(varSizeRequired, builder.CreateAdd(env.i64Const(2), builder.CreateMul(row.getSize(i), env.i64Const(2))));
                } else {
                    throw std::runtime_error("unsupported type " + t.desc() + " in fast CSV writer");
                }
            }

            // min-max with option type
            space_needed = std::max(space_needed, num_columns * (1 + null_value.length()));

            auto buf = env.malloc(builder, builder.CreateAdd(varSizeRequired, env.i64Const(space_needed)));
            Value* buf_ptr = buf;
            auto& ctx = env.getContext();
            auto func = builder.GetInsertBlock()->getParent(); assert(func);

            auto nullConst = env.strConst(builder, null_value);
            auto trueConst = env.strConst(builder, trueValue);
            auto falseConst = env.strConst(builder, falseValue);

            auto quotedSize = env.CreateFirstBlockAlloca(builder, env.i64Type(), "quoted_str_size");
            // now go over buffer
            for(int i = 0; i < row.numElements(); ++i) {
                auto val = row.get(i);
                auto size = row.getSize(i);
                auto is_null = row.getIsNull(i);
                auto t = types[i];

                // directly use constants!
                //@TODO: can optimize constants next to each other!
                if(t.isConstantValued()) {
                    t = simplifyConstantType(t);

                    // check again, b.c. constants are replaced...

                    if(t.isConstantValued() && python::Type::STRING == t.underlying()) {
                        auto const_val = quoteForCSV(str_value_from_python_raw_value(t.constant()), delimiter, quotechar);

                        // add the delimiter here as well
                        if(i != num_columns - 1)
                            const_val += char2str(delimiter);

                        val = env.strConst(builder, const_val);
                        auto length = env.i64Const(const_val.size());
                        builder.CreateMemCpy(buf_ptr, 0, val, 0, length);
                        buf_ptr = builder.CreateGEP(buf_ptr, length);
                        continue;
                    }

                    if(t.isConstantValued() && (python::Type::I64 == t.underlying() ||
                    python::Type::BOOLEAN == t.underlying() ||
                    python::Type::F64 == t.underlying())) {
                        auto const_val = t.constant();

                        // add the delimiter here as well
                        if(i != num_columns - 1)
                            const_val += char2str(delimiter);

                        val = env.strConst(builder, const_val);
                        auto length = env.i64Const(const_val.size());
                        builder.CreateMemCpy(buf_ptr, 0, val, 0, length);
                        buf_ptr = builder.CreateGEP(buf_ptr, length);
                        continue;
                    }

                    // else, use underlying type!
                    if(t.isConstantValued())
                        t = t.underlying();
                }


                // option?
                BasicBlock* bbNext = nullptr;
                BasicBlock* bbNone = nullptr;
                BasicBlock* bbValue = nullptr;
                llvm::Value* nullBufVal = nullptr;
                if(t.isOptionType()) {
                    // create if block for null and not
                    bbNone = BasicBlock::Create(ctx, "cell(" + to_string(i)+")_null", func);
                    bbValue = BasicBlock::Create(ctx, "cell(" + to_string(i)+")_notnull", func);
                    bbNext = BasicBlock::Create(ctx, "cell(" + to_string(i)+")_done", func);
                    builder.CreateCondBr(builder.CreateICmpEQ(is_null, env.i1Const(true)), bbNone, bbValue);
                    builder.SetInsertPoint(bbNone);
                    if(!null_value.empty()) {
                        builder.CreateMemCpy(buf_ptr, 0, nullConst, 0, null_value.length());
                        nullBufVal = builder.CreateGEP(buf_ptr, env.i32Const(null_value.length()));
                    } else nullBufVal = buf_ptr;

                    builder.CreateBr(bbNext);
                    builder.SetInsertPoint(bbValue);
                }

                // simple types
                if(t.withoutOptions() == python::Type::BOOLEAN) {
                    auto boolCond = builder.CreateICmpNE(env.boolConst(false), val);
                    // copy via if
                    BasicBlock* bbTrue = BasicBlock::Create(ctx,"cell(" + to_string(i)+")_true", func);
                    BasicBlock* bbFalse = BasicBlock::Create(ctx,"cell(" + to_string(i)+")_false", func);
                    BasicBlock* bbDone = BasicBlock::Create(ctx,"cell(" + to_string(i)+")_truefalse_done", func);
                    builder.CreateCondBr(boolCond, bbTrue, bbFalse);
                    builder.SetInsertPoint(bbTrue);
                    builder.CreateMemCpy(buf_ptr, 0, trueConst, 0, trueValue.length());
                    auto true_buf_ptr = builder.CreateGEP(buf_ptr, env.i32Const(trueValue.length()));
                    builder.CreateBr(bbDone);
                    builder.SetInsertPoint(bbFalse);
                    builder.CreateMemCpy(buf_ptr, 0, falseConst, 0, falseValue.length());
                    auto false_buf_ptr = builder.CreateGEP(buf_ptr, env.i32Const(falseValue.length()));
                    builder.CreateBr(bbDone);

                    builder.SetInsertPoint(bbDone);
                    auto phi = builder.CreatePHI(env.i8ptrType(), 2);
                    phi->addIncoming(true_buf_ptr, bbTrue);
                    phi->addIncoming(false_buf_ptr, bbFalse);
                    buf_ptr = phi;
                } else if(t.withoutOptions() == python::Type::I64) {
                    // call fast int to str which auto moves the pointer
                    auto ft = i64toa_prototype(ctx, env.getModule().get());
                    // NOTE: must be <= 20
                    auto bytes_written = builder.CreateCall(ft, {val, buf_ptr});
                    buf_ptr = builder.CreateGEP(buf_ptr, bytes_written);
                } else if(t.withoutOptions() == python::Type::F64) {
                    // call ryu fast double to str function with fixed precision
                    auto ft = d2fixed_prototype(ctx, env.getModule().get());
                    // NOTE: must be <= 310 + max_float_precision
                    auto bytes_written = builder.CreateCall(ft, {val, env.i32Const(max_float_precision), buf_ptr});
                    buf_ptr = builder.CreateGEP(buf_ptr, bytes_written);
                } else if(t.withoutOptions() == python::Type::STRING) {
                    // Note by directly copying over without the additional rtmalloc, higher speed could be achieved as well...
                    // use SSE42 instructions to quickly check if quoting is necessary
                    // copy over everything but need to quote first
                    auto func = quoteForCSV_prototype(env.getContext(), env.getModule().get());
                    val = builder.CreateCall(func, {val, size, quotedSize, env.i8Const(delimiter), env.i8Const(quotechar)});
                    size = builder.CreateLoad(quotedSize);
                    auto length = builder.CreateSub(size, env.i64Const(1));
                    builder.CreateMemCpy(buf_ptr, 0, val, 0, length);
                    buf_ptr = builder.CreateGEP(buf_ptr, length);
                } else if(t.withoutOptions() == python::Type::NULLVALUE) {
                    if(!null_value.empty()) {
                        builder.CreateMemCpy(buf_ptr, 0, nullConst, 0, null_value.length());
                        buf_ptr = builder.CreateGEP(buf_ptr, env.i32Const(null_value.length()));
                    }
                }

                if(t.isOptionType()) {
                    assert(bbNext);
                    auto lastBlock = builder.GetInsertBlock();
                    builder.CreateBr(bbNext);
                    builder.SetInsertPoint(bbNext);
                    // phi update
                    auto phi = builder.CreatePHI(env.i8ptrType(), 2);
                    phi->addIncoming(nullBufVal, bbNone);
                    phi->addIncoming(buf_ptr, lastBlock);
                    buf_ptr = phi;
                }

                // store delimiter if not last column
                if(i != num_columns - 1) {
                    builder.CreateStore(env.i8Const(delimiter), buf_ptr);
                    buf_ptr = builder.CreateGEP(buf_ptr, env.i32Const(1)); // move by 1 byte
                }
            }

            // newline delimited?
            if(newLineDelimited) {
                builder.CreateStore(env.i8Const('\n'), buf_ptr);
                buf_ptr = builder.CreateGEP(buf_ptr, env.i32Const(1)); // move by 1 byte
            }

            // compute buf_length via ptr diff
            auto buf_length = builder.CreateSub(builder.CreatePtrToInt(buf_ptr, env.i64Type()), builder.CreatePtrToInt(buf, env.i64Type()));

            return SerializableValue(buf, buf_length);
        }


        llvm::Function *PipelineBuilder::buildWithCSVRowWriter(const std::string &callbackName, int64_t operatorID,
                                                               bool returnCallbackResult,
                                                               const std::string &null_value, bool newLineDelimited,
                                                               char delimiter, char quotechar) {

            // Note:
            // this function is rather slow. We want to speed it up to make IO faster
            // for this following tricks can be used
            // 1.) Use Ryu for float to str conversion (https://github.com/ulfjack/ryu)
            //     ==> need to use 310 + precision chars as buf for each float oO
            // 2.) Use branchlut from https://github.com/miloyip/itoa-benchmark for int to str
            //     ==> max char length of 64bit int: 20 chars
            // 3.) explicitly codegen for string& Co (i.e.
            // => allocate temp buffer via malloc & free
            //

            // call callback function with data serialized in CSV format
            using namespace llvm;
            using namespace std;

            // use last Row as row to serialize, change here if desired
            auto row = _lastRowResult;
            IRBuilder<> builder(_lastBlock);
            auto writeCallbackFnName = callbackName;
            auto userData = _argUserData;

            // // old: use sprintf based writer, this is quite slow
            //auto csv_row = sprintf_csvwriter(builder, env(), row, null_value, newLineDelimited, delimiter, quotechar);

            // new: codegen writer with fast itoa, dtoa functions
            auto csv_row = fast_csvwriter(builder, env(), row, null_value, newLineDelimited, delimiter, quotechar);

            // typedef int64_t(*write_row_f)(void*, uint8_t*, int64_t);
            auto& ctx = env().getContext();
            FunctionType *writeCallback_type = FunctionType::get(ctypeToLLVM<int64_t>(ctx), {ctypeToLLVM<void*>(ctx), ctypeToLLVM<uint8_t*>(ctx), ctypeToLLVM<int64_t>(ctx)}, false);
            auto callback_func = env().getModule()->getOrInsertFunction(writeCallbackFnName, writeCallback_type);
            auto callbackECVal = builder.CreateCall(callback_func, {userData, csv_row.val, csv_row.size});

            if(returnCallbackResult)
                assignWriteCallbackReturnValue(builder, operatorID, callbackECVal);

            // assign output var!
            assignToVariable(builder, "numOutputRows", env().i64Const(1));

            _lastBlock = builder.GetInsertBlock();
            assert(_lastBlock);
            // @Todo: exception code value for callback function!!!

            // connect blocks together
            return build();
        }

        PipelineBuilder::PipelineResult PipelineBuilder::call(llvm::IRBuilder<> &builder,
                                                              llvm::Function *func,
                                                              const FlattenedTuple &ft,
                                                              llvm::Value *userData,
                                                              llvm::Value *rowNumber,
                                                              llvm::Value* intermediate) {
            assert(func && userData && rowNumber);
            auto tuplePtr = ft.loadToPtr(builder);

            // TODO: get rid off unnecessary load/store instructions here...

            // type checks
            auto env = ft.getEnv();
            assert(userData->getType() == env->i8ptrType());

            // @TODO: get rid off first block alloca and use llvm::lifetime::begin and end...

            // alloc variable in first block
            auto result_ptr = LLVMEnvironment::CreateFirstBlockAlloca(builder,
                                                                      resultStructType(builder.getContext()),
                                                                      "pipeline_result");

            // failure from load?
            std::vector<llvm::Value*> args{result_ptr, userData, tuplePtr, rowNumber};
            if(intermediate) {
                args.push_back(intermediate);
            }

            builder.CreateCall(func, args);

            // load via StructGEP
            PipelineResult pr;

            pr.resultCode = builder.CreateLoad(LLVMEnvironment::CreateStructGEP(builder, result_ptr, 0));
            pr.exceptionOperatorID = builder.CreateLoad(LLVMEnvironment::CreateStructGEP(builder, result_ptr, 1));
            pr.numProducedRows = builder.CreateLoad(LLVMEnvironment::CreateStructGEP(builder, result_ptr, 2));
            return pr;
        }

        llvm::BasicBlock* PipelineBuilder::createExceptionBlock(const std::string &name) {
            using namespace llvm;
            assert(_constructorBlock);
            auto block = BasicBlock::Create(_constructorBlock->getContext(), name, _constructorBlock->getParent());
            _exceptionBlocks.emplace_back(block);
            return block;
        }

        llvm::Function* createSingleProcessRowWrapper(PipelineBuilder& pip, const std::string& name) {
            auto pipFunc = pip.getFunction();

            if(!pipFunc)
                return nullptr;

            // create function
            using namespace llvm;
            using namespace std;

            // create (internal) llvm function to be inlined with all contents
            auto& context = pipFunc->getContext();

            // signature is basically i8* userData, ?* row
            FunctionType *func_type = FunctionType::get(Type::getInt64Ty(context),
                                                        {Type::getInt8PtrTy(context, 0),
                                                         Type::getInt8PtrTy(context, 0),
                                                         Type::getInt64Ty(context), Type::getInt64Ty(context)}, false);
            auto func = Function::Create(func_type, Function::ExternalLinkage, name, pipFunc->getParent());

            // set arg names
            auto args = mapLLVMFunctionArgs(func, {"userData", "rowBuf", "bufSize", "rowNumber"});

            auto body = BasicBlock::Create(context, "body", func);
            IRBuilder<> builder(body);

            FlattenedTuple tuple(&pip.env());
            tuple.init(pip.inputRowType());
            tuple.deserializationCode(builder, args["rowBuf"]);
            Value* bytesRead = tuple.getSize(builder);

            // add potentially exception handler function
            PipelineBuilder::call(builder, pipFunc, tuple, args["userData"], args["rowNumber"]);

            builder.CreateRet(bytesRead);
            return func;
        }

        bool normalTypeCompatible(const python::Type& normalType, const python::Type& superType) {

            assert(normalType.isTupleType() && superType.isTupleType());

            // todo: implement
            if(normalType.parameters().size() != superType.parameters().size())
                return false;

            return true;
        }

        std::shared_ptr<FlattenedTuple> decodeCells(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                                    const python::Type& rowType,
                                                    llvm::Value* numCells, llvm::Value* cellsPtr, llvm::Value* sizesPtr,
                                                    llvm::BasicBlock* exceptionBlock,
                                                    const std::vector<std::string>& null_values) {
            using namespace llvm;
            using namespace std;
            auto ft = make_shared<FlattenedTuple>(&env);

            ft->init(rowType);
            assert(rowType.isTupleType());
            assert(exceptionBlock);

            assert(cellsPtr->getType() == env.i8ptrType()->getPointerTo()); // i8** => array of char* pointers
            assert(sizesPtr->getType() == env.i64ptrType()); // i64* => array of int64_t

            // check numCells
            auto func = builder.GetInsertBlock()->getParent(); assert(func);
            BasicBlock* bbCellNoOk = BasicBlock::Create(env.getContext(), "noCellsOK", func);
            auto cell_match_cond = builder.CreateICmpEQ(numCells, llvm::ConstantInt::get(numCells->getType(), (uint64_t)rowType.parameters().size()));
            builder.CreateCondBr(cell_match_cond, bbCellNoOk, exceptionBlock);

            BasicBlock* nullErrorBlock = exceptionBlock;
            BasicBlock* valueErrorBlock = exceptionBlock;


            auto cellRowType = rowType;
            // if single tuple element, just use that... (i.e. means pipeline interprets first arg as tuple...)
            assert(cellRowType.isTupleType());
            if(cellRowType.parameters().size() == 1 && cellRowType.parameters().front().isTupleType()
               && cellRowType.parameters().front().parameters().size() > 1)
                cellRowType = cellRowType.parameters().front();

            assert(cellRowType.parameters().size() == ft->flattenedTupleType().parameters().size()); /// this must hold!

            builder.SetInsertPoint(bbCellNoOk);
            // check type & assign
            for(int i = 0; i < cellRowType.parameters().size(); ++i) {
                auto t = cellRowType.parameters()[i];

                llvm::Value* isnull = nullptr;

                // option type? do NULL value interpretation
                if(t.isOptionType()) {
                    auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env.i64Const(i)), "x" + std::to_string(i));
                    isnull = env.compareToNullValues(builder, val, null_values, true);
                } else if(t != python::Type::NULLVALUE) {
                    // null check, i.e. raise NULL value exception!
                    auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env.i64Const(i)), "x" + std::to_string(i));
                    auto null_check = env.compareToNullValues(builder, val, null_values, true);

                    // if positive, exception!
                    // else continue!
                    BasicBlock* bbNullCheckPassed = BasicBlock::Create(builder.getContext(), "col" + std::to_string(i) + "_null_check_passed", builder.GetInsertBlock()->getParent());
                    builder.CreateCondBr(null_check, nullErrorBlock, bbNullCheckPassed);
                    builder.SetInsertPoint(bbNullCheckPassed);
                }

                t = t.withoutOptions();

                // values?
                if(python::Type::STRING == t) {
                    // fill in
                    auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env.i64Const(i)),
                                                  "x" + std::to_string(i));
                    auto size = builder.CreateLoad(builder.CreateGEP(sizesPtr, env.i64Const(i)),
                                                   "s" + std::to_string(i));
                    ft->assign(i, val, size, isnull);
                } else if(python::Type::BOOLEAN == t) {
                    // conversion code here
                    auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env.i64Const(i)), "x" + std::to_string(i));
                    auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env.i64Const(i)), "s" + std::to_string(i));
                    auto val = parseBoolean(env, builder, valueErrorBlock, cellStr, cellSize, isnull);
                    ft->assign(i, val.val, val.size, isnull);
                } else if(python::Type::I64 == t) {
                    // conversion code here
                    auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env.i64Const(i)), "x" + std::to_string(i));
                    auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env.i64Const(i)), "s" + std::to_string(i));
                    auto val = parseI64(env, builder, valueErrorBlock, cellStr, cellSize, isnull);
                    ft->assign(i, val.val, val.size, isnull);
                } else if(python::Type::F64 == t) {
                    // conversion code here
                    auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env.i64Const(i)), "x" + std::to_string(i));
                    auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env.i64Const(i)), "s" + std::to_string(i));
                    auto val = parseF64(env, builder, valueErrorBlock, cellStr, cellSize, isnull);
                    ft->assign(i, val.val, val.size, isnull);
                } else if(python::Type::NULLVALUE == t) {
                    // perform null check only, & set null element depending on result
                    auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env.i64Const(i)), "x" + std::to_string(i));
                    isnull = env.compareToNullValues(builder, val, null_values, true);

                    // if not null, exception! ==> i.e. ValueError!
                    BasicBlock* bbNullCheckPassed = BasicBlock::Create(builder.getContext(), "col" + std::to_string(i) + "_value_check_passed", builder.GetInsertBlock()->getParent());
                    builder.CreateCondBr(isnull, bbNullCheckPassed, valueErrorBlock);
                    builder.SetInsertPoint(bbNullCheckPassed);
                    ft->assign(i, nullptr, nullptr, env.i1Const(true)); // set NULL (should be ignored)
                } else {
                    // NOTE: only flat, primitives yet supported. I.e. there can't be lists/dicts within a cell...
                    throw std::runtime_error("unsupported type " + t.desc() + " in decodeCells encountered");
                }
            }

            return ft;
        }

        llvm::Function* createProcessExceptionRowWrapper(PipelineBuilder& pip,
                const std::string& name, const python::Type& normalCaseType,
                const std::map<int, int>& normalToGeneralMapping,
                const std::vector<std::string>& null_values,
                const CompilePolicy& policy) {

            // uncomment to get debug printing for this function
            // #define PRINT_EXCEPTION_PROCESSING_DETAILS

            auto& logger = Logger::instance().logger("codegen");

            auto pipFunc = pip.getFunction();

            if(!pipFunc)
                return nullptr;

            auto generalCaseType = pip.inputRowType();
            bool normalCaseAndGeneralCaseCompatible = checkCaseCompatibility(normalCaseType, generalCaseType, normalToGeneralMapping);

            if(!normalCaseAndGeneralCaseCompatible) {
                logger.debug("normal and general case are not compatible, forcing all exceptions on fallback (interpreter) path.");
                std::stringstream ss;
                ss<<"normal -> general\n";
                for(unsigned i = 0; i < normalCaseType.parameters().size(); ++i) {

                    if(normalToGeneralMapping.find(i) == normalToGeneralMapping.end()) {
                        logger.error("invalid index in normal -> general map found");
                        continue;
                    }

                    ss<<"("<<i<<"): "<<normalCaseType.parameters()[i].desc()
                                     <<" -> "<<generalCaseType.parameters()[normalToGeneralMapping.at(i)].desc()
                                     <<"\n";
                }
                logger.debug(ss.str());
            }

            // the type are a bit screwed because of tuple mode or not
            // @TODO: make this cleaner in further releases...
            // // check that pip's inputtype + normal type are compatible
            // if(!normalTypeCompatible(normalCaseType, generalCaseType))
            //     throw std::runtime_error("can't generate slow code path for incompatible/not upgradeable compilable type");

            auto num_columns = generalCaseType.parameters().size();

            // create function
            using namespace llvm;
            using namespace std;

            // create (internal) llvm function to be inlined with all contents
            auto& context = pipFunc->getContext();
            auto& env = pip.env();

            // void* userData, int64_t rowNumber, int64_t ExceptionCode, uint8_t* inputBuffer, int64_t inputBufferSize
            FunctionType *func_type = FunctionType::get(Type::getInt64Ty(context),
                                                        {Type::getInt8PtrTy(context, 0),
                                                         Type::getInt64Ty(context),
                                                         Type::getInt64Ty(context),
                                                         Type::getInt8PtrTy(context, 0),
                                                         Type::getInt64Ty(context)}, false);
            auto func = Function::Create(func_type, Function::ExternalLinkage, name, pipFunc->getParent());

            // set arg names
            auto args = mapLLVMFunctionArgs(func, {"userData",  "rowNumber", "exceptionCode", "rowBuf", "bufSize",});

            auto body = BasicBlock::Create(context, "body", func);
            IRBuilder<> builder(body);
#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
             env.debugPrint(builder, "slow process functor entered!");
             env.debugPrint(builder, "exception buffer size is: ", args["bufSize"]);
#endif
            // decode according to exception type => i.e. decode according to pipeline builder + nullvalue opt!
            auto ecCode = args["exceptionCode"];
            auto dataPtr = args["rowBuf"];

            auto bbStringFieldDecode = BasicBlock::Create(context, "decodeStrings", func);
            auto bbNormalCaseDecode = BasicBlock::Create(context, "decodeNormalCase", func);
            auto bbCommonCaseDecode = BasicBlock::Create(context, "decodeCommonCase", func);

            auto switchInst = builder.CreateSwitch(ecCode, bbNormalCaseDecode, 2);
            switchInst->addCase(cast<ConstantInt>(env.i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT))), bbStringFieldDecode);
            switchInst->addCase(cast<ConstantInt>(env.i64Const(ecToI64(ExceptionCode::NORMALCASEVIOLATION))), bbCommonCaseDecode);


            // three decode options
            {
                // 1.) decode string fields & match with exception case type
                // i.e. first: num-columns check, second type check
                // => else exception, i.e. handle in interpreter
                BasicBlock *bbStringDecodeFailed = BasicBlock::Create(context, "decodeStringsFailed", func);
                builder.SetInsertPoint(bbStringFieldDecode);
#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
                env.debugPrint(builder, "decoding a string type exception");
#endif

                // decode into noCells, cellsPtr, sizesPtr etc.
                auto noCells = builder.CreateLoad(builder.CreatePointerCast(dataPtr, env.i64ptrType()));

#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
                 env.debugPrint(builder, "parsed #cells: ", noCells);
#endif
                dataPtr = builder.CreateGEP(dataPtr, env.i32Const(sizeof(int64_t)));
                // heap alloc arrays, could be done on stack as well but whatever
                auto cellsPtr = builder.CreatePointerCast(
                        env.malloc(builder, env.i64Const(num_columns * sizeof(uint8_t*))),
                        env.i8ptrType()->getPointerTo());
                auto sizesPtr = builder.CreatePointerCast(env.malloc(builder, env.i64Const(num_columns * sizeof(int64_t))),
                                                          env.i64ptrType());
                for (unsigned i = 0; i < num_columns; ++i) {
                    // decode size + offset & store accordingly!
                    auto info = builder.CreateLoad(builder.CreatePointerCast(dataPtr, env.i64ptrType()));
                    // truncation yields lower 32 bit (= offset)
                    Value *offset = builder.CreateTrunc(info, Type::getInt32Ty(context));
                    // right shift by 32 yields size
                    Value *size = builder.CreateLShr(info, 32);

                    builder.CreateStore(size, builder.CreateGEP(sizesPtr, env.i32Const(i)));
                    builder.CreateStore(builder.CreateGEP(dataPtr, offset),
                                        builder.CreateGEP(cellsPtr, env.i32Const(i)));

#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
                      env.debugPrint(builder, "cell("  + std::to_string(i) + ") size: ", size);
                      env.debugPrint(builder, "cell("  + std::to_string(i) + ") offset: ", offset);
                      env.debugPrint(builder, "cell " + std::to_string(i) + ": ", builder.CreateLoad(builder.CreateGEP(cellsPtr, env.i32Const(i))));
#endif

                    dataPtr = builder.CreateGEP(dataPtr, env.i32Const(sizeof(int64_t)));
                }

                auto ft = decodeCells(env, builder, generalCaseType, noCells, cellsPtr, sizesPtr, bbStringDecodeFailed,
                                      null_values);

                // call pipeline & return its code
                auto res = PipelineBuilder::call(builder, pipFunc, *ft, args["userData"], args["rowNumber"]);
                auto resultCode = builder.CreateZExtOrTrunc(res.resultCode, env.i64Type());
                auto resultOpID = builder.CreateZExtOrTrunc(res.exceptionOperatorID, env.i64Type());
                auto resultNumRowsCreated = builder.CreateZExtOrTrunc(res.numProducedRows, env.i64Type());

#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
                 env.debugPrint(builder, "calling pipeline yielded #rows: ", resultNumRowsCreated);
#endif
                env.freeAll(builder);
                builder.CreateRet(resultCode);

                builder.SetInsertPoint(bbStringDecodeFailed);
#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
                 env.debugPrint(builder, "string decode failed");
#endif
                env.freeAll(builder);
                builder.CreateRet(ecCode); // original exception code.
            }
            // 2.) decode normal case type & upgrade to exception case type, then apply all resolvers & Co
            {
                builder.SetInsertPoint(bbNormalCaseDecode);
#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
                 env.debugPrint(builder, "exception is in normal case format, feed through resolvers&Co");
#endif
                // i.e. same code as in pip upgradeType
                FlattenedTuple ft(&env);
                ft.init(normalCaseType);
                ft.deserializationCode(builder, args["rowBuf"]);

                FlattenedTuple tuple(&env); // general case tuple
                if(!normalCaseAndGeneralCaseCompatible) {
                    // can null compatibility be achieved? if not jump directly to returning exception forcing it onto interpreter path
                    assert(pip.inputRowType().isTupleType());
                    auto col_types = pip.inputRowType().parameters();
                    auto normal_col_types = normalCaseType.parameters();
                    // fill in according to mapping normal case type
                    for(auto keyval : normalToGeneralMapping) {
                        assert(keyval.first < normal_col_types.size());
                        assert(keyval.second < col_types.size());
                        col_types[keyval.second] = normal_col_types[keyval.first];
                    }
                    python::Type extendedNormalCaseType = python::Type::makeTupleType(col_types);
                    if(canAchieveAtLeastNullCompatibility(extendedNormalCaseType, pip.inputRowType())) {
                        // null-extraction and then call pipeline
                        BasicBlock *bb_failure = BasicBlock::Create(context, "nullextract_failed", func);
                        tuple = normalToGeneralTupleWithNullCompatibility(builder,
                                                                          &env,
                                                                          ft,
                                                                          normalCaseType,
                                                                          pip.inputRowType(),
                                                                          normalToGeneralMapping,
                                                                          bb_failure,
                                                                          policy.allowNumericTypeUnification);
                        builder.SetInsertPoint(bb_failure);
                        // all goes onto exception path
                        // retain original exception, force onto interpreter path
                        env.freeAll(builder);
                        builder.CreateRet(ecCode); // original
                    } else {
                        // all goes onto exception path
                        // retain original exception, force onto interpreter path
                        env.freeAll(builder);
                        builder.CreateRet(ecCode); // original
                    }
                } else {
                    // upcast to general type!
                    tuple = normalToGeneralTuple(builder, ft, normalCaseType, pip.inputRowType(), normalToGeneralMapping);

#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
                    ft.print(builder);
                 env.debugPrint(builder, "row casted, processing pipeline now!");
                 tuple.print(builder);
#endif
                    auto res = PipelineBuilder::call(builder, pipFunc, tuple, args["userData"], args["rowNumber"]);
                    auto resultCode = builder.CreateZExtOrTrunc(res.resultCode, env.i64Type());
                    auto resultOpID = builder.CreateZExtOrTrunc(res.exceptionOperatorID, env.i64Type());
                    auto resultNumRowsCreated = builder.CreateZExtOrTrunc(res.numProducedRows, env.i64Type());
                    env.freeAll(builder);
                    builder.CreateRet(resultCode);
                }
            }


            // 3.) decode common/exception case type
            {
                builder.SetInsertPoint(bbCommonCaseDecode);
                // only if cases are compatible
                if(normalCaseAndGeneralCaseCompatible) {
#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
                     env.debugPrint(builder, "exception is in super type format, feed through resolvers&Co");
#endif
                    // easiest, no additional steps necessary...
                    FlattenedTuple tuple(&pip.env());
                    tuple.init(pip.inputRowType());
                    tuple.deserializationCode(builder, args["rowBuf"]);

                    // add potentially exception handler function
                    auto res = PipelineBuilder::call(builder, pipFunc, tuple, args["userData"], args["rowNumber"]);
                    auto resultCode = builder.CreateZExtOrTrunc(res.resultCode, env.i64Type());
                    auto resultOpID = builder.CreateZExtOrTrunc(res.exceptionOperatorID, env.i64Type());
                    auto resultNumRowsCreated = builder.CreateZExtOrTrunc(res.numProducedRows, env.i64Type());
                    env.freeAll(builder);
                    builder.CreateRet(resultCode);
                } else {
                    // retain original exception, force onto interpreter path
                    env.freeAll(builder);
                    builder.CreateRet(ecCode); // original
                }
            }

            return func;
        }

        bool PipelineBuilder::addTypeUpgrade(const python::Type &rowType) {
            using namespace std;
            using namespace llvm;
            auto& logger = Logger::instance().logger("PipelineBuilder");

            assert(rowType.isTupleType());

            if((_lastRowResult.flattenedTupleType().parameters().size() != rowType.parameters().size())) {
                logger.error("types not compatible.");
                return false;
            }

            IRBuilder<> builder(_lastBlock);
            try {
                _lastRowResult = _lastRowResult.upcastTo(builder, rowType);
            } catch (const std::exception& e) {
                logger.error("type upcast failed: " + std::string(e.what()));
                return false;
            }

            return true;
        }

        void PipelineBuilder::beginForLoop(llvm::IRBuilder<> &builder, llvm::Value *numIterations) {

            using namespace llvm;
            auto& context = builder.getContext();

            // numIterations should be i32!
            assert(numIterations);
            assert(numIterations->getType() == _env->i32Type());

            // start loop here
            auto loopVar = _env->CreateFirstBlockAlloca(builder, _env->i32Type(), "loop_i");
            builder.CreateStore(_env->i32Const(0), loopVar);
            BasicBlock* bbLoopCondition = BasicBlock::Create(context, "loop_cond", builder.GetInsertBlock()->getParent());
            BasicBlock* bbLoopBody = BasicBlock::Create(context, "loop_body", builder.GetInsertBlock()->getParent());

            builder.CreateBr(bbLoopCondition);
            builder.SetInsertPoint(bbLoopCondition);

            // loopVar < num_rows_to_join
            auto cond = builder.CreateICmpNE(builder.CreateLoad(loopVar), numIterations);
            //_env->debugPrint(builder, "loop var is: ", builder.CreateLoad(loopVar));
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(loopVar), _env->i32Const(1)), loopVar); // update loop var...
            builder.CreateCondBr(cond, bbLoopBody, leaveBlock()); // loop done, i.e. pipeline ended

            builder.SetInsertPoint(bbLoopBody);
            _loopLevel++;

            // push back condition as current leave block!
            _leaveBlocks.push_back(bbLoopCondition);
        }


        void PipelineBuilder::createInnerJoinBucketLoop(llvm::IRBuilder<>& builder,
                                                        llvm::Value* num_rows_to_join,
                                                        llvm::Value* bucketPtrVar,
                                                        bool buildRight,
                                                        python::Type buildBucketType,
                                                        python::Type resultType,
                                                        int probeKeyIndex) {
            using namespace llvm;
            using namespace std;

            // start loop over all rows in bucket, need to update pointer var to decode the next one
            //_env->debugPrint(builder, "found num_rows_to_join: ", num_rows_to_join);
            beginForLoop(builder, num_rows_to_join);

            // there should be at least one row (omit weird loop for now b.c. more difficult...)
            auto bucketPtr = builder.CreateLoad(bucketPtrVar);
            auto row_length = builder.CreateLoad(builder.CreatePointerCast(bucketPtr, _env->i32ptrType()));
            auto row_ptr = builder.CreateGEP(bucketPtr, _env->i32Const(sizeof(int32_t)));
            // update bucketPtr Var with sizeof(int32_t) + data length
            builder.CreateStore(builder.CreateGEP(bucketPtr, builder.CreateAdd(row_length, _env->i32Const(sizeof(int32_t)))), bucketPtrVar);

            //_env->debugPrint(builder, "decoding in-bucket row with length : ", row_length);

            // combine into one row
            auto ftProbe = _lastRowResult; // last row type is always the probe type (i.e. the larger one)

            codegen::FlattenedTuple ftBuild(_env.get());
            // only deserialize if there are cols in the bucket (special case: one column join)
            if(!buildBucketType.parameters().empty()) {
                ftBuild.init(buildBucketType); //!!! important, b.c. key does not get saved to bucket...
                ftBuild.deserializationCode(builder, row_ptr);
            }

            //_env->debugPrint(builder, "ftBuild is: ");
            // for(int i = 0; i < ftBuild.numElements(); ++i) {
            //     _env->debugPrint(builder, "column (" + to_string(i) + "): ", ftBuild.get(i));
            // }

            //_env->debugPrint(builder, "ftProbe is: ");
            // for(int i = 0; i < ftProbe.numElements(); ++i) {
            //     _env->debugPrint(builder, "column (" + to_string(i) + "): ", ftProbe.get(i));
            // }

            codegen::FlattenedTuple ftResult(_env.get());
            ftResult.init(resultType);
            // cout<<"output row type is: "<<ftResult.getTupleType().desc()<<endl;
            if(buildRight) {

                // decode result type
                // => note
                // add all from probe, then build
                // !!! direction !!!
                int pos = 0;
                for (int i = 0; i < ftProbe.numElements(); ++i) {
                    if (i != probeKeyIndex)
                        ftResult.assign(pos++, ftProbe.get(i), ftProbe.getSize(i), ftProbe.getIsNull(i));
                }

                // add key from probe
                ftResult.assign(pos, ftProbe.get(probeKeyIndex), ftProbe.getSize(probeKeyIndex),
                                ftProbe.getIsNull(probeKeyIndex));
                pos++;

                // add build elements (all here b.c. from in-bucket type...)
                for (int i = 0; i < ftBuild.numElements(); ++i) {
                    ftResult.assign(pos++, ftBuild.get(i), ftBuild.getSize(i), ftBuild.getIsNull(i));
                }
            } else {

                // add all from build, then from probe
                // !!! direction !!!
                int pos = 0;

                // add build elements (all here b.c. from in-bucket type...)
                for (int i = 0; i < ftBuild.numElements(); ++i) {
                    // this here is in bucket tuple, so key was already stripped away from it
                    ftResult.assign(pos++, ftBuild.get(i), ftBuild.getSize(i), ftBuild.getIsNull(i));
                }

                // add key from probe
                ftResult.assign(pos, ftProbe.get(probeKeyIndex), ftProbe.getSize(probeKeyIndex),
                                ftProbe.getIsNull(probeKeyIndex));
                pos++;

                // add all except key from probe
                for (int i = 0; i < ftProbe.numElements(); ++i) {
                    if (i != probeKeyIndex)
                        ftResult.assign(pos++, ftProbe.get(i), ftProbe.getSize(i), ftProbe.getIsNull(i));
                }
            }

            // // output tuple (debug)
            // _env->debugPrint(builder, "output row type of result after hash: " + ftResult.getTupleType().desc());
            // _env->debugPrint(builder, "ftResult is: ");
            // for(int i = 0; i < ftResult.numElements(); ++i) {
            //     _env->debugPrint(builder, "column (" + to_string(i) + "): ", ftResult.get(i));
            // }

            // variable!
            _lastRowResult = ftResult;

            _lastBlock = builder.GetInsertBlock();

            // _env->debugPrint(builder, "got result");
        }

        void PipelineBuilder::createLeftJoinBucketLoop(llvm::IRBuilder<> &builder, llvm::Value *num_rows_to_join,
                                                       llvm::Value *bucketPtrVar, bool buildRight,
                                                       python::Type buildBucketType, python::Type resultType,
                                                       int probeKeyIndex, llvm::Value *match_found) {
            using namespace llvm;
            using namespace std;


            // rows
            codegen::FlattenedTuple ftProbe = _lastRowResult; // last row type is always the probe type (i.e. the larger one) => valid because of left join!
            codegen::FlattenedTuple ftBuild(_env.get());
            codegen::FlattenedTuple ftResult(_env.get());
            if(!buildBucketType.parameters().empty())
                ftBuild.init(buildBucketType); //!!! important, b.c. key does not get saved to bucket...
            ftResult.init(resultType);
            // cout<<"output row type is: "<<ftResult.getTupleType().desc()<<endl;

            // start loop over all rows in bucket, need to update pointer var to decode the next one
            //_env->debugPrint(builder, "found num_rows_to_join: ", num_rows_to_join);
            beginForLoop(builder, num_rows_to_join);

            // load bucket only if match found is true, else

            BasicBlock* curBlock = builder.GetInsertBlock();
            BasicBlock* bbBucketResult = BasicBlock::Create(builder.getContext(), "fetch_bucket_result", builder.GetInsertBlock()->getParent());
            BasicBlock* bbResult = BasicBlock::Create(builder.getContext(), "result", builder.GetInsertBlock()->getParent());

            builder.CreateCondBr(match_found, bbBucketResult, bbResult);

            builder.SetInsertPoint(bbBucketResult);
            // there should be at least one row (omit weird loop for now b.c. more difficult...)
            auto bucketPtr = builder.CreateLoad(bucketPtrVar);
            auto row_length = builder.CreateLoad(builder.CreatePointerCast(bucketPtr, _env->i32ptrType()));
            auto row_ptr = builder.CreateGEP(bucketPtr, _env->i32Const(sizeof(int32_t)));
            // update bucketPtr Var with sizeof(int32_t) + data length
            builder.CreateStore(builder.CreateGEP(bucketPtr, builder.CreateAdd(row_length, _env->i32Const(sizeof(int32_t)))), bucketPtrVar);

            // _env->debugPrint(builder, "decoding in-bucket row with length : ", row_length);

            // only deserialize if there are cols in the bucket (special case: one column join)
            if(!buildBucketType.parameters().empty())
                ftBuild.deserializationCode(builder, row_ptr);
            builder.CreateBr(bbResult);

            // replace everything in ftBuild with phi nodes (a bunch of them)
            builder.SetInsertPoint(bbResult);
            for(int i = 0; i < ftBuild.numElements(); ++i) {
                auto val = ftBuild.get(i);
                auto size = ftBuild.getSize(i);
                auto is_null = ftBuild.getIsNull(i);
                auto phiVal = builder.CreatePHI(val->getType(), 2);
                phiVal->addIncoming(_env->nullConstant(val->getType()), curBlock);
                phiVal->addIncoming(val, bbBucketResult);
                auto phiSize = builder.CreatePHI(size->getType(), 2);
                phiSize->addIncoming(_env->nullConstant(size->getType()), curBlock);
                phiSize->addIncoming(size, bbBucketResult);
                auto phiIsNull = builder.CreatePHI(is_null->getType(), 2);
                phiIsNull->addIncoming(_env->i1Const(true), curBlock); // no match, so NULL
                phiIsNull->addIncoming(is_null, bbBucketResult);
                ftBuild.assign(i, phiVal, phiSize, phiIsNull);
            }

            // assignment from before
            if(buildRight) {

                // decode result type
                // => note
                // add all from probe, then build
                // !!! direction !!!
                int pos = 0;
                for (int i = 0; i < ftProbe.numElements(); ++i) {
                    if (i != probeKeyIndex)
                        ftResult.assign(pos++, ftProbe.get(i), ftProbe.getSize(i), ftProbe.getIsNull(i));
                }

                // add key from probe
                ftResult.assign(pos, ftProbe.get(probeKeyIndex), ftProbe.getSize(probeKeyIndex),
                                ftProbe.getIsNull(probeKeyIndex));
                pos++;

                // add build elements (all here b.c. from in-bucket type...)
                for (int i = 0; i < ftBuild.numElements(); ++i) {
                    // upgrade to option
                    auto val = ftBuild.get(i);
                    auto size = ftBuild.getSize(i);
                    auto is_null = ftBuild.getIsNull(i);
                    if(val && !is_null)
                        is_null = _env->i1Const(false);
                    ftResult.assign(pos++, val, size, is_null);
                }
            } else {

               // this has to be a right join
               // => not yet implemented
               throw std::runtime_error("a left join with left build side is a right join. Not yet implemented");
            }

            // // output tuple (debug)
            // _env->debugPrint(builder, "output row type of result after hash: " + ftResult.getTupleType().desc());
            // _env->debugPrint(builder, "ftResult is: ");
            // for(int i = 0; i < ftResult.numElements(); ++i) {
            //     _env->debugPrint(builder, "column (" + to_string(i) + "): ", ftResult.get(i));
            // }

            // variable!
            _lastRowResult = ftResult;
            _lastBlock = builder.GetInsertBlock();

           //  _env->debugPrint(builder, "got result");
        }

        bool PipelineBuilder::addHashJoinProbe(int64_t leftKeyIndex,
                                               const python::Type &leftRowType,
                                               int64_t rightKeyIndex,
                                               const python::Type &rightRowType,
                                               const JoinType &jt,
                                               bool buildRight,
                                               llvm::Value *hash_map,
                                               llvm::Value *null_bucket) {
            using namespace llvm;
            using namespace std;

            assert(hash_map && null_bucket);

            IRBuilder<> builder(_lastBlock);
            auto& context = builder.getContext();

            // _env->debugPrint(builder, "start join of " + leftRowType.desc() + " and " + rightRowType.desc());

            // hashmap & nullbucket should be i8**ptrs
            hash_map = builder.CreateLoad(hash_map);
            null_bucket = builder.CreateLoad(null_bucket);
            assert(hash_map->getType() == _env->i8ptrType());
            assert(null_bucket->getType() == _env->i8ptrType());

            // hash_map/null bucket should be globals...

            // probe key index
            int probeKeyIndex = buildRight ? leftKeyIndex : rightKeyIndex;

            // extract key from lastRow result
            auto key = _lastRowResult.getLoad(builder, vector<int>{probeKeyIndex});
            auto probeKeyType = buildRight ? leftRowType.parameters()[leftKeyIndex]
                                           : rightRowType.parameters()[rightKeyIndex];

            // check what types are supported (as of now, only string!)
            // ==> create string key here else...
            if (probeKeyType.withoutOptions() != python::Type::STRING && probeKeyType != python::Type::NULLVALUE &&
                probeKeyType.withoutOptions() != python::Type::I64)
                throw std::runtime_error("only key types are string/i64/nullvalue");

            // create build-bucket type (i.e. whatever is in the bucket)
            auto buildType = buildRight ? rightRowType : leftRowType; assert(buildType.isTupleType());
            auto buildKeyIndex = buildRight ? rightKeyIndex : leftKeyIndex;
            vector<python::Type> py_bucket_types;
            for(int i = 0; i < buildType.parameters().size(); ++i)
                if(i != buildKeyIndex)
                    py_bucket_types.push_back(buildType.parameters()[i]);
            auto buildBucketType = python::Type::makeTupleType(py_bucket_types);
            auto resultType = combinedJoinType(leftRowType, leftKeyIndex, rightRowType, rightKeyIndex, jt);

            // call make key to transform possibly null result into nullptr...
            SerializableValue hash_map_key;
            if (probeKeyType.withoutOptions() == python::Type::STRING || probeKeyType == python::Type::NULLVALUE) {
                hash_map_key = makeKey(builder, key, false);
                if (hash_map_key.val)
                    assert(hash_map_key.val->getType() == _env->i8ptrType());
            } else {
                hash_map_key = key;
            }

#ifndef NDEBUG
            // if(hash_map_key.val)
            //     _env->debugPrint(builder, "hash_map_key is: ", hash_map_key.val);
#endif
            // _env->debugPrint(builder, "_lastRowResult is: ");
            // for(int i = 0; i < _lastRowResult.numElements(); ++i) {
            //     _env->debugPrint(builder, "column (" + to_string(i) + "): ", _lastRowResult.get(i));
            // }


            auto bucket_value = _env->CreateFirstBlockAlloca(builder, _env->i8ptrType(), "bucket_value");
            builder.CreateStore(_env->i8nullptr(), bucket_value);

            // C++ code:
            // bucket = nullptr
            // if(isnull(key))
            // bucket = null_bucket
            // else if(

            // null bucket check
            if(probeKeyType.isOptionType()) {
                // set to null bucket per default
                builder.CreateStore(null_bucket, bucket_value);

                // add blocks for the check and
                BasicBlock* bbNotNull = BasicBlock::Create(context, "probe_via_map", builder.GetInsertBlock()->getParent());
                BasicBlock* bbNext = BasicBlock::Create(context, "probe_done", builder.GetInsertBlock()->getParent());
                assert(hash_map_key.is_null);
                builder.CreateCondBr(hash_map_key.is_null, bbNext, bbNotNull);
                builder.SetInsertPoint(bbNotNull);
                if (probeKeyType.withoutOptions() == python::Type::STRING || probeKeyType == python::Type::NULLVALUE) {
                    auto found_val = _env->callBytesHashmapGet(builder, hash_map, hash_map_key.val, hash_map_key.size,
                                                               bucket_value); // overwrite bucket_value...
                } else {
                    auto found_val = _env->callIntHashmapGet(builder, hash_map, hash_map_key.val,
                                                             bucket_value); // overwrite bucket_value...
                }
                builder.CreateBr(bbNext);
                builder.SetInsertPoint(bbNext);
            } else if(probeKeyType != python::Type::NULLVALUE) {
                // probe step
                if (probeKeyType.withoutOptions() == python::Type::STRING || probeKeyType == python::Type::NULLVALUE) {
                    // i8* hmap, i8* key, i64 key_size, i8** bucket
                    auto found_val = _env->callBytesHashmapGet(builder, hash_map, hash_map_key.val, hash_map_key.size,
                                                               bucket_value); // overwrite bucket_value...
                } else {
                    // i8* hmap, i64 key, i8** bucket
                    auto found_val = _env->callIntHashmapGet(builder, hash_map, hash_map_key.val,
                                                             bucket_value); // overwrite bucket_value...
                }
            } else {
                // null value
                assert(probeKeyType == python::Type::NULLVALUE);
                // => bucket found is null bucket!
                builder.CreateStore(null_bucket, bucket_value);
            }

            // condition on bucket_value, i.e. if bucket != nullptr, then there's a match!
            auto found_val = builder.CreateICmpNE(builder.CreateLoad(bucket_value), _env->i8nullptr());

#ifndef NDEBUG
            // _env->debugPrint(builder, "match found: ", found_val);
#endif
            // inner join first:
            // ==> quite simple, if there is a match: process all rows & continue pipeline, if not: pipeline is done
            // go to destructor block
            if(jt == JoinType::INNER) {
                BasicBlock *bbMatchFound = BasicBlock::Create(context, "match_found", builder.GetInsertBlock()->getParent());
                builder.CreateCondBr(found_val, bbMatchFound, leaveBlock());
                builder.SetInsertPoint(bbMatchFound);

                // now it gets a bit tricky here, because it's time for a loop
                // bucket is valid, so extract num rows found
                // (cf. TransformTask for in-bucket data structure)
                //uint64_t info = (num_rows << 32ul) | bucket_size;
                auto bucket = builder.CreateLoad(bucket_value);
                auto info = builder.CreateLoad(builder.CreatePointerCast(bucket, _env->i64ptrType()));
                // truncation yields lower 32 bit (= bucket_size)
                auto bucket_size = builder.CreateTrunc(info, _env->i32Type(), "bucket_size");
                // right shift by 32 yields size (= num_rows)
                auto num_rows_to_join = builder.CreateLShr(info, 32, "num_rows_to_join");
                num_rows_to_join = builder.CreateTrunc(num_rows_to_join, _env->i32Type());

                // var for bucket ptr
                auto bucketPtrVar = _env->CreateFirstBlockAlloca(builder, _env->i8ptrType(), "bucket_ptr");
                builder.CreateStore(builder.CreateGEP(bucket, _env->i32Const(sizeof(int64_t))), bucketPtrVar); // offset bucket by 8 bytes / 64 bit

                createInnerJoinBucketLoop(builder, num_rows_to_join, bucketPtrVar, buildRight, buildBucketType,
                                          resultType, probeKeyIndex);

            } else if(jt == JoinType::LEFT) {

                // var for bucket ptr
                auto bucketPtrVar = _env->CreateFirstBlockAlloca(builder, _env->i8ptrType(), "bucket_ptr");

                // it's a left join, so there will be always at least one row as result.
                // matchfound only dictates whether it's going to be with nulls or not
                auto curBlock = builder.GetInsertBlock();
                BasicBlock *bbMatchFound = BasicBlock::Create(context, "bucket_found", builder.GetInsertBlock()->getParent());
                BasicBlock *bbNext = BasicBlock::Create(context, "left_join_next", builder.GetInsertBlock()->getParent());
                builder.CreateCondBr(found_val, bbMatchFound, bbNext);
                builder.SetInsertPoint(bbMatchFound);

                // now it gets a bit tricky here, because it's time for a loop
                // bucket is valid, so extract num rows found
                // (cf. TransformTask for in-bucket data structure)
                //uint64_t info = (num_rows << 32ul) | bucket_size;
                auto bucket = builder.CreateLoad(bucket_value);
                auto info = builder.CreateLoad(builder.CreatePointerCast(bucket, _env->i64ptrType()));
                // truncation yields lower 32 bit (= bucket_size)
                auto bucket_size = builder.CreateTrunc(info, _env->i32Type(), "bucket_size");
                // right shift by 32 yields size (= num_rows)
                auto bucket_num_rows_to_join = builder.CreateLShr(info, 32, "num_rows_to_join");
                bucket_num_rows_to_join = builder.CreateTrunc(bucket_num_rows_to_join, _env->i32Type());

                builder.CreateStore(builder.CreateGEP(bucket, _env->i32Const(sizeof(int64_t))), bucketPtrVar); // offset bucket by 8 bytes / 64 bit

                builder.CreateBr(bbNext);


                builder.SetInsertPoint(bbNext);
                // phi node for num rows (=> 1 in case no match found)
                auto num_rows_to_join = builder.CreatePHI(_env->i32Type(), 2);
                num_rows_to_join->addIncoming(bucket_num_rows_to_join, bbMatchFound);
                num_rows_to_join->addIncoming(_env->i32Const(1), curBlock);

                createLeftJoinBucketLoop(builder, num_rows_to_join, bucketPtrVar, buildRight, buildBucketType,
                                          resultType, probeKeyIndex, found_val);
            } else throw std::runtime_error("join type not yet supported!");

            // important in order to continue with other operations...
            _lastBlock = builder.GetInsertBlock();
            _lastSchemaType = resultType;

            return true;
        }


       bool PipelineBuilder::addAggregate(const int64_t operatorID, const UDF &aggUDF, const python::Type& aggType,
                                          double normalCaseThreshold,
                                          bool allowUndefinedBehavior, bool sharedObjectPropagation) {

            // use intermediate to save to.
            // also, the number of rows should be zero.

           using namespace llvm;
           using namespace std;

           auto& logger = Logger::instance().logger("PipelineBuilder");

           // check type is compatible
           auto aggLLVMType = env().pythonToLLVMType(aggType);
           assert(aggLLVMType->getPointerTo() == intermediateOutputPtr()->getType());

           IRBuilder<> builder(_lastBlock);
           auto& context = builder.getContext();

           // fetch aggregate value
           FlattenedTuple ftAgg = FlattenedTuple::fromLLVMStructVal(_env.get(), builder, intermediateOutputPtr(), aggType);

           // debug code
           auto x0 = builder.CreateStructGEP(intermediateOutputPtr(), 0);
           auto x1 = builder.CreateLoad(x0);

           // // compile aggregation function and add it in.

            // new combined flattened tuple to pass to function
            auto combinedType = python::Type::makeTupleType({aggType, _lastRowResult.getTupleType()}); // this should be compatible to input type of aggUDF!
            FlattenedTuple ftin(_env.get());
            ftin.init(combinedType);
            ftin.set(builder, {0}, ftAgg);
            ftin.set(builder, {1}, _lastRowResult);

            // compile UDF
           if(aggUDF.empty())
               throw std::runtime_error("UDF is empty in aggregate, this should not happen");

           // compile dependent on udf
           auto cf = aggUDF.isCompiled() ? const_cast<UDF&>(aggUDF).compile(env()) :
                     const_cast<UDF&>(aggUDF).compileFallback(env(), _constructorBlock, _destructorBlock);

           // stop if compilation didn't succeed
           if(!cf.good())
               return false;

           // store in what operator called here (needed for exception handler)
           assignToVariable(builder, "exceptionOperatorID", env().i64Const(operatorID));
           // as stated in the map operation, the result type needs to be allocated within the entry block
           IRBuilder<> variableBuilder(_constructorBlock);
           _lastTupleResultVar = variableBuilder.CreateAlloca(cf.getLLVMResultType(env()),
                                                              0, nullptr);
           _lastRowInput = _lastRowResult;

           auto exceptionBlock = createExceptionBlock();
           auto ftout = cf.callWithExceptionHandler(builder,
                                                    ftin,
                                                    _lastTupleResultVar,
                                                    exceptionBlock,
                                                    getPointerToVariable(builder, "exceptionCode"));

           _lastBlock = builder.GetInsertBlock();
           assert(_lastBlock);

           // debug print
           auto v = ftout.get(0);

           // store to global aggregate var result
           ftout.storeTo(builder, intermediateOutputPtr());
           _lastRowResult = FlattenedTuple::fromLLVMStructVal(_env.get(), builder, intermediateOutputPtr(), aggType);

           auto result_type = python::Type::propagateToTupleType(cf.output_type); // progagate b.c. this yields a row.

           if (_lastRowResult.getTupleType() != result_type) {
               logger.error("wrong output type. Expected " + result_type.desc() + " got " +
                            _lastRowResult.getTupleType().desc());
               return false;
           }

           // update what the last schema type is
           _lastInputSchemaType = _lastSchemaType;
           _lastSchemaType = result_type;

           _lastOperatorType = LogicalOperatorType::AGGREGATE;
           _lastOperatorColumnIndex = -1;
           return true;
        }
    }
}