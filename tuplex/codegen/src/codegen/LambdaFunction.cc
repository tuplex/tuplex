//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <codegen/LambdaFunction.h>

namespace tuplex {
    namespace codegen {

        using namespace llvm;

        //! global variable to store meta info in order to retrieve lambdas from environment.
        static std::map<std::string, std::tuple<python::Type, python::Type> > registeredLambdas;

// change this to builder class...
// ==> need later also helper funcs to call a function that throws an exception.
// I.e. need to abstract all of this a little more.
// builder would be nice because then the problem of the retvalue would disappear.


// i.e. have two objects:
// 1. LambdaFunctionBuilder --> makes sure lambda function is correctly constructed
// 2. LambdaFunction as object with private constructor
// ==> has static function, so can be used to create a call to it...

        void LambdaFunctionBuilder::createLLVMFunction(const std::string &name, const python::Type &argType,
                                                       bool isFirstArgTuple, NParameterList *parameters,
                                                       const python::Type &retType) {

            assert(_env);

            // creates internal function object
            _func._env = _env;
            _func._name = name;
            _func._pyArgType = argType;
            _func._pyRetType = retType;

            _fti.init(_func._pyArgType);
            _fto.init(_func._pyRetType);

            // create function + load arguments. Necessary instructions for this are added at the start of the basic body block.
            auto paramType = parameterTypes();

            // check whether constants are in parameterTypes, they need to get treated separately!


            // create LLVM function for one tuple type in, one tuple type out
            auto &ctx = _env->getContext();

#ifndef NDEBUG
            if(!retType.isTupleType())
                _logger.info("optimization potential for return type " + retType.desc() + ", function " + name);
            if(!argType.isTupleType())
                _logger.info("optimization potential for return type " + argType.desc() + ", function " + name);
#endif

            // propagate to tuple type for now. Note: there is an option to optimize for single type functions!
            assert(_env->getOrCreateTupleType(_fto.flattenedTupleType()) == _fto.getLLVMType());
            assert(_env->getOrCreateTupleType(_fti.flattenedTupleType()) == _fti.getLLVMType());
            auto outRowLLVMType = _fto.getLLVMType()->getPointerTo();
            auto inRowLLVMType = _fti.getLLVMType()->getPointerTo();

            FunctionType *FT = FunctionType::get(_env->i64Type(), {outRowLLVMType,
                                                                   inRowLLVMType}, false);

            auto linkage = Function::InternalLinkage;
            auto func = Function::Create(FT, linkage, name, _env->getModule().get());

            // rename arguments & set attributes
            // add attributes to the arguments (sret, byval)
            for (int i = 0; i < func->arg_size(); ++i) {
                auto& arg = *(func->arg_begin() + i);

                // set attribute
                if(0 == i) {
                    arg.setName("outRow");
                    // maybe align by 8?

                    _retValPtr = &arg; // set retval ptr!
                }

                if(1 == i) {
                    arg.setName("inRow");
                    //arg.addAttr(Attribute::ByVal);
                    // maybe align by 8?
                }
            }

            // disable all function attributes...
             // add norecurse to function & inline hint
             func->addFnAttr(Attribute::NoRecurse);
             func->addFnAttr(Attribute::InlineHint);

            // is this safe??? --> probably not, leave for now... Optimization only makes sense if no external func call was used... @TODO: opt potential here.
            //func->addFnAttr(Attribute::NoUnwind); // explicitly disable unwind! (no external lib calls!)

            // store in internal data structure
            _func._func = func;

            // create first basic block within function & add statements to load tuple elements correctly
            // and store them via a lookup map
            _body = BasicBlock::Create(_context, "body", _func._func);
            IRBuilder<> builder(_body);

            unflattenParameters(builder, parameters, isFirstArgTuple);
        }

        LambdaFunctionBuilder &LambdaFunctionBuilder::create(NLambda *lambda,
                                                             std::string func_name) {
            assert(_env);
            assert(lambda);

            auto argType = lambda->getInferredType().getParamsType();
            auto retType = lambda->getInferredType().getReturnType();
            _logger.info("generating lambda function for " + argType.desc() + " -> " + retType.desc());

            createLLVMFunction(func_name, argType, lambda->isFirstArgTuple(), lambda->_arguments.get(), retType);

            return *this;
        }

        LambdaFunctionBuilder& LambdaFunctionBuilder::create(NFunction *func) {
            assert(_env);
            assert(func);

            auto func_name = func->_name->_name;

            auto argType = python::Type::makeTupleType(func->getInferredType().parameters());
            auto retType = func->getInferredType().getReturnType();
            _logger.info("generating function " + func_name + " for " + argType.desc() + " -> " + retType.desc());

            createLLVMFunction(func_name, argType, func->isFirstArgTuple(), func->_parameters.get(), retType);
            return *this;
        }


        void LambdaFunctionBuilder::unflattenParameters(llvm::IRBuilder<> &builder, NParameterList *params,
                                                        bool isFirstArgTuple) {
            assert(_func._pyArgType != python::Type::UNKNOWN);
            assert(_func._func);
            assert(params);

            // clear old lookup
            _paramLookup.clear();
            auto pyArgType = _func._pyArgType;

            std::vector<llvm::Argument *> args;
            for(auto& arg : _func._func->args())
                args.push_back(&arg);

            if (pyArgType.parameters().size() == 1) {
                if (isFirstArgTuple && pyArgType.parameters().front() != python::Type::EMPTYTUPLE) {
                    // create ftarg from llvm struct val (i.e. the pointer)
                    assert(args.back()->getName() == "inRow");
                    auto ftarg = FlattenedTuple::fromLLVMStructVal(_env, builder, args.back(), pyArgType);
                    auto argname = static_cast<NParameter *>(params->_args[0].get())->_identifier->_name;
                    _paramLookup[argname] = SerializableValue(args.back(), nullptr, nullptr); // use the pointer as tuple var!

                } else {
                    // there is one arg. Because for now, a single arg is always a pointer to a tuple type, load it
                    auto ftarg = FlattenedTuple::fromLLVMStructVal(_env, builder, args.back(), pyArgType);
                    assert(ftarg.numElements() == 1);

                    // simple name lookup
                    auto argname = static_cast<NParameter *>(params->_args[0].get())->_identifier->_name;
                    _paramLookup[argname] = SerializableValue(ftarg.get(0), ftarg.getSize(0), ftarg.getIsNull(0));
                }
            } else {
                assert(args.back()->getName() == "inRow");
                auto ftarg = FlattenedTuple::fromLLVMStructVal(_env, builder, args.back(), pyArgType);

                // simple name lookup in this case too
                auto pyargs = pyArgType.parameters();

                // create loads for subtrees...
                for (int i = 0; i < pyargs.size(); ++i) {
                    auto argname = static_cast<NParameter *>(params->_args[i].get())->_identifier->_name;
                    _paramLookup[argname] = ftarg.getLoad(builder, {i});
                }
            }
        }

        LambdaFunction LambdaFunctionBuilder::exitWithException(const ExceptionCode &ec) {
            auto builder = getLLVMBuilder();
            auto ecCode = _env->i64Const(ecToI64(ec));
            builder.CreateRet(ecCode);
            _body = nullptr;
            return _func;
        }

        LambdaFunction LambdaFunctionBuilder::addReturn(const SerializableValue &retValue) {
            assert(_retValPtr);

            auto res = retValue.val;
            auto builder = getLLVMBuilder();
            auto output_type = _fto.getTupleType();

            // @TODO: optimize & test/resolve for tuples! it's not a struct type but rather a pointer to a struct type!
            assert(python::Type::UNKNOWN != output_type);

            // there are a couple special cases to handle here
            // 1. retValue might be representing null or one of the constants
            if(!res && (output_type == python::Type::NULLVALUE ||
                       output_type == python::Type::EMPTYLIST ||
                       output_type == python::Type::EMPTYDICT ||
                       output_type == python::Type::EMPTYLIST)) {

                auto s = _env->getLLVMTypeName(_retValPtr->getType());
                // there is nothing to store here, struct type is type {} anyways...
                int64_t ec = (int64_t) ExceptionCode::SUCCESS;
                builder.CreateRet(_env->i64Const(ec));

                _body = nullptr;

                // save info on this function to static global here (to retrieve later)
                registeredLambdas[_func._name] = std::make_tuple(_func._pyArgType, _func._pyRetType);
                return _func;
            }

            // retValue might be also a pointer to a tuple type
            if(res && res->getType()->isPointerTy() && res->getType()->getPointerElementType()->isStructTy()) {
                _fto = FlattenedTuple::fromLLVMStructVal(_env, builder, res, output_type);
                res = _fto.getLoad(builder);
            }

            // in the case of a single expression, there may be not a tuple on the stack.
            // for this case, take the result and widen it to a tuple for returning
            // consider for this a function lambda x: x + 2 as simple example
            else if(!_func._pyRetType.isTupleType()) {
                assert(_fto.numElements() == 1);
                // assign to fto tuple, create load & update res
                // can be nullptr for option types...
                //assert(retValue.val);
                //assert(retValue.size);
                _fto.assign(0, retValue.val, retValue.size, retValue.is_null);
                res = _fto.getLoad(builder);
            }

            // the same applies for emptytuple (special case)
            if (res->getType() == _env->getEmptyTupleType()) {
                _fto.assign(0, retValue.val, retValue.size, retValue.is_null);
                res = _fto.getLoad(builder);
            }

            assert(res->getType()->isStructTy()); // needs to be tuple type!
            static_assert((int64_t) ExceptionCode::SUCCESS == 0, "define this as 0");

            // store res
            builder.CreateStore(res, _retValPtr);
            int64_t ec = (int64_t) ExceptionCode::SUCCESS;
            builder.CreateRet(_env->i64Const(ec));
            _body = nullptr;

            // save info on this function to static global here (to retrieve later)
            registeredLambdas[_func._name] = std::make_tuple(_func._pyArgType, _func._pyRetType);

            return _func;
        }

        std::vector<llvm::Type *> LambdaFunctionBuilder::parameterTypes() {
            auto parameter_types = _fti.getTypes();
            int numInputElements = parameter_types.size();
            for (int i = 0; i < numInputElements; ++i)
                parameter_types.push_back(Type::getInt64Ty(_context));

            return parameter_types;
        }


        llvm::IRBuilder<> LambdaFunctionBuilder::addException(llvm::IRBuilder<> &builder, llvm::Value *ecCode,
                                                              llvm::Value *condition) {

            // convert ecCode to i32 if possible
            if(ecCode->getType() != _env->i32Type()) {
                // debug check:
                // constant int?
                if(llvm::isa<ConstantInt>(ecCode)) {
                    auto ec_value = llvm::cast<ConstantInt>(ecCode)->getSExtValue();
                    // make sure it's in i32 range
                    assert(ec_value < std::numeric_limits<int32_t>::max() && std::numeric_limits<int32_t>::min() < ec_value);
                    ecCode = builder.CreateZExtOrTrunc(ecCode, _env->i32Type());
                } else {
                    _logger.debug("warn: ecCode is truncated");
                    ecCode = builder.CreateZExtOrTrunc(ecCode, _env->i32Type());
                }
            }

            assert(ecCode->getType() == _env->i32Type());


            // if no condition is given, use trivial one (i.e. compare against SUCCESS)
            if(!condition)
                condition = builder.CreateICmpNE(ecCode, _env->i32Const(ecToI32(ExceptionCode::SUCCESS)));

            // based on condition returns exception code or not
            // alters builder with new block!
            BasicBlock *normalBB = BasicBlock::Create(_env->getContext(), "next", builder.GetInsertBlock()->getParent());
            auto bb_name = normalBB->getName().str();
            std::cout<<"created block "<<bb_name<<std::endl;
            if(bb_name == "next68")
                std::cout<<"critical block"<<std::endl;
            BasicBlock *exceptionBB = BasicBlock::Create(_env->getContext(), "except", builder.GetInsertBlock()->getParent());

            builder.CreateCondBr(condition, exceptionBB, normalBB);
            builder.SetInsertPoint(exceptionBB);
            builder.CreateRet(builder.CreateZExt(ecCode, _env->i64Type()));

            builder.SetInsertPoint(normalBB);
            setLastBlock(normalBB);
            return builder;
        }

        llvm::IRBuilder<> LambdaFunctionBuilder::addException(llvm::IRBuilder<> &builder, ExceptionCode ec,
                                                              llvm::Value *condition) {
            return addException(builder, _env->i32Const(ecToI32(ec)), condition);
        }


        LambdaFunction LambdaFunction::getFromEnvironment(LLVMEnvironment &env, const std::string &funcName) {
            // check whether function can be retrieved from env and is also stored within the global map here
            auto func = env.getModule()->getFunction(funcName);
            if(!func)
                EXCEPTION("could not retrieve function " + funcName + " from LLVM module");

            auto types = registeredLambdas[funcName];
            auto argType = std::get<0>(types);
            auto retType = std::get<1>(types);

            LambdaFunction lf;
            lf._name = funcName;
            lf._func = func;
            lf._env = &env;
            lf._pyRetType = retType;
            lf._pyArgType = argType;
            return lf;
        }

        void LambdaFunction::callWithExceptionHandler(llvm::IRBuilder<> &builder, llvm::Value* const resVal, llvm::BasicBlock* const handler,
                                                              llvm::Value* const exceptionCode,
                                                              const std::vector<llvm::Value *>&  args) {

            // calls the function and stores retCode in exception Code
            assert(_env->i64Type()->getPointerTo(0) == exceptionCode->getType());

            llvm::Function* callingFunction = builder.GetInsertBlock()->getParent();
            assert(callingFunction);

            assert(resVal);
            assert(resVal->getType() == getResultType()->getPointerTo(0));


            std::vector<llvm::Value*> allargs{resVal};
            allargs.insert(allargs.end(), args.begin(), args.end());
            assert(_func);
            auto retCode = builder.CreateCall(_func, allargs);

            // store exception Code
            builder.CreateStore(retCode, exceptionCode);

            // create condition on whether not equal to success code
            int64_t success = (int64_t)ExceptionCode::SUCCESS;
            auto cond = builder.CreateICmpEQ(retCode, _env->i64Const(success));

            // create basic blocks for branching
            BasicBlock *bSuccess = BasicBlock::Create(_env->getContext(), "normal", callingFunction);
            builder.CreateCondBr(cond, bSuccess, handler);
            builder.SetInsertPoint(bSuccess);
        }

        llvm::Type* LambdaFunction::getResultType() {
            FlattenedTuple ft(_env);
            ft.init(_pyRetType);
            return ft.getLLVMType();
        }
    }
}