//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/codegen/CellSourceTaskBuilder.h>
#include <codegen/CodegenHelper.h>
#include <llvm/Transforms/Utils/BuildLibCalls.h>

namespace tuplex {
    namespace codegen {
        llvm::Function * CellSourceTaskBuilder::build(bool terminateEarlyOnLimitCode) {
            using namespace llvm;

            auto& logger = Logger::instance().logger("codegen");

            FunctionType* FT = FunctionType::get(env().i64Type(), {env().i8ptrType(),
                                                                   env().i64Type(),
                                                                   env().i8ptrType()->getPointerTo(),
                                                                   env().i64ptrType()}, false);

#if LLVM_VERSION_MAJOR < 9
            // compatibility
            Function* func = cast<llvm::Function>(env().getModule()->getOrInsertFunction(_functionName, FT));
#else
            Function* func = cast<llvm::Function>(env().getModule()->getOrInsertFunction(_functionName, FT).getCallee());
#endif

            // fetch args
            auto args = mapLLVMFunctionArgs(func, {"userData", "rowNumber", "cells", "cell_sizes"});
            llvm::Value* userData = args["userData"];
            llvm::Value* cellsPtr = args["cells"];
            llvm::Value* sizesPtr = args["cell_sizes"];

            BasicBlock* bbEntry = BasicBlock::Create(env().getContext(), "entry", func);

            IRBuilder<> builder(bbEntry);

            // where to store how many output rows are produced from this call.
            Value *outputRowNumberVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "outputRowNumberVar");
            builder.CreateStore(args["rowNumber"], outputRowNumberVar);

            // perform any checks on cells upfront!
            // --> i.e. after normal-case checks are performed, can parse as normal-case row!
            auto outputRowNumber = builder.CreateLoad(outputRowNumberVar);
            generateChecks(builder, userData, outputRowNumber, cellsPtr, sizesPtr);

            // get FlattenedTuple from deserializing all things + perform value conversions/type checks...
            auto ft = parseNormalCaseRow(builder, cellsPtr, sizesPtr);

            // if pipeline is set, call it!
            if(pipeline()) {
                auto pipFunc = pipeline()->getFunction();
                if(!pipFunc)
                    throw std::runtime_error("error in pipeline function");

                // debug:
                // env().debugPrint(builder, "parsed following tuple from CSV: ");
                // ft.print(builder);

                auto res = PipelineBuilder::call(builder, pipFunc, ft, userData, builder.CreateLoad(outputRowNumberVar), initIntermediate(builder));
                auto ecCode = builder.CreateZExtOrTrunc(res.resultCode, env().i64Type());
                auto ecOpID = builder.CreateZExtOrTrunc(res.exceptionOperatorID, env().i64Type());
                auto numRowsCreated = builder.CreateZExtOrTrunc(res.numProducedRows, env().i64Type());

                if(terminateEarlyOnLimitCode)
                    generateTerminateEarlyOnCode(builder, ecCode, ExceptionCode::OUTPUT_LIMIT_REACHED);

                // // -- debug print row numbers
                // env().debugPrint(builder, "numRowsCreatedByPipeline", numRowsCreated);
                // env().debugPrint(builder, "outputRowNumber", builder.CreateLoad(outputRowNumberVar));
                // // -- end debug print

                // create exception logic for row
                // only build this part if exception handler is set...
                if(hasExceptionHandler()) {
                    // ==> Note: this is handled in pipeline typically, no need to call here...
                    auto exceptionRaised = builder.CreateICmpNE(ecCode,
                                                                env().i64Const(ecToI32(ExceptionCode::SUCCESS)));

                    llvm::BasicBlock *bbNoException = llvm::BasicBlock::Create(env().getContext(),
                                                                               "pipeline_ok",
                                                                               builder.GetInsertBlock()->getParent());

                    // how should exceptions get serialized?
                    // --> here, all normal checks have passed. If we want to force processing on the slow path to decode
                    // a normal case typed row for that particular case, then should emit normal-case violation as code
                    // and DO NOT upcast.

                    // if exception is to be processed in the given general-case format, any code except normalcaseviolation will do.
                    // for simplicity,

                    // there's a design flaw here. Basically need to store type of exception AND format of exception row.
                    // -> could use lower/upper 32bit again for this...
                    // e.g., if there's a resolver present for a certain type THEN use the general-case format?? --> need to be a bit more flexible wrt.

                    // TODO: need to clarify row formats that are passed down. make note of format for hyperspecialization.

                    // --> when using ALWAYS emit general-case exceptions, then the singleprcoess wrapper function has to be differently designed.
                    // --> i.e., the serialization logic is either pushed onto the fast path or the slow path.
                    // --> this is basically what this means...
                    // => when using the emit general-case exceptions, then any BADPARSE_STRING_INPUT is ALWAYS an interpreter exception.
                    // i.e., could have an option for hyperspecializastion to force any normalcase vuokation exception to become a badparse string input exception,
                    // that is then forved onto the slowpath decode.

                    // TODO: add debugPrints on row formats...

                    // the formats seem to be the cleanest...
                    // -> introduce that!
                    // @TODO.

                    // --> when using hyperspecialiation, should always use that option.
                    logger.debug("UDF exceptions are emitted in general case format " + _inputRowTypeGeneralCase.desc());

                    // need to option to have SEPARATE general-case format established (?) for exeception handling.
                    // logger.warn("make the boolean here " + std::string(__FILE__) + ":" + std::to_string(__LINE__) + " an option:");
                    bool serialize_exception_in_tuplex_format = exception_serialization_format() == ExceptionSerializationFormat::GENERALCASE
                                                                || exception_serialization_format() == ExceptionSerializationFormat::NORMALCASE; // <-- make this an option!
                    if(!isNormalCaseAndGeneralCaseCompatible()) // check: when cases are not compatible, always false...
                        serialize_exception_in_tuplex_format = false;

                    // always emit general-case exceptions
                    // if normal <-> general are incompatible, serialize as NormalCaseViolationError that requires interpreter!
                    if(serialize_exception_in_tuplex_format) {
                        // _env->debugPrint(builder, "serializing exception row in tuplex format.");
                        // add here exception block for pipeline errors, serialize tuple etc...


                        // force exception code to be generalcaseviolation so everything is being decoded properly>
                        // -> true exception code will be again produced by general-case (or interpreter)
                        logger.warn("serializing exceptions in general-case format leads to exception root "
                                    "information being lost here, b.c. exception code is reset to "
                                    "general-case violation. Hence, resolve code or fallback code must "
                                    "restore correcy exception code for display.");
                        ecCode = _env->i64Const(ecToI64(ExceptionCode::GENERALCASEVIOLATION)); // <-- hack

                        // _env->debugPrint(builder, "exception rows serialized to buffer.");
                        // debug print
                        logger.debug("CellSourceTaskBuilder: input row type in which exceptions from pipeline are stored that are **not** parse-exceptions is " + ft.getTupleType().desc());
                        std::unordered_map<ExceptionSerializationFormat, std::string> name_lookup{{ExceptionSerializationFormat::GENERALCASE, "general"},
                                                                                                     {ExceptionSerializationFormat::NORMALCASE, "normal"}};
                        logger.debug("serializing exceptions in " + name_lookup.at(exception_serialization_format()) + " exception row format");
                        logger.debug("I.e., when creating resolve tasks for this pipeline - set exceptionRowType to this type.");

                        llvm::BasicBlock *curBlock = builder.GetInsertBlock();

                        // new, use lazy func!
                        auto bbException = exceptionBlock(builder, userData,
                                                          ecCode, ecOpID,
                                                          [this, ft, outputRowNumberVar](llvm::IRBuilder<>& builder) {
                           ExceptionDetails except_details;

                            // -> move this here into exception block! rtmalloc makes optimization else impossible...
                            auto serialized_row = serializedExceptionRow(builder, ft, exception_serialization_format());
                            except_details.badDataPtr = serialized_row.val;
                            except_details.badDataLength = serialized_row.size;
                            except_details.fmt = exception_serialization_format();
                            except_details.rowNumber = builder.CreateLoad(outputRowNumberVar);
                           return except_details;
                        });

                        builder.CreateBr(bbNoException);
                        // add branching to previous block
                        builder.SetInsertPoint(curBlock);
                        builder.CreateCondBr(exceptionRaised, bbException, bbNoException);
                    } else {
                        // the exceptions are incompatible, therefore forcing a badparse string input exception (need to decode exceptions therefore)
                        // -> this doesn't allow for proper resolved etc. logic. wrt to counting exceptions (i.e. requires slow path/interpreter path to exist!)
                        logger.warn("this requires slowpath/interpreter path to exist");


                        auto nc_ecCode = _env->i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT));
                        auto nc_ecOpID = _env->i64Const(_operatorID);

                        // important to get curBlock here.
                        llvm::BasicBlock *curBlock = builder.GetInsertBlock();


                        // new: use lazy func!
                        // -> move except serialization into except block (rtmalloc difficult to optimize)

                        auto bbException = exceptionBlock(builder, userData,
                                                          nc_ecCode, nc_ecOpID,
                                                          [this, nc_ecCode, &cellsPtr, &sizesPtr, outputRowNumberVar](llvm::IRBuilder<>& builder) {
                            // serialize as bad parse -> NOTE: the normal-case checks have passed. Hence, use dummies
                            // _env->printValue(builder, llvm::cast<llvm::Value>(nc_ecCode), "cell source parse failed with code, serializing true data: ");
                            auto serialized_row = serializeBadParseException(builder, cellsPtr, sizesPtr, true, true);
                            ExceptionDetails except_details;

                            except_details.badDataPtr = serialized_row.val;
                            except_details.badDataLength = serialized_row.size;
                            except_details.fmt = exception_serialization_format();
                            except_details.rowNumber = builder.CreateLoad(outputRowNumberVar);
                            return except_details;
                        });
                        builder.CreateBr(bbNoException);
                        // add branching to previous block
                        builder.SetInsertPoint(curBlock);
                        builder.CreateCondBr(exceptionRaised, bbException, bbNoException);
                    }

                    builder.SetInsertPoint(bbNoException); // continue inserts & Co

                    // if intermediate callback desired, perform!
                    if(_intermediateType != python::Type::UNKNOWN && !_intermediateCallbackName.empty()) {
                        writeIntermediate(builder, userData, _intermediateCallbackName);
                    }

                    exitWith(builder, ExceptionCode::SUCCESS);
                    // builder.CreateRet(env().i64Const(ecToI64(ExceptionCode::SUCCESS)));
                } else {

                    // if intermediate callback desired, perform!
                    if(_intermediateType != python::Type::UNKNOWN && !_intermediateCallbackName.empty()) {
                        writeIntermediate(builder, userData, _intermediateCallbackName);
                    }

                    exitWith(builder, ecCode);
                    //// propagate result to callee, because can be used to update counters
                    //builder.CreateRet(builder.CreateZExtOrTrunc(ecCode, env().i64Type()));
                }
            } else {
                // if intermediate callback desired, perform!
                if(_intermediateType != python::Type::UNKNOWN && !_intermediateCallbackName.empty()) {
                    writeIntermediate(builder, userData, _intermediateCallbackName);
                }


                // create success ret
                exitWith(builder, ExceptionCode::SUCCESS);
                //builder.CreateRet(env().i64Const(ecToI64(ExceptionCode::SUCCESS)));
            }
            return func;
        }

        llvm::Value* null_check(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const std::vector<std::string>& null_values,
                                llvm::Value* cell_str, llvm::Value* cell_size) {
            if(null_values.empty())
                return env.i1Const(false);

            // TODO: could do short cut compare with sizes...
            return env.compareToNullValues(builder, cell_str, null_values, true);
        }

        SerializableValue parse_string_cell(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::BasicBlock* bParseError,
                                            const python::Type& cell_type, const std::vector<std::string>& null_values,
                                            llvm::Value* cell_str, llvm::Value* cell_size) {
            using namespace llvm;

            assert(bParseError);
            assert(cell_str && cell_str->getType() == env.i8ptrType());
            assert(cell_size && cell_size->getType() == env.i64Type());

            Value* is_null = env.i1Const(false);
            Value *value = nullptr;
            Value *size = nullptr;
            Value* is_null_var = nullptr;
            Value* value_var = nullptr;
            Value* size_var = nullptr;
            SerializableValue ret;
            BasicBlock* bParse = nullptr;
            BasicBlock* bParseDone = nullptr;
            python::Type primitive_type = cell_type.withoutOption();
            if(cell_type.isOptionType()) {
                bParse = BasicBlock::Create(env.getContext(), "parse_cell", builder.GetInsertBlock()->getParent());
                bParseDone = BasicBlock::Create(env.getContext(), "parse_cell_done", builder.GetInsertBlock()->getParent());

                is_null_var = env.CreateFirstBlockAlloca(builder, env.i1Type());
                value_var = env.CreateFirstBlockAlloca(builder, env.pythonToLLVMType(primitive_type));
                size_var = env.CreateFirstBlockAlloca(builder, env.i64Type());

                // perform null-check
                is_null = null_check(env, builder, null_values, cell_str, cell_size);

                // env.printValue(builder, is_null, "null check=");

                builder.CreateStore(is_null, is_null_var);
                builder.CreateCondBr(is_null, bParseDone, bParse);
                builder.SetInsertPoint(bParse);
            }

            // parse (incl. check)
            if(python::Type::STRING == primitive_type) {
                // fill in
                ret = SerializableValue(cell_str, cell_size, is_null);
            } else if(python::Type::BOOLEAN == primitive_type) {
                // conversion code here
                auto val = parseBoolean(env, builder, bParseError, cell_str, cell_size, is_null);
                ret = SerializableValue(val.val, val.size, is_null);
            } else if(python::Type::I64 == primitive_type) {
                // conversion code here
                auto val = parseI64(env, builder, bParseError, cell_str, cell_size, is_null);
                ret = SerializableValue(val.val, val.size, is_null);
            } else if(python::Type::F64 == primitive_type) {
                // conversion code here
                auto val = parseF64(env, builder, bParseError, cell_str, cell_size, is_null);
                ret = SerializableValue(val.val, val.size, is_null);
            } else if(python::Type::NULLVALUE == primitive_type) {
                is_null = null_check(env, builder, null_values, cell_str, cell_size);
            } else {
                throw std::runtime_error("unsupported type " + primitive_type.desc() + " encountered in parse_cell.");
            }

            if(cell_type.isOptionType()) {
                // store in vars
                if(ret.val)
                    builder.CreateStore(ret.val, value_var);
                if(ret.size)
                    builder.CreateStore(ret.size, size_var);

                builder.CreateBr(bParseDone);
                builder.SetInsertPoint(bParseDone);
                is_null = builder.CreateLoad(is_null_var);
                if(ret.val)
                    value = builder.CreateLoad(value_var);
                if(ret.size)
                    size = builder.CreateLoad(size_var);
                ret = SerializableValue(value, size, is_null);
            }

            return ret;
        }

        SerializableValue CellSourceTaskBuilder::cachedParse(llvm::IRBuilder<>& builder,
                                                             const python::Type& type, size_t colNo,
                                                             llvm::Value* cellsPtr,
                                                             llvm::Value* sizesPtr) {
            using namespace llvm;

            auto key = std::make_tuple(colNo, type);

            logger().debug("cachedParse col=" + std::to_string(colNo) + " (" + type.desc() + ")");

            //@TODO: there's an error here, i.e. need to force parsing for both general/normal first on path
            // to avoid domination errors in codegen.
            bool no_cache = true; // HACK to disable cachedParse.

#ifndef NDEBUG
            {
                // // perform parse
                // auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(colNo)), "x" + std::to_string(colNo));
                // auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env().i64Const(colNo)), "s" + std::to_string(colNo));
                // env().printValue(builder, cellStr, "cell no=" + std::to_string(colNo) + ":  ");
            };
#endif

            auto it = _parseCache.find(key);
            if(no_cache || it == _parseCache.end()) {
                // perform parse
                 auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(colNo)), "x" + std::to_string(colNo));
                 auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env().i64Const(colNo)), "s" + std::to_string(colNo));

                SerializableValue ret;

                llvm::Value* isnull = nullptr;
                python::Type t = type;
                    // option type? do NULL value interpretation
                    if(t.isOptionType()) {
                        auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(colNo)), "x" + std::to_string(colNo));
                        isnull = nullCheck(builder, val);
                    } else if(t != python::Type::NULLVALUE) {
                        // null check, i.e. raise NULL value exception!
                        auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(colNo)), "x" + std::to_string(colNo));
                        auto null_check = nullCheck(builder, val);

                        // if positive, exception!
                        // else continue!
                        BasicBlock* bbNullCheckPassed = BasicBlock::Create(builder.getContext(), "col" + std::to_string(colNo) + "_null_check_passed", builder.GetInsertBlock()->getParent());
                        builder.CreateCondBr(null_check, nullErrorBlock(builder), bbNullCheckPassed);
                        builder.SetInsertPoint(bbNullCheckPassed);
                    }

                    t = simplifyConstantType(t);
                    t = t.withoutOption();

                    // constant?
                    if(t.isConstantValued()) {
                        ret = codegen::constantValuedTypeToLLVM(builder, t);
                    }

                    // values?
                    else if(python::Type::STRING == t) {
                        // fill in
                        auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(colNo)), "x" + std::to_string(colNo));
                        auto size = builder.CreateLoad(builder.CreateGEP(sizesPtr, env().i64Const(colNo)), "s" + std::to_string(colNo));
                        ret = SerializableValue(val, size, isnull);
                    } else if(python::Type::BOOLEAN == t) {
                        // conversion code here
                        auto val = parseBoolean(*_env, builder, valueErrorBlock(builder), cellStr, cellSize, isnull);
                        ret = SerializableValue(val.val, val.size, isnull);
                    } else if(python::Type::I64 == t) {
                        // conversion code here
                        auto val = parseI64(*_env, builder, valueErrorBlock(builder), cellStr, cellSize, isnull);
                        ret = SerializableValue(val.val, val.size, isnull);
                    } else if(python::Type::F64 == t) {
                        // conversion code here
                        auto val = parseF64(*_env, builder, valueErrorBlock(builder), cellStr, cellSize, isnull);
                        ret = SerializableValue(val.val, val.size, isnull);
                    } else if(python::Type::NULLVALUE == t) {
                        // perform null check only, & set null element depending on result
                        auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(colNo)), "x" + std::to_string(colNo));
                        isnull = nullCheck(builder, val);

                        // if not null, exception! ==> i.e. ValueError!
                        BasicBlock* bbNullCheckPassed = BasicBlock::Create(builder.getContext(), "col" + std::to_string(colNo) + "_value_check_passed", builder.GetInsertBlock()->getParent());
                        builder.CreateCondBr(isnull, bbNullCheckPassed, valueErrorBlock(builder));
                        builder.SetInsertPoint(bbNullCheckPassed);
                        ret = SerializableValue(nullptr, nullptr, env().i1Const(true)); // set NULL (should be ignored)
                    } else {
                        throw std::runtime_error("unsupported type " + t.desc() + " in CSV Parser gen encountered (CellSourceTaskBuilder)");
                    }

                    // cache & return
                    _parseCache[key] = ret;
                    return ret;
            } else {
                // return entry
                return it->second;
            }
        }

        void CellSourceTaskBuilder::generateChecks(llvm::IRBuilder<>& builder, llvm::Value* userData,
                                                   llvm::Value* rowNumber, llvm::Value* cellsPtr,
                                                   llvm::Value* sizesPtr) {
            using namespace llvm;

            auto& logger = Logger::instance().logger("codegen");

            if(_checks.empty())
                return;

            logger.debug("CellSourceTaskBuilder with " + pluralize(_checks.size(), "check"));

            // sanity check, emit warning if check was given but col not read?
            for(const auto& check : _checks) {
                if(check.colNo >= _generalCaseColumnsToSerialize.size())
                    logger.warn("check has invalid column number");
                else {
                    if(!_generalCaseColumnsToSerialize[check.colNo])
                        logger.warn("CellSourceTaskBuilder received check for col=" + std::to_string(check.colNo) + ", but column is eliminated in pushdown!");
                }
                std::stringstream ss;
                if(check.type == CheckType::CHECK_CONSTANT) {
                    ss<<"detected possible constant check == "<<check.constant_type().constant()<<" for column "<<check.colNo<<"\n";
                } else {
                    ss<<"emitting unknown check for column "<<check.colNo<<"\n";
                }
                logger.info(ss.str());
            }

            // Interesting questions re. checks: => these checks should be performed first. What is the optimal order of checks to perform?
            // what to test first for?

            llvm::Value* allChecksPassed = _env->i1Const(true);
            // _env->debugPrint(builder, "Checking normalcase for rowno=", rowNumber);

            // Also, need to have some optimization re parsing. Parsing is quite expensive, so only parse if required!
            for(int i = 0; i < _generalCaseColumnsToSerialize.size(); ++i) {
                // should column be serialized? if so emit type logic!
                if(_generalCaseColumnsToSerialize[i]) {
                    // find all checks for that column
                    for(const auto& check : _checks) {
                        if(check.colNo == i) {
                            // string type? direct compare
                            llvm::Value* check_cond = nullptr;

                            // emit code for check
                            auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)), "x" + std::to_string(i));
                            auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env().i64Const(i)), "s" + std::to_string(i));

                            // what type of check is it?
                            // only support constant check yet
                            if(check.type == CheckType::CHECK_CONSTANT) {
                                {
                                    std::stringstream ss;
                                    ss<<"general-case requires column, emit code for constant check == "
                                      <<check.constant_type().constant()
                                      <<" for column "<<check.colNo;
                                    logger.info(ss.str());
                                }

                                auto const_type = check.constant_type();
                                // performing check against string constant
                                assert(const_type.isConstantValued());
                                auto elementType = const_type.underlying();

                                //  auto t = rowType.parameters()[rowTypePos];?
//                                assert(elementType == )

                                auto value = const_type.constant();
                                if(elementType.isOptionType()) {
                                    // is the constant null? None?
                                    if(value == "None" || value == "null")  {
                                        elementType = python::Type::NULLVALUE;
                                    } else
                                        elementType = elementType.elementType();
                                }

                                if(python::Type::STRING == elementType) {
                                    // direct compare
                                    auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);
				                    // _env->debugPrint(builder, "Checking whether cellsize==", _env->i64Const(const_type.constant().size() + 1));
                                    auto constant_value = const_type.constant(); // <-- constant has the actual value here!
                                    check_cond = builder.CreateICmpEQ(val.size, _env->i64Const(constant_value.size() + 1));
                                    check_cond = builder.CreateAnd(check_cond, _env->fixedSizeStringCompare(builder, val.val, constant_value));
                                } else if(python::Type::NULLVALUE == elementType) {
                                    // special case: perform null check against array!
                                    // null check (??)
                                    auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);
                                    check_cond =  val.is_null;
                                } else if(python::Type::BOOLEAN == elementType) {
                                    auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);

                                    // compare value
                                    auto c_val = parseBoolString(const_type.constant());
                                    check_cond = builder.CreateICmpEQ(_env->boolConst(c_val), val.val);
                                } else if(python::Type::I64 == elementType) {

                                    // this check can be performed faster (if no leading 0s are assumed).
                                    // i.e. use https://news.ycombinator.com/item?id=21019007 intrinsic.
                                    // can also use that intrinsic for string comparison!

                                    auto const_string = value;

                                    auto sizePtr = builder.CreateGEP(sizesPtr, env().i64Const(i));
                                    auto str_size = builder.CreateLoad(sizePtr);
                                    // size correct?
                                    auto size_ok = builder.CreateICmpEQ(str_size, env().i64Const(value.size() + 1));
                                    auto ptr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)));
                                    const auto& DL = env().getModule()->getDataLayout();
                                    //const TargetLibraryInfo *TLI = nullptr; // <-- this may be wrong?

                                    //    if (!BaselineInfoImpl)
                                    //     BaselineInfoImpl =
                                    //         TargetLibraryInfoImpl(Triple(F.getParent()->getTargetTriple()));
                                    //   return TargetLibraryInfo(*BaselineInfoImpl, &F);

//                                    TargetLibraryAnalysis TLA;
//                                    FunctionAnalysisManager DummyFAM;
//                                    auto F  = builder.GetInsertBlock()->getParent();
//                                    auto TLI = TLA.run(*F, DummyFAM);
//                                    auto M = builder.GetInsertBlock()->getParent()->getParent();
//                                    auto BaselineInfoImpl = TargetLibraryInfoImpl(Triple(M->getTargetTriple()));
//                                    auto TLI2 = TargetLibraryInfo(BaselineInfoImpl);
//
//                                    memcmp_prototype()
//                                    // bcmp means simply compare memory, but no need to retrieve first pos of different byte.
//                                    // -> cheaper to execute.
//                                    auto cmp_ok = llvm::emitBCmp(ptr,
//                                                                 env().strConst(builder, value),
//                                                                 env().i64Const(value.size()),
//                                                                 builder,
//                                                                 DL,
//                                                                 &TLI);

                                    // should be optimized to bcmp
                                    auto memcmpFunc = memcmp_prototype(env().getContext(), env().getModule().get());
                                    auto n = env().i64Const(value.size());
                                    auto cmp_ptr = env().strConst(builder, value);
                                    assert(ptr);
                                    assert(ptr->getType() == env().i8ptrType());
                                    assert(cmp_ptr->getType() == env().i8ptrType());
                                    auto cmp_ok = builder.CreateICmpEQ(env().i64Const(0), builder.CreateCall(memcmpFunc, {ptr, cmp_ptr, n}));
                                    assert(cmp_ok);
                                    assert(cmp_ok->getType() == env().i1Type());
                                    check_cond = builder.CreateAnd(size_ok, cmp_ok);


                                    // #error "prevent expensive check here, simply compare length of string and value contents!"
                                    // old code here: i.e., full parse...
                                    // auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);
//
                                    // // compare value
                                    // auto c_val = parseI64String(const_type.constant());
                                    // check_cond = builder.CreateICmpEQ(_env->i64Const(c_val), val.val);
                                } else if(python::Type::F64 == elementType) {
                                    auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);

                                    // compare value
                                    auto c_val = parseF64String(const_type.constant());
                                    // todo: compare maybe a with abs?
                                    check_cond = builder.CreateFCmpOEQ(_env->f64Const(c_val), val.val);
                                } else {
                                    // fail check, because unsupported type
                                    std::stringstream ss;
                                    ss<<"unsupported type for check "<<elementType.desc()<<" found, fail normal check for all rows";
                                    logger.error(ss.str());
                                    check_cond = _env->i1Const(false);
                                }

                                // now perform check
                                // if !check -> normal_case violation!
                                // else, all good!

                                // // debug:
                                // _env->debugPrint(builder, "performing constant check for col=" + std::to_string(i) + " , " + check.constant_type().desc() + " (1=passed): ", check_cond);
                                // _env->debugPrint(builder, "cellStr", cellStr);
                                // _env->debugPrint(builder, "cellSize", cellSize);

                            } else {
                                logger.warn("unsupported check type encountered");
                            }
                            // append to all checks
                            allChecksPassed = builder.CreateAnd(allChecksPassed, check_cond);
                        }
                    }
                }
            }

            // generated code for all checks, now parse&emite row if checks did not pass!
            auto func = builder.GetInsertBlock()->getParent(); assert(func);
            BasicBlock *bbChecksPassed = BasicBlock::Create(_env->getContext(), "normal_case_checks_passed", func);
            BasicBlock *bbChecksFailed = BasicBlock::Create(_env->getContext(), "normal_case_checks_failed", func);

            builder.CreateCondBr(allChecksPassed, bbChecksPassed, bbChecksFailed);
            builder.SetInsertPoint(bbChecksFailed);

//            // if configured as logging normal-case failures, the proper exception to produce for a failed check is parsed input.
//            // else, if general-case exceptions are accepted, produce a normal-case violation
//            if(exceptionsRowType() == _inputRowTypeGeneralCase) {
//                // need to parse full row (with general case types!)
//                auto generalcase_row = parseGeneralCaseRow(builder, cellsPtr, sizesPtr); // this should handle automatically bad parse.
//                auto serialized_row = serializedExceptionRow(builder, generalcase_row, ExceptionSerializationFormat::GENERALCASE);
//
//                if(exceptionsRowType() != _inputRowTypeGeneralCase) {
//                    // i.e., to solve this should introduce another ExceptionCode::NORMALCASECHECKFAILED which is then in
//                    // the resolve path properly decoded as general-case exception.
//                    throw std::runtime_error("failure here, need to make sure exceptions are always passed in as general case format! Else, the whole hyperspecialziation etc. thing makes no sense.");
//                }
//
//                //// check failed?
//                // _env->debugPrint("check failed. calling except handler with general-case row format...");
//
//                // directly generate call to handler -> no ignore checks necessary.
//                // _env->debugPrint(builder, "normal checks didn't pass");
//                callExceptHandler(builder, userData, _env->i64Const(ecToI64(ExceptionCode::NORMALCASEVIOLATION)),
//                                  _env->i64Const(_operatorID), rowNumber, serialized_row.val, serialized_row.size);
//
//                // processing done, rest needs to be done via different path.
//                builder.CreateRet(env().i64Const(ecToI64(ExceptionCode::SUCCESS)));
//            } else {
//                // bad parse exception! => get's matched/resolved first on fallback path.
//                // DO NOT USE dummies here
//                auto serialized_row = serializeBadParseException(builder, cellsPtr, sizesPtr, false);
//                callExceptHandler(builder, userData, _env->i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)),
//                                  _env->i64Const(_operatorID), rowNumber, serialized_row.val, serialized_row.size);
//
//                // processing done, rest needs to be done via different path.
//                builder.CreateRet(env().i64Const(ecToI64(ExceptionCode::SUCCESS)));
//            }

            {
                // let the slow path do the general-case matching, i.e. produce a bad parse exception.
                // above code allows to use directly general-case format for efficiency. Yet, keep logic smaller for fast
                // code path.

                // bad parse exception! => gets matched/resolved first on fallback path.
                // DO NOT USE dummies here
                // _env->debugPrint(builder, "checks failed, serializing row without dummies.");
                auto serialized_row = serializeBadParseException(builder, cellsPtr, sizesPtr, false, true);
                callExceptHandler(builder, userData, _env->i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)),
                                  _env->i64Const(_operatorID), rowNumber,
                                  ExceptionSerializationFormat::STRING_CELLS,
                                  serialized_row.val, serialized_row.size);

                // processing done, rest needs to be done via different path.
                exitWith(builder, ExceptionCode::SUCCESS);
                //builder.CreateRet(env().i64Const(ecToI64(ExceptionCode::SUCCESS)));
            }

            builder.SetInsertPoint(bbChecksPassed); // continue generating here...
        }

        FlattenedTuple CellSourceTaskBuilder::cellsToTuple(llvm::IRBuilder<>& builder,
                                                           const std::vector<bool> columnsToSerialize,
                                                           const python::Type& inputRowType,
                                                           llvm::Value* cellsPtr,
                                                            llvm::Value* sizesPtr) {

            using namespace llvm;

            auto rowType = restrictRowType(columnsToSerialize, inputRowType);

            assert(columnsToSerialize.size() == inputRowType.parameters().size());

            FlattenedTuple ft(&env());
            ft.init(rowType);

            // create flattened tuple & fill its values.
            // Note: might need to do value conversion first!!!
            int rowTypePos = 0;
            for(int i = 0; i < columnsToSerialize.size(); ++i) {
                // should column be serialized? if so emit type logic!
                if(columnsToSerialize[i]) {
                    assert(rowTypePos < rowType.parameters().size());
                    auto t = rowType.parameters()[rowTypePos];

                    auto val = cachedParse(builder, t, i, cellsPtr, sizesPtr);
                    ft.setElement(builder, rowTypePos, val.val, val.size, val.is_null);
                    rowTypePos++;
                }
            }

            return ft;
        }

        SerializableValue
        CellSourceTaskBuilder::serializeFullRowAsBadParseException(llvm::IRBuilder<> &builder, llvm::Value *cellsPtr,
                                                                   llvm::Value *sizesPtr) const {
            std::vector<llvm::Value*> cell_strs;
            std::vector<llvm::Value*> cell_sizes;

            // perform FULL serialize to make sure extract works! -> this is wasting a lot of space...
            // could optimize IF normal-case columns are contained within general-case, else a different schema is necesary.
            for(int i = 0; i < _columnsToSerialize.size(); ++i) {
                auto colNo = i;
                llvm::Value* cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, _env->i64Const(colNo)), "x" + std::to_string(colNo));
                llvm::Value* cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, _env->i64Const(colNo)), "s" + std::to_string(colNo));

                // zero terminate str
                cellStr = _env->zeroTerminateString(builder, cellStr, cellSize);
                assert(cellStr->getType() == _env->i8ptrType());
                assert(cellSize->getType() == _env->i64Type());
                cell_strs.push_back(cellStr);
                cell_sizes.push_back(cellSize);
            }

            SerializableValue row;

            size_t numCells = _columnsToSerialize.size();
            llvm::Value* buf_size = _env->i64Const(sizeof(int64_t) + numCells * sizeof(int64_t));
            // add actual data sizes up.
            llvm::Value* varlen_total_size = _env->i64Const(0);
            for(const auto& vsize : cell_sizes) {
                varlen_total_size = builder.CreateAdd(varlen_total_size, vsize);
            }
            buf_size = builder.CreateAdd(varlen_total_size, buf_size);

            // alloc temp buffer (rtmalloc!)
            row.val = _env->malloc(builder, buf_size);
            row.size = buf_size;

            // first 64bit is actual number of rows.
            llvm::Value* buf = row.val;
            builder.CreateStore(_env->i64Const(numCells), builder.CreatePointerCast(buf, _env->i64ptrType()));
            buf = builder.CreateGEP(buf, _env->i32Const(sizeof(int64_t)));

            // store cells now incl. offsets...
            llvm::Value* acc_size = _env->i64Const(0);
            for(unsigned i = 0; i < numCells; ++i) {
                auto cell = cell_strs[i];
                auto cell_size = cell_sizes[i];

                assert(cell && cell->getType() == _env->i8ptrType());
                assert(cell_size && cell_size->getType() == _env->i64Type());

                // special case empty str?
                assert(cell && cell_size);
                // the offset is computed using how many varlen fields have been already serialized
                llvm::Value *offset = builder.CreateAdd(_env->i64Const(((int64_t)numCells - (int64_t)i) * sizeof(int64_t)), acc_size);
                // len | size
                llvm::Value *info = builder.CreateOr(builder.CreateZExt(offset, _env->i64Type()), builder.CreateShl(builder.CreateZExt(cell_size, _env->i64Type()), 32));
                builder.CreateStore(info, builder.CreateBitCast(buf, _env->i64ptrType()), false);

                // perform memcpy
                auto dest_ptr = builder.CreateGEP(buf, offset);
                builder.CreateMemCpy(dest_ptr, 0, cell, 0, cell_size);

                acc_size = builder.CreateAdd(acc_size, cell_size);
                buf = builder.CreateGEP(buf, _env->i32Const(sizeof(int64_t)));
            }

            return row;
        }

        SerializableValue CellSourceTaskBuilder::serializeGeneralColumnsAsBadParseException(llvm::IRBuilder<> &builder,
                                                                                            llvm::Value *cellsPtr,
                                                                                            llvm::Value *sizesPtr) const {
            // this is normal-case behavior, however bad-parse exceptions are always in general-case format.
            // -> hence need to upcast to number of columns!
            size_t numGeneralCaseCells = 0;
            auto cell_count = std::min(_generalCaseColumnsToSerialize.size(), _columnsToSerialize.size());
            for(unsigned i = 0; i < cell_count; ++i)
                numGeneralCaseCells += _generalCaseColumnsToSerialize[i];

            // now serialize into buffer (cf. char* serializeParseException(int64_t numCells,
            //            char **cells,
            //            int64_t* sizes,
            //            size_t *buffer_size,
            //            std::vector<bool> colsToSerialize,
            //            decltype(malloc) allocator) function)

            std::vector<llvm::Value*> cell_strs;
            std::vector<llvm::Value*> cell_sizes;

            _env->debugPrint(builder, "loading all general case cell strings");

            for(int i = 0; i < cell_count; ++i) {
                // should column be serialized? if so emit type logic!
                if(_generalCaseColumnsToSerialize[i]) {
                    auto colNo = i;
                    llvm::Value* cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, _env->i64Const(colNo)), "x" + std::to_string(colNo));
                    llvm::Value* cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, _env->i64Const(colNo)), "s" + std::to_string(colNo));

                    assert(cellStr && cellSize);
                    _env->printValue(builder, cellStr, "cell" + std::to_string(colNo) + ": ");
                    _env->printValue(builder, cellStr, "cell size " + std::to_string(colNo) + ": ");

                    // zero terminate str
                    cellStr = _env->zeroTerminateString(builder, cellStr, cellSize);
                    assert(cellStr->getType() == _env->i8ptrType());
                    assert(cellSize->getType() == _env->i64Type());
                    cell_strs.push_back(cellStr);
                    cell_sizes.push_back(cellSize);
                }
            }

            _env->debugPrint(builder, "serializing all general case cell strings");
            return serializeCellVector(builder, cell_strs, cell_sizes);
        }

        inline bool is_str_like_llvm_type(llvm::Type* type) {
            if(!type)
                return false;
            if(type->isPointerTy() && type->getPointerElementType()->isIntegerTy(8))
                return true; //i8ptrtype

//            std::string str;
//            llvm::raw_string_ostream os(str);
//            type->print(os);
//            os.flush();
//            std::cout<<str<<std::endl;
            return false;
        }

        SerializableValue serialize_cell_vector(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                                const std::vector<llvm::Value *> &cells,
                                                const std::vector<llvm::Value *> &cell_sizes,
                                                llvm::Value *empty_str) {
            assert(cells.size() == cell_sizes.size());

            // allocate space, if empty str is used -> allocate only single char!
            auto num_cells = cells.size();
            llvm::Value* buf_size = env.i64Const(sizeof(int64_t) * (1 + num_cells)); // 8 bytes for number of cells + 8bytes for all cells!


            // debug print
#ifndef NDEBUG
            // for(unsigned i = 0; i < cells.size(); ++i) {
            //     if(cells[i])
            //         env.printValue(builder, cells[i], "cell " + std::to_string(i) + "= ");
            //     if(cell_sizes[i])
            //         env.printValue(builder, cell_sizes[i], "cell size" + std::to_string(i) + "= ");
            // }
#endif

            // _env->printValue(builder, buf_size, "fixed buf part in bytes: ");

            // check how much varlength space is required!
            size_t num_empty_str = 0;
            llvm::Value* varlen_total_size = env.i64Const(0);
            for(unsigned i = 0; i < num_cells; ++i) {
                // nullptr cell? do not count for buffer size!
                // -> 0 store.
                if(!cells[i]) {
                    num_empty_str++;
                    continue;
                }

                if(cells[i] != empty_str) {
                    varlen_total_size = builder.CreateAdd(varlen_total_size, cell_sizes[i]);
                } else {
                    num_empty_str++;
                }
            }

            // _env->printValue(builder, varlen_total_size, "total varlen bytes (excl. empty str) required: ");

            buf_size = builder.CreateAdd(buf_size, varlen_total_size);
            if(0 != num_empty_str)
                buf_size = builder.CreateAdd(buf_size, env.i64Const(1));

            // _env->printValue(builder, buf_size, "total bytes required (" + std::to_string(num_empty_str) + "x empty str): ");

            SerializableValue row;
            row.val = env.malloc(builder, buf_size);
            row.size = buf_size;

            // store now everything
            // first 64bit is actual number of rows.
            llvm::Value* buf = row.val;
            if(0 != num_empty_str) {
                // store empty string (0) at end of buffer
                auto idx = builder.CreateGEP(buf, builder.CreateSub(buf_size, env.i64Const(1)));
                builder.CreateStore(env.i8Const('\0'), idx);
            }

            builder.CreateStore(env.i64Const(num_cells), builder.CreatePointerCast(buf, env.i64ptrType()));
            buf = builder.CreateGEP(buf, env.i32Const(sizeof(int64_t)));

            // store general case cells now...
            llvm::Value* acc_size = env.i64Const(0);
            llvm::Value* empty_str_offset = nullptr;
            for(unsigned pos = 0; pos < num_cells; ++pos) {
                auto cell = cells[pos];
                auto cell_size = cell_sizes[pos];

                bool regular_cell = cell && cell_size && cell != empty_str;
                // special case empty str?
                if(regular_cell) {

                    //assert(cell && is_str_like_llvm_type(cell->getType()));
                    assert(cell_size && cell_size->getType() == env.i64Type());

                    // the offset is computed using how many varlen fields have been already serialized
                    llvm::Value *offset = builder.CreateAdd(env.i64Const(((int64_t)num_cells - (int64_t)pos) * sizeof(int64_t)), acc_size);
                    // len | size
                    llvm::Value *info = pack_offset_and_size(builder, offset, cell_size);
                    builder.CreateStore(info, builder.CreateBitCast(buf, env.i64ptrType()), false);

                    // perform memcpy
                    auto dest_ptr = builder.CreateGEP(buf, offset);
                    builder.CreateMemCpy(dest_ptr, 0, cell, 0, cell_size);

                    acc_size = builder.CreateAdd(acc_size, cell_size);
                    buf = builder.CreateGEP(buf, env.i32Const(sizeof(int64_t)));
                } else {
                    // dummy: use empty string at the end of buffer
                    // --> need to compute correct offset.
                    auto remaining_cells_offset = env.i64Const(((int64_t)num_cells - (int64_t)pos) * sizeof(int64_t));
                    llvm::Value *offset = builder.CreateAdd(remaining_cells_offset, varlen_total_size);
                    assert(offset->getType() == env.i64Type());
                    llvm::Value *info = pack_offset_and_size(builder, offset, env.i64Const(1));
                    builder.CreateStore(info, builder.CreateBitCast(buf, env.i64ptrType()), false);

                    // move buffer
                    buf = builder.CreateGEP(buf, env.i32Const(sizeof(int64_t)));
                }
            }

            return row;
        }

        SerializableValue
        CellSourceTaskBuilder::serializeCellVector(llvm::IRBuilder<> &builder, const std::vector<llvm::Value *> &cells,
                                                   const std::vector<llvm::Value *> &cell_sizes,
                                                   llvm::Value *empty_str) const {
           return serialize_cell_vector(*_env, builder, cells, cell_sizes, empty_str);
        }

        SerializableValue
        CellSourceTaskBuilder::serializeBadParseException(llvm::IRBuilder<> &builder, llvm::Value *cellsPtr,
                                                          llvm::Value *sizesPtr, bool use_dummies, bool use_only_projected_general_case_columns) const {

            // special case: serialize full row? may be triggered through hyper mode?
            if(!use_only_projected_general_case_columns)
                return serializeFullRowAsBadParseException(builder, cellsPtr, sizesPtr);

            // else, project for general-case row!
            if(!use_dummies)
                return serializeGeneralColumnsAsBadParseException(builder, cellsPtr, sizesPtr);

            size_t numNormalCaseColsToSerialize = 0;

            std::vector<llvm::Value*> cell_strs;
            std::vector<llvm::Value*> cell_sizes;

#ifndef NDEBUG
             // _env->debugPrint(builder, "CellSourceTaskBuilder: serializeBadParseException");
#endif

            for(int i = 0; i < _columnsToSerialize.size(); ++i) {
                // should column be serialized? if so emit type logic!
                if(_columnsToSerialize[i]) {
                    auto colNo = i;
                    llvm::Value* cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, _env->i64Const(colNo)), "x" + std::to_string(colNo));
                    llvm::Value* cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, _env->i64Const(colNo)), "s" + std::to_string(colNo));

                    // zero terminate str
                    cellStr = _env->zeroTerminateString(builder, cellStr, cellSize);
                    assert(cellStr->getType() == _env->i8ptrType());
                    assert(cellSize->getType() == _env->i64Type());
                    cell_strs.push_back(cellStr);
                    cell_sizes.push_back(cellSize);
                    numNormalCaseColsToSerialize++;
                }
            }

            // this is normal-case behavior, however bad-parse exceptions are always in general-case format.
            // -> hence need to upcast to number of columns!
            size_t numGeneralCaseCells = 0;
            for(unsigned i = 0; i < _generalCaseColumnsToSerialize.size(); ++i)
                numGeneralCaseCells += _generalCaseColumnsToSerialize[i];

            // now serialize into buffer (cf. char* serializeParseException(int64_t numCells,
            //            char **cells,
            //            int64_t* sizes,
            //            size_t *buffer_size,
            //            std::vector<bool> colsToSerialize,
            //            decltype(malloc) allocator) function)

            // convert normal-case to general-case cells
            std::vector<llvm::Value*> gen_cells(numGeneralCaseCells, nullptr);
            std::vector<llvm::Value*> gen_cell_sizes(numGeneralCaseCells, nullptr);

            auto empty_str = _env->strConst(builder, "");
            auto empty_str_size = _env->i64Const(1);

            size_t normal_pos = 0;
            size_t general_pos = 0;
            for(unsigned i = 0; i < _columnsToSerialize.size(); ++i) {
                if(_columnsToSerialize[i]) {
                    // mapping?
                    if(!_normalToGeneralMapping.empty() && numGeneralCaseCells != numNormalCaseColsToSerialize) {
                        auto gen_idx = _normalToGeneralMapping.at(normal_pos);
                        assert(gen_idx < numGeneralCaseCells);

                        // this assert is violated by e.g. a constant property, parsed as check! handle accordingly...

                        // if general case cell is not found in columns then it must be a check
                        if(_generalCaseColumnsToSerialize[gen_idx]) {
                            gen_cells[gen_idx] = cell_strs[normal_pos];
                            gen_cell_sizes[gen_idx] = cell_sizes[normal_pos];
                        } else {
                            // ... => should be constant? // or ignored?

                        }

                    } else {
                        gen_cells[general_pos] = cell_strs[normal_pos];
                        gen_cell_sizes[general_pos] = cell_sizes[normal_pos];
                    }
                    normal_pos++;
                }

                // because _columnsToSerialize and general columns to serialize may be different, either use dummies or reuse
                if(i < _generalCaseColumnsToSerialize.size() && _generalCaseColumnsToSerialize[i]) {
                    if(!gen_cells[general_pos]) {
                        if(use_dummies) {
                            gen_cells[general_pos] = empty_str;
                            gen_cell_sizes[general_pos] = empty_str_size;
                        } else {
                            // originals
                            size_t colNo = i;
                            llvm::Value* cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, _env->i64Const(colNo)), "x" + std::to_string(colNo));
                            llvm::Value* cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, _env->i64Const(colNo)), "s" + std::to_string(colNo));

                            // zero terminate str
                            cellStr = _env->zeroTerminateString(builder, cellStr, cellSize);
                            gen_cells[general_pos] = cellStr;
                            gen_cell_sizes[general_pos] = cellSize;
                        }

                        // special case: constant normal check
                        for(const auto& check : _checks) {
                            if(i == check.colNo && check.type == CheckType::CHECK_CONSTANT) {
                                std::string str_value = check.constant_type().constant();
                                gen_cells[general_pos] = _env->strConst(builder, str_value);
                                gen_cell_sizes[general_pos] = _env->i64Const(str_value.size() + 1);
                            }
                        }
                    }
                    general_pos++;
                }
            }

            // ------
            // now serialize!
            return serializeCellVector(builder, gen_cells, gen_cell_sizes, empty_str);
        }


        llvm::BasicBlock* CellSourceTaskBuilder::valueErrorBlock(llvm::IRBuilder<> &builder) {
            using namespace llvm;

            // create value error block lazily
            if(!_valueErrorBlock) {
                _valueErrorBlock = BasicBlock::Create(env().getContext(), "value_error", builder.GetInsertBlock()->getParent());

                IRBuilder<> b(_valueErrorBlock);

#ifndef NDEBUG
                 // env().debugPrint(b, "value error block entered.");
#endif
                // could use here value error as well. However, for internal resolve use badparse string input!
                exitWith(b, ExceptionCode::BADPARSE_STRING_INPUT);
                // b.CreateRet(env().i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));
            }

            return _valueErrorBlock;
        }

        llvm::BasicBlock* CellSourceTaskBuilder::nullErrorBlock(llvm::IRBuilder<> &builder) {
            using namespace llvm;
            if(!_nullErrorBlock) {
                _nullErrorBlock = BasicBlock::Create(env().getContext(), "null_error", builder.GetInsertBlock()->getParent());
                IRBuilder<> b(_nullErrorBlock);

#ifndef NDEBUG
                 // _env->debugPrint(b, "emitting NULLERROR (CellSourceTaskBuilder)");
#endif
                // b.CreateRet(env().i64Const(ecToI64(ExceptionCode::NULLERROR))); // internal error! => use this to force compiled processing?

                // use this to force fallback processing...
                exitWith(b, ExceptionCode::BADPARSE_STRING_INPUT);
                // b.CreateRet(env().i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));
            }
            return _nullErrorBlock;
        }
    }
}
