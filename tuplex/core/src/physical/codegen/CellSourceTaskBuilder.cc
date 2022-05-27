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

                    // if normal <-> general are incompatible, serialize as NormalCaseViolationError that requires interpreter!
                    if(isNormalCaseAndGeneralCaseCompatible()) {
                        // add here exception block for pipeline errors, serialize tuple etc...
                        auto serialized_row = serializedExceptionRow(builder, ft);

                        // debug print
                        logger.debug("CellSourceTaskBuilder: input row type in which exceptions from pipeline are stored that are **not** parse-exceptions is " + ft.getTupleType().desc());
                        logger.debug("I.e., when creating resolve tasks for this pipeline - set exceptionRowType to this type.");
                        outputRowNumber = builder.CreateLoad(outputRowNumberVar);
                        llvm::BasicBlock *curBlock = builder.GetInsertBlock();
                        auto bbException = exceptionBlock(builder, userData, ecCode, ecOpID, outputRowNumber,
                                                          serialized_row.val, serialized_row.size);
                        builder.CreateBr(bbNoException);
                        // add branching to previous block
                        builder.SetInsertPoint(curBlock);
                        builder.CreateCondBr(exceptionRaised, bbException, bbNoException);
                    } else {
                        outputRowNumber = builder.CreateLoad(outputRowNumberVar);
                        auto nc_ecCode = _env->i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT));
                        auto nc_ecOpID = _env->i64Const(_operatorID);
                        auto serialized_row = serializeBadParseException(builder, cellsPtr, sizesPtr);

                        // important to get curBlock here.
                        llvm::BasicBlock *curBlock = builder.GetInsertBlock();
                        auto bbException = exceptionBlock(builder, userData, nc_ecCode, nc_ecOpID, outputRowNumber,
                                                          serialized_row.val, serialized_row.size);
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

                    builder.CreateRet(env().i64Const(ecToI64(ExceptionCode::SUCCESS)));
                } else {

                    // if intermediate callback desired, perform!
                    if(_intermediateType != python::Type::UNKNOWN && !_intermediateCallbackName.empty()) {
                        writeIntermediate(builder, userData, _intermediateCallbackName);
                    }

                    // propagate result to callee, because can be used to update counters
                    builder.CreateRet(builder.CreateZExtOrTrunc(ecCode, env().i64Type()));
                }
            } else {
                // if intermediate callback desired, perform!
                if(_intermediateType != python::Type::UNKNOWN && !_intermediateCallbackName.empty()) {
                    writeIntermediate(builder, userData, _intermediateCallbackName);
                }


                // create success ret
                builder.CreateRet(env().i64Const(ecToI64(ExceptionCode::SUCCESS)));
            }
            return func;
        }

        SerializableValue CellSourceTaskBuilder::cachedParse(llvm::IRBuilder<>& builder, const python::Type& type, size_t colNo, llvm::Value* cellsPtr, llvm::Value* sizesPtr) {
            using namespace llvm;

            auto key = std::make_tuple(colNo, type);

            //@TODO: there's an error here, i.e. need to force parsing for both general/normal first on path
            // to avoid domination errors in codegen.
            bool no_cache = true; // HACK to disable cachedParse.

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
                    t = t.withoutOptions();

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

        void CellSourceTaskBuilder::generateChecks(llvm::IRBuilder<>& builder, llvm::Value* userData, llvm::Value* rowNumber, llvm::Value* cellsPtr, llvm::Value* sizesPtr) {
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
                    ss<<"emitting constant check == "<<check.constant_type().constant()<<" for column "<<check.colNo<<"\n";
                } else {
                    ss<<"emitting unknown check for column "<<check.colNo<<"\n";
                }
                logger.info(ss.str());
            }

            // Interesting questions re. checks: => these checks should be performed first. What is the optimal order of checks to perform?
            // what to test first for?

            llvm::Value* allChecksPassed = _env->i1Const(true);
            _env->debugPrint(builder, "Checking normalcase for rowno=", rowNumber);

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
				    _env->debugPrint(builder, "Checking whether cellsize==", _env->i64Const(const_type.constant().size() + 1));
                                    auto constant_value = str_value_from_python_raw_value(const_type.constant());
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
                                    auto val = cachedParse(builder, elementType, i, cellsPtr, sizesPtr);

                                    // compare value
                                    auto c_val = parseI64String(const_type.constant());
                                    check_cond = builder.CreateICmpEQ(_env->i64Const(c_val), val.val);
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
                                _env->debugPrint(builder, "performing constant check for col=" + std::to_string(i) + " , " + check.constant_type().desc() + " (1=passed): ", check_cond);
				_env->debugPrint(builder, "cellStr", cellStr);
				_env->debugPrint(builder, "cellSize", cellSize);

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

            // need to parse full row (with general case types!)
            auto generalcase_row = parseGeneralCaseRow(builder, cellsPtr, sizesPtr);
            auto serialized_row = serializedExceptionRow(builder, generalcase_row);

            // directly generate call to handler -> no ignore checks necessary.
            _env->debugPrint(builder, "normal checks didn't pass");
            callExceptHandler(builder, userData, _env->i64Const(ecToI64(ExceptionCode::NORMALCASEVIOLATION)),
                                              _env->i64Const(_operatorID), rowNumber, serialized_row.val, serialized_row.size);

            // processing done, rest needs to be done via different path.
            builder.CreateRet(env().i64Const(ecToI64(ExceptionCode::SUCCESS)));

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
        CellSourceTaskBuilder::serializeBadParseException(llvm::IRBuilder<> &builder, llvm::Value *cellsPtr,
                                                          llvm::Value *sizesPtr) const {
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

                    cell_strs.push_back(cellStr);
                    cell_sizes.push_back(cellSize);
                    numNormalCaseColsToSerialize++;
                }
            }

            // this is normal-case behavior, however exceptions are always in general-case format.
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

            size_t numCells = numGeneralCaseCells; //this->_columnsToSerialize.size();
            llvm::Value* buf_size = _env->i64Const(sizeof(int64_t) + numCells * sizeof(int64_t));
            // add actual data sizes up.
            llvm::Value* varlen_total_size = _env->i64Const(0);
            for(const auto& vsize : cell_sizes) {
                varlen_total_size = builder.CreateAdd(varlen_total_size, vsize);
            }
            buf_size = builder.CreateAdd(varlen_total_size, buf_size);

            // use a single empty string in addition when general/normal differ for the dummies.
            llvm::Value* buf_size_without_empty_str = nullptr;
            if(numGeneralCaseCells != numNormalCaseColsToSerialize) {
                buf_size_without_empty_str = buf_size;
                buf_size = builder.CreateAdd(buf_size, _env->i64Const(1));
            }

            SerializableValue row;
            // alloc temp buffer (rtmalloc!)
            row.val = _env->malloc(builder, buf_size);
            row.size = buf_size;

            // first 64bit is actual number of rows.
            llvm::Value* buf = row.val;
            if(buf_size_without_empty_str) {
                // store empty string (0)
                auto idx = builder.CreateGEP(buf, buf_size);
                builder.CreateStore(_env->i8Const('\0'), idx);
            }

            builder.CreateStore(_env->i64Const(numGeneralCaseCells), builder.CreatePointerCast(buf, _env->i64ptrType()));
            buf = builder.CreateGEP(buf, _env->i32Const(sizeof(int64_t)));

            // convert normal-case to general-case cells
            std::vector<llvm::Value*> gen_cells(numGeneralCaseCells, nullptr);
            std::vector<llvm::Value*> gen_cell_sizes(numGeneralCaseCells, nullptr);

            size_t normal_pos = 0;
            for(unsigned i = 0; i < _columnsToSerialize.size(); ++i) {
                if(_columnsToSerialize[i]) {
                    // mapping?
                    if(!_normalToGeneralMapping.empty() && numGeneralCaseCells != numNormalCaseColsToSerialize) {
                        auto gen_idx = _normalToGeneralMapping.at(normal_pos);
                        assert(gen_idx < numGeneralCaseCells);
                        assert(_generalCaseColumnsToSerialize[gen_idx]);
                        gen_cells[gen_idx] = cell_strs[normal_pos];
                        gen_cells[gen_idx] = cell_sizes[normal_pos];
                    } else {
                        gen_cells[i] = cell_strs[normal_pos];
                        gen_cell_sizes[i] = cell_sizes[normal_pos];
                    }
                    normal_pos++;
                }
            }

            // store general case cells now...
            size_t general_pos = 0;
            llvm::Value* acc_size = _env->i64Const(0);
            llvm::Value* empty_str_offset = nullptr;
            for(unsigned i = 0; i < _generalCaseColumnsToSerialize.size(); ++i) {
                if(_generalCaseColumnsToSerialize[i]) {
                    //   uint64_t info = (uint64_t)sizes[i] & 0xFFFFFFFF;
                    //
                    //                // offset = jump + acc size
                    //                uint64_t offset = (numCellsToSerialize - normal_pos) * sizeof(int64_t) + acc_size;
                    //                *(uint64_t*)buf = (info << 32u) | offset;
                    //                memcpy(buf_ptr + sizeof(int64_t) * (numCellsToSerialize + 1) + acc_size, cells[i], sizes[i]);
                    //
                    //                // memcmp check?
                    //                assert(memcmp(buf + offset, cells[i], sizes[i]) == 0);
                    //
                    //                buf += sizeof(int64_t);
                    //                acc_size += sizes[i];
                    //                normal_pos++;

                    auto cell = gen_cells[general_pos];
                    auto cell_size = gen_cell_sizes[general_pos];

                    // special case empty str?
                    if(cell && cell_size) {
                        // the offset is computed using how many varlen fields have been already serialized
                        llvm::Value *offset = builder.CreateAdd(_env->i64Const(((int64_t)numGeneralCaseCells - (int64_t)general_pos) * sizeof(int64_t)), acc_size);
                        // len | size
                        llvm::Value *info = builder.CreateOr(builder.CreateZExt(offset, _env->i64Type()), builder.CreateShl(builder.CreateZExt(cell_size, _env->i64Type()), 32));
                        builder.CreateStore(info, builder.CreateBitCast(buf, _env->i64ptrType()), false);


                        // perform memcpy
                        auto dest_ptr = builder.CreateGEP(buf, offset);
                        builder.CreateMemCpy(dest_ptr, 0, cell, 0, cell_size);

                        acc_size = builder.CreateAdd(acc_size, cell_size);
                        buf = builder.CreateGEP(buf, _env->i32Const(sizeof(int64_t)));
                    } else {
                        // dummy: use empty string at the end of buffer
                        // --> need to compute correct offset.
                        auto remaining_cells_offset = _env->i64Const(((int64_t)numGeneralCaseCells - (int64_t)general_pos) * sizeof(int64_t));
                        llvm::Value *offset = builder.CreateAdd(remaining_cells_offset, varlen_total_size);
                        llvm::Value *info = builder.CreateOr(builder.CreateZExt(offset, _env->i64Type()),
                                                             builder.CreateShl(builder.CreateZExt(_env->i64Const(sizeof(char)), _env->i64Type()), 32));
                        builder.CreateStore(info, builder.CreateBitCast(buf, _env->i64ptrType()), false);

                        // move buffer
                        buf = builder.CreateGEP(buf, _env->i32Const(sizeof(int64_t)));
                    }

                    general_pos++;
                }
            }

            return row;
        }


        llvm::BasicBlock* CellSourceTaskBuilder::valueErrorBlock(llvm::IRBuilder<> &builder) {
            using namespace llvm;

            // create value error block lazily
            if(!_valueErrorBlock) {
                _valueErrorBlock = BasicBlock::Create(env().getContext(), "value_error", builder.GetInsertBlock()->getParent());

                IRBuilder<> b(_valueErrorBlock);

                // could use here value error as well. However, for internal resolve use badparse string input!
                b.CreateRet(env().i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));
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
                b.CreateRet(env().i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));
            }
            return _nullErrorBlock;
        }
    }
}
