//
// Created by leonhard on 10/12/22.
//
#include <physical/codegen/ResolveHelper.h>
#include <physical/codegen/JsonSourceTaskBuilder.h>
#include <physical/codegen/CellSourceTaskBuilder.h>

namespace tuplex {
    namespace codegen {


        void handlePythonParallelizeException(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ecCode) {
            using namespace llvm;

            auto& ctx = builder.getContext();

            BasicBlock* bIsPyParallelize = BasicBlock::Create(ctx, "is_python_parallelize_exception", builder.GetInsertBlock()->getParent());
            BasicBlock* bIsNot = BasicBlock::Create(ctx, "is_not", builder.GetInsertBlock()->getParent());

            auto is_py_parallelize_cond = builder.CreateICmpEQ(ecCode, env.i64Const(ecToI64(ExceptionCode::PYTHON_PARALLELIZE)));
            builder.CreateCondBr(is_py_parallelize_cond, bIsPyParallelize, bIsNot);

            builder.SetInsertPoint(bIsPyParallelize);
            // this exception is handled by the interpreter, so use interpreter for this!
            env.freeAll(builder);
            builder.CreateRet(ecCode);

            builder.SetInsertPoint(bIsNot);
        }

        FlattenedTuple decodeCSVCells(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                      const std::shared_ptr<FileInputOperator>& input_op,
                                      const python::Type& pip_input_row_type,
                                      const ExceptionCode& return_code_on_parse_error,
                                      llvm::Value* buf,
                                      llvm::Value* buf_size) {

            using namespace llvm;
            assert(input_op);
            auto& logger = Logger::instance().logger("codegen");
            assert(pip_input_row_type.isTupleType());

            FlattenedTuple ft(&env);
            ft.init(pip_input_row_type);

            auto num_cells = builder.CreateLoad(builder.CreatePointerCast(buf, env.i64ptrType()));
            int64_t num_desired_cells = pip_input_row_type.parameters().size();

            // quick check on whether number of cells matches.
            auto cell_count_match = builder.CreateICmpEQ(num_cells, env.i64Const(num_desired_cells));

            env.printValue(builder, num_cells, "checking cell count (expected: " + std::to_string(num_desired_cells) + ")");

            BasicBlock* bCellCountOK = BasicBlock::Create(env.getContext(), "cell_count_ok", builder.GetInsertBlock()->getParent());
            BasicBlock* bFailure =  BasicBlock::Create(env.getContext(), "row_mismatch", builder.GetInsertBlock()->getParent());

            builder.CreateCondBr(cell_count_match, bCellCountOK, bFailure);
            builder.SetInsertPoint(bFailure);
            env.printValue(builder, cell_count_match, "failed cell count match with: ");
            builder.CreateRet(env.i64Const(ecToI64(return_code_on_parse_error)));

            // continue, parse cells according to schema!
            builder.SetInsertPoint(bCellCountOK);

            Value* ptr = builder.CreateGEP(buf, env.i64Const(sizeof(int64_t)));
            // need to parse all cells
            for(unsigned i = 0; i < num_desired_cells; ++i) {
                // decode cell & size
                auto info = builder.CreateLoad(builder.CreatePointerCast(ptr, env.i64ptrType()));
                llvm::Value* offset=nullptr, *cell_size = nullptr;
                std::tie(offset, cell_size) = unpack_offset_and_size(builder, info);
                auto cell_str = builder.CreateGEP(ptr, offset);

                auto cell_type = pip_input_row_type.parameters()[i];
                env.printValue(builder, cell_str, "parsing cell " + std::to_string(i) + " (type: " + cell_type.desc() + " )");

                // parse cell and assign.
                auto cell = parse_string_cell(env, builder, bFailure, cell_type, input_op->null_values(), cell_str, cell_size);

                // assign to tuple
                ft.set(builder, {(int)i}, cell.val, cell.size, cell.is_null);

                ptr = builder.CreateGEP(ptr, env.i64Const(sizeof(int64_t)));
            }

            // env.freeAll(builder); // <-- is this correct?

            env.debugPrint(builder, "parsed ft from CSV");

            return ft;
        }


        FlattenedTuple decodeBadParseStringInputException(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                                          const std::shared_ptr<FileInputOperator>& input_op,
                                                          const python::Type& pip_input_row_type,
                                                          const ExceptionCode& return_code_on_parse_error,
                                                          llvm::Value* buf,
                                                          llvm::Value* buf_size) {
            using namespace llvm;
            assert(input_op);

            auto& logger = Logger::instance().logger("codegen");

            // which input format should be parsed as?
            //auto input_row_type = pip.
            FlattenedTuple ft(&env);
            ft.init(pip_input_row_type);

            // check the file format
            switch(input_op->fileFormat()) {
                case FileFormat::OUTFMT_JSON: {

                    // parse tuple for pipeline, on failure return the code above.

                    // note: for JSON it's about the output columns (no check yet here)
                    auto parseF = json_generateParseStringFunction(env,
                                                                   "general_case_parse_string",
                                                                   input_op->getOutputSchema().getRowType(),
                                                                   input_op->columns());

                    if(input_op->getOutputSchema().getRowType() != pip_input_row_type) {
                        std::stringstream ss;
                        ss<<"input op input schema: "<<input_op->getInputSchema().getRowType().desc()<<std::endl;
                        ss<<"pip input schema: "<<pip_input_row_type.desc()<<std::endl;
                        logger.debug(ss.str());
                    }

                    // @TODO: make sure parseF output is compatible with pip input row type
                    assert(input_op->getOutputSchema().getRowType() == pip_input_row_type);

                    // extract string and length from data buffer

                    auto num_cells = builder.CreateLoad(builder.CreatePointerCast(buf, env.i64ptrType()));
                    // env.printValue(builder, num_cells, "num cells: ");

                    // for JSON, single info and cell
                    auto ptr = builder.CreateGEP(buf, env.i64Const(sizeof(int64_t)));
                    auto info = builder.CreateLoad(builder.CreatePointerCast(ptr, env.i64ptrType()));

                    llvm::Value* offset=nullptr, *str_size = nullptr;
                    std::tie(offset, str_size) = unpack_offset_and_size(builder, info);

                    auto str = builder.CreateGEP(ptr, offset);

                    // env.printValue(builder, offset, "offset (should be 8): ");
                    // env.printValue(builder, str, "data: ");
                    // env.printValue(builder, str_size, "data size: ");

                    assert(parseF);

                    // call by parsing function
                    auto tuple_var = env.CreateFirstBlockAlloca(builder, ft.getLLVMType());
                    auto rc = builder.CreateCall(parseF, {tuple_var, str, str_size});

                    // env.printValue(builder, rc, "parse bad string - call result: ");

                    BasicBlock* bParseOK = BasicBlock::Create(env.getContext(), "parse_for_pipeline_ok", builder.GetInsertBlock()->getParent());
                    BasicBlock* bParseFailed =  BasicBlock::Create(env.getContext(), "parse_for_pipeline_failed", builder.GetInsertBlock()->getParent());

                    auto rc_ok = builder.CreateICmpEQ(rc, env.i64Const(ecToI64(ExceptionCode::SUCCESS)));

                    builder.CreateCondBr(rc_ok, bParseOK, bParseFailed);

                    builder.SetInsertPoint(bParseFailed);
                    env.freeAll(builder);
                    builder.CreateRet(env.i64Const(ecToI64(return_code_on_parse_error)));

                    builder.SetInsertPoint(bParseOK);
                    ft = FlattenedTuple::fromLLVMStructVal(&env, builder, tuple_var, pip_input_row_type);
                    return ft;
                    break;
                }
                case FileFormat::OUTFMT_CSV: {
                    // csv is broken up into multiple cells that need to be matched (incl. check on size!)
                    return decodeCSVCells(env, builder, input_op, pip_input_row_type, return_code_on_parse_error, buf, buf_size);
                    break;
                }
                default: {
                    throw std::runtime_error("found input operator with unsupported file format, need to implement...");
                    break;
                }
            }

            return ft;
        }

        void handleBadParseStringInputException(LLVMEnvironment& env,
                                                llvm::IRBuilder<>& builder, const python::Type& pip_input_row_type,
                                                llvm::Function* pipeline_func,
                                                const std::shared_ptr<FileInputOperator>& input_op,
                                                llvm::Value* ecCode,
                                                llvm::Value* rowNumber,
                                                llvm::Value* userData,
                                                llvm::Value* buf,
                                                llvm::Value* buf_size) {
            using namespace llvm;

            assert(buf && buf->getType() == env.i8ptrType());
            assert(buf_size && buf_size->getType() == env.i64Type());

            auto& ctx = builder.getContext();

            BasicBlock* bIsBadParseStringInput = BasicBlock::Create(ctx, "is_bad_string_parse_exception", builder.GetInsertBlock()->getParent());
            BasicBlock* bIsNot = BasicBlock::Create(ctx, "is_not", builder.GetInsertBlock()->getParent());

            auto is_bad_parse_cond = builder.CreateICmpEQ(ecCode, env.i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));
            builder.CreateCondBr(is_bad_parse_cond, bIsBadParseStringInput, bIsNot);

            builder.SetInsertPoint(bIsBadParseStringInput);

            // is input op given? -> then can handle, else abort immediately.
            if(!input_op) {
                // this exception is handled by the interpreter, so use interpreter for this!
                env.freeAll(builder);
                builder.CreateRet(ecCode);
            }

            // all good, now handle everything here.
            auto ft = decodeBadParseStringInputException(env, builder, input_op, pip_input_row_type,
                                                         ExceptionCode::GENERALCASEVIOLATION, buf, buf_size);


            // process using pipeline
            //PipelineBuilder::call(builder, pip.build(), ft, userData, )

            auto pip_res = PipelineBuilder::call(builder, pipeline_func, ft, userData, rowNumber); // no intermediate support right now.

            // create if based on resCode to go into exception block
            ecCode = builder.CreateZExtOrTrunc(pip_res.resultCode, env.i64Type());
            auto ecOpID = builder.CreateZExtOrTrunc(pip_res.exceptionOperatorID, env.i64Type());
            auto numRowsCreated = builder.CreateZExtOrTrunc(pip_res.numProducedRows, env.i64Type());

            // env.printValue(builder, ecCode, "slow pip ec= ");

            // use provided return code.
            env.freeAll(builder);
            builder.CreateRet(ecCode);

            // before exiting function, make sure to set builder to correct insert point.
            builder.SetInsertPoint(bIsNot);
        }

        // new version, require explicitly stored format information.
        llvm::Function* createProcessExceptionRowWrapper(LLVMEnvironment& env,
                                                         const python::Type& pip_input_row_type,
                                                         llvm::Function* pipeline_func,
                                                         const std::shared_ptr<FileInputOperator>& input_op,
                                                         const std::string& name,
                                                         const python::Type& normalCaseType,
                                                         const std::map<int, int>& normalToGeneralMapping,
                                                         const std::vector<std::string>& null_values,
                                                         const CompilePolicy& policy) {

            auto& logger = Logger::instance().logger("codegen");
            auto pipFunc = pipeline_func;

            if(!pipFunc)
                return nullptr;

            auto generalCaseType = pip_input_row_type;
            bool normalCaseAndGeneralCaseCompatible = checkCaseCompatibility(normalCaseType, generalCaseType, normalToGeneralMapping);

            {
                std::stringstream ss;
                ss<<"creating slow path based on\n";
                ss<<"\tnormalcase:  "<<normalCaseType.desc()<<"\n";
                ss<<"\tgeneralcase: "<<generalCaseType.desc()<<"\n";
                logger.debug(ss.str());
            }

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


            // create function
            using namespace llvm;
            using namespace std;

            // create (internal) llvm function to be inlined with all contents
            auto& ctx = pipFunc->getContext();

            // void* userData, int64_t rowNumber, int64_t ExceptionCode, uint8_t* inputBuffer, int64_t inputBufferSize
            FunctionType *func_type = FunctionType::get(Type::getInt64Ty(ctx),
                                                        {Type::getInt8PtrTy(ctx, 0),
                                                         Type::getInt64Ty(ctx),
                                                         Type::getInt64Ty(ctx),
                                                         Type::getInt8PtrTy(ctx, 0),
                                                         Type::getInt64Ty(ctx)}, false);
            auto func = Function::Create(func_type, Function::ExternalLinkage, name, pipFunc->getParent());

            // set arg names
            auto args = mapLLVMFunctionArgs(func, {"userData",  "rowNumber", "exceptionCode", "rowBuf", "bufSize",});

            auto body = BasicBlock::Create(ctx, "body", func);
            IRBuilder<> builder(body);
            // decode according to exception type => i.e. decode according to pipeline builder + nullvalue opt!
            auto ecCode = args["exceptionCode"];
            auto dataPtr = args["rowBuf"];
            auto dataSize = args["bufSize"];
            auto userData = args["userData"];
            auto rowNo = args["rowNumber"];

            // exceptions are stored in a variety of formats
            // 1. PYTHON_PARALLELIZE -> stored as pickled object, can't decode. Requires interpreter functor.
            handlePythonParallelizeException(env, builder, ecCode);

            // 2. NORMALCASE_VIOLATION_IN_NORMAL_FORMAT
            // -> decode using normal-case format and upcast to general-case format.

            // 3. NORMALCASE_VIOLATION_IN_GENERAL_FORMAT
            // -> decode using general-case format, process.

            // 4. BADPARSE_STRING_INPUT
            // -> attempt to parse to general-case format, if fails return.
            handleBadParseStringInputException(env, builder, pip_input_row_type, pipFunc,
                                               input_op, ecCode, rowNo, userData, dataPtr, dataSize);


            // debug
#ifndef NDEBUG
            env.printValue(builder, ecCode, "general path, got exception code (unhandled): ");
#endif

            // no success, return original ecCode
            env.freeAll(builder);
            builder.CreateRet(ecCode);

            // erase (empty) blocks with no predecessor and successor
            for(auto it = func->begin(); it != func->end(); ++it) {
                if(it->empty()) {
                    auto block = &(*it);
                    size_t pred_count = predecessorCount(block);
                    size_t succ_count = successorBlockCount(block);
                    if(0 == pred_count && 0 == succ_count)
                        it = it->eraseFromParent();
                }
            }

            return func;
        }


//        llvm::Function* createProcessExceptionRowWrapper(PipelineBuilder& pip,
//                                                         const std::string& name, const python::Type& normalCaseType,
//                                                         const std::map<int, int>& normalToGeneralMapping,
//                                                         const std::vector<std::string>& null_values,
//                                                         const CompilePolicy& policy) {
//
//            auto& logger = Logger::instance().logger("codegen");
//            auto pipFunc = pip.getFunction();
//
//            if(!pipFunc)
//                return nullptr;
//
//            // debug
//#define PRINT_EXCEPTION_PROCESSING_DETAILS
//
//            auto generalCaseType = pip.inputRowType();
//            bool normalCaseAndGeneralCaseCompatible = checkCaseCompatibility(normalCaseType, generalCaseType, normalToGeneralMapping);
//
//            {
//                std::stringstream ss;
//                ss<<"creating slow path based on\n";
//                ss<<"\tnormalcase:  "<<normalCaseType.desc()<<"\n";
//                ss<<"\tgeneralcase: "<<generalCaseType.desc()<<"\n";
//                logger.info(ss.str());
//            }
//
//            if(!normalCaseAndGeneralCaseCompatible) {
//                logger.debug("normal and general case are not compatible, forcing all exceptions on fallback (interpreter) path.");
//                std::stringstream ss;
//                ss<<"normal -> general\n";
//                for(unsigned i = 0; i < normalCaseType.parameters().size(); ++i) {
//
//                    if(normalToGeneralMapping.find(i) == normalToGeneralMapping.end()) {
//                        logger.error("invalid index in normal -> general map found");
//                        continue;
//                    }
//
//                    ss<<"("<<i<<"): "<<normalCaseType.parameters()[i].desc()
//                      <<" -> "<<generalCaseType.parameters()[normalToGeneralMapping.at(i)].desc()
//                      <<"\n";
//                }
//                logger.debug(ss.str());
//            }
//
//            auto num_columns = generalCaseType.parameters().size();
//
//            // create function
//            using namespace llvm;
//            using namespace std;
//
//            // create (internal) llvm function to be inlined with all contents
//            auto& context = pipFunc->getContext();
//            auto& env = pip.env();
//
//            // void* userData, int64_t rowNumber, int64_t ExceptionCode, uint8_t* inputBuffer, int64_t inputBufferSize
//            FunctionType *func_type = FunctionType::get(Type::getInt64Ty(context),
//                                                        {Type::getInt8PtrTy(context, 0),
//                                                         Type::getInt64Ty(context),
//                                                         Type::getInt64Ty(context),
//                                                         Type::getInt8PtrTy(context, 0),
//                                                         Type::getInt64Ty(context)}, false);
//            auto func = Function::Create(func_type, Function::ExternalLinkage, name, pipFunc->getParent());
//
//            // set arg names
//            auto args = mapLLVMFunctionArgs(func, {"userData",  "rowNumber", "exceptionCode", "rowBuf", "bufSize",});
//
//            auto body = BasicBlock::Create(context, "body", func);
//            IRBuilder<> builder(body);
//            // decode according to exception type => i.e. decode according to pipeline builder + nullvalue opt!
//            auto encodedCode = args["exceptionCode"];
//            auto dataPtr = args["rowBuf"];
//
//            // extract serialization format and code
//            llvm::Value* ecCode = nullptr;
//            llvm::Value* exFmt = nullptr;
//            env.extract32iFrom64i(builder, encodedCode, &exFmt, &ecCode);
//            ecCode = builder.CreateZExt(ecCode, env.i64Type());
//
//#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
//            env.debugPrint(builder, "slow process functor entered!");
//            env.debugPrint(builder, "exception buffer size is: ", args["bufSize"]);
//            env.debugPrint(builder, "row number: ", args["rowNumber"]);
//            env.debugPrint(builder, "ecCode: ", ecCode);
//            env.debugPrint(builder, "exception storage format: ", exFmt);
//#endif
//
//            auto bbStringFieldDecode = BasicBlock::Create(context, "decodeStrings", func);
//            auto bbNormalCaseDecode = BasicBlock::Create(context, "decodeNormalCase", func);
//            auto bbCommonCaseDecode = BasicBlock::Create(context, "decodeCommonCase", func);
//            auto bbUnknownFormat = BasicBlock::Create(context, "unknownFormat", func);
//
//            // enter appropriate decode block based on format.
//            auto switchInst = builder.CreateSwitch(exFmt, bbUnknownFormat, 3);
//            switchInst->addCase(cast<ConstantInt>(env.i32Const(static_cast<int32_t>(ExceptionSerializationFormat::STRING_CELLS))), bbStringFieldDecode);
//            switchInst->addCase(cast<ConstantInt>(env.i32Const(static_cast<int32_t>(ExceptionSerializationFormat::NORMALCASE))), bbNormalCaseDecode);
//            switchInst->addCase(cast<ConstantInt>(env.i32Const(static_cast<int32_t>(ExceptionSerializationFormat::GENERALCASE))), bbCommonCaseDecode);
//
//
//            // three decode options
//            {
//                // 1.) decode string fields & match with exception case type
//                // i.e. first: num-columns check, second type check
//                // => else exception, i.e. handle in interpreter
//                BasicBlock *bbStringDecodeFailed = BasicBlock::Create(context, "decodeStringsFailed", func);
//                builder.SetInsertPoint(bbStringFieldDecode);
//#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
//                env.debugPrint(builder, "decoding a string type exception");
//#endif
//
//                // decode into noCells, cellsPtr, sizesPtr etc.
//                auto noCells = builder.CreateLoad(builder.CreatePointerCast(dataPtr, env.i64ptrType()));
//
//#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
//                env.debugPrint(builder, "parsed #cells: ", noCells);
//#endif
//                dataPtr = builder.CreateGEP(dataPtr, env.i32Const(sizeof(int64_t)));
//                // heap alloc arrays, could be done on stack as well but whatever
//                auto cellsPtr = builder.CreatePointerCast(
//                        env.malloc(builder, env.i64Const(num_columns * sizeof(uint8_t*))),
//                        env.i8ptrType()->getPointerTo());
//                auto sizesPtr = builder.CreatePointerCast(env.malloc(builder, env.i64Const(num_columns * sizeof(int64_t))),
//                                                          env.i64ptrType());
//                for (unsigned i = 0; i < num_columns; ++i) {
//                    // decode size + offset & store accordingly!
//                    auto info = builder.CreateLoad(builder.CreatePointerCast(dataPtr, env.i64ptrType()));
//                    // truncation yields lower 32 bit (= offset)
//                    Value *offset = builder.CreateTrunc(info, Type::getInt32Ty(context));
//                    // right shift by 32 yields size
//                    Value *size = builder.CreateLShr(info, 32);
//
//                    builder.CreateStore(size, builder.CreateGEP(sizesPtr, env.i32Const(i)));
//                    builder.CreateStore(builder.CreateGEP(dataPtr, offset),
//                                        builder.CreateGEP(cellsPtr, env.i32Const(i)));
//
//#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
//                    env.debugPrint(builder, "cell("  + std::to_string(i) + ") size: ", size);
//                    env.debugPrint(builder, "cell("  + std::to_string(i) + ") offset: ", offset);
//                    env.debugPrint(builder, "cell " + std::to_string(i) + ": ", builder.CreateLoad(builder.CreateGEP(cellsPtr, env.i32Const(i))));
//#endif
//
//                    dataPtr = builder.CreateGEP(dataPtr, env.i32Const(sizeof(int64_t)));
//                }
//
//                // check whether there are any non primitives within generalCaseType
//                bool any_non_primitives_found = false;
//                for(auto type : generalCaseType.parameters()) {
//                    // get rid off opt/option
//                    type = deoptimizedType(type);
//                    type = type.withoutOptionsRecursive();
//                    if(!type.isPrimitiveType() && type != python::Type::STRING && type != python::Type::NULLVALUE)
//                        any_non_primitives_found = true;
//                }
//
//                if(!any_non_primitives_found) {
//                    auto ft = decodeCells(env, builder, generalCaseType, noCells, cellsPtr, sizesPtr, bbStringDecodeFailed,
//                                          null_values);
//
//                    // call pipeline & return its code
//                    auto res = PipelineBuilder::call(builder, pipFunc, *ft, args["userData"], args["rowNumber"]);
//                    auto resultCode = builder.CreateZExtOrTrunc(res.resultCode, env.i64Type());
//                    auto resultOpID = builder.CreateZExtOrTrunc(res.exceptionOperatorID, env.i64Type());
//                    auto resultNumRowsCreated = builder.CreateZExtOrTrunc(res.numProducedRows, env.i64Type());
//
//#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
//                    env.debugPrint(builder, "calling pipeline yielded #rows: ", resultNumRowsCreated);
//#endif
//                    env.freeAll(builder);
//                    builder.CreateRet(resultCode);
//
//                    builder.SetInsertPoint(bbStringDecodeFailed);
//#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
//                    env.debugPrint(builder, "string decode failed");
//#endif
//                    env.freeAll(builder);
//                    builder.CreateRet(ecCode); // original exception code.
//                } else {
//                    // do not generate code.
//                    env.freeAll(builder);
//                    auto resultCode = env.i64Const(ecToI64(ExceptionCode::GENERALCASEVIOLATION));
//                    builder.CreateRet(resultCode);
//                }
//            }
//            // 2.) decode normal case type & upgrade to exception case type, then apply all resolvers & Co
//            {
//                builder.SetInsertPoint(bbNormalCaseDecode);
//#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
//                env.debugPrint(builder, "exception is in normal case format, feed through resolvers&Co");
//#endif
//                // i.e. same code as in pip upgradeType
//                FlattenedTuple ft(&env);
//                ft.init(normalCaseType);
//                ft.deserializationCode(builder, args["rowBuf"]);
//
//                FlattenedTuple tuple(&env); // general case tuple
//                if(!normalCaseAndGeneralCaseCompatible) {
//                    // can null compatibility be achieved? if not jump directly to returning exception forcing it onto interpreter path
//                    assert(pip.inputRowType().isTupleType());
//                    auto col_types = pip.inputRowType().parameters();
//                    auto normal_col_types = normalCaseType.parameters();
//                    // fill in according to mapping normal case type
//                    for(auto keyval : normalToGeneralMapping) {
//                        assert(keyval.first < normal_col_types.size());
//                        assert(keyval.second < col_types.size());
//                        col_types[keyval.second] = normal_col_types[keyval.first];
//                    }
//                    python::Type extendedNormalCaseType = python::Type::makeTupleType(col_types);
//                    if(canAchieveAtLeastNullCompatibility(extendedNormalCaseType, pip.inputRowType())) {
//                        // null-extraction and then call pipeline
//                        BasicBlock *bb_failure = BasicBlock::Create(context, "nullextract_failed", func);
//                        tuple = normalToGeneralTupleWithNullCompatibility(builder,
//                                                                          &env,
//                                                                          ft,
//                                                                          normalCaseType,
//                                                                          pip.inputRowType(),
//                                                                          normalToGeneralMapping,
//                                                                          bb_failure,
//                                                                          policy.allowNumericTypeUnification);
//                        builder.SetInsertPoint(bb_failure);
//                        // all goes onto exception path
//                        // retain original exception, force onto interpreter path
//                        env.freeAll(builder);
//                        builder.CreateRet(ecCode); // original
//                    } else {
//                        // all goes onto exception path
//                        // retain original exception, force onto interpreter path
//                        env.freeAll(builder);
//                        builder.CreateRet(ecCode); // original
//                    }
//                } else {
//                    // upcast to general type!
//                    tuple = normalToGeneralTuple(builder, ft, normalCaseType, pip.inputRowType(), normalToGeneralMapping);
//
//#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
//                    ft.print(builder);
//                    env.debugPrint(builder, "row casted, processing pipeline now!");
//                    tuple.print(builder);
//#endif
//                    auto res = PipelineBuilder::call(builder, pipFunc, tuple, args["userData"], args["rowNumber"]);
//                    auto resultCode = builder.CreateZExtOrTrunc(res.resultCode, env.i64Type());
//                    auto resultOpID = builder.CreateZExtOrTrunc(res.exceptionOperatorID, env.i64Type());
//                    auto resultNumRowsCreated = builder.CreateZExtOrTrunc(res.numProducedRows, env.i64Type());
//                    env.freeAll(builder);
//                    builder.CreateRet(resultCode);
//                }
//            }
//
//
//            // 3.) decode common/exception case type
//            {
//                builder.SetInsertPoint(bbCommonCaseDecode);
//                // only if cases are compatible
//                if(normalCaseAndGeneralCaseCompatible) {
//#ifdef PRINT_EXCEPTION_PROCESSING_DETAILS
//                    env.debugPrint(builder, "exception is in super type format, feed through resolvers&Co");
//#endif
//                    // easiest, no additional steps necessary...
//                    FlattenedTuple tuple(&pip.env());
//                    tuple.init(pip.inputRowType());
//                    tuple.deserializationCode(builder, args["rowBuf"]);
//
//                    // add potentially exception handler function
//                    auto res = PipelineBuilder::call(builder, pipFunc, tuple, args["userData"], args["rowNumber"]);
//                    auto resultCode = builder.CreateZExtOrTrunc(res.resultCode, env.i64Type());
//                    auto resultOpID = builder.CreateZExtOrTrunc(res.exceptionOperatorID, env.i64Type());
//                    auto resultNumRowsCreated = builder.CreateZExtOrTrunc(res.numProducedRows, env.i64Type());
//                    env.freeAll(builder);
//                    builder.CreateRet(resultCode);
//                } else {
//                    // retain original exception, force onto interpreter path
//                    env.freeAll(builder);
//                    builder.CreateRet(ecCode); // original
//                }
//            }
//
//            // unknown format
//            // 4.)
//            builder.SetInsertPoint(bbUnknownFormat);
//            env.debugPrint(builder, "unknown exception format encountered", exFmt);
//            // retain original exception, force onto interpreter path
//            env.freeAll(builder);
//            builder.CreateRet(ecCode); // original
//
//
//            // erase (empty) blocks with no predecessor and successor
//            for(auto it = func->begin(); it != func->end(); ++it) {
//                if(it->empty()) {
//                    auto block = &(*it);
//                    size_t pred_count = predecessorCount(block);
//                    size_t succ_count = successorBlockCount(block);
//                    if(0 == pred_count && 0 == succ_count)
//                        it = it->eraseFromParent();
//                }
//            }
//
//            return func;
//        }
    }
}