//
// Created by leonhard on 10/3/22.
//

#include <physical/codegen/JsonSourceTaskBuilder.h>
#include <experimental/StructDictHelper.h>
#include <experimental/ListHelper.h>
#include <physical/experimental/JSONParseRowGenerator.h>

namespace tuplex {
    namespace codegen {
        JsonSourceTaskBuilder::JsonSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                                                     int64_t input_operator_id,
                                                     const python::Type &normalCaseRowType,
                                                     const python::Type &generalCaseRowType,
                                                     const std::vector<std::string>& normal_case_columns,
                                                     const std::vector<std::string>& general_case_columns,
                                                     bool unwrap_first_level,
                                                     const std::map<int, int>& normalToGeneralMapping,
                                                     const std::string &name,
                                                     bool serializeExceptionsAsGeneralCase): BlockBasedTaskBuilder(env,
                                                                                                                   normalCaseRowType,
                                                                                                                   generalCaseRowType,
                                                                                                                   normalToGeneralMapping,
                                                                                                                   name,
                                                                                                                   serializeExceptionsAsGeneralCase),
                                                     _normalCaseRowType(normalCaseRowType),
                                                     _generalCaseRowType(generalCaseRowType),
                                                     _normal_case_columns(normal_case_columns),
                                                     _general_case_columns(general_case_columns),
                                                     _unwrap_first_level(unwrap_first_level),
                                                     _functionName(name),
                                                     _inputOperatorID(input_operator_id) {

        }

        llvm::Function* JsonSourceTaskBuilder::build(bool terminateEarlyOnFailureCode) {
            using namespace llvm;

            assert(_env);
            auto& ctx = _env->getContext();
            // build a basic block based function (compatible with function signature read_block_f in CodeDefs.h
            auto arg_types = std::vector<llvm::Type*>({ctypeToLLVM<void*>(ctx),
                                                       ctypeToLLVM<uint8_t*>(ctx),
                                                       ctypeToLLVM<int64_t>(ctx),
                                                       ctypeToLLVM<int64_t*>(ctx),
                                                       ctypeToLLVM<int64_t*>(ctx),
                                                       ctypeToLLVM<int8_t>(ctx)});
            FunctionType* FT = FunctionType::get(ctypeToLLVM<int64_t>(ctx), arg_types, false);
            auto func = getOrInsertFunction(_env->getModule().get(), _functionName, FT);

            auto args = mapLLVMFunctionArgs(func, {"userData", "inPtr", "inSize", "outNormalRowCount", "outBadRowCount", "ignoreLastRow"});

            BasicBlock* bbEntry = BasicBlock::Create(env().getContext(), "entry", func);
            IRBuilder<> builder(bbEntry);

            auto bufPtr = args["inPtr"];
            auto bufSize = args["inSize"];

            initVars(builder);

            auto parsed_bytes = generateParseLoop(builder, bufPtr, bufSize, args["userData"], _normal_case_columns,
                                                  _general_case_columns, _unwrap_first_level, terminateEarlyOnFailureCode);

            builder.CreateRet(parsed_bytes);

            return func;
        }

        void JsonSourceTaskBuilder::initVars(llvm::IRBuilder<> &builder) {
            _normalRowCountVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));
            _generalRowCountVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));
            _fallbackRowCountVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));

            _normalMemorySizeVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));
            _generalMemorySizeVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));
            _fallbackMemorySizeVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));

            _rowNumberVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0), "row_no");

            _parsedBytesVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0), "truncated_bytes");

            // init JsonItem row object var
            _row_object_var = _env->CreateFirstBlockVariable(builder, _env->i8nullptr(), "row_object");
        }

        llvm::Value *JsonSourceTaskBuilder::generateParseLoop(llvm::IRBuilder<> &builder,
                                                              llvm::Value *bufPtr,
                                                              llvm::Value *bufSize,
                                                              llvm::Value *userData,
                                                              const std::vector<std::string>& normal_case_columns,
                                                              const std::vector<std::string>& general_case_columns,
                                                              bool unwrap_first_level,
                                                              bool terminateEarlyOnLimitCode) {
            using namespace llvm;
            using namespace std;

            auto& ctx = _env->getContext();

            // this will be a loop
            auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "loop_exit", F);

            // init json parse
            // auto j = JsonParser_init();
            // if(!j)
            //     throw std::runtime_error("failed to initialize parser");
            // JsonParser_open(j, buf, buf_size);
            // while(JsonParser_hasNextRow(j)) {
            //     if(JsonParser_getDocType(j) != JsonParser_objectDocType()) {

            auto parser = initJsonParser(builder);

            // init row number
            _rowNumberVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0), "row_no");
            // _badParseCountVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0), "badparse_count");

            // create single free block
            _freeStart = _freeEnd = BasicBlock::Create(ctx, "free_row_objects", F);

            {
                // create here free obj block.
                llvm::IRBuilder<> b(_freeStart);

                // _env->printValue(b, rowNumber(b), "entered free row objects for row no=");
                // release the row var if required
                json_release_object(*_env, b, _row_object_var);
                _freeEnd = b.GetInsertBlock();
            }

            llvm::Value *rc = openJsonBuf(builder, parser, bufPtr, bufSize);
            llvm::Value *rc_cond = _env->i1neg(builder,
                                              builder.CreateICmpEQ(rc, _env->i64Const(ecToI64(ExceptionCode::SUCCESS))));
            exitMainFunctionWithError(builder, rc_cond, rc);
            builder.CreateBr(bLoopHeader);


            // ---- loop condition ---
            // go from current block to header
            builder.SetInsertPoint(bLoopHeader);
            // condition (i.e. hasNextDoc)
            auto cond = hasNextRow(builder, parser);
            builder.CreateCondBr(cond, bLoopBody, bLoopExit);



            // ---- loop body ----
            // body
            builder.SetInsertPoint(bLoopBody);
            // generate here...
            // _env->debugPrint(builder, "parsed row");

            // check whether it's of object type -> parse then as object (only supported type so far!)
            cond = isDocumentOfObjectType(builder, parser);

            BasicBlock* bbParseAsGeneralCaseRow = BasicBlock::Create(_env->getContext(), "parse_as_general_row", builder.GetInsertBlock()->getParent());
            BasicBlock* bbNormalCaseSuccess = BasicBlock::Create(_env->getContext(), "normal_row_found", builder.GetInsertBlock()->getParent());
            BasicBlock* bbGeneralCaseSuccess = BasicBlock::Create(_env->getContext(), "general_row_found", builder.GetInsertBlock()->getParent());
            BasicBlock* bbFallback = BasicBlock::Create(_env->getContext(), "fallback_row_found", builder.GetInsertBlock()->getParent());

            // // filter promotion? -> happens here.
            // if(hackyPromoteEventFilter) {
            //     BasicBlock* bbKeep = BasicBlock::Create(_env->getContext(), "keep_row_after_promo", builder.GetInsertBlock()->getParent());
            //     BasicBlock* bbSkip = BasicBlock::Create(_env->getContext(), "skip_row_after_promo", builder.GetInsertBlock()->getParent());
            //
            //     auto keep_row = emitHackyFilterPromo(builder, parser, hackyEventName, bbKeep);
            //     builder.CreateCondBr(keep_row, bbKeep, bbSkip);
            //
            //     // create skip part -> count as normal row.
            //     builder.SetInsertPoint(bbSkip);
            //     incVar(builder, _normalRowCountVar);
            //     builder.CreateBr(_freeStart);
            //
            //     builder.SetInsertPoint(bbKeep);
            // }


            // parse here as normal row
            auto normal_case_row = parseRow(builder, _normalCaseRowType, normal_case_columns, unwrap_first_level, parser, bbParseAsGeneralCaseRow); //parseRowAsStructuredDict(builder, _normalCaseRowType, parser, bbParseAsGeneralCaseRow);
            builder.CreateBr(bbNormalCaseSuccess);

            builder.SetInsertPoint(bbParseAsGeneralCaseRow);

            // short cut: if normal case and general case are identical -> go to fallback from here!
            FlattenedTuple general_case_row(_env.get());
            if(_normalCaseRowType == _generalCaseRowType) {
                builder.CreateBr(bbFallback);
            } else {
                general_case_row = parseRow(builder, _generalCaseRowType, general_case_columns, unwrap_first_level, parser, bbFallback);//parseRowAsStructuredDict(builder, _generalCaseRowType, parser, bbFallback);
                builder.CreateBr(bbGeneralCaseSuccess);
            }

            // now create the blocks for each scenario
            {
                // 1. normal case
                builder.SetInsertPoint(bbNormalCaseSuccess);
                _env->debugPrint(builder, "try parsing as normal-case row...");

                // if pipeline exists, call pipeline on normal tuple!
                if(pipeline()) {
                    auto processRowFunc = pipeline()->getFunction();
                    if(!processRowFunc)
                        throw std::runtime_error("invalid function from pipeline builder in JsonSourceTaskBuilder");

                    auto pip_res = PipelineBuilder::call(builder, processRowFunc, normal_case_row, userData, rowNumber(builder), initIntermediate(builder));

                    // create if based on resCode to go into exception block
                    auto ecCode = builder.CreateZExtOrTrunc(pip_res.resultCode, env().i64Type());
                    auto ecOpID = builder.CreateZExtOrTrunc(pip_res.exceptionOperatorID, env().i64Type());
                    auto numRowsCreated = builder.CreateZExtOrTrunc(pip_res.numProducedRows, env().i64Type());

                    if(terminateEarlyOnLimitCode)
                        generateTerminateEarlyOnCode(builder, ecCode, ExceptionCode::OUTPUT_LIMIT_REACHED);
                }

                // serialized size (as is)
                auto normal_size = normal_case_row.getSize(builder);
                incVar(builder, _normalMemorySizeVar, normal_size);
                _env->debugPrint(builder, "got normal-case row!");

                // inc by one
                incVar(builder, _normalRowCountVar);
                builder.CreateBr(_freeStart);
            }

            {
                // 2. general case
                builder.SetInsertPoint(bbGeneralCaseSuccess);

                _env->debugPrint(builder, "try parsing as general-case row...");

                // serialized size (as is)
                auto general_size = general_case_row.getSize(builder);

                serializeAsNormalCaseException(builder, userData, _inputOperatorID, rowNumber(builder), general_case_row);

                // in order to store an exception, need 8 bytes for each: rowNumber, ecCode, opID, eSize + the size of the row
                general_size = builder.CreateAdd(general_size, _env->i64Const(4 * sizeof(int64_t)));
                incVar(builder, _generalMemorySizeVar, general_size);
                _env->debugPrint(builder, "got general-case row!");
                incVar(builder, _generalRowCountVar);
                builder.CreateBr(_freeStart);
            }

            {
                // 3. fallback case
                builder.SetInsertPoint(bbFallback);

                 _env->debugPrint(builder, "try parsing as fallback-case row...");

                // same like in general case, i.e. stored as badStringParse exception
                // -> store here the raw json
                auto Frow = getOrInsertFunction(_env->getModule().get(), "JsonParser_getMallocedRowAndSize", _env->i8ptrType(),
                                                _env->i8ptrType(), _env->i64ptrType());
                auto size_var = _env->CreateFirstBlockAlloca(builder, _env->i64Type());
                auto line = builder.CreateCall(Frow, {parser, size_var});

                // create badParse exception
                // @TODO: throughput can be improved by using a single C++ function for all of this!
                serializeBadParseException(builder, userData, _inputOperatorID, rowNumber(builder), line, builder.CreateLoad(size_var));

                _env->debugPrint(builder, "got fallback-case row!");

                // should whitespace lines be skipped?
                bool skip_whitespace_lines = true;

                if(skip_whitespace_lines) {
                    // note: line could be empty line -> check whether to skip or not!
                    auto Fws = getOrInsertFunction(_env->getModule().get(), "Json_is_whitespace", ctypeToLLVM<bool>(_env->getContext()),
                                                   _env->i8ptrType(), _env->i64Type());
                    auto ws_rc = builder.CreateCall(Fws, {line, builder.CreateLoad(size_var)});
                    auto is_ws = builder.CreateICmpEQ(ws_rc, cbool_const(_env->getContext(), true));


                    // skip whitespace?
                    BasicBlock* bIsNotWhitespace = BasicBlock::Create(ctx, "not_whitespace", builder.GetInsertBlock()->getParent());
                    BasicBlock* bFallbackDone = BasicBlock::Create(ctx, "fallback_done", builder.GetInsertBlock()->getParent());

                    // decrease row count
                    incVar(builder, _rowNumberVar, -1);

                    builder.CreateCondBr(is_ws, bFallbackDone, bIsNotWhitespace);

                    // only inc for non whitespace (would serialize here!)
                    builder.SetInsertPoint(bIsNotWhitespace);
                    incVar(builder, _fallbackMemorySizeVar, builder.CreateLoad(size_var));
                    incVar(builder, _fallbackRowCountVar);
                    incVar(builder, _rowNumberVar); // --> trick, else the white line is counted as row!
                    builder.CreateBr(bFallbackDone);
                    builder.SetInsertPoint(bFallbackDone);
                }
                else {
                    incVar(builder, _fallbackMemorySizeVar, builder.CreateLoad(size_var));
                    incVar(builder, _fallbackRowCountVar);
                }


                // this is ok here, b.c. it's local.
                _env->cfree(builder, line);
                builder.CreateBr(_freeStart);
            }


            {
//                auto s = struct_dict_serialized_memory_size(_env, builder, row_var, row_type);
//                // _env->printValue(builder, s.val, "size of row materialized in bytes is: ");
//
//                // rtmalloc and serialize!
//                auto mem_ptr = _env->malloc(builder, s.val);
//                auto serialization_res = struct_dict_serialize_to_memory(_env, builder, row_var, row_type, mem_ptr);
//                // _env->printValue(builder, serialization_res.size, "realized serialization size is: ");
//
//                // inc total size with serialization size!
//                auto cur_total = builder.CreateLoad(_outTotalSerializationSize);
//                auto new_total = builder.CreateAdd(cur_total, serialization_res.size);
//                builder.CreateStore(new_total, _outTotalSerializationSize);
//
//                // now, load entries to struct type in LLVM
//                // then calculate serialized size and print it.
//                //auto v = struct_dict_load_from_values(_env, builder, row_type, entries);
            }

//            auto bbSchemaMismatch = emitBadParseInputAndMoveToNextRow(builder, parser, _env->i1neg(builder, cond));

            // go to free start
            //builder.CreateBr(_freeStart);

            // free data..
            // --> parsing will generate there free statements per row

            builder.SetInsertPoint(_freeEnd); // free is done -> now move onto next row.
            // go to next row
            moveToNextRow(builder, parser);

            // this will only work when allocating everything local!
            // -> maybe better craft a separate process row function?

            // link back to header
            builder.CreateBr(bLoopHeader);

            // ---- post loop block ----
            // continue in loop exit.
            builder.SetInsertPoint(bLoopExit);

            // before parser is freed, query the parsed bytes!
            auto parsed_bytes = parsedBytes(builder, parser, bufSize);
            builder.CreateStore(parsed_bytes, _parsedBytesVar);

            // free JSON parse (global object)
            freeJsonParse(builder, parser);

            return builder.CreateLoad(_parsedBytesVar);
        }


        llvm::Value *
        JsonSourceTaskBuilder::parsedBytes(llvm::IRBuilder<> &builder, llvm::Value *parser, llvm::Value *buf_size) {
            using namespace llvm;
            // call func
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_TruncatedBytes", _env->i64Type(), _env->i8ptrType());
            assert(parser && parser->getType() == _env->i8ptrType());

            auto truncated_bytes = builder.CreateCall(F, {parser});
            return builder.CreateSub(buf_size, truncated_bytes);
        }

        // json helper funcs
        llvm::Value *JsonSourceTaskBuilder::initJsonParser(llvm::IRBuilder<> &builder) {

            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_Init", _env->i8ptrType());

            auto j = builder.CreateCall(F, {});
            auto is_null = builder.CreateICmpEQ(j, _env->i8nullptr());
            exitMainFunctionWithError(builder, is_null, _env->i64Const(ecToI64(ExceptionCode::NULLERROR)));
            return j;
        }

        llvm::Value *JsonSourceTaskBuilder::openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf,
                                                     llvm::Value *buf_size) {
            assert(j);
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_open", _env->i64Type(), _env->i8ptrType(),
                                         _env->i8ptrType(), _env->i64Type());
            return builder.CreateCall(F, {j, buf, buf_size});
        }

        void JsonSourceTaskBuilder::freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env->getContext();
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_Free", llvm::Type::getVoidTy(ctx),
                                         _env->i8ptrType());
            builder.CreateCall(F, j);
        }

        void JsonSourceTaskBuilder::exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition,
                                                              llvm::Value *exitCode) {
            using namespace llvm;
            auto &ctx = _env->getContext();
            auto F = builder.GetInsertBlock()->getParent();

            assert(exitCondition->getType() == _env->i1Type());
            assert(exitCode->getType() == _env->i64Type());

            // branch and exit
            BasicBlock *bbExit = BasicBlock::Create(ctx, "exit_with_error", F);
            BasicBlock *bbContinue = BasicBlock::Create(ctx, "no_error", F);
            builder.CreateCondBr(exitCondition, bbExit, bbContinue);
            builder.SetInsertPoint(bbExit);
            builder.CreateRet(builder.CreateMul(_env->i64Const(-1), exitCode)); // truncated bytes, but use here negative code.
            builder.SetInsertPoint(bbContinue);
        }

        llvm::Value *JsonSourceTaskBuilder::hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env->getContext();
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_hasNextRow", ctypeToLLVM<bool>(ctx),
                                         _env->i8ptrType());

            auto v = builder.CreateCall(F, {j});
            return builder.CreateICmpEQ(v, llvm::ConstantInt::get(
                    llvm::Type::getIntNTy(ctx, ctypeToLLVM<bool>(ctx)->getIntegerBitWidth()), 1));
        }

        void JsonSourceTaskBuilder::moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            // move
            using namespace llvm;
            auto &ctx = _env->getContext();
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_moveToNextRow", ctypeToLLVM<bool>(ctx),
                                         _env->i8ptrType());
            builder.CreateCall(F, {j});

            // update row number (inc +1)
            auto row_no = rowNumber(builder);
            builder.CreateStore(builder.CreateAdd(row_no, _env->i64Const(1)), _rowNumberVar);

            // @TODO: free everything so far??
            _env->freeAll(builder); // -> call rtfree!
        }

        llvm::Value *
        JsonSourceTaskBuilder::parseRowAsStructuredDict(llvm::IRBuilder<> &builder, const python::Type &dict_type,
                                                        llvm::Value *j, llvm::BasicBlock *bbSchemaMismatch) {
            assert(j);
            using namespace llvm;
            auto &ctx = _env->getContext();

            // get initial object
            // => this is from parser
            auto Fgetobj = getOrInsertFunction(_env->getModule().get(), "JsonParser_getObject", _env->i64Type(),
                                               _env->i8ptrType(), _env->i8ptrType()->getPointerTo(0));

            auto obj_var = _row_object_var;

            // release and store nullptr
            //builder.CreateStore(_env->i8nullptr(), obj_var);
            json_release_object(*_env, builder, obj_var);

            builder.CreateCall(Fgetobj, {j, obj_var});

            // don't forget to free everything...
            // alloc variable
            auto struct_dict_type = create_structured_dict_type(*_env.get(), dict_type);
            auto row_var = _env->CreateFirstBlockAlloca(builder, struct_dict_type);
            struct_dict_mem_zero(*_env.get(), builder, row_var, dict_type); // !!! important !!!

            // // create new block where all objects allocated for gen are freed.
            // BasicBlock* bParseFree = BasicBlock::Create(ctx, "parse_free", builder.GetInsertBlock()->getParent());

            // create dict parser and store to row_var
            JSONParseRowGenerator gen(*_env.get(), dict_type, bbSchemaMismatch);
            gen.parseToVariable(builder, builder.CreateLoad(obj_var), row_var);
//            // update free end
//            auto bParseFreeStart = bParseFree;
//            bParseFree = gen.generateFreeAllVars(bParseFree);
//
//            // jump now to parse free
//            builder.CreateBr(bParseFreeStart);
//            builder.SetInsertPoint(bParseFree);

            // create new block (post - parse)
            BasicBlock *bPostParse = BasicBlock::Create(ctx, "post_parse", builder.GetInsertBlock()->getParent());

            builder.CreateBr(bPostParse);
            builder.SetInsertPoint(bPostParse);
            _env->debugPrint(builder, "free done.");

            // free obj_var...
            //json_freeObject(*_env.get(), builder, builder.CreateLoad(obj_var));
            json_release_object(*_env, builder, obj_var);
#ifndef NDEBUG
            builder.CreateStore(_env->i8nullptr(), obj_var);
#endif
            return row_var;
        }

        llvm::Value *JsonSourceTaskBuilder::isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j) {
            using namespace llvm;
            auto &ctx = _env->getContext();
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_getDocType", _env->i64Type(),
                                         _env->i8ptrType());
            auto call_res = builder.CreateCall(F, j);
            auto cond = builder.CreateICmpEQ(call_res, _env->i64Const(JsonParser_objectDocType()));
            return cond;
        }

        FlattenedTuple JsonSourceTaskBuilder::parseRow(llvm::IRBuilder<> &builder, const python::Type &row_type,
                                                       const std::vector<std::string>& columns,
                                                       bool unwrap_first_level, llvm::Value *parser,
                                                       llvm::BasicBlock *bbSchemaMismatch) {
            auto& logger = Logger::instance().logger("codegen");
            assert(row_type.isTupleType());

            FlattenedTuple ft(_env.get());
            ft.init(row_type);

            // should first level be unwrapped or not?
            if(unwrap_first_level) {
                // -> need to have columns info
                assert(columns.size() >= row_type.parameters().size());

                if(columns.size() < row_type.parameters().size()) {
                    builder.CreateBr(bbSchemaMismatch);
                    // maybe need to initialize with dummies?
                    return ft;
                }

                // parse using a struct type composed of columns and the data!
                std::vector<python::StructEntry> entries;
                auto num_entries = std::min(columns.size(), row_type.parameters().size());

#ifndef NDEBUG
                // loading entries:
                {
                    std::stringstream ss;
                    for(unsigned i = 0; i < num_entries; ++i) {
                        ss<<i<<": "<<columns[i]<<" -> "<<row_type.parameters()[i].desc()<<std::endl;
                    }
                    logger.debug("columns:\n" + ss.str());
                }
#endif

                for(unsigned i = 0; i < num_entries; ++i) {
                    python::StructEntry entry;
                    entry.keyType = python::Type::STRING;
                    entry.key = escape_to_python_str(columns[i]);
                    entry.valueType = row_type.parameters()[i];
                    entries.push_back(entry);
                }
                auto dict_type = python::Type::makeStructuredDictType(entries);

                // parse dictionary
                auto dict = parseRowAsStructuredDict(builder, dict_type, parser, bbSchemaMismatch);

                // fetch columns from dict and assign to tuple!
                for(int i = 0; i < num_entries; ++i) {
                    SerializableValue value;

                    // fetch value from dict!
                    value = struct_dict_get_or_except(*_env.get(), builder, dict_type, escape_to_python_str(columns[i]),
                                                      python::Type::STRING, dict, bbSchemaMismatch);

                    ft.set(builder, {i}, value.val, value.size, value.is_null);
                }
            } else {
                assert(row_type.parameters().size() == 1);
                auto dict_type = row_type.parameters()[0];
                assert(dict_type.isStructuredDictionaryType()); // <-- this should be true, or an alternative parsing strategy devised...
                if(!dict_type.isStructuredDictionaryType())
                    throw std::runtime_error("parsing JSON files with type " + dict_type.desc() + " not yet implemented.");

                _env->debugPrint(builder, "starting to parse row...");
                // parse struct dict
                auto s = parseRowAsStructuredDict(builder, dict_type, parser, bbSchemaMismatch);
                _env->debugPrint(builder, "row parsed.");

                // assign to tuple (for further processing)
                ft.setElement(builder, 0, s, nullptr, nullptr);
            }

            return ft;
        }

        void JsonSourceTaskBuilder::serializeAsNormalCaseException(llvm::IRBuilder<> &builder, llvm::Value *userData,
                                                                   int64_t operatorID, llvm::Value *row_no,
                                                                   const tuplex::codegen::FlattenedTuple &general_case_row) {
            // checks
            assert(row_no);
            assert(row_no->getType() == _env->i64Type());

            // this is a regular exception -> i.e. cf. deserializeExceptionFromMemory for mem layout
            auto mem = general_case_row.serializeToMemory(builder);
            auto buf = mem.val;
            auto buf_size = mem.size;

            _env->printValue(builder, rowNumber(builder), "row number: ");
            _env->printValue(builder, buf_size, "emitting normal-case violation in general case format of size: ");

            // buf is now fully serialized. => call exception handler from pipeline.
            auto handler_name = exception_handler();
            if(!hasExceptionHandler() || handler_name.empty())
                throw std::runtime_error("invalid, no handler specified");

            auto eh_func = exception_handler_prototype(_env->getContext(), _env->getModule().get(), handler_name);
            llvm::Value *ecCode = _env->i64Const(ecToI64(ExceptionCode::NORMALCASEVIOLATION));
            llvm::Value *ecOpID = _env->i64Const(operatorID);
            builder.CreateCall(eh_func, {userData, ecCode, ecOpID, row_no, buf, buf_size});
        }

        void JsonSourceTaskBuilder::serializeBadParseException(llvm::IRBuilder<> &builder,
                                                               llvm::Value* userData,
                                                               int64_t operatorID,
                                                               llvm::Value *row_no,
                                                               llvm::Value *str,
                                                               llvm::Value *str_size) {

            // checks
            assert(row_no && str && str_size);
            assert(row_no->getType() == _env->i64Type());
            assert(str->getType() == _env->i8ptrType());
            assert(str_size->getType() == _env->i64Type());

            // memory layout for this is like in serializeParseException
            // yet for JSON there's only a single cell
            // buf_size = sizeof(int64_t) + numCells * sizeof(int64_t) + SUM_i sizes[i]
            // --> here:
            llvm::Value* buf_size = builder.CreateAdd(_env->i64Const(sizeof(int64_t) * 2), str_size);

            llvm::Value* buf = _env->malloc(builder, buf_size);
            llvm::Value* ptr = buf;

            // write first number of cells to serialize. ( = 1)
            builder.CreateStore(_env->i64Const(1), builder.CreatePointerCast(ptr, _env->i64ptrType()));
            ptr = builder.CreateGEP(ptr, _env->i64Const(sizeof(int64_t)));
            // write str info (size + offset)
            size_t offset = sizeof(int64_t); // trivial offset
            auto info = pack_offset_and_size(builder, _env->i64Const(offset), str_size);
            builder.CreateStore(info, builder.CreatePointerCast(ptr, _env->i64ptrType()));
            ptr = builder.CreateGEP(ptr, _env->i64Const(sizeof(int64_t)));
            // memcpy string
            builder.CreateMemCpy(ptr, 0, str, 0, str_size);

            // buf is now fully serialized. => call exception handler from pipeline.
            auto handler_name = exception_handler();
            if(!hasExceptionHandler() || handler_name.empty())
                throw std::runtime_error("invalid, no handler specified");

            auto eh_func = exception_handler_prototype(_env->getContext(), _env->getModule().get(), handler_name);
            llvm::Value *ecCode = _env->i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT));
            llvm::Value *ecOpID = _env->i64Const(operatorID);
            builder.CreateCall(eh_func, {userData, ecCode, ecOpID, row_no, buf, buf_size});
        }
    }
}