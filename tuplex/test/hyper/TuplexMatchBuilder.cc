//
// Created by leonhard on 9/26/22.
//

#include "TuplexMatchBuilder.h"

namespace tuplex {
    namespace codegen {

        llvm::Value* TuplexMatchBuilder::parseRowAsStructuredDict(llvm::IRBuilder<> &builder, const python::Type& row_type, llvm::Value *j,
                                                          llvm::BasicBlock *bbSchemaMismatch) {
            assert(j);
            using namespace llvm;
            auto &ctx = _env.getContext();

            // get initial object
            // => this is from parser
            auto Fgetobj = getOrInsertFunction(_env.getModule().get(), "JsonParser_getObject", _env.i64Type(),
                                               _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));

            auto obj_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr(), "row_object");
            builder.CreateCall(Fgetobj, {j, obj_var});

            // don't forget to free everything...
            // alloc variable
            auto struct_dict_type = create_structured_dict_type(_env, row_type);
            auto row_var = _env.CreateFirstBlockAlloca(builder, struct_dict_type);
            struct_dict_mem_zero(_env, builder, row_var, row_type); // !!! important !!!


            // create dict parser and store to row_var
            JSONParseRowGenerator gen(_env, row_type,  _freeEnd, bbSchemaMismatch);
            gen.parseToVariable(builder, builder.CreateLoad(obj_var), row_var);

            // free obj_var...
            json_freeObject(_env, builder, builder.CreateLoad(obj_var));
#ifndef NDEBUG
            builder.CreateStore(_env.i8nullptr(), obj_var);
#endif
            return row_var;
        }

        llvm::Value *TuplexMatchBuilder::isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j) {
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_getDocType", _env.i64Type(),
                                         _env.i8ptrType());
            auto call_res = builder.CreateCall(F, j);
            auto cond = builder.CreateICmpEQ(call_res, _env.i64Const(JsonParser_objectDocType()));
            return cond;
        }

        llvm::Value *TuplexMatchBuilder::hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_hasNextRow", ctypeToLLVM<bool>(ctx),
                                         _env.i8ptrType());

            auto v = builder.CreateCall(F, {j});
            return builder.CreateICmpEQ(v, llvm::ConstantInt::get(
                    llvm::Type::getIntNTy(ctx, ctypeToLLVM<bool>(ctx)->getIntegerBitWidth()), 1));
        }

        void TuplexMatchBuilder::moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            // move
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_moveToNextRow", ctypeToLLVM<bool>(ctx),
                                         _env.i8ptrType());
            builder.CreateCall(F, {j});

            // update row number (inc +1)
            auto row_no = rowNumber(builder);
            builder.CreateStore(builder.CreateAdd(row_no, _env.i64Const(1)), _rowNumberVar);

            // @TODO: free everything so far??
            _env.freeAll(builder); // -> call rtfree!
        }

        void TuplexMatchBuilder::exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition,
                                                              llvm::Value *exitCode) {
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            assert(exitCondition->getType() == _env.i1Type());
            assert(exitCode->getType() == _env.i64Type());

            // branch and exit
            BasicBlock *bbExit = BasicBlock::Create(ctx, "exit_with_error", F);
            BasicBlock *bbContinue = BasicBlock::Create(ctx, "no_error", F);
            builder.CreateCondBr(exitCondition, bbExit, bbContinue);
            builder.SetInsertPoint(bbExit);
            builder.CreateRet(exitCode);
            builder.SetInsertPoint(bbContinue);
        }

        llvm::Value *TuplexMatchBuilder::initJsonParser(llvm::IRBuilder<> &builder) {

            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_Init", _env.i8ptrType());

            auto j = builder.CreateCall(F, {});
            auto is_null = builder.CreateICmpEQ(j, _env.i8nullptr());
            exitMainFunctionWithError(builder, is_null, _env.i64Const(ecToI64(ExceptionCode::NULLERROR)));
            return j;
        }

        llvm::Value *TuplexMatchBuilder::openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf,
                                                        llvm::Value *buf_size) {
            assert(j);
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_open", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i64Type());
            return builder.CreateCall(F, {j, buf, buf_size});
        }

        void TuplexMatchBuilder::freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_Free", llvm::Type::getVoidTy(ctx),
                                         _env.i8ptrType());
            builder.CreateCall(F, j);
        }

        void TuplexMatchBuilder::generateParseLoop(llvm::IRBuilder<> &builder, llvm::Value *bufPtr,
                                                      llvm::Value *bufSize) {
            using namespace llvm;
            auto &ctx = _env.getContext();

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
            _rowNumberVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "row_no");
            // _badParseCountVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "badparse_count");

            // create single free block
            _freeStart = _freeEnd = BasicBlock::Create(ctx, "free_row_objects", F);

#ifndef NDEBUG
            {
                // debug: create an info statement for free block
                llvm::IRBuilder<> b(_freeStart);
                // _env.printValue(b, rowNumber(b), "entered free row objects for row no=");
            }
#endif
            llvm::Value *rc = openJsonBuf(builder, parser, bufPtr, bufSize);
            llvm::Value *rc_cond = _env.i1neg(builder,
                                              builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS))));
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
            // _env.debugPrint(builder, "parsed row");

            // check whether it's of object type -> parse then as object (only supported type so far!)
            cond = isDocumentOfObjectType(builder, parser);

            BasicBlock* bbParseAsGeneralCaseRow = BasicBlock::Create(_env.getContext(), "parse_as_general_row", builder.GetInsertBlock()->getParent());
            BasicBlock* bbNormalCaseSuccess = BasicBlock::Create(_env.getContext(), "normal_row_found", builder.GetInsertBlock()->getParent());
            BasicBlock* bbGeneralCaseSuccess = BasicBlock::Create(_env.getContext(), "general_row_found", builder.GetInsertBlock()->getParent());
            BasicBlock* bbFallback = BasicBlock::Create(_env.getContext(), "fallback_row_found", builder.GetInsertBlock()->getParent());
            // print out structure -> this is the parse
            auto normal_case_row = parseRowAsStructuredDict(builder, _normalCaseRowType, parser, bbParseAsGeneralCaseRow);
            builder.CreateBr(bbNormalCaseSuccess);

            builder.SetInsertPoint(bbParseAsGeneralCaseRow);
            auto general_case_row = parseRowAsStructuredDict(builder, _generalCaseRowType, parser, bbFallback);
            builder.CreateBr(bbGeneralCaseSuccess);

            // now create the blocks for each scenario
            {
                // 1. normal case
                builder.SetInsertPoint(bbNormalCaseSuccess);

                // serialized size (as is)
                auto s = struct_dict_type_serialized_memory_size(_env, builder, normal_case_row, _normalCaseRowType);
                incVar(builder, _normalMemorySizeVar, s.val);

                // inc by one
                incVar(builder, _normalRowCountVar);
                builder.CreateBr(_freeStart);
            }

            {
                // 2. general case
                builder.SetInsertPoint(bbGeneralCaseSuccess);

                // serialized size (as is)
                auto s = struct_dict_type_serialized_memory_size(_env, builder, general_case_row, _generalCaseRowType);
                auto general_size = s.val;

                // in order to store an exception, need 8 bytes for each: rowNumber, ecCode, opID, eSize + the size of the row
                general_size = builder.CreateAdd(general_size, _env.i64Const(4 * sizeof(int64_t)));
                incVar(builder, _generalMemorySizeVar, general_size);

                incVar(builder, _generalRowCountVar);
                builder.CreateBr(_freeStart);
            }

            {
                // 3. fallback case
                builder.SetInsertPoint(bbFallback);

                // same like in general case, i.e. stored as badStringParse exception
                // -> store here the raw json
                auto Frow = getOrInsertFunction(_env.getModule().get(), "JsonParser_getMallocedRowAndSize", _env.i8ptrType(),
                                                _env.i8ptrType(), _env.i64ptrType());
                auto size_var = _env.CreateFirstBlockAlloca(builder, _env.i64Type());
                auto line = builder.CreateCall(Frow, {parser, size_var});

                // should whitespace lines be skipped?
                bool skip_whitespace_lines = true;

                if(skip_whitespace_lines) {
                    // note: line could be empty line -> check whether to skip or not!
                    auto Fws = getOrInsertFunction(_env.getModule().get(), "Json_is_whitespace", ctypeToLLVM<bool>(_env.getContext()),
                                                   _env.i8ptrType(), _env.i64Type());
                    auto ws_rc = builder.CreateCall(Fws, {line, builder.CreateLoad(size_var)});
                    auto is_ws = builder.CreateICmpEQ(ws_rc, cbool_const(_env.getContext(), true));


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
                _env.cfree(builder, line);
                builder.CreateBr(_freeStart);
            }


            {
//                auto s = struct_dict_type_serialized_memory_size(_env, builder, row_var, row_type);
//                // _env.printValue(builder, s.val, "size of row materialized in bytes is: ");
//
//                // rtmalloc and serialize!
//                auto mem_ptr = _env.malloc(builder, s.val);
//                auto serialization_res = struct_dict_serialize_to_memory(_env, builder, row_var, row_type, mem_ptr);
//                // _env.printValue(builder, serialization_res.size, "realized serialization size is: ");
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

//            auto bbSchemaMismatch = emitBadParseInputAndMoveToNextRow(builder, parser, _env.i1neg(builder, cond));

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

            // free JSON parse (global object)
            freeJsonParse(builder, parser);

            // _env.printValue(builder, rowNumber(builder), "parsed rows: ");
            // _env.printValue(builder, builder.CreateLoad(_badParseCountVar),
            //                 "thereof bad parse rows (schema mismatch): ");
            //
            // // store in vars
            // builder.CreateStore(rowNumber(builder), _outTotalRowsVar);
            // builder.CreateStore(builder.CreateLoad(_badParseCountVar), _outTotalBadRowsVar);
        }


        void TuplexMatchBuilder::writeOutput(llvm::IRBuilder<> &builder, llvm::Value *var, llvm::Value *val) {
            using namespace llvm;

            assert(var && val);
            assert(var->getType() == val->getType()->getPointerTo());

            // if var != nullptr:
            // *var = val
            auto& ctx = _env.getContext();
            BasicBlock *bDone = BasicBlock::Create(ctx, "output_done", builder.GetInsertBlock()->getParent());
            BasicBlock *bWrite = BasicBlock::Create(ctx, "output_write", builder.GetInsertBlock()->getParent());

            auto cond = builder.CreateICmpNE(var, _env.nullConstant(var->getType()));
            builder.CreateCondBr(cond, bWrite, bDone);

            builder.SetInsertPoint(bWrite);
            builder.CreateStore(val, var);
            builder.CreateBr(bDone);

            builder.SetInsertPoint(bDone);
        }

        void TuplexMatchBuilder::build() {
            using namespace llvm;
            auto &ctx = _env.getContext();

            // create main function (takes buffer and buf_size, later take the other tuplex stuff)
            FunctionType *FT = FunctionType::get(ctypeToLLVM<int64_t>(ctx),
                                                 {ctypeToLLVM<char *>(ctx),
                                                  ctypeToLLVM<int64_t>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),}, false);

            Function *F = Function::Create(FT, llvm::GlobalValue::ExternalLinkage, _functionName,
                                           *_env.getModule().get());
            auto m = mapLLVMFunctionArgs(F, {"buf", "buf_size", "out_total_rows",
                                             "out_normal_rows", "out_general_rows", "out_fallback_rows",
                                             "out_normal_size", "out_general_size", "out_fallback_size"});

            auto bbEntry = BasicBlock::Create(ctx, "entry", F);
            IRBuilder<> builder(bbEntry);

            // allocate variables
            // _outTotalRowsVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            // _outTotalBadRowsVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            // _outTotalSerializationSize = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));

            _normalRowCountVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            _generalRowCountVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            _fallbackRowCountVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));

            _normalMemorySizeVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            _generalMemorySizeVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            _fallbackMemorySizeVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));

            _rowNumberVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "row_no");

             // dummy parse, simply print type and value with type checking.
             generateParseLoop(builder, m["buf"], m["buf_size"]);

            // i.e. parse first as normal row -> fail, try to parse as general row -> fail, fallback.

            writeOutput(builder, m["out_total_rows"], rowNumber(builder));
            writeOutput(builder, m["out_normal_rows"], builder.CreateLoad(_normalRowCountVar));
            writeOutput(builder, m["out_general_rows"], builder.CreateLoad(_generalRowCountVar));
            writeOutput(builder, m["out_fallback_rows"], builder.CreateLoad(_fallbackRowCountVar));

            writeOutput(builder, m["out_normal_size"], builder.CreateLoad(_normalMemorySizeVar));
            writeOutput(builder, m["out_general_size"], builder.CreateLoad(_generalMemorySizeVar));
            writeOutput(builder, m["out_fallback_size"], builder.CreateLoad(_fallbackMemorySizeVar));

            builder.CreateRet(_env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
        }
    }
}