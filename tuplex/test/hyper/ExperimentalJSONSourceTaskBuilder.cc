//
// Created by leonhard on 9/26/22.
//

#include "ExperimentalJSONSourceTaskBuilder.h"

namespace tuplex {
    namespace codegen {
        void ExperimentalJSONSourceTaskBuilder::checkRC(llvm::IRBuilder<> &builder, const std::string &key, llvm::Value *rc) {
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            BasicBlock *bbPrint = BasicBlock::Create(ctx, key + "_present", F);
            BasicBlock *bbNext = BasicBlock::Create(ctx, key + "_done", F);

            // check what the rc values are
            auto bad_value = builder.CreateICmpNE(rc, _env.i64Const(0));
            builder.CreateCondBr(bad_value, bbPrint, bbNext);
            builder.SetInsertPoint(bbPrint);

            // _env.printValue(builder, rc, "rc for key=" + key + " is: ");

            builder.CreateBr(bbNext);
            builder.SetInsertPoint(bbNext);
        }

        void ExperimentalJSONSourceTaskBuilder::printValueInfo(llvm::IRBuilder<> &builder,
                                                               const std::string &key,
                                                               const python::Type &valueType,
                                                               llvm::Value *keyPresent,
                                                               const tuplex::codegen::SerializableValue &value) {

            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            BasicBlock *bbPresent = BasicBlock::Create(ctx, key + "_present", F);
            BasicBlock *bbNotNull = BasicBlock::Create(ctx, key + "_notnull", F);
            BasicBlock *bbNext = BasicBlock::Create(ctx, key + "_done", F);

            builder.CreateCondBr(keyPresent, bbPresent, bbNext);

            builder.SetInsertPoint(bbPresent);
            _env.debugPrint(builder, "key " + key + " is present");
            auto is_null = value.is_null ? value.is_null : _env.i1Const(false);
            builder.CreateCondBr(is_null, bbNext, bbNotNull);

            builder.SetInsertPoint(bbNotNull);
            if(value.val && !valueType.isStructuredDictionaryType())
                _env.printValue(builder, value.val, "decoded key=" + key + " as " + valueType.desc());
            builder.CreateBr(bbNext);

            builder.SetInsertPoint(bbNext);
        }



        void ExperimentalJSONSourceTaskBuilder::parseAndPrintStructuredDictFromObject(llvm::IRBuilder<> &builder, llvm::Value *j,
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
            auto struct_dict_type = create_structured_dict_type(_env, _rowType);
            auto row_var = _env.CreateFirstBlockAlloca(builder, struct_dict_type);
            struct_dict_mem_zero(_env, builder, row_var, _rowType); // !!! important !!!


            // create dict parser and store to row_var
            JSONParseRowGenerator gen(_env, _rowType, bbSchemaMismatch);
            gen.parseToVariable(builder, builder.CreateLoad(obj_var), row_var);

            auto s = struct_dict_serialized_memory_size(_env, builder, row_var, _rowType);
            // _env.printValue(builder, s.val, "size of row materialized in bytes is: ");

            // rtmalloc and serialize!
            auto mem_ptr = _env.malloc(builder, s.val);
            auto serialization_res = struct_dict_serialize_to_memory(_env, builder, row_var, _rowType, mem_ptr);
            // _env.printValue(builder, serialization_res.size, "realized serialization size is: ");

            // inc total size with serialization size!
            auto cur_total = builder.CreateLoad(_outTotalSerializationSize);
            auto new_total = builder.CreateAdd(cur_total, serialization_res.size);
            builder.CreateStore(new_total, _outTotalSerializationSize);

            // now, load entries to struct type in LLVM
            // then calculate serialized size and print it.
            //auto v = struct_dict_load_from_values(_env, builder, _rowType, entries);


            //// => call with row type
            //parseAndPrint(builder, builder.CreateLoad(obj_var), "", true, _rowType, true, bbSchemaMismatch);

            // free obj_var...
            json_freeObject(_env, builder, builder.CreateLoad(obj_var));
#ifndef NDEBUG
            builder.CreateStore(_env.i8nullptr(), obj_var);
#endif

            // build schema mismatch block.


        }

        llvm::Value *ExperimentalJSONSourceTaskBuilder::isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j) {
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_getDocType", _env.i64Type(),
                                         _env.i8ptrType());
            auto call_res = builder.CreateCall(F, j);
            auto cond = builder.CreateICmpEQ(call_res, _env.i64Const(JsonParser_objectDocType()));
            return cond;
        }

        llvm::BasicBlock *
        ExperimentalJSONSourceTaskBuilder::emitBadParseInputAndMoveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j,
                                                                             llvm::Value *condition) {
            using namespace llvm;
            auto &ctx = _env.getContext();

            auto F = builder.GetInsertBlock()->getParent();

            BasicBlock *bbOK = BasicBlock::Create(ctx, "ok", F);
            BasicBlock *bbEmitBadParse = BasicBlock::Create(ctx, "bad_parse", F);
            builder.CreateCondBr(condition, bbEmitBadParse, bbOK);

            // ---- bad parse blocks ----
            //            auto line = JsonParser_getMallocedRow(j);
            //            free(line);
            // --> i.e. call exception handler from here...
            builder.SetInsertPoint(bbEmitBadParse);
            auto Frow = getOrInsertFunction(_env.getModule().get(), "JsonParser_getMallocedRow", _env.i8ptrType(),
                                            _env.i8ptrType());
            auto line = builder.CreateCall(Frow, j);

            // // simply print (later call with error)
            // _env.printValue(builder, rowNumber(builder), "bad parse encountered for row number: ");

            // inc value
            auto count = builder.CreateLoad(_badParseCountVar);
            builder.CreateStore(builder.CreateAdd(count, _env.i64Const(1)), _badParseCountVar);

            //_env.printValue(builder, line, "bad-parse for row: ");
            // this is ok here, b.c. it's local.
            _env.cfree(builder, line);

            // go to free block -> that will then take care of moving back to header.
            builder.CreateBr(_freeStart);

            // ok block
            builder.SetInsertPoint(bbOK);
            return bbEmitBadParse;
        }

        llvm::Value *ExperimentalJSONSourceTaskBuilder::hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_hasNextRow", ctypeToLLVM<bool>(ctx),
                                         _env.i8ptrType());

            auto v = builder.CreateCall(F, {j});
            return builder.CreateICmpEQ(v, llvm::ConstantInt::get(
                    llvm::Type::getIntNTy(ctx, ctypeToLLVM<bool>(ctx)->getIntegerBitWidth()), 1));
        }

        void ExperimentalJSONSourceTaskBuilder::moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
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

        void ExperimentalJSONSourceTaskBuilder::exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition,
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

        llvm::Value *ExperimentalJSONSourceTaskBuilder::initJsonParser(llvm::IRBuilder<> &builder) {

            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_Init", _env.i8ptrType());

            auto j = builder.CreateCall(F, {});
            auto is_null = builder.CreateICmpEQ(j, _env.i8nullptr());
            exitMainFunctionWithError(builder, is_null, _env.i64Const(ecToI64(ExceptionCode::NULLERROR)));
            return j;
        }

        llvm::Value *ExperimentalJSONSourceTaskBuilder::openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf,
                                                                    llvm::Value *buf_size) {
            assert(j);
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_open", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i64Type());
            return builder.CreateCall(F, {j, buf, buf_size});
        }

        void ExperimentalJSONSourceTaskBuilder::freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_Free", llvm::Type::getVoidTy(ctx),
                                         _env.i8ptrType());
            builder.CreateCall(F, j);
        }

        void ExperimentalJSONSourceTaskBuilder::generateParseLoop(llvm::IRBuilder<> &builder, llvm::Value *bufPtr,
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
            _badParseCountVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "badparse_count");

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
            auto bbSchemaMismatch = emitBadParseInputAndMoveToNextRow(builder, parser, _env.i1neg(builder, cond));

            // print out structure -> this is the parse
            parseAndPrintStructuredDictFromObject(builder, parser, bbSchemaMismatch);

            // go to free start
            builder.CreateBr(_freeStart);

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

            _env.printValue(builder, rowNumber(builder), "parsed rows: ");
            _env.printValue(builder, builder.CreateLoad(_badParseCountVar),
                            "thereof bad parse rows (schema mismatch): ");

            // store in vars
            builder.CreateStore(rowNumber(builder), _outTotalRowsVar);
            builder.CreateStore(builder.CreateLoad(_badParseCountVar), _outTotalBadRowsVar);
        }


        void ExperimentalJSONSourceTaskBuilder::writeOutput(llvm::IRBuilder<> &builder, llvm::Value *var, llvm::Value *val) {
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

        void ExperimentalJSONSourceTaskBuilder::build() {
            using namespace llvm;
            auto &ctx = _env.getContext();

            // create main function (takes buffer and buf_size, later take the other tuplex stuff)
            FunctionType *FT = FunctionType::get(ctypeToLLVM<int64_t>(ctx),
                                                 {ctypeToLLVM<char *>(ctx),
                                                  ctypeToLLVM<int64_t>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),}, false);

            Function *F = Function::Create(FT, llvm::GlobalValue::ExternalLinkage, _functionName,
                                           *_env.getModule().get());
            auto m = mapLLVMFunctionArgs(F, {"buf", "buf_size", "out_total_rows", "out_bad_parse_rows", "out_total_size"});

            auto bbEntry = BasicBlock::Create(ctx, "entry", F);
            IRBuilder<> builder(bbEntry);

            // allocate variables
            _outTotalRowsVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            _outTotalBadRowsVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            _outTotalSerializationSize = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));

            // dummy parse, simply print type and value with type checking.
            generateParseLoop(builder, m["buf"], m["buf_size"]);

            writeOutput(builder, m["out_total_rows"], builder.CreateLoad(_outTotalRowsVar));
            writeOutput(builder, m["out_bad_parse_rows"], builder.CreateLoad(_outTotalBadRowsVar));
            writeOutput(builder, m["out_total_size"], builder.CreateLoad(_outTotalSerializationSize));

            builder.CreateRet(_env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
        }
    }
}