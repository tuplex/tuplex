//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/TuplexSourceTaskBuilder.h>

namespace tuplex {
    namespace codegen {
        llvm::Function* TuplexSourceTaskBuilder::build(bool terminateEarlyOnLimitCode) {
            auto func = createFunction();

            // create main loop
            createMainLoop(func, terminateEarlyOnLimitCode);

            return func;
        }

        void TuplexSourceTaskBuilder::processRow(IRBuilder &builder, llvm::Value *userData,
                                                 const FlattenedTuple &tuple,
                                                 llvm::Value *normalRowCountVar,
                                                 llvm::Value *badRowCountVar,
                                                 llvm::Value *rowNumberVar,
                                                 llvm::Value *inputRowPtr,
                                                 llvm::Value *inputRowSize,
                                                 bool terminateEarlyOnLimitCode,
                                                 llvm::Function *processRowFunc) {
            using namespace llvm;

            // call pipeline function, then increase normalcounter
            if(processRowFunc) {
                callProcessFuncWithHandler(builder, userData, tuple, normalRowCountVar, rowNumberVar, inputRowPtr,
                                           inputRowSize, terminateEarlyOnLimitCode, processRowFunc);
            } else {
                Value *normalRowCount = builder.CreateLoad(normalRowCountVar, "normalRowCount");
                builder.CreateStore(builder.CreateAdd(normalRowCount, env().i64Const(1)), normalRowCountVar);
            }
        }

        void TuplexSourceTaskBuilder::callProcessFuncWithHandler(IRBuilder &builder, llvm::Value *userData,
                                                                 const FlattenedTuple& tuple,
                                                                 llvm::Value *normalRowCountVar,
                                                                 llvm::Value *rowNumberVar,
                                                                 llvm::Value *inputRowPtr,
                                                                 llvm::Value *inputRowSize,
                                                                 bool terminateEarlyOnLimitCode,
                                                                 llvm::Function *processRowFunc) {
            auto& context = env().getContext();
            auto pip_res = PipelineBuilder::call(builder, processRowFunc, tuple, userData, builder.CreateLoad(rowNumberVar), initIntermediate(builder));

            // create if based on resCode to go into exception block
            auto ecCode = builder.CreateZExtOrTrunc(pip_res.resultCode, env().i64Type());
            auto ecOpID = builder.CreateZExtOrTrunc(pip_res.exceptionOperatorID, env().i64Type());
            auto numRowsCreated = builder.CreateZExtOrTrunc(pip_res.numProducedRows, env().i64Type());

            if(terminateEarlyOnLimitCode)
                generateTerminateEarlyOnCode(builder.get(), ecCode, ExceptionCode::OUTPUT_LIMIT_REACHED);

            // add number of rows created to output row number variable
            auto outputRowNumber = builder.CreateLoad(rowNumberVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(rowNumberVar), numRowsCreated), rowNumberVar);

            auto exceptionRaised = builder.CreateICmpNE(ecCode, env().i64Const(ecToI32(ExceptionCode::SUCCESS)));

            llvm::BasicBlock* bbPipelineOK = llvm::BasicBlock::Create(context, "pipeline_ok", builder.GetInsertBlock()->getParent());
            llvm::BasicBlock* curBlock = builder.GetInsertBlock();
            llvm::BasicBlock* bbPipelineFailed = exceptionBlock(builder, userData, ecCode, ecOpID, outputRowNumber, inputRowPtr, inputRowSize); // generate exception block (incl. ignore & handler if necessary)

            llvm::BasicBlock* lastExceptionBlock = builder.GetInsertBlock();
            llvm::BasicBlock* bbPipelineDone = llvm::BasicBlock::Create(context, "pipeline_done", builder.GetInsertBlock()->getParent());

            builder.SetInsertPoint(curBlock);
            builder.CreateCondBr(exceptionRaised, bbPipelineFailed, bbPipelineOK);

            // pipeline ok
            builder.SetInsertPoint(bbPipelineOK);
            llvm::Value *normalRowCount = builder.CreateLoad(normalRowCountVar, "normalRowCount");
            builder.CreateStore(builder.CreateAdd(normalRowCount, env().i64Const(1)), normalRowCountVar);

            builder.CreateBr(bbPipelineDone);

            // connect exception block to pipeline failure
            builder.SetInsertPoint(lastExceptionBlock);
            builder.CreateBr(bbPipelineDone);

            builder.SetInsertPoint(bbPipelineDone);

            // call runtime free all
            _env->freeAll(builder.get());
        }

        void TuplexSourceTaskBuilder::createMainLoop(llvm::Function *read_block_func, bool terminateEarlyOnLimitCode) {
            using namespace std;
            using namespace llvm;

            assert(read_block_func);

            auto& context = env().getContext();

            auto argUserData = arg("userData");
            auto argInPtr = arg("inPtr");
            auto argInSize = arg("inSize");
            auto argOutNormalRowCount = arg("outNormalRowCount");
            auto argOutBadRowCount = arg("outBadRowCount");
            auto argIgnoreLastRow = arg("ignoreLastRow");

            BasicBlock *bbBody = BasicBlock::Create(context, "entry", read_block_func);

            IRBuilder builder(bbBody);


            // there should be a check if argInSize is 0
            // if so -> handle separately, i.e. return immediately
#warning "add here argInSize > 0 check"


            // compute endptr from args
            Value *endPtr = builder.CreateGEP(argInPtr, argInSize, "endPtr");
            Value *currentPtrVar = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "readPtrVar");
            // later use combi of normal & bad rows
            //Value *normalRowCountVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "normalRowCountVar");
            //Value *badRowCountVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "badRowCountVar");
            Value *outRowCountVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "outRowCountVar"); // counter for output row number (used for exception resolution)
            builder.CreateStore(argInPtr, currentPtrVar);
            //builder.CreateStore(env().i64Const(0), normalRowCountVar);
            //builder.CreateStore(env().i64Const(0), badRowCountVar);
            //builder.CreateStore(env().i64Const(0), outRowCountVar);

            Value *normalRowCountVar = argOutNormalRowCount;
            Value *badRowCountVar = argOutBadRowCount;
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(argOutBadRowCount),
                                                  builder.CreateLoad(argOutNormalRowCount)), outRowCountVar);



            // get num rows to read & process in loop
            Value *numRowsVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "numRowsVar");
            Value *input_ptr = builder.CreatePointerCast(argInPtr, env().i64Type()->getPointerTo(0));
            builder.CreateStore(builder.CreateLoad(input_ptr), numRowsVar);
            // store current input ptr
            Value *currentInputPtrVar = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "ptr");
            builder.CreateStore(builder.CreateGEP(argInPtr, env().i32Const(sizeof(int64_t))), currentInputPtrVar);


            // variable for current row number...
            Value *rowVar = builder.CreateAlloca(env().i64Type(), 0, nullptr);
            builder.CreateStore(env().i64Const(0), rowVar);

            BasicBlock* bbLoopCondition = BasicBlock::Create(context, "loop_cond", read_block_func);
            BasicBlock* bbLoopBody = BasicBlock::Create(context, "loop_body", read_block_func);
            BasicBlock* bbLoopDone = BasicBlock::Create(context, "loop_done", read_block_func);

            // go from entry block to loop body
            builder.CreateBr(bbLoopBody);

            // --------------
            // loop condition
            builder.SetInsertPoint(bbLoopCondition);
            Value *row = builder.CreateLoad(rowVar, "row");
            Value* nextRow = builder.CreateAdd(env().i64Const(1), row);
            Value* numRows = builder.CreateLoad(numRowsVar, "numRows");
            builder.CreateStore(nextRow, rowVar, "row");
            auto cond = builder.CreateICmpSLT(nextRow, numRows);
            builder.CreateCondBr(cond, bbLoopBody, bbLoopDone);


            // ---------
            // loop body
            builder.SetInsertPoint(bbLoopBody);
            // decode tuple from input ptr
            FlattenedTuple ft(_env.get());
            ft.init(_inputRowType);
            Value* oldInputPtr = builder.CreateLoad(currentInputPtrVar, "ptr");
            ft.deserializationCode(builder.get(), oldInputPtr);
            Value* newInputPtr = builder.CreateGEP(oldInputPtr, ft.getSize(builder.get())); // @TODO: maybe use inbounds
            builder.CreateStore(newInputPtr, currentInputPtrVar);

            // call function --> incl. exception handling
            // process row here -- BEGIN
            Value *inputRowSize = ft.getSize(builder.get());
            processRow(builder, argUserData, ft, normalRowCountVar, badRowCountVar, outRowCountVar, oldInputPtr, inputRowSize, terminateEarlyOnLimitCode, pipeline() ? pipeline()->getFunction() : nullptr);
            // end process row here -- END
            builder.CreateBr(bbLoopCondition);

            // ---------
            // loop done
            builder.SetInsertPoint(bbLoopDone);

            // if intermediate callback desired, perform!
            if(_intermediateType != python::Type::UNKNOWN && !_intermediateCallbackName.empty()) {
                writeIntermediate(builder, argUserData, _intermediateCallbackName);
            }

            env().storeIfNotNull(builder, builder.CreateLoad(normalRowCountVar), argOutNormalRowCount);
            env().storeIfNotNull(builder, builder.CreateLoad(badRowCountVar), argOutBadRowCount);

            // return bytes read
            Value* curPtr = builder.CreateLoad(currentInputPtrVar, "ptr");
            Value* bytesRead = builder.CreateSub(builder.CreatePtrToInt(curPtr, env().i64Type()),
                                                 builder.CreatePtrToInt(argInPtr, env().i64Type()));
            builder.CreateRet(bytesRead);
        }
    }
}