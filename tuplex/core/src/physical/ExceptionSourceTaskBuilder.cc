//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/1/2021                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/ExceptionSourceTaskBuilder.h>

namespace tuplex {
    namespace codegen {
        llvm::Function* ExceptionSourceTaskBuilder::build(bool terminateEarlyOnFailureCode) {
            auto func = createFunctionWithExceptions();

            // create main loop
            createMainLoop(func, terminateEarlyOnFailureCode);

            return func;
        }

        void ExceptionSourceTaskBuilder::processRow(IRBuilder &builder, llvm::Value *userData,
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
                callProcessFuncWithHandler(builder, userData, tuple, normalRowCountVar, badRowCountVar, rowNumberVar, inputRowPtr,
                                           inputRowSize, terminateEarlyOnLimitCode, processRowFunc);
            } else {
                Value *normalRowCount = builder.CreateLoad(normalRowCountVar, "normalRowCount");
                builder.CreateStore(builder.CreateAdd(normalRowCount, env().i64Const(1)), normalRowCountVar);
            }
        }

        void ExceptionSourceTaskBuilder::callProcessFuncWithHandler(IRBuilder &builder, llvm::Value *userData,
                                                                 const FlattenedTuple& tuple,
                                                                 llvm::Value *normalRowCountVar,
                                                                 llvm::Value *badRowCountVar,
                                                                 llvm::Value *rowNumberVar,
                                                                 llvm::Value *inputRowPtr,
                                                                 llvm::Value *inputRowSize,
                                                                 bool terminateEarlyOnLimitCode,
                                                                 llvm::Function *processRowFunc) {
            auto& context = env().getContext();
            auto pip_res = PipelineBuilder::call(builder.get(), processRowFunc, tuple, userData, builder.CreateLoad(rowNumberVar), initIntermediate(builder));

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

            llvm::BasicBlock* bbPipelineFailedUpdate = llvm::BasicBlock::Create(context, "pipeline_failed", builder.GetInsertBlock()->getParent());
            llvm::BasicBlock* bbPipelineOK = llvm::BasicBlock::Create(context, "pipeline_ok", builder.GetInsertBlock()->getParent());
            llvm::BasicBlock* curBlock = builder.GetInsertBlock();
            llvm::BasicBlock* bbPipelineFailed = exceptionBlock(builder, userData, ecCode, ecOpID, outputRowNumber, inputRowPtr, inputRowSize); // generate exception block (incl. ignore & handler if necessary)


            llvm::BasicBlock* lastExceptionBlock = builder.GetInsertBlock();
            llvm::BasicBlock* bbPipelineDone = llvm::BasicBlock::Create(context, "pipeline_done", builder.GetInsertBlock()->getParent());

            builder.SetInsertPoint(curBlock);
            builder.CreateCondBr(exceptionRaised, bbPipelineFailedUpdate, bbPipelineOK);

            builder.SetInsertPoint(bbPipelineFailedUpdate);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(badRowCountVar), env().i64Const(1)), badRowCountVar);
            builder.CreateBr(bbPipelineFailed);

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

        void ExceptionSourceTaskBuilder::createMainLoop(llvm::Function *read_block_func, bool terminateEarlyOnLimitCode) {
            using namespace std;
            using namespace llvm;

            assert(read_block_func);

            auto& context = env().getContext();

            auto argUserData = arg("userData");
            auto argInPtr = arg("inPtr");
            auto argInSize = arg("inSize");
            auto argExpPtrs = arg("expPtrs");
            auto argExpPtrSizes = arg("expPtrSizes");
            auto argNumExps = arg("numExps");
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
            Value *outRowCountVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "outRowCountVar"); // counter for output row number (used for exception resolution)
            builder.CreateStore(argInPtr, currentPtrVar);

            Value *normalRowCountVar = argOutNormalRowCount;
            Value *badRowCountVar = argOutBadRowCount;
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(argOutBadRowCount),
                                                  builder.CreateLoad(argOutNormalRowCount)), outRowCountVar);

            // current index into array of exception partitions
            auto curExpIndVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curExpIndVar");
            builder.CreateStore(env().i64Const(0), curExpIndVar);

            // current partition pointer
            auto curExpPtrVar = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "curExpPtrVar");
            builder.CreateStore(builder.CreateLoad(argExpPtrs), curExpPtrVar);

            // number of rows total in current partition
            auto curExpNumRowsVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curExpNumRowsVar");
            builder.CreateStore(builder.CreateLoad(argExpPtrSizes), curExpNumRowsVar);

            // current row number in current partition
            auto curExpCurRowVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curExpCurRowVar");
            builder.CreateStore(env().i64Const(0), curExpCurRowVar);

            // accumulator used to update exception indices when rows are filtered, counts number of previously fitlered rows
            auto expAccVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "expAccVar");
            builder.CreateStore(env().i64Const(0), expAccVar);

            // used to see if rows are filtered during pipeline execution
            auto prevRowNumVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "prevRowNumVar");
            auto prevBadRowNumVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "prevBadRowNumVar");

            // current number of exceptions prosessed across all partitions
            auto expCurRowVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "expCurRowVar");
            builder.CreateStore(env().i64Const(0), expCurRowVar);

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

            builder.CreateStore(builder.CreateLoad(outRowCountVar), prevRowNumVar);
            builder.CreateStore(builder.CreateLoad(badRowCountVar), prevBadRowNumVar);

            // call function --> incl. exception handling
            // process row here -- BEGIN
            Value *inputRowSize = ft.getSize(builder.get());
            processRow(builder, argUserData, ft, normalRowCountVar, badRowCountVar, outRowCountVar, oldInputPtr, inputRowSize, terminateEarlyOnLimitCode, pipeline() ? pipeline()->getFunction() : nullptr);

            // After row is processed we need to update exceptions if the row was filtered
            // We check that: outRowCountVar == prevRowCountVar (no new row was emitted)
            //                badRowCountVar == prevBadRowNumVar (it was filtered, not just an exception)
            //                expCurRowVar < argNumExps (we still have exceptions that need updating)
            // if (outRowCountVar == prevRowNumVar && badRowCountVar == prevBadRowNumVar && expCurRowVar < argNumExps)
            auto bbExpUpdate = llvm::BasicBlock::Create(context, "exp_update", builder.GetInsertBlock()->getParent());
            auto expCond = builder.CreateICmpEQ(builder.CreateLoad(outRowCountVar), builder.CreateLoad(prevRowNumVar));
            auto badCond = builder.CreateICmpEQ(builder.CreateLoad(badRowCountVar), builder.CreateLoad(prevBadRowNumVar));
            auto remCond = builder.CreateICmpSLT(builder.CreateLoad(expCurRowVar), argNumExps);
            builder.CreateCondBr(builder.CreateAnd(remCond, builder.CreateAnd(badCond, expCond)), bbExpUpdate, bbLoopCondition);

            // We have determined a row is filtered so we can iterate through all the input exceptions that occured before this
            // row and decrement their row index with the number of previously filtered rows (expAccVar).
            // This is a while loop that iterates over all exceptions that occured before this filtered row
            //
            // while (expCurRowVar < numExps && ((*outNormalRowCount - 1) + expCurRowVar) >= *((int64_t *) curExpPtrVar))
            //
            // *outNormalRowCount - 1 changes cardinality of rows into its row index, we add the number of previously processed
            // exceptions because the normal rows do not know about the exceptions to obtain the correct index. It's then compared
            // against the row index of the exception pointed to currently in our partition
            builder.SetInsertPoint(bbExpUpdate);
            auto bbIncrement = llvm::BasicBlock::Create(context, "increment", builder.GetInsertBlock()->getParent());
            auto bbIncrementDone = llvm::BasicBlock::Create(context, "increment_done", builder.GetInsertBlock()->getParent());
            auto curExpRowIndPtr = builder.CreatePointerCast(builder.CreateLoad(curExpPtrVar), env().i64ptrType());
            auto incCond = builder.CreateICmpSGE(builder.CreateAdd(builder.CreateLoad(badRowCountVar), builder.CreateAdd(builder.CreateSub(builder.CreateLoad(normalRowCountVar), env().i64Const(1)), builder.CreateLoad(expCurRowVar))), builder.CreateLoad(curExpRowIndPtr));
            auto remCond2 = builder.CreateICmpSLT(builder.CreateLoad(expCurRowVar), argNumExps);
            builder.CreateCondBr(builder.CreateAnd(remCond2, incCond), bbIncrement, bbIncrementDone);

            // Body of the while loop we need to
            // 1. decrement the current exception row index by the expAccVar (all rows previously filtered)
            // 2. Increment our partition pointer to next exception
            // 3. Change partitions if we've exhausted all exceptions in the current, but still have more remaining in tototal
            //
            // Increment to the next exception by adding eSize and 4*sizeof(int64_t) to the partition pointer
            // *((int64_t *) curExpPtrVar) -= expAccVar;
            // curExpPtrVar += 4 * sizeof(int64_t) + ((int64_t *)curExpPtrVar)[3];
            // expCurRowVar += 1;
            // curExpCurRowVar += 1;
            //
            // Finally we check to see if a partition change is required
            // if (expCurRowVar < numExps && curExpCurRowVar >= curExpNumRowsVar)
            builder.SetInsertPoint(bbIncrement);
            // Change row index and go to next exception in partition
            auto curExpRowIndPtr2 = builder.CreatePointerCast(builder.CreateLoad(curExpPtrVar), env().i64Type()->getPointerTo(0));
            builder.CreateStore(builder.CreateSub(builder.CreateLoad(curExpRowIndPtr2), builder.CreateLoad(expAccVar)), curExpRowIndPtr2);
            auto curOffset = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curOffset");
            builder.CreateStore(builder.CreateLoad(builder.CreateGEP(curExpRowIndPtr2, env().i64Const(3))), curOffset);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curOffset), env().i64Const(4 * sizeof(int64_t))), curOffset);
            builder.CreateStore(builder.CreateGEP(builder.CreateLoad(curExpPtrVar), builder.CreateLoad(curOffset)), curExpPtrVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curExpCurRowVar), env().i64Const(1)), curExpCurRowVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(expCurRowVar), env().i64Const(1)), expCurRowVar);
            // See if partition change needs to occur
            auto bbChange = llvm::BasicBlock::Create(context, "change", builder.GetInsertBlock()->getParent());
            auto changeCond = builder.CreateICmpSGE(builder.CreateLoad(curExpCurRowVar), builder.CreateLoad(curExpNumRowsVar));
            auto leftCond = builder.CreateICmpSLT(builder.CreateLoad(expCurRowVar), argNumExps);
            builder.CreateCondBr(builder.CreateAnd(leftCond, changeCond), bbChange, bbExpUpdate);

            // This block changes to the next partition
            // curExpCurRowVar = 0;
            // curExpIndVar = curExpIndVar + 1;
            // curExpPtrVar = expPtrs[curExpIndVar];
            // curExpNumRowsVar = expPtrSizes[curExpIndVar];
            builder.SetInsertPoint(bbChange);
            builder.CreateStore(env().i64Const(0), curExpCurRowVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curExpIndVar), env().i64Const(1)), curExpIndVar);
            builder.CreateStore(builder.CreateLoad(builder.CreateGEP(argExpPtrs, builder.CreateLoad(curExpIndVar))), curExpPtrVar);
            builder.CreateStore(builder.CreateLoad(builder.CreateGEP(argExpPtrSizes, builder.CreateLoad(curExpIndVar))), curExpNumRowsVar);
            builder.CreateBr(bbExpUpdate);

            // Finally increment the expAccVar by 1 becasue a row was filtered
            // expAccVar += 1;
            builder.SetInsertPoint(bbIncrementDone);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(expAccVar), env().i64Const(1)), expAccVar);
            builder.CreateBr(bbLoopCondition);

            // ---------
            // loop done
            builder.SetInsertPoint(bbLoopDone);
            auto bbRemainingExceptions = llvm::BasicBlock::Create(context, "remaining_exceptions", builder.GetInsertBlock()->getParent());
            auto bbRemainingDone = llvm::BasicBlock::Create(context, "remaining_done", builder.GetInsertBlock()->getParent());
            auto expRemaining = builder.CreateICmpSLT(builder.CreateLoad(expCurRowVar), argNumExps);
            builder.CreateCondBr(expRemaining, bbRemainingExceptions, bbRemainingDone);

            // We have processed all of the normal rows. If we have not exhausted all of our exceptions
            // we just iterate through the remaining exceptions and decrement their row index by the final
            // value of expAccVar counting our filtered rows.
            // Same code as above, but just don't need to keep updating expAccVar by 1.
            builder.SetInsertPoint(bbRemainingExceptions);
            auto curExpRowIndPtr3 = builder.CreatePointerCast(builder.CreateLoad(curExpPtrVar), env().i64Type()->getPointerTo(0));
            builder.CreateStore(builder.CreateSub(builder.CreateLoad(curExpRowIndPtr3), builder.CreateLoad(expAccVar)), curExpRowIndPtr3);
            auto curOffset2 = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curOffset2");
            builder.CreateStore(builder.CreateLoad(builder.CreateGEP(curExpRowIndPtr3, env().i64Const(3))), curOffset2);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curOffset2), env().i64Const(4 * sizeof(int64_t))), curOffset2);
            builder.CreateStore(builder.CreateGEP(builder.CreateLoad(curExpPtrVar), builder.CreateLoad(curOffset2)), curExpPtrVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curExpCurRowVar), env().i64Const(1)), curExpCurRowVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(expCurRowVar), env().i64Const(1)), expCurRowVar);

            auto bbChange2 = llvm::BasicBlock::Create(context, "change2", builder.GetInsertBlock()->getParent());
            auto changeCond2 = builder.CreateICmpSGE(builder.CreateLoad(curExpCurRowVar), builder.CreateLoad(curExpNumRowsVar));
            auto leftCond2 = builder.CreateICmpSLT(builder.CreateLoad(expCurRowVar), argNumExps);
            builder.CreateCondBr(builder.CreateAnd(leftCond2, changeCond2), bbChange2, bbLoopDone);

            builder.SetInsertPoint(bbChange2);
            builder.CreateStore(env().i64Const(0), curExpCurRowVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curExpIndVar), env().i64Const(1)), curExpIndVar);
            builder.CreateStore(builder.CreateLoad(builder.CreateGEP(argExpPtrs, builder.CreateLoad(curExpIndVar))), curExpPtrVar);
            builder.CreateStore(builder.CreateLoad(builder.CreateGEP(argExpPtrSizes, builder.CreateLoad(curExpIndVar))), curExpNumRowsVar);
            builder.CreateBr(bbLoopDone);

            builder.SetInsertPoint(bbRemainingDone);
            // if intermediate callback desired, perform!
            if(_intermediateType != python::Type::UNKNOWN && !_intermediateCallbackName.empty()) {
                writeIntermediate(builder, argUserData, _intermediateCallbackName);
            }

            env().storeIfNotNull(builder.get(), builder.CreateLoad(normalRowCountVar), argOutNormalRowCount);
            env().storeIfNotNull(builder.get(), builder.CreateLoad(badRowCountVar), argOutBadRowCount);

            // return bytes read
            Value* curPtr = builder.CreateLoad(currentInputPtrVar, "ptr");
            Value* bytesRead = builder.CreateSub(builder.get().CreatePtrToInt(curPtr, env().i64Type()), builder.get().CreatePtrToInt(argInPtr, env().i64Type()));
            builder.get().CreateRet(bytesRead);
        }
    }
}