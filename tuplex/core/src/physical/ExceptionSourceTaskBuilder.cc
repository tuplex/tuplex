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
                Value *normalRowCount = builder.CreateLoad(builder.getInt64Ty(), normalRowCountVar, "normalRowCount");
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
            auto pip_res = PipelineBuilder::call(builder, processRowFunc, tuple, userData,
                                                 builder.CreateLoad(builder.getInt64Ty(), rowNumberVar),
                                                 initIntermediate(builder));

            // create if based on resCode to go into exception block
            auto ecCode = builder.CreateZExtOrTrunc(pip_res.resultCode, env().i64Type());
            auto ecOpID = builder.CreateZExtOrTrunc(pip_res.exceptionOperatorID, env().i64Type());
            auto numRowsCreated = builder.CreateZExtOrTrunc(pip_res.numProducedRows, env().i64Type());

            if(terminateEarlyOnLimitCode)
                generateTerminateEarlyOnCode(builder, ecCode, ExceptionCode::OUTPUT_LIMIT_REACHED);

            // add number of rows created to output row number variable
            auto outputRowNumber = builder.CreateLoad(builder.getInt64Ty(), rowNumberVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), rowNumberVar), numRowsCreated), rowNumberVar);

            auto exceptionRaised = builder.CreateICmpNE(ecCode, env().i64Const(ecToI32(ExceptionCode::SUCCESS)));

            llvm::BasicBlock* bbPipelineFailedUpdate = llvm::BasicBlock::Create(context, "pipeline_failed", builder.GetInsertBlock()->getParent());
            llvm::BasicBlock* bbPipelineOK = llvm::BasicBlock::Create(context, "pipeline_ok", builder.GetInsertBlock()->getParent());
            llvm::BasicBlock* curBlock = builder.GetInsertBlock();
            llvm::BasicBlock* bbPipelineFailed = exceptionBlock(builder, userData, ecCode, ecOpID,
                                                                outputRowNumber, inputRowPtr, inputRowSize); // generate exception block (incl. ignore & handler if necessary)


            llvm::BasicBlock* lastExceptionBlock = builder.GetInsertBlock();
            llvm::BasicBlock* bbPipelineDone = llvm::BasicBlock::Create(context, "pipeline_done",
                                                                        builder.GetInsertBlock()->getParent());

            builder.SetInsertPoint(curBlock);
            builder.CreateCondBr(exceptionRaised, bbPipelineFailedUpdate, bbPipelineOK);

            builder.SetInsertPoint(bbPipelineFailedUpdate);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), badRowCountVar),
                                                  env().i64Const(1)), badRowCountVar);
            builder.CreateBr(bbPipelineFailed);

            // pipeline ok
            builder.SetInsertPoint(bbPipelineOK);
            llvm::Value *normalRowCount = builder.CreateLoad(builder.getInt64Ty(), normalRowCountVar, "normalRowCount");
            builder.CreateStore(builder.CreateAdd(normalRowCount, env().i64Const(1)), normalRowCountVar);
            builder.CreateBr(bbPipelineDone);

            // connect exception block to pipeline failure
            builder.SetInsertPoint(lastExceptionBlock);
            builder.CreateBr(bbPipelineDone);

            builder.SetInsertPoint(bbPipelineDone);

            // call runtime free all
            _env->freeAll(builder);
        }

        void ExceptionSourceTaskBuilder::createMainLoop(llvm::Function *read_block_func, bool terminateEarlyOnLimitCode) {
            using namespace std;
            using namespace llvm;

            assert(read_block_func);

            // Initialize context
            auto& context = env().getContext();

            // Load function arguments
            auto argUserData = arg("userData");
            auto argInPtr = arg("inPtr");
            auto argInSize = arg("inSize");
            auto argOutNormalRowCount = arg("outNormalRowCount");
            auto argOutBadRowCount = arg("outBadRowCount");
            auto argIgnoreLastRow = arg("ignoreLastRow");
            auto totalFilterCounter = arg("totalFilterCounter");
            auto totalNormalRowCounter = arg("totalNormalRowCounter");
            auto totalGeneralRowCounter = arg("totalGeneralRowCounter");
            auto totalFallbackRowCounter = arg("totalFallbackRowCounter");
            auto generalPartitions = arg("generalPartitions");
            auto numGeneralPartitions = arg("numGeneralPartitions");
            auto generalIndexOffset = arg("generalIndexOffset");
            auto generalRowOffset = arg("generalRowOffset");
            auto generalByteOffset = arg("generalByteOffset");
            auto fallbackPartitions = arg("fallbackPartitions");
            auto numFallbackPartitions = arg("numFallbackPartitions");
            auto fallbackIndexOffset = arg("fallbackIndexOffset");
            auto fallbackRowOffset = arg("fallbackRowOffset");
            auto fallbackByteOffset = arg("fallbackByteOffset");

            // Initialize function body
            BasicBlock *bbBody = BasicBlock::Create(context, "entry", read_block_func);

            IRBuilder builder(bbBody);

            // Define basic blocks for function
            auto bbInitializeGeneral = llvm::BasicBlock::Create(context, "initialize_general", builder.GetInsertBlock()->getParent());
            auto bbDeclareFallback = llvm::BasicBlock::Create(context, "declare_fallback", builder.GetInsertBlock()->getParent());
            auto bbInitializeFallback = llvm::BasicBlock::Create(context, "initialize_fallback", builder.GetInsertBlock()->getParent());
            auto bbUpdateGeneralCond = llvm::BasicBlock::Create(context, "update_general_cond", builder.GetInsertBlock()->getParent());
            auto bbUpdateGeneralBody = llvm::BasicBlock::Create(context, "update_general_body", builder.GetInsertBlock()->getParent());
            auto bbNextGeneralPartition = llvm::BasicBlock::Create(context, "next_general_partition", builder.GetInsertBlock()->getParent());
            auto bbUpdateFallbackCond = llvm::BasicBlock::Create(context, "update_fallback_cond", builder.GetInsertBlock()->getParent());
            auto bbUpdateFallbackBody = llvm::BasicBlock::Create(context, "update_fallback_body", builder.GetInsertBlock()->getParent());
            auto bbNextFallbackPartition = llvm::BasicBlock::Create(context, "next_fallback_partition", builder.GetInsertBlock()->getParent());
            auto bbUpdateDone = llvm::BasicBlock::Create(context, "update_done", builder.GetInsertBlock()->getParent());
            auto bbLoopCondition = BasicBlock::Create(context, "loop_cond", read_block_func);
            auto bbLoopBody = BasicBlock::Create(context, "loop_body", read_block_func);
            auto bbLoopDone = BasicBlock::Create(context, "loop_done", read_block_func);

            // Initialize values for normal partitions
            auto endPtr = builder.MovePtrByBytes(argInPtr, argInSize, "endPtr");
            auto currentPtrVar = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "readPtrVar");
            auto outRowCountVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "outRowCountVar"); // counter for output row number (used for exception resolution)
            builder.CreateStore(argInPtr, currentPtrVar);
            // Update the arguments at the end
            auto normalRowCountVar = argOutNormalRowCount;
            auto badRowCountVar = argOutBadRowCount;
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), argOutBadRowCount),
                                                  builder.CreateLoad(builder.getInt64Ty(), argOutNormalRowCount)), outRowCountVar);
            // get num rows to read & process in loop
            auto numRowsVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "numRowsVar");
            auto input_ptr = builder.CreatePointerCast(argInPtr, env().i64Type()->getPointerTo(0));
            builder.CreateStore(builder.CreateLoad(builder.getInt64Ty(), input_ptr), numRowsVar);
            // store current input ptr
            auto currentInputPtrVar = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "ptr");
            builder.CreateStore(builder.MovePtrByBytes(argInPtr, sizeof(int64_t)), currentInputPtrVar);
            // variable for current row number...
            auto rowVar = builder.CreateAlloca(env().i64Type(), 0, nullptr);
            builder.CreateStore(env().i64Const(0), rowVar);

            // used to see if rows are filtered during pipeline execution
            auto prevRowNumVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "prevRowNumVar");
            auto prevBadRowNumVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "prevBadRowNumVar");

            // Initialize values for index updating
            // uint8_t *curGeneralPtr;
            // int64_t curGeneralNumRows = 0;
            // if (*generalIndexOffset < numGeneralPartitions) {
            //     curGeneralPtr = generalPartitions[*generalIndexOffset];
            //     curGeneralNumRows = *curGeneralPtr;
            //     curGeneralPtr += sizeof(int64_t) + *generalByteOffset;
            // }
            auto curGeneralPtr = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "curGeneralPtr");
            auto curGeneralNumRows = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curGeneralNumRows");
            builder.CreateStore(env().i64Const(0), curGeneralNumRows);
            auto shouldInitializeGeneral = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), generalIndexOffset), numGeneralPartitions);
            builder.CreateCondBr(shouldInitializeGeneral, bbInitializeGeneral, bbDeclareFallback);

            builder.SetInsertPoint(bbInitializeGeneral);
            builder.CreateStore(builder.CreateLoad(env().i8ptrType(), builder.CreateGEP(env().i8ptrType(), generalPartitions, builder.CreateLoad(builder.getInt64Ty(), generalIndexOffset))), curGeneralPtr);
            builder.CreateStore(builder.CreateLoad(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curGeneralPtr), env().i64ptrType())), curGeneralNumRows);
            builder.CreateStore(builder.MovePtrByBytes(builder.CreateLoad(env().i8ptrType(), curGeneralPtr), builder.CreateAdd(env().i64Const(sizeof(int64_t)), builder.CreateLoad(builder.getInt64Ty(), generalByteOffset))), curGeneralPtr);
            builder.CreateBr(bbDeclareFallback);

            // uint8_t *curFallbackPtr;
            // int64_t curFallbackNumRows = 0;
            // if (*fallbackIndexOffset < numFallbackPartitions) {
            //     curFallbackPtr = fallbackPartitions[*fallbackIndexOffset];
            //     curFallbackNumRows = *curFallbackPtr;
            //     curFallbackPtr += sizeof(int64_t) + *fallbackByteOffset;
            // }
            builder.SetInsertPoint(bbDeclareFallback);
            auto curFallbackPtr = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "curFallbackPtr");
            auto curFallbackNumRows = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curFallbackNumRows");
            builder.CreateStore(env().i64Const(0), curFallbackNumRows);
            auto shouldInitializeFallback = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), fallbackIndexOffset), numFallbackPartitions);
            builder.CreateCondBr(shouldInitializeFallback, bbInitializeFallback, bbLoopBody);

            builder.SetInsertPoint(bbInitializeFallback);
            builder.CreateStore(builder.CreateLoad(env().i8ptrType(), builder.CreateGEP(env().i8ptrType(), fallbackPartitions, builder.CreateLoad(builder.getInt64Ty(), fallbackIndexOffset))), curFallbackPtr);
            builder.CreateStore(builder.CreateLoad(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curFallbackPtr), env().i64ptrType())), curFallbackNumRows);
            builder.CreateStore(builder.MovePtrByBytes(builder.CreateLoad(env().i8ptrType(), curFallbackPtr), builder.CreateAdd(env().i64Const(sizeof(int64_t)), builder.CreateLoad(builder.getInt64Ty(), fallbackByteOffset))), curFallbackPtr);
            builder.CreateBr(bbLoopBody);

            // loop condition
            builder.SetInsertPoint(bbLoopCondition);
            Value *row = builder.CreateLoad(builder.getInt64Ty(), rowVar, "row");
            Value* nextRow = builder.CreateAdd(env().i64Const(1), row);
            Value* numRows = builder.CreateLoad(builder.getInt64Ty(), numRowsVar, "numRows");
            builder.CreateStore(nextRow, rowVar, "row");
            auto cond = builder.CreateICmpSLT(nextRow, numRows);
            builder.CreateCondBr(cond, bbLoopBody, bbLoopDone);

            // loop body
            builder.SetInsertPoint(bbLoopBody);
            // decode tuple from input ptr
            FlattenedTuple ft(_env.get());
            ft.init(_inputRowType);
            Value* oldInputPtr = builder.CreateLoad(env().i8ptrType(), currentInputPtrVar, "ptr");
            ft.deserializationCode(builder, oldInputPtr);
            Value* newInputPtr = builder.MovePtrByBytes(oldInputPtr, ft.getSize(builder));
            builder.CreateStore(newInputPtr, currentInputPtrVar);
            builder.CreateStore(builder.CreateLoad(builder.getInt64Ty(), outRowCountVar), prevRowNumVar);
            builder.CreateStore(builder.CreateLoad(builder.getInt64Ty(), badRowCountVar), prevBadRowNumVar);

            // call function --> incl. exception handling
            // process row here -- BEGIN
            Value *inputRowSize = ft.getSize(builder);
            processRow(builder, argUserData, ft, normalRowCountVar, badRowCountVar, outRowCountVar, oldInputPtr, inputRowSize, terminateEarlyOnLimitCode, pipeline() ? pipeline()->getFunction() : nullptr);

            builder.CreateStore(builder.CreateAdd(env().i64Const(1), builder.CreateLoad(builder.getInt64Ty(), totalNormalRowCounter)), totalNormalRowCounter);

            // After row is processed we need to update exceptions if the row was filtered
            // We check that: outRowCountVar == prevRowCountVar (no new row was emitted)
            //                badRowCountVar == prevBadRowNumVar (it was filtered, not just an exception)
            // if (outRowCountVar == prevRowNumVar && badRowCountVar == prevBadRowNumVar)
            auto rowNotEmitted = builder.CreateICmpEQ(builder.CreateLoad(builder.getInt64Ty(), outRowCountVar), builder.CreateLoad(builder.getInt64Ty(), prevRowNumVar));
            auto rowNotException = builder.CreateICmpEQ(builder.CreateLoad(builder.getInt64Ty(), badRowCountVar), builder.CreateLoad(builder.getInt64Ty(), prevBadRowNumVar));
            builder.CreateCondBr(builder.CreateAnd(rowNotEmitted, rowNotException), bbUpdateGeneralCond, bbLoopCondition);

            // Update general cond
            // while (*generalRowOffset < curGeneralNumRows && *((int64_t*)curGeneralPtr) < curNormalRowInd + totalGeneralRowCounter)
            builder.SetInsertPoint(bbUpdateGeneralCond);
            auto generalRowsRemainCond = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), generalRowOffset), builder.CreateLoad(builder.getInt64Ty(), curGeneralNumRows));
            auto curGeneralRowInd = builder.CreateLoad(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curGeneralPtr), env().i64ptrType()));
            auto generalIndexLTCond = builder.CreateICmpSLT(curGeneralRowInd, builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), totalGeneralRowCounter), builder.CreateLoad(builder.getInt64Ty(), totalNormalRowCounter)));
            builder.CreateCondBr(builder.CreateAnd(generalRowsRemainCond, generalIndexLTCond), bbUpdateGeneralBody, bbUpdateFallbackCond);

            // Update general body
            // generalNewRowInd = *((int64_t*)curGeneralPtr) - totalFilterCounter;
            // *((int64_t*)curGeneralPtr) = generalNewRowInd;
            // auto generalRowDelta = 4 * sizeof(int64_t) + ((int64_t*)curGeneralPtr)[3];
            // curGeneralPtr += generalRowDelta;
            // *generalByteOffset += generalRowDelta;
            // *generalRowOffset++;
            // *totalGeneralRowCounter++;
            builder.SetInsertPoint(bbUpdateGeneralBody);
            auto generalNewRowInd = builder.CreateSub(builder.CreateLoad(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curGeneralPtr), env().i64ptrType())), builder.CreateLoad(builder.getInt64Ty(), totalFilterCounter));
            builder.CreateStore(generalNewRowInd, builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curGeneralPtr), env().i64ptrType()));
            auto generalRowDelta = builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), builder.CreateGEP(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curGeneralPtr), env().i64ptrType()), env().i64Const(3))), env().i64Const(4 * sizeof(int64_t)));
            builder.CreateStore(builder.MovePtrByBytes(builder.CreateLoad(env().i8ptrType(), curGeneralPtr), generalRowDelta), curGeneralPtr);
            builder.CreateStore(builder.CreateAdd(generalRowDelta, builder.CreateLoad(builder.getInt64Ty(), generalByteOffset)), generalByteOffset);
            builder.CreateStore(builder.CreateAdd(env().i64Const(1), builder.CreateLoad(builder.getInt64Ty(), generalRowOffset)), generalRowOffset);
            builder.CreateStore(builder.CreateAdd(env().i64Const(1), builder.CreateLoad(builder.getInt64Ty(), totalGeneralRowCounter)), totalGeneralRowCounter);

            // if (*generalRowOffset == curGeneralNumRows && *generalIndexOffset < numGeneralPartitions - 1)
            auto generalNoRowsRemain = builder.CreateICmpEQ(builder.CreateLoad(builder.getInt64Ty(), generalRowOffset), builder.CreateLoad(builder.getInt64Ty(), curGeneralNumRows));
            auto generalHasMorePartitions = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), generalIndexOffset), builder.CreateSub(numGeneralPartitions, env().i64Const(1)));
            builder.CreateCondBr(builder.CreateAnd(generalNoRowsRemain, generalHasMorePartitions), bbNextGeneralPartition, bbUpdateGeneralCond);

            // generalIndexOffset += 1;
            // *generalRowOffset = 0;
            // *generalByteOffset = 0;
            // curGeneralPtr = generalPartitions[*generalIndexOffset];
            // curGeneralNumRows = *((int64_t*)curGeneralPtr);
            // curGeneralPtr += sizeof(int64_t);
            builder.SetInsertPoint(bbNextGeneralPartition);

            // generalIndexOffset += 1
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), generalIndexOffset), env().i64Const(1)), generalIndexOffset);

            // *generalRowOffset = 0
            // *generalByteOffset = 0
            builder.CreateStore(env().i64Const(0), generalRowOffset);
            builder.CreateStore(env().i64Const(0), generalByteOffset);

            // curGeneralPtr = generalPartitions[*generalIndexOffset]
            llvm::Value* generalIndexOffsetValue = builder.CreateLoad(builder.getInt64Ty(), generalIndexOffset);
            llvm::Value* generalPartitionsElement = builder.CreateLoad(env().i64ptrType(), builder.CreateGEP(env().i64ptrType(), builder.CreateBitCast(generalPartitions, _env->i64ptrType()->getPointerTo()), generalIndexOffsetValue));
            generalPartitionsElement = builder.CreateBitCast(generalPartitionsElement, env().i8ptrType());
            builder.CreateStore(generalPartitionsElement, curGeneralPtr);

            // curGeneralNumRows = *((int64_t*)curGeneralPtr);
            auto curGeneralNumRowsValue = builder.CreateLoad(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curGeneralPtr), env().i64ptrType()));
            builder.CreateStore(curGeneralNumRowsValue, curGeneralNumRows);

            // curGeneralPtr += sizeof(int64_t); // <-- is this accurate?
            auto new_general_ptr = builder.MovePtrByBytes(builder.CreateLoad(env().i8ptrType(), curGeneralPtr),
                                                          builder.CreateAdd(env().i64Const(sizeof(int64_t)), builder.CreateLoad(builder.getInt64Ty(), generalByteOffset)));
            //new_general_ptr = builder.CreateBitCast(new_general_ptr, env().i64ptrType());
            builder.CreateStore( new_general_ptr, curGeneralPtr);
            builder.CreateBr(bbUpdateGeneralCond);

            // Update fallback cond
            // while (*fallbackRowOffset < curFallbackNumRows && *((int64_t*)curFallbackPtr) < curNormalRowInd + totalGeneralRowCounter + totalFallbackRowCounter)
            builder.SetInsertPoint(bbUpdateFallbackCond);
            auto fallbackRowsRemainCond = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), fallbackRowOffset), builder.CreateLoad(builder.getInt64Ty(), curFallbackNumRows));
            auto curFallbackRowInd = builder.CreateLoad(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curFallbackPtr), env().i64ptrType()));
            auto fallbackIndexLTCond = builder.CreateICmpSLT(curFallbackRowInd, builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), totalGeneralRowCounter), builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), totalFallbackRowCounter), builder.CreateLoad(builder.getInt64Ty(), totalNormalRowCounter))));
            builder.CreateCondBr(builder.CreateAnd(fallbackRowsRemainCond, fallbackIndexLTCond), bbUpdateFallbackBody, bbUpdateDone);

            // Update fallback body
            // fallbackNewRowInd = *((int64_t*)curFallbackPtr) - totalFilterCounter;
            // *((int64_t*)curFallbackPtr) = fallbackNewRowInd;
            // auto fallbackRowDelta = 4 * sizeof(int64_t) + ((int64_t*)curFallbackPtr)[3];
            // curFallbackPtr += fallbackRowDelta;
            // *fallbackByteOffset += fallbackRowDelta;
            // *fallbackRowOffset++;
            // *totalFallbackRowCounter++;
            builder.SetInsertPoint(bbUpdateFallbackBody);
            auto fallbackNewRowInd = builder.CreateSub(builder.CreateLoad(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curFallbackPtr), env().i64ptrType())), builder.CreateLoad(builder.getInt64Ty(), totalFilterCounter));
            builder.CreateStore(fallbackNewRowInd, builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curFallbackPtr), env().i64ptrType()));
            auto fallbackRowDelta = builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), builder.CreateGEP(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curFallbackPtr), env().i64ptrType()), env().i64Const(3))), env().i64Const(4 * sizeof(int64_t)));
            builder.CreateStore(builder.MovePtrByBytes(builder.CreateLoad(env().i8ptrType(), curFallbackPtr), fallbackRowDelta), curFallbackPtr);
            builder.CreateStore(builder.CreateAdd(fallbackRowDelta, builder.CreateLoad(builder.getInt64Ty(), fallbackByteOffset)), fallbackByteOffset);
            builder.CreateStore(builder.CreateAdd(env().i64Const(1), builder.CreateLoad(builder.getInt64Ty(), fallbackRowOffset)), fallbackRowOffset);
            builder.CreateStore(builder.CreateAdd(env().i64Const(1), builder.CreateLoad(builder.getInt64Ty(), totalFallbackRowCounter)), totalFallbackRowCounter);

            // if (*fallbackRowOffset == curFallbackNumRows && *fallbackIndexOffset < numFallbackPartitions - 1)
            auto fallbackNoRowsRemain = builder.CreateICmpEQ(builder.CreateLoad(builder.getInt64Ty(), fallbackRowOffset), builder.CreateLoad(builder.getInt64Ty(), curFallbackNumRows));
            auto fallbackHasMorePartitions = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), fallbackIndexOffset), builder.CreateSub(numFallbackPartitions, env().i64Const(1)));
            builder.CreateCondBr(builder.CreateAnd(fallbackNoRowsRemain, fallbackHasMorePartitions), bbNextFallbackPartition, bbUpdateFallbackCond);

            // fallbackIndexOffset += 1;
            // *fallbackRowOffset = 0;
            // *fallbackByteOffset = 0;
            // curFallbackPtr = fallbackPartitions[*fallbackIndexOffset];
            // curFallbackNumRows = *((int64_t*)curFallbackPtr);
            // curFallbackPtr += sizeof(int64_t);
            builder.SetInsertPoint(bbNextFallbackPartition);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), fallbackIndexOffset), env().i64Const(1)), fallbackIndexOffset);
            builder.CreateStore(env().i64Const(0), fallbackRowOffset);
            builder.CreateStore(env().i64Const(0), fallbackByteOffset);
            builder.CreateStore(builder.CreateLoad(env().i8ptrType(), builder.CreateGEP(env().i8ptrType(), fallbackPartitions, builder.CreateLoad(builder.getInt64Ty(), fallbackIndexOffset))), curFallbackPtr);
            builder.CreateStore(builder.CreateLoad(builder.getInt64Ty(), builder.CreatePointerCast(builder.CreateLoad(env().i8ptrType(), curFallbackPtr), env().i64ptrType())), curFallbackNumRows);
            builder.CreateStore(builder.MovePtrByBytes(builder.CreateLoad(env().i8ptrType(), curFallbackPtr), builder.CreateAdd(env().i64Const(sizeof(int64_t)), builder.CreateLoad(builder.getInt64Ty(), fallbackByteOffset))), curFallbackPtr);
            builder.CreateBr(bbUpdateFallbackCond);

            // Update done
            // totalFilterCounter += 1;
            builder.SetInsertPoint(bbUpdateDone);
            builder.CreateStore(builder.CreateAdd(env().i64Const(1), builder.CreateLoad(builder.getInt64Ty(), totalFilterCounter)), totalFilterCounter);
            builder.CreateBr(bbLoopCondition);

            builder.SetInsertPoint(bbLoopDone);
            if(_intermediateType != python::Type::UNKNOWN && !_intermediateCallbackName.empty()) {
                writeIntermediate(builder, argUserData, _intermediateCallbackName);
            }

            env().storeIfNotNull(builder, builder.CreateLoad(builder.getInt64Ty(), normalRowCountVar), argOutNormalRowCount);
            env().storeIfNotNull(builder, builder.CreateLoad(builder.getInt64Ty(), badRowCountVar), argOutBadRowCount);

            // return bytes read
            Value* curPtr = builder.CreateLoad(env().i8ptrType(), currentInputPtrVar, "ptr");
            Value* bytesRead = builder.CreateSub(builder.CreatePtrToInt(curPtr, env().i64Type()), builder.CreatePtrToInt(argInPtr, env().i64Type()));
            builder.CreateRet(bytesRead);
        }
    }
}