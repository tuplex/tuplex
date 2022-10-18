//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/JITCSVSourceTaskBuilder.h>

// uncomment to print detailed info about parsing (helpful for debugging)
// #define TRACE_PARSER

namespace tuplex {
    namespace codegen {
        llvm::Function* JITCSVSourceTaskBuilder::build(bool terminateEarlyOnFailureCode) {

            // check if pipeline exists
            if(pipeline()) {
                auto processRowFunc = pipeline()->getFunction();
                if(!processRowFunc)
                    throw std::runtime_error("invalid function from pipeline builder in JITCSVSourceTaskBuilder");
            }

            // build CSV parser
            generateParser();

            // build wrapper function
            auto func = createFunction(); // TODO: add name here

            // build all necessary ingredients
            createMainLoop(func, terminateEarlyOnFailureCode);

            return func;
        }

        void JITCSVSourceTaskBuilder::generateParser() {
            // add cells & built row parser for CSV
            // here, serialize them all. Later: pushdown!

#ifndef NDEBUG
            Logger::instance().logger("codegen").info("generating CSV Parser for " + _inputRowType.desc());
#endif

            // ==> i.e. this might change some of the pipeline parameters!!!
            assert(_inputRowType.isTupleType());

            // add all columns to be serialized IFF no selectionPushdown given
            if(_columnsToSerialize.empty()) {
                assert(_fileInputRowType == _inputRowType); // they should be the same...

                for (auto colType : _fileInputRowType.parameters())
                    _parseRowGen->addCell(colType, true);
            } else {
                assert(_fileInputRowType.parameters().size() == _columnsToSerialize.size());
                for(int i = 0; i < _fileInputRowType.parameters().size(); ++i) {
                    auto colType = _fileInputRowType.parameters()[i];
                    _parseRowGen->addCell(colType, _columnsToSerialize[i]);
                }
            }
            _parseRowGen->build(); // build as internal function only

            _inputRowType = _parseRowGen->serializedType(); // get the type of the CSV row parser ==> this is the restricted one!
        }

        FlattenedTuple JITCSVSourceTaskBuilder::createFlattenedTupleFromCSVParseResult(IRBuilder& builder, llvm::Value *parseResult,
                                                                                       const python::Type &parseRowType) {
            FlattenedTuple ft(&env());
            ft.init(parseRowType);



            auto numColumns = parseRowType.parameters().size();
            for(int col = 0; col < numColumns; ++col) {
                _env->debugPrint(builder, "get col result for column " + std::to_string(col));
                auto val = _parseRowGen->getColumnResult(builder, col, parseResult);

                _env->debugPrint(builder, "set column " + std::to_string(col));
                ft.set(builder, {col}, val.val, val.size, val.is_null);

#ifdef TRACE_PARSER
                if(val.val)
                    _env->debugPrint(builder, "column " + std::to_string(col), val.val);
#endif

            }

            // note that in order to avoid memcpy, strings that are not dequoted (hence runtime allocated) are not zero terminated.
            // need to force zero termination!
            ft.enableForcedZeroTerminatedStrings();

            return ft;
        }

        void JITCSVSourceTaskBuilder::processRow(IRBuilder& builder,
                                                 llvm::Value* userData, llvm::Value* parseCode, llvm::Value *parseResult,
                                                 llvm::Value *normalRowCountVar,
                                                 llvm::Value *badRowCountVar,
                                                 llvm::Value *outputRowNumberVar,
                                                 llvm::Value *inputRowPtr,
                                                 llvm::Value *inputRowSize,
                                                 bool terminateEarlyOnLimitCode,
                                                 llvm::Function* processRowFunc) {

#warning "incomplete exception handling here!!!"

            using namespace llvm;
            // check what the parse result was
            // ==> call exception handler or not

            // only account for non-empty lines
            auto lineStart = builder.CreateLoad(builder.CreateGEP(parseResult, {env().i32Const(0), env().i32Const(1)}));
            auto lineEnd = builder.CreateLoad(builder.CreateGEP(parseResult, {env().i32Const(0), env().i32Const(2)}));

            BasicBlock* bbParseError = BasicBlock::Create(env().getContext(), "parse_error", builder.GetInsertBlock()->getParent());
            BasicBlock* bbParseSuccess = BasicBlock::Create(env().getContext(), "parse_success", builder.GetInsertBlock()->getParent());
            BasicBlock* bbProcessEnd = BasicBlock::Create(env().getContext(), "parse_end", builder.GetInsertBlock()->getParent());

            // then go through the pipeline if functions are added (i.e. normal trafo builder stuff)
            auto parseSuccessCond = builder.CreateICmpEQ(parseCode, env().i32Const(0));
            builder.CreateCondBr(parseSuccessCond, bbParseSuccess, bbParseError);

            // -- block begin --
            builder.SetInsertPoint(bbParseSuccess);
            Value *normalRowCount = builder.CreateLoad(normalRowCountVar, "normalRowCount");
            builder.CreateStore(builder.CreateAdd(normalRowCount, env().i64Const(1)), normalRowCountVar);

#ifdef TRACE_PARSER
            env().debugPrint(builder, "normalRowCount: ", builder.CreateLoad(normalRowCountVar));
            // print message that row was parsed!
            env().debugPrint(builder, "parsed line of length w/o errors", builder.CreateSub(builder.CreatePtrToInt(lineEnd, env().i64Type()),
                                                                                        builder.CreatePtrToInt(lineStart, env().i64Type())));
            env().debugCellPrint(builder, lineStart, lineEnd);
#endif

            env().debugPrint(builder, "creating FlattenedTuple from csv result");

            // create whatever needs to be done with this row... (iterator style)
            // other option would be to write this to internal memory format & then spit out another processor...
            // --> doesn't matter, let's use the slow route
            // i.e. normal processor would serialize to Memory here... ==> add this as option too
            // load from csv (if csv input was given, make this later more flexible! better class + refactoring necessary!!!)
            auto ft = createFlattenedTupleFromCSVParseResult(builder, parseResult, _inputRowType);

            env().debugPrint(builder, "FlattenedTuple created.");

            //        // serialize to CSV if option was added
            //        // else serialize to memory
            //        serializeToCSVWriteCallback(builder, ft, userData, "csvRowCallback");
#warning "row number for exception handling is missing here!!!"
            if(processRowFunc) {
                auto& context = env().getContext();


                // dummy: inc normalR

                // debug: print out parsed line, good to check that everything worked...
                auto lineStart = builder.CreateLoad(builder.CreateGEP(parseResult, {_env->i32Const(0), _env->i32Const(1)}));
                auto lineEnd = builder.CreateLoad(builder.CreateGEP(parseResult, {_env->i32Const(0), _env->i32Const(2)}));
                //env().debugCellPrint(builder, lineStart, lineEnd);

                auto res = PipelineBuilder::call(builder, processRowFunc, ft,
                                                 userData, builder.CreateLoad(outputRowNumberVar),
                                                 initIntermediate(builder));

                auto ecCode = builder.CreateZExtOrTrunc(res.resultCode, env().i64Type());
                auto ecOpID = builder.CreateZExtOrTrunc(res.exceptionOperatorID, env().i64Type());
                auto numRowsCreated = builder.CreateZExtOrTrunc(res.numProducedRows, env().i64Type()); // important!!! ==> add to outputRowVar!

                if(terminateEarlyOnLimitCode)
                    generateTerminateEarlyOnCode(builder, ecCode, ExceptionCode::OUTPUT_LIMIT_REACHED);

                // call rtfree all after processing one row...
                _env->freeAll(builder);
                // env().debugPrint(builder, "ecCode of processRowFunc: ", ecCode);

                auto exceptionRaised = builder.CreateICmpNE(ecCode, env().i64Const(ecToI32(ExceptionCode::SUCCESS)));

                llvm::BasicBlock* bbNoException = llvm::BasicBlock::Create(context, "pipeline_ok", builder.GetInsertBlock()->getParent());

                // create exception block, serialize input row depending on result
                // note: creating exception block automatically sets builder to this block
                auto serialized_row = ft.serializeToMemory(builder);
                auto outputRowNumber = builder.CreateLoad(outputRowNumberVar);
                llvm::BasicBlock* curBlock = builder.GetInsertBlock();
                llvm::BasicBlock* bbException = exceptionBlock(builder, userData, ecCode, ecOpID,
                        outputRowNumber, serialized_row.val, serialized_row.size); // generate exception block (incl. ignore & handler if necessary)
                // env().debugPrint(builder, "exception occurred at row: ", builder.CreateLoad(outputRowNumberVar));
                // env().debugPrint(builder, "throwing operator ID: ", ecOpID);
                // env().debugPrint(builder, "code: ", ecCode);
                builder.CreateBr(bbNoException);

                // add branching to previous block
                builder.SetInsertPoint(curBlock);
                builder.CreateCondBr(exceptionRaised, bbException, bbNoException);

                builder.SetInsertPoint(bbNoException); // continue inserts & Co
                // update output row number with how many rows were actually created...
                // outputRowNumber += numRowsCreated
                builder.CreateStore(builder.CreateAdd(builder.CreateLoad(outputRowNumberVar), numRowsCreated), outputRowNumberVar);
                // leave builder in this block...
            }

            builder.CreateBr(bbProcessEnd);
            // -- block end --


            // -- block begin --
            builder.SetInsertPoint(bbParseError);

            BasicBlock *bbNonEmptyLine = BasicBlock::Create(env().getContext(), "parse_error_non_empty_line", builder.GetInsertBlock()->getParent());

            auto isEmptyLine = builder.CreateICmpEQ(lineStart, lineEnd);
            builder.CreateCondBr(isEmptyLine, bbProcessEnd, bbNonEmptyLine);

            builder.SetInsertPoint(bbNonEmptyLine);
#ifdef TRACE_PARSER

            // bad line, print!
            env().debugPrint(builder, "bad row encountered, parse Code is: ", parseCode);
            env().debugCellPrint(builder, lineStart, lineEnd);

            // compute the potential output row number
            // ==> CSV is text based. I.e. put the whole line as exception in there!
            // ==> needs counting here too
            env().debugPrint(builder, "current output row var is: ", builder.CreateLoad(outputRowNumberVar));
#endif


            // create bad Row (string input) with length field to parse quickly...
            // @TODO: create exceptionLength function which stores exception length for different types...



            // old, simply copy full line
            //   auto lineLength = builder.CreatePtrDiff(lineEnd, lineStart);
            //            auto badDataLength = builder.CreateAdd(env().i64Const(sizeof(int64_t)), lineLength);
            //            auto badDataPtr = env().malloc(builder, badDataLength);
            //            builder.CreateStore(lineLength, builder.CreateBitCast(badDataPtr, env().i64ptrType()), true);
            //
            //#if LLVM_VERSION_MAJOR < 9
            //            builder.CreateMemCpy(builder.CreateGEP(badDataPtr, env().i64Const(sizeof(int64_t))), lineStart, lineLength, false);
            //#else
            //            builder.CreateMemCpy(builder.CreateGEP(badDataPtr, env().i64Const(sizeof(int64_t))), 0,  lineStart, 0, lineLength, false);
            //#endif

            // new: use cell info result
            auto cellInfo = _parseRowGen->getCellInfo(builder, parseResult);
            auto badDataPtr = cellInfo.val;
            auto badDataLength = cellInfo.size;

            auto oldBlock = builder.GetInsertBlock();
            // save row here
            // NOTE: BADPARSE_STRING_INPUT is an internal exception ==> resolve via Python pipeline...
            auto bbBadRowException = exceptionBlock(builder, userData,
                                                    env().i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)),
                                                    env().i64Const(_operatorID), builder.CreateLoad(outputRowNumberVar),
                                                    badDataPtr, badDataLength);
            auto curBlock = builder.GetInsertBlock();

            // connect blocks together, continue inserting on this exception block.
            builder.SetInsertPoint(oldBlock);
            builder.CreateBr(bbBadRowException); // always call exception handling here...

            // continue inserting on the last block obtained from builder after calling exception block creation...
            builder.SetInsertPoint(curBlock);

            // add 1 to output row counter ==> save bad row with STRING_BADPARSE_CODE
            // outputRowNumber++;
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(outputRowNumberVar), env().i64Const(1)), outputRowNumberVar);

            Value *badRowCount = builder.CreateLoad(badRowCountVar, "badRowCount");
            builder.CreateStore(builder.CreateAdd(badRowCount, env().i64Const(1)), badRowCountVar);
            builder.CreateBr(bbProcessEnd);
            // -- block end --

            builder.SetInsertPoint(bbProcessEnd);


            // @TODO: row is processed, so free runtime memory allocated for processing this row...
            env().freeAll(builder);
        }

        void JITCSVSourceTaskBuilder::createMainLoop(llvm::Function* read_block_func, bool terminateEarlyOnLimitCode) {
            using namespace llvm;
            using namespace std;

            assert(read_block_func);

            auto& context = env().getContext();

            auto pipFunc = pipeline() ? pipeline()->getFunction() : nullptr;

            auto argUserData = arg("userData");
            auto argInPtr = arg("inPtr");
            auto argInSize = arg("inSize");
            auto argOutNormalRowCount = arg("outNormalRowCount");
            auto argOutBadRowCount = arg("outBadRowCount");
            auto argIgnoreLastRow = arg("ignoreLastRow");

            BasicBlock *bbBody = BasicBlock::Create(context, "entry", read_block_func);

            IRBuilder builder(bbBody);

            _env->debugPrint(builder, "enter main loop");

            // there should be a check if argInSize is 0
            // if so -> handle separately, i.e. return immediately
#warning "add here argInSize > 0 check"


            // compute endptr from args
            Value *endPtr = builder.CreateGEP(argInPtr, argInSize, "endPtr");
            Value *currentPtrVar = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "readPtrVar");
            // later use combi of normal & bad rows
            // dont create extra vars, instead reuse the ones before!
            //            Value *normalRowCountVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "normalRowCountVar");
            //            Value *badRowCountVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "badRowCountVar");
            Value *normalRowCountVar = argOutNormalRowCount;
            Value *badRowCountVar = argOutBadRowCount;
            Value *outputRowNumberVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "outputRowNumberVar");
            builder.CreateStore(argInPtr, currentPtrVar);

            // params passed will be used to
            //            builder.CreateStore(env().i64Const(0), normalRowCountVar);
            //            builder.CreateStore(env().i64Const(0), badRowCountVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(argOutBadRowCount), builder.CreateLoad(argOutNormalRowCount)), outputRowNumberVar);

            // call parse row on data
            auto parseRowF = _parseRowGen->getFunction();
            auto resStructVar = builder.CreateAlloca(_parseRowGen->resultType(), 0, nullptr, "resultVar");
            auto parseCodeVar = builder.CreateAlloca(env().i32Type(), 0, nullptr, "parseCodeVar");

            // do here a
            // do {
            // ...
            // } while (cond);
            // loop

            // loop condition
            BasicBlock *bLoopCond = BasicBlock::Create(context, "loopCond", read_block_func);
            BasicBlock *bLoopDone = BasicBlock::Create(context, "loopDone", read_block_func);
            BasicBlock *bLoopBody = BasicBlock::Create(context, "loopBody", read_block_func);

            // parse first row
            env().debugPrint(builder, "parse row...");
            auto parseCode = builder.CreateCall(parseRowF, {resStructVar, builder.CreateLoad(currentPtrVar, "readPtr"), endPtr}, "parseCode");
            builder.CreateStore(parseCode, parseCodeVar);
            env().printValue(builder, parseCode, "parsed row with code: ");
            auto numParsedBytes = builder.CreateLoad(builder.CreateGEP(resStructVar, {env().i32Const(0), env().i32Const(0)}), "parsedBytes");
            // numParsedBytes should be > 0!
            builder.CreateStore(builder.CreateGEP(builder.CreateLoad(currentPtrVar, "readPtr"), numParsedBytes), currentPtrVar);
            builder.CreateBr(bLoopCond);

            // loop body
            builder.SetInsertPoint(bLoopBody);
#ifdef TRACE_PARSER
            env().debugPrint(builder, "entered loop body, readPtr=", builder.CreatePtrToInt(builder.CreateLoad(currentPtrVar, "readPtr"), env().i64Type()));
#endif

            // process row here -- BEGIN
            env().debugPrint(builder, "process row");
            processRow(builder, argUserData, builder.CreateLoad(parseCodeVar), resStructVar, normalRowCountVar, badRowCountVar, outputRowNumberVar, nullptr, nullptr, terminateEarlyOnLimitCode,pipFunc);
            env().debugPrint(builder, "row done.");
            // end process row here -- END

#ifdef TRACE_PARSER
            env().debugPrint(builder, "process row done");
#endif

            // fetch next row -- BEGIN
#ifdef TRACE_PARSER
            env().debugPrint(builder, "calling parseRowF...");
            env().debugPrint(builder, "endPtr=", builder.CreatePtrToInt(endPtr, env().i64Type()));
            env().debugPrint(builder, "--");
            auto snippet = env().malloc(builder, env().i64Const(512));
#if LLVM_VERSION_MAJOR < 9
            builder.CreateMemCpy(snippet, builder.CreateLoad(currentPtrVar, "readPtr"), 512, 0, true);
#else
            builder.CreateMemCpy(snippet, 0, builder.CreateLoad(currentPtrVar, "readPtr"), 0, env().i64Const(512), true);
#endif
            builder.CreateStore(env().i8Const(' '), builder.CreateGEP(snippet, env().i64Const(506)));
            builder.CreateStore(env().i8Const('.'), builder.CreateGEP(snippet, env().i64Const(507)));
            builder.CreateStore(env().i8Const('.'), builder.CreateGEP(snippet, env().i64Const(508)));
            builder.CreateStore(env().i8Const('.'), builder.CreateGEP(snippet, env().i64Const(509)));
            builder.CreateStore(env().i8Const('\n'), builder.CreateGEP(snippet, env().i64Const(510)));
            builder.CreateStore(env().i8Const('\0'), builder.CreateGEP(snippet, env().i64Const(511)));
            env().debugPrint(builder, "readPtr: ", snippet);
            env().debugPrint(builder, "--");

#endif
            parseCode = builder.CreateCall(parseRowF, {resStructVar, builder.CreateLoad(currentPtrVar, "readPtr"), endPtr}, "parseCode");
            builder.CreateStore(parseCode, parseCodeVar);
            numParsedBytes = builder.CreateLoad(builder.CreateGEP(resStructVar, {env().i32Const(0), env().i32Const(0)}), "parsedBytes");
            // parseRow always returns ok if rows works, however, it could be the case the parse was good but the last
            // line was only partially attained
            // hence, need to check that endptr is 0, else it was a partial parse if this was the last line parsed...

            // numParsedBytes should be > 0!
#ifdef TRACE_PARSER
            env().debugPrint(builder, "numParsedBytes=", numParsedBytes);
#endif

            builder.CreateStore(builder.CreateGEP(builder.CreateLoad(currentPtrVar, "readPtr"), numParsedBytes), currentPtrVar);
            builder.CreateBr(bLoopCond);
            // fetch next row  -- END

            // condition
            builder.SetInsertPoint(bLoopCond);
            Value *cond = builder.CreateICmpULT(builder.CreatePtrToInt(builder.CreateLoad(currentPtrVar, "readPtr"), env().i64Type()),
                                                builder.CreatePtrToInt(endPtr, env().i64Type()));
#ifdef TRACE_PARSER
            env().debugPrint(builder, "readPtr", builder.CreatePtrToInt(builder.CreateLoad(currentPtrVar, "readPtr"), env().i64Type()));
            env().debugPrint(builder, "endPtr", builder.CreatePtrToInt(endPtr, env().i64Type()));
            env().debugPrint(builder, "loopCond: if readPtr < endPtr goto loop_body, else done", cond);
#endif
            builder.CreateCondBr(cond, bLoopBody, bLoopDone);



            // -- block start --
            builder.SetInsertPoint(bLoopDone);
#ifdef TRACE_PARSER
            env().debugPrint(builder, "loop done", env().i64Const(0));
#endif

            // ignore error on last bloc
            // ignore last row error if configured to do

            BasicBlock* bbIf = BasicBlock::Create(context, "if_block", read_block_func);
            BasicBlock* bbElse = BasicBlock::Create(context, "else_block", read_block_func);

            // check whether buffer is '\0' terminated. If so, eof was reached
            // the last parsed char is *(endPtr-1)
            // note that when here in the code argInSize >0 must hold!
            // there is a check in the beginning
            auto endPtrNotEof = builder.CreateICmpNE(builder.CreateLoad(builder.CreateGEP(endPtr, env().i64Const(-1))), env().i8Const(0));
            auto parseErrorInLastRow = builder.CreateICmpNE(builder.CreateLoad(parseCodeVar), env().i32Const(0));
            auto badLastRow = builder.CreateOr(endPtrNotEof, parseErrorInLastRow);
            auto ignoreLastParseError = builder.CreateAnd(badLastRow,
                                                          env().booleanToCondition(builder, argIgnoreLastRow));
#ifdef TRACE_PARSER
            env().debugPrint(builder, "is last val different than eof? ", endPtrNotEof);
        env().debugPrint(builder, "badLastRow", badLastRow);
        env().debugPrint(builder, "parse code is for last row: ", builder.CreateLoad(parseCodeVar));
#endif

            builder.CreateCondBr(ignoreLastParseError, bbIf, bbElse); // maybe add weights here...
            // -- block end --



            // --- block start ---
            // ignore last error block (what to report)
            builder.SetInsertPoint(bbIf);
#ifdef TRACE_PARSER
            env().debugPrint(builder, "ended in if block", env().i64Const(1));
#endif
            // do not just store info, rather reuse this!
            //env().storeIfNotNull(builder, builder.CreateLoad(normalRowCountVar), argOutNormalRowCount);
            //env().storeIfNotNull(builder, builder.CreateLoad(badRowCountVar), argOutBadRowCount);

            // load begin of faulty line if there was an error, else no problem
            auto lineStart = builder.CreateLoad(builder.CreateGEP(resStructVar, {env().i32Const(0), env().i32Const(1)}));
            auto totalReadBytes = builder.CreateSub(builder.CreatePtrToInt(lineStart, env().i64Type()),
                                                    builder.CreatePtrToInt(argInPtr, env().i64Type()));

            auto retVal = builder.CreateSelect(badLastRow, totalReadBytes, argInSize);


            // ignoring potential lat row, this is a function detemrining block.
            // -> write intermediate!
            if(!_intermediateCallbackName.empty())
                writeIntermediate(builder, argUserData, _intermediateCallbackName);

            builder.CreateRet(retVal);
            // --- block end ---


            // -- block start --
            // dont ignore last error, i.e. need to call exception handler perhaps again
            builder.SetInsertPoint(bbElse);
#ifdef TRACE_PARSER
            env().debugPrint(builder, "ended in else block", env().i64Const(1));
#endif
            // process row here -- BEGIN
            processRow(builder, argUserData, builder.CreateLoad(parseCodeVar), resStructVar, normalRowCountVar, badRowCountVar, outputRowNumberVar, nullptr, nullptr, terminateEarlyOnLimitCode, pipFunc);
            // end process row here -- EN

            env().storeIfNotNull(builder, builder.CreateLoad(normalRowCountVar), argOutNormalRowCount);
            env().storeIfNotNull(builder, builder.CreateLoad(badRowCountVar), argOutBadRowCount);
            // this here should be the same AS the inputSize.
            //    totalReadBytes = builder.CreateSub(builder.CreatePtrToInt(builder.CreateLoad(currentPtrVar), env->i64Type()),
            //            builder.CreatePtrToInt(argInPtr, env->i64Type()));
            //    builder.CreateRet(totalReadBytes);

            // last row may have changed intermediate again, so write now!
            if(!_intermediateCallbackName.empty())
                writeIntermediate(builder, argUserData, _intermediateCallbackName);
            builder.CreateRet(argInSize);
            // -- block end --
        }
    }
}