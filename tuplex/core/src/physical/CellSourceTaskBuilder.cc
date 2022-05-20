//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/CellSourceTaskBuilder.h>
#include <CodegenHelper.h>

namespace tuplex {
    namespace codegen {
        llvm::Function * CellSourceTaskBuilder::build(bool terminateEarlyOnLimitCode) {
            using namespace llvm;

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

            IRBuilder builder(bbEntry);

            // where to store how many output rows are produced from this call.
            Value *outputRowNumberVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "outputRowNumberVar");
            builder.CreateStore(args["rowNumber"], outputRowNumberVar);

            // get FlattenedTuple from deserializing all things + perform value conversions/type checks...
            auto ft = cellsToTuple(builder, cellsPtr, sizesPtr);

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
                    // add here exception block for pipeline errors, serialize tuple etc...
                    auto serialized_row = ft.serializeToMemory(builder);
                    auto outputRowNumber = builder.CreateLoad(outputRowNumberVar);
                    llvm::BasicBlock *curBlock = builder.GetInsertBlock();
                    auto bbException = exceptionBlock(builder, userData, ecCode, ecOpID, outputRowNumber,
                                                      serialized_row.val, serialized_row.size);
                    builder.CreateBr(bbNoException);

                    // add branching to previpus block
                    builder.SetInsertPoint(curBlock);
                    builder.CreateCondBr(exceptionRaised, bbException, bbNoException);

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

        FlattenedTuple CellSourceTaskBuilder::cellsToTuple(llvm::IRBuilder<>& builder, llvm::Value* cellsPtr, llvm::Value* sizesPtr) {

            using namespace llvm;

            auto rowType = restrictRowType(_columnsToSerialize, _fileInputRowType);

            assert(_columnsToSerialize.size() == _fileInputRowType.parameters().size());

            FlattenedTuple ft(&env());
            ft.init(rowType);

            // create flattened tuple & fill its values.
            // Note: might need to do value conversion first!!!
            int rowTypePos = 0;
            for(int i = 0; i < _columnsToSerialize.size(); ++i) {

                // should column be serialized? if so emit type logic!
                if(_columnsToSerialize[i]) {
                    assert(rowTypePos < rowType.parameters().size());
                    auto t = rowType.parameters()[rowTypePos];

                    llvm::Value* isnull = nullptr;

                    // option type? do NULL value interpretation
                    if(t.isOptionType()) {
                        auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)), "x" + std::to_string(i));
                        isnull = nullCheck(builder, val);
                    } else if(t != python::Type::NULLVALUE) {
                        // null check, i.e. raise NULL value exception!
                        auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)), "x" + std::to_string(i));
                        auto null_check = nullCheck(builder, val);

                        // if positive, exception!
                        // else continue!
                        BasicBlock* bbNullCheckPassed = BasicBlock::Create(builder.getContext(), "col" + std::to_string(i) + "_null_check_passed", builder.GetInsertBlock()->getParent());
                        builder.CreateCondBr(null_check, nullErrorBlock(builder), bbNullCheckPassed);
                        builder.SetInsertPoint(bbNullCheckPassed);
                    }

                    t = t.withoutOptions();

                    // values?
                    if(python::Type::STRING == t) {
                        // fill in
                        auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)), "x" + std::to_string(i));
                        auto size = builder.CreateLoad(builder.CreateGEP(sizesPtr, env().i64Const(i)), "s" + std::to_string(i));
                        ft.setElement(builder, rowTypePos, val, size, isnull);
                    } else if(python::Type::BOOLEAN == t) {
                        // conversion code here
                        auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)), "x" + std::to_string(i));
                        auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env().i64Const(i)), "s" + std::to_string(i));
                        auto val = parseBoolean(*_env, builder, valueErrorBlock(builder), cellStr, cellSize, isnull);
                        ft.setElement(builder, rowTypePos, val.val, val.size, isnull);
                    } else if(python::Type::I64 == t) {
                        // conversion code here
                        auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)), "x" + std::to_string(i));
                        auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env().i64Const(i)), "s" + std::to_string(i));
                        auto val = parseI64(*_env, builder, valueErrorBlock(builder), cellStr, cellSize, isnull);
                        ft.setElement(builder, rowTypePos, val.val, val.size, isnull);
                    } else if(python::Type::F64 == t) {
                        // conversion code here
                        auto cellStr = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)), "x" + std::to_string(i));
                        auto cellSize = builder.CreateLoad(builder.CreateGEP(sizesPtr, env().i64Const(i)), "s" + std::to_string(i));
                        auto val = parseF64(*_env, builder, valueErrorBlock(builder), cellStr, cellSize, isnull);
                        ft.setElement(builder, rowTypePos, val.val, val.size, isnull);
                    } else if(python::Type::NULLVALUE == t) {
                        // perform null check only, & set null element depending on result
                        auto val = builder.CreateLoad(builder.CreateGEP(cellsPtr, env().i64Const(i)), "x" + std::to_string(i));
                        isnull = nullCheck(builder, val);

                        // if not null, exception! ==> i.e. ValueError!
                        BasicBlock* bbNullCheckPassed = BasicBlock::Create(builder.getContext(), "col" + std::to_string(i) + "_value_check_passed", builder.GetInsertBlock()->getParent());
                        builder.CreateCondBr(isnull, bbNullCheckPassed, valueErrorBlock(builder));
                        builder.SetInsertPoint(bbNullCheckPassed);
                        ft.setElement(builder, rowTypePos, nullptr, nullptr, env().i1Const(true)); // set NULL (should be ignored)
                    } else {
                        throw std::runtime_error("unsupported type " + t.desc() + " in CSV Parser gen encountered (CellSourceTaskBuilder)");
                    }

                    rowTypePos++;
                }
            }

            return ft;
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

                b.CreateRet(env().i64Const(ecToI64(ExceptionCode::NULLERROR))); // internal error!
            }
            return _nullErrorBlock;
        }

    }
}