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
            auto ft_ptr = cellsToTuple(builder, cellsPtr, sizesPtr);
            auto ft = *ft_ptr;

            // if pipeline is set, call it!
            if(pipeline()) {
                auto pipFunc = pipeline()->getFunction();
                if(!pipFunc)
                    throw std::runtime_error("error in pipeline function");

                auto res = PipelineBuilder::call(builder, pipFunc, ft,
                                                 userData,
                                                 builder.CreateLoad(builder.getInt64Ty(), outputRowNumberVar),
                                                 initIntermediate(builder));
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
                    auto outputRowNumber = builder.CreateLoad(builder.getInt64Ty(), outputRowNumberVar);
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

        std::shared_ptr<FlattenedTuple> CellSourceTaskBuilder::cellsToTuple(IRBuilder& builder, llvm::Value* cellsPtr, llvm::Value* sizesPtr) {

            using namespace llvm;

            auto rowType = restrictRowType(_columnsToSerialize, _fileInputRowType);

            assert(_columnsToSerialize.size() == _fileInputRowType.parameters().size());


            // create mapping of target_idx -> original_idx
            std::vector<size_t> index_mapping;
            for(unsigned i = 0; i < _columnsToSerialize.size(); ++i) {
                if(_columnsToSerialize[i])
                    index_mapping.emplace_back(i);
            }

            return decodeCells(*_env, builder, rowType, numCells(),
                                  cellsPtr, sizesPtr, nullErrorBlock(builder), valueErrorBlock(builder), _nullValues, index_mapping);
        }


        llvm::BasicBlock* CellSourceTaskBuilder::valueErrorBlock(IRBuilder &builder) {
            using namespace llvm;

            // create value error block lazily
            if(!_valueErrorBlock) {
                _valueErrorBlock = BasicBlock::Create(env().getContext(), "value_error", builder.GetInsertBlock()->getParent());

                IRBuilder b(_valueErrorBlock);

                // could use here value error as well. However, for internal resolve use badparse string input!
                b.CreateRet(env().i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));
            }

            return _valueErrorBlock;
        }

        llvm::BasicBlock* CellSourceTaskBuilder::nullErrorBlock(IRBuilder &builder) {
            using namespace llvm;
            if(!_nullErrorBlock) {
                _nullErrorBlock = BasicBlock::Create(env().getContext(),
                                               "null_error",
                                                     builder.GetInsertBlock()->getParent());
                IRBuilder b(_nullErrorBlock);
                b.CreateRet(env().i64Const(ecToI64(ExceptionCode::NULLERROR))); // internal error!
            }
            return _nullErrorBlock;
        }

    }
}