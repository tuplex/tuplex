//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/BlockBasedTaskBuilder.h>

// uncomment to debug code generated code
//#define TRACE_PARSER

namespace tuplex {
    namespace codegen {
        llvm::Function* BlockBasedTaskBuilder::createFunction() {
            using namespace llvm;
            using namespace std;

            auto& context = env().getContext();
            FunctionType* read_block_type = FunctionType::get(env().i64Type(), {env().i8ptrType(),
                                                                                env().i8ptrType(),
                                                                                env().i64Type(),
                                                                                env().i64Type()->getPointerTo(0),
                                                                                env().i64Type()->getPointerTo(0),
                                                                                env().getBooleanType()}, false);
            // create function and set argNames
            Function* read_block_func = Function::Create(read_block_type, Function::ExternalLinkage, _desiredFuncName, env().getModule().get());

            std::vector<llvm::Argument*> args;
            for(auto& arg : read_block_func->args()) {
                args.push_back(&arg);
            }

            // rename args
            vector<string> argNames{"userData",
                                    "inPtr",
                                    "inSize",
                                    "outNormalRowCount",
                                    "outBadRowCount",
                                    "ignoreLastRow"};
            for(int i = 0; i < argNames.size(); ++i) {
                args[i]->setName(argNames[i]);
                _args[argNames[i]] = args[i];
            }

            _func = read_block_func;
            return _func;
        }

        llvm::Function* BlockBasedTaskBuilder::createFunctionWithExceptions() {
            using namespace llvm;
            using namespace std;

            auto& context = env().getContext();
            FunctionType* read_block_type = FunctionType::get(env().i64Type(), {env().i8ptrType(),
                                                                                env().i8ptrType(),
                                                                                env().i64Type(),
                                                                                env().i8ptrType()->getPointerTo(0),
                                                                                env().i64Type()->getPointerTo(0),
                                                                                env().i64Type(),
                                                                                env().i64Type()->getPointerTo(0),
                                                                                env().i64Type()->getPointerTo(0),
                                                                                env().getBooleanType()}, false);
            // create function and set argNames
            Function* read_block_func = Function::Create(read_block_type, Function::ExternalLinkage, _desiredFuncName, env().getModule().get());

            std::vector<llvm::Argument*> args;
            for(auto& arg : read_block_func->args()) {
                args.push_back(&arg);
            }

            // rename args
            vector<string> argNames{"userData",
                                    "inPtr",
                                    "inSize",
                                    "expPtrs",
                                    "expPtrSizes",
                                    "numExps",
                                    "outNormalRowCount",
                                    "outBadRowCount",
                                    "ignoreLastRow"};
            for(int i = 0; i < argNames.size(); ++i) {
                args[i]->setName(argNames[i]);
                _args[argNames[i]] = args[i];
            }

            _func = read_block_func;
            return _func;
        }

        void BlockBasedTaskBuilder::setIntermediateInitialValueByRow(const python::Type &intermediateType,
                                                                     const Row &row) {
            _intermediateInitialValue = row;
            _intermediateType = intermediateType;
        }

        void BlockBasedTaskBuilder::setIntermediateWriteCallback(const std::string &callbackName) {
            _intermediateCallbackName = callbackName;
        }

        llvm::BasicBlock* BlockBasedTaskBuilder::exceptionBlock(llvm::IRBuilder<>& builder,
                llvm::Value* userData,
                llvm::Value *exceptionCode,
                                                                llvm::Value *exceptionOperatorID,
                                                                llvm::Value *rowNumber,
                                                                llvm::Value *badDataPtr,
                                                                llvm::Value *badDataLength) {
            // creates new exception block w. handlers and so on
            using namespace llvm;
            auto func = builder.GetInsertBlock()->getParent(); assert(func);
            BasicBlock* block = BasicBlock::Create(env().getContext(), "except", func);
            builder.SetInsertPoint(block); // first block
            auto& context = env().getContext();

            if(!_exceptionHandlerName.empty()) {

                auto eh_func = codegen::exception_handler_prototype(env().getContext(), env().getModule().get(), _exceptionHandlerName);

                // check if ignore codes are present
                if(!_codesToIgnore.empty()) {
                    // create condition and only call handler if the code combination is not to be ignored...
                    BasicBlock* bb = BasicBlock::Create(context, "dispatch_exception", func);
                    BasicBlock* bbDone = BasicBlock::Create(context, "exception_done", func);

                    // create condition & then branch
                    int64_t opID = std::get<0>(_codesToIgnore.front());
                    int64_t code = ecToI32(std::get<1>(_codesToIgnore.front()));
                    Value* ignoreCond = builder.CreateAnd(builder.CreateICmpEQ(env().i64Const(opID), exceptionOperatorID),
                                                          builder.CreateICmpEQ(env().i64Const(code), exceptionCode));
                    for(int i = 1; i < _codesToIgnore.size(); ++i) {
                        opID = std::get<0>(_codesToIgnore[i]);
                        code = ecToI32(std::get<1>(_codesToIgnore[i]));
                        ignoreCond = builder.CreateOr(ignoreCond,
                                                      builder.CreateAnd(builder.CreateICmpEQ(env().i64Const(opID), exceptionOperatorID),
                                                                        builder.CreateICmpEQ(env().i64Const(code), exceptionCode)));
                    }

                    builder.CreateCondBr(ignoreCond, bbDone, bb);
                    builder.SetInsertPoint(bb);

                     // _env->debugPrint(builder, "calling exception functor from BlockBasedTaskBuilder");

                    builder.CreateCall(eh_func, {userData, exceptionCode, exceptionOperatorID, rowNumber, badDataPtr, badDataLength});
                    builder.CreateBr(bbDone);

                    builder.SetInsertPoint(bbDone);
                } else {

                    // _env->debugPrint(builder, "simple call: calling exception functor from BlockBasedTaskBuilder");
                    // _env->debugPrint(builder, "row number of exception is: ", rowNumber);

                    // simple call to exception handler...
                    builder.CreateCall(eh_func, {userData, exceptionCode, exceptionOperatorID, rowNumber, badDataPtr, badDataLength});
                }
            }
            return block;
        }

        llvm::Value * BlockBasedTaskBuilder::initIntermediate(IRBuilder &builder) {
            // return nullptr if unspecified (triggers default behavior w/o intermediate for pipeline)
            if(_intermediateType == python::Type::UNKNOWN)
                return nullptr;

            // map to LLVM struct and alloc intermediate!
            auto llvmType = _env->pythonToLLVMType(_intermediateType);

            // initialize lazily
            if(!_intermediate) {
                auto b = codegen::IRBuilder(getFirstBlockBuilder(builder.get()));

                // now store into var!
                // @TODO: upcast?
                auto ft = FlattenedTuple::fromRow(_env.get(), b.get(), _intermediateInitialValue);
                auto var = ft.loadToPtr(b.get(), "intermediate");
                _intermediate = var;
            }

            assert(_intermediate);

            return _intermediate;
        }

        void BlockBasedTaskBuilder::writeIntermediate(IRBuilder &builder, llvm::Value* userData,
                                                      const std::string &intermediateCallbackName) {
            using namespace llvm;

            FlattenedTuple row = FlattenedTuple::fromLLVMStructVal(_env.get(), builder.get(), _intermediate, _intermediateType);

            auto serialized_row = row.serializeToMemory(builder.get());

            // call callback
            // typedef int64_t(*write_row_f)(void*, uint8_t*, int64_t);
            auto& ctx = env().getContext();
            FunctionType *writeCallback_type = FunctionType::get(ctypeToLLVM<int64_t>(ctx), {ctypeToLLVM<void*>(ctx),
                    ctypeToLLVM<uint8_t*>(ctx),
                            ctypeToLLVM<int64_t>(ctx)}, false);
            auto callback_func = env().getModule()->getOrInsertFunction(intermediateCallbackName, writeCallback_type);
            auto callbackECVal = builder.CreateCall(callback_func, {userData, serialized_row.val, serialized_row.size});
        }

        void BlockBasedTaskBuilder::generateTerminateEarlyOnCode(llvm::IRBuilder<> &builder, llvm::Value *ecCode,
                                                                 ExceptionCode code) {
            using namespace llvm;

            // create block & terminate early!
            auto& ctx = builder.GetInsertBlock()->getContext();
            auto bbEarlyTermination = BasicBlock::Create(ctx, "terminate_early", builder.GetInsertBlock()->getParent());
            auto bbContinue = BasicBlock::Create(ctx, "continue_execution", builder.GetInsertBlock()->getParent());
            auto terminateEarlyCond = builder.CreateICmpEQ(ecCode, env().i64Const(ecToI64(code)));
            builder.CreateCondBr(terminateEarlyCond, bbEarlyTermination, bbContinue);
            builder.SetInsertPoint(bbEarlyTermination);
            builder.CreateRet(ecCode);
            builder.SetInsertPoint(bbContinue);
        }
    }
}