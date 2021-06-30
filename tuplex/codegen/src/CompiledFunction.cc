//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <CompiledFunction.h>
#include <ExceptionCodes.h>
#include <Utils.h>


//// Note: To avoid the problems with TypeFactory (not shared across dynamic librarires!!!)
//// use pointer. In a future release, the code to convert to a PyObject should be codegen
//// hence, this may be not necessary anymore.
//
//// i.e. then it should be as simple as calling
////auto wrapperFunc = mod->getOrInsertFunction("callPythonCode", wrapperFuncType);
//auto wrapperFuncTypePtr = PointerType::get(wrapperFuncType, 0);
//auto wrapperFuncAddr = env.i64Const(reinterpret_cast<int64_t>(callPythonCode));
//auto wrapperFunc = builder.CreateIntToPtr(wrapperFuncAddr, wrapperFuncTypePtr, "callPythonCode");

namespace tuplex {
    namespace codegen {

        FlattenedTuple CompiledFunction::callWithExceptionHandler(llvm::IRBuilder<> &builder,
                                                                  const FlattenedTuple &args,
                                                                  llvm::Value *const resPtr,
                                                                  llvm::BasicBlock *const handler,
                                                                  llvm::Value *const exceptionCode) {
            assert(handler);

            // call the other function, however create exception based serialization failure code
            using namespace llvm;
            BasicBlock *bbSerializationFailure = BasicBlock::Create(args.getEnv()->getContext(), "fallbackSerializationFailure", builder.GetInsertBlock()->getParent());

            llvm::IRBuilder<> b(bbSerializationFailure);
            b.CreateStore(args.getEnv()->i64Const(ecToI32(ExceptionCode::PYTHONFALLBACK_SERIALIZATION)), exceptionCode);
            b.CreateBr(handler);

            auto ret = callWithExceptionHandler(builder, args, resPtr, handler, exceptionCode, bbSerializationFailure);

            // if serialization block has no predecessors, remove it!
            if(!bbSerializationFailure->getSinglePredecessor())
                bbSerializationFailure->eraseFromParent();

            return ret;
        }

        FlattenedTuple CompiledFunction::callWithExceptionHandler(llvm::IRBuilder<> &builder,
                                                                  const FlattenedTuple &args,
                                                                  llvm::Value* const resPtr,
                                                                  llvm::BasicBlock *const handler,
                                                                  llvm::Value *const exceptionCode,
                                                                  llvm::BasicBlock* const failureBlock) {
            using namespace llvm;
            auto &logger = Logger::instance().logger("codegen");

            assert(handler);

            // note: this check here is a little weird since depending on the call convention
            // i.e. whether the function takes a single parameter OR multiple ones args.getTupleType and input_type
            // do correspond or not
//            // first check types
//            if (args.getTupleType() != input_type) {
//                logger.error("input type is " + args.getTupleType().desc() + " but expected " + input_type.desc());
//                return FlattenedTuple(nullptr);
//            }

            assert(args.getEnv());
            LLVMEnvironment &env = *args.getEnv();
            auto& context = env.getContext();

            // init output tuple
            FlattenedTuple fto(args.getEnv());
            fto.init(output_type);


            /// CREATE CALL TO UDF
            // step 1: param type is always a tuple. So check what values need to be passed...
            assert(input_type.isTupleType());
            std::vector<python::Type> typesOfParameters = input_type.parameters();//cg.getParameterTypes().argTypes;

            // fetch UDF implied types
            python::Type retType = output_type;
            python::Type paramType = input_type;

            // call the udf...
            // calls the function and stores retCode in exception Code
            llvm::Function *callingFunction = builder.GetInsertBlock()->getParent();
            assert(callingFunction);
            assert(resPtr);
            assert(resPtr->getType() == fto.getLLVMType()->getPointerTo(0));
            assert(env.i64Type()->getPointerTo(0) == exceptionCode->getType());

            // temp alloc & store flattenedTuple val
            auto inRowPtr = args.loadToPtr(builder);

            // call compiled function or fallback?
            if (!usesFallback()) {
                auto resArgName = function->arg_begin()->getName();
                auto paramArgName = (function->arg_begin() + 1)->getName();

                auto retCode = builder.CreateCall(function, {resPtr, inRowPtr});

                // store exception Code
                builder.CreateStore(retCode, exceptionCode);

                // create condition on whether not equal to success code
                int64_t success = (int64_t) ExceptionCode::SUCCESS;
                auto cond = builder.CreateICmpEQ(retCode, env.i64Const(success));

                // create basic blocks for branching
                BasicBlock *bSuccess = BasicBlock::Create(env.getContext(), "normal", callingFunction);
                builder.CreateCondBr(cond, bSuccess, handler);
                builder.SetInsertPoint(bSuccess);

                fto = FlattenedTuple::fromLLVMStructVal(&env, builder, resPtr, output_type);
            } else {

#warning "fallback + bitmaps not yet implemented"

                if(input_type.isOptional())
                    throw std::runtime_error("optional types + fallback not yet implemented!!!");

                assert(function);
                assert(function_ptr);

                auto mod = builder.GetInsertBlock()->getParent()->getParent();
                assert(mod);

                // serialize input vals to temporary allocated memory
                auto serializedSize = args.getSize(builder);

                // alloc
                auto temporary_input = env.malloc(builder, serializedSize);

                args.serializationCode(builder, temporary_input, serializedSize, failureBlock);

                // now call python function
                // use a simple wrapper function to call to python
                // int32_t callPythonCode(PyObject* func, uint8_t** out, int64_t* out_size, uint8_t* input, int64_t input_size, int32_t in_typeHash, int32_t out_typeHash)
                auto wrapperFuncType = FunctionType::get(Type::getInt32Ty(context), {Type::getInt8PtrTy(context, 0),
                                                                                     Type::getInt8PtrTy(context, 0)->getPointerTo(0),
                                                                                     Type::getInt64PtrTy(context, 0),
                                                                                     Type::getInt8PtrTy(context, 0),
                                                                                     Type::getInt64Ty(context),
                                                                                     Type::getInt32Ty(context),
                                                                                     Type::getInt32Ty(context)}, false);

                auto wrapperFunc = mod->getOrInsertFunction(_pythonInvokeName, wrapperFuncType);
                auto outputVar = builder.CreateAlloca(Type::getInt8PtrTy(context, 0));
                auto outputSizeVar = builder.CreateAlloca(Type::getInt64Ty(context));
                auto resCode = builder.CreateCall(wrapperFunc, {function_ptr,
                                                                outputVar,
                                                                outputSizeVar,
                                                                temporary_input,
                                                                serializedSize,
                                                                env.i32Const(input_type.hash()),
                                                                env.i32Const(output_type.hash())});

                // if code is not zero, jump to exception handler
                builder.CreateStore(builder.CreateSExt(resCode, env.i64Type()), exceptionCode);
                // condition
                auto successCond = builder.CreateICmpEQ(resCode, env.i32Const(ecToI32(ExceptionCode::SUCCESS)));
                BasicBlock* bSuccess = BasicBlock::Create(context, "normal", callingFunction);
                builder.CreateCondBr(successCond, bSuccess, handler);
                builder.SetInsertPoint(bSuccess);

                // deserialize content (in normal block!)
                // call deserializationCode
                FlattenedTuple ftr(&env);

                // flatten out
                ftr.init(output_type);
                ftr.deserializationCode(builder, builder.CreateLoad(outputVar));
                fto = ftr;
            }

            return fto;
        }
    }
}