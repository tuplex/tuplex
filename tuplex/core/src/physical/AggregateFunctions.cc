//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/AggregateFunctions.h>
#include <RuntimeInterface.h>

namespace tuplex {
    namespace codegen {
        llvm::Function* createAggregateInitFunction(LLVMEnvironment* env, const std::string& name, const Row& initialValue, const python::Type aggType, decltype(malloc) allocator) {
            using namespace llvm;

            assert(env);
            assert(allocator == malloc || allocator == runtime::rtmalloc); // only two supported alloc functions. Could support any in future...

            Row row = aggType != python::Type::UNKNOWN ? initialValue.upcastedRow(python::Type::propagateToTupleType(aggType)) : initialValue;

            // create new function
            auto func_type = FunctionType::get(env->i64Type(), {env->i8ptrType()->getPointerTo(), env->i64ptrType()}, false);
            auto func = Function::Create(func_type, Function::ExternalLinkage, name, env->getModule().get());

            // set arg names
            auto args = mapLLVMFunctionArgs(func, {"agg", "agg_size"});

            auto body = BasicBlock::Create(env->getContext(), "body", func);
            IRBuilder builder(body);

            auto ft = FlattenedTuple::fromRow(env, builder, row);

            auto size = ft.getSize(builder); assert(size->getType() == env->i64Type());
            builder.CreateStore(size, args["agg_size"]);

            // allocate using the allocator a pointer, then serialize everything to it...
            Value* ptr = nullptr;
            if(allocator == malloc) {
                // use C-malloc
                ptr = env->cmalloc(builder, size);
            } else {
                // use rtmalloc
                ptr = env->malloc(builder, size);
            }

            // serialize tuple to it!
            ft.serialize(builder, ptr);

            builder.CreateStore(ptr, args["agg"]);
            builder.CreateRet(env->i64Const(ecToI64(ExceptionCode::SUCCESS)));

            return func;
        }


        // this function basically should take
        // int64_t combineAggregates(void** aggOut, int64_t* aggOut_size, void* agg, int64_t agg_size)
        llvm::Function *createAggregateCombineFunction(LLVMEnvironment *env, const std::string &name, const UDF &udf,
                                                       const python::Type& aggType,
                                                       decltype(malloc) allocator) {
            using namespace llvm;

            assert(env);
            assert(allocator == malloc || allocator == runtime::rtmalloc); // only two supported alloc functions. Could support any in future...

            // create new function
            auto func_type = FunctionType::get(env->i64Type(), {env->i8ptrType()->getPointerTo(), env->i64ptrType(), env->i8ptrType(), env->i64Type()}, false);
            auto func = Function::Create(func_type, Function::ExternalLinkage, name, env->getModule().get());

            // set arg names
            auto args = mapLLVMFunctionArgs(func, {"out", "out_size", "agg", "agg_size"});

            auto body = BasicBlock::Create(env->getContext(), "body", func);
            IRBuilder builder(body);

            // do not touch agg, this is externally handled.

            // make sure udf is correctly typed
            // check it's valid
            if(udf.empty())
                throw std::runtime_error("UDF is empty in aggregate, this should not happen");
            auto params = udf.getInputParameters();
            assert(params.size() == 2);
            assert(python::Type::propagateToTupleType(std::get<1>(params[0])) == python::Type::propagateToTupleType(aggType));
            assert(python::Type::propagateToTupleType(std::get<1>(params[1])) == python::Type::propagateToTupleType(aggType));

            // deserialize using aggType
            FlattenedTuple ftAgg(env); ftAgg.init(aggType);
            ftAgg.deserializationCode(builder, args["agg"]);

            FlattenedTuple ftOther(env); ftOther.init(aggType);
            ftOther.deserializationCode(builder, builder.CreateLoad(args["out"]));

            // compile the UDF now and call it.
            auto combinedType = python::Type::makeTupleType({aggType, aggType}); // this should be compatible to input type of aggUDF!
            FlattenedTuple ftin(env);
            ftin.init(combinedType);
            ftin.set(builder, {0}, ftOther);
            ftin.set(builder, {1}, ftAgg);

            // compile dependent on udf
            assert(udf.isCompiled());
            auto cf = const_cast<UDF&>(udf).compile(*env);
            // stop if compilation didn't succeed
            if(!cf.good())
                return nullptr;

            auto resultVar = env->CreateFirstBlockAlloca(builder, cf.getLLVMResultType(*env));
            auto exceptionVar = env->CreateFirstBlockAlloca(builder, env->i64Type());
            builder.CreateStore(env->i64Const(ecToI64(ExceptionCode::SUCCESS)), exceptionVar);

            auto exceptionBlock = BasicBlock::Create(env->getContext(), "except", func);
            IRBuilder eb(exceptionBlock);
            eb.get().CreateRet(eb.CreateLoad(exceptionVar));

            auto ftOut = cf.callWithExceptionHandler(builder, ftin, resultVar, exceptionBlock, exceptionVar);

            // if it's variably allocated, free out after combine and realloc...
            if(aggType.isFixedSizeType()) {
                // simply overwrite output!
                ftOut.serialize(builder, builder.CreateLoad(args["out"]));
            } else {
                // free & alloc new output!
                Value* ptr = builder.CreateLoad(args["out"]);
                Value* size = ftOut.getSize(builder);
                if(allocator == malloc) {
                    env->cfree(builder, ptr);
                    ptr = env->cmalloc(builder, size);
                } else {
                    // rtmalloc?
                    // nothing to free
                    ptr = env->malloc(builder, size);
                }

                // serialize to ptr
                ftOut.serialize(builder, ptr);
                builder.CreateStore(ptr, args["out"]);
                builder.CreateStore(size, args["out_size"]);
            }

            builder.CreateRet(builder.CreateLoad(exceptionVar));
            return func;
        }

        // this function basically should take
        // int64_t aggregate(void** aggOut, void* row, int64_t row_size) where aggOut has an 8-byte size field at the beginning (e.g. the format is: size | data)
        llvm::Function *createAggregateFunction(LLVMEnvironment *env, const std::string &name, const UDF &udf,
                                                       const python::Type &aggType,
                                                       const python::Type &rowType,
                                                       decltype(malloc) allocator) {
            using namespace llvm;

            assert(env);
            assert(allocator == malloc || allocator == runtime::rtmalloc); // only two supported alloc functions. Could support any in future...

            // create new function
            auto func_type = FunctionType::get(env->i64Type(), {env->i8ptrType()->getPointerTo(), env->i8ptrType(), env->i64Type()}, false);
            auto func = Function::Create(func_type, Function::ExternalLinkage, name, env->getModule().get());

            // set arg names
            auto args = mapLLVMFunctionArgs(func, {"out", "row", "row_size"});

            auto body = BasicBlock::Create(env->getContext(), "body", func);
            IRBuilder builder(body);

            // pull the row out of the input buffer
            auto buf_offset = env->i64Const(8);
            auto out_row_buf = builder.CreateGEP(builder.CreateLoad(args["out"]), buf_offset);

            // do not touch row, this is externally handled.

            // make sure udf is correctly typed
            // check it's valid
            if(udf.empty())
                throw std::runtime_error("UDF is empty in createAggregateFunction, this should not happen");
            auto params = udf.getInputParameters();
            assert(params.size() == 2);
            assert(python::Type::propagateToTupleType(std::get<1>(params[0])) == python::Type::propagateToTupleType(aggType));
            assert(python::Type::propagateToTupleType(std::get<1>(params[1])) == python::Type::propagateToTupleType(rowType));

            // deserialize using aggType
            FlattenedTuple ftAgg(env); ftAgg.init(aggType);
            ftAgg.deserializationCode(builder, out_row_buf);

            // deserialize using rowType
            FlattenedTuple ftRow(env); ftRow.init(rowType);
            ftRow.deserializationCode(builder, args["row"]);

            // compile the UDF now and call it.
            auto combinedType = python::Type::makeTupleType({aggType, rowType}); // this should be compatible to input type of aggUDF!
            FlattenedTuple ftin(env);
            ftin.init(combinedType);
            ftin.set(builder, {0}, ftAgg);
            ftin.set(builder, {1}, ftRow);

            // compile dependent on udf
            assert(udf.isCompiled());
            auto cf = const_cast<UDF&>(udf).compile(*env);
            // stop if compilation didn't succeed
            if(!cf.good())
                return nullptr;

            auto resultVar = tuplex::codegen::LLVMEnvironment::CreateFirstBlockAlloca(builder, cf.getLLVMResultType(*env));
            auto exceptionVar = tuplex::codegen::LLVMEnvironment::CreateFirstBlockAlloca(builder, env->i64Type());
            builder.CreateStore(env->i64Const(ecToI64(ExceptionCode::SUCCESS)), exceptionVar);

            auto exceptionBlock = BasicBlock::Create(env->getContext(), "except", func);
            IRBuilder eb(exceptionBlock);
            eb.get().CreateRet(eb.CreateLoad(exceptionVar));

            auto ftOut = cf.callWithExceptionHandler(builder, ftin, resultVar, exceptionBlock, exceptionVar);

            // if it's variably allocated, free out after combine and realloc...
            if(aggType.isFixedSizeType()) {
                // simply overwrite output!
                ftOut.serialize(builder, out_row_buf);
            } else {
                // free & alloc new output!
                Value* ptr = builder.CreateLoad(args["out"]);
                Value* size = ftOut.getSize(builder);
                if(allocator == malloc) {
                    env->cfree(builder, ptr);
                    ptr = env->cmalloc(builder, builder.CreateAdd(size, buf_offset));
                } else {
                    // rtmalloc?
                    // nothing to free
                    ptr = env->malloc(builder, builder.CreateAdd(size, buf_offset));
                }

                // serialize to ptr
                auto buf_ptr = builder.CreateGEP(ptr, buf_offset);
                auto size_ptr = builder.CreatePointerCast(ptr, env->i64ptrType());
                builder.CreateStore(size, size_ptr);
                ftOut.serialize(builder, buf_ptr);
                builder.CreateStore(ptr, args["out"]);
            }

            builder.CreateRet(builder.CreateLoad(exceptionVar));
            return func;
        }
    }
}