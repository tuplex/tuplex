//
// Created by leonhard on 9/22/22.
//

#include <experimental/ListHelper.h>
#include <experimental/StructDictHelper.h>
#include <FlattenedTuple.h>

namespace tuplex {
    namespace codegen {

        void list_free(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            // list should only use runtime allocations -> hence free later!
        }

        void list_init_empty(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            using namespace llvm;
            using namespace std;

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing to do

            // check ptr has correct type
            auto llvm_list_type = env.getOrCreateListType(list_type);
            if(list_ptr->getType() != llvm_list_type->getPointerTo())
                throw std::runtime_error("expected pointer of " + env.getLLVMTypeName(llvm_list_type->getPointerTo()) + " but list_ptr has " + env.getLLVMTypeName(list_ptr->getType()));

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            if(elementType.isSingleValued()) {
                // the list is represented as single i64
                builder.CreateStore(env.i64Const(0), list_ptr);
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {

                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                llvm::Type* llvm_element_type = env.pythonToLLVMType(elementType);
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()), idx_values);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {

                // string array/pyobject array is special. It contains 4 members! capacity, size and then arrays for the values and sizes
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                builder.CreateStore(env.i8nullptr(), idx_values);

                auto idx_sizes = CreateStructGEP(builder, list_ptr, 3);
                builder.CreateStore(env.i8nullptr(), idx_sizes);
            } else if(elementType.isStructuredDictionaryType()) {
                // pointer to the structured dict type!

                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto llvm_element_type = env.getOrCreateStructuredDictType(elementType);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()), idx_values);

            } else if(elementType.isListType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto llvm_element_type = env.getOrCreateListType(elementType);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()), idx_values);
            } else if(elementType.isTupleType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto llvm_element_type = env.getOrCreateTupleType(elementType);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()), idx_values);
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }


        void list_reserve_capacity(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* capacity, bool initialize) {
            // reserve capacity
            // --> free upfront
            list_free(env, builder, list_ptr, list_type);

            using namespace llvm;
            using namespace std;

            assert(list_ptr);
            assert(capacity && capacity->getType() == env.i64Type());

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing to do

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            if(elementType.isSingleValued()) {
                // the list is represented as single i64
                builder.CreateStore(env.i64Const(0), list_ptr);
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {

                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(capacity, idx_capacity);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                llvm::Type* llvm_element_type = env.pythonToLLVMType(elementType);

                // allocate new memory of size sizeof(int64_t) * capacity
                auto data_size = builder.CreateMul(env.i64Const(sizeof(int64_t)), capacity);
                auto data_ptr = builder.CreatePointerCast(env.malloc(builder, data_size), llvm_element_type->getPointerTo());

                if(initialize) {
                    // call memset
                    builder.CreateMemSet(data_ptr, env.i8Const(0), data_size, 0);
                }

                builder.CreateStore(data_ptr, idx_values);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {

                // string array/pyobject array is special. It contains 4 members! capacity, size and then arrays for the values and sizes
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(capacity, idx_capacity);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                auto idx_sizes = CreateStructGEP(builder, list_ptr, 3);

                // allocate new memory of size sizeof(int64_t) * capacity
                auto data_size = builder.CreateMul(env.i64Const(sizeof(int64_t)), capacity);
                auto data_ptr = env.malloc(builder, data_size); // is already i8 pointer...
                auto data_sizes_ptr = builder.CreatePointerCast(env.malloc(builder, data_size), env.i64ptrType());

                if(initialize) {
                    // call memset
                    builder.CreateMemSet(data_ptr, env.i8Const(0), data_size, 0);
                    builder.CreateMemSet(data_sizes_ptr, env.i8Const(0), data_size, 0);
                }

                builder.CreateStore(data_ptr, idx_values);
                builder.CreateStore(data_sizes_ptr, idx_sizes);
            } else if(elementType.isStructuredDictionaryType()) {

                // pointer to the structured dict type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.getOrCreateStructuredDictType(elementType);
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);

                // allocate new memory of size sizeof(int64_t) * capacity
                auto data_size = builder.CreateMul(env.i64Const(sizeof(int64_t)), capacity);
                auto data_ptr = builder.CreatePointerCast(env.malloc(builder, data_size), llvm_element_type->getPointerTo());

                if(initialize) {
                    // call memset
                    builder.CreateMemSet(data_ptr, env.i8Const(0), data_size, 0);
                }
                builder.CreateStore(data_ptr, idx_values);

            } else if(elementType.isListType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.getOrCreateListType(elementType);
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);

                // allocate new memory of size sizeof(int64_t) * capacity
                auto data_size = builder.CreateMul(env.i64Const(sizeof(int64_t)), capacity);
                auto data_ptr = builder.CreatePointerCast(env.malloc(builder, data_size), llvm_element_type->getPointerTo());

                if(initialize) {
                    // call memset
                    builder.CreateMemSet(data_ptr, env.i8Const(0), data_size, 0);
                }
                builder.CreateStore(data_ptr, idx_values);
            } else if(elementType.isTupleType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.getOrCreateTupleType(elementType);
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);

                // allocate new memory of size sizeof(int64_t) * capacity
                auto data_size = builder.CreateMul(env.i64Const(sizeof(int64_t)), capacity);
                auto data_ptr = builder.CreatePointerCast(env.malloc(builder, data_size), llvm_element_type->getPointerTo());

                if(initialize) {
                    // call memset
                    builder.CreateMemSet(data_ptr, env.i8Const(0), data_size, 0);
                }
                builder.CreateStore(data_ptr, idx_values);
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }

        void list_store_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                              llvm::Value* list_ptr,
                              const python::Type& list_type,
                              llvm::Value* idx,
                              const SerializableValue& value) {
            using namespace llvm;
            using namespace std;

            assert(list_ptr);
            assert(idx && idx->getType() == env.i64Type());

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing to do

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            if(elementType.isSingleValued()) {
                // nothing gets stored, ignore.
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                llvm::Type* llvm_element_type = env.pythonToLLVMType(elementType);
                assert(value.val && llvm_element_type == value.val->getType());
                auto ptr = builder.CreateLoad(idx_values);
                auto idx_value = builder.CreateGEP(ptr, idx);
                builder.CreateStore(value.val, idx_value);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                auto idx_sizes = CreateStructGEP(builder, list_ptr, 3);

                // store value.val and value.size
                assert(value.val && value.size);

                auto ptr_values = builder.CreateLoad(idx_values);
                auto ptr_sizes = builder.CreateLoad(idx_sizes);

                auto idx_value = builder.CreateGEP(ptr_values, idx);
                auto idx_size = builder.CreateGEP(ptr_sizes, idx);

                builder.CreateStore(value.val, idx_value);
                builder.CreateStore(value.size, idx_size);
            } else if(elementType.isStructuredDictionaryType()) {
                // pointer to the structured dict type!

                // this is quite simple, store a HEAP allocated pointer.
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.getOrCreateStructuredDictType(elementType);
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);

                // store pointer
                assert(value.val);
                auto ptr = builder.CreateLoad(idx_values);
                auto idx_value = builder.CreateGEP(ptr, idx);

                builder.CreateStore(value.val, idx_value);

            } else if(elementType.isListType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.getOrCreateListType(elementType);
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);

                // store pointer
                assert(value.val);
                auto ptr = builder.CreateLoad(idx_values);
                auto idx_value = builder.CreateGEP(ptr, idx);

                builder.CreateStore(value.val, idx_value);
            } else if(elementType.isTupleType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.getOrCreateTupleType(elementType);
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);

                // store pointer --> should be HEAP allocated pointer. (maybe add attributes to check?)
                assert(value.val);
                auto ptr = builder.CreateLoad(idx_values);
                auto idx_value = builder.CreateGEP(ptr, idx);

                // what type is value.val?
                if(!value.val)
                    throw std::runtime_error("can not store nullptr as tuple");
                if(value.val->getType() == llvm_element_type) {
                    builder.CreateStore(value.val, idx_value); // store struct.tuple to pointer!
                } else if(value.val->getType() == llvm_element_type->getPointerTo()) {
                    // got a pointer, need to load then store!
                    auto tuple = builder.CreateLoad(value.val);
                    builder.CreateStore(tuple, idx_value);
                } else {
                    std::stringstream err;
                    err<<"given value has type "<<env.getLLVMTypeName(value.val->getType())
                       <<" but expected type "<<env.getLLVMTypeName(llvm_element_type)
                       <<" or "<<env.getLLVMTypeName(llvm_element_type->getPointerTo());
                    throw std::runtime_error(err.str());
                }
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }


        void list_store_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* size) {
            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing to do

            // check ptr has correct type
            auto llvm_list_type = env.getOrCreateListType(list_type);
            if(list_ptr->getType() != llvm_list_type->getPointerTo())
                throw std::runtime_error("expected pointer of " + env.getLLVMTypeName(llvm_list_type->getPointerTo()) + " but list_ptr has " + env.getLLVMTypeName(list_ptr->getType()));

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            if(elementType.isSingleValued()) {
                // the list is represented as single i64
                builder.CreateStore(size, list_ptr);
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else if(elementType.isStructuredDictionaryType()) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else if(elementType.isListType()) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else if(elementType.isTupleType()) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }

        llvm::Value* list_length(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return env.i64Const(0); // empty list is well,... guess (drum roll) -> empty.

            // check ptr has correct type
            auto llvm_list_type = env.getOrCreateListType(list_type);
            if(list_ptr->getType() != llvm_list_type->getPointerTo())
                throw std::runtime_error("expected pointer of " + env.getLLVMTypeName(llvm_list_type->getPointerTo()) + " but list_ptr has " + env.getLLVMTypeName(list_ptr->getType()));

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            if(elementType.isSingleValued()) {
                // the list is represented as single i64
                return builder.CreateLoad(list_ptr);
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                return builder.CreateLoad(idx_size);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                return builder.CreateLoad(idx_size);
            } else if(elementType.isStructuredDictionaryType()) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                return builder.CreateLoad(idx_size);
            } else if(elementType.isListType()) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                return builder.CreateLoad(idx_size);
            } else if(elementType.isTupleType()) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                return builder.CreateLoad(idx_size);
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }

        llvm::Value* list_of_structs_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            using namespace llvm;

            assert(list_type.isListType());
            assert(list_type.elementType().isStructuredDictionaryType());

            auto element_type = list_type.elementType();

            // this requires a loop (maybe generate instead function?)
            auto size_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
            // size field requires 8 bytes
            llvm::Value* size = env.i64Const(8);

            // fetch length
            auto len = list_length(env, builder, list_ptr, list_type);
            // now store len x 8 bytes for the individual length of entries.
            // then store len x 8 bytes for the offsets.
            // --> pretty inefficient storage. can get optimized, but no time...

            auto len4 = builder.CreateMul(env.i64Const(8 * 2), len);

            size = builder.CreateAdd(len4, size);
            builder.CreateStore(size, size_var);

            auto loop_i = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), loop_i);

            // start loop going over the size entries (--> this could be vectorized!)
            auto& ctx = env.getContext(); auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "var_size_loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "var_size_loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "var_size_loop_done", F);

            auto idx_values = CreateStructGEP(builder, list_ptr, 2);
            auto ptr_values = builder.CreateLoad(idx_values);

            builder.CreateBr(bLoopHeader);

            {
                // --- header ---
                builder.SetInsertPoint(bLoopHeader);
                // if i < len:
                auto loop_i_val = builder.CreateLoad(loop_i);
                auto loop_cond = builder.CreateICmpULT(loop_i_val, len);
                builder.CreateCondBr(loop_cond, bLoopBody, bLoopExit);
            }


            {
                // --- body ---
                builder.SetInsertPoint(bLoopBody);
                auto loop_i_val = builder.CreateLoad(loop_i);

                // fetch size by calling struct_size on each retrieved pointer! (they should be ALL valid)
                // --> no check here!
                auto item = builder.CreateGEP(ptr_values, loop_i_val);

                // call function! (or better said: emit the necessary code...)
                auto item_size = struct_dict_type_serialized_memory_size(env, builder, item, element_type).val;

                size = builder.CreateAdd(item_size, builder.CreateLoad(size_var));
                builder.CreateStore(size, size_var);

                // inc.
                builder.CreateStore(builder.CreateAdd(env.i64Const(1), loop_i_val), loop_i);
                builder.CreateBr(bLoopHeader);
            }

            builder.SetInsertPoint(bLoopExit);
            return builder.CreateLoad(size_var);
        }

        llvm::Value* list_of_tuples_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            using namespace llvm;

            assert(list_type.isListType());
            assert(list_type.elementType().isTupleType());

            auto element_type = list_type.elementType();

            // this requires a loop (maybe generate instead function?)
            auto size_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
            // size field requires 8 bytes
            llvm::Value* size = env.i64Const(8);

            // fetch length
            auto len = list_length(env, builder, list_ptr, list_type);
            // now store len x 8 bytes for the individual length of entries.
            // then store len x 8 bytes for the offsets.
            // --> pretty inefficient storage. can get optimized, but no time...

            auto len4 = builder.CreateMul(env.i64Const(8 * 2), len);

            size = builder.CreateAdd(len4, size);
            builder.CreateStore(size, size_var);

            auto loop_i = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), loop_i);

            // start loop going over the size entries (--> this could be vectorized!)
            auto& ctx = env.getContext(); auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "var_size_loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "var_size_loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "var_size_loop_done", F);

            auto idx_values = CreateStructGEP(builder, list_ptr, 2);
            auto ptr_values = builder.CreateLoad(idx_values);

            builder.CreateBr(bLoopHeader);

            {
                // --- header ---
                builder.SetInsertPoint(bLoopHeader);
                // if i < len:
                auto loop_i_val = builder.CreateLoad(loop_i);
                auto loop_cond = builder.CreateICmpULT(loop_i_val, len);
                builder.CreateCondBr(loop_cond, bLoopBody, bLoopExit);
            }


            {
                // --- body ---
                builder.SetInsertPoint(bLoopBody);
                auto loop_i_val = builder.CreateLoad(loop_i);

                // fetch size by calling struct_size on each retrieved pointer! (they should be ALL valid)
                // --> no check here!
                auto item = builder.CreateGEP(ptr_values, loop_i_val);

                // call function! (or better said: emit the necessary code...)
                FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(&env, builder, item, element_type);
                auto item_size = ft.getSize(builder);

                size = builder.CreateAdd(item_size, builder.CreateLoad(size_var));
                builder.CreateStore(size, size_var);

                // inc.
                builder.CreateStore(builder.CreateAdd(env.i64Const(1), loop_i_val), loop_i);
                builder.CreateBr(bLoopHeader);
            }

            builder.SetInsertPoint(bLoopExit);
            return builder.CreateLoad(size_var);
        }

        llvm::Value* list_serialized_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            using namespace llvm;
            using namespace std;

            assert(list_ptr);

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return env.i64Const(0); // nothing to do

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            if(elementType.isSingleValued()) {
                // nothing gets stored, ignore.
                return env.i64Const(8); // just store the size field.
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {
                // it's the size field + the size * sizeof(int64_t)
                auto len = list_length(env, builder, list_ptr, list_type);
                auto size = builder.CreateAdd(env.i64Const(8), builder.CreateMul(env.i64Const(8), len));
                return size;
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {

                // this requires a loop (maybe generate instead function?)
                auto size_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
                // size field requires 8 bytes
                llvm::Value* size = env.i64Const(8);

                // fetch length
                auto len = list_length(env, builder, list_ptr, list_type);
                // now store len x 8 bytes for the individual length of entries.
                // then store len x 8 bytes for the offsets.
                // --> pretty inefficient storage. can get optimized, but no time...

                auto len4 = builder.CreateMul(env.i64Const(8 * 2), len);

                size = builder.CreateAdd(len4, size);
                builder.CreateStore(size, size_var);

                auto loop_i = env.CreateFirstBlockAlloca(builder, env.i64Type());
                builder.CreateStore(env.i64Const(0), loop_i);

                // start loop going over the size entries (--> this could be vectorized!)
                auto& ctx = env.getContext(); auto F = builder.GetInsertBlock()->getParent();
                BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "var_size_loop_header", F);
                BasicBlock *bLoopBody = BasicBlock::Create(ctx, "var_size_loop_body", F);
                BasicBlock *bLoopExit = BasicBlock::Create(ctx, "var_size_loop_done", F);

                auto idx_sizes = CreateStructGEP(builder, list_ptr, 3);
                auto ptr_sizes = builder.CreateLoad(idx_sizes);

                builder.CreateBr(bLoopHeader);

                {
                    // --- header ---
                    builder.SetInsertPoint(bLoopHeader);
                    // if i < len:
                    auto loop_i_val = builder.CreateLoad(loop_i);
                    auto loop_cond = builder.CreateICmpULT(loop_i_val, len);
                    builder.CreateCondBr(loop_cond, bLoopBody, bLoopExit);
                }


                {
                    // --- body ---
                    builder.SetInsertPoint(bLoopBody);
                    auto loop_i_val = builder.CreateLoad(loop_i);

                    // fetch size
                    auto idx_size = builder.CreateGEP(ptr_sizes, loop_i_val);
                    auto item_size = builder.CreateLoad(idx_size);
                    size = builder.CreateAdd(item_size, builder.CreateLoad(size_var));
                    builder.CreateStore(size, size_var);

                    // inc.
                    builder.CreateStore(builder.CreateAdd(env.i64Const(1), loop_i_val), loop_i);
                    builder.CreateBr(bLoopHeader);
                }

                builder.SetInsertPoint(bLoopExit);
                return builder.CreateLoad(size_var);
            } else if(elementType.isStructuredDictionaryType()) {
                // pointer to the structured dict type!
                // this is quite involved, therefore put into its own function. basically iterate over elements and then query their size!
                return list_of_structs_size(env, builder, list_ptr, list_type);
            } else if(elementType.isListType()) {
                throw std::runtime_error("list of list size not yet supported");
            } else if(elementType.isTupleType()) {
                return list_of_tuples_size(env, builder, list_ptr, list_type);
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }

        static llvm::Value* list_serialize_fixed_sized_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* dest_ptr) {
            auto elementType = list_type.elementType();
            assert(elementType == python::Type::I64
                   || elementType == python::Type::F64
                   || elementType == python::Type::BOOLEAN);

            // note that size if basically 8 bytes for size + 8 * len
            // need to write in a loop -> yet can speed it up using memcpy!

            // it's the size field + the size * sizeof(int64_t)
            auto len = list_length(env, builder, list_ptr, list_type);
            auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
            builder.CreateStore(len, casted_dest_ptr);
            dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(8));

            // memcpy data_ptr
            auto idx_values = CreateStructGEP(builder, list_ptr, 2);
            auto ptr_values = builder.CreatePointerCast(builder.CreateLoad(idx_values), env.i8ptrType());
            auto data_size = builder.CreateMul(env.i64Const(8), len); // size in bytes!
            builder.CreateMemCpy(dest_ptr, 0, ptr_values, 0, data_size);
            auto size = builder.CreateAdd(env.i64Const(8), data_size);
            return size;
        }

        llvm::Value* list_of_structs_serialize_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* dest_ptr) {
            // quite complex, basically write like strings/pyobjects incl. offset array!

            // skipped for now...

            return env.i64Const(0);
        }

        llvm::Value* list_of_tuples_serialize_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* dest_ptr) {
            // quite complex, basically write like strings/pyobjects incl. offset array!

            // skipped for now...

            return env.i64Const(0);
        }


        llvm::Value* list_serialize_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* dest_ptr) {

            using namespace llvm;
            using namespace std;

            assert(list_ptr);
            assert(list_type.isListType());

            assert(dest_ptr && dest_ptr->getType() == env.i8ptrType());

            if(python::Type::EMPTYLIST == list_type)
                return env.i64Const(0); // nothing to do

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            if(elementType.isSingleValued()) {
                // store length of list to dest_ptr!
                auto len = list_length(env, builder, list_ptr, list_type);
                auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
                builder.CreateStore(len, casted_dest_ptr);
                return env.i64Const(8);
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {
                return list_serialize_fixed_sized_to(env, builder, list_ptr, list_type, dest_ptr);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                throw std::runtime_error("serializing string data not yet supported");
//                // this requires a loop (maybe generate instead function?)
//                auto size_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
//                // size field requires 8 bytes
//                llvm::Value* size = env.i64Const(8);
//
//                // fetch length
//                auto len = list_length(env, builder, list_ptr, list_type);
//                // now store len x 8 bytes for the individual length of entries.
//                // then store len x 8 bytes for the offsets.
//                // --> pretty inefficient storage. can get optimized, but no time...
//
//                auto len4 = builder.CreateMul(env.i64Const(8 * 2), len);
//
//                size = builder.CreateAdd(len4, size);
//                builder.CreateStore(size, size_var);
//
//                auto loop_i = env.CreateFirstBlockAlloca(builder, env.i64Type());
//                builder.CreateStore(env.i64Const(0), loop_i);
//
//                // start loop going over the size entries (--> this could be vectorized!)
//                auto& ctx = env.getContext(); auto F = builder.GetInsertBlock()->getParent();
//                BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "var_size_loop_header", F);
//                BasicBlock *bLoopBody = BasicBlock::Create(ctx, "var_size_loop_body", F);
//                BasicBlock *bLoopExit = BasicBlock::Create(ctx, "var_size_loop_done", F);
//
//                auto idx_sizes = CreateStructGEP(builder, list_ptr, 3);
//                auto ptr_sizes = builder.CreateLoad(idx_sizes);
//
//                builder.CreateBr(bLoopHeader);
//
//                {
//                    // --- header ---
//                    builder.SetInsertPoint(bLoopHeader);
//                    // if i < len:
//                    auto loop_i_val = builder.CreateLoad(loop_i);
//                    auto loop_cond = builder.CreateICmpULT(loop_i_val, len);
//                    builder.CreateCondBr(loop_cond, bLoopBody, bLoopExit);
//                }
//
//
//                {
//                    // --- body ---
//                    builder.SetInsertPoint(bLoopBody);
//                    auto loop_i_val = builder.CreateLoad(loop_i);
//
//                    // fetch size
//                    auto idx_size = builder.CreateGEP(ptr_sizes, loop_i_val);
//                    auto item_size = builder.CreateLoad(idx_size);
//                    size = builder.CreateAdd(item_size, builder.CreateLoad(size_var));
//                    builder.CreateStore(size, size_var);
//
//                    // inc.
//                    builder.CreateStore(builder.CreateAdd(env.i64Const(1), loop_i_val), loop_i);
//                    builder.CreateBr(bLoopHeader);
//                }
//
//                builder.SetInsertPoint(bLoopExit);
//                return builder.CreateLoad(size_var);
            } else if(elementType.isStructuredDictionaryType()) {
                return list_of_structs_serialize_to(env, builder, list_ptr, list_type, dest_ptr);
            } else if(elementType.isTupleType()) {
                return list_of_tuples_serialize_to(env, builder, list_ptr, list_type, dest_ptr);
            } else {
                throw std::runtime_error("Unsupported list to serialize: " + list_type.desc());
            }

            return nullptr;
        }

    }
}