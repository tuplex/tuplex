//
// Created by leonhard on 9/22/22.
//

#include "ListHelper.h"
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
                throw std::runtime_error("merge struct dict type into LLVMEnvironment type system...");
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
                throw std::runtime_error("merge struct dict type into LLVMEnvironment type system...");
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
                builder.CreateStore(value.size, idx_sizes);
            } else if(elementType.isStructuredDictionaryType()) {
                // pointer to the structured dict type!
                throw std::runtime_error("merge struct dict type into LLVMEnvironment type system...");
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
                // pointer to the structured dict type!
                throw std::runtime_error("merge struct dict type into LLVMEnvironment type system...");
            } else if(elementType.isListType()) {
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(size, idx_size);
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }

    }
}