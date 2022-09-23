//
// Created by leonhard on 9/22/22.
//

#include "ListHelper.h"
#include <FlattenedTuple.h>

namespace tuplex {
    namespace codegen {
        void list_init_empty(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            using namespace llvm;
            using namespace std;

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing todo

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
            } else if(elementType == python::Type::I64 || elementType == python::Type::F64
                     || elementType == python::Type::BOOLEAN || elementType.isTupleType()) {

                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                llvm::Type* llvm_element_type = nullptr;
                if(elementType.isTupleType())
                    llvm_element_type = env.pythonToLLVMType(flattenedType(elementType));

                else
                    llvm_element_type = env.pythonToLLVMType(elementType);
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()), idx_values);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT
                      || (elementType.isDictionaryType() && !elementType.isStructuredDictionaryType())) {

                // string array/pyobject array is special. It contains 4 members! capacity, size and then arrays for the values and sizes
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_capacity->getType() == env.i64ptrType());
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
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto llvm_element_type = env.getOrCreateListType(elementType);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()), idx_values);
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }
    }
}