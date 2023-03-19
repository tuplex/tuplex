//
// Created by leonhard on 9/22/22.
//

#include <experimental/ListHelper.h>
#include <experimental/StructDictHelper.h>
#include <codegen/FlattenedTuple.h>

namespace tuplex {
    namespace codegen {

        // forward declare:
        llvm::Value* list_serialize_varitems_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr,
                                                const python::Type& list_type, llvm::Value* dest_ptr,
                                                std::function<llvm::Value*(LLVMEnvironment&, llvm::IRBuilder<>&, llvm::Value*, llvm::Value*)> f_item_size,
                                                std::function<llvm::Value*(LLVMEnvironment&, llvm::IRBuilder<>&, llvm::Value*, llvm::Value*, llvm::Value*)> f_item_serialize_to);

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
            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            if(elementType.isSingleValued()) {
                if(elements_optional) {
                    auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                    builder.CreateStore(env.i64Const(0), idx_capacity);
                    auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                    builder.CreateStore(env.i64Const(0), idx_size);
                    auto idx_opt_values = CreateStructGEP(builder, list_ptr, 2);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                } else {
                    // the list is represented as single i64
                    builder.CreateStore(env.i64Const(0), list_ptr);
                }
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

                if(elements_optional) {
                    auto idx_opt_values = CreateStructGEP(builder, list_ptr, 3);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }

            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {

                // string array/pyobject array is special. It contains 4 members! capacity, size and then arrays for the values and sizes
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);
                auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_size);

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                builder.CreateStore(env.nullConstant(env.i8ptrType()->getPointerTo()), idx_values);

                auto idx_sizes = CreateStructGEP(builder, list_ptr, 3);
                builder.CreateStore(env.nullConstant(env.i64ptrType()), idx_sizes);

                if(elements_optional) {
                    auto idx_opt_values = CreateStructGEP(builder, list_ptr, 4);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }
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

                if(elements_optional) {
                    auto idx_opt_values = CreateStructGEP(builder, list_ptr, 3);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }

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

                if(elements_optional) {
                    auto idx_opt_values = CreateStructGEP(builder, list_ptr, 3);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }
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
                builder.CreateStore(env.nullConstant(llvm_element_type->getPointerTo()->getPointerTo()), idx_values);

                if(elements_optional) {
                    auto idx_opt_values = CreateStructGEP(builder, list_ptr, 3);
                    builder.CreateStore(env.nullConstant(env.i8ptrType()), idx_opt_values);
                }
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }

        // helper function to allocate and store a pointer via rtmalloc + potentially initialize it
        void list_init_array(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, llvm::Value* capacity, size_t struct_index, bool initialize) {
            using namespace llvm;
            using namespace std;

            assert(list_ptr && capacity);

            auto idx_values = CreateStructGEP(builder, list_ptr, struct_index);
            assert(idx_values && idx_values->getType()->isPointerTy());

            auto struct_type = list_ptr->getType()->getPointerElementType();
            assert(struct_type->isStructTy());
            assert(struct_index < struct_type->getStructNumElements());
            llvm::Type* llvm_element_type = struct_type->getStructElementType(struct_index)->getPointerElementType();

            const auto& DL = env.getModule()->getDataLayout();
            // debug
            std::string t_name = env.getLLVMTypeName(llvm_element_type);
            size_t llvm_element_size = DL.getTypeAllocSize(llvm_element_type);

            // allocate new memory of size sizeof(int64_t) * capacity
            auto data_size = builder.CreateMul(env.i64Const(llvm_element_size), capacity);

            // // multiple of alignment (8)
            // data_size = env.roundUpToMultiple(builder, data_size, 8);

            auto data_ptr = builder.CreatePointerCast(env.malloc(builder, data_size), llvm_element_type->getPointerTo());

            if(initialize) {
                // call memset
                builder.CreateMemSet(data_ptr, env.i8Const(0), data_size, 0);
            }

            builder.CreateStore(data_ptr, idx_values);
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
            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            if(elementType.isSingleValued()) {
                if(elements_optional) {
                    // a list consists of 3 fields: capacity, size and a pointer to the members
                    auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                    builder.CreateStore(capacity, idx_capacity);

                    list_init_array(env, builder, list_ptr, capacity, 2, initialize);
                } else
                // the list is represented as single i64
                builder.CreateStore(env.i64Const(0), list_ptr);
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {

                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(capacity, idx_capacity);

                list_init_array(env, builder, list_ptr, capacity, 2, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, capacity, 3, initialize);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {

                // string array/pyobject array is special. It contains 4 members! capacity, size and then arrays for the values and sizes
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(capacity, idx_capacity);

                list_init_array(env, builder, list_ptr, capacity, 2, initialize);
                list_init_array(env, builder, list_ptr, capacity, 3, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, capacity, 4, initialize);
            } else if(elementType.isStructuredDictionaryType()) {

                // pointer to the structured dict type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                list_init_array(env, builder, list_ptr, capacity, 2, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, capacity, 3, initialize);

            } else if(elementType.isListType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                list_init_array(env, builder, list_ptr, capacity, 2, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, capacity, 3, initialize);
            } else if(elementType.isTupleType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                list_init_array(env, builder, list_ptr, capacity, 2, initialize);
                if(elements_optional)
                    list_init_array(env, builder, list_ptr, capacity, 3, initialize);
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }

        SerializableValue list_load_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                              llvm::Value* list_ptr,
                              const python::Type& list_type,
                              llvm::Value* idx) {
            using namespace llvm;
            using namespace std;

            SerializableValue ret;

            assert(list_ptr);
            assert(idx && idx->getType() == env.i64Type());

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                throw std::runtime_error("there's no value to be loaded from an empty list");

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            // if optional elements are used, only store if optional indicator is true!
            BasicBlock* bElementIsNotNull = nullptr;
            BasicBlock* bLoadDone = nullptr;
            BasicBlock* bIsNullLastBlock = nullptr;
            if(elements_optional) {
                unsigned struct_opt_index = -1;
                if(elementType.isSingleValued()) {
                    // nothing gets stored, ignore.
                    struct_opt_index = 2;
                } else if(elementType == python::Type::I64
                          || elementType == python::Type::F64
                          || elementType == python::Type::BOOLEAN) {
                    struct_opt_index = 3;
                } else if(elementType == python::Type::STRING
                          || elementType == python::Type::PYOBJECT) {
                    struct_opt_index = 4;
                } else if(elementType.isStructuredDictionaryType()) {
                    struct_opt_index = 3;
                } else if(elementType.isListType()) {
                    struct_opt_index = 3;
                } else if(elementType.isTupleType()) {
                    struct_opt_index = 3;
                } else {
                    throw std::runtime_error("Unsupported list element type: " + list_type.desc());
                }

                // create blocks
                auto& ctx = env.getContext();
                auto F = builder.GetInsertBlock()->getParent();

                bElementIsNotNull = BasicBlock::Create(ctx, "load_element", F);
                bLoadDone = BasicBlock::Create(ctx, "load_done", F);

                // load whether null or not
                if(list_ptr->getType()->isPointerTy()) {
                    auto idx_nulls = CreateStructGEP(builder, list_ptr, struct_opt_index);
                    auto ptr = builder.CreateLoad(idx_nulls);
                    auto idx_null = builder.CreateGEP(ptr, idx);
                    ret.is_null = builder.CreateTrunc(builder.CreateLoad(idx_null), env.i1Type()); // could be == i8(0) as well
                } else {
                    auto idx_nulls = CreateStructGEP(builder, list_ptr, struct_opt_index);
                    auto ptr = idx_nulls;
                    auto idx_null = builder.CreateGEP(ptr, idx);
                    ret.is_null = builder.CreateTrunc(builder.CreateLoad(idx_null), env.i1Type()); // could be == i8(0) as well
                }

                // jump now according to block!
                bIsNullLastBlock = builder.GetInsertBlock();
                builder.CreateCondBr(ret.is_null, bLoadDone, bElementIsNotNull);
                builder.SetInsertPoint(bElementIsNotNull);
            }

            if(elementType.isSingleValued()) {
                // nothing to load, keep as is.
                ret.is_null = env.i1Const(false);
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {

                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                llvm::Type* llvm_element_type = env.pythonToLLVMType(elementType);
                if(list_ptr->getType()->isPointerTy()) {
                    auto ptr = builder.CreateLoad(idx_values);
                    auto idx_value = builder.CreateGEP(ptr, idx);
                    ret.val = builder.CreateLoad(idx_value);
                } else {
                    auto idx_value = builder.CreateGEP(idx_values, idx);
                    ret.val = builder.CreateLoad(idx_value);
                }
                ret.size = env.i64Const(sizeof(int64_t));
                ret.is_null = env.i1Const(false);
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);
                auto idx_sizes = CreateStructGEP(builder, list_ptr, 3);

                if(list_ptr->getType()->isPointerTy()) {
                    auto ptr_values = builder.CreateLoad(idx_values);
                    auto ptr_sizes = builder.CreateLoad(idx_sizes);

                    auto idx_value = builder.CreateGEP(ptr_values, idx);
                    auto idx_size = builder.CreateGEP(ptr_sizes, idx);

                    ret.val = builder.CreateLoad(idx_value);
                    ret.size = builder.CreateLoad(idx_size);
                } else {
                    auto idx_value = builder.CreateGEP(idx_values, idx);
                    auto idx_size = builder.CreateGEP(idx_sizes, idx);

                    ret.val = builder.CreateLoad(idx_value);
                    ret.size = builder.CreateLoad(idx_size);
                }

            } else if(elementType.isStructuredDictionaryType()) {
                // pointer to the structured dict type!

                // this is quite simple, store a HEAP allocated pointer.
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.getOrCreateStructuredDictType(elementType);
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);

                // load pointer
                auto ptr = builder.CreateLoad(idx_values);
                auto idx_value = builder.CreateGEP(ptr, idx);

                ret.val = builder.CreateLoad(idx_value);
            } else if(elementType.isListType()) {
                // pointers to the list type!
                // similar to above - yet, keep it here extra for more control...
                // a list consists of 3 fields: capacity, size and a pointer to the members
                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
                builder.CreateStore(env.i64Const(0), idx_capacity);

                auto llvm_element_type = env.getOrCreateListType(elementType);
                auto idx_values = CreateStructGEP(builder, list_ptr, 2);

                // load pointer
                auto ptr = builder.CreateLoad(idx_values);
                auto idx_value = builder.CreateGEP(ptr, idx);

                ret.val = builder.CreateLoad(idx_value);
            } else if(elementType.isTupleType()) {
                throw std::runtime_error("tuple-load not yet supported, fix with code below");

                // below is store code:
//                // pointers to the list type!
//                // similar to above - yet, keep it here extra for more control...
//                // a list consists of 3 fields: capacity, size and a pointer to the members
//                auto idx_capacity = CreateStructGEP(builder, list_ptr, 0); assert(idx_capacity->getType() == env.i64ptrType());
//                builder.CreateStore(env.i64Const(0), idx_capacity);
//
//                auto llvm_element_type = env.getOrCreateTupleType(elementType);
//
//                auto ptr_values = CreateStructLoad(builder, list_ptr, 2); // this should be a pointer
//                auto tuple = value.val;
//
//                // load FlattenedTuple from value (this ensures it is a heap ptr)
//                FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(&env, builder, tuple, elementType);
//
//                // this here works.
//                // auto heap_ptr = ft.loadToPtr(builder);
//
//                // must be a heap ptr, else invalid.
//                auto heap_ptr = ft.loadToHeapPtr(builder);
//
//                // // debug:
//                // env.printValue(builder, idx, "storing tuple of type " + elementType.desc() + " at index: ");
//                // env.printValue(builder, ft.getSize(builder), "tuple size is: ");
//                // env.printValue(builder, heap_ptr, "pointer to store: ");
//
//                // store pointer --> should be HEAP allocated pointer. (maybe add attributes to check?)
//                auto target_idx = builder.CreateGEP(ptr_values, idx);
//
//                // store the heap ptr.
//                builder.CreateStore(heap_ptr, target_idx, true);
//
//                // debug:
//                // env.printValue(builder, builder.CreateLoad(target_idx), "pointer stored - post update: ");

            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }

            // connect blocks + create storage for null
            if(elements_optional) {
                assert(bLoadDone);
                auto lastBlock = builder.GetInsertBlock();
                builder.CreateBr(bLoadDone);
                builder.SetInsertPoint(bLoadDone);

                // update val/size with phi based
                auto phi_val = builder.CreatePHI(ret.val->getType(), 2);
                auto phi_size = builder.CreatePHI(env.i64Type(), 2);
                phi_val->addIncoming(ret.val, lastBlock);
                phi_val->addIncoming(env.nullConstant(ret.val->getType()), bIsNullLastBlock);
                phi_size->addIncoming(ret.size, lastBlock);
                phi_size->addIncoming(env.i64Const(0), bIsNullLastBlock);
                ret.val = phi_val;
                ret.size = phi_size;
            }

            return ret;
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

            // use i32 for idx because of LLVM bugs
            idx = builder.CreateZExtOrTrunc(idx, env.i32Type());

            assert(list_type.isListType());
            if(python::Type::EMPTYLIST == list_type)
                return; // nothing to do

            // @TODO: create verify function for list pointer -> only one valid scenario ok...!

            // check ptr has correct type
            auto llvm_list_type = env.getOrCreateListType(list_type);
            if(list_ptr->getType() != llvm_list_type->getPointerTo())
                throw std::runtime_error("expected pointer of " + env.getLLVMTypeName(llvm_list_type->getPointerTo()) + " but list_ptr has " + env.getLLVMTypeName(list_ptr->getType()));

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();
            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            // if optional elements are used, only store if optional indicator is true!
            BasicBlock* bElementIsNotNull = nullptr;
            BasicBlock* bStoreDone = nullptr;
            if(elements_optional) {
                unsigned struct_opt_index = -1;
                if(elementType.isSingleValued()) {
                    // nothing gets stored, ignore.
                    struct_opt_index = 2;
                } else if(elementType == python::Type::I64
                          || elementType == python::Type::F64
                          || elementType == python::Type::BOOLEAN) {
                    struct_opt_index = 3;
                } else if(elementType == python::Type::STRING
                          || elementType == python::Type::PYOBJECT) {
                    struct_opt_index = 4;
                } else if(elementType.isStructuredDictionaryType()) {
                    struct_opt_index = 3;
                } else if(elementType.isListType()) {
                    struct_opt_index = 3;
                } else if(elementType.isTupleType()) {
                    struct_opt_index = 3;
                } else {
                    throw std::runtime_error("Unsupported list element type: " + list_type.desc());
                }

                // create blocks
                auto& ctx = env.getContext();
                auto F = builder.GetInsertBlock()->getParent();

                bElementIsNotNull = BasicBlock::Create(ctx, "store_element", F);
                bStoreDone = BasicBlock::Create(ctx, "store_done", F);

                // need to ALWAYS store null
                auto idx_nulls = CreateStructGEP(builder, list_ptr, struct_opt_index);
                assert(value.is_null);
                auto ptr = builder.CreateLoad(idx_nulls);
                auto idx_null = builder.CreateGEP(ptr, idx);
                builder.CreateStore(builder.CreateZExtOrTrunc(value.is_null, env.i8Type()), idx_null);

                // jump now according to block!
                builder.CreateCondBr(value.is_null, bStoreDone, bElementIsNotNull);
                builder.SetInsertPoint(bElementIsNotNull);
            }


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

                auto ptr_values = CreateStructLoad(builder, list_ptr, 2); // this should be a pointer
                auto tuple = value.val;

                // load FlattenedTuple from value (this ensures it is a heap ptr)
                FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(&env, builder, tuple, elementType);

                // this here works.
                // auto heap_ptr = ft.loadToPtr(builder);

                // must be a heap ptr, else invalid.
                auto heap_ptr = ft.loadToHeapPtr(builder);

                // // debug:
                // env.printValue(builder, idx, "storing tuple of type " + elementType.desc() + " at index: ");
                // env.printValue(builder, ft.getSize(builder), "tuple size is: ");
                // env.printValue(builder, heap_ptr, "pointer to store: ");

                // store pointer --> should be HEAP allocated pointer. (maybe add attributes to check?)
                auto target_idx = builder.CreateGEP(ptr_values, idx);

                // debug print:
                // env.printValue(builder, builder.CreateLoad(target_idx), "pointer currently stored: ");
                // {
                //     env.debugPrint(builder, "-- check of heap_ptr --");
                //     auto tuple_type = elementType;
                //     auto num_tuple_elements = tuple_type.parameters().size();
                //     llvm::Value* item = builder.CreateLoad(heap_ptr); // <-- this will be wrong. need to fix that in order to work.
                //     //item = heap_ptr; // <-- this should work (?)
                //
                //     auto item_type_name = env.getLLVMTypeName(item->getType());
                //     std::cout<<"[DEBUG] item type anme: "<<item_type_name<<std::endl;
                //     auto ft_check = FlattenedTuple::fromLLVMStructVal(&env, builder, item, tuple_type);
                //     for(unsigned i = 0; i < num_tuple_elements; ++i) {
                //         auto item_size = ft_check.getSize(i);
                //         auto element_type = tuple_type.parameters()[i];
                //         env.printValue(builder, item_size, "size of tuple element " + std::to_string(i) + " of type " + element_type.desc() + " is: ");
                //         env.printValue(builder, ft_check.get(i), "content of tuple element " + std::to_string(i) + " of type " + element_type.desc() + " is: ");
                //     }
                //     env.debugPrint(builder, "-- check end --");
                // }

                // store the heap ptr.
                builder.CreateStore(heap_ptr, target_idx, true);

                // debug:
                // env.printValue(builder, builder.CreateLoad(target_idx), "pointer stored - post update: ");

            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }

            // connect blocks + create storage for null
            if(elements_optional) {
                assert(bStoreDone);
                builder.CreateBr(bStoreDone);
                builder.SetInsertPoint(bStoreDone);
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

            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            if(elementType.isSingleValued()) {
                if(elements_optional) {
                    // capacity is field 0...
                    auto idx_size = CreateStructGEP(builder, list_ptr, 1); assert(idx_size->getType() == env.i64ptrType());
                    builder.CreateStore(size, idx_size);
                } else {
                    // the list is represented as single i64
                    builder.CreateStore(size, list_ptr);
                }
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
            // if(list_ptr->getType() != llvm_list_type->getPointerTo())
            //    throw std::runtime_error("expected pointer of " + env.getLLVMTypeName(llvm_list_type->getPointerTo()) + " but list_ptr has " + env.getLLVMTypeName(list_ptr->getType()));

            // cf. now getOrCreateListType(...) ==> different layouts depending on element type.
            // init accordingly.
            auto elementType = list_type.elementType();

            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            // check that list type is supported
            if(!(elementType.isSingleValued()
            || elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN
                      || elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT
                      || elementType.isStructuredDictionaryType()
                      || elementType.isListType()
                      || elementType.isTupleType())) {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }

            // shorten the code below
            if(elementType.isSingleValued() && !elements_optional) {
                if(list_ptr->getType()->isPointerTy())
                    // the list is represented as single i64
                    return builder.CreateLoad(list_ptr);
                else {
                    assert(list_ptr->getType() == env.i64Type());
                    return list_ptr;
                }
            } else {
                auto size_position = 1;

                if(list_ptr->getType()->isPointerTy()) {
                    auto idx_size = CreateStructGEP(builder, list_ptr, size_position); assert(idx_size->getType() == env.i64ptrType());
                    return builder.CreateLoad(idx_size);
                } else {
                    return builder.CreateExtractValue(list_ptr, std::vector<unsigned>(1, size_position));
                }
            }
        }

        llvm::Value* list_of_structs_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            using namespace llvm;

            // should be same as in serialize_to!!!
            auto f_struct_element_size = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.isStructuredDictionaryType());

                auto ptr_values = CreateStructLoad(builder, list_ptr, 2);

                auto item = builder.CreateGEP(ptr_values, index);
                auto item_size = struct_dict_serialized_memory_size(env, builder, item, element_type).val;

                return item_size;
            };

            return list_of_varitems_serialized_size(env, builder, list_ptr, list_type, f_struct_element_size);
        }

        llvm::Value* list_of_lists_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type) {
            // new

            // define proper helper functions here
            auto list_get_list_item_size = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.isListType());

                auto ptr_values = CreateStructLoad(builder, list_ptr, 2);
                assert(ptr_values->getType()->isPointerTy());

                // fetch size by calling struct_size on each retrieved pointer! (they should be ALL valid)
                // --> no check here!
                auto item = builder.CreateGEP(ptr_values, index);

                // call function! (or better said: emit the necessary code...)
                auto item_size = list_serialized_size(env, builder, item, element_type);

                return item_size;
            };

            return list_of_varitems_serialized_size(env, builder, list_ptr, list_type, list_get_list_item_size);
        }

        FlattenedTuple get_tuple_item(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* index) {

            assert(index && index->getType() == env.i64Type());
            auto element_type = list_type.elementType();
            assert(element_type.isTupleType());


            // what type is there?
            unsigned ptr_position = 2;
            llvm::Value* ptr_values = nullptr;
            if(list_ptr->getType()->isPointerTy()) {
                auto idx_ptr = CreateStructGEP(builder, list_ptr, ptr_position);
                //assert(idx_size->getType() == env.i64ptrType());
                ptr_values = builder.CreateLoad(idx_ptr);
            } else {
                ptr_values = builder.CreateExtractValue(list_ptr, std::vector<unsigned>(1, ptr_position));
            }

            //auto ptr_values = CreateStructGEP(builder, list_ptr, 2); // should be struct.tuple**

            auto t_ptr_values = env.getLLVMTypeName(ptr_values->getType());
            // now load the i-th element from ptr_values as struct.tuple*
            auto item_ptr = builder.CreateInBoundsGEP(ptr_values, std::vector<llvm::Value*>(1, index));
            auto t_item_ptr = env.getLLVMTypeName(item_ptr->getType()); // should be struct.tuple**

            auto item = builder.CreateLoad(item_ptr); // <-- should be struct.tuple*
            auto t_item = env.getLLVMTypeName(item->getType());
            assert(item->getType()->isPointerTy()); // <-- this here fails...

            // debug print: retrieve heap ptr
            // env.printValue(builder, item, "stored heap ptr is: ");
            //
            // // printing the i64 from the pointer...
            // auto dbg_ptr = builder.CreatePointerCast(item, env.i8ptrType());
            // for(unsigned i = 0; i < 10; ++i) {
            //     auto i64_val = builder.CreateLoad(builder.CreatePointerCast(dbg_ptr, env.i64ptrType()));
            //     dbg_ptr = builder.CreateGEP(dbg_ptr, env.i64Const(sizeof(int64_t)));
            //     env.printValue(builder, i64_val, "bytes " + std::to_string(i * 8) + "-" + std::to_string((i+1)*8) + ": ");
            // }


            // call function! (or better said: emit the necessary code...)
            FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(&env, builder, item, element_type);
            return ft;
        }


        llvm::Value* list_of_tuples_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                         llvm::Value* list_ptr, const python::Type& list_type) {

            auto f_tuple_element_size = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                    llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                auto element_type = list_type.elementType();
                assert(element_type.isTupleType());

                if(python::Type::EMPTYTUPLE == element_type)
                    return env.i64Const(0);

                auto ft = get_tuple_item(env, builder, list_ptr, list_type, index);

                // get size
                auto item_size = ft.getSize(builder);
                return item_size;
            };

            auto l_size = list_of_varitems_serialized_size(env, builder, list_ptr, list_type, f_tuple_element_size);

            return l_size;
        }

        static llvm::Value* list_serialize_fixed_sized_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                                          llvm::Value* list_ptr,
                                                          const python::Type& list_type,
                                                          llvm::Value* dest_ptr) {
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

        llvm::Value* list_of_structs_serialize_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                                  llvm::Value* list_ptr, const python::Type& list_type,
                                                  llvm::Value* dest_ptr) {
            // quite complex, basically write like strings/pyobjects incl. offset array!

            // -> there may be structs that are of fixed size. Could write them optimized. for now, write as var always...
            auto f_struct_element_size = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.isStructuredDictionaryType());

                auto ptr_values = CreateStructLoad(builder, list_ptr, 2);

                auto item = builder.CreateGEP(ptr_values, index);
                auto item_size = struct_dict_serialized_memory_size(env, builder, item, element_type).val;

                return item_size;
            };

            auto f_struct_element_serialize_to = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                                            llvm::Value* list_ptr, llvm::Value* index, llvm::Value* dest_ptr) -> llvm::Value* {

                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.isStructuredDictionaryType());

                auto ptr_values = CreateStructLoad(builder, list_ptr, 2);
                auto item = builder.CreateGEP(ptr_values, index);

                // call struct dict serialize
                auto s_result = struct_dict_serialize_to_memory(env, builder, item, element_type, dest_ptr);
                return s_result.val;
            };


            return list_serialize_varitems_to(env, builder, list_ptr, list_type,
                                              dest_ptr, f_struct_element_size, f_struct_element_serialize_to);
        }

        llvm::Value* list_of_tuples_serialize_to(LLVMEnvironment& env,
                                                 llvm::IRBuilder<>& builder,
                                                 llvm::Value* list_ptr,
                                                 const python::Type& list_type,
                                                 llvm::Value* dest_ptr) {
            // quite complex, basically write like strings/pyobjects incl. offset array!

            // -> there may be structs that are of fixed size. Could write them optimized.
            // for now, write as var always...
            // -> use same Lambda func for size helper func...
            auto f_tuple_element_size = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();
                assert(element_type.isTupleType());

                if(python::Type::EMPTYTUPLE == element_type)
                    return env.i64Const(0);

                auto ft = get_tuple_item(env, builder, list_ptr, list_type, index);

                // get size
                auto item_size = ft.getSize(builder);
                return item_size;
            };

            auto f_tuple_element_serialize_to = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                                             llvm::Value* list_ptr, llvm::Value* index, llvm::Value* dest_ptr) -> llvm::Value* {

                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                if(python::Type::EMPTYTUPLE == element_type)
                    return env.i64Const(0);

                auto ft = get_tuple_item(env, builder, list_ptr, list_type, index);
                auto s_size = ft.serialize(builder, dest_ptr);
                return s_size;
            };

            // debug print
            // env.debugPrint(builder, "---\nstarting list serialize\n---");

            return list_serialize_varitems_to(env, builder, list_ptr, list_type,
                                              dest_ptr, f_tuple_element_size, f_tuple_element_serialize_to);
        }

        // generic list of variable fields serialization function
        llvm::Value* list_serialize_varitems_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr,
                                               const python::Type& list_type, llvm::Value* dest_ptr,
                                                std::function<llvm::Value*(LLVMEnvironment&, llvm::IRBuilder<>&, llvm::Value*, llvm::Value*)> f_item_size,
                                                std::function<llvm::Value*(LLVMEnvironment&, llvm::IRBuilder<>&, llvm::Value*, llvm::Value*, llvm::Value*)> f_item_serialize_to) {
            using namespace llvm;

            assert(dest_ptr && dest_ptr->getType() == env.i8ptrType());

            // serialization format of this is as follows:

            // -> (offset|size) is packed
            // len | (offset|size) | .... | (offset|size) | item1 | .... |item_len

            // fetch length of list
            auto len = list_length(env, builder, list_ptr, list_type);

            // debug print:
            // env.printValue(builder, len, "serializing var item list of length: ");

            // write len of list to dest_ptr
            auto ptr = dest_ptr;
            builder.CreateStore(len, builder.CreatePointerCast(ptr, env.i64ptrType()));
            // env.debugPrint(builder, "stored length to pointer address");
            ptr = builder.CreateGEP(ptr, env.i64Const(sizeof(int64_t)));

            auto info_start_ptr = ptr;
            auto var_ptr = builder.CreateGEP(ptr, builder.CreateMul(len, env.i64Const(sizeof(int64_t)))); // offset from current is len * sizeof(int64_t)

            // generate loop to go over items.
            auto loop_i = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), loop_i);

            // start loop going over the size entries (--> this could be vectorized!)
            auto& ctx = env.getContext(); auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "list_of_var_items_serialize_loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "list_of_var_items_serialize_loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "list_of_var_items_serialize_loop_done", F);

            auto varlen_bytes_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), varlen_bytes_var);
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

                // env.printValue(builder, loop_i_val, "serializing item: ");

                // get item's serialized size
                auto item_size = f_item_size(env, builder, list_ptr, loop_i_val);

                // env.printValue(builder, item_size, "item size to serialize is: ");

                auto varlen_bytes = builder.CreateLoad(varlen_bytes_var);
                assert(varlen_bytes->getType() == env.i64Type());
                // env.printValue(builder, varlen_bytes, "so far serialized (bytes): ");
                auto item_dest_ptr = builder.CreateGEP(var_ptr, varlen_bytes);
                // env.debugPrint(builder, "calling item func");
                auto actual_size = f_item_serialize_to(env, builder, list_ptr, loop_i_val, item_dest_ptr);
                //env.printValue(builder, actual_size, "actually realized item size is: ");
                // env.debugPrint(builder, "item func called.");
                // offset is (numSerialized - serialized_idx) * sizeof(int64_t) + varsofar.
                // i.e. here (len - loop_i_val) * 8 + var
                auto offset = builder.CreateAdd(varlen_bytes, builder.CreateMul(env.i64Const(8), builder.CreateSub(len, loop_i_val)));

                // env.printValue(builder, offset, "field offset:  ");

                // save offset and item size.
                // where to write this? -> current
                auto info = pack_offset_and_size(builder, offset, item_size);

                // store info & inc pointer
                auto info_ptr = builder.CreateGEP(info_start_ptr, builder.CreateMul(env.i64Const(8), loop_i_val));
                builder.CreateStore(info, builder.CreatePointerCast(info_ptr, env.i64ptrType()));

                // inc. variable length bytes serialized so far
                builder.CreateStore(builder.CreateAdd(varlen_bytes, item_size), varlen_bytes_var);

                // inc. loop counter
                builder.CreateStore(builder.CreateAdd(env.i64Const(1), loop_i_val), loop_i);
                builder.CreateBr(bLoopHeader);
            }

            builder.SetInsertPoint(bLoopExit);

            // calculate actual size
            auto varlen_bytes = builder.CreateLoad(varlen_bytes_var);

            auto size = builder.CreateAdd(env.i64Const(8), builder.CreateAdd(builder.CreateMul(env.i64Const(8), len), varlen_bytes));
            assert(size->getType() == env.i64Type());

            // env.printValue(builder, size, "total list size serialized: ");

            return size;
        }

        // generic list of variable fields serialization function
        llvm::Value* list_of_varitems_serialized_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr,
                                                const python::Type& list_type,
                                                std::function<llvm::Value*(LLVMEnvironment&, llvm::IRBuilder<>&, llvm::Value*, llvm::Value*)> f_item_size) {
            using namespace llvm;

            // serialization format of this is as follows:

            // -> (offset|size) is packed
            // len | (offset|size) | .... | (offset|size) | item1 | .... |item_len

            // fetch length of list
            auto len = list_length(env, builder, list_ptr, list_type);

            // env.printValue(builder, len, "---\ncomputing serialized size of list with #elements = ");


            // generate loop to go over items.
            auto loop_i = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), loop_i);

            // start loop going over the size entries (--> this could be vectorized!)
            auto& ctx = env.getContext(); auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "list_of_var_items_size_loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "list_of_var_items_size_loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "list_of_var_items_size_loop_done", F);

            auto varlen_bytes_var = env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), varlen_bytes_var);
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

                // get item's serialized size
                auto item_size = f_item_size(env, builder, list_ptr, loop_i_val);
                auto varlen_bytes = builder.CreateLoad(varlen_bytes_var);

                // inc. variable length bytes serialized so far
                builder.CreateStore(builder.CreateAdd(varlen_bytes, item_size), varlen_bytes_var);

                // inc. loop counter
                builder.CreateStore(builder.CreateAdd(env.i64Const(1), loop_i_val), loop_i);
                builder.CreateBr(bLoopHeader);
            }

            builder.SetInsertPoint(bLoopExit);

            // calculate actual size
            auto varlen_bytes = builder.CreateLoad(varlen_bytes_var);
            auto size = builder.CreateAdd(env.i64Const(8), builder.CreateAdd(builder.CreateMul(env.i64Const(8), len), varlen_bytes));
            assert(size->getType() == env.i64Type());
            return size;
        }


        llvm::Value* list_of_lists_serialize_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr,
                                                const python::Type& list_type, llvm::Value* dest_ptr) {

            // define proper helper functions here
            // @TODO: refactor s.t. this here is the same as the in the list size function.
            auto list_get_list_item_size = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.isListType());

                auto ptr_values = CreateStructLoad(builder, list_ptr, 2);
                assert(ptr_values->getType()->isPointerTy());

                // fetch size by calling struct_size on each retrieved pointer! (they should be ALL valid)
                // --> no check here!
                auto item = builder.CreateGEP(ptr_values, index);

                // call function! (or better said: emit the necessary code...)
                auto item_size = list_serialized_size(env, builder, item, element_type);

                return item_size;
            };

            auto list_serialize_list_item = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, llvm::Value* index, llvm::Value* dest_ptr) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type.isListType());

                auto ptr_values = CreateStructLoad(builder, list_ptr, 2);
                assert(ptr_values->getType()->isPointerTy());

                // fetch size by calling struct_size on each retrieved pointer! (they should be ALL valid)
                // --> no check here!
                auto item = builder.CreateGEP(ptr_values, index);

                // call function! (or better said: emit the necessary code...)
                auto item_size = list_serialize_to(env, builder, item, element_type, dest_ptr);

                return item_size;
            };

            llvm::Value* l_size = list_serialize_varitems_to(env, builder, list_ptr, list_type, dest_ptr, list_get_list_item_size, list_serialize_list_item);
            return l_size;
        }

        llvm::Value* list_serialized_size_str_like(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr,
        const python::Type& list_type) {
            auto list_get_str_like_item_size = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type == python::Type::STRING || element_type == python::Type::PYOBJECT);

                auto idx_sizes = CreateStructGEP(builder, list_ptr, 3);
                auto ptr_sizes = builder.CreateLoad(idx_sizes);
                auto idx_size = builder.CreateGEP(ptr_sizes, index);
                auto item_size = builder.CreateLoad(idx_size);
                return item_size;
            };

            return list_of_varitems_serialized_size(env, builder, list_ptr, list_type, list_get_str_like_item_size);
        }

        llvm::Value* list_serialize_str_like_to(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr,
                                                const python::Type& list_type, llvm::Value* dest_ptr) {
            auto list_get_str_like_item_size = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr, llvm::Value* index) -> llvm::Value* {
                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type == python::Type::STRING || element_type == python::Type::PYOBJECT);

                auto idx_sizes = CreateStructGEP(builder, list_ptr, 3);
                auto ptr_sizes = builder.CreateLoad(idx_sizes);
                auto idx_size = builder.CreateGEP(ptr_sizes, index);
                auto item_size = builder.CreateLoad(idx_size);
                return item_size;
            };

            auto list_serialize_str_like_item = [list_type](LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                    llvm::Value* list_ptr, llvm::Value* index, llvm::Value* dest_ptr) -> llvm::Value* {

                assert(index && index->getType() == env.i64Type());

                auto element_type = list_type.elementType();

                assert(element_type == python::Type::STRING || element_type == python::Type::PYOBJECT);

                auto ptr_values = CreateStructLoad(builder, list_ptr, 2);
                auto ptr_sizes = CreateStructLoad(builder, list_ptr, 3);

                assert(ptr_values->getType() == env.i8ptrType()->getPointerTo(0));
                assert(ptr_sizes->getType() == env.i64ptrType());

                auto idx_value = builder.CreateGEP(ptr_values, index);
                auto idx_size = builder.CreateGEP(ptr_sizes, index);

                auto item_size = builder.CreateLoad(idx_size);
                auto str_ptr = builder.CreateLoad(idx_value);
                assert(item_size->getType() == env.i64Type());
                assert(str_ptr->getType() == env.i8ptrType());

                // env.printValue(builder, str_ptr, "serializing str: ");
                // env.printValue(builder, item_size, "str size is: ");

                // memcpy contents
                builder.CreateMemCpy(dest_ptr, 0, str_ptr, 0, item_size);

                return item_size;
            };

            llvm::Value* l_size = list_serialize_varitems_to(env, builder, list_ptr, list_type, dest_ptr,
                                                             list_get_str_like_item_size, list_serialize_str_like_item);
            return l_size;
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
                return list_serialize_str_like_to(env, builder, list_ptr, list_type, dest_ptr);
            } else if(elementType.isStructuredDictionaryType()) {
                return list_of_structs_serialize_to(env, builder, list_ptr, list_type, dest_ptr);
            } else if(elementType.isTupleType()) {
                return list_of_tuples_serialize_to(env, builder, list_ptr, list_type, dest_ptr);
            } else if(elementType.isListType()) {
                return list_of_lists_serialize_to(env, builder, list_ptr, list_type, dest_ptr);
            } else {
                throw std::runtime_error("Unsupported list to serialize: " + list_type.desc());
            }

            return nullptr;
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

            auto elements_optional = elementType.isOptionType();
            if(elements_optional)
                elementType = elementType.getReturnType();

            // optional? => add size!
            llvm::Value* opt_size = env.i64Const(0);
            if(elements_optional) {
                auto len = list_length(env, builder, list_ptr, list_type);
                // store 1 byte?
                opt_size = builder.CreateMul(env.i64Const(1), len);
                // for efficiency, round up to multiple of 8. (alignment)
                opt_size = env.roundUpToMultiple(builder, opt_size, sizeof(int64_t));
            }

            if(elementType.isSingleValued()) {
                if(elements_optional) {
                    return builder.CreateAdd(env.i64Const(8), opt_size);
                } else {
                    // nothing gets stored, ignore.
                    return env.i64Const(8); // just store the size field.
                }
            } else if(elementType == python::Type::I64
                      || elementType == python::Type::F64
                      || elementType == python::Type::BOOLEAN) {
                // it's the size field + the size * sizeof(int64_t) + opt_size
                auto len = list_length(env, builder, list_ptr, list_type);
                llvm::Value* l_size = builder.CreateAdd(env.i64Const(8), builder.CreateMul(env.i64Const(8), len));
                if(elements_optional)
                    l_size = builder.CreateAdd(l_size, opt_size);
                l_size = builder.CreateAdd(l_size, opt_size);
                return l_size;
            } else if(elementType == python::Type::STRING
                      || elementType == python::Type::PYOBJECT) {

                if(elements_optional)
                    throw std::runtime_error("Option[str] or Option[pyobject] serialization of list elements not yet supported");

                // new
                return list_serialized_size_str_like(env, builder, list_ptr, list_type);

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
//                auto ptr_sizes = CreateStructLoad(builder, list_ptr, 3);
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
//
//                llvm::Value* l_size = builder.CreateLoad(size_var);
//                l_size = builder.CreateAdd(l_size, opt_size);
//                return l_size;
            } else if(elementType.isStructuredDictionaryType()) {
                // pointer to the structured dict type!
                // this is quite involved, therefore put into its own function. basically iterate over elements and then query their size!
                llvm::Value* l_size =  list_of_structs_size(env, builder, list_ptr, list_type);
                l_size = builder.CreateAdd(l_size, opt_size);
                return l_size;
            } else if(elementType.isListType()) {
                llvm::Value* l_size = list_of_lists_size(env, builder, list_ptr, list_type);
                l_size = builder.CreateAdd(l_size, opt_size);
                return l_size;
            } else if(elementType.isTupleType()) {
                llvm::Value* l_size = list_of_tuples_size(env, builder, list_ptr, list_type);
                l_size = builder.CreateAdd(l_size, opt_size);
                return l_size;
            } else {
                throw std::runtime_error("Unsupported list element type: " + list_type.desc());
            }
        }

        std::tuple<llvm::Value*, SerializableValue> list_deserialize_from(LLVMEnvironment& env,
                                                                          llvm::IRBuilder<>& builder,
                                                                          llvm::Value* ptr,
                                                                          const python::Type& list_type) {
            using namespace std;
            using namespace llvm;

            assert(ptr && ptr->getType() == env.i8ptrType());
            assert(list_type.isListType());

            // alloc list ptr
            SerializableValue list_val;
            auto llvm_list_type = env.getOrCreateListType(list_type);

            list_val.val = env.CreateFirstBlockAlloca(builder, llvm_list_type);
            list_init_empty(env, builder, list_val.val, list_type);

            if(python::Type::EMPTYLIST == list_type)
                return make_tuple(ptr, list_val);

            // else, decode!


            return make_tuple(ptr, list_val);
        }

        llvm::Value* list_upcast(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* list_ptr,
                                 const python::Type& list_type, const python::Type& target_list_type) {

            env.debugPrint(builder, "enter list upcast function " + list_type.desc() + " -> " + target_list_type.desc());

            // make sure current element and target element type are compatible!
            assert(python::canUpcastType(list_type, target_list_type));

            // check elements are ok
            assert(list_type.isListType() && target_list_type.isListType());

            auto el_type = list_type.elementType();
            auto target_el_type = target_list_type.elementType();
            assert(python::canUpcastType(el_type, target_el_type));

            auto llvm_target_list_type = env.pythonToLLVMType(target_list_type);
            auto target_list_ptr = env.CreateFirstBlockAlloca(builder, llvm_target_list_type);

            // special case: empty list -> something?
            if(list_type == python::Type::EMPTYLIST) {
                // -> simply alloc empty list of target type and return
                list_init_empty(env, builder, target_list_ptr, target_list_type);
                return target_list_ptr;
            }

            // get list size of original list & alloc new list with target type!
            auto list_size = list_length(env, builder, list_ptr, list_type);
            list_init_empty(env, builder, target_list_ptr, target_list_type);
            list_reserve_capacity(env, builder, target_list_ptr, target_list_type, list_size);

            // create loop to fill in values from old list into new list after upcast (should this be emitted as function to inline?)
            auto& ctx = builder.getContext();
            auto func = builder.GetInsertBlock()->getParent();
            // create integer var
            auto i_var =env.CreateFirstBlockAlloca(builder, env.i64Type());
            builder.CreateStore(env.i64Const(0), i_var);
            llvm::BasicBlock* bbLoopHeader = llvm::BasicBlock::Create(ctx, "list_upcast_loop_header", func);
            llvm::BasicBlock* bbLoopBody = llvm::BasicBlock::Create(ctx, "list_upcast_loop_body", func);
            llvm::BasicBlock* bbLoopDone = llvm::BasicBlock::Create(ctx, "list_upcast_loop_done", func);
            env.debugPrint(builder, "go to loop header, entry for upcast");
            builder.CreateBr(bbLoopHeader);
            builder.SetInsertPoint(bbLoopHeader);
            auto icmp = builder.CreateICmpSLT(builder.CreateLoad(i_var), list_size);
            builder.CreateCondBr(icmp, bbLoopBody, bbLoopDone);

            builder.SetInsertPoint(bbLoopBody);
            // inc.
            auto idx = builder.CreateLoad(i_var);
            builder.CreateStore(builder.CreateAdd(idx, env.i64Const(1)), i_var);
            // store element (after upcast)
//            auto src_el = list_load_value(env, builder, list_ptr, list_type, idx);
//            auto target_el = env.upcastValue(builder, src_el, el_type, target_el_type);
//            list_store_value(env, builder, target_list_ptr, target_list_type, idx, target_el);
            builder.CreateBr(bbLoopHeader);

            builder.SetInsertPoint(bbLoopDone);
            return target_list_ptr;
        }
    }
}