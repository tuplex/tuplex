//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <codegen/FlattenedTuple.h>
#include <Logger.h>
#include <sstream>

#include "experimental/ListHelper.h"
#include "experimental/StructDictHelper.h"

namespace tuplex {
    namespace codegen {
        std::vector<python::Type> FlattenedTuple::getFieldTypes() const {
            return _tree.fieldTypes();
        }

        FlattenedTuple
        FlattenedTuple::fromLLVMStructVal(LLVMEnvironment *env, llvm::IRBuilder<> &builder, llvm::Value *ptr,
                                          const python::Type &type) {
            assert(env);
            assert(ptr);

            auto tupleType = python::Type::propagateToTupleType(type); // convenience type for loops down there
            assert(tupleType.isTupleType());

            // init tuple
            FlattenedTuple t(env);
            t.init(type); // original type

            auto llvmType = ptr->getType();

            std::vector<codegen::SerializableValue> vals;

            // check that pointer is of correct type
            auto llvm_tuple_type = env->getOrCreateTupleType(t._flattenedTupleType);

            // two options: either it's a pointer to llvm type OR the type directly (i.e. in struct access)
            if(llvmType->isPointerTy()) {
                assert(llvmType->isPointerTy());
                assert(llvmType->getPointerElementType()->isStructTy());
                assert(llvmType->getPointerElementType() == t.getLLVMType());

                assert(llvmType->getPointerElementType() == llvm_tuple_type);

                // now fill in values using getelementptr
                for (unsigned int i = 0; i < t.numElements(); ++i)
                    vals.emplace_back(env->getTupleElement(builder, t._flattenedTupleType, ptr, i));
            } else {
                // needs to be llvmtype
                assert(llvmType == t.getLLVMType());

                assert(llvmType == llvm_tuple_type);

                for(unsigned int i = 0; i < t.numElements(); ++i) {
                    vals.emplace_back(env->extractTupleElement(builder, t._flattenedTupleType, ptr, i));
                }
            }


            t._tree.setElements(vals);
            return t;
        }

        llvm::Value* FlattenedTuple::get(std::vector<int> index) {
            return _tree.get(index).val;
        }

        llvm::Value* FlattenedTuple::getSize(std::vector<int> index) const {
            return _tree.get(index).size;
        }

        python::Type FlattenedTuple::fieldType(const std::vector<int>& index) const {

            // if index size is zero, this may mean the flattened tuple is an empty tuple
            if(index.size() == 0) {
                assert(isEmptyTuple());
                return python::Type::EMPTYTUPLE;
            }

            assert(index.size() > 0);

            return _tree.fieldType(index);
        }

        void FlattenedTuple::setDummy(llvm::IRBuilder<> &builder, const std::vector<int> &index) {
            // is it a single value or a compound/tuple type?
            auto field_type = _tree.fieldType(index);
            if(field_type.isTupleType() && field_type != python::Type::EMPTYTUPLE) {
                // need to assign a subtree
                auto subtree = _tree.subTree(index);
                auto subtree_type = subtree.tupleType();

                // decode into flattened tree as well!
                FlattenedTuple ft_sub = FlattenedTuple(_env);
                // assign dummys to subtree
                assert(subtree_type.isTupleType());
                for(int i = 0; i < subtree_type.parameters().size(); ++i) {
                    ft_sub.setDummy(builder, {i});
                }

                // go over tree & assign values from subtree!
                _tree.setSubTree(index, ft_sub._tree);
            } else {

                // generate dummy value from type!
                auto& logger = Logger::instance().logger("codegen");
                logger.debug("generating dummy for type " + field_type.desc());

                auto dummy_type = field_type.isOptionType() ? field_type.elementType() : field_type;
                auto size = _env->i64Const(0);
                auto is_null = _env->i1Const(false); // bitmap !!!
                auto llvm_type = _env->pythonToLLVMType(dummy_type);
                auto value = _env->nullConstant(llvm_type);
                auto dummy = codegen::SerializableValue(value, size, is_null);

                // special case: string, use empty string
                if(dummy_type == python::Type::STRING) {
                    dummy = codegen::SerializableValue(_env->strConst(builder, ""),
                                                       _env->i64Const(1),
                                                       _env->i1Const(false));
                }

                _tree.set(index, dummy);
            }
        }

        void FlattenedTuple::set(llvm::IRBuilder<> &builder, const std::vector<int>& index, llvm::Value *value, llvm::Value *size, llvm::Value *is_null) {

            // is it a single value or a compound/tuple type?
            auto field_type = _tree.fieldType(index);
            if(field_type.isTupleType() && field_type != python::Type::EMPTYTUPLE) {
                // need to assign a subtree
                assert(value->getType()->isStructTy() || value->getType()->getPointerElementType()->isStructTy()); // struct or struct*

                auto subtree = _tree.subTree(index);
                auto subtree_type = subtree.tupleType();

                // decode into flattened tree as well!
                FlattenedTuple ft_sub = FlattenedTuple::fromLLVMStructVal(_env, builder, value, subtree_type);

                // go over tree & assign values from subtree!
                _tree.setSubTree(index, ft_sub._tree);
            } else {
                _tree.set(index, codegen::SerializableValue(value, size, is_null));
            }
        }

        void FlattenedTuple::set(llvm::IRBuilder<> &builder, const std::vector<int> &index, const FlattenedTuple &t) {
            auto subtree = _tree.subTree(index);
            auto subtree_type = subtree.tupleType();
            assert(subtree_type == t.tupleType());
            _tree.setSubTree(index, t._tree);
        }

        void FlattenedTuple::init(const python::Type &type) {
            _tree = TupleTree<codegen::SerializableValue>(type);

            // compute flattened type version
            _flattenedTupleType = python::Type::makeTupleType(getFieldTypes());
        }

        std::vector<llvm::Type*> FlattenedTuple::getTypes() {

            std::vector<llvm::Type*> types;

            // iterate over all fields & return mapped types
            for(auto type : _tree.fieldTypes()) {
                auto deoptimized_type = type.isConstantValued() ? type.underlying() : type;
                types.push_back(_env->pythonToLLVMType(deoptimized_type.withoutOption()));
            }

            return types;
        }

        void FlattenedTuple::deserializationCode(llvm::IRBuilder<>& builder, llvm::Value *input) {

            using namespace llvm;
            using namespace std;

            assert(_env);

            auto lastPtr = input;
            auto& context = _env->getContext();
            auto boolType = _env->getBooleanType();


            // first: check if bitmap is contained, if so deserialize bitmap!
            vector<llvm::Value*> bitmap;
            bool hasBitmap = tupleType().isOptional();
            if(hasBitmap) {
                // new:
                auto atype = getLLVMType()->getStructElementType(0); assert(atype->isArrayTy());
                auto numBits = atype->getArrayNumElements();
                std::tie(lastPtr, bitmap) = deserializeBitmap(*_env, builder, lastPtr, numBits);
                // old
                // auto atype = getLLVMType()->getStructElementType(0); assert(atype->isArrayTy());
                // int numBitmapElements = core::ceilToMultiple(atype->getArrayNumElements(), (uint64_t)64) / 64;
                //
                // for(int i = 0; i < numBitmapElements; ++i) {
                //     // read as 64bit int from memory
                //     auto bitmapElement = builder.CreateLoad(builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), "bitmap_part");
                //     bitmap.emplace_back(bitmapElement);
                //     // set
                //     lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)));
                // }
            }

            // go over all fields & retrieve them
            int opt_counter = 0;
            for(int i = 0; i < numElements(); ++i) {
                auto type = _tree.fieldType(i);

                if(type == python::Type::NULLVALUE) {
                    _tree.set(i, codegen::SerializableValue(nullptr, _env->i64Const(0), _env->i1Const(true)));
                    continue; // skip nullvalue, won't be serialized.
                }
                if(python::Type::EMPTYTUPLE == type) {
                    // no load necessary for empty tuple. Simply load the dummy struct
                    Value *load = builder.CreateLoad(builder.CreateAlloca(_env->getEmptyTupleType(), 0, nullptr));
                    _tree.set(i, codegen::SerializableValue(load, _env->i64Const(sizeof(int64_t)), _env->i1Const(false)));
                    continue;
                }
                if(python::Type::EMPTYDICT == type) {
                    // TODO: @LeonhardFS this is the code that was in there before (I just moved it up), but is this the correct value to use for an emptydict?
                    // no load required, just set to nullptr
                    _tree.set(i, codegen::SerializableValue(_env->i8nullptr(), _env->i64Const(sizeof(int64_t)), _env->i1Const(false)));
                    continue;
                }
                if(python::Type::EMPTYLIST == type) {
                    _tree.set(i, codegen::SerializableValue(_env->i8nullptr(), _env->i64Const(sizeof(int64_t)), _env->i1Const(false)));
                }

                llvm::Value* isnull = nullptr;
                // check if type is nullable, if so extract bit from bitmap
                if(hasBitmap && type.isOptionType()) {

                    // index check
                    assert(opt_counter == std::get<2>(getTupleIndices(_flattenedTupleType, i)));
                    isnull = _env->extractNthBit(builder, bitmap[opt_counter / 64], _env->i64Const(opt_counter % 64));

                    opt_counter++;

                    // get return type for extraction
                    type = type.getReturnType();
                    if(type == python::Type::EMPTYTUPLE) {
                        Value *load = builder.CreateLoad(builder.CreateAlloca(_env->getEmptyTupleType(), 0, nullptr));
                        _tree.set(i, codegen::SerializableValue(load, _env->i64Const(sizeof(int64_t)), isnull));
                        continue;
                    }
                    if(type == python::Type::EMPTYDICT) {
                        _tree.set(i, codegen::SerializableValue(_env->i8nullptr(), _env->i64Const(sizeof(int64_t)), isnull));
                        continue;
                    }
                    if(type == python::Type::EMPTYLIST) {
                        _tree.set(i, codegen::SerializableValue(_env->i8nullptr(), _env->i64Const(sizeof(int64_t)), isnull));
                        continue;
                    }
                } else
                    isnull = _env->i1Const(false);

                std::string twine = "in" + std::to_string(i);

                // is it a varfield? if so, retrieve via offset jump!
                // also deserialize size...
                // i.e. string, list[...], dict[...], ...
                if(!type.isFixedSizeType() && type != python::Type::EMPTYDICT) {
                    // deserialize string
                    // load directly from memory (offset in lower 32bit, size in upper 32bit)
                    Value *varInfo = builder.CreateLoad(builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)),
                                                        "offset");

                    // new:
                    Value *offset = nullptr;
                    Value *size = nullptr;
                    std::tie(offset, size) = unpack_offset_and_size(builder, varInfo);

                    // old:
                    // // truncation yields lower 32 bit (= offset)
                    // Value *offset = builder.CreateTrunc(varInfo, Type::getInt32Ty(context));
                    // // right shift by 32 yields size
                    // Value *size = builder.CreateLShr(varInfo, 32, "varsize");


                    // add offset to get starting point of varlen argument's memory region
                    Value *ptr = builder.CreateGEP(lastPtr, offset, twine);
                    assert(ptr->getType() == Type::getInt8PtrTy(context, 0));
                    if(type == python::Type::STRING || type == python::Type::PYOBJECT) {
                        _tree.set(i, codegen::SerializableValue(ptr, size, isnull));
                    } else if(type == python::Type::EMPTYDICT) {
                        throw std::runtime_error("Should not happen!");
                    } else if(type.isDictionaryType()) {
                        if(type.isStructuredDictionaryType()) {
                            // deserialize from ptr
                            auto dict_val = struct_dict_deserialize_from_memory(*_env, builder, ptr, type);
                            _tree.set(i, dict_val);
                        } else {
                            // create the dictionary pointer
                            auto dictPtr = builder.CreateCall(cJSONParse_prototype(_env->getContext(), _env->getModule().get()),
                                                              {ptr});
                            _tree.set(i, codegen::SerializableValue(dictPtr, size, isnull));
                        }
                    } else if(type.isListType()) {
                        throw std::runtime_error("list deserializaiton must be updated, not yet implemented");
                        assert(type != python::Type::EMPTYLIST);
                        auto llvmType = _env->getOrCreateListType(type);
                        llvm::Value *listAlloc = _env->CreateFirstBlockAlloca(builder, llvmType, "listAlloc");

                        // get number of elements
                        auto numElements = builder.CreateLoad(builder.CreateBitCast(ptr, Type::getInt64PtrTy(context, 0)), "list_num_elements");
                        llvm::Value* listSize = builder.CreateAlloca(Type::getInt64Ty(context));
                        builder.CreateStore(builder.CreateAdd(builder.CreateMul(numElements, _env->i64Const(8)), _env->i64Const(8)), listSize); // start list size as 8 * numElements + 8 ==> have to add string lengths for string case

                        // load the list with its initial size
                        auto list_capacity_ptr = CreateStructGEP(builder, listAlloc,  0);
                        builder.CreateStore(numElements, list_capacity_ptr);
                        auto list_len_ptr = CreateStructGEP(builder, listAlloc,  1);
                        builder.CreateStore(numElements, list_len_ptr);

                        auto elementType = type.elementType();
                        if(elementType == python::Type::STRING) {
                            auto offset_ptr = builder.CreateBitCast(builder.CreateGEP(ptr, _env->i64Const(sizeof(int64_t))), Type::getInt64PtrTy(context, 0)); // get pointer to i64 serialized array of offsets
                            // need to point to each of the strings and calculate lengths
                            llvm::Function *func = builder.GetInsertBlock()->getParent();
                            assert(func);
                            BasicBlock *loopCondition = BasicBlock::Create(context, "list_loop_condition", func);
                            BasicBlock *loopBodyEntry = BasicBlock::Create(context, "list_loop_body_entry", func);
                            BasicBlock *loopBodyLastEl = BasicBlock::Create(context, "list_loop_body_lastElement", func);
                            BasicBlock *loopBodyReg = BasicBlock::Create(context, "list_loop_body_regular", func);
                            BasicBlock *loopBodyEnd = BasicBlock::Create(context, "list_loop_body_end", func);
                            BasicBlock *after = BasicBlock::Create(context, "list_after", func);

                            // allocate the char* array
                            auto list_arr_malloc = builder.CreatePointerCast(_env->malloc(builder, builder.CreateMul(numElements, _env->i64Const(8))),
                                    llvmType->getStructElementType(2));
                            // allocate the sizes array
                            auto list_sizearr_malloc = builder.CreatePointerCast(_env->malloc(builder, builder.CreateMul(numElements, _env->i64Const(8))),
                                    llvmType->getStructElementType(3));

                            // read the elements
                            auto loopCounter = builder.CreateAlloca(Type::getInt64Ty(context));
                            builder.CreateStore(_env->i64Const(0), loopCounter);
                            builder.CreateBr(loopCondition);

                            builder.SetInsertPoint(loopCondition);
                            auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(loopCounter), numElements);
                            builder.CreateCondBr(loopNotDone, loopBodyEntry, after);

                            builder.SetInsertPoint(loopBodyEntry);
                            // store the pointer to the string
                            auto curOffset = builder.CreateLoad(builder.CreateGEP(offset_ptr, builder.CreateLoad(loopCounter)));
                            auto next_str_ptr = builder.CreateGEP(list_arr_malloc, builder.CreateLoad(loopCounter));
                            auto curStrPtr = builder.CreateGEP(builder.CreateBitCast(builder.CreateGEP(offset_ptr, builder.CreateLoad(loopCounter)), Type::getInt8PtrTy(context, 0)), curOffset);
                            builder.CreateStore(curStrPtr, next_str_ptr);

                            // set up to calculate the size based on offsets
                            auto next_size_ptr = builder.CreateGEP(list_sizearr_malloc, builder.CreateLoad(loopCounter));
                            auto lastElement = builder.CreateICmpEQ(builder.CreateLoad(loopCounter), builder.CreateSub(numElements, _env->i64Const(1)));
                            builder.CreateCondBr(lastElement, loopBodyLastEl, loopBodyReg);

                            builder.SetInsertPoint(loopBodyReg);
                            // get the next serialized offset
                            auto nextOffset = builder.CreateLoad(builder.CreateGEP(offset_ptr, builder.CreateAdd(builder.CreateLoad(loopCounter), _env->i64Const(1))));
                            auto curLenReg = builder.CreateSub(nextOffset, builder.CreateSub(curOffset, _env->i64Const(sizeof(uint64_t))));
                            // store it into the list
                            builder.CreateStore(curLenReg, next_size_ptr);
                            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(listSize), curLenReg), listSize);
                            builder.CreateBr(loopBodyEnd);

                            builder.SetInsertPoint(loopBodyLastEl);
                            auto curLenLast = builder.CreateSub(size, builder.CreateMul(_env->i64Const(8), numElements)); // size - 8*(nE) (no -1 because of the size field before)
                            curLenLast = builder.CreateSub(curLenLast, curOffset);
                            // store it into the list
                            builder.CreateStore(curLenLast, next_size_ptr);
                            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(listSize), curLenLast), listSize);
                            builder.CreateBr(loopBodyEnd);

                            builder.SetInsertPoint(loopBodyEnd);
                            // update the loop variable and return
                            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(loopCounter), _env->i64Const(1)), loopCounter);
                            builder.CreateBr(loopCondition);

                            builder.SetInsertPoint(after);
                            // store the malloc'd and populated array to the struct
                            auto list_arr = CreateStructGEP(builder, listAlloc, 2);
                            builder.CreateStore(list_arr_malloc, list_arr);
                            auto list_sizearr = CreateStructGEP(builder, listAlloc, 3);
                            builder.CreateStore(list_sizearr_malloc, list_sizearr);
                        }
                        else if(elementType == python::Type::BOOLEAN) {
                            ptr = builder.CreateBitCast(builder.CreateGEP(ptr, _env->i64Const(sizeof(int64_t))), Type::getInt64PtrTy(context, 0)); // get pointer to i64 serialized array of booleans
                            // need to copy the values out because serialized boolean = 8 bytes, but llvm boolean = 1 byte
                            llvm::Function *func = builder.GetInsertBlock()->getParent();
                            assert(func);
                            BasicBlock *loopCondition = BasicBlock::Create(context, "list_loop_condition", func);
                            BasicBlock *loopBody = BasicBlock::Create(context, "list_loop_body", func);
                            BasicBlock *after = BasicBlock::Create(context, "list_after", func);
                            // allocate the array
                            auto list_arr_malloc = builder.CreatePointerCast(_env->malloc(builder, numElements),
                                                                             llvmType->getStructElementType(2));

                            // read the elements
                            auto loopCounter = builder.CreateAlloca(Type::getInt64Ty(context));
                            builder.CreateStore(_env->i64Const(0), loopCounter);
                            builder.CreateBr(loopCondition);

                            builder.SetInsertPoint(loopCondition);
                            auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(loopCounter), numElements);
                            builder.CreateCondBr(loopNotDone, loopBody, after);

                            builder.SetInsertPoint(loopBody);
                            auto list_el = builder.CreateGEP(list_arr_malloc, builder.CreateLoad(loopCounter)); // next list element
                            // get the next serialized value
                            auto serializedbool = builder.CreateLoad(builder.CreateGEP(ptr, builder.CreateLoad(loopCounter)));
                            auto truncbool = builder.CreateTrunc(serializedbool, boolType);
                            // store it into the list
                            builder.CreateStore(truncbool, list_el);
                            // update the loop variable and return
                            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(loopCounter), _env->i64Const(1)), loopCounter);
                            builder.CreateBr(loopCondition);

                            builder.SetInsertPoint(after);
                            // store the malloc'd and populated array to the struct
                            auto list_arr = CreateStructGEP(builder, listAlloc, 2);
                            builder.CreateStore(list_arr_malloc, list_arr);
                        }
                        else if(elementType == python::Type::I64 || elementType == python::Type::F64) {
                            // can just directly point to the serialized data
                            auto list_arr = CreateStructGEP(builder, listAlloc, 2);
                            builder.CreateStore(builder.CreateBitCast(builder.CreateGEP(ptr, _env->i64Const(sizeof(int64_t))),
                                    llvmType->getStructElementType(2)), list_arr);
                        } else {
                            Logger::instance().defaultLogger().error("unknown type '" + type.desc() + "' to be deserialized!");
                        }

                        // set the deserialized list
                        _tree.set(i, codegen::SerializableValue(builder.CreateLoad(listAlloc), builder.CreateLoad(listSize), isnull));
                    } else {
                        Logger::instance().defaultLogger().error("unknown type '" + type.desc() + "' to be deserialized!");
                    }
                } else {
                    // directly read value
                    // depending on type, generate load instructions & summing instructions for var length fields to bytesread
                    if(python::Type::BOOLEAN == type) {

                        // load directly from memory
                        Value *tmp = builder.CreateLoad(builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)));

                        // cast to boolean type
                        Value *load = builder.CreateTrunc(tmp, boolType, twine);
                        _tree.set(i, codegen::SerializableValue(load, _env->i64Const(sizeof(int64_t)), isnull));

                    } else if(python::Type::I64 == type) {

                        // load directly from memory
                        Value *load = builder.CreateLoad(builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), twine);
                        _tree.set(i, codegen::SerializableValue(load, _env->i64Const(sizeof(int64_t)), isnull));

                    } else if(python::Type::F64 == type) {

                        // load directly from memory
                        Value *load = builder.CreateLoad(builder.CreateBitCast(lastPtr, Type::getDoublePtrTy(context, 0)), twine);
                        _tree.set(i, codegen::SerializableValue(load, _env->i64Const(sizeof(int64_t)), isnull));

                    } else if(python::Type::EMPTYTUPLE == type) {
                        throw std::runtime_error("Should not happen EMPTYTUPLE");
                    } else if(python::Type::EMPTYDICT == type) {
                        throw std::runtime_error("Should not happen EMPTYDICT");
                    } else if(type.isListType()) {
                        // lists of fixed size are just represented by a length
                        Value *num_elements = builder.CreateLoad(builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), twine);
                        _tree.set(i, codegen::SerializableValue(num_elements, _env->i64Const(sizeof(int64_t)), isnull));
                    } else if(type.isConstantValued()) {
                        // simple constant gen
                        _tree.set(i, constantValuedTypeToLLVM(builder, type));
                        continue; // important to skip 8-byte skip when decoding!
                    } else {
                        Logger::instance().defaultLogger().error("unknown type '" + type.desc() + "' to be deserialized!");
                    }
                }

                // inc last ptr (SKIP for constants)
                lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)), "inptr");
            }
        }

        llvm::Value* FlattenedTuple::serializationCode(llvm::IRBuilder<>& builder, llvm::Value *output,
                                                       llvm::Value *capacity, llvm::BasicBlock* insufficientCapacityHandler) const {
            using namespace llvm;
            assert(_env);
            auto& context = _env->getContext();

            // LLVM checks
            assert(output);
            assert(capacity);
            assert(insufficientCapacityHandler);
            assert(output->getType() == llvm::Type::getInt8PtrTy(context, 0));
            assert(capacity->getType() == llvm::Type::getInt64Ty(context));

            auto types = getFieldTypes();
            bool hasVarField = !getTupleType().isFixedSizeType();
            Value *serializationSize = getSize(builder);


            // Add capacity check...
            llvm::Function *func = builder.GetInsertBlock()->getParent();
            assert(func);
            BasicBlock *enoughCapacity = BasicBlock::Create(context, "serializeToMemory", func);

            // if(serializationSize <= capacity && outputPtr)goto enoughCapacity, else insufficientCapacityHandler
            Value *condValCapacity = builder.CreateICmpULE(serializationSize, capacity);
            assert(output->getType() == llvm::Type::getInt8PtrTy(context, 0));
            Value *validOutputPtr = builder.CreateICmpNE(output, llvm::ConstantPointerNull::get(Type::getInt8PtrTy(context, 0)));
            Value *condVal = builder.CreateAnd(condValCapacity, validOutputPtr);
            builder.CreateCondBr(condVal, enoughCapacity, insufficientCapacityHandler);

            // then block...
            // -------
            IRBuilder<> bThen(enoughCapacity);
            serialize(bThen, output);

            // set builder to insert on then block
            builder.SetInsertPoint(enoughCapacity);
            return serializationSize;
        }



        llvm::Value* FlattenedTuple::serialize(llvm::IRBuilder<>& builder, llvm::Value *ptr) const {
            using namespace llvm;
            using namespace std;

            auto& context = _env->getContext();
            auto types = getFieldTypes();
            bool hasVarField = !getTupleType().withoutOptionsRecursive().isFixedSizeType();

            auto original_start_ptr = ptr;

            // _env->debugPrint(builder, "starting serialization of tuple:");
            // this->print(builder);

            // serialize fixed size + varlen fields out
            Value *lastPtr = ptr;
            size_t numSerializedElements = 0;
            for(const auto &t: types) {
                if(!(t.isSingleValued() || (t.isOptionType() && t.getReturnType().isSingleValued()))) {
                    numSerializedElements++;
                }
            }
            Value *varlenBasePtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t) * (numSerializedElements + 1)), "varbaseptr");
            Value *varlenSize = _env->i64Const(0);

            // bitmap needed?
            bool hasBitmap = getTupleType().isOptional();

            // step 1: serialize bitmap
            if(hasBitmap) {
                auto bitmap = getBitmap(builder);

                assert(!bitmap.empty());

                // store bitmap to output ptr
                for(auto be : bitmap) {
                    assert(be->getType() == _env->i64Type());
                    builder.CreateStore(be, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)));

                    // warning multiple
                    lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)), "outptr");
                }

                // add 8 bytes to varlen base ptr
                varlenBasePtr = builder.CreateGEP(varlenBasePtr, _env->i32Const(sizeof(int64_t) * bitmap.size()), "varlenbaseptr");
            }

            // _env->printValue(builder, builder.CreatePtrDiff(varlenBasePtr, original_start_ptr), "varlen pointer start offset is: ");

            // step 2: serialize fields
            // go through elements
            int serialized_idx = 0; // index of serialized fields
            for(unsigned i = 0; i < numElements(); ++i) {
                // is varlenfield? == llvm pointer type
                auto field = _tree.get(i).val;
                auto size = _tree.get(i).size;
                auto is_null = _tree.get(i).is_null;
                auto is_not_null = is_null ? _env->i1neg(builder, is_null) : _env->i1Const(true);
                if(!is_null)
                    is_null = _env->i1Const(false);
                bool is_option_field = types[i].isOptionType();
                auto fieldType = types[i].withoutOption();

                // _env->printValue(builder, builder.CreatePtrDiff(lastPtr, original_start_ptr), "writing field of type "
                // + types[i].desc() + ", current field (no=" + std::to_string(i) + "/" + std::to_string(numElements()) + ") offset=");
                // _env->printValue(builder, builder.CreatePtrDiff(varlenBasePtr, original_start_ptr), "varbaseptr offset=");
                // _env->printValue(builder, varlenSize, "varlen_size=");

                if(is_option_field)
                    assert(is_null && is_not_null);

                 // // debug
                 // if(field) _env->debugPrint(builder, "serializing field " + std::to_string(i) + ": ", field);
                 // if(size)_env->debugPrint(builder, "serializing field size" + std::to_string(i) + ": ", size);
                 // if(is_null)_env->debugPrint(builder, "serializing field is_null " + std::to_string(i) + ": ", is_null);

                 // do not need to serialize: EmptyTuple, EmptyDict, EmptyList??, NULLVALUE

                if(fieldType.isSingleValued())
                    continue; // skip all these fields, they do not need to get serialized...

                // skip constants, they're known!
                if(fieldType.isConstantValued())
                    continue;

                // @TODO: improve this for NULL VALUES...
                if(!field) {
                    // dummy value
                    if(fieldType.withoutOption() == python::Type::BOOLEAN)
                        field = _env->boolConst(false);
                    if(fieldType.withoutOption() == python::Type::I64)
                        field = _env->i64Const(0);
                    if(fieldType.withoutOption() == python::Type::F64)
                        field = _env->f64Const(0.0);
                    if(fieldType.withoutOption() == python::Type::STRING)
                        field = _env->strConst(builder, "");
                }
                if(!size)
                    size = _env->i64Const(8);

                // structured dict and regular dictionaries (JSON)
                if(fieldType.isDictionaryType() && fieldType != python::Type::EMPTYDICT) {

                    // serialize struct dict
                    if(fieldType.isStructuredDictionaryType()) {
                        auto dict_type = types[i].withoutOption();

                        // struct dicts are a var field (ignore the special case here)
                        size = struct_dict_serialized_memory_size(*_env, builder, field, dict_type).val;

                        // note: when null, don't serialize anything.
                        if(types[i].isOptionType())
                            size = builder.CreateSelect(_tree.get(i).is_null, _env->i64Const(0), size);

                        // the offset is computed using how many varlen fields have been already serialized
                        int64_t field_offset_in_bytes = (numSerializedElements + 1 - serialized_idx) * sizeof(int64_t);
                        Value *offset = builder.CreateAdd(_env->i64Const(field_offset_in_bytes), varlenSize);

                         // // debug print
                         // _env->printValue(builder, size, "serializing element " + std::to_string(i+1) + " of " + std::to_string(numElements()) +" as dict of size: ");
                         // _env->printValue(builder, offset, "serializing dict to offset: ");

                        // store offset + length
                        // len | size
                        auto info = pack_offset_and_size(builder, offset, size);
                        builder.CreateStore(info, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);

                        // write to i8 pointer
                        Value *outptr = builder.CreateGEP(lastPtr, offset, "varoff");

                        // write actual data to outptr
                        auto s_info = struct_dict_serialize_to_memory(*_env, builder, field, dict_type, outptr);

                        // _env->printValue(builder, s_info.size, "actually serialized size: ");

#ifndef NDEBUG
                         // print info
                         auto bitmap_pos_idx = 0;
                         if(struct_dict_has_bitmap(dict_type)) {
                             auto bitmap_val = builder.CreateLoad(builder.CreatePointerCast(builder.CreateGEP(s_info.val, _env->i64Const(bitmap_pos_idx)), _env->i64ptrType()));
                             // _env->printValue(builder, bitmap_val, "bitmap:      ");
                             bitmap_pos_idx += 8;
                         }
                        if(struct_dict_has_presence_map(dict_type)) {
                            auto bitmap_val = builder.CreateLoad(builder.CreatePointerCast(builder.CreateGEP(s_info.val, _env->i64Const(bitmap_pos_idx)), _env->i64ptrType()));
                            // _env->printValue(builder, bitmap_val, "presence map: ");
                            bitmap_pos_idx += 8;
                        }
#endif
                        // also varlensize needs to be output separately, so add
                        varlenSize = builder.CreateAdd(varlenSize, size);
                        lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)), "outptr");
                        serialized_idx++;
                        continue; // field done.
                    } else {
                        assert(!is_option_field); // --> need to implement if logic for this!

                        field = builder.CreateCall(
                                cJSONPrintUnformatted_prototype(_env->getContext(), _env->getModule().get()),
                                {field});
                        size = builder.CreateAdd(
                                builder.CreateCall(strlen_prototype(_env->getContext(), _env->getModule().get()), {field}),
                                _env->i64Const(1));
                    }
                }

                assert(field);
                assert(size);

                // special is empty dict, empty list and NULL. I.e. though they in principle are var fields, they are fixed size.
                // ==> serialize them as 0 (later optimize this away). TODO: this comment is out of date, right? we have optimized the serialization away.
                if(fieldType.isListType() && !fieldType.elementType().isSingleValued()) {
                    // new list version, similar to struct dict using its own helper functions
                    auto list_type = types[i].withoutOption();

                    // struct dicts are a var field (ignore the special case here)
                    size = list_serialized_size(*_env, builder, field, list_type);

                    // note: when null, don't serialize anything.
                    if(types[i].isOptionType())
                        size = builder.CreateSelect(_tree.get(i).is_null, _env->i64Const(0), size);

                    // the offset is computed using how many varlen fields have been already serialized
                    int64_t field_offset_in_bytes = (numSerializedElements + 1 - serialized_idx) * sizeof(int64_t);
                    Value *offset = builder.CreateAdd(_env->i64Const(field_offset_in_bytes), varlenSize);

                    // store offset + length
                    // len | size
                    auto info = pack_offset_and_size(builder, offset, size);
                    builder.CreateStore(info, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);

                    // write to i8 pointer
                    Value *outptr = builder.CreateGEP(lastPtr, offset, "varoff");

                    // write actual data to outptr
                    auto s_info = list_serialize_to(*_env, builder, field, list_type, outptr);

                    // also varlensize needs to be output separately, so add
                    varlenSize = builder.CreateAdd(varlenSize, size);
                    lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)), "outptr");
                    serialized_idx++;
                    continue; // field done.
                } else if(fieldType != python::Type::EMPTYDICT && fieldType != python::Type::NULLVALUE && field->getType()->isPointerTy()) {
                    // assert that meaning is true.
                    assert(!fieldType.isFixedSizeType());

                    // this is a var field
                    // memcpy with offset

                    // the offset is computed using how many varlen fields have been already serialized
                    Value *offset = builder.CreateAdd(_env->i64Const((numSerializedElements + 1 - serialized_idx) * sizeof(int64_t)), varlenSize);

                    // option trick: in case of isnull we don't want to copy anything b.c. it will ruin the memory!
                    // -> therefore: multiply with is isnull
                    if(is_option_field) {
                        size = builder.CreateMul(size, builder.CreateZExt(is_not_null, _env->i64Type()));
                    }

                    // // debug print
                    // _env->printValue(builder, size, "serializing " + fieldType.desc() + " of size: ");
                    // _env->printValue(builder, offset, "serializing " + fieldType.desc() + " to offset: ");

                    // store offset + length
                    // len | size
                    auto info = pack_offset_and_size(builder, offset, size);
                    // old:
                    //Value *info = builder.CreateOr(builder.CreateZExt(offset, Type::getInt64Ty(context)), builder.CreateShl(
                    //        builder.CreateZExt(size, Type::getInt64Ty(context)), 32)
                    //);

                    if(is_option_field) {
                        info = builder.CreateMul(info, builder.CreateZExt(is_not_null, _env->i64Type()));
                    }

                    builder.CreateStore(info, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);

                    // copy memory of i8 pointer
                    assert(field->getType()->isPointerTy());
                    assert(field->getType() == Type::getInt8PtrTy(context, 0));
                    Value *outptr = builder.CreateGEP(lastPtr, offset, "varoff");


#if LLVM_VERSION_MAJOR < 9
                    builder.CreateMemCpy(outptr, field, size, 0, true);
#else
                    // API update here, old API only allows single alignment.
                    // new API allows src and dest alignment separately
                    builder.CreateMemCpy(outptr, 0, field, 0, size, true);
#endif

                    // string & forced zero termination?
                    if ((fieldType == python::Type::STRING ||
                         fieldType.isDictionaryType()) && _forceZeroTerminatedStrings) {
                        // write 0 for string
                        auto lastCharPtr = builder.CreateGEP(outptr, builder.CreateSub(size, _env->i64Const(1)));
                        builder.CreateStore(_env->i8Const('\0'), lastCharPtr);
                    }

                    // also varlensize needs to be output separately, so add
                    varlenSize = builder.CreateAdd(varlenSize, size);

                    lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)), "outptr");
                } else {
                    assert(fieldType.isFixedSizeType());


                    // trick -> if is_null store 0. -> can use multiply/select for this.

                    // depending on type perform cast & copy out
                    if(python::Type::BOOLEAN == fieldType) {
                        // check what the boolean type is
                        auto boolType = _env->getBooleanType();

                        // bitcast?
                        Value *boolVal = field;
                        if(boolType->getIntegerBitWidth() != Type::getInt64Ty(context)->getIntegerBitWidth()) {
                            // cast up
                            boolVal = builder.CreateZExt(boolVal, Type::getInt64Ty(context));
                        }

                        // option trick
                        if(is_option_field) {
                            boolVal = builder.CreateMul(boolVal, builder.CreateZExt(is_not_null, Type::getInt64Ty(context)));
                        }

                        // store within output
                        Value *store = builder.CreateStore(boolVal, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);
                        lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)), "outptr");
                    } else if(python::Type::I64 == fieldType) {
                        // option trick
                        if(is_option_field) {
                            field = builder.CreateMul(field, builder.CreateZExt(is_not_null, Type::getInt64Ty(context)), "", true);
                        }
                        assert(field->getType() == _env->i64Type());
                        // store within output
                        Value *store = builder.CreateStore(field, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);
                        lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)), "outptr");
                    } else if(python::Type::F64 == fieldType) {
                        // option trick
                        if(is_option_field) {
                            assert(is_not_null);

                            // zero field via select
                            field = builder.CreateSelect(is_not_null, field, _env->f64Const(0.0));

                            // multiplication results in issue with llvm 9.0.1
                        }
                        assert(field->getType() == _env->doubleType());
                        // store within output
                        Value *store = builder.CreateStore(field, builder.CreateBitCast(lastPtr, Type::getDoublePtrTy(context, 0)), false);
                        lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(double)), "outptr");
                    } else if(fieldType.isListType() && fieldType.elementType().isSingleValued()) {
                        throw std::runtime_error("list not supported yet");
                        // store within output - the field is just the size of the list
                        Value *store = builder.CreateStore(field, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);
                        lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)), "outptr");
                    } else {
                        std::stringstream ss;
                        ss<<"unknown fixed type '"<<fieldType.desc()<<"' wanted to be serialized";
                        throw std::runtime_error(ss.str());
                    }
                }

                assert(!fieldType.isSingleValued()); // only inc if serialized field!
                serialized_idx++;
            }

            // if varfield was encountered, store varfield size!
            if(hasVarField) {
                builder.CreateStore(varlenSize, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0))); // last field
                lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)));
                // add varlength
                lastPtr = builder.CreateGEP(lastPtr, varlenSize);
            }

            // return diff
            auto bytes_written = builder.CreatePtrDiff(lastPtr, original_start_ptr);

            // _env->printValue(builder, bytes_written, "bytes written: ");
            return bytes_written;
        }

        void FlattenedTuple::setElement(llvm::IRBuilder<>& builder,
                                        const int iElement,
                                        llvm::Value *val,
                                        llvm::Value *size,
                                        llvm::Value *is_null) {
            // make sure it is a valid index
            assert(0 <= iElement && iElement < tupleType().parameters().size());

            // validate data
            if(size)
                assert(size->getType() == _env->i64Type());
            if(is_null)
                assert(is_null->getType() == _env->i1Type());

            auto type = tupleType().parameters()[iElement];

            // update for option type
            auto elementType = type.isOptionType() ? type.getReturnType() : type;

            // which type is it?
            // nested?
            // => if so copy over elements
            if(elementType.isTupleType() && elementType != python::Type::EMPTYTUPLE) {

                FlattenedTuple ft = FlattenedTuple::fromLLVMStructVal(_env, builder, val, elementType);
                auto indices = ft._tree.getMultiIndices();

                // copy over all elements' pointers
                for(int j = 0; j < ft.numElements(); ++j) {
                    std::vector<int> index({iElement});
                    for(const auto& i : indices[j])
                        index.push_back(i);

                    set(builder, index, ft.get(j), ft.getSize(j), ft.getIsNull(j));
                }

            } else if(elementType.isPrimitiveType() || elementType.isDictionaryType()
            || elementType == python::Type::GENERICDICT || elementType.isListType() || elementType == python::Type::PYOBJECT) {

                // special case: struct dict
                if(elementType.isStructuredDictionaryType()) {
                    // either struct.dict or struct.dict* supported.
                    assert(val->getType()->isStructTy() ||
                          (val->getType()->isPointerTy() && val->getType()->getPointerElementType()->isStructTy()));
                    if(val->getType()->isPointerTy()) {
                        // perform load!
                        val = builder.CreateLoad(val);
                    }
                    size = nullptr; // needs to be calculated...
                }

                // just copy over the pointers
                set(builder, {iElement}, val, size, is_null);

            } else if(elementType == python::Type::EMPTYTUPLE) {
                // empty tuple will result in constants
                // i.e. set the value to a load of the empty tuple special type and the size to sizeof(int64_t)
                assert(_env);
                auto alloc = builder.CreateAlloca(_env->getEmptyTupleType(), 0, nullptr);
                auto load = builder.CreateLoad(alloc);
                set(builder, {iElement}, load, _env->i64Const(sizeof(int64_t)), _env->i1Const(false));
            } else if(elementType == python::Type::NULLVALUE) {
                set(builder, {iElement}, nullptr, nullptr, _env->i1Const(true));
            } else if(elementType.isConstantValued()) {
                // check what the underlying is and then set accordingly with constant!
                auto value = codegen::constantValuedTypeToLLVM(builder, elementType);
                set(builder, {iElement}, value.val, value.size, value.is_null);
            } else {
                Logger::instance().logger("codegen").error("unknown type encountered while trying to set flattened tuple structure");
                return;
            }
        }

        bool FlattenedTuple::containsVarLenField() const {
            return !tupleType().isFixedSizeType();
        }

        llvm::Value* FlattenedTuple::getSize(llvm::IRBuilder<>& builder) const {
            // @TODO: make this more performant by NOT serializing anymore NULL, EMPTYDICT, EMPTYTUPLE, ...
            using namespace llvm;

            llvm::Value* s = _env->i64Const(0);

            // special case empty tuple --> just return 0
            if(isEmptyTuple())
                return s;

            // ==> calc via num elements * sizeof(int64_t) + add varlen sizes + varlen64bitint + bitmap.

            // calc number of serialized fixed-len fields, exclude null, emptytuple, emptydict
            // and all constants b.c. they won't get serialized.
            int64_t numSerializedFixedLenFields = 0;
            for(auto& t : _flattenedTupleType.parameters()) {
                if(!(t.isSingleValued() || t.isConstantValued() || (t.isOptionType() && t.getReturnType().isSingleValued()))) {
                    numSerializedFixedLenFields += 1;
                }
            }
            s = _env->i64Const( numSerializedFixedLenFields * sizeof(int64_t));


            // _env->debugPrint(builder, "fixed size fields take bytes: ", s);

#ifndef NDEBUG
            size_t numSerializedFixedLenFieldsCheck = 0;
            for(int i = 0; i < _tree.elements().size(); ++i) {
                // should be the same as _tree.fieldType(i).isSingleValued()
                if (!(_tree.fieldType(i).isSingleValued() ||
                        _tree.fieldType(i).isConstantValued() ||
                      (_tree.fieldType(i).isOptionType() && _tree.fieldType(i).getReturnType().isSingleValued()))) {
                    numSerializedFixedLenFieldsCheck += 1;
                }
            }
            assert(numSerializedFixedLenFields == numSerializedFixedLenFieldsCheck);
#endif

            // add varlen sizes
            for(int i = 0; i < _tree.elements().size(); ++i) {
                auto el = _tree.get(i); //_tree.elements()[i];
                auto type = _tree.fieldType(i);

                // skip single-valued elements.
                if(type.isSingleValued())
                    continue;

                // not single-valued? there should be a value!
                if(!el.val) {
                    // is it an option type? -> fill in with dummy type.
                    // could be that isnull is i1const(true)
                    if(type.isOptionType())
                        el.val = _env->dummyValue(builder, type.getReturnType()).val;

                    // still not existing? fill in with nullConstant
                    if(!el.val)
                        el.val = _env->i64Const(0);
                }
                assert(el.val);

                // _env->printValue(builder, s, "cur size before serializing " + type.desc());

                // special case: option
                BasicBlock* bNext = nullptr;
                BasicBlock* lastBlockBeforeNullCheck = nullptr;
                llvm::Value* size_before_null_check = s;
                if(type.isOptionType()) {
                    // only add size IF not null
                    assert(el.is_null);
                    auto& ctx = _env->getContext();
                    BasicBlock* bAddSize = BasicBlock::Create(ctx, "entry_valid", builder.GetInsertBlock()->getParent());
                    bNext = BasicBlock::Create(ctx, "next", builder.GetInsertBlock()->getParent());
                    lastBlockBeforeNullCheck = builder.GetInsertBlock();

                    // _env->printValue(builder, el.is_null, "element of type " + type.desc() + " is null: ");

                    builder.CreateCondBr(el.is_null, bNext, bAddSize);
                    builder.SetInsertPoint(bAddSize);

                    // remove option
                    type = type.getReturnType();
                }

                assert(type != python::Type::UNKNOWN);
                assert(!type.isSingleValued() && !type.isConstantValued());
                assert(!type.isOptionType());
                if(!type.isFixedSizeType()) {

                    // special cases list and struct
                    if(type.isStructuredDictionaryType()) {
                        auto s_size = struct_dict_serialized_memory_size(*_env, builder, el.val, type).val;

                        // debug print:
                        // _env->printValue(builder, s_size, "struct dict of " + type.desc() + " size is: ");

                        assert(s_size && s_size->getType() == _env->i64Type());
                        s = builder.CreateAdd(s, s_size);
                    } else if(type.isListType()) {
                        auto l_size = list_serialized_size(*_env, builder, el.val, type);
                        assert(l_size && l_size->getType() == _env->i64Type());
                        s = builder.CreateAdd(s, l_size);
                    } else {
                        // string etc.
                        assert(el.size && el.size->getType() == _env->i64Type());

                        // _env->printValue(builder, el.size, "size of element " + std::to_string(i) + " of type " + type.desc() + ": ");
                        s = builder.CreateAdd(s, el.size); // 0 for varlen option!
                        // _env->printValue(builder, s, "size: ");
                    }
                }

                // close if-dep size
                if(bNext) {
                    auto lastBlock = builder.GetInsertBlock();
                    builder.CreateBr(bNext);
                    builder.SetInsertPoint(bNext);
                    // create phi node
                    auto phi = builder.CreatePHI(_env->i64Type(), 2);
                    phi->addIncoming(s, lastBlock);
                    phi->addIncoming(size_before_null_check, lastBlockBeforeNullCheck);
                    s = phi;
                }
            }

            // _env->debugPrint(builder, "including varlen fields that's bytes: ", s);

            // check whether varlen field is contained (true for strings only so far. Later, also for arrays, dicts, ...)
            if(containsVarLenField())
                s = builder.CreateAdd(s, _env->i64Const(sizeof(int64_t)));

            // _env->debugPrint(builder, "+fast varlen skipfield that's bytes: ", s);

            // if contains bitmap, add multiple of 8 bytes
            if(tupleType().isOptional()) {
                auto bitmapType = getLLVMType()->getStructElementType(0);
                assert(bitmapType->isArrayTy());
                assert(bitmapType->getArrayElementType() == _env->i1Type());
                int numOptionalElements = bitmapType->getArrayNumElements();

                auto bitmap64Size = core::ceilToMultiple(numOptionalElements, 64) / 8; // 8 bits = 1 byte

                // i1 array type
                s = builder.CreateAdd(s, _env->i64Const(bitmap64Size));
            }

            // _env->debugPrint(builder, "final size required is: ", s);

            return s;
        }

        llvm::Type* FlattenedTuple::getLLVMType() const {
            // create llvm struct type (all the lifting should be in LLVM environment)
            return _env->getOrCreateTupleType(_flattenedTupleType);
        }

        llvm::Value* FlattenedTuple::alloc(llvm::IRBuilder<> &builder, const std::string& twine) const {
            // copy structure llvm like out
            auto llvmType = getLLVMType();

            auto cb = getFirstBlockBuilder(builder);
            auto tuple_ptr = cb.CreateAlloca(llvmType, 0, nullptr, twine);

            // get tuple indices and initialize values!
            for(unsigned i = 0; i < _flattenedTupleType.parameters().size(); ++i) {
                auto indices = getTupleIndices(_flattenedTupleType, i);

                int val_idx=-1, size_idx=-1, bitmap_idx=-1;
                std::tie(val_idx, size_idx, bitmap_idx) = indices;

                // explit null initialization of data, uncomment to perform
                // if(val_idx >= 0) {
                //     auto nc = _env->nullConstant(llvmType->getStructElementType(val_idx));
                //     cb.CreateStore(nc, cb.CreateStructGEP(tuple_ptr, val_idx));
                // }
                // if(size_idx >= 0) {
                //     cb.CreateStore(_env->i64Const(0), cb.CreateStructGEP(tuple_ptr, size_idx));
                // }
            }

            return tuple_ptr;
            // // create tuple & initialize with values!
            // return _env->CreateFirstBlockAlloca(builder, llvmType, twine);
        }

        void FlattenedTuple::storeTo(llvm::IRBuilder<> &builder, llvm::Value *ptr, bool is_volatile) const {
            // check that type corresponds
            auto llvmType = getLLVMType();

            assert(ptr->getType() == llvmType->getPointerTo(0));

            // special case empty tuple:
            // here it is just a load
            // ==> an empty tuple can't have a bitmap!
            if(isEmptyTuple()) {
                throw std::runtime_error("need to figure this out..."); // what needs to be stored here anyways??
                assert(1 == numElements());
                // store size for packed empty tuple
                auto loc = builder.CreateGEP(ptr, {_env->i32Const(0), _env->i32Const(numElements())});
                builder.CreateStore(_tree.get(0).size,
                                    loc, is_volatile);
                return;
            }

            // storing is straight-forward, just use helper function (don't bother with bitmap logic!) from LLVMEnv.
            for(unsigned i = 0; i < _tree.numElements(); ++i) {
                auto el = _tree.get(i);
                 // if(el.val)
                 //     _env->printValue(builder, el.val, "storing to ptr element " + std::to_string(i));
                 // if(el.size)
                 //     _env->printValue(builder, el.size, "storing to ptr size of element " + std::to_string(i));
                _env->setTupleElement(builder, _flattenedTupleType, ptr, i, el, is_volatile);
            }
        }

        llvm::Value* FlattenedTuple::getLoad(llvm::IRBuilder<> &builder) const {
            auto alloc = this->alloc(builder);
            storeTo(builder, alloc);
            return builder.CreateLoad(alloc);
        }

        void FlattenedTuple::assign(const int i, llvm::Value *val, llvm::Value *size, llvm::Value *isnull) {
            auto& context = _env->getContext();

            if(isnull)
                assert(isnull->getType() == _env->i1Type());

            assert(i >= 0 && i < numElements());

            // check that assigned val matches primitive type
            auto types = getFieldTypes();

            auto type = deoptimizedType(types[i].withoutOption());

            // null val & size for nulltype
            if(type == python::Type::NULLVALUE) {
                val = nullptr;
                size = nullptr;
            }

            if(val) {

                // debug:
                if(!(val->getType() == llvm::Type::getInt8PtrTy(context, 0)
                     || val->getType() == llvm::Type::getInt64Ty(context)
                     || val->getType() == llvm::Type::getDoubleTy(context)
                     || val->getType() == _env->getBooleanType()
                     || val->getType() == _env->getEmptyTupleType()
                     || val->getType()->isStructTy())
                     || (val->getType()->isPointerTy() && val->getType()->getPointerElementType()->isStructTy())) {
                    // std::cerr<<"invalid val type: "<<_env->getLLVMTypeName(val->getType())<<std::endl;
                }

                // val must be a primitive
                assert(val->getType() == llvm::Type::getInt8PtrTy(context, 0)
                       || val->getType() == llvm::Type::getInt64Ty(context)
                       || val->getType() == llvm::Type::getDoubleTy(context)
                       || val->getType() == _env->getBooleanType()
                       || val->getType() == _env->getEmptyTupleType()
                       || val->getType()->isStructTy()
                       || (val->getType() == _env->i64ptrType() && type.isListType() && type.elementType().isSingleValued()) // special case for List[null]
                       || (val->getType()->isPointerTy() && val->getType()->getPointerElementType()->isStructTy()));


                if (val->getType() == llvm::Type::getInt8PtrTy(context, 0)) {
                    // must be string, dict, list
                    assert(type == python::Type::STRING ||
                           type.isDictionaryType() || type == python::Type::GENERICDICT ||
                           type.isListType() || type == python::Type::GENERICLIST ||
                           type == python::Type::NULLVALUE);
                }
                if(val->getType() == llvm::Type::getInt64Ty(context)) {
                    assert(type == python::Type::I64
                           || type == python::Type::BOOLEAN
                           || (type.isListType() && type.elementType().isSingleValued()));
                }
                if(val->getType() == llvm::Type::getDoubleTy(context))
                    assert(type == python::Type::F64);
                // if(val->getType() == _env->getBooleanType()) {
                //     assert(type == python::Type::BOOLEAN);
                // }

                if(val->getType()->isStructTy()) {
                    if (val->getType() == _env->getEmptyTupleType())
                        assert(type == python::Type::EMPTYTUPLE);
                    else
                        assert((type.isListType() && !type.elementType().isSingleValued())
                        || type.isStructuredDictionaryType());
                }
            }

            // size must be 64bit
            if(size)
                assert(size->getType() == llvm::Type::getInt64Ty(context));

            // some checks
            if(isnull)
                assert(isnull->getType() == llvm::Type::getInt1Ty(context));

            _tree.set(i, codegen::SerializableValue(val, size, isnull));
        }

        codegen::SerializableValue FlattenedTuple::getLoad(llvm::IRBuilder<> &builder, const std::vector<int> &index) {
            auto subtree = _tree.subTree(index);
            FlattenedTuple dummy(_env);
            dummy._tree = subtree;
            dummy._flattenedTupleType = python::Type::makeTupleType(dummy.getFieldTypes());

            using namespace std;

            // special case: subtree is single parameter -> directly use vals from tree!
            // note also special case empty tuple, else it will be sandwiched as (()) leading to errors...
            if(!subtree.tupleType().isTupleType() || subtree.tupleType() == python::Type::EMPTYTUPLE) {
                assert(subtree.numElements() == 1);
                auto size = subtree.get(0).size;
                if(size)
                    assert(size->getType() == _env->i64Type());
                return subtree.get(0);
            }

            auto size = dummy.getSize(builder);
            assert(size && size->getType() == _env->i64Type());
            return codegen::SerializableValue(dummy.getLoad(builder), size);
        }

        codegen::SerializableValue FlattenedTuple::serializeToMemory(llvm::IRBuilder<> &builder) const {

             // _env->debugPrint(builder, "entering serialize to memory");

            auto buf_size = getSize(builder);

             // debug
             // _env->debugPrint(builder, "buf_size to serialize is: ", buf_size);

            // debug print
            auto buf = _env->malloc(builder, buf_size);

            // serialize
            serialize(builder, buf);
            return codegen::SerializableValue(buf, buf_size);
        }

        std::vector<llvm::Value*> FlattenedTuple::getBitmap(llvm::IRBuilder<> &builder) const {
            using namespace std;

            auto types = getFieldTypes();
            assert(types.size() == numElements());


            // get bitmap size from llvm type
            auto llvmType = static_cast<llvm::StructType*>(getLLVMType());
            assert(llvmType->getStructElementType(0)->isArrayTy()); // bitmap

            // TODO: conversion can be done MORE efficiently, by more clever loading...
            int numOptionalElements = llvmType->getStructElementType(0)->getArrayNumElements();
            auto numBitmapElements = core::ceilToMultiple(numOptionalElements, 64) / 64; // make 64bit bitmaps
            // ndebug check
#ifndef NDEBUG
            auto numOptions = 0;
            for(auto t : types)
                if(t.isOptionType())
                    numOptions++;
            assert(numOptions <= numOptionalElements);
#endif

            // construct bitmap using or operations
            vector<llvm::Value*> bitmapArray;
            for(int i = 0; i < numBitmapElements; ++i)
                bitmapArray.emplace_back(_env->i64Const(0));

            // go through values and add to respective bitmap
            for(int i = 0; i < numElements(); ++i) {
                if(types[i].isOptionType()) {
                    // get index within bitmap
                    auto bitmapPos = std::get<2>(getTupleIndices(_flattenedTupleType, i));
                    assert(_tree.get(i).is_null);

                    bitmapArray[bitmapPos / 64] = builder.CreateOr(bitmapArray[bitmapPos / 64], builder.CreateShl(
                            builder.CreateZExt(_tree.get(i).is_null, _env->i64Type()),
                            _env->i64Const(bitmapPos % 64)));
                }
            }

            return bitmapArray;
        }


        void FlattenedTuple::print(llvm::IRBuilder<> &builder) const {
            // print tuple out for debug purposes
            using namespace  std;

            _env->debugPrint(builder, "tuple: (" + to_string(numElements()) + " elements)");

            for(int i = 0; i < numElements(); ++i) {
                auto t = fieldType(i);
                auto val = get(i);
                auto size = getSize(i);
                auto isnull = getIsNull(i);
                auto cellStr = "element("+to_string(i)+") ";
                _env->debugPrint(builder, "- " + cellStr + "type: " + t.desc());
                if(val)_env->debugPrint(builder, "  " + cellStr + "value: ", val);
                if(size)_env->debugPrint(builder, "  " + cellStr + "size: ", size);
                if(isnull)_env->debugPrint(builder, "  " + cellStr + "is_null: ", isnull);
            }
        }

        FlattenedTuple FlattenedTuple::fromRow(LLVMEnvironment *env, llvm::IRBuilder<>& builder, const Row &row) {
            FlattenedTuple ft(env);
            ft.init(row.getRowType());

            auto field_tree = tupleToTree(row.getAsTuple());
            auto indices = field_tree.getMultiIndices();
            for(auto idx : indices) {
                auto val = env->primitiveFieldToLLVM(builder, field_tree.get(idx));
                ft.set(builder, idx, val.val, val.size, val.is_null);
            }
            return ft;
        }

        FlattenedTuple FlattenedTuple::upcastTo(llvm::IRBuilder<>& builder,
                                                const python::Type& target_type,
                                                bool allow_simple_tuple_wrap) const {

            auto& logger = Logger::instance().logger("codegen");

            // create new FlattenedTuple
            FlattenedTuple ft(_env);
            ft.init(target_type);
            auto env = _env;

            // check, make sure they upcastable...
            if(!python::canUpcastType(getTupleType(), target_type)) {

                logger.debug("Can not upcast from type " + getTupleType().desc() + " to type " + target_type.desc());

                // special case: simple tuple wrap, e.g. for a single tuple ()
                if(!getTupleType().isTupleType() && python::canUpcastType(python::Type::propagateToTupleType(getTupleType()), target_type)) {
                    // ok, handle here -> this is a special case!
                    auto num_desired = ft.numElements();
                    auto num_given = numElements();

                    if(num_desired == 1 && num_given == 1) {
                        ft.setElement(builder, 0, get(0), getSize(0), getIsNull(0));
                        return ft;
                    } else {
                        throw std::runtime_error("wrapping not possible for upcast");
                    }
                } else if(getTupleType().isTupleType() && getTupleType().parameters().size() == 1) {
                    // ok, handle here -> this is a special case!
                    auto num_desired = ft.numElements();
                    auto num_given = numElements();

                    // this is ok as well, i.e. (...) gets wrapped as (())

                    logger.debug("another special case encountered...");

                } else {
                    auto err_msg = "Code generation failure, can't upcast type " + getTupleType().desc() + " to type " + target_type.desc();
                    Logger::instance().logger("codegen").debug(err_msg);
                    throw std::runtime_error(err_msg);
                }
            }

            // upgrade params
            auto paramsNew = ft.flattenedTupleType().parameters();
            auto paramsOld = this->flattenedTupleType().parameters();
            if(paramsNew.size() != paramsOld.size())
                throw std::runtime_error("types have different number of elements");

            auto num_params = paramsNew.size();
            for(unsigned i = 0; i < num_params; ++i) {
                auto val = this->get(i);
                auto size = this->getSize(i);
                auto is_null = this->getIsNull(i);

                // upcast from ... to
                auto from_type = paramsOld[i];
                auto to_type = paramsNew[i];
                auto from = SerializableValue(val, size, is_null);
                // _env->debugPrint(builder, "upcasting " + from_type.desc() + " -> " + to_type.desc() + " (col=" + std::to_string(i) + ")");
                auto to = _env->upcastValue(builder, from, from_type, to_type);
                ft.assign(i, to.val, to.size, to.is_null);
            }

            return ft;
        }

        llvm::Value *FlattenedTuple::loadToHeapPtr(llvm::IRBuilder<> &builder) const {
            auto llvm_type = getLLVMType(); assert(llvm_type);

            const auto& DL = _env->getModule()->getDataLayout();
            auto tuple_size = DL.getTypeAllocSize(llvm_type);

            auto ptr = builder.CreatePointerCast(_env->malloc(builder, tuple_size), llvm_type->getPointerTo());

#ifndef NDEBUG
            // memset to zero
            builder.CreateMemSet(ptr, _env->i8Const(0), tuple_size, 0);
#endif

            // // alloc and then memcpy (can't directly store, not even using volatile mode).
            // auto tmp_ptr = _env->CreateFirstBlockAlloca(builder, getLLVMType());
            // storeTo(builder, tmp_ptr);
            // builder.CreateMemCpy(ptr, 0, tmp_ptr, 0, tuple_size);

            storeTo(builder, ptr, true);

            // // debug: check pointer
            // auto item = ptr;
            // auto dbg_ptr = builder.CreatePointerCast(item, _env->i8ptrType());
            // auto N = tuple_size / 8;
            // _env->printValue(builder, item, "byte check for pointer: ");
            // for(unsigned i = 0; i < N; ++i) {
            //     auto i64_val = builder.CreateLoad(builder.CreatePointerCast(dbg_ptr, _env->i64ptrType()));
            //     dbg_ptr = builder.CreateGEP(dbg_ptr, _env->i64Const(sizeof(int64_t)));
            //     _env->printValue(builder, i64_val, "bytes " + std::to_string(i * 8) + "-" + std::to_string((i+1)*8) + ": ");
            // }

            return ptr;
        }
    }
}