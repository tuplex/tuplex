//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <FlattenedTuple.h>
#include <Logger.h>
#include <sstream>

namespace tuplex {
    namespace codegen {
        std::vector<python::Type> FlattenedTuple::getFieldTypes() const {
            return _tree.fieldTypes();
        }

        FlattenedTuple
        FlattenedTuple::fromLLVMStructVal(LLVMEnvironment *env, const codegen::IRBuilder& builder, llvm::Value *ptr,
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

            // two options: either it's a pointer to llvm type OR the type directly (i.e. in struct access)
            if(llvmType->isPointerTy()) {
                assert(llvmType->isPointerTy());
                // deprecated with opaque pointers
                //assert(llvmType->getPointerElementType()->isStructTy());
                //assert(llvmType->getPointerElementType() == t.getLLVMType());

                // now fill in values using getelementptr
                for (unsigned int i = 0; i < t.numElements(); ++i)
                    vals.emplace_back(env->getTupleElement(builder, t._flattenedTupleType, ptr, i));
            } else {
                // needs to be llvmtype
                assert(llvmType == t.getLLVMType());

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

        python::Type FlattenedTuple::fieldType(const std::vector<int>& index) {

            // if index size is zero, this may mean the flattened tuple is an empty tuple
            if(index.size() == 0) {
                assert(isEmptyTuple());
                return python::Type::EMPTYTUPLE;
            }

            assert(index.size() > 0);

            return _tree.fieldType(index);
        }

        void FlattenedTuple::set(const codegen::IRBuilder& builder, const std::vector<int>& index, llvm::Value *value, llvm::Value *size, llvm::Value *is_null) {

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

        void FlattenedTuple::set(const codegen::IRBuilder& builder, const std::vector<int> &index, const FlattenedTuple &t) {
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

        // TODO: this seems like a duplication of LLVMEnvironment::pythonToLLVMType??
        llvm::Type* pythonToLLVMType(LLVMEnvironment& env, python::Type type) {
            // primitives first
            if(python::Type::BOOLEAN == type)
                return env.getBooleanType();
            if(python::Type::I64 == type)
                return env.i64Type();
            if(python::Type::F64 ==  type)
                return env.doubleType();
            if(python::Type::EMPTYTUPLE == type)
                return env.getEmptyTupleType();

            // dummy type for NULL value is i8ptr
            if(python::Type::NULLVALUE == type)
                return env.i8ptrType();

            // NEW!!! i8* is string type or dictionary type incl. empty tuple.
            if(python::Type::STRING == type || type.isDictionaryType())
                return env.i8ptrType();

            if(type.isListType()) {
                return env.createOrGetListType(type);
            }

            if(python::Type::PYOBJECT == type)
                return env.i8ptrType();

            Logger::instance().defaultLogger().warn("python type could not be converted to llvm type");

            return nullptr;
        }

        std::vector<llvm::Type*> FlattenedTuple::getTypes() {

            std::vector<llvm::Type*> types;

            // iterate over all fields & return mapped types
            for(auto type : _tree.fieldTypes()) {
                types.push_back(pythonToLLVMType(*_env, type.withoutOptions()));
            }

            return types;
        }

        void FlattenedTuple::deserializationCode(const codegen::IRBuilder& builder, llvm::Value *input) {

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
                auto atype = getLLVMType()->getStructElementType(0); assert(atype->isArrayTy());
                int numBitmapElements = core::ceilToMultiple(atype->getArrayNumElements(), (uint64_t)64) / 64;

                for(int i = 0; i < numBitmapElements; ++i) {
                    // read as 64bit int from memory
                    auto bitmapElement = builder.CreateLoad(_env->i64Type(), builder.CreateBitCast(lastPtr, _env->i64ptrType()), "bitmap_part");
                    bitmap.emplace_back(bitmapElement);
                    // set
                    lastPtr = builder.MovePtrByBytes(lastPtr, sizeof(int64_t));
                }
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
                    Value *load = builder.CreateLoad(_env->getEmptyTupleType(),
                                                     builder.CreateAlloca(_env->getEmptyTupleType(), 0, nullptr));
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
                    Value *varInfo = builder.CreateLoad(builder.getInt64Ty(), builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)),
                                                        "offset");

                    // truncation yields lower 32 bit (= offset)
                    Value *offset = builder.CreateTrunc(varInfo, Type::getInt32Ty(context));
                    // right shift by 32 yields size
                    Value *size = builder.CreateTrunc(builder.CreateLShr(varInfo, 32, "varsize"), Type::getInt32Ty(context));
                    size = builder.CreateZExtOrTrunc(size, Type::getInt64Ty(context));

                    // // debug print
                    // _env->printValue(builder, varInfo, "var info=");
                    // _env->printValue(builder, offset, "var type offset=");
                    // _env->printValue(builder, size, "var type size=");

                    // add offset to get starting point of varlen argument's memory region
                    Value *ptr = builder.MovePtrByBytes(lastPtr, offset, twine); //builder.CreateGEP(_env->i8ptrType(), lastPtr, offset, twine);
                    assert(ptr->getType() == Type::getInt8PtrTy(context, 0));
                    if(type == python::Type::STRING || type == python::Type::PYOBJECT) {
                        // // debug print string:
                        // _env->printValue(builder, ptr, "decoded str= ");

                        _tree.set(i, codegen::SerializableValue(ptr, size, isnull));
                    } else if(type == python::Type::EMPTYDICT) {
                        throw std::runtime_error("Should not happen!");
                    } else if(type.isDictionaryType()) {
                        // create the dictionary pointer
                        auto dictPtr = builder.CreateCall(cJSONParse_prototype(_env->getContext(), _env->getModule().get()),
                                                          {ptr});
                        _tree.set(i, codegen::SerializableValue(dictPtr, size, isnull));
                    } else if(type.isListType()) {
                        assert(type != python::Type::EMPTYLIST);
                        auto llvmType = _env->createOrGetListType(type);
                        llvm::Value *listAlloc = _env->CreateFirstBlockAlloca(builder, llvmType, "listAlloc");

                        // get number of elements
                        auto numElements = builder.CreateLoad(builder.getInt64Ty(), builder.CreateBitCast(ptr, Type::getInt64PtrTy(context, 0)), "list_num_elements");
                        llvm::Value* listSize = builder.CreateAlloca(Type::getInt64Ty(context));
                        builder.CreateStore(builder.CreateAdd(builder.CreateMul(numElements, _env->i64Const(8)), _env->i64Const(8)), listSize); // start list size as 8 * numElements + 8 ==> have to add string lengths for string case
                        _env->printValue(builder, builder.CreateLoad(builder.getInt64Ty(), listSize), "(deserialized) list size is (line:"+std::to_string(__LINE__)+"): ");

                        // load the list with its initial size
                        auto list_capacity_ptr = builder.CreateStructGEP(listAlloc, llvmType, 0); //_env->CreateStructGEP(builder, listAlloc,  0);
                        builder.CreateStore(numElements, list_capacity_ptr);
                        auto list_len_ptr = builder.CreateStructGEP(listAlloc, llvmType, 1); //_env->CreateStructGEP(builder, listAlloc,  1);
                        builder.CreateStore(numElements, list_len_ptr);

                        // check data
                        _env->printValue(builder, numElements, "processing list of type " + type.desc() + " with num elements= ");

                        auto elementType = type.elementType();
                        if(elementType == python::Type::STRING) {
                            auto offset_ptr = builder.CreateBitCast(builder.MovePtrByBytes(ptr, sizeof(int64_t)),
                                                                    Type::getInt64PtrTy(context, 0)); // get pointer to i64 serialized array of offsets
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
                            auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                                     numElements);
                            builder.CreateCondBr(loopNotDone, loopBodyEntry, after);

                            builder.SetInsertPoint(loopBodyEntry);
                            // store the pointer to the string
                            auto curOffset = builder.CreateLoad(builder.getInt64Ty(),
                                                                builder.CreateGEP(builder.getInt64Ty(),
                                                                                  offset_ptr,
                                                                                  builder.CreateLoad(builder.getInt64Ty(), loopCounter)));
                            _env->printValue(builder, curOffset, "cur offset to read string from is: ");
                            auto next_str_ptr = builder.CreateGEP(_env->i8ptrType(), list_arr_malloc, builder.CreateLoad(builder.getInt64Ty(), loopCounter));
                            auto curStrPtr = builder.MovePtrByBytes(builder.CreateBitCast(builder.CreateGEP(builder.getInt64Ty(),
                                                                                                       offset_ptr,
                                                                                                       builder.CreateLoad(builder.getInt64Ty(),
                                                                                                                          loopCounter)),
                                                                                     Type::getInt8PtrTy(context, 0)),
                                                               curOffset);
                            _env->printValue(builder, curStrPtr, "current string to deserialize is: ");
                            builder.CreateStore(curStrPtr, next_str_ptr);

                            _env->printValue(builder, builder.CreateLoad(_env->i8ptrType(), next_str_ptr), "saved string (recovered) is: ");

                            // set up to calculate the size based on offsets
                            auto next_size_ptr = builder.CreateGEP(builder.getInt64Ty(), list_sizearr_malloc, builder.CreateLoad(builder.getInt64Ty(), loopCounter));
                            auto lastElement = builder.CreateICmpEQ(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                                    builder.CreateSub(numElements, _env->i64Const(1)));
                            builder.CreateCondBr(lastElement, loopBodyLastEl, loopBodyReg);

                            builder.SetInsertPoint(loopBodyReg);
                            // get the next serialized offset
                            auto offset_ptr_bytes_offset = builder.CreateMul(_env->i64Const(sizeof(int64_t)), builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), loopCounter),
                                                                                                                                _env->i64Const(1)));
                            auto nextOffset = builder.CreateLoad(builder.getInt64Ty(),
                                                                 builder.CreateBitCast(builder.MovePtrByBytes(builder.CreateBitCast(offset_ptr, _env->i8ptrType()),
                                                                                   offset_ptr_bytes_offset), _env->i64ptrType()));
                            _env->printValue(builder, offset_ptr_bytes_offset, "offset bytes=");
                            _env->printValue(builder, nextOffset, "nextOffset= ");
                            _env->printValue(builder, curOffset, "curOffset= ");
                            auto curLenReg = builder.CreateSub(nextOffset, builder.CreateSub(curOffset, _env->i64Const(sizeof(uint64_t))));
                            // store it into the list
                            builder.CreateStore(curLenReg, next_size_ptr);
                            _env->printValue(builder, curLenReg, "curLenReg= ");
                            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), listSize), curLenReg), listSize);
                            _env->printValue(builder, builder.CreateLoad(builder.getInt64Ty(), listSize), "(deserialized) list size is (line:"+std::to_string(__LINE__)+"): ");

                            builder.CreateBr(loopBodyEnd);

                            builder.SetInsertPoint(loopBodyLastEl);
                            auto curLenLast = builder.CreateSub(size, builder.CreateMul(_env->i64Const(8), numElements)); // size - 8*(nE) (no -1 because of the size field before)
                            curLenLast = builder.CreateSub(curLenLast, curOffset);
                            // store it into the list
                            builder.CreateStore(curLenLast, next_size_ptr);
                            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), listSize), curLenLast), listSize);
                            _env->printValue(builder, builder.CreateLoad(builder.getInt64Ty(), listSize), "(deserialized) list size is (line:"+std::to_string(__LINE__)+"): ");

                            builder.CreateBr(loopBodyEnd);

                            builder.SetInsertPoint(loopBodyEnd);
                            // update the loop variable and return
                            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), loopCounter), _env->i64Const(1)), loopCounter);
                            builder.CreateBr(loopCondition);

                            builder.SetInsertPoint(after);
                            // store the malloc'd and populated array to the struct
                            auto list_arr = builder.CreateStructGEP(listAlloc, llvmType, 2); //_env->CreateStructGEP(builder, listAlloc, 2);
                            builder.CreateStore(list_arr_malloc, list_arr);
                            auto list_sizearr = builder.CreateStructGEP(listAlloc, llvmType, 3); //_env->CreateStructGEP(builder, listAlloc, 3);
                            builder.CreateStore(list_sizearr_malloc, list_sizearr);
                        }
                        else if(elementType == python::Type::BOOLEAN) {
                            ptr = builder.CreateBitCast(builder.MovePtrByBytes(ptr, sizeof(int64_t)), Type::getInt64PtrTy(context, 0)); // get pointer to i64 serialized array of booleans
                            // need to copy the values out because serialized boolean = 8 bytes, but llvm boolean = 1 byte
                            llvm::Function *func = builder.GetInsertBlock()->getParent();
                            assert(func);
                            BasicBlock *loopCondition = BasicBlock::Create(context, "list_loop_condition", func);
                            BasicBlock *loopBody = BasicBlock::Create(context, "list_loop_body", func);
                            BasicBlock *after = BasicBlock::Create(context, "list_after", func);

                            // how much space to reserve for list elements
                            auto& DL = _env->getModule()->getDataLayout();
                            auto llvm_element_type = _env->getBooleanType();
                            int64_t dl_element_size = static_cast<int64_t>(DL.getTypeAllocSize(llvm_element_type));
                            auto alloc_size = builder.CreateMul(numElements, _env->i64Const(dl_element_size));

                            // allocate the array
                            auto list_arr_malloc = builder.CreatePointerCast(_env->malloc(builder, alloc_size),
                                                                             llvm_element_type->getPointerTo());

                            // read the elements
                            auto loopCounter = builder.CreateAlloca(Type::getInt64Ty(context));
                            builder.CreateStore(_env->i64Const(0), loopCounter);
                            builder.CreateBr(loopCondition);

                            builder.SetInsertPoint(loopCondition);
                            auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), loopCounter), numElements);
                            builder.CreateCondBr(loopNotDone, loopBody, after);

                            builder.SetInsertPoint(loopBody);
                            auto loop_i = builder.CreateLoad(builder.getInt64Ty(), loopCounter);
                            auto list_el = builder.CreateGEP(_env->i64Type(), list_arr_malloc, loop_i); // next list element
                            // get the next serialized value
                            auto serializedbool = builder.CreateLoad(builder.getInt64Ty(),
                                                                     builder.CreateGEP(_env->i64Type(),
                                                                                       ptr,
                                                                                       loop_i));
                            auto truncbool = builder.CreateZExtOrTrunc(serializedbool, boolType);

                            // print
                            _env->printValue(builder, loop_i, "list element no= ");
                            _env->printValue(builder, truncbool, "element value: ");

                            // store it into the list
                            builder.CreateStore(truncbool, list_el);
                            // update the loop variable and return
                            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), loopCounter), _env->i64Const(1)),
                                                loopCounter);
                            builder.CreateBr(loopCondition);

                            builder.SetInsertPoint(after);
                            // store the malloc'd and populated array to the struct
                            auto list_arr = builder.CreateStructGEP(listAlloc, llvmType, 2); //_env->CreateStructGEP(builder, listAlloc, 2);
                            builder.CreateStore(list_arr_malloc, list_arr);
                        }
                        else if(elementType == python::Type::I64) {
                            // can just directly point to the serialized data
                            auto list_arr = builder.CreateStructGEP(listAlloc, llvmType, 2); //_env->CreateStructGEP(builder, listAlloc, 2);
                            auto data_ptr = builder.CreateBitCast(builder.MovePtrByBytes(ptr, _env->i64Const(sizeof(int64_t))),
                                                                  _env->i64ptrType());

                            builder.CreateStore(data_ptr, list_arr);
                        }  else if(elementType == python::Type::F64) {
                            // can just directly point to the serialized data
                            auto list_arr = builder.CreateStructGEP(listAlloc, llvmType, 2); //_env->CreateStructGEP(builder, listAlloc, 2);

                            auto data_ptr = builder.CreateBitCast(builder.MovePtrByBytes(ptr, _env->i64Const(sizeof(int64_t))),
                                                                  _env->doublePointerType());

                            builder.CreateStore(data_ptr, list_arr);
                        } else {
                            Logger::instance().defaultLogger().error("unknown type '" + type.desc() + "' to be deserialized!");
                        }

                        _env->printValue(builder, builder.CreateLoad(builder.getInt64Ty(), listSize), "(deserialized) list size is: ");

                        // set the deserialized list
                        _tree.set(i, codegen::SerializableValue(builder.CreateLoad(llvmType, listAlloc),
                                                                builder.CreateLoad(builder.getInt64Ty(), listSize),
                                                                isnull));
                    } else {
                        Logger::instance().defaultLogger().error("unknown type '" + type.desc() + "' to be deserialized!");
                    }
                } else {
                    // directly read value
                    // depending on type, generate load instructions & summing instructions for var length fields to bytesread
                    if(python::Type::BOOLEAN == type) {

                        // load directly from memory
                        Value *tmp = builder.CreateLoad(builder.getInt64Ty(),
                                                        builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)));

                        // cast to boolean type
                        Value *load = builder.CreateTrunc(tmp, boolType, twine);
                        _tree.set(i, codegen::SerializableValue(load, _env->i64Const(sizeof(int64_t)), isnull));

                    } else if(python::Type::I64 == type) {

                        // load directly from memory
                        Value *load = builder.CreateLoad(_env->i64Type(),
                                                         builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), twine);
                        _tree.set(i, codegen::SerializableValue(load, _env->i64Const(sizeof(int64_t)), isnull));

                    } else if(python::Type::F64 == type) {

                        // load directly from memory
                        Value *load = builder.CreateLoad(_env->doubleType(), builder.CreateBitCast(lastPtr, Type::getDoublePtrTy(context, 0)), twine);
                        _tree.set(i, codegen::SerializableValue(load, _env->i64Const(sizeof(int64_t)), isnull));

                    } else if(python::Type::EMPTYTUPLE == type) {
                        throw std::runtime_error("Should not happen EMPTYTUPLE");
                    } else if(python::Type::EMPTYDICT == type) {
                        throw std::runtime_error("Should not happen EMPTYDICT");
                    } else if(type.isListType()) {
                        // lists of fixed size are just represented by a length
                        Value *num_elements = builder.CreateLoad(_env->i64Type(), builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), twine);
                        _tree.set(i, codegen::SerializableValue(num_elements, _env->i64Const(sizeof(int64_t)), isnull));
                    } else {
                        Logger::instance().defaultLogger().error("unknown type '" + type.desc() + "' to be deserialized!");
                    }
                }

                // inc last ptr
                lastPtr = builder.MovePtrByBytes(lastPtr, sizeof(int64_t), "inptr");
            }
        }

        llvm::Value* FlattenedTuple::serializationCode(const codegen::IRBuilder& builder, llvm::Value *output,
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
            codegen::IRBuilder bThen(enoughCapacity);
            serialize(bThen, output);

            // set builder to insert on then block
            builder.SetInsertPoint(enoughCapacity);
            return serializationSize;
        }

        void FlattenedTuple::serialize(const codegen::IRBuilder& builder, llvm::Value *ptr) const {
            using namespace llvm;
            using namespace std;

            auto& context = _env->getContext();
            auto types = getFieldTypes();
            bool hasVarField = !getTupleType().withoutOptions().isFixedSizeType();

            // serialize fixed size + varlen fields out
            Value *lastPtr = ptr;
            size_t numSerializedElements = 0;
            for(const auto &t: types) {
                if(!(t.isSingleValued() || (t.isOptionType() && t.getReturnType().isSingleValued()))) {
                    numSerializedElements++;
                }
            }
            Value *varlenBasePtr = builder.MovePtrByBytes(lastPtr, sizeof(int64_t) * (numSerializedElements + 1), "varbaseptr");
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
                    lastPtr = builder.MovePtrByBytes(lastPtr, sizeof(int64_t), "outptr");
                }

                // add multiple of 8 bytes to varlen base ptr for bitmap
                varlenBasePtr = builder.MovePtrByBytes(varlenBasePtr, sizeof(int64_t) * bitmap.size(), "varlenbaseptr");
            }

            // step 2: serialize fields
            // go through elements
            int serialized_idx = 0; // index of serialized fields
            for(int i = 0; i < numElements(); ++i) {
                // is varlenfield? == llvm pointer type
                auto field = _tree.get(i).val;
                auto size = _tree.get(i).size;
                auto fieldType = types[i].withoutOptions();

                 // debug
                  if(field) _env->debugPrint(builder, "serializing field " + std::to_string(i) + ": ", field);
                  if(size)_env->debugPrint(builder, "serializing field size" + std::to_string(i) + ": ", size);

                 // do not need to serialize: EmptyTuple, EmptyDict, EmptyList??, NULLVALUE

                if(fieldType.isSingleValued())
                    continue; // skip all these fields, they do not need to get serialized...

                // @TODO: improve this for NULL VALUES...
                if(!field) {
                    // dummy value
                    if(fieldType.withoutOptions() == python::Type::BOOLEAN)
                        field = _env->boolConst(false);
                    if(fieldType.withoutOptions() == python::Type::I64)
                        field = _env->i64Const(0);
                    if(fieldType.withoutOptions() == python::Type::F64)
                        field = _env->f64Const(0.0);
                    if(fieldType.withoutOptions() == python::Type::STRING)
                        field = _env->strConst(builder, "");
                }
                if(!size)
                    size = _env->i64Const(0);


                assert(field);
                assert(size);

                if(fieldType.isDictionaryType() && fieldType != python::Type::EMPTYDICT) {
                    field = builder.CreateCall(
                            cJSONPrintUnformatted_prototype(_env->getContext(), _env->getModule().get()),
                            {field});
                    size = builder.CreateAdd(
                            builder.CreateCall(strlen_prototype(_env->getContext(), _env->getModule().get()), {field}),
                            _env->i64Const(1));
                }

                // special is empty dict, empty list and NULL. I.e. though they in principle are var fields, they are fixed size.
                // ==> serialize them as 0 (later optimize this away). TODO: this comment is out of date, right? we have optimized the serialization away.
                if(fieldType.isListType() && !fieldType.elementType().isSingleValued()) {
                    assert(!fieldType.isFixedSizeType());
                    // the offset is computed using how many varlen fields have been already serialized
                    int64_t fixed_offset = (static_cast<int64_t>(numSerializedElements) + 1 - serialized_idx) * static_cast<int64_t>(sizeof(int64_t));
                    Value *offset = builder.CreateAdd(_env->i64Const(fixed_offset), varlenSize);
                    // len | size
                    Value *info = builder.CreateOr(builder.CreateZExt(offset, Type::getInt64Ty(context)), builder.CreateShl(builder.CreateZExt(size, Type::getInt64Ty(context)), 32));
                    builder.CreateStore(info, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);

                    _env->printValue(builder, varlenSize, "current acc varlensize=");
                    _env->printValue(builder, offset, "serializing list (tuple element "+ std::to_string(i) + ") to offset=");

                    // get pointer to output space
                    Value *outptr = builder.MovePtrByBytes(lastPtr, offset, "list_varoff");

                    auto llvmType = _env->createOrGetListType(fieldType);

                    // serialize the number of elements
                    auto listLen = builder.CreateExtractValue(field,  {1});
                    auto listLenSerialPtr = builder.CreateBitCast(outptr, Type::getInt64PtrTy(context, 0));
                    builder.CreateStore(listLen, listLenSerialPtr);
                    outptr = builder.MovePtrByBytes(outptr, sizeof(int64_t)); // advance
                    auto elementType = fieldType.elementType();
                    if(elementType == python::Type::STRING) {
                        outptr = builder.CreateBitCast(outptr, Type::getInt64PtrTy(context, 0)); // get offset array pointer
                        // need to copy the values in because serialized boolean = 8 bytes, but llvm boolean = 1 byte
                        llvm::Function *func = builder.GetInsertBlock()->getParent();
                        assert(func);
                        BasicBlock *loopCondition = BasicBlock::Create(context, "s_list_loop_condition", func);
                        BasicBlock *loopBody = BasicBlock::Create(context, "s_list_loop_body", func);
                        BasicBlock *after = BasicBlock::Create(context, "s_list_after", func);

                        auto list_arr = builder.CreateExtractValue(field, {2});
                        auto list_size_arr = builder.CreateExtractValue(field, {3});

                        // read the elements
                        auto loopCounter = builder.CreateAlloca(Type::getInt64Ty(context));
                        auto curStrOffset = builder.CreateAlloca(Type::getInt64Ty(context));
                        builder.CreateStore(_env->i64Const(0), loopCounter);
                        builder.CreateStore(builder.CreateMul(_env->i64Const(8), listLen), curStrOffset);
                        builder.CreateBr(loopCondition);

                        builder.SetInsertPoint(loopCondition);
                        auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), loopCounter), listLen);
                        builder.CreateCondBr(loopNotDone, loopBody, after);

                        builder.SetInsertPoint(loopBody);
                        // store the serialized size
                        auto serialized_size_ptr = builder.MovePtrByBytes(builder.CreateBitCast(outptr, _env->i8ptrType()),
                                                                          builder.CreateMul(_env->i64Const(sizeof(int64_t)),
                                                                                            builder.CreateLoad(builder.getInt64Ty(), loopCounter))); // get pointer to location for serialized value
                        builder.CreateStore(builder.CreateLoad(builder.getInt64Ty(), curStrOffset), builder.CreateBitCast(serialized_size_ptr, _env->i64ptrType())); // store the current offset to the location



                        // store the serialized string
                        auto cur_size = builder.CreateLoad(builder.getInt64Ty(), builder.CreateGEP(builder.getInt64Ty(), list_size_arr, builder.CreateLoad(builder.getInt64Ty(), loopCounter))); // get size of current string
                        auto cur_str = builder.CreateLoad(_env->i8ptrType(), builder.CreateGEP(_env->i8ptrType(), list_arr, builder.CreateLoad(builder.getInt64Ty(), loopCounter))); // get current string pointer
                        auto serialized_str_ptr = builder.MovePtrByBytes(builder.CreateBitCast(serialized_size_ptr, Type::getInt8PtrTy(context, 0)), builder.CreateLoad(builder.getInt64Ty(), curStrOffset));
#if LLVM_VERSION_MAJOR < 9
                        builder.CreateMemCpy(serialized_str_ptr, cur_str, cur_size, 0, true);
#else
                        // API update here, old API only allows single alignment.
                        // new API allows src and dest alignment separately
                        builder.CreateMemCpy(serialized_str_ptr, 0, cur_str, 0, cur_size, true);
#endif
                        // debug:
                        _env->printValue(builder, cur_size, "cur_size=");
                        _env->printValue(builder, cur_str, "cur_str=");
                        _env->printValue(builder, serialized_size_ptr, "serialized str ptr=");


                        // update the loop variables and return
                        builder.CreateStore(builder.CreateSub(builder.CreateLoad(builder.getInt64Ty(), curStrOffset), _env->i64Const(sizeof(uint64_t))), curStrOffset); // curStrOffset -= 8
                        builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), curStrOffset), cur_size), curStrOffset); // curStrOffset += cur_str_len
                        builder.CreateStore(builder.CreateAdd(builder.CreateLoad(builder.getInt64Ty(), loopCounter), _env->i64Const(1)), loopCounter); // loopCounter += 1
                        builder.CreateBr(loopCondition);

                        builder.SetInsertPoint(after); // point builder to the ending block
                    } else if(elementType == python::Type::BOOLEAN) {
                        outptr = builder.CreateBitCast(outptr, Type::getInt64PtrTy(context, 0));
                        // need to copy the values in because serialized boolean = 8 bytes, but llvm boolean = 1 byte
                        llvm::Function *func = builder.GetInsertBlock()->getParent();
                        assert(func);
                        BasicBlock *loopCondition = BasicBlock::Create(context, "ds_list_loop_condition", func);
                        BasicBlock *loopBody = BasicBlock::Create(context, "ds_list_loop_body", func);
                        BasicBlock *after = BasicBlock::Create(context, "ds_list_after", func);

                        auto list_arr = builder.CreateExtractValue( field, {2});
                        // read the elements
                        auto loopCounter = builder.CreateAlloca(Type::getInt64Ty(context));
                        builder.CreateStore(_env->i64Const(0), loopCounter);
                        builder.CreateBr(loopCondition);

                        builder.SetInsertPoint(loopCondition);
                        auto loopNotDone = builder.CreateICmpSLT(builder.CreateLoad(builder.getInt64Ty(), loopCounter), listLen);
                        builder.CreateCondBr(loopNotDone, loopBody, after);

                        builder.SetInsertPoint(loopBody);
                        auto loop_i = builder.CreateLoad(loopCounter);
                        Value* list_el = builder.CreateLoad(_env->getBooleanType(), builder.CreateGEP(_env->getBooleanType(), list_arr, loop_i)); // next list element
                        list_el = builder.CreateZExtOrTrunc(list_el, Type::getInt64Ty(context)); // upcast to 8 bytes
                        auto byte_offset = builder.CreateMul(_env->i64Const(sizeof(int64_t)), loop_i);

                        _env->printValue(builder, byte_offset, "serializing to byte offset=");
                        _env->printValue(builder, list_el, "serializing element: ");

                        auto serialized_ptr = builder.MovePtrByBytes(builder.CreateBitCast(outptr, _env->i8ptrType()), byte_offset); // get pointer to location for serialized value
                        serialized_ptr = builder.CreateBitCast(serialized_ptr, _env->i64ptrType());
                        builder.CreateStore(list_el, serialized_ptr); // store the boolean into the serialization space
                        // update the loop variable and return
                        builder.CreateStore(builder.CreateAdd(loop_i, _env->i64Const(1)), loopCounter);
                        builder.CreateBr(loopCondition);

                        builder.SetInsertPoint(after); // point builder to the ending block
                    } else if(elementType == python::Type::I64 || elementType == python::Type::F64) {
                        // can just directly memcpy the array
                        auto list_arr = builder.CreateExtractValue(field, {2});
#if LLVM_VERSION_MAJOR < 9
                        builder.CreateMemCpy(outptr, list_arr, builder.CreateMul(listLen, _env->i64Const(sizeof(uint64_t))), 0, true);
#else
                        // API update here, old API only allows single alignment.
                        // new API allows src and dest alignment separately
                        builder.CreateMemCpy(outptr, 0, list_arr, 0, builder.CreateMul(listLen, _env->i64Const(sizeof(uint64_t))), true);
#endif
                    } else {
                        throw std::runtime_error("unknown list type " + fieldType.desc() + " to be serialized!");
                    }

                    _env->printValue(builder, size, "serialized list " + fieldType.desc() + " of size= ");
                    // update running variables
                    varlenSize = builder.CreateAdd(varlenSize, size);
                    lastPtr = builder.MovePtrByBytes(lastPtr, sizeof(int64_t), "outptr");
                } else if(fieldType != python::Type::EMPTYDICT && fieldType != python::Type::NULLVALUE && field->getType()->isPointerTy()) {
                    // assert that meaning is true.
                    assert(!fieldType.isFixedSizeType());

                    // this is a var field
                    // memcpy with offset

                    // the offset is computed using how many varlen fields have been already serialized
                    Value *offset = builder.CreateAdd(_env->i64Const((numSerializedElements + 1 - serialized_idx) * sizeof(int64_t)), varlenSize);

                    // store offset + length
                    // len | size
                    Value *info = builder.CreateOr(builder.CreateZExt(offset, Type::getInt64Ty(context)), builder.CreateShl(
                            builder.CreateZExt(size, Type::getInt64Ty(context)), 32)
                    );

                    builder.CreateStore(info, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);

                    // copy memory of i8 pointer
                    assert(field->getType()->isPointerTy());
                    assert(field->getType() == Type::getInt8PtrTy(context, 0));
                    Value *outptr = builder.MovePtrByBytes(lastPtr, offset, "varoff");


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
                        auto lastCharPtr = builder.MovePtrByBytes(outptr, builder.CreateSub(size, _env->i64Const(1)));
                        builder.CreateStore(_env->i8Const('\0'), lastCharPtr);
                    }

                    // also varlensize needs to be output separately, so add
                    varlenSize = builder.CreateAdd(varlenSize, size);

                    lastPtr = builder.MovePtrByBytes(lastPtr, sizeof(int64_t), "outptr");
                } else {
                    assert(fieldType.isFixedSizeType());

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

                        _env->printValue(builder, boolVal, "serializing in flattened tuple bool value=");

                        // store within output
                        Value *store = builder.CreateStore(boolVal, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);
                        lastPtr = builder.MovePtrByBytes(lastPtr, sizeof(int64_t), "outptr");
                    }
                    else if(python::Type::I64 == fieldType) {
                        // store within output
                        Value *store = builder.CreateStore(field, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);
                        lastPtr = builder.MovePtrByBytes(lastPtr, sizeof(int64_t), "outptr");
                    } else if(python::Type::F64 == fieldType) {
                        // store within output
                        Value *store = builder.CreateStore(field, builder.CreateBitCast(lastPtr, Type::getDoublePtrTy(context, 0)), false);
                        lastPtr = builder.MovePtrByBytes(lastPtr, sizeof(double), "outptr");
                    } else if(fieldType.isListType() && fieldType.elementType().isSingleValued()) {
                        // store within output - the field is just the size of the list
                        Value *store = builder.CreateStore(field, builder.CreateBitCast(lastPtr, Type::getInt64PtrTy(context, 0)), false);
                        lastPtr = builder.MovePtrByBytes(lastPtr, sizeof(int64_t), "outptr");
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
            }
        }

        void FlattenedTuple::setElement(const codegen::IRBuilder& builder,
                                        const int iElement,
                                        llvm::Value *val,
                                        llvm::Value *size,
                                        llvm::Value *is_null) {
            // make sure it is a valid index
            assert(0 <= iElement && iElement < tupleType().parameters().size());

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
                // just copy over the pointers
                set(builder, {iElement}, val, size, is_null);
            } else if(elementType == python::Type::EMPTYTUPLE) {
                // empty tuple will result in constants
                // i.e. set the value to a load of the empty tuple special type and the size to sizeof(int64_t)
                assert(_env);
                auto llvm_empty_tuple_type = _env->getEmptyTupleType();
                auto alloc = builder.CreateAlloca(llvm_empty_tuple_type, 0, nullptr);
                auto load = builder.CreateLoad(llvm_empty_tuple_type, alloc);
                set(builder, {iElement}, load, _env->i64Const(sizeof(int64_t)), _env->i1Const(false));
            } else if(elementType == python::Type::NULLVALUE) {
                set(builder, {iElement}, nullptr, nullptr, _env->i1Const(true));
            } else {
                Logger::instance().logger("codegen").error("unknown type encountered while trying to set flattened tuple structure");
                return;
            }
        }

        bool FlattenedTuple::containsVarLenField() const {
            return !tupleType().isFixedSizeType();
        }

        llvm::Value* FlattenedTuple::getSize(const codegen::IRBuilder& builder) const {
            // @TODO: make this more performant by NOT serializing anymore NULL, EMPTYDICT, EMPTYTUPLE, ...

            llvm::Value* s = _env->i64Const(0);

            // special case empty tuple --> just return 0
            if(isEmptyTuple())
                return s;

            // ==> calc via num elements * sizeof(int64_t) + add varlen sizes + varlen64bitint + bitmap.

            // calc number of serialized fixed-len fields, exclude null,emptytuple,emptydict
            size_t numSerializedFixedLenFields = 0;
            for(auto& t : _flattenedTupleType.parameters()) {
                if(!(t.isSingleValued() || (t.isOptionType() && t.getReturnType().isSingleValued()))) {
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
                      (_tree.fieldType(i).isOptionType() && _tree.fieldType(i).getReturnType().isSingleValued()))) {
                    numSerializedFixedLenFieldsCheck += 1;
                }
            }
            assert(numSerializedFixedLenFields == numSerializedFixedLenFieldsCheck);
#endif

            // add varlen sizes
            for(int i = 0; i < _tree.elements().size(); ++i) {
                auto el = _tree.elements()[i];
                if(el.val) // i.e. for null elements, don't add a size.
                    assert(el.size);
                else continue;

                if(!_tree.fieldType(i).isFixedSizeType()) {
                    s = builder.CreateAdd(s, el.size); // 0 for varlen option!

                    // debug
                    // _env->debugPrint(builder, "element " + std::to_string(i) + ": ", el.val);
                    // _env->debugPrint(builder, "element " + std::to_string(i) + " size: ", el.size);
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

        llvm::Value* FlattenedTuple::alloc(const codegen::IRBuilder& builder, const std::string& twine) const {
            // copy structure llvm like out
            auto llvmType = getLLVMType();
            return _env->CreateFirstBlockAlloca(builder, llvmType, twine);
        }

        void FlattenedTuple::storeTo(const codegen::IRBuilder& builder, llvm::Value *ptr) const {
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
                builder.CreateStore(_tree.get(0).size, builder.CreateGEP(ptr, {_env->i32Const(0), _env->i32Const(numElements())}));
                return;
            }

            // storing is straight-forward, just use helper function (don't bother with bitmap logic!) from LLVMEnv.
            for(int i = 0; i < _tree.numElements(); ++i)
                _env->setTupleElement(builder, _flattenedTupleType, ptr, i, _tree.get(i));
        }

        llvm::Value* FlattenedTuple::getLoad(const codegen::IRBuilder& builder) const {
            auto alloc = this->alloc(builder);
            storeTo(builder, alloc);
            return builder.CreateLoad(getLLVMType(), alloc);
        }

        void FlattenedTuple::assign(const int i, llvm::Value *val, llvm::Value *size, llvm::Value *isnull) {
            auto& context = _env->getContext();

            if(isnull)
                assert(isnull->getType() == _env->i1Type());

            assert(i >= 0 && i < numElements());

            // check that assigned val matches primitive type
            auto types = getFieldTypes();

            auto type = types[i].withoutOptions();

            // null val & size for nulltype
            if(type == python::Type::NULLVALUE) {
                val = nullptr;
                size = nullptr;
            }

            if(val) {
                // val must be a primitive
                assert(val->getType() == llvm::Type::getInt8PtrTy(context, 0)
                       || val->getType() == llvm::Type::getInt64Ty(context)
                       || val->getType() == llvm::Type::getDoubleTy(context)
                       || val->getType() == _env->getBooleanType()
                       || val->getType() == _env->getEmptyTupleType()
                       || val->getType()->isStructTy());


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

                if(val->getType()->isStructTy()) {
                    if (val->getType() == _env->getEmptyTupleType())
                        assert(type == python::Type::EMPTYTUPLE);
                    else
                        assert(type.isListType() && !type.elementType().isSingleValued());
                }
            }

            // size must be 64bit
            if(size)
                assert(size->getType() == llvm::Type::getInt64Ty(context));

            _tree.set(i, codegen::SerializableValue(val, size, isnull));
        }

        codegen::SerializableValue FlattenedTuple::getLoad(const codegen::IRBuilder& builder, const std::vector<int> &index) {
            auto subtree = _tree.subTree(index);
            FlattenedTuple dummy(_env);
            dummy._tree = subtree;
            dummy._flattenedTupleType = python::Type::makeTupleType(dummy.getFieldTypes());

            using namespace std;

            // special case: subtree is single parameter -> directly use vals from tree!
            // note also special case empty tuple, else it will be sandwiched as (()) leading to errors...
            if(!subtree.tupleType().isTupleType() || subtree.tupleType() == python::Type::EMPTYTUPLE) {
                assert(subtree.numElements() == 1);
                return subtree.get(0);
            }

            return codegen::SerializableValue(dummy.getLoad(builder), dummy.getSize(builder));
        }

        codegen::SerializableValue FlattenedTuple::serializeToMemory(const codegen::IRBuilder& builder) const {

            auto buf_size = getSize(builder);

            // debug
            // _env->debugPrint(builder, "buf_size to serialize is: ", buf_size);

            // debug print
            auto buf = _env->malloc(builder, buf_size);

            // serialize
            serialize(builder, buf);
            return codegen::SerializableValue(buf, buf_size);
        }

        std::vector<llvm::Value*> FlattenedTuple::getBitmap(const codegen::IRBuilder& builder) const {
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
            assert(numOptions == numOptionalElements);
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


#ifndef NDEBUG
        void FlattenedTuple::print(const codegen::IRBuilder& builder) {
            // print tuple out for debug purposes
            using namespace  std;

            _env->debugPrint(builder, "tuple: (" + to_string(numElements()) + " elements)");

            for(int i = 0; i < numElements(); ++i) {
                auto t = fieldType(i);
                auto val = get(i);
                auto size = getSize(i);
                auto isnull = getIsNull(i);
                auto cellStr = "cell("+to_string(i)+") ";
                _env->debugPrint(builder, cellStr + "type: " + t.desc());
                if(val)_env->debugPrint(builder, cellStr + "value: ", val);
                if(size)_env->debugPrint(builder, cellStr + "size: ", size);
                if(isnull)_env->debugPrint(builder, cellStr + "is_null: ", isnull);
            }
        }
#endif

        FlattenedTuple FlattenedTuple::fromRow(LLVMEnvironment *env, const codegen::IRBuilder& builder, const Row &row) {
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

        inline std::tuple<llvm::Value*, llvm::Value*> decodeSingleCell(LLVMEnvironment& env, IRBuilder& builder, llvm::Value* cellsPtr, llvm::Value* sizesPtr, unsigned i) {
            auto cellStr = builder.CreateLoad(env.i8ptrType(), builder.CreateGEP(env.i8ptrType(), cellsPtr, env.i64Const(i)), "x" + std::to_string(i));
            auto cellSize = builder.CreateLoad(env.i64Type(), builder.CreateGEP(env.i64Type(), sizesPtr, env.i64Const(i)), "s" + std::to_string(i));
            return std::make_tuple(cellStr, cellSize);
        }

        std::shared_ptr<FlattenedTuple> decodeCells(LLVMEnvironment& env, IRBuilder& builder,
                                                    const python::Type& rowType,
                                                    size_t numCells,
                                                    llvm::Value* cellsPtr,
                                                    llvm::Value* sizesPtr,
                                                    llvm::BasicBlock* nullErrorBlock,
                                                    llvm::BasicBlock* valueErrorBlock,
                                                    const std::vector<std::string>& null_values,
                                                    const std::vector<size_t>& cell_indices) {
            using namespace llvm;
            using namespace std;
            auto ft = make_shared<FlattenedTuple>(&env);

            ft->init(rowType);
            assert(rowType.isTupleType());
            assert(nullErrorBlock);
            assert(valueErrorBlock);

            assert(cellsPtr->getType() == env.i8ptrType()->getPointerTo()); // i8** => array of char* pointers
            assert(sizesPtr->getType() == env.i64ptrType()); // i64* => array of int64_t

            auto cellRowType = rowType;
            // if single tuple element, just use that... (i.e. means pipeline interprets first arg as tuple...)
            assert(cellRowType.isTupleType());
            if(cellRowType.parameters().size() == 1 && cellRowType.parameters().front().isTupleType()
               && cellRowType.parameters().front().parameters().size() > 1)
                cellRowType = cellRowType.parameters().front();

            assert(cellRowType.parameters().size() == ft->flattenedTupleType().parameters().size()); /// this must hold!

            // check, if rowType.size() != numCells, cell_indices must provide valid mapping.
            if(cellRowType.parameters().size() != numCells) {
                assert(cell_indices.size() == cellRowType.parameters().size());
                for(auto idx : cell_indices)
                    assert(idx < numCells);
            }

            // check type & assign
            for(int i = 0; i < cellRowType.parameters().size(); ++i) {
                auto t = cellRowType.parameters()[i];

                // mapping from cellPtrs -> tuple
                auto original_idx = cell_indices.empty() ? i : cell_indices[i];
                auto llvm_original_idx = env.i64Const(i);
                llvm::Value* isnull = nullptr;

                // option type? do NULL value interpretation
                if(t.isOptionType()) {
                    auto cellStr = builder.CreateLoad(env.i8ptrType(), builder.CreateGEP(env.i8ptrType(), cellsPtr, llvm_original_idx), "x" + std::to_string(original_idx));
                    isnull = env.compareToNullValues(builder, cellStr, null_values, true);
                } else if(t != python::Type::NULLVALUE) {
                    // null check, i.e. raise NULL value exception!
                    auto val = builder.CreateLoad(env.i8ptrType(),
                                                  builder.CreateGEP(env.i8ptrType(), cellsPtr, llvm_original_idx),
                                                  "x" + std::to_string(original_idx));
                    auto null_check = env.compareToNullValues(builder, val, null_values, true);

                    // if positive, exception!
                    // else continue!
                    BasicBlock* bbNullCheckPassed = BasicBlock::Create(builder.getContext(),
                                                                       "col" + std::to_string(original_idx) + "_null_check_passed",
                                                                       builder.GetInsertBlock()->getParent());
                    builder.CreateCondBr(null_check, nullErrorBlock, bbNullCheckPassed);
                    builder.SetInsertPoint(bbNullCheckPassed);
                }

                t = t.withoutOptions();

                llvm::Value* cellStr = nullptr, *cellSize = nullptr;

                // values?
                if(python::Type::STRING == t) {
                    // fill in
                    auto val = builder.CreateLoad(env.i8ptrType(), builder.CreateGEP(env.i8ptrType(),
                                                                                     cellsPtr, llvm_original_idx),
                                                  "x" + std::to_string(i));
                    auto size = builder.CreateLoad(env.i64Type(), builder.CreateGEP(env.i64Type(), sizesPtr, llvm_original_idx),
                                                   "s" + std::to_string(i));
                    ft->assign(i, val, size, isnull);
                } else if(python::Type::BOOLEAN == t) {
                    // conversion code here
                    std::tie(cellStr, cellSize) = decodeSingleCell(env, builder, cellsPtr, sizesPtr, original_idx);
                    auto val = parseBoolean(env, builder, valueErrorBlock, cellStr, cellSize, isnull);
                    ft->assign(i, val.val, val.size, isnull);
                } else if(python::Type::I64 == t) {
                    // conversion code here
                    std::tie(cellStr, cellSize) = decodeSingleCell(env, builder, cellsPtr, sizesPtr, original_idx);
                    auto val = parseI64(env, builder, valueErrorBlock, cellStr, cellSize, isnull);
                    ft->assign(i, val.val, val.size, isnull);
                } else if(python::Type::F64 == t) {
                    // conversion code here
                    std::tie(cellStr, cellSize) = decodeSingleCell(env, builder, cellsPtr, sizesPtr, original_idx);
                    auto val = parseF64(env, builder, valueErrorBlock, cellStr, cellSize, isnull);
                    ft->assign(i, val.val, val.size, isnull);
                } else if(python::Type::NULLVALUE == t) {
                    // perform null check only, & set null element depending on result
                    std::tie(cellStr, cellSize) = decodeSingleCell(env, builder, cellsPtr, sizesPtr, original_idx);
                    isnull = env.compareToNullValues(builder, cellStr, null_values, true);

                    // if not null, exception! ==> i.e. ValueError!
                    BasicBlock* bbNullCheckPassed = BasicBlock::Create(builder.getContext(), "col" + std::to_string(original_idx) + "_value_check_passed", builder.GetInsertBlock()->getParent());
                    builder.CreateCondBr(isnull, bbNullCheckPassed, valueErrorBlock);
                    builder.SetInsertPoint(bbNullCheckPassed);
                    ft->assign(i, nullptr, nullptr, env.i1Const(true)); // set NULL (should be ignored)
                } else {
                    // NOTE: only flat, primitives yet supported. I.e. there can't be lists/dicts within a cell...
                    throw std::runtime_error("unsupported type " + t.desc() + " in decodeCells encountered");
                }
            }

            return ft;
        }

        std::shared_ptr<FlattenedTuple> decodeCells(LLVMEnvironment& env, IRBuilder& builder,
                                                    const python::Type& rowType,
                                                    llvm::Value* numCells,
                                                    llvm::Value* cellsPtr,
                                                    llvm::Value* sizesPtr,
                                                    llvm::BasicBlock* cellCountMismatchErrorBlock,
                                                    llvm::BasicBlock* nullErrorBlock,
                                                    llvm::BasicBlock* valueErrorBlock,
                                                    const std::vector<std::string>& null_values,
                                                    const std::vector<size_t>& cell_indices) {
            using namespace llvm;

            auto num_parameters = (uint64_t)rowType.parameters().size();

            assert(cellCountMismatchErrorBlock);

            // check numCells
            auto func = builder.GetInsertBlock()->getParent(); assert(func);
            BasicBlock* bbCellNoOk = BasicBlock::Create(env.getContext(), "noCellsOK", func);
            auto cell_match_cond = builder.CreateICmpEQ(numCells, llvm::ConstantInt::get(numCells->getType(), num_parameters));
            builder.CreateCondBr(cell_match_cond, bbCellNoOk, cellCountMismatchErrorBlock);
            builder.SetInsertPoint(bbCellNoOk);

            return decodeCells(env, builder, rowType, num_parameters, cellsPtr,
                               sizesPtr, nullErrorBlock, valueErrorBlock, null_values, cell_indices);
        }
    }
}