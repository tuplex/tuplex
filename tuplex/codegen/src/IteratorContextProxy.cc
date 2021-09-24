//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <IteratorContextProxy.h>

namespace tuplex {
    namespace codegen {

        SerializableValue IteratorContextProxy::initIterContext(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                            const python::Type &iterableType,
                                                            const SerializableValue &iterable) {
            using namespace llvm;

            if(iterableType == python::Type::EMPTYLIST || iterableType == python::Type::EMPTYTUPLE) {
                // use dummy value for empty iterator
                return SerializableValue(_env->i64Const(0), _env->i64Const(8));
            }

            if(!(iterableType.isListType() || iterableType.isTupleType() || iterableType == python::Type::RANGE || iterableType == python::Type::STRING)) {
                throw std::runtime_error("unsupported iterable type" + iterableType.desc());
            }

            llvm::Type *iteratorContextType = _env->createOrGetIterIteratorType(iterableType);
            auto initBBAddr = _env->createOrGetUpdateIteratorIndexFunctionDefaultBlockAddress(builder, iterableType,
                                                                                              false);
            auto iteratorContextStruct = _env->CreateFirstBlockAlloca(builder, iteratorContextType, "iter_iterator_alloc");
            llvm::Value *iterableStruct = nullptr;
            if(iterableType.isListType() || iterableType.isTupleType()) {
                // TODO: need to change this when codegen for lists gets updated
                iterableStruct = _env->CreateFirstBlockAlloca(builder, iterable.val->getType(), "iter_arg_alloc");
            } else {
                iterableStruct = iterable.val;
            }

            // initialize block address in iterator struct to initBB
            auto blockAddrPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(0)});
            builder.CreateStore(initBBAddr, blockAddrPtr);

            // initialize index
            auto indexPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(1)});
            if(iterableType == python::Type::RANGE) {
                // initialize index to -step
                auto startPtr = builder.CreateGEP(_env->getRangeObjectType(), iterableStruct, {_env->i32Const(0), _env->i32Const(0)});
                auto start = builder.CreateLoad(startPtr);
                auto stepPtr = builder.CreateGEP(_env->getRangeObjectType(), iterableStruct, {_env->i32Const(0), _env->i32Const(2)});
                auto step = builder.CreateLoad(stepPtr);
                builder.CreateStore(builder.CreateSub(start, step), indexPtr);
            } else {
                // initialize index to -1
                builder.CreateStore(_env->i32Const(-1), indexPtr);
            }

            // store pointer to iterable struct
            auto iterablePtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(2)});
            if(iterableType.isListType() || iterableType.isTupleType()) {
                // copy original struct
                builder.CreateStore(iterable.val, iterableStruct);
            }
            builder.CreateStore(iterableStruct, iterablePtr);

            // store length for string or tuple
            if(iterableType == python::Type::STRING) {
                auto iterableLengthPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(3)});
                builder.CreateStore(builder.CreateSub(iterable.size, _env->i64Const(1)), iterableLengthPtr);
            } else if(iterableType.isTupleType()) {
                auto iterableLengthPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(3)});
                builder.CreateStore(_env->i64Const(iterableType.parameters().size()), iterableLengthPtr);
            }

            auto* dl = new DataLayout(_env->getModule().get());
            return SerializableValue(iteratorContextStruct, _env->i64Const(dl->getTypeAllocSize(iteratorContextType)));
        }

        SerializableValue IteratorContextProxy::initReversedContext(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                                const python::Type &argType,
                                                                const SerializableValue &arg) {
            using namespace llvm;

            if(argType == python::Type::EMPTYLIST || argType == python::Type::EMPTYTUPLE) {
                // use dummy value for empty iterator
                return SerializableValue(_env->i64Const(0), _env->i64Const(8));
            }

            if(!(argType.isListType() || argType.isTupleType() || argType == python::Type::RANGE || argType == python::Type::STRING)) {
                throw std::runtime_error("cannot reverse" + argType.desc());
            }

            llvm::Type *iteratorContextType = _env->createOrGetReversedIteratorType(argType);
            auto initBBAddr = _env->createOrGetUpdateIteratorIndexFunctionDefaultBlockAddress(builder, argType,true);
            auto iteratorContextStruct = _env->CreateFirstBlockAlloca(builder, iteratorContextType, "reversed_iterator_alloc");
            llvm::Value *seqStruct = nullptr;
            if(argType.isListType() || argType.isTupleType()) {
                // TODO: need to change this when codegen for lists gets updated
                seqStruct = _env->CreateFirstBlockAlloca(builder, arg.val->getType(), "reversed_arg_alloc");
            } else if(argType == python::Type::RANGE) {
                seqStruct = _env->CreateFirstBlockAlloca(builder, _env->getRangeObjectType(), "reversed_arg_alloc");
            } else {
                seqStruct = arg.val;
            }

            // initialize block address in iterator struct to initBB
            auto blockAddrPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(0)});
            builder.CreateStore(initBBAddr, blockAddrPtr);

            // initialize index
            auto indexPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(1)});
            // initialize index to object length for list, tuple or string object
            if(argType.isListType()) {
                builder.CreateStore(builder.CreateTrunc(builder.CreateExtractValue(arg.val, {1}), _env->i32Type()), indexPtr);
            } else if(argType.isTupleType()) {
                builder.CreateStore(_env->i32Const(argType.parameters().size()), indexPtr);
            } else if(argType == python::Type::STRING) {
                builder.CreateStore(builder.CreateTrunc(builder.CreateSub(arg.size, _env->i64Const(1)), _env->i32Type()), indexPtr);
            } else if(argType == python::Type::RANGE) {
                // use the following logic to reverse the range:
                // given a range(start, end, step)
                // stepSign = (step >> 63) | 1 , i.e. stepSign = 1 if step > 0 else -1; stepSign is guaranteed non-zero
                // rangeLength = (end - start - stepSign) // step + 1 , rangeLength is the number of integers within the range
                // rangeLength = rangeLength & ~(rangeLength >> 63) , i.e. if rangeLength < 0, set it to 0
                // reversedRange = range(start-step+rangeLength*step, start-step, -step)
                auto start = builder.CreateLoad(builder.CreateGEP(_env->getRangeObjectType(), arg.val, {_env->i32Const(0), _env->i32Const(0)}));
                auto end = builder.CreateLoad(builder.CreateGEP(_env->getRangeObjectType(), arg.val, {_env->i32Const(0), _env->i32Const(1)}));
                auto step = builder.CreateLoad(builder.CreateGEP(_env->getRangeObjectType(), arg.val, {_env->i32Const(0), _env->i32Const(2)}));
                auto stepSign = builder.CreateOr(builder.CreateAShr(step, _env->i64Const(63)), _env->i64Const(1));
                auto rangeLength = builder.CreateAdd(builder.CreateSDiv(builder.CreateSub(builder.CreateSub(end, start), stepSign), step), _env->i64Const(1));
                rangeLength = builder.CreateAnd(rangeLength, builder.CreateNot(builder.CreateAShr(rangeLength, _env->i64Const(63))));
                auto newEnd = builder.CreateSub(start, step);
                auto newStep = builder.CreateNeg(step);
                auto newStart = builder.CreateAdd(newEnd, builder.CreateMul(rangeLength, step));
                // store new start, end, step values to seqStruct
                auto newStartPtr = builder.CreateGEP(_env->getRangeObjectType(), seqStruct, {_env->i32Const(0), _env->i32Const(0)});
                auto newEndPtr = builder.CreateGEP(_env->getRangeObjectType(), seqStruct, {_env->i32Const(0), _env->i32Const(1)});
                auto newStepPtr = builder.CreateGEP(_env->getRangeObjectType(), seqStruct, {_env->i32Const(0), _env->i32Const(2)});
                builder.CreateStore(newStart, newStartPtr);
                builder.CreateStore(newEnd, newEndPtr);
                builder.CreateStore(newStep, newStepPtr);
                // initialize index to newStart - newStep
                builder.CreateStore(builder.CreateSub(newStart, newStep), indexPtr);
            }

            // store pointer to iterable struct
            auto seqPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(2)});
            if(argType.isListType() || argType.isTupleType()) {
                // copy original struct
                builder.CreateStore(arg.val, seqStruct);
            }
            builder.CreateStore(seqStruct, seqPtr);

            auto* dl = new DataLayout(_env->getModule().get());
            return SerializableValue(iteratorContextStruct, _env->i64Const(dl->getTypeAllocSize(iteratorContextType)));
        }

        SerializableValue IteratorContextProxy::initZipContext(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                               const std::vector<SerializableValue> &iterables,
                                                               const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            if(iterables.empty()) {
                // use dummy value for empty iterator
                return SerializableValue(_env->i64Const(0), _env->i64Const(8));
            }

            auto iterablesType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;
            llvm::Type *iteratorContextType = _env->createOrGetZipIteratorType(iterablesType, argsIteratorInfo);
            if(iteratorContextType == _env->i64Type()) {
                // empty iterator
                return SerializableValue(_env->i64Const(0), _env->i64Const(8));
            }
            auto iteratorContextStruct = _env->CreateFirstBlockAlloca(builder, iteratorContextType, "zip_iterator_alloc");
            // store pointers to iterator structs
            for (size_t i = 0; i < iterablesType.parameters().size(); ++i) {
                auto currType = iterablesType.parameters()[i];
                assert(currType.isIterableType());
                auto iterablePtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(i)});
                llvm::Value *iteratorVal;
                if(currType.isIteratorType()) {
                    iteratorVal = iterables[i].val;
                } else {
                    if(!(currType.isListType() || currType.isTupleType() || currType == python::Type::RANGE || currType == python::Type::STRING)) {
                        throw std::runtime_error("unsupported iterable type" + currType.desc());
                    }
                    iteratorVal = initIterContext(lfb, builder, currType, iterables[i]).val;
                }
                builder.CreateStore(iteratorVal, iterablePtr);
            }

            auto* dl = new DataLayout(_env->getModule().get());
            return SerializableValue(iteratorContextStruct, _env->i64Const(dl->getTypeAllocSize(iteratorContextType)));
        }

        SerializableValue IteratorContextProxy::initEnumerateContext(LambdaFunctionBuilder &lfb,
                                                                     llvm::IRBuilder<> &builder,
                                                                     const SerializableValue &iterable,
                                                                     llvm::Value *startVal,
                                                                     const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            auto iterableType = iteratorInfo->argsType;
            if(iterableType == python::Type::EMPTYITERATOR || iterableType == python::Type::EMPTYLIST || iterableType == python::Type::EMPTYTUPLE) {
                // empty iterator
                return SerializableValue(_env->i64Const(0), _env->i64Const(8));
            }
            if(!(iterableType.isIteratorType() || iterableType.isListType() || iterableType.isTupleType() || iterableType == python::Type::RANGE || iterableType == python::Type::STRING)) {
                throw std::runtime_error("unsupported iterable type" + iterableType.desc());
            }
            auto argIteratorInfo = iteratorInfo->argsIteratorInfo.front();
            llvm::Type *iteratorContextType = _env->createOrGetEnumerateIteratorType(iterableType, argIteratorInfo);
            auto iteratorContextStruct = _env->CreateFirstBlockAlloca(builder, iteratorContextType, "enumerate_iterator_alloc");
            auto startValPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(0)});
            builder.CreateStore(startVal, startValPtr);
            auto iterablePtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env->i32Const(0), _env->i32Const(1)});
            llvm::Value *iteratorVal;
            if(iterableType.isIteratorType()) {
                iteratorVal = iterable.val;
            } else {
                iteratorVal = initIterContext(lfb, builder, iterableType, iterable).val;
            }
            builder.CreateStore(iteratorVal, iterablePtr);

            auto* dl = new DataLayout(_env->getModule().get());
            return SerializableValue(iteratorContextStruct, _env->i64Const(dl->getTypeAllocSize(iteratorContextType)));
        }

        SerializableValue IteratorContextProxy::createIteratorNextCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder,
                                                                   const python::Type &yieldType,
                                                                   llvm::Value *iterator,
                                                                   const SerializableValue &defaultArg,
                                                                   const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            BasicBlock *currBB = builder.GetInsertBlock();
            BasicBlock *notExhaustedBB = BasicBlock::Create(_env->getContext(), "notExhaustedBB", currBB->getParent());
            BasicBlock *defaultArgBB = BasicBlock::Create(_env->getContext(), "defaultArgBB", currBB->getParent());
            BasicBlock *endBB = BasicBlock::Create(_env->getContext(), "endBB", currBB->getParent());

            auto exhausted = updateIteratorIndex(builder, iterator, iteratorInfo);
            // if a default value is provided, use phi nodes to choose from value based on index (iterator not exhausted) or default value (iterator exhausted)
            // else check for exception and return value based on index if iterator not exhausted
            if(defaultArg.val) {
                builder.CreateCondBr(exhausted, defaultArgBB, notExhaustedBB);
            } else {
                lfb.addException(builder, ExceptionCode::STOPITERATION, exhausted);
                builder.CreateBr(notExhaustedBB);
            }

            builder.SetInsertPoint(notExhaustedBB);
            auto nextVal = getIteratorNextElement(builder, yieldType, iterator, iteratorInfo);
            llvm::Value *retValNotExhausted = nextVal.val, *retSizeNotExhausted = nextVal.size;
            builder.CreateBr(endBB);

            builder.SetInsertPoint(defaultArgBB);
            builder.CreateBr(endBB);

            builder.SetInsertPoint(endBB);
            lfb.setLastBlock(endBB);
            if(defaultArg.val) {
                auto retVal = builder.CreatePHI(_env->pythonToLLVMType(yieldType), 2);
                auto retSize = builder.CreatePHI(_env->i64Type(), 2);
                retVal->addIncoming(retValNotExhausted, notExhaustedBB);
                retSize->addIncoming(retSizeNotExhausted, notExhaustedBB);
                retVal->addIncoming(defaultArg.val, defaultArgBB);
                retSize->addIncoming(defaultArg.size, defaultArgBB);
                return SerializableValue(retVal, retSize);
            } else {
                return SerializableValue(retValNotExhausted, retSizeNotExhausted);
            }
        }

        llvm::Value *IteratorContextProxy::updateIteratorIndex(llvm::IRBuilder<> &builder,
                                                               llvm::Value *iterator,
                                                               const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            llvm::Type *iteratorContextType = iterator->getType()->getPointerElementType();
            std::string funcName;
            auto iteratorName = iteratorInfo->iteratorName;

            if(iteratorName == "zip") {
                return updateZipIndex(builder, iterator, iteratorInfo);
            }

            if(iteratorName == "enumerate") {
                return updateEnumerateIndex(builder, iterator, iteratorInfo);
            }

            auto iterablesType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;
            std::string prefix;
            if(iteratorName == "iter") {
                if(iterablesType.isIteratorType()) {
                    // iter() call on an iterator, ignore the outer iter and call again
                    assert(argsIteratorInfo.front());
                    return updateIteratorIndex(builder, iterator, argsIteratorInfo.front());
                }
            } else if(iteratorName == "reversed") {
                prefix = "reverse";
            } else {
                throw std::runtime_error("unsupported iterator" + iteratorName);
            }

            if(iterablesType.isListType()) {
                funcName = "list_" + prefix + "iterator_update";
            } else if(iterablesType == python::Type::STRING) {
                funcName = "str_" + prefix + "iterator_update";
            } else if(iterablesType == python::Type::RANGE){
                // range_iterator is always used
                funcName = "range_iterator_update";
            } else if(iterablesType.isTupleType()) {
                funcName = "tuple_" + prefix + "iterator_update";
            } else {
                throw std::runtime_error("Iterator struct " + _env->getLLVMTypeName(iteratorContextType) + " does not have the corresponding LLVM UpdateIteratorIndex function");
            }

            // function type: i1(*struct.iterator)
            FunctionType *ft = llvm::FunctionType::get(llvm::Type::getInt1Ty(_env->getContext()),
                                                       {llvm::PointerType::get(iteratorContextType, 0)}, false);
            auto *nextFunc = _env->getModule()->getOrInsertFunction(funcName, ft).getCallee();
            auto exhausted = builder.CreateCall(nextFunc, iterator);
            return exhausted;
        }

        SerializableValue IteratorContextProxy::getIteratorNextElement(llvm::IRBuilder<> &builder,
                                                                   const python::Type &yieldType,
                                                                   llvm::Value *iterator,
                                                                   const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            llvm::Type *iteratorContextType = iterator->getType()->getPointerElementType();
            std::string funcName;
            auto iteratorName = iteratorInfo->iteratorName;

            if(iteratorName == "zip") {
                return getZipNextElement(builder, yieldType, iterator, iteratorInfo);
            }

            if(iteratorName == "enumerate") {
                return getEnumerateNextElement(builder, yieldType, iterator, iteratorInfo);
            }

            auto iterablesType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;
            if(iteratorName == "iter") {
                if(iterablesType.isIteratorType()) {
                    // iter() call on an iterator, ignore the outer iter and call again
                    assert(argsIteratorInfo.front());
                    return getIteratorNextElement(builder, yieldType, iterator, argsIteratorInfo.front());
                }
            } else if(iteratorName != "reversed") {
                throw std::runtime_error("unsupported iterator" + iteratorName);
            }

            // get current element value and size of current value
            llvm::Value *retVal = nullptr, *retSize = nullptr;
            auto indexPtr = builder.CreateGEP(iteratorContextType, iterator, {_env->i32Const(0), _env->i32Const(1)});
            auto index = builder.CreateLoad(indexPtr);
            auto iterableAllocPtr = builder.CreateGEP(iteratorContextType, iterator, {_env->i32Const(0), _env->i32Const(2)});
            auto iterableAlloc = builder.CreateLoad(iterableAllocPtr);
            if(iterablesType.isListType()) {
                auto valArrayPtr = builder.CreateGEP(_env->getListType(iterablesType), iterableAlloc, {_env->i32Const(0), _env->i32Const(2)});
                auto valArray = builder.CreateLoad(valArrayPtr);
                auto currValPtr = builder.CreateGEP(valArray, index);
                retVal = builder.CreateLoad(currValPtr);
                if(yieldType == python::Type::I64 || yieldType == python::Type::F64 || yieldType == python::Type::BOOLEAN) {
                    // note: list internal representation currently uses 1 byte for bool (although this field is never used)
                    retSize = _env->i64Const(8);
                } else if(yieldType == python::Type::STRING || yieldType.isDictionaryType()) {
                    auto sizeArrayPtr = builder.CreateGEP(_env->getListType(iterablesType), iterableAlloc, {_env->i32Const(0), _env->i32Const(3)});
                    auto sizeArray = builder.CreateLoad(sizeArrayPtr);
                    auto currSizePtr = builder.CreateGEP(sizeArray, index);
                    retSize = builder.CreateLoad(currSizePtr);
                } else if(yieldType.isTupleType()) {
                    if(!yieldType.isFixedSizeType()) {
                        // retVal is a pointer to tuple struct
                        retVal = builder.CreateLoad(retVal);
                    }
                    auto ft = FlattenedTuple::fromLLVMStructVal(_env, builder, retVal, yieldType);
                    retSize = ft.getSize(builder);
                }
            } else if(iterablesType == python::Type::STRING) {
                auto currCharPtr = builder.CreateGEP(_env->i8Type(), iterableAlloc, index);
                // allocate new string (1-byte character with a 1-byte null terminator)
                retSize = _env->i64Const(2);
                retVal = builder.CreatePointerCast(_env->malloc(builder, retSize), _env->i8ptrType());
                builder.CreateStore(builder.CreateLoad(currCharPtr), retVal);
                auto nullCharPtr = builder.CreateGEP(_env->i8Type(), retVal, _env->i32Const(1));
                builder.CreateStore(_env->i8Const(0), nullCharPtr);
            } else if(iterablesType == python::Type::RANGE) {
                retVal = index;
                retSize = _env->i64Const(8);
            } else if(iterablesType.isTupleType()) {
                // only works with homogenous tuple
                auto tupleLength = iterablesType.parameters().size();

                // create array & index
                auto array = builder.CreateAlloca(_env->pythonToLLVMType(yieldType), _env->i64Const(tupleLength));
                auto sizes = builder.CreateAlloca(_env->i64Type(), _env->i64Const(tupleLength));

                // store the elements into the array
                std::vector<python::Type> tupleType(tupleLength, yieldType);
                FlattenedTuple flattenedTuple = FlattenedTuple::fromLLVMStructVal(_env, builder, iterableAlloc, python::Type::makeTupleType(tupleType));

                std::vector<SerializableValue> elements;
                std::vector<llvm::Type *> elementTypes;
                for (int i = 0; i < tupleLength; ++i) {
                    auto load = flattenedTuple.getLoad(builder, {i});
                    elements.push_back(load);
                    elementTypes.push_back(load.val->getType());
                }

                // fill in array elements
                for (int i = 0; i < tupleLength; ++i) {
                    builder.CreateStore(elements[i].val, builder.CreateGEP(array, _env->i32Const(i)));
                    builder.CreateStore(elements[i].size, builder.CreateGEP(sizes, _env->i32Const(i)));
                }

                // load from array
                retVal = builder.CreateLoad(builder.CreateGEP(array, builder.CreateTrunc(index, _env->i32Type())));
                retSize = builder.CreateLoad(builder.CreateGEP(sizes, builder.CreateTrunc(index, _env->i32Type())));
            }
            return SerializableValue(retVal, retSize);
        }

        llvm::Value *IteratorContextProxy::updateZipIndex(llvm::IRBuilder<> &builder,
                                                          llvm::Value *iterator,
                                                          const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            auto argsType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            int zipSize = argsType.parameters().size();
            if(zipSize == 0) {
                return _env->i1Const(true);
            }

            BasicBlock *currBB = builder.GetInsertBlock();
            BasicBlock *exhaustedBB = BasicBlock::Create(_env->getContext(), "exhaustedBB", currBB->getParent());
            BasicBlock *endBB = BasicBlock::Create(_env->getContext(), "endBB", currBB->getParent());

            builder.SetInsertPoint(exhaustedBB);
            builder.CreateBr(endBB);

            builder.SetInsertPoint(endBB);
            // zipExhausted indicates whether the given zip iterator is exhausted
            auto zipExhausted = builder.CreatePHI(_env->i1Type(), 2);
            zipExhausted->addIncoming(_env->i1Const(true), exhaustedBB);

            std::vector<BasicBlock *> zipElementEntryBB;
            std::vector<BasicBlock *> zipElementCondBB;
            for (int i = 0; i < zipSize; ++i) {
                BasicBlock *currElementEntryBB = BasicBlock::Create(_env->getContext(), "zipElementBB" + std::to_string(i), currBB->getParent());
                BasicBlock *currElementCondBB = BasicBlock::Create(_env->getContext(), "currCondBB" + std::to_string(i), currBB->getParent());
                zipElementEntryBB.push_back(currElementEntryBB);
                zipElementCondBB.push_back(currElementCondBB);
            }
            zipExhausted->addIncoming(_env->i1Const(false), zipElementCondBB[zipSize - 1]);

            builder.SetInsertPoint(currBB);
            builder.CreateBr(zipElementEntryBB[0]);
            // iterate over all arg iterators
            // if the current arg iterator is exhausted, jump directly to exhaustedBB and zipExhausted will be set to true
            for (int i = 0; i < zipSize; ++i) {
                builder.SetInsertPoint(zipElementEntryBB[i]);
                auto currIteratorPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(i)});
                auto currIterator = builder.CreateLoad(currIteratorPtr);
                auto currIteratorInfo = argsIteratorInfo[i];
                assert(currIteratorInfo);
                auto exhausted = updateIteratorIndex(builder, currIterator, currIteratorInfo);
                builder.CreateBr(zipElementCondBB[i]);
                builder.SetInsertPoint(zipElementCondBB[i]);
                if(i == zipSize - 1) {
                    builder.CreateCondBr(exhausted, exhaustedBB, endBB);
                } else {
                    builder.CreateCondBr(exhausted, exhaustedBB, zipElementEntryBB[i+1]);
                }
            }
            builder.SetInsertPoint(endBB);

            return zipExhausted;
        }

        SerializableValue IteratorContextProxy::getZipNextElement(llvm::IRBuilder<> &builder,
                                                                  const python::Type &yieldType,
                                                                  llvm::Value *iterator,
                                                                  const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;
            auto argsType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            FlattenedTuple ft(_env);
            ft.init(yieldType);

            // previously UpdateIteratorIndexFunction was called on each arg iterator which increments index of each arg iterator by 1
            // restore index for all arg iterators
            incrementIteratorIndex(builder, iterator, iteratorInfo, -1);
            for (int i = 0; i < argsType.parameters().size(); ++i) {
                auto currIteratorPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(i)});
                auto currIterator = builder.CreateLoad(currIteratorPtr);
                auto currIteratorInfo = argsIteratorInfo[i];
                // update current arg iterator index before fetching value
                incrementIteratorIndex(builder, currIterator, currIteratorInfo, 1);
                auto currIteratorNextVal = getIteratorNextElement(builder, yieldType.parameters()[i], currIterator, currIteratorInfo);
                ft.setElement(builder, i, currIteratorNextVal.val, currIteratorNextVal.size, currIteratorNextVal.is_null);
            }
            auto retVal = ft.getLoad(builder);
            auto retSize = ft.getSize(builder);
            return SerializableValue(retVal, retSize);
        }

        llvm::Value *IteratorContextProxy::updateEnumerateIndex(llvm::IRBuilder<> &builder,
                                                                llvm::Value *iterator,
                                                                const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            auto argIteratorInfo = iteratorInfo->argsIteratorInfo.front();
            auto argIteratorPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(1)});
            auto argIterator = builder.CreateLoad(argIteratorPtr);
            auto enumerateExhausted = updateIteratorIndex(builder, argIterator, argIteratorInfo);

            return enumerateExhausted;
        }

        SerializableValue IteratorContextProxy::getEnumerateNextElement(llvm::IRBuilder<> &builder,
                                                                  const python::Type &yieldType,
                                                                  llvm::Value *iterator,
                                                                  const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            auto argIteratorInfo = iteratorInfo->argsIteratorInfo.front();

            FlattenedTuple ft(_env);
            ft.init(yieldType);
            auto startValPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(0)});
            auto startVal = builder.CreateLoad(startValPtr);
            auto start = SerializableValue(startVal, _env->i64Const(8));
            auto argIteratorPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(1)});
            auto argIterator = builder.CreateLoad(argIteratorPtr);
            auto val = getIteratorNextElement(builder, yieldType.parameters()[1], argIterator, argIteratorInfo);
            ft.setElement(builder, 0, start.val, start.size, start.is_null);
            ft.setElement(builder, 1, val.val, val.size, val.is_null);
            auto retVal = ft.getLoad(builder);
            auto retSize = ft.getSize(builder);
            // increment start index value
            auto newStartVal = builder.CreateAdd(startVal, _env->i64Const(1));
            builder.CreateStore(newStartVal, startValPtr);

            return SerializableValue(retVal, retSize);
        }

        void IteratorContextProxy::incrementIteratorIndex(llvm::IRBuilder<> &builder, llvm::Value *iterator, const std::shared_ptr<IteratorInfo> &iteratorInfo, int offset) {
            using namespace llvm;

            auto iteratorName = iteratorInfo->iteratorName;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            if(iteratorName == "zip") {
                for (int i = 0; i < argsIteratorInfo.size(); ++i) {
                    auto currIteratorPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(i)});
                    auto currIterator = builder.CreateLoad(currIteratorPtr);
                    incrementIteratorIndex(builder, currIterator, argsIteratorInfo[i], offset);
                }
                return;
            }

            if(iteratorName == "enumerate") {
                auto currIteratorPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(1)});
                auto currIterator = builder.CreateLoad(currIteratorPtr);
                incrementIteratorIndex(builder, currIterator, argsIteratorInfo.front(), offset);
                return;
            }

            auto iterablesType = iteratorInfo->argsType;
            if(iteratorName == "iter") {
                if(iterablesType.isIteratorType()) {
                    // iter() call on an iterator, ignore the outer iter and call again
                    assert(argsIteratorInfo.front());
                    incrementIteratorIndex(builder, iterator, argsIteratorInfo.front(), offset);
                    return;
                }
            } else if(iteratorName == "reversed") {
                // for reverseiterator, need to decrement index by offset
                offset = -offset;
            } else {
                throw std::runtime_error("unsupported iterator" + iteratorName);
            }

            // change index field
            auto indexPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(1)});
            auto currIndex = builder.CreateLoad(indexPtr);
            if(iterablesType == python::Type::RANGE) {
                // index will change by offset * step
                auto rangePtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(2)});
                auto range = builder.CreateLoad(rangePtr);
                auto stepPtr = builder.CreateGEP(_env->getRangeObjectType(), range, {_env->i32Const(0), _env->i32Const(2)});
                auto step = builder.CreateLoad(stepPtr);
                builder.CreateStore(builder.CreateAdd(currIndex, builder.CreateMul(_env->i64Const(offset), step)), indexPtr);
            } else {
                builder.CreateStore(builder.CreateAdd(currIndex, _env->i32Const(offset)), indexPtr);
            }
        }
    }
}