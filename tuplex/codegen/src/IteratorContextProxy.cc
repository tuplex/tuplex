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

        SerializableValue IteratorContextProxy::initIterContext(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder,
                                                            const python::Type &iterableType,
                                                            const SerializableValue &iterable) {
            throw std::runtime_error("deprecated");

            using namespace llvm;

            if(iterableType == python::Type::EMPTYLIST || iterableType == python::Type::EMPTYTUPLE) {
                // use dummy value for empty iterator
                return SerializableValue(_env->i64Const(0), _env->i64Const(8));
            }

            if(!(iterableType.isListType() || iterableType.isTupleType() || iterableType == python::Type::RANGE || iterableType == python::Type::STRING)) {
                throw std::runtime_error("unsupported iterable type " + iterableType.desc());
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
                auto start = builder.CreateLoad(_env->i64Type(), startPtr);
                auto stepPtr = builder.CreateGEP(_env->getRangeObjectType(), iterableStruct, {_env->i32Const(0), _env->i32Const(2)});
                auto step = builder.CreateLoad(_env->i64Type(), stepPtr);
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

        SerializableValue IteratorContextProxy::initReversedContext(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder,
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

            // @TODO: what about string? -> should perform better iterator testing.

            llvm::Type *iteratorContextType = _env->createOrGetReversedIteratorType(argType);
            auto initBBAddr = _env->createOrGetUpdateIteratorIndexFunctionDefaultBlockAddress(builder, argType,true);
            auto iteratorContextStruct = _env->CreateFirstBlockAlloca(builder, iteratorContextType, "reversed_iterator_alloc");
            llvm::Value *seqStruct = nullptr;
            if(argType.isTupleType()) {
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

                // what type is it? pointer or struct?
#ifndef NDEBUG
                if(!arg.val->getType()->isPointerTy()) {
                    // old
                    // builder.CreateStore(builder.CreateTrunc(builder.CreateExtractValue(arg.val, {1}), _env->i32Type()), indexPtr);
                    throw std::runtime_error("make sure to pass in list as ptr");
                }
#endif
                // new:
                auto llvm_list_type = _env->createOrGetListType(argType);
                auto list_len = builder.CreateLoad(builder.getInt64Ty(), builder.CreateStructGEP(arg.val, llvm_list_type, 1));
                auto index_val = builder.CreateZExtOrTrunc(list_len, _env->i32Type());
                builder.CreateStore(index_val, indexPtr);

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
                auto start = builder.CreateLoad(_env->i64Type(), builder.CreateGEP(_env->getRangeObjectType(), arg.val, {_env->i32Const(0), _env->i32Const(0)}));
                auto end = builder.CreateLoad(_env->i64Type(), builder.CreateGEP(_env->getRangeObjectType(), arg.val, {_env->i32Const(0), _env->i32Const(1)}));
                auto step = builder.CreateLoad(_env->i64Type(), builder.CreateGEP(_env->getRangeObjectType(), arg.val, {_env->i32Const(0), _env->i32Const(2)}));
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
            if(argType.isTupleType()) {
                // copy original struct
                builder.CreateStore(arg.val, seqStruct);
            }
            builder.CreateStore(seqStruct, seqPtr);

            auto* dl = new DataLayout(_env->getModule().get());
            return SerializableValue(iteratorContextStruct, _env->i64Const(dl->getTypeAllocSize(iteratorContextType)));
        }

        SerializableValue IteratorContextProxy::initZipContext(LambdaFunctionBuilder &lfb, const codegen::IRBuilder& builder,
                                                               const std::vector<SerializableValue> &iterables,
                                                               const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            ZipIterator it(*_env);

            return it.initContext(lfb, builder, iterables, python::Type::UNKNOWN, iteratorInfo);
        }

        SerializableValue IteratorContextProxy::initEnumerateContext(LambdaFunctionBuilder &lfb,
                                                                     const codegen::IRBuilder& builder,
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
                throw std::runtime_error("unsupported iterable type " + iterableType.desc());
            }

            auto iteratorContextType = createIteratorContextTypeFromIteratorInfo(*_env, *iteratorInfo);

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

        SerializableValue IteratorContextProxy::createIteratorNextCall(LambdaFunctionBuilder &lfb,
                                                                       const codegen::IRBuilder& builder,
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

            _env->printValue(builder, exhausted, "iterator exhausted: ");

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
            _env->debugPrint(builder, "in default arg block");
            builder.CreateBr(endBB);

            builder.SetInsertPoint(endBB);
            lfb.setLastBlock(endBB);

            auto llvm_yield_type = _env->pythonToLLVMType(yieldType);
            auto default_yield_value = defaultArg.val;
            auto default_yield_size = defaultArg.size;

            // sometime size is nullptr fill with default (0)
            if(!default_yield_size)
                default_yield_size = _env->i64Const(0);

            if(default_yield_value && !yieldType.isImmutable()) {
                llvm_yield_type = llvm_yield_type->getPointerTo();
//                if(default_yield_value)
//                    default_yield_value = _env->nullConstant(defaultArg.val->getType());
            }

            if(default_yield_value) {
                auto retVal = builder.CreatePHI(llvm_yield_type, 2);
                auto retSize = builder.CreatePHI(_env->i64Type(), 2);
                retVal->addIncoming(retValNotExhausted, notExhaustedBB);
                retSize->addIncoming(retSizeNotExhausted, notExhaustedBB);
                retVal->addIncoming(default_yield_value, defaultArgBB);
                retSize->addIncoming(default_yield_size, defaultArgBB);
                _env->printValue(builder, retVal, "has default value: ");
                _env->printValue(builder, retSize, "default value size: ");
                return SerializableValue(retVal, retSize);
            } else {
                return SerializableValue(retValNotExhausted, retSizeNotExhausted);
            }
        }

        // free function
        llvm::Value* update_iterator_index(LLVMEnvironment& env,
                                           const codegen::IRBuilder& builder,
                                           llvm::Value* iterator,
                                           const std::shared_ptr<IteratorInfo>& iteratorInfo) {

            assert(iteratorInfo);
            auto iterablesType = iteratorInfo->argsType;

            if(iteratorInfo->iteratorName == "iter") {
                // special case, iterablesType is another iterator: -> update that iterator
                if(iterablesType.isIteratorType()) {
                    // get the underlying type and update
                    assert(iteratorInfo->argsIteratorInfo.size() == 1);
                    return update_iterator_index(env, builder, iterator, iteratorInfo->argsIteratorInfo.front());
                }

                // must be a primitive to iterate over, update accordingly.
                SequenceIterator it(env);
                return it.updateIndex(builder, iterator, iterablesType, iteratorInfo);
            }

            if(iteratorInfo->iteratorName == "reversed") {
                ReversedIterator it(env);
                return it.updateIndex(builder, iterator, iterablesType, iteratorInfo);
            }

            if(iteratorInfo->iteratorName == "zip") {
                ZipIterator it(env);
                // iterablesType no necessary for zip
                return it.updateIndex(builder, iterator, iterablesType, iteratorInfo);
            }

            if(iteratorInfo->iteratorName == "enumerate") {
                EnumerateIterator it(env);
                return it.updateIndex(builder, iterator, iterablesType, iteratorInfo);
            }

            throw std::runtime_error("unimplemented iterator " + iteratorInfo->iteratorName + " requested for update");
        }

        // free function for general next element dispatch
        SerializableValue next_from_iterator(LLVMEnvironment& env,
                                             const codegen::IRBuilder& builder,
                                             const python::Type &yieldType,
                                             llvm::Value *iterator,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            // use general dispatch function

            auto iterablesType = iteratorInfo->argsType;

            // use same dispatch here as in update index to the new class structure
            if(iteratorInfo->iteratorName == "iter") {

                // is it another iterator? simply call next on it
                if(iterablesType.isIteratorType()) {
                    // get the underlying type and update
                    assert(iteratorInfo->argsIteratorInfo.size() == 1);
                    return next_from_iterator(env, builder, yieldType, iterator, iteratorInfo->argsIteratorInfo.front());
                }

                SequenceIterator it(env);
                return it.nextElement(builder, yieldType, iterator, iterablesType, iteratorInfo);
            }

            if(iteratorInfo->iteratorName == "reversed") {
                ReversedIterator it(env);
                return it.nextElement(builder, yieldType, iterator, iterablesType, iteratorInfo);
            }

            if(iteratorInfo->iteratorName == "enumerate") {
                EnumerateIterator it(env);
                return it.nextElement(builder, yieldType, iterator, iterablesType, iteratorInfo);
            }

            if(iteratorInfo->iteratorName == "zip") {
                ZipIterator it(env);
                return it.nextElement(builder, yieldType, iterator, iterablesType, iteratorInfo);
            }

            throw std::runtime_error("unimplemented iterator " + iteratorInfo->iteratorName + " requested for next");
        }

        // free function for global dispatch
        void increment_iterator_index(LLVMEnvironment& env, const codegen::IRBuilder& builder,
                               llvm::Value *iterator,
                               const std::shared_ptr<IteratorInfo> &iteratorInfo,
                               int32_t offset) {
            using namespace llvm;

            auto iteratorName = iteratorInfo->iteratorName;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            // general iterator type
            auto llvm_iterator_type = createIteratorContextTypeFromIteratorInfo(env, *iteratorInfo);

            if(iteratorName == "zip") {
                for (int i = 0; i < argsIteratorInfo.size(); ++i) {
                    auto currIteratorPtr = builder.CreateStructGEP(iterator, llvm_iterator_type, i);

                    // get iterator type
                    auto llvm_inner_iterator_type = createIteratorContextTypeFromIteratorInfo(env, *argsIteratorInfo[i]);

                    auto currIterator = builder.CreateLoad(llvm_inner_iterator_type->getPointerTo(), currIteratorPtr);
                    increment_iterator_index(env, builder, currIterator, argsIteratorInfo[i], offset);
                }
                return;
            }

            if(iteratorName == "enumerate") {
                // get iterator type
                auto llvm_inner_iterator_type = createIteratorContextTypeFromIteratorInfo(env, *argsIteratorInfo.front());

                auto currIteratorPtr = builder.CreateStructGEP(iterator, llvm_iterator_type, 1);
                auto currIterator = builder.CreateLoad(llvm_inner_iterator_type->getPointerTo(0), currIteratorPtr);
                increment_iterator_index(env, builder, currIterator, argsIteratorInfo.front(), offset);
                return;
            }

            auto iterablesType = iteratorInfo->argsType;
            if(iteratorName == "iter") {
                if(iterablesType.isIteratorType()) {
                    // iter() call on an iterator, ignore the outer iter and call again
                    assert(argsIteratorInfo.front());
                    increment_iterator_index(env, builder, iterator, argsIteratorInfo.front(), offset);
                    return;
                }
            } else if(iteratorName == "reversed") {
                // for reverseiterator, need to decrement index by offset
                offset = -offset;
            } else {
                throw std::runtime_error("unsupported iterator" + iteratorName);
            }

            // change index field
            auto indexPtr = builder.CreateStructGEP(iterator, llvm_iterator_type, 1);
            // is iterator always i32?
            auto currIndex = builder.CreateLoad(builder.getInt32Ty(), indexPtr);
            if(iterablesType == python::Type::RANGE) {

//
//                auto startPtr = builder.CreateGEP(_env.getRangeObjectType(), iterableStruct, {_env.i32Const(0), _env.i32Const(0)});
//                auto start = builder.CreateLoad(_env.i64Type(), startPtr);
//                auto stepPtr = builder.CreateGEP(env.getRangeObjectType(), iterator, {env.i32Const(0), env.i32Const(2)});
//                auto step = builder.CreateLoad(env.i64Type(), stepPtr);
//#error ""
                // index will change by offset * step
                auto rangePtr = builder.CreateGEP(env.getRangeObjectType(), iterator, {env.i32Const(0), env.i32Const(2)});
                auto range = builder.CreateLoad(env.getRangeObjectType(), rangePtr);
                auto stepPtr = builder.CreateGEP(env.getRangeObjectType(), range, {env.i32Const(0), env.i32Const(2)});
                auto step = builder.CreateLoad(stepPtr);
                builder.CreateStore(builder.CreateAdd(currIndex, builder.CreateMul(env.i64Const(offset), step)), indexPtr);
            } else {
                builder.CreateStore(builder.CreateAdd(currIndex, env.i32Const(offset)), indexPtr);
            }
        }

        llvm::Value *IteratorContextProxy::updateIteratorIndex(const codegen::IRBuilder& builder,
                                                               llvm::Value *iterator,
                                                               const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            assert(iteratorInfo);

            // new:
           // -> invoke general dispatch function
            auto updated_iterator = update_iterator_index(*_env, builder, iterator, iteratorInfo);

            assert(updated_iterator);

            return updated_iterator;

            // old:
            // iterator is a pointer to



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
                prefix = "reverse_";
            } else {
                throw std::runtime_error("unsupported iterator" + iteratorName);
            }

            auto iterable_name = _env->iterator_name_from_type(iterablesType);
            if(iterable_name.empty()) {
              throw std::runtime_error("Iterator struct " + _env->getLLVMTypeName(iteratorContextType)
                                           + " does not have the corresponding LLVM UpdateIteratorIndex function");
            } else if(iterablesType == python::Type::RANGE) {
                // special case range -> it's one structure (for all!)
                funcName = "range_iterator_update";
            } else {
              if(!strEndsWith(iterable_name, "_"))
                iterable_name += "_";
              funcName = iterable_name + prefix + "iterator_update";
            }

            // function type: i1(*struct.iterator)
            FunctionType *ft = llvm::FunctionType::get(llvm::Type::getInt1Ty(_env->getContext()),
                                                       {llvm::PointerType::get(iteratorContextType, 0)}, false);

            auto& logger = Logger::instance().logger("codegen");
            logger.debug("iterator context type: " + _env->getLLVMTypeName(iteratorContextType));
            logger.debug("ft type: " + _env->getLLVMTypeName(ft));
            logger.debug("iterator type: " + _env->getLLVMTypeName(iterator->getType()));

            // ok, update is something crazy fancy here: mod.getOrInsertFunction(name, FT).getCallee()->getType()->getPointerElementType()->isFunctionTy()

            auto nextFunc_value = llvm::getOrInsertCallable(*_env->getModule(), funcName, ft);
            llvm::FunctionCallee nextFunc_callee(ft, nextFunc_value);
            auto exhausted = builder.CreateCall(nextFunc_callee, iterator);
            return exhausted;
        }

        SerializableValue IteratorContextProxy::getIteratorNextElement(const codegen::IRBuilder& builder,
                                                                   const python::Type &yieldType,
                                                                   llvm::Value *iterator,
                                                                   const std::shared_ptr<IteratorInfo> &iteratorInfo) {

            // new

            return next_from_iterator(*_env, builder, yieldType, iterator, iteratorInfo);

            // old code:
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
                auto valArrayPtr = builder.CreateGEP(_env->createOrGetListType(iterablesType), iterableAlloc, {_env->i32Const(0), _env->i32Const(2)});
                auto valArray = builder.CreateLoad(valArrayPtr);
                auto currValPtr = builder.CreateGEP(valArray, index);
                retVal = builder.CreateLoad(currValPtr);
                if(yieldType == python::Type::I64 || yieldType == python::Type::F64 || yieldType == python::Type::BOOLEAN) {
                    // note: list internal representation currently uses 1 byte for bool (although this field is never used)
                    retSize = _env->i64Const(8);
                } else if(yieldType == python::Type::STRING || yieldType.isDictionaryType()) {
                    auto sizeArrayPtr = builder.CreateGEP(_env->createOrGetListType(iterablesType), iterableAlloc, {_env->i32Const(0), _env->i32Const(3)});
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
                auto array = builder.CreateAlloca(_env->pythonToLLVMType(yieldType), 0, _env->i64Const(tupleLength));
                auto sizes = builder.CreateAlloca(_env->i64Type(), 0, _env->i64Const(tupleLength));

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

        llvm::Value *IteratorContextProxy::updateZipIndex(const codegen::IRBuilder& builder,
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

        SerializableValue IteratorContextProxy::getZipNextElement(const codegen::IRBuilder& builder,
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
                auto currIteratorInfo = argsIteratorInfo[i];
                auto llvm_curr_iterator_type = createIteratorContextTypeFromIteratorInfo(*_env, *currIteratorInfo.get());
                auto currIteratorPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(i)});
                auto currIterator = builder.CreateLoad(llvm_curr_iterator_type->getPointerTo(), currIteratorPtr);

                // update current arg iterator index before fetching value
                incrementIteratorIndex(builder, currIterator, currIteratorInfo, 1);
                auto currIteratorNextVal = getIteratorNextElement(builder, yieldType.parameters()[i], currIterator, currIteratorInfo);
                ft.setElement(builder, i, currIteratorNextVal.val, currIteratorNextVal.size, currIteratorNextVal.is_null);
            }
            auto retVal = ft.getLoad(builder);
            auto retSize = ft.getSize(builder);
            return SerializableValue(retVal, retSize);
        }

        llvm::Value *IteratorContextProxy::updateEnumerateIndex(const codegen::IRBuilder& builder,
                                                                llvm::Value *iterator,
                                                                const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            auto argIteratorInfo = iteratorInfo->argsIteratorInfo.front();
            auto argIteratorPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(1)});
            auto argIterator = builder.CreateLoad(argIteratorPtr);
            auto enumerateExhausted = updateIteratorIndex(builder, argIterator, argIteratorInfo);

            return enumerateExhausted;
        }

        SerializableValue IteratorContextProxy::getEnumerateNextElement(const codegen::IRBuilder& builder,
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

        void IteratorContextProxy::incrementIteratorIndex(const codegen::IRBuilder& builder,
                                                          llvm::Value *iterator,
                                                          const std::shared_ptr<IteratorInfo> &iteratorInfo,
                                                          int offset) {
            using namespace llvm;

            auto iteratorName = iteratorInfo->iteratorName;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            if(iteratorName == "zip") {
                for (int i = 0; i < argsIteratorInfo.size(); ++i) {
                    auto currIteratorPtr = builder.CreateGEP(iterator, {_env->i32Const(0), _env->i32Const(i)});

                    // get iterator type
                    auto llvm_iterator_type = createIteratorContextTypeFromIteratorInfo(*_env, *argsIteratorInfo[i]);

                    auto currIterator = builder.CreateLoad(llvm_iterator_type->getPointerTo(), currIteratorPtr);
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
            auto currIndex = builder.CreateLoad(builder.getInt32Ty(), indexPtr);
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

        // helper to retrieve iteratorcontexttype from iteratorInfo
        llvm::Type* createIteratorContextTypeFromIteratorInfo(LLVMEnvironment& env, const IteratorInfo& iteratorInfo) {
            // coupled with FunctionRegistry
            if(iteratorInfo.iteratorName == "enumerate") {
                auto argIteratorInfo = iteratorInfo.argsIteratorInfo.front();
                auto iterableType = iteratorInfo.argsType;
                llvm::Type *iteratorContextType = env.createOrGetEnumerateIteratorType(iterableType, argIteratorInfo);
                return iteratorContextType;
            }

            if(iteratorInfo.iteratorName == "iter") {
                auto iterableType = iteratorInfo.argsType;

                // special case: is iterator, get the type of the inner iterator
                if(iterableType.isIteratorType()) {
                    assert(iteratorInfo.argsIteratorInfo.size() == 1);
                    return createIteratorContextTypeFromIteratorInfo(env, *iteratorInfo.argsIteratorInfo.front());
                }

                llvm::Type *iteratorContextType = env.createOrGetIterIteratorType(iterableType);
                return iteratorContextType;
            }

            if(iteratorInfo.iteratorName == "reversed") {
                auto iterableType = iteratorInfo.argsType;
                return env.createOrGetReversedIteratorType(iterableType);
            }

            if(iteratorInfo.iteratorName == "zip") {
                auto iterablesType = iteratorInfo.argsType;
                auto argsIteratorInfo = iteratorInfo.argsIteratorInfo;
                return env.createOrGetZipIteratorType(iterablesType, argsIteratorInfo);
            }

            throw std::runtime_error("invalid iterator info for iterator " + iteratorInfo.iteratorName + " given, can't deduce llvm type.");
        }

        SerializableValue
        SequenceIterator::initContext(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                      const codegen::IRBuilder &builder,
                                      const SerializableValue& iterable,
                                      const python::Type& iterableType,
                                      const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            // empty sequence? -> return dummy value
            if(iterableType == python::Type::EMPTYLIST ||
               iterableType == python::Type::EMPTYTUPLE ||
               iterableType == python::Type::EMPTYDICT) {
                // use dummy value for empty iterator
                return SerializableValue(_env.i64Const(0), _env.i64Const(8));
            }

            // generator? -> return generator as is
            if(iterableType.isIteratorType()) {
                return iterable; // <-- must hold pointer to iterator struct.
            }

            if(!(iterableType.isListType() ||
                 iterableType.isTupleType() ||
                 iterableType == python::Type::RANGE ||
                 iterableType == python::Type::STRING)) {
                throw std::runtime_error("unsupported iterable type " + iterableType.desc() + " for iterator " + name());
            }

            // mapping of python type -> llvm type.
            auto llvm_iterable_type = _env.pythonToLLVMType(iterableType);

            _env.debugPrint(builder, "init context for " + iterableType.desc());


            llvm::Type *iteratorContextType = _env.createOrGetIterIteratorType(iterableType);
            auto initBBAddr = _env.createOrGetUpdateIteratorIndexFunctionDefaultBlockAddress(builder, iterableType,
                                                                                              false);
            auto iteratorContextStruct = _env.CreateFirstBlockAlloca(builder, iteratorContextType, "iter_iterator_alloc");
            llvm::Value *iterableStruct = nullptr;

            auto copy_iterable_by_value = iterableType.isTupleType() || python::Type::STRING == iterableType;

            if(copy_iterable_by_value) { // <-- tuple is immutable, so storing a copy is fine!
                assert(iterable.val->getType() == llvm_iterable_type);
                // copy-by-value
                iterableStruct = _env.CreateFirstBlockAlloca(builder, llvm_iterable_type, "iter_arg_alloc");
            } else {
                // reference to the value to iterate over (copy-by-reference)
                iterableStruct = iterable.val;
            }

            // initialize block address in iterator struct to initBB
            auto blockAddrPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env.i32Const(0), _env.i32Const(0)});
            builder.CreateStore(initBBAddr, blockAddrPtr);

            // initialize index
            auto indexPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env.i32Const(0), _env.i32Const(1)});
            if(iterableType == python::Type::RANGE) {
                // initialize index to -step
                auto startPtr = builder.CreateGEP(_env.getRangeObjectType(), iterableStruct, {_env.i32Const(0), _env.i32Const(0)});
                auto start = builder.CreateLoad(_env.i64Type(), startPtr);
                auto stepPtr = builder.CreateGEP(_env.getRangeObjectType(), iterableStruct, {_env.i32Const(0), _env.i32Const(2)});
                auto step = builder.CreateLoad(_env.i64Type(), stepPtr);
                builder.CreateStore(builder.CreateSub(start, step), indexPtr);
            } else {
                // initialize index to -1
                builder.CreateStore(_env.i32Const(-1), indexPtr);
            }

            // store pointer to iterable struct
            auto iterablePtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env.i32Const(0), _env.i32Const(2)});
            if(copy_iterable_by_value) {
                // copy original struct
                builder.CreateStore(iterable.val, iterableStruct);
            } else {
                iterableStruct = iterable.val; // copy by reference
            }

            // special case string:
            if(python::Type::STRING == iterableType) {
                auto str_value = builder.CreateLoad(_env.i8ptrType(), iterableStruct);
                builder.CreateStore(str_value, iterablePtr);
            } else {
                builder.CreateStore(iterableStruct, iterablePtr);
            }

            // store length for string or tuple
            if(iterableType == python::Type::STRING) {
                auto iterableLengthPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env.i32Const(0), _env.i32Const(3)});
                builder.CreateStore(builder.CreateSub(iterable.size, _env.i64Const(1)), iterableLengthPtr);
            } else if(iterableType.isTupleType()) {
                auto iterableLengthPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env.i32Const(0), _env.i32Const(3)});
                builder.CreateStore(_env.i64Const(iterableType.parameters().size()), iterableLengthPtr);
            }

            // this is problematic for cross-compilation, need to set target layout BEFORE compiling.
            auto& DL = _env.getModule()->getDataLayout();
            return SerializableValue(iteratorContextStruct, _env.i64Const(DL.getTypeAllocSize(iteratorContextType)));
        }

        SerializableValue
        IIterator::initContext(tuplex::codegen::LambdaFunctionBuilder &lfb, const codegen::IRBuilder &builder,
                               const std::vector<SerializableValue> &iterables, const python::Type &iterableType,
                               const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            if(iterables.size() != 1) {
                throw std::runtime_error("iterator expects single argument");
            }

            return initContext(lfb, builder, iterables.front(), iterableType, iteratorInfo);
        }

        SerializableValue
        IIterator::initContext(tuplex::codegen::LambdaFunctionBuilder &lfb, const codegen::IRBuilder &builder,
                               const tuplex::codegen::SerializableValue &iterable, const python::Type &iterableType,
                               const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            throw std::runtime_error("init context with single argument not implemented for " + name());
        }

        SerializableValue
        IIterator::currentElement(const tuplex::codegen::IRBuilder &builder, const python::Type &iterableType,
                                  const python::Type& yieldType,
                                  llvm::Value* iterator, const std::shared_ptr<IteratorInfo>& iteratorInfo) {
            using namespace llvm;

            auto llvm_iterator_context_type = createIteratorContextTypeFromIteratorInfo(_env, *iteratorInfo.get()); //_env.createOrGetIterIteratorType(iterableType); //createIteratorContextTypeFromIteratorInfo(_env, *iteratorInfo.get());

            auto iterablesType = iteratorInfo->argsType;

            // get current element value and size of current value
            llvm::Value *retVal = nullptr, *retSize = nullptr;
            auto indexPtr = builder.CreateStructGEP(iterator, llvm_iterator_context_type, 1);
            auto llvm_index_type = iterableType == python::Type::RANGE ? _env.i64Type() : _env.i32Type();
            auto index = builder.CreateLoad(llvm_index_type, indexPtr); // <- index should be i32 or i64
            auto iterableAllocPtr = builder.CreateGEP(llvm_iterator_context_type, iterator, {_env.i32Const(0), _env.i32Const(2)});
            auto iterableAlloc = builder.CreateLoad(llvm_iterator_context_type->getStructElementType(2), iterableAllocPtr);
            if(iterablesType.isListType()) {
                auto llvm_list_type = _env.createOrGetListType(iterablesType);
                auto element_type = iterablesType.elementType();
                auto llvm_list_element_type = _env.pythonToLLVMType(element_type);
                auto valArrayPtr = builder.CreateGEP(llvm_list_type, iterableAlloc, {_env.i32Const(0), _env.i32Const(2)});
                auto valArray = builder.CreateLoad(llvm_list_type->getStructElementType(2), valArrayPtr);

                // special case: for tuple & list is the element type a pointer
                auto llvm_list_element_load_type = llvm_list_element_type;
                if((element_type.isTupleType() && !element_type.isFixedSizeType() && python::Type::EMPTYTUPLE != element_type) ||
                   (element_type.isListType() && python::Type::EMPTYLIST != element_type))
                    llvm_list_element_load_type = llvm_list_element_type->getPointerTo();

                auto currValPtr = builder.CreateGEP(llvm_list_element_load_type, valArray, index);
                retVal = builder.CreateLoad(llvm_list_element_load_type, currValPtr);
                if(yieldType == python::Type::I64 || yieldType == python::Type::F64 || yieldType == python::Type::BOOLEAN) {
                    // note: list internal representation currently uses 1 byte for bool (although this field is never used)
                    retSize = _env.i64Const(8);
                } else if(yieldType == python::Type::STRING || yieldType.isDictionaryType()) {
                    auto sizeArrayPtr = builder.CreateGEP(llvm_list_type, iterableAlloc, {_env.i32Const(0), _env.i32Const(3)});
                    auto sizeArray = builder.CreateLoad(_env.i64ptrType(), sizeArrayPtr);
                    auto currSizePtr = builder.CreateGEP(builder.getInt64Ty(), sizeArray, index);
                    retSize = builder.CreateLoad(builder.getInt64Ty(), currSizePtr);
                } else if(yieldType.isTupleType()) {
                    if(!yieldType.isFixedSizeType()) {
                        auto llvm_tuple_type = _env.getOrCreateTupleType(yieldType);
                        // retVal is a pointer to tuple struct
                        retVal = builder.CreateLoad(llvm_tuple_type, retVal);
                    }
                    auto ft = FlattenedTuple::fromLLVMStructVal(&_env, builder, retVal, yieldType);
                    retSize = ft.getSize(builder);
                }
            } else if(iterablesType == python::Type::STRING) {
                auto currCharPtr = builder.CreateGEP(_env.i8Type(), iterableAlloc, index);
                // allocate new string (1-byte character with a 1-byte null terminator)
                retSize = _env.i64Const(2);
                retVal = builder.CreatePointerCast(_env.malloc(builder, retSize), _env.i8ptrType());
                builder.CreateStore(builder.CreateLoad(builder.getInt8Ty(), currCharPtr), retVal);
                auto nullCharPtr = builder.CreateGEP(_env.i8Type(), retVal, _env.i32Const(1));
                builder.CreateStore(_env.i8Const(0), nullCharPtr);
            } else if(iterablesType == python::Type::RANGE) {
                retVal = index;
                retSize = _env.i64Const(8);
            } else if(iterablesType.isTupleType() && python::Type::EMPTYTUPLE != iterablesType) {
                // only works with homogenous tuple
                auto tupleLength = iterablesType.parameters().size();

                _env.printValue(builder, _env.i64Const(tupleLength), "data access into tuple with length=");

                // create array & index
                auto array = builder.CreateAlloca(_env.pythonToLLVMType(yieldType), 0, _env.i64Const(tupleLength));
                auto sizes = builder.CreateAlloca(_env.i64Type(), 0, _env.i64Const(tupleLength));

                // store the elements into the array
                std::vector<python::Type> tupleType(tupleLength, yieldType);
                FlattenedTuple flattenedTuple = FlattenedTuple::fromLLVMStructVal(&_env, builder, iterableAlloc, python::Type::makeTupleType(tupleType));

                std::vector<SerializableValue> elements;
                std::vector<llvm::Type *> elementTypes;
                for (int i = 0; i < tupleLength; ++i) {
                    auto load = flattenedTuple.getLoad(builder, {i});
                    elements.push_back(load);
                    elementTypes.push_back(load.val->getType());
                }

                auto element_type = iterablesType.parameters().front();
                auto llvm_element_type = _env.pythonToLLVMType(element_type);

                // fill in array elements
                for (int i = 0; i < tupleLength; ++i) {
                    builder.CreateStore(elements[i].val, builder.CreateGEP(llvm_element_type, array, _env.i32Const(i)));
                    builder.CreateStore(elements[i].size, builder.CreateGEP(builder.getInt64Ty(), sizes, _env.i32Const(i)));
                }

                _env.printValue(builder, index, "retrieving tuple element with index=");

                // special case: mutable element -> pass as pointer
                if(element_type.isImmutable()) {
                    // load from array
                    retVal = builder.CreateLoad(llvm_element_type, builder.CreateGEP(llvm_element_type, array, builder.CreateTrunc(index, _env.i32Type())));
                } else {
                    // pass the pointer
                    retVal = builder.CreateGEP(llvm_element_type, array, builder.CreateTrunc(index, _env.i32Type()));
                    assert(retVal->getType()->isPointerTy());
                }

                // load size from array
                retSize = builder.CreateLoad(builder.getInt64Ty(), builder.CreateGEP(builder.getInt64Ty(), sizes, builder.CreateTrunc(index, _env.i32Type())));
            } else {
                throw std::runtime_error("unsupported iterables type: " + iterablesType.desc());
            }

            // TODO: what about options?
            return SerializableValue(retVal, retSize);
        }

        SerializableValue
        SequenceIterator::nextElement(const codegen::IRBuilder &builder,
                                      const python::Type &yieldType,
                                      llvm::Value *iterator,
                                      const python::Type& iterableType,
                                      const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            // fetch element from current context state

            using namespace llvm;


            std::string funcName;
            auto iteratorName = iteratorInfo->iteratorName;
            auto iterablesType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            if(iterablesType.isIteratorType()) {
                // iter() call on an iterator, ignore the outer iter and call again
                assert(argsIteratorInfo.front());

                // dispatch here again
                return {};
                //return getIteratorNextElement(builder, yieldType, iterator, argsIteratorInfo.front());
            }

            return currentElement(builder, iterablesType, yieldType, iterator, iteratorInfo);
        }

        llvm::Value *SequenceIterator::updateIndex(const codegen::IRBuilder &builder,
                                                   llvm::Value *iterator,
                                                   const python::Type& iterableType,
                                                   const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            assert(iteratorInfo);
            auto iteratorName = iteratorInfo->iteratorName;
            auto iterablesType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            if(iterablesType.isIteratorType()) {
                // iter() call on an iterator, ignore the outer iter and call again
                assert(argsIteratorInfo.front());

                // do dispatch here to whichever type of iterator it is...
                return update_iterator_index(_env, builder, iterator, argsIteratorInfo.front());
            }

            std::string funcName;
            std::string prefix;
            auto iterable_name = _env.iterator_name_from_type(iterablesType);
            if(iterable_name.empty()) {
                throw std::runtime_error("Iterator struct for " + iterablesType.desc()
                                         + " does not have the corresponding LLVM UpdateIteratorIndex function");
            } else if(iterablesType == python::Type::RANGE) {
                // special case range -> it's one structure (for all!)
                funcName = "range_iterator_update";
            } else {
                if(!strEndsWith(iterable_name, "_"))
                    iterable_name += "_";
                funcName = iterable_name + prefix + "iterator_update";
            }

            auto llvm_iterator_context_type = _env.createOrGetIterIteratorType(iterableType);//createIteratorContextTypeFromIteratorInfo(_env, *iteratorInfo.get());

            // function type: i1(*struct.iterator)
            FunctionType *ft = llvm::FunctionType::get(llvm::Type::getInt1Ty(_env.getContext()),
                                                       {llvm::PointerType::get(llvm_iterator_context_type, 0)}, false);

            auto& logger = Logger::instance().logger("codegen");
            logger.debug("iterator context type: " + _env.getLLVMTypeName(llvm_iterator_context_type));
            logger.debug("ft type: " + _env.getLLVMTypeName(ft));
            logger.debug("iterator type: " + _env.getLLVMTypeName(iterator->getType()));

            // ok, update is something crazy fancy here: mod.getOrInsertFunction(name, FT).getCallee()->getType()->getPointerElementType()->isFunctionTy()

            auto nextFunc_value = llvm::getOrInsertCallable(*_env.getModule(), funcName, ft);
            llvm::FunctionCallee nextFunc_callee(ft, nextFunc_value);
            auto exhausted = builder.CreateCall(nextFunc_callee, iterator);

            assert(exhausted);
            return exhausted;
        }

        std::string SequenceIterator::name() const {
            return "";
        }

        SerializableValue
        ReversedIterator::initContext(tuplex::codegen::LambdaFunctionBuilder &lfb, const codegen::IRBuilder &builder,
                                      const tuplex::codegen::SerializableValue &iterable,
                                      const python::Type &iterableType,
                                      const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            return {};
        }

        SerializableValue
        ReversedIterator::nextElement(const codegen::IRBuilder &builder, const python::Type &yieldType,
                                      llvm::Value *iterator, const python::Type &iterableType,
                                      const std::shared_ptr<IteratorInfo> &iteratorInfo) {

            llvm::Type *iteratorContextType = iterator->getType()->getPointerElementType();
            std::string funcName;
            auto iteratorName = iteratorInfo->iteratorName;
            auto iterablesType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            assert(iteratorName == "reversed");

            // simply fetch element at index
            return currentElement(builder, iterableType, yieldType, iterator, iteratorInfo);
        }

        llvm::Value *ReversedIterator::updateIndex(const codegen::IRBuilder &builder, llvm::Value *iterator,
                                                   const python::Type &iterableType,
                                                   const std::shared_ptr<IteratorInfo> &iteratorInfo) {

            using namespace llvm;
            llvm::Type *iteratorContextType = iterator->getType()->getPointerElementType();
            std::string funcName;
            auto iteratorName = iteratorInfo->iteratorName;

            auto iterablesType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;
            std::string prefix;

            if(iteratorName == "reversed") {
                prefix = "reverse_";
            }

            auto iterable_name = _env.iterator_name_from_type(iterablesType);
            if(iterable_name.empty()) {
                throw std::runtime_error("Iterator struct " + _env.getLLVMTypeName(iteratorContextType)
                                         + " does not have the corresponding LLVM UpdateIteratorIndex function");
            } else if(iterablesType == python::Type::RANGE) {
                // special case range -> it's one structure (for all!)
                funcName = "range_iterator_update";
            } else {
                if(!strEndsWith(iterable_name, "_"))
                    iterable_name += "_";
                funcName = iterable_name + prefix + "iterator_update";
            }

            // function type: i1(*struct.iterator)
            FunctionType *ft = llvm::FunctionType::get(llvm::Type::getInt1Ty(_env.getContext()),
                                                       {llvm::PointerType::get(iteratorContextType, 0)}, false);

            auto& logger = Logger::instance().logger("codegen");
            logger.debug("iterator context type: " + _env.getLLVMTypeName(iteratorContextType));
            logger.debug("ft type: " + _env.getLLVMTypeName(ft));
            logger.debug("iterator type: " + _env.getLLVMTypeName(iterator->getType()));

            // ok, update is something crazy fancy here: mod.getOrInsertFunction(name, FT).getCallee()->getType()->getPointerElementType()->isFunctionTy()

            auto nextFunc_value = llvm::getOrInsertCallable(*_env.getModule(), funcName, ft);
            llvm::FunctionCallee nextFunc_callee(ft, nextFunc_value);
            auto exhausted = builder.CreateCall(nextFunc_callee, iterator);
            assert(exhausted);
            return exhausted;
        }

        std::string ReversedIterator::name() const {
            return "";
        }

        SerializableValue
        ZipIterator::initContext(tuplex::codegen::LambdaFunctionBuilder &lfb, const codegen::IRBuilder &builder,
                                 const std::vector<tuplex::codegen::SerializableValue> &iterables, const python::Type &iterableType,
                                 const std::shared_ptr<IteratorInfo> &iteratorInfo) {

            using namespace llvm;

            if(iterables.empty()) {
                // use dummy value for empty iterator
                return SerializableValue(_env.i64Const(0), _env.i64Const(8));
            }

            auto iterablesType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;
            auto iteratorContextType = createIteratorContextTypeFromIteratorInfo(_env, *iteratorInfo);

            if(iteratorContextType == _env.i64Type()) {
                // empty iterator
                return SerializableValue(_env.i64Const(0), _env.i64Const(8));
            }
            auto iteratorContextStruct = _env.CreateFirstBlockAlloca(builder, iteratorContextType, "zip_iterator_alloc");
            // store pointers to iterator structs
            for (size_t i = 0; i < iterablesType.parameters().size(); ++i) {
                auto currType = iterablesType.parameters()[i];
                assert(currType.isIterableType());
                auto iterablePtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env.i32Const(0), _env.i32Const(i)});
                llvm::Value *iteratorVal;
                if(currType.isIteratorType()) {
                    iteratorVal = iterables[i].val;
                } else {
                    if(!(currType.isListType() || currType.isTupleType() || currType == python::Type::RANGE || currType == python::Type::STRING)) {
                        throw std::runtime_error("unsupported iterable type " + currType.desc());
                    }

                    // use default dispatch method for iter
                    SequenceIterator it(_env);
                    iteratorVal = it.initContext(lfb, builder, iterables[i], currType, nullptr).val;

                    // old:
                    // iteratorVal = initIterContext(lfb, builder, currType, iterables[i]).val;
                }
                builder.CreateStore(iteratorVal, iterablePtr);
            }

            auto* dl = new DataLayout(_env.getModule().get());
            return SerializableValue(iteratorContextStruct, _env.i64Const(dl->getTypeAllocSize(iteratorContextType)));
        }

        llvm::Value *ZipIterator::updateIndex(const codegen::IRBuilder &builder, llvm::Value *iterator,
                                              const python::Type &iterableType,
                                              const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            auto& ctx = _env.getContext();

            auto argsType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            int zipSize = argsType.parameters().size();
            if(zipSize == 0) {
                return _env.i1Const(true);
            }

            BasicBlock *currBB = builder.GetInsertBlock();
            BasicBlock *exhaustedBB = BasicBlock::Create(ctx, "exhaustedBB", currBB->getParent());
            BasicBlock *endBB = BasicBlock::Create(ctx, "endBB", currBB->getParent());

            builder.SetInsertPoint(exhaustedBB);
            builder.CreateBr(endBB);

            builder.SetInsertPoint(endBB);
            // zipExhausted indicates whether the given zip iterator is exhausted
            auto zipExhausted = builder.CreatePHI(_env.i1Type(), 2);
            zipExhausted->addIncoming(_env.i1Const(true), exhaustedBB);

            std::vector<BasicBlock *> zipElementEntryBB;
            std::vector<BasicBlock *> zipElementCondBB;
            for (int i = 0; i < zipSize; ++i) {
                BasicBlock *currElementEntryBB = BasicBlock::Create(_env.getContext(), "zipElementBB" + std::to_string(i), currBB->getParent());
                BasicBlock *currElementCondBB = BasicBlock::Create(_env.getContext(), "currCondBB" + std::to_string(i), currBB->getParent());
                zipElementEntryBB.push_back(currElementEntryBB);
                zipElementCondBB.push_back(currElementCondBB);
            }
            zipExhausted->addIncoming(_env.i1Const(false), zipElementCondBB[zipSize - 1]);

            builder.SetInsertPoint(currBB);
            builder.CreateBr(zipElementEntryBB[0]);
            // iterate over all arg iterators
            // if the current arg iterator is exhausted, jump directly to exhaustedBB and zipExhausted will be set to true
            for (int i = 0; i < zipSize; ++i) {

                assert(iteratorInfo);
                assert(i < iteratorInfo->argsIteratorInfo.size());
                assert(iteratorInfo->argsIteratorInfo[i]);

                auto curr_iterator_type = argsType.parameters()[i];
                auto llvm_curr_iterator_type = createIteratorContextTypeFromIteratorInfo(_env, *iteratorInfo->argsIteratorInfo[i].get());

                builder.SetInsertPoint(zipElementEntryBB[i]);
                auto currIteratorPtr = builder.CreateGEP(iterator, {_env.i32Const(0), _env.i32Const(i)});
                auto currIterator = builder.CreateLoad(llvm_curr_iterator_type->getPointerTo(), currIteratorPtr);
                auto currIteratorInfo = argsIteratorInfo[i];
                assert(currIteratorInfo);

                // new:
                auto exhausted = update_iterator_index(_env, builder, currIterator, currIteratorInfo);

                // // old:
                // auto exhausted = updateIteratorIndex(builder, currIterator, currIteratorInfo);

                builder.CreateBr(zipElementCondBB[i]);
                builder.SetInsertPoint(zipElementCondBB[i]);
                if(i == zipSize - 1) {
                    builder.CreateCondBr(exhausted, exhaustedBB, endBB);
                } else {
                    builder.CreateCondBr(exhausted, exhaustedBB, zipElementEntryBB[i+1]);
                }
            }
            builder.SetInsertPoint(endBB);
            assert(zipExhausted);
            return zipExhausted;
        }

        SerializableValue ZipIterator::nextElement(const codegen::IRBuilder &builder, const python::Type &yieldType,
                                                   llvm::Value *iterator, const python::Type &iterableType,
                                                   const std::shared_ptr<IteratorInfo> &iteratorInfo) {

            using namespace llvm;
            auto argsType = iteratorInfo->argsType;
            auto argsIteratorInfo = iteratorInfo->argsIteratorInfo;

            FlattenedTuple ft(&_env);
            ft.init(yieldType);

            auto llvm_iterator_type = createIteratorContextTypeFromIteratorInfo(_env, *iteratorInfo);

            // previously UpdateIteratorIndexFunction was called on each arg iterator which increments index of each arg iterator by 1
            // restore index for all arg iterators
            increment_iterator_index(_env, builder, iterator, iteratorInfo, -1);
            for (int i = 0; i < argsType.parameters().size(); ++i) {
                auto currIteratorInfo = argsIteratorInfo[i];
                auto llvm_curr_iterator_type = createIteratorContextTypeFromIteratorInfo(_env, *currIteratorInfo.get());
                auto currIteratorPtr = builder.CreateStructGEP(iterator, llvm_iterator_type, i); //{_env.i32Const(0), _env.i32Const(i)});
                auto currIterator = builder.CreateLoad(llvm_curr_iterator_type->getPointerTo(), currIteratorPtr);

                // update current arg iterator index before fetching value
                increment_iterator_index(_env, builder, currIterator, currIteratorInfo, 1);

                auto currIteratorNextVal = next_from_iterator(_env, builder, yieldType.parameters()[i], currIterator, currIteratorInfo);
                ft.setElement(builder, i, currIteratorNextVal.val, currIteratorNextVal.size, currIteratorNextVal.is_null);
            }
            auto retVal = ft.getLoad(builder);
            auto retSize = ft.getSize(builder);
            return SerializableValue(retVal, retSize);
        }

        std::string ZipIterator::name() const {
            return "";
        }

        SerializableValue
        EnumerateIterator::initContext(tuplex::codegen::LambdaFunctionBuilder &lfb, const codegen::IRBuilder &builder,
                                       const std::vector<SerializableValue> &iterables,
                                       const python::Type &iterablesType,
                                       const std::shared_ptr<IteratorInfo> &iteratorInfo) {

            using namespace llvm;

            auto num_params = iterablesType.parameters().size();
            if(num_params < 1 || num_params > 2)
                throw std::runtime_error("invalid arguments for enumerate call, takes 1 or 2 parameters. Given: " + iterablesType.desc());

            assert(iterables.size() == num_params);

            // start value depends on params. If two are given, use second arg. else, default val is 0
            llvm::Value* startVal = num_params == 2 ? iterables[1].val : _env.i64Const(0);
            assert(startVal->getType() == _env.i64Type());

            // what to actually iterate on
            auto iterable = iterables.front(); // what to iterate over

            assert(iterablesType.isTupleType());
            auto iterable_type = iterablesType.parameters().front();


            if(iterable_type == python::Type::EMPTYITERATOR
            || iterable_type == python::Type::EMPTYLIST
            || iterable_type == python::Type::EMPTYTUPLE) {
                // empty iterator
                return SerializableValue(_env.i64Const(0), _env.i64Const(8));
            }
            if(!(iterable_type.isIteratorType() || iterable_type.isListType()
            || iterable_type.isTupleType() || iterable_type == python::Type::RANGE || iterable_type == python::Type::STRING)) {
                throw std::runtime_error("unsupported iterable type " + iterable_type.desc() + " for enumerate");
            }

            auto iteratorContextType = createIteratorContextTypeFromIteratorInfo(_env, *iteratorInfo);

            auto iteratorContextStruct = _env.CreateFirstBlockAlloca(builder, iteratorContextType, "enumerate_iterator_alloc");
            auto startValPtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env.i32Const(0), _env.i32Const(0)});
            builder.CreateStore(startVal, startValPtr);
            auto iterablePtr = builder.CreateGEP(iteratorContextType, iteratorContextStruct, {_env.i32Const(0), _env.i32Const(1)});
            llvm::Value *iteratorVal = nullptr;
            if(iterable_type.isIteratorType()) {
                iteratorVal = iterable.val;
            } else {
                // get sequence iterator context for given iterable
                SequenceIterator it(_env);
                auto info = iteratorInfo ? iteratorInfo->argsIteratorInfo.front() : nullptr; // <-- is there another iterator in there?
                auto iterator = it.initContext(lfb, builder, iterable, iterable_type, info);
                iteratorVal = iterator.val;
            }
            assert(iteratorVal);
            // store iterator context (the pointer)
            builder.CreateStore(iteratorVal, iterablePtr);

            auto* dl = new DataLayout(_env.getModule().get());
            return SerializableValue(iteratorContextStruct, _env.i64Const(dl->getTypeAllocSize(iteratorContextType)));
        }

        llvm::Value *EnumerateIterator::updateIndex(const codegen::IRBuilder &builder, llvm::Value *iterator,
                                                    const python::Type &iterableType,
                                                    const std::shared_ptr<IteratorInfo> &iteratorInfo) {
            using namespace llvm;

            auto argIteratorInfo = iteratorInfo->argsIteratorInfo.front();

            // get llvm type of iterator being pointed to
            auto llvm_inner_iterator_type = createIteratorContextTypeFromIteratorInfo(_env, *argIteratorInfo);
            auto llvm_iterator_type = createIteratorContextTypeFromIteratorInfo(_env, *iteratorInfo);

            auto argIteratorPtr = builder.CreateStructGEP(iterator, llvm_iterator_type, 1);
            auto argIterator = builder.CreateLoad(llvm_inner_iterator_type->getPointerTo(), argIteratorPtr);

            // old:
            //auto argIteratorPtr = builder.CreateGEP(iterator, {_env.i32Const(0), _env.i32Const(1)});
            //auto argIterator = builder.CreateLoad(argIteratorPtr);

            // inner iterator needs to get updated
            auto enumerateExhausted = update_iterator_index(_env, builder, argIterator, argIteratorInfo);
            assert(enumerateExhausted);
            return enumerateExhausted;
        }

        SerializableValue
        EnumerateIterator::nextElement(const codegen::IRBuilder &builder, const python::Type &yieldType,
                                       llvm::Value *iterator, const python::Type &iterableType,
                                       const std::shared_ptr<IteratorInfo> &iteratorInfo) {

            // enumerate returns a tuple
            using namespace llvm;

            auto argIteratorInfo = iteratorInfo->argsIteratorInfo.front();

            // get llvm type of iterator being pointed to
            auto llvm_inner_iterator_type = createIteratorContextTypeFromIteratorInfo(_env, *argIteratorInfo);
            auto llvm_iterator_type = createIteratorContextTypeFromIteratorInfo(_env, *iteratorInfo);

            FlattenedTuple ft(&_env);
            ft.init(yieldType);
            auto startValPtr = builder.CreateStructGEP(iterator, llvm_iterator_type, 0);
            auto startVal = builder.CreateLoad(builder.getInt64Ty(), startValPtr);
            auto start = SerializableValue(startVal, _env.i64Const(8));
            auto argIteratorPtr = builder.CreateStructGEP(iterator, llvm_iterator_type, 1);
            auto argIterator = builder.CreateLoad(llvm_inner_iterator_type->getPointerTo(), argIteratorPtr);

            // fetch next element from underlying iterator
            auto val = next_from_iterator(_env, builder, yieldType.parameters()[1], argIterator, argIteratorInfo);

            ft.setElement(builder, 0, start.val, start.size, start.is_null);
            ft.setElement(builder, 1, val.val, val.size, val.is_null);
            auto retVal = ft.getLoad(builder);
            auto retSize = ft.getSize(builder);

            // increment start index value
            auto newStartVal = builder.CreateAdd(startVal, _env.i64Const(1));
            builder.CreateStore(newStartVal, startValPtr);

            return SerializableValue(retVal, retSize);
        }

    }
}