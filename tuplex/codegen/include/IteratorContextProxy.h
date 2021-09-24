//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ITERATORCONTEXTPROXY_H
#define TUPLEX_ITERATORCONTEXTPROXY_H

#include <llvm/IR/IRBuilder.h>
#include <LambdaFunction.h>
#include <LLVMEnvironment.h>
#include <ASTAnnotation.h>

namespace tuplex {
    namespace codegen{
        /*!
         * helper class for iterator-related calls
         */
        class IteratorContextProxy {
        private:
            LLVMEnvironment *_env;

        public:
            explicit IteratorContextProxy(LLVMEnvironment *env) : _env(env) {}

            /*!
             * Initialize list/string/tuple/range iterator context for iter() call.
             * @param lfb
             * @param builder
             * @param iterableType
             * @param iterable
             * @return SerializableValue with val being a pointer to llvm struct representing the list/string/tuple iterator context
             */
            SerializableValue initIterContext(LambdaFunctionBuilder &lfb,
                                              llvm::IRBuilder<> &builder,
                                              const python::Type &iterableType,
                                              const SerializableValue &iterable);


            /*!
             * Initialize list/string/tuple/range iterator context for reversed() call.
             * @param lfb
             * @param builder
             * @param argType
             * @param arg
             * @return SerializableValue with val being a pointer to llvm struct representing the list/string/tuple iterator context
             */
            SerializableValue initReversedContext(LambdaFunctionBuilder &lfb,
                                              llvm::IRBuilder<> &builder,
                                              const python::Type &argType,
                                              const SerializableValue &arg);

            /*!
             * Initialize iterator context for zip() call.
             * @param lfb
             * @param builder
             * @param iterables
             * @param iteratorInfo
             * @return val: pointer to llvm struct representing the zip iterator context
             */
            SerializableValue initZipContext(LambdaFunctionBuilder& lfb,
                                             llvm::IRBuilder<> &builder,
                                             const std::vector<SerializableValue> &iterables,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Initialize iterator context for enumerate() call.
             * @param lfb
             * @param builder
             * @param iterable
             * @param startVal
             * @param iteratorInfo
             * @return val: pointer to llvm struct representing the enumerate iterator context
             */
            SerializableValue initEnumerateContext(LambdaFunctionBuilder& lfb,
                                                   llvm::IRBuilder<> &builder,
                                                   const SerializableValue &iterable,
                                                   llvm::Value *startVal,
                                                   const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Create next() call on an iterator by calling updateIteratorIndex and then getIteratorNextElement
             * if the iterator is not exhausted.
             * @param lfb
             * @param builder
             * @param yieldType
             * @param iterator
             * @param defaultArg
             * @param iteratorInfo
             * @return next element generated from the iterator, or default value if iterator is exhausted and a default value is provided
             */
            SerializableValue createIteratorNextCall(LambdaFunctionBuilder &lfb,
                                                     llvm::IRBuilder<> &builder,
                                                     const python::Type &yieldType,
                                                     llvm::Value *iterator,
                                                     const SerializableValue &defaultArg,
                                                     const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Update index for a general iterator in preparing for the getIteratorNextElement call.
             * @param builder
             * @param iterator
             * @param iteratorInfo
             * @return true if iterator is exhausted (getIteratorNextElement should not get called later), otherwise false
             */
            llvm::Value *updateIteratorIndex(llvm::IRBuilder<> &builder,
                                             llvm::Value *iterator,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Generate the next element of a general iterator.
             * Only to be called after calling updateIteratorIndex.
             * @param builder
             * @param yieldType
             * @param iterator
             * @param iteratorInfo
             * @return element of yieldType
             */
            SerializableValue getIteratorNextElement(llvm::IRBuilder<> &builder,
                                                     const python::Type &yieldType,
                                                     llvm::Value *iterator,
                                                     const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Update index for a zip iterator in preparing for the getIteratorNextElement call by calling updateIteratorIndex on each argument.
             * If any argument is exhausted, return true and stop calling updateIteratorIndex on rest of the arguments.
             * Only return false if none of the argument iterators is exhausted.
             * @param builder
             * @param iterator
             * @param iteratorInfo
             * @return true if iterator is exhausted (getIteratorNextElement should not get called later), otherwise false
             */
            llvm::Value *updateZipIndex(llvm::IRBuilder<> &builder,
                                        llvm::Value *iterator,
                                        const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Generate the next element of a zip iterator.
             * Only to be called after calling updateIteratorIndex.
             * @param builder
             * @param yieldType
             * @param iterator
             * @param iteratorInfo
             * @return tuple element of yieldType
             */
            SerializableValue getZipNextElement(llvm::IRBuilder<> &builder,
                                                const python::Type &yieldType,
                                                llvm::Value *iterator,
                                                const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Generate the next element of a enumerate iterator.
             * Only to be called after calling updateIteratorIndex.
             * @param builder
             * @param iterator
             * @param iteratorInfo
             * @return true if iterator is exhausted (getIteratorNextElement should not get called later), otherwise false
             */
            llvm::Value *updateEnumerateIndex(llvm::IRBuilder<> &builder,
                                              llvm::Value *iterator,
                                              const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Generate the next element of a enumerate iterator.
             * Only to be called after calling updateIteratorIndex.
             * @param builder
             * @param yieldType
             * @param iterator
             * @param iteratorInfo
             * @return tuple element of yieldType
             */
            SerializableValue getEnumerateNextElement(llvm::IRBuilder<> &builder,
                                                      const python::Type &yieldType,
                                                      llvm::Value *iterator,
                                                      const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Increment index field of a list/string/tuple iterator by offset.
             * Increment index field of a range iterator by step * offset.
             * Decrement index field of any reverseiterator by offset.
             * For zip and enumerate, will use recursive calls on their arguments until a list/string/tuple iterator or a reverseiterator is reached.
             * @param builder
             * @param iterator
             * @param iteratorInfo
             * @param offset can be negative
             */
            void incrementIteratorIndex(llvm::IRBuilder<> &builder, llvm::Value *iterator, const std::shared_ptr<IteratorInfo> &iteratorInfo, int offset);
        };
    }
}

#endif //TUPLEX_ITERATORCONTEXTPROXY_H