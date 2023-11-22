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
#include <CodegenHelper.h>

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
                                              const codegen::IRBuilder &builder,
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
                                              const codegen::IRBuilder& builder,
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
                                             const codegen::IRBuilder& builder,
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
                                                   const codegen::IRBuilder& builder,
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
                                                     const codegen::IRBuilder& builder,
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
            llvm::Value *updateIteratorIndex(const codegen::IRBuilder& builder,
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
            SerializableValue getIteratorNextElement(const codegen::IRBuilder& builder,
                                                     const python::Type &yieldType,
                                                     llvm::Value *iterator,
                                                     const std::shared_ptr<IteratorInfo> &iteratorInfo);

        };

        /*!
         * create iteratorcontext info type depending on iteratorInfo.
         * @param env
         * @param iteratorInfo
         * @return corresponding llvm::Type
         */
        extern llvm::Type* createIteratorContextTypeFromIteratorInfo(LLVMEnvironment& env, const IteratorInfo& iteratorInfo);

        extern void increment_iterator_index(LLVMEnvironment& env, const codegen::IRBuilder& builder,
                                             llvm::Value *iterator,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo,
                                             int32_t offset);
    }

    namespace codegen {
        // interface to generate various iterators
        class IIterator {
        public:
            IIterator(LLVMEnvironment& env) : _env(env) {}

            virtual SerializableValue initContext(LambdaFunctionBuilder &lfb,
                                                  const codegen::IRBuilder &builder,
                                                  const SerializableValue& iterable,
                                                  const python::Type &iterableType,
                                                  const std::shared_ptr<IteratorInfo> &iteratorInfo);

            // some iterators (e.g., zip) may have multiple arguments. Hence, allow for that as well using default single-arg function
            virtual SerializableValue initContext(LambdaFunctionBuilder &lfb,
                                                  const codegen::IRBuilder &builder,
                                                  const std::vector<SerializableValue>& iterables,
                                                  const python::Type &iterableType,
                                                  const std::shared_ptr<IteratorInfo> &iteratorInfo);

            virtual llvm::Value* updateIndex(const codegen::IRBuilder& builder,
                                     llvm::Value *iterator,
                                     const python::Type& iterableType,
                                     const std::shared_ptr<IteratorInfo> &iteratorInfo) = 0;

            virtual SerializableValue nextElement(const codegen::IRBuilder& builder,
                                          const python::Type &yieldType,
                                          llvm::Value *iterator,
                                          const python::Type& iterableType,
                                          const std::shared_ptr<IteratorInfo> &iteratorInfo) = 0;

            virtual std::string name() const = 0;
        protected:
            LLVMEnvironment& _env;

            virtual SerializableValue currentElement(const IRBuilder& builder,
                                                     const python::Type& iterableType,
                                                     const python::Type& yieldType,
                                                     llvm::Value* iterator,
                                                     const std::shared_ptr<IteratorInfo>& iteratorInfo);
        };

        // code generation for iter(...)
        class SequenceIterator : public IIterator {
        public:
            SequenceIterator(LLVMEnvironment& env) : IIterator(env) {}

            SerializableValue initContext(LambdaFunctionBuilder &lfb,
                                          const codegen::IRBuilder &builder,
                                          const SerializableValue& iterable,
                                          const python::Type &iterableType,
                                          const std::shared_ptr<IteratorInfo> &iteratorInfo) override;

            virtual llvm::Value* updateIndex(const codegen::IRBuilder& builder,
                                             llvm::Value *iterator,
                                             const python::Type& iterableType,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo) override;

            virtual SerializableValue nextElement(const codegen::IRBuilder& builder,
                                                  const python::Type &yieldType,
                                                  llvm::Value *iterator,
                                                  const python::Type& iterableType,
                                                  const std::shared_ptr<IteratorInfo> &iteratorInfo) override;


            std::string name() const override;

        };

        class EnumerateIterator : public SequenceIterator {
        public:
            EnumerateIterator(LLVMEnvironment& env) : SequenceIterator(env) {}

            // same init as sequence iterator, only difference is in retrieving the next element (tuple)
            SerializableValue initContext(tuplex::codegen::LambdaFunctionBuilder &lfb, const codegen::IRBuilder &builder, const std::vector<SerializableValue> &iterables, const python::Type &iterableType, const std::shared_ptr<IteratorInfo> &iteratorInfo) override;

            llvm::Value* updateIndex(const codegen::IRBuilder &builder, llvm::Value *iterator, const python::Type &iterableType, const std::shared_ptr<IteratorInfo> &iteratorInfo) override;

            SerializableValue nextElement(const codegen::IRBuilder &builder, const python::Type &yieldType, llvm::Value *iterator, const python::Type &iterableType, const std::shared_ptr<IteratorInfo> &iteratorInfo) override;
        };


        class ReversedIterator : public IIterator {
        public:
            ReversedIterator(LLVMEnvironment& env) : IIterator(env) {}

            SerializableValue initContext(LambdaFunctionBuilder &lfb,
                                          const codegen::IRBuilder &builder,
                                          const SerializableValue& iterable,
                                          const python::Type &iterableType,
                                          const std::shared_ptr<IteratorInfo> &iteratorInfo) override;

            virtual llvm::Value* updateIndex(const codegen::IRBuilder& builder,
                                             llvm::Value *iterator,
                                             const python::Type& iterableType,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo) override;

            virtual SerializableValue nextElement(const codegen::IRBuilder& builder,
                                                  const python::Type &yieldType,
                                                  llvm::Value *iterator,
                                                  const python::Type& iterableType,
                                                  const std::shared_ptr<IteratorInfo> &iteratorInfo) override;


            std::string name() const override;
        };

        class ZipIterator : public IIterator {
        public:
            explicit ZipIterator(LLVMEnvironment& env) : IIterator(env) {}

            SerializableValue initContext(LambdaFunctionBuilder &lfb,
                                          const codegen::IRBuilder &builder,
                                          const std::vector<SerializableValue>& iterables,
                                          const python::Type &iterableType,
                                          const std::shared_ptr<IteratorInfo> &iteratorInfo) override;

            virtual llvm::Value* updateIndex(const codegen::IRBuilder& builder,
                                             llvm::Value *iterator,
                                             const python::Type& iterableType,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo) override;

            virtual SerializableValue nextElement(const codegen::IRBuilder& builder,
                                                  const python::Type &yieldType,
                                                  llvm::Value *iterator,
                                                  const python::Type& iterableType,
                                                  const std::shared_ptr<IteratorInfo> &iteratorInfo) override;


            std::string name() const override;
        };
    }
}

#endif //TUPLEX_ITERATORCONTEXTPROXY_H