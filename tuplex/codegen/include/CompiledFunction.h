//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_COMPILEDFUNCTION_H
#define TUPLEX_COMPILEDFUNCTION_H

#include <CodegenHelper.h>
#include <FlattenedTuple.h>

namespace tuplex {
    namespace codegen {
        //! helper struct to store essential information of a compiled function
        class CompiledFunction {
        public:
            llvm::Value *function_ptr; //! stored optionally for fallback mechanisms the ptr
            llvm::Function *function; //! the function to be called
            python::Type input_type;
            python::Type output_type;

            CompiledFunction() : function_ptr(nullptr), function(nullptr) {}

            CompiledFunction(const CompiledFunction &other) :
                             function_ptr(other.function_ptr),
                             function(other.function),
                             input_type(other.input_type),
                             output_type(other.output_type) {}

            CompiledFunction &operator=(const CompiledFunction &other) {
                function_ptr = other.function_ptr;
                function = other.function;
                input_type = other.input_type;
                output_type = other.output_type;
                return *this;
            }


            /*!
             * llvm function type
             * @return llvm function type of stored function or nullptr
             */
            llvm::Type *getFunctionType() const { return function ? function->getType() : nullptr; }

            /*!
             * checks whether this class holds a valid function without errors
             * @return
             */
            bool good() const { return function; }

            /*!
             * is the function stored an actual compiled one or using a fallback?
             * @return true when fallback solution is used.
             */
            bool usesFallback() const { return function_ptr; }

            /*!
             * name of the function
             * @return name of the function
             */
            std::string name() const {
                assert(function);
                return function->getName().str();
            }


            /*!
             * calls the stored function (either compiled or fallback version)
             * @param builder builder corresponding to block where function is called.
             * Note that the builder will become updated to the block when the call succeeded
             * @param args the input arguments to the function
             * @param handler BasicBlock where to branch out in the case of an exception
             * @param exceptionCode i64* value where to store the exception.
             * @param failureBlock block where to go when alloc fails
             * @return the output of the exception (valid in normal block)
             */
            FlattenedTuple callWithExceptionHandler(llvm::IRBuilder<> &builder,
                                                    const FlattenedTuple &args,
                                                    llvm::Value *const resPtr,
                                                    llvm::BasicBlock *const handler,
                                                    llvm::Value *const exceptionCode,
                                                    llvm::BasicBlock *const failureBlock);


            FlattenedTuple callWithExceptionHandler(llvm::IRBuilder<> &builder,
                                                    const FlattenedTuple &args,
                                                    llvm::Value *const resPtr,
                                                    llvm::BasicBlock *const handler,
                                                    llvm::Value *const exceptionCode);

            /*!
             * LLVM struct type of the returned tuple of this function
             * @return LLVM struct type
             */
            inline llvm::Type *getLLVMResultType(LLVMEnvironment &env) const {
                FlattenedTuple ft(&env);
                ft.init(output_type);
                return ft.getLLVMType();
            }

            void setPythonInvokeName(const std::string &name) { _pythonInvokeName = name; }

        private:
            std::string _pythonInvokeName;
        };
    }
}
#endif //TUPLEX_COMPILEDFUNCTION_H