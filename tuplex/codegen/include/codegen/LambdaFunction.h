//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LAMBDAFUNCTION_H
#define TUPLEX_LAMBDAFUNCTION_H

#include <codegen/FlattenedTuple.h>
#include <codegen/CodegenHelper.h>
#include <ast/ASTNodes.h>
#include <ExceptionCodes.h>

namespace tuplex {
    namespace codegen {

        class LambdaFunctionBuilder;
        class LambdaFunction;

        class LambdaFunction {
        private:
            std::string     _name;
            python::Type    _pyArgType;
            python::Type    _pyRetType;
            llvm::Function  *_func;
            LLVMEnvironment *_env;

        public:
            LambdaFunction() : _func(nullptr), _env(nullptr) {}

            /*!
             * LLVM struct type of the returned tuple of this function
             * @return LLVM struct type
             */
            llvm::Type* getResultType();

            /*!
             * retrieves LambdaFunction object from environment
             * @param env environment from where to retrieve lambda function
             * @param funcName name under which lambda function is registered.
             * @return LambdaFunction object that can generate call code
             */
            static LambdaFunction getFromEnvironment(LLVMEnvironment& env, const std::string& funcName);

            /*!
             * calls the function. Changes builder to have insertion point where normal code continues to execute.
             * @param builder Builder object of basic block where function is called from
             * @param resVal pointer to the struct where to store the result of the function. Needs to be allocated outside of a loop body, else LLVM will segfault.
             * @param handler BasicBlock to enter when an exception was encountered
             * @param exceptionCode where to store the exception data
             * @param args (flattened) arguments needed by the function (includes sizes)
             */
            void callWithExceptionHandler(llvm::IRBuilder<>& builder,
                                          llvm::Value* const resVal,
                                          llvm::BasicBlock* const handler,
                                          llvm::Value* const exceptionCode,
                                          const std::vector<llvm::Value*>& args);

            friend class LambdaFunctionBuilder;
        };

        class LambdaFunctionBuilder {
        private:

            MessageHandler &_logger;
            llvm::LLVMContext &_context;
            LLVMEnvironment *_env;

            LambdaFunction _func;
            FlattenedTuple _fti;
            FlattenedTuple _fto;
            llvm::BasicBlock *_body; // last active block
            llvm::Value *_retValPtr;

            std::map <std::string, SerializableValue> _paramLookup;

            // helper functions
            llvm::Type *retType() { return _fto.getLLVMType(); }

            std::vector<llvm::Type *> parameterTypes();

            /*!
             * helper function to fill _paramLookup with llvm::Values
             */
            void unflattenParameters(llvm::IRBuilder<>& builder, NParameterList* params, bool isFirstArgTuple);

            inline llvm::Value *i1Const(const bool value) {
                return llvm::Constant::getIntegerValue(llvm::Type::getInt1Ty(_context), llvm::APInt(1, value));
            }

            void createLLVMFunction(const std::string& name, const python::Type& argType, bool isFirstArgTuple, NParameterList *parameters, const python::Type& retType);

        public:
            LambdaFunctionBuilder(MessageHandler &logger, LLVMEnvironment *env) : _logger(logger), _env(env),
                                                                                  _context(env->getContext()),
                                                                                  _body(nullptr), _fto(env), _fti(env),
                                                                                  _retValPtr(nullptr) {}

            LambdaFunctionBuilder& create(NLambda *lambda, std::string func_name);
            LambdaFunctionBuilder& create(NFunction* func);

            llvm::IRBuilder<> getLLVMBuilder() { assert(_body); return llvm::IRBuilder<>(_body); }

            llvm::IRBuilder<> addException(llvm::IRBuilder<>& builder, ExceptionCode ec, llvm::Value *condition);
            llvm::IRBuilder<> addException(llvm::IRBuilder<>& builder, llvm::Value* ecCode, llvm::Value *condition);

            /*!
             * the original python return type of the function.
             * @return python::Type of the original function. NOT THE ROW TYPE.
             */
            python::Type pythonRetType() const { return _func._pyRetType; }

            /*!
             * add a return statement
             * @param retValue the serializablevalue
             * @return itself
             */
            LambdaFunction addReturn(const SerializableValue &retValue);

            /*!
             * generates an internal normalcase violation and terminates basic block. After this, getLastBlock will be nullptr.
             * @return the function itself.
             */
            inline LambdaFunction exitNormalCase() { return exitWithException(ExceptionCode::NORMALCASEVIOLATION); }

            /*!
             * helper to stop generation in expressions
             * @return
             */
            inline bool hasExited() const { return _body == nullptr; }

            /*!
             * finishes current basicblock and exits it with exception.
             * @param ec which code to exit with
             * @return the function itself
             */
            LambdaFunction exitWithException(const ExceptionCode& ec);

            inline llvm::IRBuilder<> setLastBlock(llvm::BasicBlock* bb) {
                assert(bb);
                _body = bb;
               return getLLVMBuilder();
            }

            inline llvm::BasicBlock* getLastBlock() const { return _body; }

            inline SerializableValue getParameter(const std::string &name) {
                assert(_paramLookup.size() > 0);
                auto it = _paramLookup.find(name);
                if(_paramLookup.end() == it)
                    return SerializableValue(nullptr, nullptr);
                else
                    return it->second;
            }

            /*!
             * returns vector of string with all parameter names (order as in function signature)
             * @return vector of parameter names as string
             */
            inline std::vector<std::string> parameterNames() const {
                std::vector<std::string> names;
                for(auto keyval : _paramLookup) {
                    names.emplace_back(keyval.first);
                }
                return names;
            }

            std::string funcName() const {
                assert(_func._func);
                return _func._func->getName();
            }
        };

    }
}

#endif //TUPLEX_LAMBDAFUNCTION_H