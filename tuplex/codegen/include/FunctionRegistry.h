//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FUNCTIONREGISTRY_H
#define TUPLEX_FUNCTIONREGISTRY_H

#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Target/TargetMachine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"

#include <deque>
#include <ApatheticVisitor.h>
#include <LLVMEnvironment.h>
#include <Token.h>
#include <LambdaFunction.h>
#include <unordered_map>
#include <IteratorContextProxy.h>

#include <Utils.h>

namespace tuplex {

    namespace codegen {

        /*!
         * class to handle Python builtin functions, typing etc.
         */
        class FunctionRegistry {
        public:
            FunctionRegistry(LLVMEnvironment& env, bool sharedObjectPropagation) : _env(env), _sharedObjectPropagation(sharedObjectPropagation) {
                _iteratorContextProxy = std::make_shared<IteratorContextProxy>(&env);
            }

            codegen::SerializableValue createGlobalSymbolCall(LambdaFunctionBuilder& lfb,
                    llvm::IRBuilder<>& builder,
                    const std::string& symbol,
                    const python::Type& argsType,
                    const python::Type& retType,
                    const std::vector<codegen::SerializableValue>& args);

            codegen::SerializableValue createAttributeCall(LambdaFunctionBuilder& lfb,
                    llvm::IRBuilder<>& builder,
                    const std::string& symbol,
                    const python::Type& callerType,
                    const python::Type& argsType,
                    const python::Type& retType,
                    const SerializableValue& caller,
                    const std::vector<codegen::SerializableValue>& args);

            // global functions
            SerializableValue createLenCall(llvm::IRBuilder<>& builder,
                    const python::Type &argsType,
                    const python::Type &retType,
                    const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createFormatCall(llvm::IRBuilder<>& builder,
                                               const SerializableValue& caller,
                                               const std::vector<tuplex::codegen::SerializableValue>& args,
                                               const std::vector<python::Type>& argsTypes);
            SerializableValue createLowerCall(llvm::IRBuilder<>& builder, const SerializableValue& caller);
            SerializableValue createUpperCall(llvm::IRBuilder<>& builder, const SerializableValue& caller);
            SerializableValue createSwapcaseCall(llvm::IRBuilder<>& builder, const SerializableValue& caller);
            SerializableValue createFindCall(llvm::IRBuilder<>& builder, const SerializableValue& caller, const SerializableValue& needle);
            SerializableValue createReverseFindCall(llvm::IRBuilder<>& builder, const SerializableValue& caller, const SerializableValue& needle);
            SerializableValue createStripCall(llvm::IRBuilder<>& builder, const SerializableValue& caller, const std::vector<tuplex::codegen::SerializableValue>& args);
            SerializableValue createLStripCall(llvm::IRBuilder<>& builder, const SerializableValue& caller, const std::vector<tuplex::codegen::SerializableValue>& args);
            SerializableValue createRStripCall(llvm::IRBuilder<>& builder, const SerializableValue& caller, const std::vector<tuplex::codegen::SerializableValue>& args);
            SerializableValue createReplaceCall(llvm::IRBuilder<>& builder, const SerializableValue& caller, const SerializableValue& from, const SerializableValue& to);
            SerializableValue createCenterCall(LambdaFunctionBuilder& lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller, const SerializableValue &width, const SerializableValue *fillchar);
            SerializableValue createJoinCall(llvm::IRBuilder<>& builder, const SerializableValue& caller, const SerializableValue& list);
            SerializableValue createSplitCall(LambdaFunctionBuilder& lfb, llvm::IRBuilder<> &builder, const tuplex::codegen::SerializableValue &caller, const tuplex::codegen::SerializableValue &delimiter);

            SerializableValue createIntCast(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createCapwordsCall(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, const SerializableValue& caller);

            SerializableValue
            createReSearchCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const python::Type &argsType,
                               const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue
            createReSubCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const python::Type &argsType,
                               const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createRandomChoiceCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const python::Type &argType, const SerializableValue &arg);

            SerializableValue createIterCall(LambdaFunctionBuilder &lfb,
                                             llvm::IRBuilder<>& builder,
                                             const python::Type &argsType,
                                             const python::Type &retType,
                                             const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createReversedCall(LambdaFunctionBuilder &lfb,
                                             llvm::IRBuilder<>& builder,
                                             const python::Type &argsType,
                                             const python::Type &retType,
                                             const std::vector<tuplex::codegen::SerializableValue> &args);

            SerializableValue createNextCall(LambdaFunctionBuilder &lfb,
                                             llvm::IRBuilder<>& builder,
                                             const python::Type &argsType,
                                             const python::Type &retType,
                                             const std::vector<tuplex::codegen::SerializableValue> &args,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo);

            SerializableValue createZipCall(LambdaFunctionBuilder &lfb,
                                             llvm::IRBuilder<>& builder,
                                             const python::Type &argsType,
                                             const python::Type &retType,
                                             const std::vector<tuplex::codegen::SerializableValue> &args,
                                             const std::shared_ptr<IteratorInfo> &iteratorInfo);

            SerializableValue createEnumerateCall(LambdaFunctionBuilder &lfb,
                                            llvm::IRBuilder<>& builder,
                                            const python::Type &argsType,
                                            const python::Type &retType,
                                            const std::vector<tuplex::codegen::SerializableValue> &args,
                                            const std::shared_ptr<IteratorInfo> &iteratorInfo);

            /*!
             * Create calls related to iterators. Including iterator generating calls (iter(), zip(), enumerate())
             * or function calls that take iteratorType as argument (next())
             * @param lfb
             * @param builder
             * @param symbol
             * @param argsType
             * @param retType
             * @param args
             * @param iteratorInfo
             * @return
             */
            SerializableValue createIteratorRelatedSymbolCall(tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                              llvm::IRBuilder<> &builder,
                                                              const std::string &symbol,
                                                              const python::Type &argsType,
                                                              const python::Type &retType,
                                                              const std::vector<tuplex::codegen::SerializableValue> &args,
                                                              const std::shared_ptr<IteratorInfo> &iteratorInfo);

            SerializableValue createDictConstructor(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);
            void getValueFromcJSON(llvm::IRBuilder<> &builder, llvm::Value *cjson_val, python::Type retType,
                                   llvm::Value *retval,
                                   llvm::Value *retsize);
            SerializableValue createCJSONPopCall(LambdaFunctionBuilder& lfb,
                                            llvm::IRBuilder<>& builder,
                                            const SerializableValue& caller,
                                            const std::vector<tuplex::codegen::SerializableValue>& args,
                                            const std::vector<python::Type>& argsTypes,
                                            const python::Type& retType);
            SerializableValue createCJSONPopItemCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller,
                              const python::Type &retType);

            SerializableValue createFloatCast(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);
            SerializableValue createBoolCast(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);
            SerializableValue createStrCast(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, python::Type argsType, const std::vector<tuplex::codegen::SerializableValue> &args);
            SerializableValue createIndexCall(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, const SerializableValue& caller, const SerializableValue& needle);
            SerializableValue createReverseIndexCall(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, const SerializableValue& caller, const SerializableValue& needle);
            SerializableValue createCountCall(llvm::IRBuilder<> &builder, const SerializableValue &caller, const SerializableValue &needle);
            SerializableValue createStartswithCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller, const SerializableValue &needle);
            SerializableValue createEndswithCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller, const SerializableValue &suffix);
            SerializableValue createIsDecimalCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller);
            SerializableValue createIsDigitCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller);
            SerializableValue createIsAlphaCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller);
            SerializableValue createIsAlNumCall(LambdaFunctionBuilder &lfb, llvm::IRBuilder<> &builder, const SerializableValue &caller);
            SerializableValue createMathToRadiansCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                                                 const python::Type &retType,
                                                                                 const std::vector<tuplex::codegen::SerializableValue> &args);
            SerializableValue createMathToDegreesCall(llvm::IRBuilder<>& builder, const python::Type &argsType,
                                                      const python::Type &retType,
                                                      const std::vector<tuplex::codegen::SerializableValue> &args);

            // math module functions
            SerializableValue createMathCeilFloorCall(LambdaFunctionBuilder& lfb, llvm::IRBuilder<>& builder, const std::string& qual_name, const SerializableValue& arg);

        private:
            LLVMEnvironment& _env;
            bool _sharedObjectPropagation;
            std::shared_ptr<IteratorContextProxy> _iteratorContextProxy;

            // lookup (symbolname, typehash)
            std::unordered_map<std::tuple<std::string, python::Type>, llvm::Function*> _funcMap;

            void constructIfElse(llvm::Value *condition, std::function<llvm::Value*(void)> ifCase,
                                                                 std::function<llvm::Value*(void)> elseCase,
                                                                 llvm::Value *res,
                                                                 tuplex::codegen::LambdaFunctionBuilder &lfb,
                                                                 llvm::IRBuilder<> &builder);
        };
    }
}

#endif //TUPLEX_FUNCTIONREGISTRY_H