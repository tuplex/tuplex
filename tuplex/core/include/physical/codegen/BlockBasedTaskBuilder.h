//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_BLOCKBASEDTASKBUILDER_H
#define TUPLEX_BLOCKBASEDTASKBUILDER_H

#include "PipelineBuilder.h"
#include "CSVParseRowGenerator.h"
#include "CodeDefs.h"

#include <memory>

namespace tuplex {
    namespace codegen {

        // to create BlockBasedPipelines, i.e. they take an input buffer
        // then deserialize somehow (aka parsing) and call a pipeline.
        class BlockBasedTaskBuilder {
        protected:
            std::shared_ptr<LLVMEnvironment> _env;

            std::shared_ptr<codegen::PipelineBuilder> pipeline() { return _pipBuilder; }

            llvm::Function *createFunction();

            /*!
             * create function to process blocks of data along with any input exceptions. Used when filters are present
             * to update the indices of the exceptions.
             * @return llvm function
             */
            llvm::Function *createFunctionWithExceptions();

            python::Type _inputRowType;
            python::Type _inputRowTypeGeneralCase;

            std::string _intermediateCallbackName;
            Row _intermediateInitialValue;
            python::Type _intermediateType;

            llvm::Value *initIntermediate(llvm::IRBuilder<> &builder);

            void writeIntermediate(llvm::IRBuilder<> &builder,
                                   llvm::Value* userData,
                                   const std::string &intermediateCallbackName);

            /*!
             * serializes the exception row
             * @param LLVM builder
             * @param ftIn the flattened tuple representing an input row
             * @return a rtmalloc'ed representation of the exception row
             */
            SerializableValue serializedExceptionRow(llvm::IRBuilder<>& builder, const FlattenedTuple& ftIn) const;

            /*!
             * returns argument of function
             * @param name name of the argument
             * @return
             */
            inline llvm::Value *arg(const std::string &name) {
                auto it = _args.find(name);
                if (it == _args.end())
                    throw std::runtime_error("unknown arg " + name + " requested");
                return it->second;
            }

            /*!
             * creates a new exception block. Builder will be set to last block (i.e. where to continue logic)
             */
            llvm::BasicBlock *exceptionBlock(llvm::IRBuilder<> &builder,
                                             llvm::Value *userData,
                                             llvm::Value *exceptionCode,
                                             llvm::Value *exceptionOperatorID,
                                             llvm::Value *rowNumber,
                                             llvm::Value *badDataPtr,
                                             llvm::Value *badDataLength);


            inline void callExceptHandler(llvm::IRBuilder<> &builder,
                                             llvm::Value *userData,
                                             llvm::Value *exceptionCode,
                                             llvm::Value *exceptionOperatorID,
                                             llvm::Value *rowNumber,
                                             llvm::Value *badDataPtr,
                                             llvm::Value *badDataLength) {
                if(!_exceptionHandlerName.empty()) {
                    auto eh_func = codegen::exception_handler_prototype(env().getContext(), env().getModule().get(), _exceptionHandlerName);
                    // simple call to exception handler...
                    builder.CreateCall(eh_func, {userData, exceptionCode, exceptionOperatorID, rowNumber, badDataPtr, badDataLength});
                } else {
                    Logger::instance().logger("codegen").debug("Calling directly exception handler without being defined. Internal bug?");
                }
            }

            bool hasExceptionHandler() const { return !_exceptionHandlerName.empty(); }

            void generateTerminateEarlyOnCode(llvm::IRBuilder<>& builder,
                                              llvm::Value* ecCode,
                                              ExceptionCode code = ExceptionCode::OUTPUT_LIMIT_REACHED);

        private:
            std::shared_ptr<codegen::PipelineBuilder> _pipBuilder;
            std::string _desiredFuncName;
            llvm::Function *_func;

            std::vector<std::tuple<int64_t, ExceptionCode>> _codesToIgnore;
            std::string _exceptionHandlerName;
            std::unordered_map<std::string, llvm::Value *> _args;

            llvm::Value *_intermediate;
            std::map<int, int> _normalToGeneralMapping;
        public:
            BlockBasedTaskBuilder() = delete;

            /*!
             * creates a function to read rows from memory and process them via a pipeline.
             * @param env LLVM codegen environment where to put everything
             * @param inputRowType the row type rows are stored in within the memory block
             * @param generalCaseInputRowType the row type exceptions should be stored in. inputRowType must be upcastable to generalCaseInputRowType.
             * @param normalToGeneralMapping normal to general mapping, i.e. which column index in normal case corresponds to which column index in general case. If empty, means it's a 1:1 trivial mapping.
             * @param name how to call the function to be generated.
             */
            BlockBasedTaskBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                                  const python::Type& inputRowType,
                                  const python::Type& generalCaseInputRowType,
                                  const std::map<int, int>& normalToGeneralMapping,
                                  const std::string &name) : _env(env), _inputRowType(inputRowType),
                                                             _inputRowTypeGeneralCase(generalCaseInputRowType),
                                                             _normalToGeneralMapping(normalToGeneralMapping),
                                                             _desiredFuncName(name),
                                                             _intermediate(nullptr),
                                                             _intermediateType(python::Type::UNKNOWN) {
                // check that upcasting is true or there is a valid mapping when sizes differ!
//                assert((_inputRowType.parameters().size() != _inputRowTypeGeneralCase.parameters().size()
//                && !_normalToGeneralMapping.empty() && _normalToGeneralMapping.size() == _inputRowType.parameters().size()) ||
//                canUpcastToRowType(_inputRowType, _inputRowTypeGeneralCase));
                assert(checkCaseCompatibility(_inputRowType, _inputRowTypeGeneralCase, _normalToGeneralMapping));
                if(_inputRowType != _inputRowTypeGeneralCase) {
                    Logger::instance().logger("codegen").debug("emitting auto-upcast for exceptions");
                }

                // create trivial mapping if empty.
                if(_normalToGeneralMapping.empty()) {
                    // make sure size matches!
                    assert(inputRowType.isTupleType());
                    assert(generalCaseInputRowType.isTupleType());
                    if(inputRowType.parameters().size() != generalCaseInputRowType.parameters().size())
                        throw std::runtime_error("row type sizes do not match, can't create trivial mapping");
                    auto num_params = inputRowType.parameters().size();
                    for(unsigned i = 0; i < num_params; ++i)
                        _normalToGeneralMapping[i] = i;
                }
            }

            LLVMEnvironment &env() { return *_env; }

            std::string getTaskFuncName() const { return _func->getName(); }

            /*!
             * set internal processing pipeline
             * @param pip
             */
            virtual void setPipeline(const std::shared_ptr<PipelineBuilder> &pip) {
                assert(!_pipBuilder);
                _pipBuilder = pip;

                // check row compatibility
                if (pip->inputRowType() != _inputRowType)
                    throw std::runtime_error("input types of pipeline and CSV Parser incompatible:\n"
                                             "pipeline expects: " + pip->inputRowType().desc() +
                                             " but csv parser yields: " + _inputRowType.desc());
            }

            void
            setIgnoreCodes(const std::vector<std::tuple<int64_t, ExceptionCode>> &codes) { _codesToIgnore = codes; }

            void setExceptionHandler(const std::string &name) { _exceptionHandlerName = name; }

            // aggregation based writers
            /*!
             * this adds an initialized intermediate, e.g. for an aggregate and initializes by the values supplied in Row
             * @param intermediateType type (row might be upcast to it!)
             * @param row the values
             */
            void setIntermediateInitialValueByRow(const python::Type &intermediateType, const Row &row);

            /*!
             * when using aggregates, within the pipeline nothing gets written back.
             * I.e., instead the aggregate gets updated.
             * Call this write back with the intermediate result after the whole input was processed.
             * @param callbackName
             */
            void setIntermediateWriteCallback(const std::string &callbackName);

            /*!
             * build task function
             * @param terminateEarlyOnFailureCode when set to true, the return code of the pipeline is checked.
             *                                    If it's non-zero, the loop is terminated. Helpful for implementing
             *                                    limit operations (i.e. there OUTPUT_LIMIT_REACHED exception is
             *                                    returned in writeRow(...)).
             * @return LLVM function of the task taking a memory block as input, returns nullptr if build failed
             */
            virtual llvm::Function *build(bool terminateEarlyOnFailureCode=false) = 0;
        };

        // @TODO:
        // later JSONSourceTaskBuilder { ... }
        // also GeneralSourceTaskBuilder { ... }
        // TuplexSourceTaskBuilder { ... } // <== takes Tuplex's internal memory format as source

        static python::Type restrictRowType(const std::vector<bool> &columnsToSerialize, const python::Type &rowType) {
            if (columnsToSerialize.empty())
                return rowType;

            std::vector<python::Type> cols;
            assert(rowType.isTupleType());
            assert(columnsToSerialize.size() == rowType.parameters().size());
            for (int i = 0; i < rowType.parameters().size(); ++i) {
                if (columnsToSerialize[i])
                    cols.push_back(rowType.parameters()[i]);
            }
            return python::Type::makeTupleType(cols);
        }
    }
}

#endif //TUPLEX_BLOCKBASEDTASKBUILDER_H