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

#include "physical/PipelineBuilder.h"
#include "physical/CSVParseRowGenerator.h"
#include "physical/CodeDefs.h"

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

            python::Type _inputRowType; //@TODO: make this private??

            std::string _intermediateCallbackName;
            Row _intermediateInitialValue;
            python::Type _intermediateType;

            llvm::Value *initIntermediate(IRBuilder &builder);

            void writeIntermediate(IRBuilder &builder,
                                   llvm::Value* userData,
                                   const std::string &intermediateCallbackName);

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
             * creates a new exception block. Builder will be set to last block (i.e. where to conitnue logic)
             */
            llvm::BasicBlock *exceptionBlock(IRBuilder &builder,
                                             llvm::Value *userData,
                                             llvm::Value *exceptionCode,
                                             llvm::Value *exceptionOperatorID,
                                             llvm::Value *rowNumber,
                                             llvm::Value *badDataPtr,
                                             llvm::Value *badDataLength);

            bool hasExceptionHandler() const { return !_exceptionHandlerName.empty(); }

            void generateTerminateEarlyOnCode(codegen::IRBuilder& builder,
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
        public:
            BlockBasedTaskBuilder() = delete;

            BlockBasedTaskBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                                  const python::Type &rowType,
                                  const std::string &name) : _env(env), _inputRowType(rowType), _desiredFuncName(name),
                                                             _intermediate(nullptr),
                                                             _intermediateType(python::Type::UNKNOWN) {}

            LLVMEnvironment &env() { return *_env; }

            std::string getTaskFuncName() const { return _func->getName().str(); }

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