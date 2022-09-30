//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_STAGEBUILDER_H
#define TUPLEX_STAGEBUILDER_H

#include "physical/execution/TransformStage.h"

// include all logical ops here
#include <logical/Operators.h>
#include "physical/codegen/CodeGenerationContext.h"

namespace tuplex {
    namespace codegen {

        /*!
         * builder class to generate code for a transform stage
         */
        class StageBuilder {
        public:
            StageBuilder() = delete;

            // deprecated
            StageBuilder(int64_t stage_number,
                         bool rootStage,
                         bool allowUndefinedBehavior,
                         bool generateParser,
                         double normalCaseThreshold,
                         bool sharedObjectPropagation,
                         bool nullValueOptimization,
                         bool constantFoldingOptimization,
                         bool updateInputExceptions,
                         bool generateSpecializedNormalCaseCodePath=true);

            /*!
            * Create new StageBuilder
            * @param policy compiler policy for stage and UDFs
            * @param stage_number number of the stage
            * @param rootStage whether is a root stage
            * @param generateParser whether to generate a parser
            * @param nullValueOptimization whether to use null value optimization
            * @param constantFoldingOptimization whether to apply constant folding or not
            * @param updateInputExceptions whether input exceptions indices need to be updated
            * @param generateSpecializedNormalCaseCodePath whether to emit specialized normal case code path or not
            */
            StageBuilder(const CompilePolicy& policy,
                         int64_t stage_number,
                         bool rootStage,
                         bool generateParser,
                         bool nullValueOptimization,
                         bool constantFoldingOptimization,
                         bool updateInputExceptions,
                         bool generateSpecializedNormalCaseCodePath=true);

            // builder functions
            void addMemoryInput(const Schema& schema, std::shared_ptr<LogicalOperator> node);
            /*!
             * add memory output writer
             * @param schema
             * @param opID operator ID
             * @param dsID output dataset ID
             */
            void addMemoryOutput(const Schema& schema, int64_t opID, int64_t dsID);

            /*!
             * change whether StageBuilder should generate null-value opt or not.
             * @param enable if true, enable NVO else no NVO.
             */
            void setNullValueOptimization(bool enable) { _nullValueOptimization = enable;}

            // saves output to a hashtable, requires caller to combine multiple hash tables later...
            void addHashTableOutput(const Schema& schema,
                                    bool bucketizeOthers,
                                    bool aggregate,
                                    const std::vector<size_t> &colKeys,
                                    const python::Type& keyType,
                                    const python::Type& bucketType);

            void addOperator(const std::shared_ptr<LogicalOperator>& op) {
                _operators.push_back(op);
            }

            void addFileInput(const std::shared_ptr<FileInputOperator> &csvop);
            void addFileOutput(const std::shared_ptr<FileOutputOperator>& fop);

            inline void setOutputLimit(size_t limit) {
                _outputLimit = limit;
            }

            TransformStage* build(PhysicalPlan* plan, IBackend* backend);
            inline TransformStage* build() { return build(nullptr, nullptr); }

            // HACK: experimental function to encode as bytes a TransformStage
            /*!
             * encode all required operators for this stage using Cereal. In addition, depending
             * on whether desired individual codepaths can get generated (or not)
             * @param plan the plan to associate with
             * @param backend which backend to ude
             * @param gen_py_code whether to generate python code or not
             * @param gen_fast_code whether to generate fast/normal-case code or not
             * @param gen_slow_code wheteher to generate slow/general-case code or not
             * @return TransformStage
             */
            TransformStage* encodeForSpecialization(PhysicalPlan* plan,
                                                    IBackend* backend,
                                                    bool gen_py_code=true,
                                                    bool gen_fast_code=false,
                                                    bool gen_slow_code=false);


            /*!
             * generate LLVM IR code
             * @param fastCodePath whether to generate for fastCodePath or not. When false, always generates mem2mem.
             * @return
             */
            static TransformStage::StageCodePath generateFastCodePath(const CodeGenerationContext& ctx,
                                                                      const CodeGenerationContext::CodePathContext& pathContext,
                                                                      const python::Type& generalCaseInputRowType,
                                                                      const std::vector<bool> &generalCaseColumnsToRead,
                                                                      const python::Type& generalCaseOutputRowType,
                                                                      const std::map<int, int>& normalToGeneralMapping,
                                                                      int stageNo,
                                                                      const std::string& env_name="tuplex_fastCodePath"); // file2mem always

        private:

            friend void hyperspecialize(TransformStage *stage, const URI& uri, size_t file_size, double nc_threshold);

            /*!
             * not always are all symbols generated, this here is a helper function to set
             * all symbols that are not contained within the llvm module to ""
             * @param m module to search over
             * @param cp code path to manupulate
             */
            static void removeMissingSymbols(llvm::Module& m, TransformStage::StageCodePath& cp);

            // flags to influence code generation
            bool _isRootStage;
            CompilePolicy _policy;

            bool _generateParser;
            bool _generateNormalCaseCodePath;
            bool _nullValueOptimization;
            bool _constantFoldingOptimization;
            bool _updateInputExceptions;

            std::vector<std::shared_ptr<LogicalOperator>> _operators;

            int64_t _stageNumber;
            int64_t _outputDataSetID;

            CodeGenerationContext createCodeGenerationContext() const;
            CodeGenerationContext::CodePathContext getGeneralPathContext() const;

            std::string _aggregateCallbackName;

            std::string _pyPipelineName;
            std::string _pyCode;            // python backup code

            std::string _pyAggregateCode;
            std::string _pyAggregateFunctionName;

            std::string _writerFuncName; // name of the function where to write stuff to.

            std::unordered_map<std::string, std::string> _fileInputParameters; // parameters specific for a file input format
            std::unordered_map<std::string, std::string> _fileOutputParameters; // parameters specific for a file output format

            // config
            EndPointMode _inputMode;
            EndPointMode _outputMode;
            std::vector<std::string> _inputColumns;
            std::vector<std::string> _outputColumns;
            URI _outputURI;
            FileFormat _inputFileFormat;
            FileFormat _outputFileFormat;
            int64_t _outputNodeID;
            int64_t _inputNodeID;
            size_t _outputLimit;

            std::shared_ptr<LogicalOperator> _inputNode;
            std::vector<bool> _columnsToRead;

            std::vector<size_t>      _hashColKeys; // the column to use as hash key
            python::Type _hashKeyType;
            python::Type _hashBucketType;
            bool        _hashSaveOthers; // whether to save other columns than the key or not. => TODO: UDAs, meanByKey etc. all will require similar things...
            bool        _hashAggregate; // whether the hashtable is an aggregate

            Schema _readSchema; //! schema for reading input
            Schema _inputSchema; //! schema after applying projection pushdown to input source code
            Schema _outputSchema; //! output schema of stage

            Schema _normalCaseInputSchema; //! schema after applying normal case optimizations
            Schema _normalCaseOutputSchema; //! schema after applying normal case optimizations


            void fillStageParameters(TransformStage* stage);

            size_t number() const { return _stageNumber; }
            int64_t outputDataSetID() const;

            inline size_t inputColumnCount() const {
                assert(_readSchema != Schema::UNKNOWN);
                return _readSchema.getRowType().parameters().size();
            }

            inline bool hasOutputLimit() const {
                return _outputLimit < std::numeric_limits<size_t>::max();
            }

            inline char csvOutputDelimiter() const {
                return _fileOutputParameters.at("delimiter")[0];
            }
            inline char csvOutputQuotechar() const {
                return _fileOutputParameters.at("quotechar")[0];
            }

            /*!
             * returns a nicely formatted overview of a bad UDF operator node
             * @param udfop
             * @return string
             */
            static std::string formatBadUDFNode(UDFOperator* udfop);

            size_t resolveOperatorCount() const {
                return std::count_if(_operators.begin(), _operators.end(), [](const std::shared_ptr<LogicalOperator> &op) {
                    return op && op->type() == LogicalOperatorType::RESOLVE;
                });
            }


            void determineSchema();

            static void fillInCallbackNames(const std::string& func_prefix, size_t stageNo, TransformStage::StageCodePath& cp);
            // holds values of hashmap globals
            std::unordered_map<int64_t, std::tuple<llvm::Value*, llvm::Value*>> _hashmap_vars;

             /*!
              * code path for mem2mem exception resolution => sh
              * @param ctx
              * @param pathContext
              * @param normalCaseType the inputSchema row type of the specialized, normal case!
              * @param normalToGeneralMapping mapping columns of normal case to general case (they may have different number of columns)
              * @return
              */
            TransformStage::StageCodePath generateResolveCodePath(const CodeGenerationContext& ctx,
                                                                  const CodeGenerationContext::CodePathContext& pathContext,
                                                                  const python::Type& normalCaseType,
                                                                  const std::map<int, int>& normalToGeneralMapping={}) const; //! generates mix of LLVM / python code for slow code path including resolvers

            struct PythonCodePath {
                std::string pyCode;
                std::string pyPipelineName;
                std::string pyAggregateCode;
                std::string pyAggregateFunctionName;
            };

            static PythonCodePath generatePythonCode(const CodeGenerationContext& ctx, int stageNo); //! generates fallback pipeline in pure python. => i.e. special case here...

            std::vector<int64_t> getOperatorIDsAffectedByResolvers(const std::vector<std::shared_ptr<LogicalOperator>> &operators);
        };

        /*
         * Returns the intermediate schema if the output node of the list of operators is an aggregate.
         * @param operators
         */
        extern python::Type intermediateType(const std::vector<std::shared_ptr<LogicalOperator>>& operators);
    }
}

#endif //TUPLEX_STAGEBUILDER_H