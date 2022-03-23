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

namespace tuplex {
    namespace codegen {

        /*!
             * helper struct to give code-gen context for a single Stage
             */
        struct CodeGenerationContext {

            // @TODO: add updateInputExceptions...

            // Common variables, i.e. config settings for global code-gen
            bool                                            allowUndefinedBehavior;
            bool                                            sharedObjectPropagation;
            bool                                            nullValueOptimization;
            bool                                            isRootStage;
            bool                                            generateParser;
            double                                          normalCaseThreshold;

            // outputMode & format are shared between the different code-paths (normal & general & fallback)
            EndPointMode                                    outputMode;
            FileFormat                                      outputFileFormat;
            int64_t                                         outputNodeID;
            Schema                                          outputSchema; //! output schema of stage
            std::unordered_map<std::string, std::string>    fileOutputParameters; // parameters specific for a file output format
            size_t                                          outputLimit;

            std::vector<size_t>                             hashColKeys; // the column to use as hash key
            python::Type                                    hashKeyType;
            bool                                            hashSaveOthers; // whether to save other columns than the key or not. => TODO: UDAs, meanByKey etc. all will require similar things...
            bool                                            hashAggregate; // whether the hashtable is an aggregate

            // input mode and parameters are also shared between the different codepaths
            EndPointMode                                    inputMode;
            FileFormat                                      inputFileFormat;
            int64_t                                         inputNodeID;
            std::unordered_map<std::string, std::string>    fileInputParameters; // parameters specific for a file input format

            std::map<int, int> normalToGeneralMapping; // mapping of column indices of normal-case to general-case columns.


//                // Resolve variables (they will be only present on slow path!)
//                std::vector<LogicalOperator*>                   resolveOperators;
//                // the input node of the general case path. => fast-path may specialize its own input Node!
//                LogicalOperator*                                inputNode;


            struct CodePathContext {
                Schema readSchema;
                Schema inputSchema;
                Schema outputSchema;

                std::shared_ptr<LogicalOperator>              inputNode;
                std::vector<std::shared_ptr<LogicalOperator>> operators;
                std::vector<bool>             columnsToRead;

                // columns to perform checks on (fastPathOnly)
                CodePathContext() : inputNode(nullptr) {}

                bool valid() const { return inputSchema.getRowType() != python::Type::UNKNOWN && inputNode; }

                nlohmann::json to_json() const;
                static CodePathContext from_json(nlohmann::json obj);

                template<class Archive> void serialize(Archive & ar) {
                    ar(readSchema, inputSchema, outputSchema, inputNode, operators, columnsToRead);
                }
            };

            CodePathContext fastPathContext;
            CodePathContext slowPathContext;

            inline char csvOutputDelimiter() const {
                return fileOutputParameters.at("delimiter")[0];
            }
            inline char csvOutputQuotechar() const {
                return fileOutputParameters.at("quotechar")[0];
            }

            inline bool hasOutputLimit() const {
                return outputLimit < std::numeric_limits<size_t>::max();
            }

            // serialization, TODO
            std::string toJSON() const;
            static CodeGenerationContext fromJSON(const std::string& json_str);

//                Schema resolveReadSchema; //! schema for reading input
//                Schema resolveInputSchema; //! schema after applying projection pushdown to input source code
//                Schema resolveOutputSchema; //! output schema of stage
//
//                Schema normalCaseInputSchema; //! schema after applying normal case optimizations

            // python::Type hashBucketType;

//                // Fast Path
//                std::vector<LogicalOperator*> fastOperators;
//                python::Type fastReadSchema;
//                python::Type fastInSchema;
//                python::Type fastOutSchema;


            // serialize using Cereal
            template<class Archive> void serialize(Archive & ar) {
                ar(allowUndefinedBehavior,
                sharedObjectPropagation,
                nullValueOptimization,
                isRootStage,
                generateParser,
                normalCaseThreshold,
                outputMode,
                outputFileFormat,
                outputNodeID,
                outputSchema,
                fileOutputParameters,
                outputLimit,
                hashColKeys,
                hashKeyType,
                hashSaveOthers,
                hashAggregate,
                inputMode,
                inputFileFormat,
                inputNodeID,
                fileInputParameters,
                normalToGeneralMapping,
                fastPathContext,
                slowPathContext);
            }
        };

        /*!
         * builder class to generate code for a transform stage
         */
        class StageBuilder {
        public:
            StageBuilder() = delete;

            /*!
             * Create new StageBuilder
             * @param stage_number number of the stage
             * @param rootStage whether is a root stage
             * @param allowUndefinedBehavior whether undefined behavior is allowed
             * @param generateParser whether to generate a parser
             * @param normalCaseThreshold between 0 and 1 threshold
             * @param sharedObjectPropagation whether to use shared object propogation
             * @param nullValueOptimization whether to use null value optimization
             * @param updateInputExceptions whether input exceptions indices need to be updated
             */
            StageBuilder(int64_t stage_number,
                         bool rootStage,
                         bool allowUndefinedBehavior,
                         bool generateParser,
                         double normalCaseThreshold,
                         bool sharedObjectPropagation,
                         bool nullValueOptimization,
                         bool updateInputExceptions);

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

            friend void hyperspecialize(TransformStage *stage, const URI& uri, size_t file_size);

            /*!
             * not always are all symbols generated, this here is a helper function to set
             * all symbols that are not contained within the llvm module to ""
             * @param m module to search over
             * @param cp code path to manupulate
             */
            static void removeMissingSymbols(llvm::Module& m, TransformStage::StageCodePath& cp);

            // flags to influence code generation
            bool _isRootStage;
            bool _allowUndefinedBehavior;
            bool _generateParser;
            double _normalCaseThreshold;
            bool _sharedObjectPropagation;
            bool _nullValueOptimization;
            bool _updateInputExceptions;

            std::vector<std::shared_ptr<LogicalOperator>> _operators;

            int64_t _stageNumber;
            int64_t _outputDataSetID;

            CodeGenerationContext createCodeGenerationContext() const;
            CodeGenerationContext::CodePathContext getGeneralPathContext() const;

            std::string _aggregateCallbackName;

            std::string _pyPipelineName;
            std::string _pyCode;            // python backup code

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
             * @praam normalCaseType: the inputSchema row type of the specialized, normal case!
             */
            TransformStage::StageCodePath generateResolveCodePath(const CodeGenerationContext& ctx,
                                                                  const CodeGenerationContext::CodePathContext& pathContext,
                                                                  const python::Type& normalCaseType) const; //! generates mix of LLVM / python code for slow code path including resolvers

            struct PythonCodePath {
                std::string pyCode;
                std::string pyPipelineName;
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