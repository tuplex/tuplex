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

#include "TransformStage.h"

// class to create TransformStages (Stagefusion!)

namespace tuplex {
    namespace codegen {

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
            void addMemoryInput(const Schema& schema, LogicalOperator* node);
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

            void addOperator(LogicalOperator* op) {
                _operators.push_back(op);
            }

            void addFileInput(FileInputOperator* csvop);
            void addFileOutput(FileOutputOperator* fop);

            inline void setOutputLimit(size_t limit) {
                _outputLimit = limit;
            }

            TransformStage* build(PhysicalPlan* plan, IBackend* backend);
            inline TransformStage* build() { return build(nullptr, nullptr); }
        private:

            // flags to influence code generation
            bool _isRootStage;
            bool _allowUndefinedBehavior;
            bool _generateParser;
            double _normalCaseThreshold;
            bool _sharedObjectPropagation;
            bool _nullValueOptimization;
            bool _updateInputExceptions;
            std::vector<LogicalOperator*> _operators;

            // codegen strings
            std::string _funcFileWriteCallbackName;
            std::string _funcMemoryWriteCallbackName;
            std::string _funcExceptionCallback;

            int64_t _stageNumber;
            int64_t _outputDataSetID;

            std::string _resolveRowFunctionName;
            std::string _resolveRowWriteCallbackName;
            std::string _resolveRowExceptionCallbackName;
            std::string _resolveHashCallbackName;

            std::string _initStageFuncName;
            std::string _releaseStageFuncName;

            std::string _aggregateInitFuncName;
            std::string _aggregateCombineFuncName;
            std::string _aggregateAggregateFuncName;
            std::string _aggregateCallbackName;

            std::string _pyPipelineName;
            std::string _irBitCode;         // store code as bitcode (faster for parsing)
            std::string _pyCode;            // python backup code

            std::string _writerFuncName; // name of the function where to write stuff to.

            std::string _funcStageName;
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

            LogicalOperator* _inputNode;
            std::vector<bool> _columnsToRead;

            std::string _funcHashWriteCallbackName; // callback for writing to hash table
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

            inline std::string next_hashmap_name() {
                static int counter = 0;
                return "hashmap" + fmt::format("{:02d}", counter++);
            }

            /*!
             * returns a nicely formatted overview of a bad UDF operator node
             * @param udfop
             * @return string
             */
            std::string formatBadUDFNode(UDFOperator* udfop);

            /*!
             * generate LLVM IR code
             * @param fastCodePath whether to generate for fastCodePath or not. When false, always generates mem2mem.
             * @return
             */
            bool generateFastCodePath(); // file2mem always

            size_t resolveOperatorCount() {
                return std::count_if(_operators.begin(), _operators.end(), [](const LogicalOperator* op) {
                    return op && op->type() == LogicalOperatorType::RESOLVE;
                });
            }

            // holds values of hashmap globals
            std::unordered_map<int64_t, std::tuple<llvm::Value*, llvm::Value*>> _hashmap_vars;

            /*!
             * code path for mem2mem exception resolution => sh
             */
            bool generateResolveCodePath(const std::shared_ptr<codegen::LLVMEnvironment>& env); //! generates mix of LLVM / python code for slow code path including resolvers

            void generatePythonCode(); //! generates fallback pipeline in pure python. => i.e. special case here...

            python::Type intermediateType() const;

            std::vector<int64_t> getOperatorIDsAffectedByResolvers(const std::vector<LogicalOperator *> &operators);
        };
    }
}

#endif //TUPLEX_STAGEBUILDER_H