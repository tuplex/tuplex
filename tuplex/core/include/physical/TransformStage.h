//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TRANSFORMSTAGE_H
#define TUPLEX_TRANSFORMSTAGE_H

#include <Schema.h>
#include <Partition.h>
#include <ExceptionInfo.h>
#include "PhysicalStage.h"
#include "LLVMOptimizer.h"
#include <logical/ParallelizeOperator.h>
#include <logical/MapOperator.h>
#include <logical/MapColumnOperator.h>
#include <logical/WithColumnOperator.h>
#include <logical/FilterOperator.h>
#include <logical/TakeOperator.h>
#include <logical/FileInputOperator.h>
#include <logical/IgnoreOperator.h>
#include <logical/ResolveOperator.h>
#include <JITCompiler.h>
#include <physical/CSVParserGenerator.h>
#include <mt/ThreadPool.h>
#include <limits>
#include <Defs.h>
#include <logical/FileOutputOperator.h>
#include <logical/AggregateOperator.h>

#ifdef BUILD_WITH_AWS
// include protobuf serialization of TrafoStage for Lambda executor
#include <Lambda.pb.h>
#endif

namespace tuplex {

    inline FileFormat proto_toFileFormat(messages::FileFormat fmt) {
        switch(fmt) {
            case messages::FileFormat::FF_CSV:
                return FileFormat::OUTFMT_CSV;
            case messages::FileFormat::FF_TEXT:
                return FileFormat::OUTFMT_TEXT;
            case messages::FileFormat::FF_TUPLEX:
                return FileFormat::OUTFMT_TUPLEX;
            default:
                return FileFormat::OUTFMT_UNKNOWN;
        }
    }


    // forward declaration of friend class
    namespace codegen {
        class StageBuilder;
    }

    inline std::string epmToStr(EndPointMode m) {
        switch(m) {
            case EndPointMode::MEMORY:
                return "memory";
            case EndPointMode::FILE:
                return "file";
            case EndPointMode::HASHTABLE:
                return "hash";
            default:
                return "unknown";
        }
    }

    /*!
     * describes a stage which maps/filters/loads/saves data.
     * execute as narrow stage with per-node/per-thread parallelism.
     */
    class TransformStage : public PhysicalStage {
    public:
        TransformStage() = delete;
        ~TransformStage() override = default;

        friend class ::tuplex::codegen::StageBuilder;

        std::vector<Partition*> inputPartitions() const { return _inputPartitions; }

#ifdef BUILD_WITH_AWS
        std::unique_ptr<messages::TransformStage> to_protobuf() const;
        static TransformStage* from_protobuf(const messages::TransformStage& msg);
#endif

        /*!
         * set input files to stage (will be serialized to partitions on driver)
         * @param uris
         * @param sizes
         */
        void setInputFiles(const std::vector<URI>& uris, const std::vector<size_t>& sizes);

        /*!
         * set input partitions to this executor
         * @param partitions vector of partitions to process for this stage
         * @param inputLimit limit the number of input rows processed by this stage
         */
        void setInputPartitions(const std::vector<Partition*>& partitions, size_t inputLimit = std::numeric_limits<size_t>::max()) {
            _inputPartitions = partitions;
            _inputLimit = inputLimit;
        }

        /*!
         * set input exceptions, i.e. rows that could come from a parallelize or csv operator.
         * @param pythonObjects
         */
        void setInputExceptions(const std::vector<Partition *>& inputExceptions) { _inputExceptions = inputExceptions; }

        std::vector<Partition *> inputExceptions() { return _inputExceptions; }

        void setPartitionToExceptionsMap(const std::unordered_map<std::string, ExceptionInfo>& partitionToExceptionsMap) { _partitionToExceptionsMap = partitionToExceptionsMap; }

        std::unordered_map<std::string, ExceptionInfo> partitionToExceptionsMap() { return _partitionToExceptionsMap; }

        /*!
         * sets maximum number of rows this pipeline will produce
         * @param outputLimit
         */
        void setOutputLimit(size_t outputLimit) {
            _outputLimit = outputLimit;

            // @TODO: move this logic to physical plan!
            // pushdown limit
            //pushDownOutputLimit();
        }

        size_t outputLimit() const { return _outputLimit; }
        size_t inputLimit() const { return _inputLimit; }

        /*!
         * check whether pipeline uses files as input or not (i.e., then it uses memory)
         */
        bool fileInputMode() const {
            return _inputMode == EndPointMode::FILE;
        }

        bool fileOutputMode() const {
            return _outputMode == EndPointMode::FILE;
        }

        Schema outputSchema() const { return _outputSchema; }
        Schema inputSchema() const { return _inputSchema; }
        Schema normalCaseOutputSchema() const { return _normalCaseOutputSchema; }
        Schema normalCaseInputSchema() const { return _normalCaseInputSchema; }
        int64_t outputDataSetID() const { return _outputDataSetID; }
        std::unordered_map<std::string, std::string> outputOptions() const;

        Schema readSchema() const { return _readSchema; }
        int64_t fileInputOperatorID() const { assert(_inputMode == EndPointMode::FILE); return _inputNodeID; }

        /*!
         * when trying to resolve exception only certain operators may have exceptions. To avoid
         * costly reprocessing, check operatorIDs.
         * @return vector of operator ID where resolvers exist (i.e. the ones which need to be forced on the slow path)
         */
        std::vector<int64_t> operatorIDsWithResolvers() const { return _operatorIDsWithResolvers; }

        URI outputURI() const { return _outputURI; }

        /*!
         * fetch data into resultset
         * @return resultset of this stage
         */
        std::shared_ptr<ResultSet> resultSet() const override { return _rs;}

        void setMemoryResult(const std::vector<Partition*>& partitions,
                             const std::vector<Partition*>& generalCase=std::vector<Partition*>{},
                             const std::unordered_map<std::string, ExceptionInfo>& parttionToExceptionsMap=std::unordered_map<std::string, ExceptionInfo>(),
                             const std::vector<std::tuple<size_t, PyObject*>>& interpreterRows=std::vector<std::tuple<size_t, PyObject*>>{},
                             const std::vector<Partition*>& remainingExceptions=std::vector<Partition*>{},
                             const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts=std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>()); // creates local result set?
        void setFileResult(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts); // creates empty result set with exceptions

        void setEmptyResult() {
            std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> ecounts;
            if(fileOutputMode())
                setFileResult(ecounts);
            else
                setMemoryResult(
                        std::vector<Partition*>(),
                        std::vector<Partition*>(),
                        std::unordered_map<std::string, ExceptionInfo>(),
                        std::vector<std::tuple<size_t, PyObject*>>(),
                        std::vector<Partition*>(),
                        ecounts);
        }

        std::string bitCode() const {
            return _irBitCode;
        }

        // to retrieve actual IR, use code
        std::string code() const {
            if(_irBitCode.empty())
                return "";

            // parse bit code & convert
            llvm::LLVMContext ctx;
            auto mod = codegen::bitCodeToModule(ctx, _irBitCode);
            if(!mod)
                return "";
            return codegen::moduleToString(*mod.get());
        }

        std::string funcName() const { return _funcStageName; }
        std::string writerFuncName() const { return _writerFuncName; }
        std::string writeMemoryCallbackName() const { return _funcMemoryWriteCallbackName; }
        std::string writeFileCallbackName() const { return _funcFileWriteCallbackName; }
        std::string writeHashCallbackName() const { return _funcHashWriteCallbackName; }
        std::string exceptionCallbackName() const { return _funcExceptionCallback; }
        std::string aggCombineCallbackName() const { return _aggregateCallbackName; }

        // std::string resolveCode() const { return _irResolveCode; }
        std::string resolveRowName() const { return _resolveRowFunctionName; }
        std::string resolveWriteCallbackName() const { return _resolveRowWriteCallbackName; }
        std::string resolveHashCallbackName() const { return _resolveHashCallbackName; }
        std::string resolveExceptionCallbackName() const { return _resolveRowExceptionCallbackName; }

        std::string purePythonCode() const { return _pyCode; }
        std::string pythonPipelineName() const { return _pyPipelineName; }

        std::vector<std::string> inputColumns() const {
            return _inputColumns;
        }

        nlohmann::json getJSON() const override;

        // CSV specific params (use at to throw exception if key was not found)
        size_t csvNumFileInputColumns() const { return std::stoi(_fileInputParameters.at("numInputColumns"));}
        bool csvHasHeader() const { return stringToBool(_fileInputParameters.at("hasHeader")); }
        inline char csvInputDelimiter() const {
            return _fileInputParameters.at("delimiter")[0];
        }

        inline char csvInputQuotechar() const {
            return _fileInputParameters.at("quotechar")[0];
        }

        inline char csvOutputDelimiter() const {
            auto it = _fileOutputParameters.find("delimiter");
            if(it == _fileOutputParameters.end())
                return ',';

            return _fileOutputParameters.at("delimiter")[0];
        }

        inline char csvOutputQuotechar() const {
            auto it = _fileOutputParameters.find("delimiter");
            if(it == _fileOutputParameters.end())
                return '"';

            return _fileOutputParameters.at("quotechar")[0];
        }

        std::vector<std::string> csvHeader() const;

        // file output specific params
        size_t splitSize() const { return std::stoi(_fileOutputParameters.at("splitSize")); }

        size_t numOutputFiles() const { return std::stoi(_fileOutputParameters.at("numParts")); }

        UDF outputPathUDF() const { std::cout<<_fileOutputParameters.at("udf")<<std::endl; return UDF(_fileOutputParameters.at("udf")); }

        FileFormat outputFormat() const { return _outputFormat; }
        FileFormat inputFormat() const { return _inputFormat; }
        void execute(const Context& context) override;

        // JITSymbols for this stage
        struct JITSymbols {
            codegen::read_block_f functor; // can be memory2memory or file2memory
            codegen::read_block_exp_f functorWithExp;
            codegen::read_block_f writeFunctor; // memory2file
            codegen::resolve_f resolveFunctor; // always memory2memory
            codegen::init_stage_f initStageFunctor;
            codegen::release_stage_f releaseStageFunctor;
            codegen::agg_init_f aggInitFunctor;
            codegen::agg_combine_f aggCombineFunctor;
            codegen::agg_agg_f aggAggregateFunctor;


            JITSymbols() : functor(nullptr),
                           functorWithExp(nullptr),
                           writeFunctor(nullptr),
                           resolveFunctor(nullptr),
                           initStageFunctor(nullptr),
                           releaseStageFunctor(nullptr),
                           aggInitFunctor(nullptr),
                           aggCombineFunctor(nullptr),
                           aggAggregateFunctor(nullptr) {}
        };

        /*!
         * compile code of this stage via LLVM JIT
         * @param jit JIT instance
         * @param optimizer optional instance of optimizer to run over code first. No optimization run when set to 0
         * @param excludeSlowPath if true, only fast path functions are compiled. This helps to have threads already busy when slow path still need to get compiled.
         * @param registerSymbols whether to add task symbols to JIT compiler
         * @return a struct of all function pointers
         */
        std::shared_ptr<JITSymbols> compile(JITCompiler& jit, LLVMOptimizer *optimizer=nullptr, bool excludeSlowPath=false, bool registerSymbols=true);

        EndPointMode outputMode() const override { return _outputMode; }
        EndPointMode inputMode() const override { return _inputMode; }

        struct InitData {
            int64_t numArgs;
            uint8_t** hash_maps;
            uint8_t** null_buckets;
            PyObject** hybrids;

            InitData() : numArgs(0), hash_maps(nullptr), null_buckets(nullptr), hybrids(nullptr) {}
        };

        const InitData initData() const { return _initData; }
        void setInitData(int64_t numArgs=0, uint8_t** hash_maps=nullptr, uint8_t** null_buckets=nullptr, PyObject** hybrids=nullptr) {
            _initData.numArgs = numArgs;
            _initData.hash_maps = hash_maps;
            _initData.null_buckets = null_buckets;
            _initData.hybrids = hybrids;
        }

        class HashResult {
        public:
            map_t hash_map;
            uint8_t* null_bucket;
            PyObject* hybrid;
            python::Type keyType;
            python::Type bucketType;
            HashResult() : hash_map(nullptr), null_bucket(nullptr), hybrid(nullptr) {}
        };

        HashResult hashResult() {
            return _hashResult;
        }

        /*!
         * general case output key type (NOT the specialized one)
         * @return
         */
        python::Type hashOutputKeyType() {
            assert(_aggMode != AggregateType::AGG_NONE || (_hashOutputKeyType.withoutOptions() == python::Type::I64 || _hashOutputKeyType.withoutOptions() == python::Type::STRING));
            return _hashOutputKeyType;
        }

        /*!
         * general case output bucket type (NOT the specialized one)
         * @return
         */
        python::Type hashOutputBucketType() {
            assert(_hashOutputKeyType != python::Type::UNKNOWN);
            return _hashOutputBucketType;
        }

        int hashtableKeyByteWidth() {
            return codegen::hashtableKeyWidth(_hashOutputKeyType);
        }

        void setHashResult(map_t hash_map, uint8_t* null_bucket,
                           PyObject* hybrid=nullptr) {
            _hashResult.hash_map = hash_map;
            _hashResult.null_bucket = null_bucket;
            _hashResult.hybrid = hybrid;
            _hashResult.keyType = _hashOutputKeyType;
            _hashResult.bucketType = _hashOutputBucketType;
        }

        std::vector<bool> columnsToKeep() const {
            return _inputColumnsToKeep;
        }

        /*!
         * whether to cache normal case/general case separately (true if .cache() is used)
         */
        bool persistSeparateCases() const {
            // this flag will be filled in StageBuilder based on CacheOperator behavior...
            return _persistSeparateCases;
        }

        /*!
         * whether to update indices of input exceptions during row processing
         */
        bool updateInputExceptions() const { return _updateInputExceptions; }

        /*!
         * @return Returns the type of the hash-grouped data. Hash-grouped data refers to when the operator is a
         *         pipeline breaker that needs the previous stage's hashmap to be converted to partitions
         *         (e.g. unique() and aggregateBy())
         */
        AggregateType dataAggregationMode() const { return _aggMode; }

        /*!
         * Set the hash-grouped data type of this Transform Stage (e.g. if it is a unique() or aggregateBy(), need
         * to set the type to AGG_UNIQUE orAGG_AGGREGATE to indicate to the stage how it should build the output table)
         * In addition AGG_GENERAL means a single object is used (i.e. no need to represent via a hash table).
         * @param t the hash-grouped data type this transform stage represents
         */
        void setDataAggregationMode(const AggregateType& t) { _aggMode = t; }

    private:
        /*!
         * creates a new TransformStage with generated code
         * @param plan to which this stage belongs to
         * @param backend backend where to execute code
         * @param number number of the stage
         * @param allowUndefinedBehavior whether to allow unsafe operations for speedups, e.g. division by zero
         */
        TransformStage(PhysicalPlan* plan,
                       IBackend* backend,
                       int64_t number,
                       bool allowUndefinedBehavior);

        std::string _irBitCode; //! llvm IR bitcode
        std::string _funcStageName; //! llvm function name of the stage function
        std::string _funcMemoryWriteCallbackName; //! llvm function name of the write callback
        std::string _funcFileWriteCallbackName; //! llvm function name of the write callback used for file output.
        std::string _funcExceptionCallback; //! llvm function of the exception callback function
        std::string _funcHashWriteCallbackName; //! the function to call when saving to hash table
        std::string _initStageFuncName; //! init function for a stage (sets up globals & Co)
        std::string _releaseStageFuncName; //! release function for a stage (releases globals & Co)
        std::string _aggregateInitFuncName; //! to initiate aggregate (allocates via C-malloc)
        std::string _aggregateCombineFuncName; //! to combine two aggregates (allocates & frees via C-malloc).
        std::string _aggregateCallbackName; //! the callback to call with an aggregate
        std::string _aggregateAggregateFuncName; //! to combine aggregate and result (allocates & frees via C-malloc).
        EndPointMode _inputMode; //! indicate whether stage takes hash table, serialized mem or files as input
        EndPointMode _outputMode; //! analog


        // Key-Value map for file format specific configuration options
        std::unordered_map<std::string, std::string> _fileInputParameters;
        std::unordered_map<std::string, std::string> _fileOutputParameters;

        std::vector<std::string> _inputColumns;
        std::vector<std::string> _outputColumns;
        Schema _readSchema;
        Schema _inputSchema;
        Schema _outputSchema;
        int64_t _outputDataSetID;
        int64_t _inputNodeID;
        std::vector<bool> _inputColumnsToKeep;
        FileFormat _inputFormat;
        FileFormat _outputFormat;
        Schema _normalCaseOutputSchema;
        Schema _normalCaseInputSchema;
        bool _persistSeparateCases;
        AggregateType _aggMode;

        std::vector<int64_t> _operatorIDsWithResolvers;

        std::vector<Partition*> _inputPartitions; //! memory input partitions for this task.
        size_t                  _inputLimit; //! limit number of input rows (inf per default)
        size_t                  _outputLimit; //! output limit, set e.g. by take, to_csv etc. (inf per default)

        std::shared_ptr<ResultSet> _rs; //! result set

        URI _outputURI; //! the output uri for file mode of this stage

        // resolve/exception handling code
        // std::string _irResolveCode;
        std::string _resolveRowFunctionName;
        std::string _resolveRowWriteCallbackName;
        std::string _resolveRowExceptionCallbackName;
        std::string _resolveHashCallbackName;

        // pure python pipeline code & names
        std::string _pyCode;
        std::string _pyPipelineName;
        std::string _writerFuncName;
        bool _updateInputExceptions;

        std::shared_ptr<ResultSet> emptyResultSet() const;

        std::shared_ptr<JITSymbols> _syms;
        InitData _initData; // pointer to pass to init...
        HashResult _hashResult; // where to store hash result (i.e. write to hash table)
        // Todo: move this to physicalplan!!!
        //void pushDownOutputLimit(); //! enable optimizations for limited pipeline by restricting input read!

        // unresolved exceptions. Important i.e. when no IO interleave is used...
        std::vector<Partition*> _inputExceptions;
        std::unordered_map<std::string, ExceptionInfo> _partitionToExceptionsMap;


        // for hash output, the key and bucket type
        python::Type _hashOutputKeyType;
        python::Type _hashOutputBucketType;

        bool hasOutputLimit() const {
            return _outputLimit < std::numeric_limits<size_t>::max();
        }
    };
}
#endif //TUPLEX_TRANSFORMSTAGE_H