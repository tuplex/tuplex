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
#include <physical/memory/Partition.h>
#include <physical/memory/PartitionGroup.h>
#include <utils/ExceptionInfo.h>
#include "physical/PhysicalStage.h"
#include "jit/LLVMOptimizer.h"
#include <logical/ParallelizeOperator.h>
#include <logical/MapOperator.h>
#include <logical/MapColumnOperator.h>
#include <logical/WithColumnOperator.h>
#include <logical/FilterOperator.h>
#include <logical/TakeOperator.h>
#include <logical/FileInputOperator.h>
#include <logical/IgnoreOperator.h>
#include <logical/ResolveOperator.h>
#include <jit/JITCompiler.h>
#include <physical/codegen/CSVParserGenerator.h>
#include <mt/ThreadPool.h>
#include <limits>
#include <logical/Defs.h>
#include <logical/FileOutputOperator.h>
#include <logical/AggregateOperator.h>

#ifdef BUILD_WITH_AWS
// include protobuf serialization of TrafoStage for Lambda executor
#include <Lambda.pb.h>
#endif

namespace tuplex {

#ifdef BUILD_WITH_AWS
    inline FileFormat proto_toFileFormat(messages::FileFormat fmt) {
        switch(fmt) {
            case messages::FileFormat::FF_CSV:
                return FileFormat::OUTFMT_CSV;
            case messages::FileFormat::FF_TEXT:
                return FileFormat::OUTFMT_TEXT;
            case messages::FileFormat::FF_TUPLEX:
                return FileFormat::OUTFMT_TUPLEX;
            case messages::FileFormat::FF_JSON:
                return FileFormat::OUTFMT_JSON;
            default:
                return FileFormat::OUTFMT_UNKNOWN;
        }
    }
#endif

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
        friend void hyperspecialize(TransformStage *stage, const URI& uri, size_t file_size, double nc_threshold);

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
         * set stage's general case normalPartitions
         * @param generalPartitions
         */
        void setGeneralPartitions(const std::vector<Partition*>& generalPartitions) { _generalPartitions = generalPartitions; }

        /*!
         * get stage's general case normalPartitions
         * @return
         */
        std::vector<Partition*> generalPartitions() const { return _generalPartitions; }

        /*!
         * set stage's fallback normalPartitions as serialized python objects
         * @param fallbackPartitions
         */
        void setFallbackPartitions(const std::vector<Partition*>& fallbackPartitions) { _fallbackPartitions = fallbackPartitions; }

        /*!
         * get fallback normalPartitions as serialized python objects
         * @return
         */
        std::vector<Partition*> fallbackPartitions() const { return _fallbackPartitions; }

        /*!
         * set merge information for each set of normal, fallback, and general partitions
         * @param partitionGroups
         */
        void setPartitionGroups(const std::vector<PartitionGroup>& partitionGroups) {
            _partitionGroups = partitionGroups;
        }

        /*!
         * get partition groups for all sets of partitions
         */
         std::vector<PartitionGroup> partitionGroups() const { return _partitionGroups; }

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

        Schema readSchema() const { return _generalCaseReadSchema; }
        Schema outputSchema() const { return _generalCaseOutputSchema; }
        Schema inputSchema() const { return _generalCaseInputSchema; }
        Schema normalCaseOutputSchema() const { return _normalCaseOutputSchema; }
        Schema normalCaseInputSchema() const { return _normalCaseInputSchema; }

        /*!
         * from original columns, get indices back which columns are read in each case. To get number of columns, use inputColumnCount
         */
        std::vector<unsigned> normalCaseInputColumnsToKeep() const { return _normalCaseColumnsToKeep; }
        std::vector<unsigned> generalCaseInputColumnsToKeep() const { return _generalCaseColumnsToKeep; }
        size_t inputColumnCount() const { return readSchema().getRowType().parameters().size(); }
        int64_t outputDataSetID() const { return _outputDataSetID; }
        std::unordered_map<std::string, std::string> outputOptions() const;

        int64_t fileInputOperatorID() const { assert(_inputMode == EndPointMode::FILE); return _inputNodeID; }

        /*!
         * when trying to resolve exception only certain operators may have exceptions. To avoid
         * costly reprocessing, check operatorIDs.
         * @return vector of operator ID where resolvers exist (i.e. the ones which need to be forced on the slow path)
         */
        std::vector<int64_t> operatorIDsWithResolvers() const { return _operatorIDsWithResolvers; }

        URI outputURI() const { return _outputURI; }


        // HACK:
        inline std::vector<std::tuple<std::string, size_t>> input_files() const {
            if(_inputMode != EndPointMode::FILE)
                return {};

            auto fileSchema = Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::STRING, python::Type::I64}));

            std::vector<std::tuple<std::string, size_t>> v;
            for(auto partition : inputPartitions()) {
                // get num
                auto numFiles = partition->getNumRows();
                const uint8_t *ptr = partition->lock();
                size_t bytesRead = 0;
                // found
                for (int i = 0; i < numFiles; ++i) {
                    // found file -> create task / split into multiple tasks
                    Row row = Row::fromMemory(fileSchema, ptr, partition->capacity() - bytesRead);
                    URI uri(row.getString(0));
                    size_t file_size = row.getInt(1);
                    v.emplace_back(uri.toString(), file_size);
                }
                partition->unlock();
            }


            return v;
        }


        /*!
         * fetch data into resultset
         * @return resultset of this stage
         */
        std::shared_ptr<ResultSet> resultSet() const override { return _rs;}

        void setMemoryResult(const std::vector<Partition*>& normalPartitions=std::vector<Partition*>{},
                             const std::vector<Partition*>& generalPartitions=std::vector<Partition*>{},
                             const std::vector<Partition*>& fallbackPartitions=std::vector<Partition*>{},
                             const std::vector<PartitionGroup>& partitionGroups=std::vector<PartitionGroup>{},
                             const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& exceptionCounts=std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>()); // creates local result set?

        void setFileResult(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts); // creates empty result set with exceptions

        void setEmptyResult() {
            std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> ecounts;
            if(fileOutputMode())
                setFileResult(ecounts);
            else
                setMemoryResult(
                        std::vector<Partition*>(),
                        std::vector<Partition*>(),
                        std::vector<Partition*>(),
                        std::vector<PartitionGroup>(),
                        ecounts);
        }

        std::string fastPathBitCode() const {
            return _fastCodePath.irBitCode;//_fastPathIRBitCode;
        }

        std::string slowPathBitCode() const {
            return _slowCodePath.irBitCode;//_slowPathIRBitCode;
        }

        std::string& fastPathBitCode() {
            return _fastCodePath.irBitCode;//_fastPathIRBitCode;
        }

        std::string& slowPathBitCode() {
            return _slowCodePath.irBitCode;//_slowPathIRBitCode;
        }

        Schema generalCaseInputSchema() const {
            return this->_generalCaseInputSchema;
        }

        // Retrieve fast path IR code
        std::string fastPathCode() const {
            if(fastPathBitCode().empty())
                return "";

            // parse bit code & convert
            llvm::LLVMContext ctx;
            auto mod = codegen::bitCodeToModule(ctx, fastPathBitCode());
            if(!mod)
                return "";
            return codegen::moduleToString(*mod.get());
        }

        // Retrieve slow path IR code
        std::string slowPathCode() const {
            if(slowPathBitCode().empty())
                return "";

            // parse bit code & convert
            llvm::LLVMContext ctx;
            auto mod = codegen::bitCodeToModule(ctx, slowPathBitCode());
            if(!mod)
                return "";
            return codegen::moduleToString(*mod.get());
        }

        std::string funcName() const { return _fastCodePath.funcStageName; }
        //std::string writerFuncName() const { throw std::runtime_error("is writer func actually used?"); return ""; } //return _fastCodePath._writerFuncName; }
        std::string writeMemoryCallbackName() const { return _fastCodePath.writeMemoryCallbackName; }
        std::string writeFileCallbackName() const { return _fastCodePath.writeFileCallbackName; }
        std::string writeHashCallbackName() const { return _fastCodePath.writeHashCallbackName; }
        std::string exceptionCallbackName() const { return _fastCodePath.writeExceptionCallbackName; }
        std::string aggCombineCallbackName() const { return _fastCodePath.writeAggregateCallbackName; }

        // std::string resolveCode() const { return _irResolveCode; }
        std::string resolveRowName() const { return _slowCodePath.funcStageName; } //@TODO rename???
        std::string resolveWriteCallbackName() const { return _slowCodePath.writeMemoryCallbackName; }
        std::string resolveHashCallbackName() const { return _slowCodePath.writeHashCallbackName; }
        std::string resolveExceptionCallbackName() const { return _slowCodePath.writeExceptionCallbackName; }

        std::string purePythonCode() const { return _pyCode; }
        std::string pythonPipelineName() const { return _pyPipelineName; }

        std::string purePythonAggregateCode() const { return _pyAggregateCode; }
        std::string pythonAggregateFunctionName() const { return _pyAggregateFunctionName; }

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

        UDF outputPathUDF() const { return UDF(_fileOutputParameters.at("udf")); }

        FileFormat outputFormat() const { return _outputFormat; }
        FileFormat inputFormat() const { return _inputFormat; }
        void execute(const Context& context) override;

        // JITSymbols for this stage
        struct JITSymbols {
            struct CodePath {
                codegen::init_stage_f initStageFunctor;
                codegen::release_stage_f releaseStageFunctor;

                CodePath() : initStageFunctor(nullptr), releaseStageFunctor(nullptr) {}
            };

            CodePath _fastCodePath;
            CodePath _slowCodePath;

            codegen::read_block_f functor; // can be memory2memory or file2memory
            codegen::read_block_exp_f functorWithExp;
            codegen::resolve_f resolveFunctor; // always memory2memory

            // @TODO: clean this up for different code-paths... buggy!
            codegen::agg_init_f aggInitFunctor;
            codegen::agg_combine_f aggCombineFunctor;
            codegen::agg_agg_f aggAggregateFunctor;

            JITSymbols() : functor(nullptr),
                           functorWithExp(nullptr),
                           writeFunctor(nullptr),
                           resolveFunctor(nullptr),
                           _fastCodePath(),
                           _slowCodePath(),
                           aggInitFunctor(nullptr),
                           aggCombineFunctor(nullptr),
                           aggAggregateFunctor(nullptr) {}


           inline void update(const std::shared_ptr<JITSymbols>& syms) {
                if(!syms)
                    return;

                // update if other is not null
                if(syms->_fastCodePath.initStageFunctor)
                    _fastCodePath.initStageFunctor = syms->_fastCodePath.initStageFunctor;
               if(syms->_fastCodePath.releaseStageFunctor)
                   _fastCodePath.releaseStageFunctor = syms->_fastCodePath.releaseStageFunctor;

               if(syms->_slowCodePath.initStageFunctor)
                   _slowCodePath.initStageFunctor = syms->_slowCodePath.initStageFunctor;
               if(syms->_slowCodePath.releaseStageFunctor)
                   _slowCodePath.releaseStageFunctor = syms->_slowCodePath.releaseStageFunctor;

               if(syms->functor)
                   functor = syms->functor;
               if(syms->functorWithExp)
                   functorWithExp = syms->functorWithExp;
               if(syms->resolveFunctor)
                   resolveFunctor = syms->resolveFunctor;
               if(syms->aggInitFunctor)
                   aggInitFunctor = syms->aggInitFunctor;
               if(syms->aggAggregateFunctor)
                   aggAggregateFunctor = syms->aggAggregateFunctor;
            }
        };

        // HACK!!!
        bool use_hyper() const { return !_encodedData.empty(); }

        /*!
         * compile code of this stage via LLVM JIT
         * @param jit JIT instance
         * @param optimizer optional instance of optimizer to run over code first. No optimization run when set to 0
         * @param excludeSlowPath if true, only fast path functions are compiled. This helps to have threads already busy when slow path still need to get compiled.
         * @param registerSymbols whether to add task symbols to JIT compiler
         * @return a struct of all function pointers
         */
        std::shared_ptr<JITSymbols> compile(JITCompiler& jit, LLVMOptimizer *optimizer=nullptr, bool excludeSlowPath=false, bool registerSymbols=true);

        std::shared_ptr<JITSymbols> compileSlowPath(JITCompiler& jit, LLVMOptimizer *optimizer=nullptr, bool registerSymbols=true);
        std::shared_ptr<JITSymbols> compileFastPath(JITCompiler& jit, LLVMOptimizer *optimizer=nullptr, bool registerSymbols=true);

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

        inline std::shared_ptr<JITSymbols> jitsyms() const {
            return _syms;
        }

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

        // restructuring code-paths
        struct StageCodePath {
            enum class Type {
                UNKNOWN_PATH,
                FAST_PATH,
                SLOW_PATH
            };

            Type type;
            std::string irBitCode; //! llvm IR bitcode for fast/slow path
            std::string initStageFuncName;  //! init function for a stage (sets up globals & Co) fast/slow path
            std::string releaseStageFuncName; //! release function for a stage (releases globals & Co) fast/slow path

            std::string funcStageName; //! llvm function name of the stage function fast/slow path
            std::string funcProcessRowName; //! llvm function to process single row fast? slow path

            std::string writeFileCallbackName;
            std::string writeMemoryCallbackName;
            std::string writeHashCallbackName;
            std::string writeExceptionCallbackName;
            std::string writeAggregateCallbackName;

            std::string aggregateInitFuncName; //! to initiate aggregate (allocates via C-malloc)
            std::string aggregateCombineFuncName; //! to combine two aggregates (allocates & frees via C-malloc).
            std::string aggregateAggregateFuncName; //! to combine aggregate and result (allocates & frees via C-malloc).

            StageCodePath() = default;

            inline bool empty() const {
                return irBitCode.empty();
            }

            // protobuf serialization
#ifdef BUILD_WITH_AWS

            StageCodePath(const messages::CodePath& p) : irBitCode(p.irbitcode()), initStageFuncName(p.initstagefuncname()),
            releaseStageFuncName(p.releasestagefuncname()), funcStageName(p.funcstagename()),
            funcProcessRowName(p.funcprocessrowname()), writeFileCallbackName(p.writefilecallbackname()),
            writeMemoryCallbackName(p.writememorycallbackname()), writeHashCallbackName(p.writehashcallbackname()),
            writeExceptionCallbackName(p.writeexceptioncallbackname()), writeAggregateCallbackName(p.writeaggregatecallbackname()),
            aggregateInitFuncName(p.aggregateinitfuncname()), aggregateCombineFuncName(p.aggregatecombinefuncname()),
            aggregateAggregateFuncName(p.aggregateaggregatefuncname()) {
            }

            inline void fill(messages::CodePath* c) const {
                if(!c)
                    return;
                c->set_irbitcode(irBitCode); // DO NOT USE c_str() here!
                c->set_initstagefuncname(initStageFuncName.c_str());
                c->set_releasestagefuncname(releaseStageFuncName.c_str());

                c->set_funcstagename(funcStageName.c_str());
                c->set_funcprocessrowname(funcProcessRowName.c_str());

                c->set_writefilecallbackname(writeFileCallbackName.c_str());
                c->set_writememorycallbackname(writeMemoryCallbackName.c_str());
                c->set_writehashcallbackname(writeHashCallbackName.c_str());
                c->set_writeexceptioncallbackname(writeExceptionCallbackName.c_str());
                c->set_writeaggregatecallbackname(writeAggregateCallbackName.c_str());

                c->set_aggregateinitfuncname(aggregateInitFuncName.c_str());
                c->set_aggregatecombinefuncname(aggregateCombineFuncName.c_str());
                c->set_aggregateaggregatefuncname(aggregateAggregateFuncName.c_str());
            }

            inline messages::CodePath* to_protobuf() const {
                auto c = new messages::CodePath();
                fill(c);
                return c;
            }
#endif
        };


//        struct TransformStageCodePath {
//            // Fast path variables
////            std::string _fastPathIRBitCode; //! llvm IR bitcode for fast path
////            std::string _fastPathInitStageFuncName; //! init function for a stage (sets up globals & Co)
////            std::string _fastPathReleaseStageFuncName; //! release function for a stage (releases globals & Co)
//
////            std::string _funcStageName; //! llvm function name of the stage function
//            std::string _funcMemoryWriteCallbackName; //! llvm function name of the write callback
//            std::string _funcFileWriteCallbackName; //! llvm function name of the write callback used for file output.
//            std::string _funcExceptionCallback; //! llvm function of the exception callback function
//            std::string _funcHashWriteCallbackName; //! the function to call when saving to hash table
////            std::string _aggregateInitFuncName; //! to initiate aggregate (allocates via C-malloc)
////            std::string _aggregateCombineFuncName; //! to combine two aggregates (allocates & frees via C-malloc).
//
//            std::string _writerFuncName;
//            std::string _aggregateCallbackName; //! the callback to call with an aggregate
//
//            // Slow path variables
////            std::string _slowPathIRBitCode; //! llvm IR bitcode for slow path
////            std::string _slowPathInitStageFuncName; //! init function for a stage (sets up globals & Co)
////            std::string _slowPathReleaseStageFuncName; //! release function for a stage (releases globals & Co)
//
////            std::string _resolveRowFunctionName;
//            std::string _resolveRowWriteCallbackName;
//            std::string _resolveRowExceptionCallbackName;
//            std::string _resolveHashCallbackName;
//
////            // Common variables
////            std::string _aggregateAggregateFuncName; //! to combine aggregate and result (allocates & frees via C-malloc).
//        };

//        TransformStageCodePath _fastCodePath;
//        TransformStageCodePath _slowCodePath;

        StageCodePath _fastCodePath;
        StageCodePath _slowCodePath;
        EndPointMode _inputMode; //! indicate whether stage takes hash table, serialized mem or files as input
        EndPointMode _outputMode; //! analog


        // Key-Value map for file format specific configuration options
        std::unordered_map<std::string, std::string> _fileInputParameters;
        std::unordered_map<std::string, std::string> _fileOutputParameters;

        std::vector<std::string> _inputColumns;
        std::vector<std::string> _outputColumns;
        Schema _generalCaseReadSchema;
        Schema _generalCaseInputSchema;
        Schema _generalCaseOutputSchema;
        int64_t _outputDataSetID;
        int64_t _inputNodeID;
        std::vector<unsigned> _normalCaseColumnsToKeep;
        std::vector<unsigned> _generalCaseColumnsToKeep;
//        std::vector<bool> _inputColumnsToKeep;
        FileFormat _inputFormat;
        FileFormat _outputFormat;
        Schema _normalCaseOutputSchema;
        Schema _normalCaseInputSchema;
        bool _persistSeparateCases;
        AggregateType _aggMode;

        std::string _encodedData; // HACK: encoded bytes to represent stage

        std::vector<int64_t> _operatorIDsWithResolvers;

        std::vector<Partition*> _inputPartitions; //! memory input partitions for this task.
        size_t                  _inputLimit; //! limit number of input rows (inf per default)
        size_t                  _outputLimit; //! output limit, set e.g. by take, to_csv etc. (inf per default)
        std::vector<Partition*> _generalPartitions; //! general case input partitions
        std::vector<Partition*> _fallbackPartitions; //! fallback case input partitions
        std::vector<PartitionGroup> _partitionGroups; //! groups partitions together for correct row indices

        std::shared_ptr<ResultSet> _rs; //! result set

        URI _outputURI; //! the output uri for file mode of this stage

        // pure python pipeline code & names
        std::string _pyCode;
        std::string _pyPipelineName;
        std::string _pyAggregateCode;
        std::string _pyAggregateFunctionName;
        std::string _writerFuncName;
        bool _updateInputExceptions;

        std::shared_ptr<ResultSet> emptyResultSet() const;

        std::shared_ptr<JITSymbols> _syms;
        InitData _initData; // pointer to pass to init...
        HashResult _hashResult; // where to store hash result (i.e. write to hash table)
        // Todo: move this to physicalplan!!!
        //void pushDownOutputLimit(); //! enable optimizations for limited pipeline by restricting input read!

        // for hash output, the key and bucket type
        python::Type _hashOutputKeyType;
        python::Type _hashOutputBucketType;

        bool hasOutputLimit() const {
            return _outputLimit < std::numeric_limits<size_t>::max();
        }
    };
}
#endif //TUPLEX_TRANSFORMSTAGE_H