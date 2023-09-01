//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 11/22/2021                                                               //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifndef TUPLEX_WORKERAPP_H
#define TUPLEX_WORKERAPP_H

#include <string>
#include <Base.h>

// error codes
#define WORKER_OK 0
#define WORKER_ERROR_INVALID_JSON_MESSAGE 100
#define WORKER_ERROR_NO_PYTHON_HOME 101
#define WORKER_ERROR_NO_TUPLEX_RUNTIME 102
#define WORKER_ERROR_INVALID_URI 103
#define WORKER_ERROR_COMPILATION_FAILED 104
#define WORKER_ERROR_STAGE_INITIALIZATION 105
#define WORKER_ERROR_STAGE_CLEANUP 106
#define WORKER_ERROR_UNSUPPORTED_INPUT 107
#define WORKER_ERROR_IO 108
#define WORKER_ERROR_EXCEPTION 109
#define WORKER_ERROR_PIPELINE_FAILED 110
#define WORKER_ERROR_UNKNOWN_MESSAGE 111
#define WORKER_ERROR_GLOBAL_INIT 112
#define WORKER_ERROR_MISSING_PYTHON_CODE 113
#define WORKER_ERROR_INCOMPATIBLE_AST_FORMAT 114

// give 32MB standard buf size, 8MB for exceptions and hash
#define WORKER_DEFAULT_BUFFER_SIZE 33554432
#define WORKER_EXCEPTION_BUFFER_SIZE 8388608
#define WORKER_HASH_BUFFER_SIZE 8388608

// protobuf
#include <Lambda.pb.h>
#include <physical/execution/TransformStage.h>
#include <physical/execution/CSVReader.h>
#include <physical/execution/TextReader.h>
#include <google/protobuf/util/json_util.h>

#ifdef BUILD_WITH_AWS
#include <aws/core/Aws.h>
#endif

#include <Serializer.h>
#include <FilePart.h>
#include "JSONUtils.h"

// Notes: For fileoutput, sometimes a reorg step might be necessary!
// e.g., when using S3 file-system could upload 5TB per object.
// => need to carefully check limits. Currently, maximum of 1,000 parts possible (largest size 5TB!)
// -> check here https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
// could do distributed S3 upload!


namespace tuplex {

    // consider https://codereview.stackexchange.com/questions/113439/copyable-atomic

    struct CodePathStatistics {
        std::atomic<int64_t> inputRowCount;
        std::atomic<int64_t> rowsOnNormalPathCount;
        std::atomic<int64_t> rowsOnGeneralPathCount;
        std::atomic<int64_t> rowsOnInterpreterPathCount;
        std::atomic<int64_t> unresolvedRowsCount;

        void reset() {
            inputRowCount = 0;
            rowsOnNormalPathCount = 0;
            rowsOnGeneralPathCount = 0;
            rowsOnInterpreterPathCount = 0;
            unresolvedRowsCount = 0;
        }

        CodePathStatistics() {
            reset();
        }

        CodePathStatistics(const CodePathStatistics& other) : inputRowCount(other.inputRowCount.load()),
        rowsOnNormalPathCount(other.rowsOnNormalPathCount.load()),
        rowsOnGeneralPathCount(other.rowsOnGeneralPathCount.load()),
        rowsOnInterpreterPathCount(other.rowsOnInterpreterPathCount.load()),
        unresolvedRowsCount(other.unresolvedRowsCount.load()) {}

        CodePathStatistics& operator = (const CodePathStatistics& other) {
            inputRowCount = other.inputRowCount.load();
            rowsOnNormalPathCount = other.rowsOnNormalPathCount.load();
            rowsOnGeneralPathCount = other.rowsOnGeneralPathCount.load();
            rowsOnInterpreterPathCount = other.rowsOnInterpreterPathCount.load();
            unresolvedRowsCount = other.unresolvedRowsCount.load();
            return *this;
        }
    };

    struct MessageStatistic {
        double totalTime;
        size_t numNormalOutputRows;
        size_t numExceptionOutputRows;
        CodePathStatistics codePathStats;
    };

    /// settings to use to initialize a worker application. Helpful to tune depending on
    /// deployment target.
    struct WorkerSettings {
        // Settings:
        // -> thread-pool, how many threads to use for tasks!
        // -> local file cache -> how much main memory, which disk dir, how much memory available on disk dir
        // executor in total how much memory available to use
        //

        size_t numThreads; //! how many threads to use (1 == single-threaded)

        size_t normalBufferSize; //! how many bytes to use for storing rows before flushing them out
        size_t exceptionBufferSize; //! how many bytes to sue for storing rows as exceptions before flushing them out
        size_t hashBufferSize; //! how many bytes to use for hashmap before flushing it out

        URI spillRootURI; //! where to store spill over files


        // ContextOption dependent variables...
        size_t runTimeMemory;
        size_t runTimeMemoryDefaultBlockSize;

        bool allowNumericTypeUnification;
        bool useInterpreterOnly;
        bool useCompiledGeneralPath;
        bool useFilterPromotion;
        bool useConstantFolding;

        bool opportuneGeneralPathCompilation; // <-- whether to kick off query optimization as early on as possible
        size_t s3PreCacheSize; // <-- whether to load S3 first and activate cache
        bool useOptimizer; // <-- whether to use optimizer for hyper or not
        size_t samplingSize; // how many mb/kb to use for sampling.
        size_t sampleLimitCount; // <-- limit count imposed on resampling
        size_t strataSize;
        size_t samplesPerStrata;

        double normalCaseThreshold; ///! used for hyperspecialziation
        codegen::ExceptionSerializationMode exceptionSerializationMode;

        size_t specializationUnitSize;

        bool useObjectFileAsInterchangeFormat;

        // use some defaults...
        WorkerSettings() : numThreads(1), normalBufferSize(WORKER_DEFAULT_BUFFER_SIZE),
        exceptionBufferSize(WORKER_EXCEPTION_BUFFER_SIZE), hashBufferSize(WORKER_HASH_BUFFER_SIZE),
        useInterpreterOnly(false), useCompiledGeneralPath(true),
        opportuneGeneralPathCompilation(true),
        useFilterPromotion(false), useConstantFolding(false),
        s3PreCacheSize(0),
        exceptionSerializationMode(codegen::ExceptionSerializationMode::SERIALIZE_AS_GENERAL_CASE),
        specializationUnitSize(0),
        samplingSize(0), useObjectFileAsInterchangeFormat(false) {

            // set some options from defaults...
            auto opt = ContextOptions::defaults();
            runTimeMemory = opt.RUNTIME_MEMORY();
            runTimeMemoryDefaultBlockSize = opt.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE();
            allowNumericTypeUnification = opt.AUTO_UPCAST_NUMBERS();
            normalCaseThreshold = opt.NORMALCASE_THRESHOLD();
            sampleLimitCount = std::numeric_limits<size_t>::max();
            strataSize = 1;
            samplesPerStrata = 1;
            samplingSize = opt.AWS_LAMBDA_SAMPLE_MAX_DETECTION_MEMORY();
        }

        WorkerSettings(const WorkerSettings& other) : numThreads(other.numThreads),
        normalBufferSize(other.normalBufferSize),
        exceptionBufferSize(other.exceptionBufferSize),
        hashBufferSize(other.hashBufferSize),
        spillRootURI(other.spillRootURI),
        runTimeMemory(other.runTimeMemory),
        runTimeMemoryDefaultBlockSize(other.runTimeMemoryDefaultBlockSize),
        allowNumericTypeUnification(other.allowNumericTypeUnification),
        useInterpreterOnly(other.useInterpreterOnly),
        useCompiledGeneralPath(other.useCompiledGeneralPath),
        useFilterPromotion(other.useFilterPromotion),
        useConstantFolding(other.useConstantFolding),
        opportuneGeneralPathCompilation(other.opportuneGeneralPathCompilation),
        s3PreCacheSize(other.s3PreCacheSize),
        useOptimizer(other.useOptimizer),
        sampleLimitCount(other.sampleLimitCount),
        normalCaseThreshold(other.normalCaseThreshold),
        exceptionSerializationMode(other.exceptionSerializationMode),
        strataSize(other.strataSize),
        samplesPerStrata(other.samplesPerStrata),
        specializationUnitSize(other.specializationUnitSize),
        samplingSize(other.samplingSize),
        useObjectFileAsInterchangeFormat(other.useObjectFileAsInterchangeFormat) {}

        inline bool operator == (const WorkerSettings& other) const {

            // compare variables...
            if(numThreads != other.numThreads)
                return false;
            if(normalBufferSize != other.normalBufferSize)
                return false;
            if(exceptionBufferSize != other.exceptionBufferSize)
                return false;
            if(hashBufferSize != other.hashBufferSize)
                return false;
            if(spillRootURI != other.spillRootURI)
                return false;
            if(runTimeMemory != other.runTimeMemory)
                return false;
            if(runTimeMemoryDefaultBlockSize != other.runTimeMemoryDefaultBlockSize)
                return false;
            if(allowNumericTypeUnification != other.allowNumericTypeUnification)
                return false;
            if(useInterpreterOnly != other.useInterpreterOnly)
                return false;
            if(useCompiledGeneralPath != other.useCompiledGeneralPath)
                return false;
            if(opportuneGeneralPathCompilation != other.opportuneGeneralPathCompilation)
                return false;
            if(useOptimizer != other.useOptimizer)
                return false;
            if(sampleLimitCount != other.sampleLimitCount)
                return false;
            if(useFilterPromotion != other.useFilterPromotion)
                return false;
            if(useConstantFolding != other.useConstantFolding)
                return false;
            if(!double_eq(normalCaseThreshold, other.normalCaseThreshold))
                return false;
            if(s3PreCacheSize != other.s3PreCacheSize)
                return false;
            if(exceptionSerializationMode != other.exceptionSerializationMode)
                return false;

            if(strataSize != other.strataSize)
                return false;
            if(samplesPerStrata != other.samplesPerStrata)
                return false;
            if(specializationUnitSize != other.specializationUnitSize)
                return false;

            if(samplingSize != other.samplingSize)
                return false;

            if(useObjectFileAsInterchangeFormat != other.useObjectFileAsInterchangeFormat)
                return false;

            return true;
        }

        bool operator != (const WorkerSettings& other) const {
            return !(*this == other);
        }
    };

    inline std::ostream& operator << (std::ostream& os, const WorkerSettings& ws) {
        os << "{";
        os << "\"numThreads\":"<<ws.numThreads<<", ";
        os << "\"normalBufferSize\":"<<ws.normalBufferSize<<", ";
        os << "\"exceptionBufferSize\":"<<ws.exceptionBufferSize<<", ";
        os << "\"hashBufferSize\":"<<ws.hashBufferSize<<", ";
        os << "\"spillRootURI\":"<<escape_json_string(ws.spillRootURI.toPath())<<", ";
        os << "\"runTimeMemory\":"<<ws.runTimeMemory<<", ";
        os << "\"runTimeMemoryDefaultBlockSize\":"<<ws.runTimeMemoryDefaultBlockSize<<", ";
        os << "\"allowNumericTypeUnification\":"<<boolToString(ws.allowNumericTypeUnification)<<", ";
        os << "\"useInterpreterOnly\":"<<boolToString(ws.useInterpreterOnly)<<", ";
        os << "\"useCompiledGeneralPath\":"<<boolToString(ws.useCompiledGeneralPath)<<", ";
        os << "\"opportuneGeneralPathCompilation\":"<<boolToString(ws.opportuneGeneralPathCompilation)<<", ";
        os << "\"useOptimizer\":"<<boolToString(ws.useOptimizer)<<", ";
        os << "\"sampleLimitCount\":"<<ws.sampleLimitCount<<", ";
        os << "\"strataSize\":"<<ws.strataSize<<", ";
        os << "\"sampleSize\":"<<ws.samplingSize<<", ";
        os << "\"samplesPerStrata\":"<<ws.samplesPerStrata<<", ";
        os << "\"useFilterPromotion\":"<<boolToString(ws.useFilterPromotion)<<", ";
        os << "\"useConstantFolding\":"<<boolToString(ws.useConstantFolding)<<", ";
        os << "\"normalCaseThreshold\":"<<ws.normalCaseThreshold<<", ";
        os << "\"exceptionSerializationMode\":"<<(int)ws.exceptionSerializationMode<<", ";
        os << "\"s3PreCacheSize\":"<<ws.s3PreCacheSize<<",";
        os << "\"specializationUnitSize\":"<<ws.specializationUnitSize<<",";
        os << "\"useObjectFileAsInterchangeFormat\":"<<boolToString(ws.useObjectFileAsInterchangeFormat);
        os << "}";
        return os;
    }

    /// main class to represent a running worker application
    /// i.e., this is an applicaton which performs some task and returns it in some way
    /// exchange could be via request response, files, shared memory? etc.
    class WorkerApp {
    public:
        WorkerApp() : _logger(Logger::instance().logger("worker")), _threadEnvs(nullptr),
        _numThreads(0), _globallyInitialized(false), _syms(std::shared_ptr<TransformStage::JITSymbols>()),
        _messageCount(0), _inputOperatorID(-1) {
#ifdef BUILD_WITH_CEREAL
            // init type system for decoding
            auto sym_table = SymbolTable::createFromEnvironment(nullptr);
#endif
            // set default reader size to 16MB
            _readerBufferSize = 16 * 1024 * 1024;
        }
        WorkerApp(const WorkerApp& other) =  delete;

        // create WorkerApp from settings
        WorkerApp(const WorkerSettings& settings) : WorkerApp() {

            // set default reader size to 16MB
            _readerBufferSize = 16 * 1024 * 1024;

            // originally a larger buffer was used... -> way too much.
            // _readerBufferSize = 128 * 1024 * 1000; // test
        }

        virtual ~WorkerApp();
        bool reinitialize(const WorkerSettings& settings);

        int messageLoop();

        /*!
         * processes a single message given as JSON
         * @param message JSON string
         * @return 0 if successful or error code depending on circumstances
         */
        int processJSONMessage(const std::string& message);

        void shutdown();

        bool isInitialized() const;

        virtual int globalInit(bool skip=false);

        inline std::string jsonStats() const {
            return _lastStat;
        }
    protected:

        std::vector<MessageStatistic> _statistics; // statistics per message

        size_t numProcessedMessages() const { return _messageCount; }

        tuplex::messages::InvocationRequest _currentMessage;
        virtual void storeInvokeRequest() {}

        void markTime(const std::string& label, double value) {
            _timeDict[label] = value;
        }

        inline int64_t inputOperatorID() const { return _inputOperatorID; }

        WorkerSettings settingsFromMessage(const tuplex::messages::InvocationRequest& req);

        virtual int processMessage(const tuplex::messages::InvocationRequest& req);

        /*!
         * fetch information about worker environment as single JSON message (right now LLVM information)
         * @return
         */
        virtual int processEnvironmentInfoMessage();

        int processTransformStage(TransformStage* tstage,
                                          const std::shared_ptr<TransformStage::JITSymbols>& syms,
                                          const std::vector<FilePart>& input_parts,
                                          const URI& output_uri);

        int processTransformStageInPythonMode(const TransformStage* tstage,
                                              const std::vector<FilePart>& input_parts,
                                              const URI& output_uri);

        tuplex::messages::InvocationResponse executeTransformTask(const TransformStage* tstage);

        std::vector<FilePart> partsFromMessage(const tuplex::messages::InvocationRequest& req, bool silent=false);

        std::shared_ptr<TransformStage::JITSymbols> compileTransformStage(TransformStage& stage, bool use_llvm_optimizer=false);


        inline std::unordered_map<std::tuple<int64_t, int64_t>, int64_t> exception_counts() const {
            std::unordered_map<std::tuple<int64_t, int64_t>, int64_t> m;
            for(unsigned i = 0; i < _numThreads; ++i) {
                for(const auto& keyval : _threadEnvs[i].exceptionCounts) {
                    m[keyval.first] += keyval.second;
                }
            }
            return m;
        }

        std::vector<URI> output_uris() const {
            return _output_uris; // return where data has been written to (for response)
        }

        // stage types
        python::Type _stage_normal_input_type;
        python::Type _stage_normal_output_type;
        python::Type _stage_general_input_type;
        python::Type _stage_general_output_type;

        python::Type normalCaseInputType() const { return _stage_normal_input_type; }
        python::Type normalCaseOutputType() const { return _stage_normal_output_type; }
        python::Type generalCaseInputType() const { return _stage_general_input_type; }
        python::Type generalCaseOutputType() const { return _stage_general_output_type; }

        // inherited variables
        WorkerSettings _settings;
        bool _globallyInitialized;
        std::shared_ptr<JITCompiler> _compiler;
        std::shared_ptr<JITCompiler> _fastCompiler; // compiler with faster compile settings/less optimizations.
        MessageHandler& _logger;
#ifdef BUILD_WITH_AWS
        Aws::SDKOptions _aws_options;
#endif


        void registerSymbolsFromStageWithCompilers(TransformStage& stage);

        void validateBitCode(const std::string& bitcode);

        inline bool has_python_resolver() const { return _has_python_resolver; }

        // cache for compiled stages (sometimes same IR gets send)
        std::unordered_map<std::string, std::shared_ptr<TransformStage::JITSymbols>> _compileCache;

        struct SpillInfo {
            std::string path;
            size_t num_rows;
            size_t file_size;
            size_t originalPartNo;
            bool isExceptionBuf;

            SpillInfo() : path(""), num_rows(0), file_size(0), originalPartNo(0), isExceptionBuf(false) {}
        };

        // 1MB growth constant (avoid to frequent reallocs)
#define DEFAULT_BUFFER_GROWTH_CONSTANT (1024 * 1024)

        // variables for each Thread
        struct ThreadEnv {
            size_t threadNo; //! which thread number
            WorkerApp* app; //! which app to use
            Buffer normalBuf; //! holds normal rows, can only hold rows of a single part row before spilling becomes active
            size_t normalOriginalPartNo; //! holds original part no for normal buffer
            size_t numNormalRows; //! how many normal rows
            Buffer exceptionBuf; //! holds exception rows
            size_t exceptionOriginalPartNo; //! holds original part no for exception buffer
            size_t numExceptionRows; //! how many exception rows
            std::unordered_map<std::tuple<int64_t, int64_t>, int64_t> exceptionCounts; //! count how many exceptions of which type occur
            void* hashMap; //! for hash output
            uint8_t* nullBucket;
            size_t hashOriginalPartNo; //! original part No
            std::vector<SpillInfo> spillFiles; //! files used for storing spilled buffers.

            // for debugging purposes (can uncomment)
            std::vector<std::string> normalToGeneralExceptSample;
            std::unordered_map<int64_t, int64_t> normalToGeneralExceptCountPerEC;
            std::vector<std::string> generalToFallbackExceptSample;
            static const size_t MAX_EXCEPT_SAMPLE = 5;

            ThreadEnv() : threadNo(0), app(nullptr), hashMap(nullptr), nullBucket(nullptr),
                          numNormalRows(0), numExceptionRows(0),
                          normalBuf(DEFAULT_BUFFER_GROWTH_CONSTANT),
                          exceptionBuf(DEFAULT_BUFFER_GROWTH_CONSTANT) {}

          /*!
           * calculates how many bytes of storage the hashmap takes!
           * @return size in bytes
           */
            size_t hashMapSize() const;

            /*!
             * reset threadenv to original state, important for consistency.
             */
            void reset() {
                normalBuf.reset();
                normalOriginalPartNo = 0;
                numNormalRows = 0;
                exceptionBuf.reset();
                exceptionOriginalPartNo = 0;
                numExceptionRows = 0;
                exceptionCounts.clear();
                hashMap = hashmap_new();
                if(nullBucket)
                    free(nullBucket);
                nullBucket = nullptr; // empty bucket.
                hashOriginalPartNo = 0;
                spillFiles.clear();

                // sample debug
                normalToGeneralExceptSample.clear();
                normalToGeneralExceptCountPerEC.clear();
                generalToFallbackExceptSample.clear();
            }
        };

        // helper struct for storing info related to sorting buffers + spill files together...
        struct WriteInfo {
            bool use_buf; // whether to use buf OR spill info
            size_t partNo;
            size_t threadNo;
            size_t num_rows;

            // data...
            uint8_t *buf;
            size_t buf_size;
            SpillInfo spill_info;

            WriteInfo() : buf(nullptr), use_buf(true), num_rows(0), buf_size(0), partNo(0), threadNo(0) {}

            WriteInfo(const WriteInfo& other) : use_buf(other.use_buf), partNo(other.partNo), threadNo(other.threadNo), num_rows(other.num_rows),
            buf(other.buf), buf_size(other.buf_size), spill_info(other.spill_info) {}

            WriteInfo& operator = (const WriteInfo& other) {
                use_buf = other.use_buf;
                partNo = other.partNo;
                threadNo = other.threadNo;
                num_rows = other.num_rows;
                buf = other.buf;
                buf_size = other.buf_size;
                spill_info = other.spill_info;
                return *this;
            }
        };

        ThreadEnv *_threadEnvs;
        size_t _numThreads;
        std::atomic_int _numPythonExceptionsDisplayed;

        std::unordered_map<std::string, double> _timeDict;

        void initThreadEnvironments(size_t numThreads);

        void resetThreadEnvironments(); // resets all buffers in working sets.

        int64_t initTransformStage(const TransformStage::InitData& initData, const std::shared_ptr<TransformStage::JITSymbols>& syms);
        int64_t releaseTransformStage(const std::shared_ptr<TransformStage::JITSymbols>& syms);

        int64_t processSource(int threadNo, int64_t inputNodeID,
                              const FilePart& part,
                              const TransformStage* tstage,
                              const std::shared_ptr<TransformStage::JITSymbols>& syms,
                              size_t* inputRowCount);

        int64_t processSourceInPython(int threadNo, int64_t inputNodeID, const FilePart& part,
                                      const TransformStage* tstage, PyObject* pipelineObject,
                                      bool acquireGIL,
                                      size_t* inputRowCount);

        int64_t writeAllPartsToOutput(const URI& output_uri, const FileFormat& output_format, const std::unordered_map<std::string, std::string>& output_options);

        // this function needs to have the same signature as codegen::cell_rows_f
        // it internally calls the processCellsInPython function.
        struct PythonExecutionContext {
            WorkerApp *app;
            int threadNo;
            size_t numInputColumns;
            PyObject* pipelineObject;
            std::vector<PyObject*> py_intermediates;
            PythonExecutionContext(): app(nullptr), threadNo(0), numInputColumns(0), pipelineObject(nullptr) {}
        };

        static int64_t pythonCellFunctor(void* userData, int64_t row_number, char **cells, int64_t* cell_sizes);

        struct JsonContextData {
            void* userData;
            std::vector<std::string> columns; // which columns to pass along (and in which order!)
            bool unwrap_first_level;

            JsonContextData():userData(nullptr), unwrap_first_level(false) {}
        };

        JsonContextData _jsonContext;
        /*!
         * functor for parsing json data in fallback mode
         * @param jsonContext json Context (the var above)
         * @param row_number which initial row number to use
         * @param buffer char buffer containing json, can assume json starts from here
         * @param bufferSize buffer size
         * @return parsed bytes
         */
        static int64_t pythonJsonFunctor(void* jsonContext, char *buf, int64_t buf_size,  int64_t* out_normal_row_count, int64_t *out_bad_row_count, bool is_last_line);

        int64_t processCellsInPython(int threadNo,
                                     PyObject* pipelineObject,
                                     const std::vector<PyObject*>& py_intermediates,
                                     int64_t row_number,
                                     size_t num_cells,
                                     char **cells,
                                     int64_t* cell_sizes);

        /*!
         * performs out-of-order exception resolution of exceptions, and appends them simply to normal buffer.
         * @param threadNo for which thread to run resolution (only exceptions of that thread will be resolved
         * @param stage stage
         * @param syms symbols
         * @return err or success code
         */
        int64_t resolveOutOfOrder(int threadNo, TransformStage* stage, std::shared_ptr<TransformStage::JITSymbols> syms);

        int64_t resolveBuffer(int threadNo, Buffer& buf, size_t numRows,
                              const TransformStage* stage,
                              const std::shared_ptr<TransformStage::JITSymbols>& syms);

        std::string exceptRowToString(int64_t ecRowNumber, const ExceptionCode& ecCode,
                                      const uint8_t* ecBuf, size_t ecBufSize, const python::Type& general_case_input_type);

        void storeExceptSample(ThreadEnv* env, int64_t ecRowNumber, const ExceptionCode& ecCode,
                               const uint8_t* ecBuf, size_t ecBufSize, const python::Type& general_case_input_type);

        /*!
         * thread-safe logger function
         * @return message handler
         */
        virtual MessageHandler& logger() const { return _logger; }

        /*!
         * spill buffer out to somewhere & reset counter
         */
        virtual void spillNormalBuffer(size_t threadNo);
        virtual void spillExceptionBuffer(size_t threadNo);
        virtual void spillHashMap(size_t threadNo);

        URI outputURIFromReq(const messages::InvocationRequest &request);

        void writeBufferToFile(const URI& outputURI,
                               const FileFormat& fmt,
                               const uint8_t* buf,
                               const size_t buf_size,
                               const size_t num_rows,
                               const std::unordered_map<std::string, std::string>& output_options);

        void writePartsToFile(const URI& outputURI,
                              const FileFormat& fmt,
                              const std::vector<WriteInfo>& parts,
                              const std::unordered_map<std::string, std::string>& outOptions);

        URI getNextOutputURI(int threadNo, const URI& baseURI, bool isBaseURIFolder, const std::string& extension);

        virtual int64_t writeRow(size_t threadNo, const uint8_t* buf, int64_t bufSize);
        virtual void writeHashedRow(size_t threadNo, const uint8_t* key, int64_t key_size, bool bucketize, uint8_t* bucket, int64_t bucket_size);
        virtual void writeException(size_t threadNo, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t* input, int64_t dataLength);


    private:
        bool _has_python_resolver;
        CodePathStatistics _codePathStats; // stats per message
        python::Type _normalCaseRowType; // given normal-case row type
        python::Type _hyperspecializedNormalCaseRowType; // this can be incompatible with the given normal case type which in this case requires a conversion.
        bool _ncAndHyperNCIncompatible;
        std::vector<URI> _output_uris; // where data was actually written to.
        std::string _lastStat; // information about last invocation/request

        std::shared_ptr<TransformStage::JITSymbols> _syms;
        std::mutex _symsMutex;
        std::unique_ptr<std::thread> _resolverCompileThread;
        codegen::resolve_f getCompiledResolver(const TransformStage* stage);

        size_t _readerBufferSize;
        size_t _messageCount;

        int64_t _inputOperatorID;

        static int64_t writeRowCallback(ThreadEnv* env, const uint8_t* buf, int64_t bufSize);
        static void writeHashCallback(ThreadEnv* env, const uint8_t* key, int64_t key_size, bool bucketize, uint8_t* bucket, int64_t bucket_size);
        static void exceptRowCallback(ThreadEnv* env, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t* input, int64_t dataLength);

        // slow path callbacks
        static int64_t slowPathRowCallback(ThreadEnv *env, uint8_t* buf, int64_t bufSize);
        static void slowPathExceptCallback(ThreadEnv* env, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t *input, int64_t dataLength);

        inline bool useHyperSpecialization(const tuplex::messages::InvocationRequest& req) const {
            return req.stage().has_serializedstage() && !req.stage().serializedstage().empty() && req.inputuris_size() > 0;
        }

        std::string jsonStat(const tuplex::messages::InvocationRequest& req, TransformStage* stage) const;

        inline bool astFormatCompatible(const tuplex::messages::InvocationRequest& req) const {
            if(req.stage().has_stageserializationmode()) {
#ifdef BUILD_WITH_CEREAL
                if(req.stage().stageserializationmode() != messages::SF_CEREAL)
                    return false;
#else
                if(req.stage().stageserializationmode() != messages::SF_JSON)
                   return false;
#endif
            }
            return true;
        }

        inline std::tuple<size_t, size_t, size_t, size_t, size_t, size_t> get_row_stats(const TransformStage* tstage) const {
            // print out info
            size_t numNormalRows = 0;
            size_t numExceptionRows = 0;
            size_t numHashRows = 0;
            size_t normalBufSize = 0;
            size_t exceptionBufSize = 0;
            size_t hashMapSize = 0;
            for(unsigned i = 0; i < _numThreads; ++i) {
                numNormalRows += _threadEnvs[i].numNormalRows;
                numExceptionRows += _threadEnvs[i].numExceptionRows;
                normalBufSize += _threadEnvs[i].normalBuf.size();
                exceptionBufSize += _threadEnvs[i].exceptionBuf.size();
                hashMapSize += _threadEnvs[i].hashMapSize();
                for(auto info : _threadEnvs[i].spillFiles) {
                    if(info.isExceptionBuf)
                        numExceptionRows += info.num_rows;
                    else
                        numNormalRows += info.num_rows;
                }
            }
            if(tstage->outputMode() == EndPointMode::HASHTABLE) {
                numHashRows = numNormalRows;
                numNormalRows = 0;
            }

            return std::make_tuple(numNormalRows, numExceptionRows, numHashRows, normalBufSize, exceptionBufSize, hashMapSize);
        }

        inline size_t get_exception_row_count() const {
            size_t numExceptionRows = 0;
            for(unsigned i = 0; i < _numThreads; ++i) {
                numExceptionRows += _threadEnvs[i].numExceptionRows;
                for(auto info : _threadEnvs[i].spillFiles) {
                    if(info.isExceptionBuf)
                        numExceptionRows += info.num_rows;
                }
            }
            return numExceptionRows;
        }

        void preCacheS3(const std::vector<FilePart>& parts);
    };


    // helper function to process within gil a row using the fallback path...
    struct FallbackPathResult {
        int64_t code; // result code
        int64_t operatorID; // operator ID if code is not SUCCESS

        // rows can be stored multiple ways:
        // 1. converted to serialized representation (specialized_target_schema)
        Buffer buf;
        size_t bufRowCount;

        Buffer generalBuf;
        size_t generalBufCount;

        // 2. plain python objects
        std::vector<PyObject*> pyObjects;

        FallbackPathResult() : buf(4096),
                               bufRowCount(0),
                               generalBuf(4096),
                               generalBufCount(0),
                               code(ecToI64(ExceptionCode::SUCCESS)),
                               operatorID(0) {}

       FallbackPathResult(FallbackPathResult&& other)  noexcept : code(other.code), operatorID(other.operatorID),
       buf(std::move(other.buf)), bufRowCount(other.bufRowCount),
       generalBuf(std::move(other.generalBuf)), generalBufCount(other.generalBufCount), pyObjects(std::move(other.pyObjects)) {}

       FallbackPathResult(const FallbackPathResult& other) = delete;
        FallbackPathResult& operator = (const FallbackPathResult& other) = delete;
    };


    extern PyObject* fallbackTupleFromParseException(const uint8_t* buf, size_t buf_size);
    extern void processRowUsingFallback(FallbackPathResult& res,
                                        PyObject* func,
                                                      int64_t ecCode,
                                                      int64_t ecOperatorID,
                                                      const Schema& normal_input_schema,
                                                      const Schema& general_input_schema,
                                                      const uint8_t* buf,
                                                      size_t buf_size,
                                                      const Schema& specialized_target_schema,
                                                      const Schema& general_target_schema,
                                                      const std::vector<PyObject*>& py_intermediates,
                                                      bool allowNumericTypeUnification,
                                                      bool returnAllAsPyObjects=false,
                                                      std::ostream *err_stream=nullptr);

    /*!
     * merges neighboring parts (only parts where partNo is consecutive!)
     * @param parts
     * @return merged parts
     */
    extern std::vector<FilePart> mergeParts(const std::vector<FilePart>& parts, size_t startPartNo=0);
}

#endif //TUPLEX_WORKERAPP_H
