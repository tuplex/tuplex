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

// give 32MB standard buf size, 8MB for exceptions and hash
#define WORKER_DEFAULT_BUFFER_SIZE 33554432
#define WORKER_EXCEPTION_BUFFER_SIZE 8388608
#define WORKER_HASH_BUFFER_SIZE 8388608

// protobuf
#include <Lambda.pb.h>
#include <physical/TransformStage.h>
#include <physical/CSVReader.h>
#include <physical/TextReader.h>
#include <google/protobuf/util/json_util.h>

#ifdef BUILD_WITH_AWS
#include <aws/core/Aws.h>
#endif

#include <Serializer.h>

// Notes: For fileoutput, sometimes a reorg step might be necessary!
// e.g., when using S3 file-system could upload 5TB per object.
// => need to carefully check limits. Currently, maximum of 1,000 parts possible (largest size 5TB!)
// -> check here https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
// could do distributed S3 upload!


namespace tuplex {


    struct FilePart {
        URI uri;
        size_t partNo; // when trying to restore in order, select here partNo
        size_t rangeStart;
        size_t rangeEnd;
    };

    /*!
     * helper function to help distribute file processing into multilpe parts across different threads
     * @param numThreads on how many threads to split file processing
     * @param uris which uris
     * @param file_sizes which file sizes
     * @param minimumPartSize minimum part size, helpful to avoid tiny parts (default: 4KB)
     * @return vector<vector<FileParts>>
     */
    extern std::vector<std::vector<FilePart>> splitIntoEqualParts(size_t numThreads,
                                                                  const std::vector<URI>& uris,
                                                                  const std::vector<size_t>& file_sizes,
                                                                  size_t minimumPartSize=1024 * 4);

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

        // use some defaults...
        WorkerSettings() : numThreads(1), normalBufferSize(WORKER_DEFAULT_BUFFER_SIZE),
        exceptionBufferSize(WORKER_EXCEPTION_BUFFER_SIZE), hashBufferSize(WORKER_HASH_BUFFER_SIZE) {

            // set some options from defaults...
            auto opt = ContextOptions::defaults();
            runTimeMemory = opt.RUNTIME_MEMORY();
            runTimeMemoryDefaultBlockSize = opt.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE();
            allowNumericTypeUnification = opt.AUTO_UPCAST_NUMBERS();
        }

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

            return true;
        }

        bool operator != (const WorkerSettings& other) const {
            return !(*this == other);
        }
    };

    /// main class to represent a running worker application
    /// i.e., this is an applicaton which performs some task and returns it in some way
    /// exchange could be via request response, files, shared memory? etc.
    class WorkerApp {
    public:
        WorkerApp() = delete;
        WorkerApp(const WorkerApp& other) =  delete;

        // create WorkerApp from settings
        WorkerApp(const WorkerSettings& settings) : _threadEnvs(nullptr), _numThreads(0), _globallyInitialized(false), _logger(Logger::instance().logger("worker")) {}

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

        virtual int globalInit();

    protected:
        WorkerSettings settingsFromMessage(const tuplex::messages::InvocationRequest& req);

         virtual int processMessage(const tuplex::messages::InvocationRequest& req);

        tuplex::messages::InvocationResponse executeTransformTask(const TransformStage* tstage);

        std::shared_ptr<TransformStage::JITSymbols> compileTransformStage(TransformStage& stage);

        // inherited variables
        WorkerSettings _settings;
        bool _globallyInitialized;
        std::shared_ptr<JITCompiler> _compiler;
        MessageHandler& _logger;
#ifdef BUILD_WITH_AWS
        Aws::SDKOptions _aws_options;
#endif

        // cache for compiled stages (sometimes same IR gets send)
        std::unordered_map<std::string, std::shared_ptr<TransformStage::JITSymbols>> _compileCache;

        struct SpillInfo {
            std::string path;
            size_t num_rows;
            size_t file_size;
            size_t originalPartNo;
            bool isExceptionBuf;
        };

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
            void* hashMap; //! for hash output
            size_t hashOriginalPartNo; //! original part No
            std::vector<SpillInfo> spillFiles; //! files used for storing spilled buffers.
            ThreadEnv() : threadNo(0), app(nullptr), hashMap(nullptr), numNormalRows(0), numExceptionRows(0), normalBuf(100),
                          exceptionBuf(100) {}

          /*!
           * calculates how many bytes of storage the hashmap takes!
           * @return size in bytes
           */
            size_t hashMapSize() const;
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
            WriteInfo() : buf(nullptr), use_buf(true), num_rows(0), buf_size(0) {};
        };

        ThreadEnv *_threadEnvs;
        size_t _numThreads;

        void initThreadEnvironments();

        int64_t initTransformStage(const TransformStage::InitData& initData, const std::shared_ptr<TransformStage::JITSymbols>& syms);
        int64_t releaseTransformStage(const std::shared_ptr<TransformStage::JITSymbols>& syms);

        int64_t processSource(int threadNo, int64_t inputNodeID, const FilePart& part, const TransformStage* tstage, const std::shared_ptr<TransformStage::JITSymbols>& syms);

        /*!
         * performs out-of-order exception resolution of exceptions, and appends them simply to normal buffer.
         * @param threadNo for which thread to run resolution (only exceptions of that thread will be resolved
         * @param stage stage
         * @param syms symbols
         * @return err or success code
         */
        int64_t resolveOutOfOrder(int threadNo, const TransformStage* stage, const std::shared_ptr<TransformStage::JITSymbols>& syms);

        int64_t resolveBuffer(int threadNo, Buffer& buf, size_t numRows, const TransformStage* stage, const std::shared_ptr<TransformStage::JITSymbols>& syms);

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

        void writeBufferToFile(const URI& outputURI,
                               const FileFormat& fmt,
                               const uint8_t* buf,
                               const size_t buf_size,
                               const size_t num_rows,
                               const TransformStage* tstage);

        void writePartsToFile(const URI& outputURI, const FileFormat& fmt,
                              const std::vector<WriteInfo>& parts, const TransformStage* stage);

        URI getNextOutputURI(int threadNo, const URI& baseURI, bool isBaseURIFolder, const std::string& extension);

        virtual int64_t writeRow(size_t threadNo, const uint8_t* buf, int64_t bufSize);
        virtual void writeHashedRow(size_t threadNo, const uint8_t* key, int64_t key_size, const uint8_t* bucket, int64_t bucket_size);
        virtual void writeException(size_t threadNo, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t* input, int64_t dataLength);
    private:
        static int64_t writeRowCallback(ThreadEnv* env, const uint8_t* buf, int64_t bufSize);
        static void writeHashCallback(ThreadEnv* env, const uint8_t* key, int64_t key_size, const uint8_t* bucket, int64_t bucket_size);
        static void exceptRowCallback(ThreadEnv* env, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t* input, int64_t dataLength);

        // slow path callbacks
        static int64_t slowPathRowCallback(ThreadEnv *env, uint8_t* buf, int64_t bufSize);
        static void slowPathExceptCallback(ThreadEnv* env, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t *input, int64_t dataLength);

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
    };


    extern PyObject* fallbackTupleFromParseException(const uint8_t* buf, size_t buf_size);
    extern FallbackPathResult processRowUsingFallback(PyObject* func,
                                                      int64_t ecCode,
                                                      int64_t ecOperatorID,
                                                      const Schema& input_schema,
                                                      const uint8_t* buf,
                                                      size_t buf_size,
                                                      const Schema& specialized_target_schema,
                                                      const Schema& general_target_schema,
                                                      const std::vector<PyObject*>& py_intermediates,
                                                      bool allowNumericTypeUnification,
                                                      bool returnAllAsPyObjects=false,
                                                      std::ostream *err_stream=nullptr);

}

#endif //TUPLEX_WORKERAPP_H
