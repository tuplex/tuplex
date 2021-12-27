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
     * @return vector<vector<FileParts>>
     */
    extern std::vector<std::vector<FilePart>> splitIntoEqualParts(size_t numThreads, const std::vector<URI>& uris, const std::vector<size_t>& file_sizes);

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
        // use some defaults...
        WorkerSettings() : numThreads(1), normalBufferSize(WORKER_DEFAULT_BUFFER_SIZE), exceptionBufferSize(WORKER_EXCEPTION_BUFFER_SIZE), hashBufferSize(WORKER_HASH_BUFFER_SIZE) {}

        inline bool operator == (const WorkerSettings& other) const {
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
        WorkerApp(const WorkerSettings& settings) : _threadEnvs(nullptr), _numThreads(0) { reinitialize(settings); }

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

    protected:
        WorkerSettings settingsFromMessage(const tuplex::messages::InvocationRequest& req);

         virtual int processMessage(const tuplex::messages::InvocationRequest& req);

        tuplex::messages::InvocationResponse executeTransformTask(const TransformStage* tstage);

        virtual int globalInit();

        std::shared_ptr<TransformStage::JITSymbols> compileTransformStage(TransformStage& stage);

        // inherited variables
        WorkerSettings _settings;
        std::shared_ptr<JITCompiler> _compiler;
#ifdef BUILD_WITH_AWS
        Aws::SDKOptions _aws_options;
#endif

        // cache for compiled stages (sometimes same IR gets send)
        std::unordered_map<std::string, std::shared_ptr<TransformStage::JITSymbols>> _compileCache;

        // variables for each Thread
        struct ThreadEnv {
            size_t threadNo; //! which thread number
            WorkerApp* app; //! which app to use
            Buffer normalBuf; //! holds normal rows
            size_t numNormalRows; //! how many normal rows
            Buffer exceptionBuf; //! holds exception rows
            size_t numExceptionRows; //! how many exception rows
            void* hashMap; //! for hash output
            std::vector<std::string> spillFiles; //! files used for storing spilled buffers.
            ThreadEnv() : threadNo(0), app(nullptr), hashMap(nullptr), numNormalRows(0), numExceptionRows(0), normalBuf(100),
                          exceptionBuf(100) {}

          /*!
           * calculates how many bytes of storage the hashmap takes!
           * @return size in bytes
           */
            size_t hashMapSize() const;
        };

        ThreadEnv *_threadEnvs;
        size_t _numThreads;

        int64_t initTransformStage(const TransformStage::InitData& initData, const std::shared_ptr<TransformStage::JITSymbols>& syms);
        int64_t releaseTransformStage(const std::shared_ptr<TransformStage::JITSymbols>& syms);

        int64_t processSource(int threadNo, int64_t inputNodeID, const URI& inputURI, const TransformStage* tstage, const std::shared_ptr<TransformStage::JITSymbols>& syms);

        virtual MessageHandler& logger() { return Logger::instance().logger("worker"); }

        /*!
         * spill buffer out to somewhere & reset counter
         */
        virtual void spillNormalBuffer(size_t threadNo);

        void writeBufferToFile(const URI& outputURI,
                               const FileFormat& fmt,
                               const uint8_t* buf,
                               const size_t buf_size,
                               const size_t num_rows,
                               const TransformStage* tstage);

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
}

#endif //TUPLEX_WORKERAPP_H
