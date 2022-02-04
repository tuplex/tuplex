//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "main.h"

#include <Logger.h>
#include <VirtualFileSystem.h>
#include <Timer.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/platform/Environment.h>
#include <JITCompiler.h>
#include <StringUtils.h>
#include <RuntimeInterface.h>

// S3 stuff
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Bucket.h>

// protobuf
#include <Lambda.pb.h>
#include <physical/TransformStage.h>
#include <physical/CSVReader.h>
#include <physical/TextReader.h>
#include <google/protobuf/util/json_util.h>


// maybe add FILE // LINE to exception


// the special callbacks (used for JITCompiler)
static_assert(sizeof(int64_t) == sizeof(size_t), "size_t must be 64bit");
int64_t writeCSVRow(void* userData, uint8_t* buffer, size_t size) {
#ifndef NDEBUG
    using namespace std;

    auto line = tuplex::fromCharPointers((char*)buffer, (char*)(buffer+size));

    // simply std::cout
    cout<<line<<endl;
#endif

    // return all ok
    return 0;
}

Aws::SDKOptions g_aws_options;
double g_aws_init_time = 0.0;
// note: because compiler uses logger, this here should be pointer for lazy init
std::shared_ptr<tuplex::JITCompiler> g_compiler;

void python_home_setup() {
    // extract python home directory
    std::string task_root = std::getenv("LAMBDA_TASK_ROOT");
    python::python_home_setup(task_root);
}

void global_init() {
    using namespace tuplex;
#ifndef NDEBUG
    std::cout<<"global init..."<<std::endl;
#endif

    // init logger to only act with stdout sink
    Logger::init({std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>()});
    Logger::instance().defaultLogger().info("global_init(): logging system initialized");

    // init aws sdk
    Timer timer;
    Aws::InitAPI(g_aws_options);
    std::string caFile = "/etc/pki/tls/certs/ca-bundle.crt";

    NetworkSettings ns;
    ns.verifySSL = true;
    ns.caFile = caFile;

    // something is wrong with the credentials, try manual listbuckets access...
    auto provider = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("tuplex");
    auto aws_cred = provider->GetAWSCredentials();
    Logger::instance().defaultLogger().info(std::string("credentials obtained via default chain: access key: ") + aws_cred.GetAWSAccessKeyId().c_str());

    // init s3 client manually
    Aws::S3::S3Client client(aws_cred);
    auto outcome = client.ListBuckets();
    if(outcome.IsSuccess()) {
        auto buckets = outcome.GetResult().GetBuckets();
        for (auto entry: buckets) {
            Logger::instance().defaultLogger().info("found uri: " + URI("s3://" + std::string(entry.GetName().c_str())).toString());
        }
    } else {
        Logger::instance().defaultLogger().error("ListBuckets failed");
    }

    // get AWS credentials from Lambda environment...
    // e.g., https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
    std::string access_key = Aws::Environment::GetEnv("AWS_ACCESS_KEY_ID").c_str();
    std::string secret_key = Aws::Environment::GetEnv("AWS_SECRET_ACCESS_KEY").c_str();
    std::string session_token = Aws::Environment::GetEnv("AWS_SESSION_TOKEN").c_str();

    Logger::instance().defaultLogger().info("AWS credentials: access key: " + access_key + " secret key: " + secret_key + " session token: " + session_token);

    // get region from AWS_REGION env
    auto region = Aws::Environment::GetEnv("AWS_REGION");
    VirtualFileSystem::addS3FileSystem(access_key, secret_key, session_token, region.c_str(), ns, true, true);
    g_aws_init_time = timer.time();

    // Note that runtime must be initialized BEFORE compiler due to linking
    runtime::init("lib/tuplex_runtime.so"); // don't change this path, this is how the lambda is packaged...

    g_compiler = std::make_shared<JITCompiler>();

    // initialize python
    python_home_setup();
    python::initInterpreter();
}

void global_cleanup() {
    using namespace tuplex;

#ifndef NDEBUG
    std::cout<<"global cleanup..."<<std::endl;
#endif

    python::closeInterpreter();
    runtime::freeRunTimeMemory();

    // shutdown logging...
    // Aws::Utils::Logging::ShutdownAWSLogging();

    Aws::ShutdownAPI(g_aws_options);
}

struct LambdaExecutor {

    size_t numOutputRows;
    size_t numExceptionRows;
    size_t bytesWritten;
    size_t capacity;
    uint8_t* buffer;

    LambdaExecutor() = delete;
    explicit LambdaExecutor(size_t max_capacity) : numOutputRows(0), numExceptionRows(0), bytesWritten(0), capacity(max_capacity) {
        buffer = new uint8_t[max_capacity];
    }

    void reset() {
        numOutputRows = 0;
        numExceptionRows = 0;
        bytesWritten = 0;
    }

    ~LambdaExecutor() {
        delete [] buffer;
    }

};

int64_t writeRowCallback(LambdaExecutor* exec, const uint8_t* buf, int64_t bufSize) {

    // write row in whatever format...
    // -> simply write to LambdaExecutor
    if(exec->bytesWritten + bufSize < exec->capacity) {
        memcpy(exec->buffer + exec->bytesWritten, buf, bufSize);
        exec->bytesWritten += bufSize;
        exec->numOutputRows++;
    } else {
        std::cerr<<"ran out of capacity!"<<std::endl; //@TODO: error handling!
    }
    return 0;
}

void writeHashCallback(void* user, const uint8_t* key, int64_t key_size, const uint8_t* bucket, int64_t bucket_size) {
    throw std::runtime_error("writeHashCallback not supported yet in Lambda!");
}

void exceptRowCallback(LambdaExecutor* exec, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t* input, int64_t dataLength) {
    exec->numExceptionRows++;
}

aws::lambda_runtime::invocation_request const* g_lambda_req = nullptr;

// @TODO: output buffer size is an issue -> need to write partial results if required!!!

// how much memory to use for the Lambda??
// TODO: make this dependent on the Lambda configuration!
// --> check env variables from here https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
static const size_t MAX_RESULT_BUFFER_SIZE = 200 * 1024 * 1024; // 200MB result buffer?
LambdaExecutor *g_executor = nullptr; // global buffer to hold results!

// whether LambdaExecutor has been set up for this invocation
static bool lambda_executor_setup = false;
void reset_executor_setup() { lambda_executor_setup = false; }


namespace tuplex {
    ContainerInfo getThisContainerInfo() {
        using namespace std;

        ContainerInfo info;
        info.reused = container_reused();
        info.requestId = g_lambda_req->request_id.c_str();
        info.uuid = uuidToString(container_id());
        info.requestsServed = g_num_requests_served;
        info.msRemaining = chrono::duration_cast<chrono::milliseconds, long>(g_lambda_req->get_time_remaining()).count();
        info.startTimestamp = g_start_timestamp;
        std::chrono::high_resolution_clock clock;
        auto utc_deadline = date::utc_clock::from_sys(g_lambda_req->deadline);
        info.deadlineTimestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(utc_deadline.time_since_epoch()).count();
        return info;
    }
}


void fillInGlobals(tuplex::messages::InvocationResponse* m) {
    using namespace std;

    if(!m)
        return;

    // fill in this container info
    m->set_allocated_container(tuplex::getThisContainerInfo().to_protobuf());

    // fill in AWS time
    m->set_awsinittime(g_aws_init_time);
}

tuplex::messages::InvocationResponse make_exception(const std::string& message) {
    using namespace std;
    tuplex::messages::InvocationResponse m;

    m.set_status(tuplex::messages::InvocationResponse_Status_ERROR);
    m.set_errormessage(message);

    fillInGlobals(&m);

    return m;
}

// Used for breakdown experiments to save timestamps of various stages of the Lambda execution
class BreakdownTimings {
private:
    int counter;
    int group_counter[2];
    const char* group_prefix[2] = {"process_mem_", "process_file_"};

    tuplex::Timer timer;

    static std::string counter_prefix(int c) {
        return ((c < 10) ? "0" : "") + std::to_string(c) + "_";
    }

    inline void mark_time_and_reset(double time, const std::string &key) {
        timings[key] = time;
        timer.reset();
    }

    inline void mark_time_group(const std::string &s, int group_num) {
        auto time = timer.time();

        if(group_counter[group_num] == -1) {
            group_counter[group_num] = counter;
            counter++;
        }
        std::string key = counter_prefix(group_counter[group_num]) + group_prefix[group_num] + s;

        mark_time_and_reset(time, key);
    }

public:
    std::map<std::string, double> timings; // the actual breakdown timings

    /*!
     * Reset the timer.
     */
    void reset() { timer.reset(); }

    /*!
     * Mark the time to process input in Tuplex internal memory format
     * @param uri the location of the internal-format data
     */
    void mark_mem_time(const tuplex::URI &uri) {
        mark_time_group(uri.toString(), 0);
    }

    /*!
     * Mark the time to process input from a file.
     * @param uri the location of the input file
     */
    void mark_file_time(const tuplex::URI &uri) {
        mark_time_group(uri.toString(), 1);
    }

    /*!
     * Mark a timestamp
     * @param s the name of the timestamp
     */
    void mark_time(const std::string &s) {
        auto time = timer.time();

        std::string key = counter_prefix(counter) + s;
        counter++;

        mark_time_and_reset(time, key);
    }

    BreakdownTimings(): counter(0), group_counter{-1, -1} {}
};

//! main function for lambda after all the error handling. This here is where work gets done
tuplex::messages::InvocationResponse lambda_main(aws::lambda_runtime::invocation_request const& lambda_req) {

    g_lambda_req = &lambda_req;
    g_num_requests_served++;

    using namespace tuplex;
    using namespace std;

    // process single message using LambdaApp
    auto app = get_app();
    if(!app)
        return make_exception("invalid worker app");

    // perform global init (this is a lazy function)
    int rc = app->globalInit();
    if(rc != WORKER_OK)
        return make_exception("failed processing with code " + std::to_string(rc));

    // process message
    rc = app->processJSONMessage(lambda_req.payload.c_str());
    if(rc != WORKER_OK)
        return make_exception("failed processing with code " + std::to_string(rc));

    // get last response/message?
    // --> what about global stats? @TODO
    auto ret = app->generateResponse();

    // fill in global stats (Lambda specific)
    fillInGlobals(&ret);

    return ret;
}
