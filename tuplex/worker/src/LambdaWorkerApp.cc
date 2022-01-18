//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 12/2/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifdef BUILD_WITH_AWS

#include <LambdaWorkerApp.h>

// AWS specific includes
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/platform/Environment.h>

#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/ListFunctionsRequest.h>
#include <aws/lambda/model/UpdateFunctionConfigurationRequest.h>
#include <aws/lambda/model/UpdateFunctionConfigurationResult.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include <AWSCommon.h>
#include <aws/lambda/LambdaClient.h>

namespace tuplex {

    // Lambda specific configuration
    const std::string LambdaWorkerApp::caFile = "/etc/pki/tls/certs/ca-bundle.crt";
    const std::string LambdaWorkerApp::tuplexRuntimePath = "lib/tuplex_runtime.so";
    const bool LambdaWorkerApp::verifySSL = true;

    struct SelfInvocationContext {
        std::atomic_int32_t numPendingRequests;
        std::mutex mutex;
        std::vector<std::string> containerIds;
        std::string tag;
        std::string functionName;
        size_t timeOutInMs;
        std::shared_ptr<Aws::Lambda::LambdaClient> client;

        static void lambdaCallback(const Aws::Lambda::LambdaClient* client,
                                        const Aws::Lambda::Model::InvokeRequest& req,
                                        const Aws::Lambda::Model::InvokeOutcome& outcome,
                                        const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx);

        SelfInvocationContext() : numPendingRequests(0) {}

        class CallbackContext : public Aws::Client::AsyncCallerContext {
        private:
            SelfInvocationContext* _ctx;
            uint32_t _no;
        public:
            CallbackContext() = delete;
            CallbackContext(SelfInvocationContext* ctx, uint32_t invocationNo) : _ctx(ctx), _no(invocationNo) {}

            SelfInvocationContext* ctx() const { return _ctx; }
            uint32_t no() const { return _no; }
        };
    };

    void SelfInvocationContext::lambdaCallback(const Aws::Lambda::LambdaClient* client,
                                               const Aws::Lambda::Model::InvokeRequest& req,
                                               const Aws::Lambda::Model::InvokeOutcome& outcome,
                                               const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx) {
        using namespace std;

        auto callback_ctx = dynamic_cast<const SelfInvocationContext::CallbackContext*>(ctx.get());
        assert(callback_ctx);
        auto self_ctx = callback_ctx->ctx();
        assert(self_ctx);

        MessageHandler& logger = Logger::instance().logger("lambda-warmup");

        int statusCode = 0;

        // lock & add container ID if successful outcome!
        if(!outcome.IsSuccess()) {
            auto &error = outcome.GetError();
            statusCode = static_cast<int>(error.GetResponseCode());

            // rate limit? => reissue request
            if(statusCode == static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS) || // i.e. 429
               statusCode == static_cast<int>(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR)) {
                // should retry...

                logger.info("should retry request... (nyimpl)");

            } else {
                logger.error("Self-Invoke request errored with code " + std::to_string(statusCode) + " details: " + std::string(error.GetMessage().c_str()));
            }
        } else {
            // write response
            auto& result = outcome.GetResult();
            statusCode = result.GetStatusCode();
            std::string version = result.GetExecutedVersion().c_str();

            // parse payload
            stringstream ss;
            auto& stream = const_cast<Aws::Lambda::Model::InvokeResult&>(result).GetPayload();
            ss<<stream.rdbuf();
            string data = ss.str();
            messages::InvocationResponse response;
            google::protobuf::util::JsonStringToMessage(data, &response);

            logger.info("got answer from self-invocation request");

            if(response.status() == messages::InvocationResponse_Status_SUCCESS) {

                if(response.containerreused()) {
                    logger.info("container reused, invoke again.");

                    // invoke again (do not change count)
                    // Tuplex request
                    messages::InvocationRequest req;
                    req.set_type(messages::MessageType::MT_WARMUP);

                    // specific warmup message contents
                    auto wm = std::make_unique<messages::WarmupMessage>();
                    wm->set_timeoutinms(self_ctx->timeOutInMs); // remaining time?
                    wm->set_invocationcount(0); // do not self-invoke again? Or should they?
                    req.set_allocated_warmup(wm.release());

                    // construct invocation request
                    Aws::Lambda::Model::InvokeRequest invoke_req;
                    invoke_req.SetFunctionName(self_ctx->functionName.c_str());
                    // note: may redesign lambda backend to work async, however then response only yields status code
                    // i.e., everything regarding state needs to be managed explicitly...
                    invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
                    // logtype to extract log data??
                    //req.SetLogtype(Aws::Lambda::Model::LogType::None);
                    std::string json_buf;
                    google::protobuf::util::MessageToJsonString(req, &json_buf);
                    invoke_req.SetBody(stringToAWSStream(json_buf));
                    invoke_req.SetContentType("application/javascript");

                    self_ctx->client->InvokeAsync(invoke_req,
                                            SelfInvocationContext::lambdaCallback,
                                            Aws::MakeShared<SelfInvocationContext::CallbackContext>(self_ctx->tag.c_str(), self_ctx, callback_ctx->no()));
                } else {
                    std::unique_lock<std::mutex> lock(self_ctx->mutex);
                    // add the ID of the container
                    const_cast<SelfInvocationContext*>(self_ctx)->containerIds.emplace_back(response.containerid().c_str());
                    // and all IDs that container invoked
                    for(auto id : response.warmedupcontainers()) {
                        self_ctx->containerIds.emplace_back(id);
                    }
                    self_ctx->numPendingRequests.fetch_add(-1, std::memory_order_release);
                }
            } else {
                // failed...
                logger.error("invoke failed, wrong code returned.");
            }
        }
    }

    // helper function to self-invoke quickly (creates new client!)
    std::vector<std::string> selfInvoke(const std::string& functionName,
                                        size_t count,
                                        size_t timeOutInMs,
                                        const AWSCredentials& credentials,
                                        const NetworkSettings& ns,
                                        std::string tag) {

        MessageHandler& logger = Logger::instance().logger("lambda-warmup");

        if(0 == count)
            return {};

        std::vector<std::string> containerIds;
        Timer timer;

        // init Lambda client
        Aws::Client::ClientConfiguration clientConfig;

        clientConfig.requestTimeoutMs = timeOutInMs; // conv seconds to ms
        clientConfig.connectTimeoutMs = timeOutInMs; // connection timeout

        // tune client, according to https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/client-config.html
        // note: max connections should not exceed max concurrency if it is below 100, else aws lambda
        // will return toomanyrequestsexception
        clientConfig.maxConnections = count;

        // to avoid thread exhaust of system, use pool thread executor with 8 threads
        clientConfig.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(tag.c_str(), count);
        clientConfig.region = credentials.default_region.c_str();

        //clientConfig.userAgent = "tuplex"; // should be perhaps set as well.
        applyNetworkSettings(ns, clientConfig);

        // change aws settings here
        Aws::Auth::AWSCredentials cred(credentials.access_key.c_str(), credentials.secret_key.c_str());

        SelfInvocationContext ctx;
        ctx.client = Aws::MakeShared<Aws::Lambda::LambdaClient>(tag.c_str(), cred, clientConfig);
        ctx.tag = tag;
        ctx.timeOutInMs = timeOutInMs;
        ctx.functionName = functionName;

        // async callback & invocation
        for(unsigned i = 0; i < count; ++i) {

            // Tuplex request
            messages::InvocationRequest req;
            req.set_type(messages::MessageType::MT_WARMUP);

            // specific warmup message contents
            auto wm = std::make_unique<messages::WarmupMessage>();
            wm->set_timeoutinms(timeOutInMs);
            wm->set_invocationcount(0); // do not self-invoke again? Or should they?
            req.set_allocated_warmup(wm.release());

            // construct invocation request
            Aws::Lambda::Model::InvokeRequest invoke_req;
            invoke_req.SetFunctionName(functionName.c_str());
            // note: may redesign lambda backend to work async, however then response only yields status code
            // i.e., everything regarding state needs to be managed explicitly...
            invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
            // logtype to extract log data??
            //req.SetLogtype(Aws::Lambda::Model::LogType::None);
            std::string json_buf;
            google::protobuf::util::MessageToJsonString(req, &json_buf);
            invoke_req.SetBody(stringToAWSStream(json_buf));
            invoke_req.SetContentType("application/javascript");
            ctx.numPendingRequests.fetch_add(1, std::memory_order_release);

            ctx.client->InvokeAsync(invoke_req,
                                SelfInvocationContext::lambdaCallback,
                                Aws::MakeShared<SelfInvocationContext::CallbackContext>(tag.c_str(), &ctx, i));
        }

        // wait till pending is 0 or timeout
        double timeout = (double)timeOutInMs / 1000.0;
        while(timer.time() < timeout && ctx.numPendingRequests > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        logger.info("warmup done, result are " + pluralize(ctx.containerIds, "container"));

        // how long did it take?
        containerIds = ctx.containerIds;
        return containerIds;
    }

    int LambdaWorkerApp::globalInit() {

        // skip if already initialized
        if(_globallyInitialized)
            return WORKER_OK;

        // Lambda specific initialization
        Timer timer;
        Aws::InitAPI(_aws_options);

        // get AWS credentials from Lambda environment...
        // Note that to run on Lambda this requires a session token!
        // e.g., https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
        std::string access_key = Aws::Environment::GetEnv("AWS_ACCESS_KEY_ID").c_str();
        std::string secret_key = Aws::Environment::GetEnv("AWS_SECRET_ACCESS_KEY").c_str();
        std::string session_token = Aws::Environment::GetEnv("AWS_SESSION_TOKEN").c_str();

        // get region from AWS_REGION env
        auto region = Aws::Environment::GetEnv("AWS_REGION");
        auto functionName = Aws::Environment::GetEnv("AWS_LAMBDA_FUNCTION_NAME");

        _functionName = functionName.c_str();

        _credentials.access_key = access_key;
        _credentials.secret_key = secret_key;
        _credentials.session_token = session_token;
        _credentials.default_region = region;

        _networkSettings.verifySSL = verifySSL;
        _networkSettings.caFile = caFile;

        VirtualFileSystem::addS3FileSystem(access_key, secret_key, session_token, region.c_str(), _networkSettings,
                                           true, true);

        runtime::init(tuplexRuntimePath);
        _compiler = std::make_shared<JITCompiler>();

        // init python & set explicitly python home for Lambda
        std::string task_root = std::getenv("LAMBDA_TASK_ROOT");
        python::python_home_setup(task_root);
        logger().debug("Set PYTHONHOME=" + task_root);
        python::initInterpreter();
        metrics.global_init_time = timer.time();

        _globallyInitialized = true;
        return WORKER_OK;
    }

    int LambdaWorkerApp::processMessage(const tuplex::messages::InvocationRequest& req) {

        _messageType = req.type();

        // check message type
        if(req.type() == messages::MessageType::MT_WARMUP) {
            logger().info("Received warmup message");

            size_t selfInvokeCount = 0;
            size_t timeOutInMs = 100;
            if(req.has_warmup()) {
                selfInvokeCount = req.warmup().invocationcount();
                timeOutInMs = req.warmup().timeoutinms();
            }

            // use self invocation
            if(selfInvokeCount > 0) {
                logger().info("invoking " + pluralize(selfInvokeCount, "other lambda") + " (timeout: " + std::to_string(timeOutInMs) + "ms)");
                _containerIds = selfInvoke(_functionName, selfInvokeCount, timeOutInMs, _credentials, _networkSettings);
                logger().info("warmup done.");
            }

            return WORKER_OK;
        } else if(req.type() == messages::MessageType::MT_TRANSFORM) {
            // validate only S3 uris are given (in debug mode)
#ifdef NDEBUG
            bool invalid_uri_found = false;
        for(const auto& str_path : req.inputuris()) {
	    URI path(str_path);
            // check paths are S3 paths
            if(path.prefix() != "s3://") {
                logger().error("InvalidPath: input path must be s3:// path, is " + path.toPath());
                invalid_uri_found = true;
            }
        }
        if(invalid_uri_found)
            return WORKER_ERROR_INVALID_URI;
#endif

            // extract settings from req
            _settings = settingsFromMessage(req);
            if(!_threadEnvs)
                initThreadEnvironments();

            // @TODO
            // can reuse here infrastructure from WorkerApp!
            return WorkerApp::processMessage(req);
        } else {
            return WORKER_ERROR_UNKNOWN_MESSAGE;
        }

        // TODO notes for Lambda:
        // 1. scale-out should work (via self-invocation!)
        // 2. Joins (i.e. allow flight query to work)
        // 3. self-specialization (for flights should work) --> requires range optimization + detection on files.
        // ==> need other optimizations as well -.-

        return WORKER_OK;
    }

    tuplex::messages::InvocationResponse LambdaWorkerApp::generateResponse() {
        tuplex::messages::InvocationResponse result;

        result.set_status(tuplex::messages::InvocationResponse_Status_SUCCESS);

        if(!_statistics.empty()) {
            auto& last = _statistics.back();
            // set metrics (num rows etc.)
            result.set_taskexecutiontime(last.totalTime);
            result.set_numrowswritten(last.numNormalOutputRows);
            result.set_numexceptions(last.numExceptionOutputRows);
        }

        // message specific results
        if(_messageType == tuplex::messages::MessageType::MT_WARMUP) {
            for(auto id : _containerIds) {
                result.add_warmedupcontainers(id);
            }
        }

        // TODO: other stuff...
//        for(const auto& uri : inputURIs) {
//            result.add_inputuris(uri.toPath());
//        }
//        result.add_outputuris(outputURI.toPath());
//        result.set_taskexecutiontime(taskTime);
//        for(const auto& keyval : timer.timings) {
//            (*result.mutable_breakdowntimes())[keyval.first] = keyval.second;
//        }

        return result;
    }
}

#endif
