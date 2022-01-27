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
        mutable std::mutex mutex;
        std::vector<ContainerInfo> containers;
        std::string tag;
        std::string functionName;
        size_t timeOutInMs;
        size_t baseDelayInMs;
        std::chrono::high_resolution_clock::time_point tstart; // start point of context
        std::shared_ptr<Aws::Lambda::LambdaClient> client;

        static void lambdaCallback(const Aws::Lambda::LambdaClient* client,
                                        const Aws::Lambda::Model::InvokeRequest& req,
                                        const Aws::Lambda::Model::InvokeOutcome& outcome,
                                        const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx);

        SelfInvocationContext() : numPendingRequests(0), tstart(std::chrono::high_resolution_clock::now()) {}

        inline double timeSinceStartInSeconds() {
            auto stop = std::chrono::high_resolution_clock::now();
            double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - tstart).count() / 1000000000.0;
            return duration;
        }

        /*!
         * checks whether container with uuid is contained already or not
         */
        inline bool contains(const std::string& uuid) const {
            std::unique_lock<std::mutex> lock(mutex);
            auto it = std::find_if(containers.cbegin(), containers.cend(), [&uuid](const ContainerInfo& info) {
                return info.uuid == uuid;
            });
            return it != containers.cend();
        }

        class CallbackContext : public Aws::Client::AsyncCallerContext {
        private:
            SelfInvocationContext* _ctx;
            uint32_t _no;
            messages::WarmupMessage _wm;
        public:
            CallbackContext() = delete;
            CallbackContext(SelfInvocationContext* ctx, uint32_t invocationNo, messages::WarmupMessage wm) : _ctx(ctx), _no(invocationNo), _wm(wm) {}

            SelfInvocationContext* ctx() const { return _ctx; }
            uint32_t no() const { return _no; }
            const messages::WarmupMessage& message() const { return _wm; }
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

            // logger.info("got answer from self-invocation request");
            double timeout = self_ctx->timeOutInMs / 1000.0;

            if(response.status() == messages::InvocationResponse_Status_SUCCESS) {

                // check if container is already part of containers or not, if reused and part of it -> reinvoke!
                if(response.container().reused() && self_ctx->contains(response.container().uuid())) {

                    // check whether to re-invoke or to leave
                    if(self_ctx->timeSinceStartInSeconds() < timeout) {
                        // logger.info("container reused, invoke again.");
                        // invoke again (do not change count)

                        // Tuplex request
                        messages::InvocationRequest req;
                        req.set_type(messages::MessageType::MT_WARMUP);

                        const auto& original_message = callback_ctx->message();
                        vector<size_t> remaining_counts;
                        for(unsigned i = 1; i < original_message.invocationcount_size(); ++i)
                            remaining_counts.push_back(original_message.invocationcount(i));

                        // specific warmup message contents
                        auto wm = std::make_unique<messages::WarmupMessage>();
                        wm->set_timeoutinms(self_ctx->timeOutInMs); // remaining time?
                        wm->set_basedelayinms(self_ctx->baseDelayInMs);
                        for(auto count : remaining_counts)
                            wm->add_invocationcount(count);
                        req.set_allocated_warmup(wm.release());

                        messages::WarmupMessage message;
                        message.set_timeoutinms(self_ctx->timeOutInMs);
                        message.set_basedelayinms(self_ctx->baseDelayInMs);
                        for(auto count : remaining_counts)
                            message.add_invocationcount(count);

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
                                                      Aws::MakeShared<SelfInvocationContext::CallbackContext>(self_ctx->tag.c_str(),
                                                                                                              self_ctx,
                                                                                                              callback_ctx->no(),
                                                                                                              message));
                    } else {
                        // atomic decref
                        self_ctx->numPendingRequests.fetch_add(-1, std::memory_order_release);
                        logger.info("warmup request timed out.");
                    }
                } else {
                    if(!response.container().reused())
                        logger.info("New container " + std::string(response.container().uuid().c_str()) + " started.");
                    else {
                        logger.info("Found already running container " + std::string(response.container().uuid().c_str()) + ".");
                    }
                    std::unique_lock<std::mutex> lock(self_ctx->mutex);
                    // add the container info of the invoker itself!
                    const_cast<SelfInvocationContext*>(self_ctx)->containers.emplace_back(response.container());
                    // and all IDs that that container invoked
                    for(auto info : response.invokedcontainers()) {
                        self_ctx->containers.emplace_back(info);
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
    std::vector<ContainerInfo> selfInvoke(const std::string& functionName,
                                        size_t count,
                                        const std::vector<size_t>& recursive_counts,
                                        size_t timeOutInMs,
                                        size_t baseDelayInMs,
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

        size_t lambdaToLambdaTimeOutInMs = 200;

        clientConfig.requestTimeoutMs = lambdaToLambdaTimeOutInMs; // conv seconds to ms
        clientConfig.connectTimeoutMs = lambdaToLambdaTimeOutInMs; // connection timeout

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
        Aws::Auth::AWSCredentials cred(credentials.access_key.c_str(),
                                       credentials.secret_key.c_str(),
                                       credentials.session_token.c_str());

        SelfInvocationContext ctx;
        ctx.client = Aws::MakeShared<Aws::Lambda::LambdaClient>(tag.c_str(), cred, clientConfig);
        ctx.tag = tag;
        ctx.timeOutInMs = timeOutInMs;
        ctx.baseDelayInMs = baseDelayInMs;
        ctx.functionName = functionName;

        double timeout = (double)timeOutInMs / 1000.0;

        // async callback & invocation
        for(unsigned i = 0; i < count; ++i) {

            // Tuplex request
            messages::InvocationRequest req;
            req.set_type(messages::MessageType::MT_WARMUP);

            // specific warmup message contents
            auto wm = std::make_unique<messages::WarmupMessage>();
            auto invoked_timeout = baseDelayInMs > timeOutInMs ? baseDelayInMs : timeOutInMs - baseDelayInMs;
            wm->set_timeoutinms(invoked_timeout);
            wm->set_basedelayinms(baseDelayInMs);
            for(auto count : recursive_counts)
                wm->add_invocationcount(count);
            req.set_allocated_warmup(wm.release());

            messages::WarmupMessage message;
            message.set_timeoutinms(invoked_timeout);
            message.set_basedelayinms(baseDelayInMs);
            for(auto count : recursive_counts)
                message.add_invocationcount(count);

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

            if(ctx.timeSinceStartInSeconds() < timeout) {
                // invoke if time is larger
                ctx.numPendingRequests.fetch_add(1, std::memory_order_release);
                ctx.client->InvokeAsync(invoke_req,
                                        SelfInvocationContext::lambdaCallback,
                                        Aws::MakeShared<SelfInvocationContext::CallbackContext>(tag.c_str(), &ctx, i, message));
            }
        }

        // wait till pending is 0 or timeout (done in individual tasks)
        while(ctx.numPendingRequests > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        logger.info("warmup done, result are " + pluralize(ctx.containers.size(), "container"));

        // how long did it take?
        return ctx.containers;
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
        using namespace std;

        _messageType = req.type();

        // check message type
        if(req.type() == messages::MessageType::MT_WARMUP) {
            logger().info("Received warmup message");
            size_t selfInvokeCount = 0;
            vector<size_t> recursive_counts;
            size_t timeOutInMs = 100;
            size_t baseDelayInMs = 75;
            if(req.has_warmup()) {
                for(unsigned i = 0; i < req.warmup().invocationcount_size(); ++i) {
                    if(0 == i)
                        selfInvokeCount = req.warmup().invocationcount(i);
                    else
                        recursive_counts.push_back(req.warmup().invocationcount(i));
                }

                timeOutInMs = req.warmup().timeoutinms();
                baseDelayInMs = req.warmup().basedelayinms();
            }

            // use self invocation
            if(selfInvokeCount > 0) {
                logger().info("invoking " + pluralize(selfInvokeCount, "other lambda") + " (timeout: " + std::to_string(timeOutInMs) + "ms)");
                Timer timer;
                auto ret = selfInvoke(_functionName,
                                                selfInvokeCount,
                                                recursive_counts,
                                                timeOutInMs,
                                                baseDelayInMs,
                                                _credentials,
                                                _networkSettings);

                // clean containers
                std::unordered_map<std::string, ContainerInfo> uniqueContainers;
                for(auto info : ret) {
                    auto it = uniqueContainers.find(info.uuid);
                    if(it == uniqueContainers.end())
                        uniqueContainers[info.uuid] = info;
                    else {
                        // update if more recent (only for reused, new should be unique!)
                        if(it->second.reused && it->second.msRemaining >= info.msRemaining) {
                            it->second = info;
                        }

                        if(!it->second.reused)
                            logger().error("internal error, 2x new with unique ID?");
                    }
                }

                _invokedContainers.clear();
                for(auto keyval : uniqueContainers) {
                    _invokedContainers.push_back(keyval.second);
                }

                // wait till delay for this func is reached
                double delayForThis = static_cast<double>(recursive_counts.size() * baseDelayInMs) / 1000.0;
                while(timer.time() < delayForThis)
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));

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


            // check whether self-invocation is used
            if(req.has_stage() && req.stage().invocationcount_size() > 0) {
                std::stringstream ss;
                ss<<"Invoking ";
                for(auto count : req.stage().invocationcount())
                    ss<<count<<", ";
                ss<<"Lambdas recursively.";
                logger().info(ss.str());

                // split into parts for all Lambdas to invoke!
                size_t total_parts = 1;
                size_t prod = 1;
                size_t num_lambdas_to_invoke = 0;
                for(auto count : req.stage().invocationcount()) {
                    if(count != 0) {
                        total_parts += count * prod; // this is recursive, so try splitting into that many parts!
                        prod *= count;

                        // set how many lambdas to invoke
                        if(num_lambdas_to_invoke == 0)
                            num_lambdas_to_invoke = count;
                    }
                }

                if(0 == num_lambdas_to_invoke) {
                    logger().error("invalid invocation count, 0 lambdas to invoke here?");
                    return WORKER_ERROR_INVALID_JSON_MESSAGE;
                }

                logger().info("Splitting submitted " + pluralize(req.inputsizes().size(), "file") + " into " + pluralize(total_parts, "part") + ".");

                // min part size should be 1MB
                std::vector<URI> uris;
                std::vector<size_t> file_sizes;
                auto num_files = req.inputuris_size();
                uris.reserve(num_files);
                file_sizes.reserve(num_files);
                if(req.inputsizes_size() != num_files) {
                    logger().error("#input files does not equal submitted sizes");
                    return WORKER_ERROR_INVALID_JSON_MESSAGE;
                }

                for(unsigned i = 0 ; i < num_files; ++i) {
                    uris.push_back(req.inputuris(i));
                    file_sizes.push_back(req.inputsizes(i));
                }

                size_t minimumPartSize = 1024 * 1024; // 1MB.
                auto parts = splitIntoEqualParts(total_parts, uris, file_sizes, minimumPartSize);

                // process data, first part is for this Lambda
                // log it here out
                {
                    std::stringstream ss;
                    for(unsigned i = 0; i < parts.size(); ++i) {
                        if(0 == i)
                            ss<<"Overview which Lambda will process what:\nLambda (this) will process: ";
                        else
                            ss<<"\nLambda ("<<i<<") will process: ";
                        for(auto part : parts[i]) {
                            ss<<"\n - "<<part.uri.toString()<<":"<<part.rangeStart<<"-"<<part.rangeEnd;
                        }
                    }
                    logger().info(ss.str());
                }

                // issue requests & wait for them

                // invoke other lambdas here...
                // -----
                // perform task on this Lambda...
                auto parts_to_execute = parts[0];

                std::vector<FilePart> other_lambda_parts;
                for(unsigned i = 1; i < parts.size(); ++i)
                    std::copy(parts[i].begin(), parts[i].end(), std::back_inserter(other_lambda_parts));
                auto before_merge_count = other_lambda_parts.size();
                other_lambda_parts = mergeParts(other_lambda_parts);
                logger().info("Merged " + pluralize(before_merge_count, "part") + " to " + pluralize(other_lambda_parts.size(), "part"));
                logger().info("Redistributing " + pluralize(other_lambda_parts.size(), "part")
                + " to " + pluralize(num_lambdas_to_invoke, "other lambda") + ", executing "
                + pluralize(parts_to_execute.size(), "part") + " on this lambda." );

                // redistribute according to how many lambdas should be invoked now
                auto lambda_parts = splitIntoEqualParts(num_lambdas_to_invoke, other_lambda_parts, minimumPartSize);

                // ------


                // @TODO...


                // prep local execution
                // only transform stage yet supported, in the future support other stages as well!
                auto tstage = TransformStage::from_protobuf(req.stage());

                // check what type of message it is & then start processing it.
                auto syms = compileTransformStage(*tstage);
                if(!syms)
                    return WORKER_ERROR_COMPILATION_FAILED;

                if(!syms->functor)
                    logger().error("functor not valid, what's going on?");

                logger().info("Executing " + pluralize(parts_to_execute.size(), "part") + " on this Lambda, spawning others");
                URI output_uri(req.outputuri());

                // should parts get merged or not??
                // i.e. initiate multi-upload requests??
                auto rc = processTransformStage(tstage, syms, parts_to_execute, output_uri);
                if(rc != WORKER_OK)
                    return rc;
                logger().info("This Lambda done executing, waiting for requests...");

                // wait for requests to finish unless this Lambda expires...


                // form message to return...
                // i.e. which parts succeeded? which are missing?

                return WORKER_OK;
            }

            // @TODO: what about remaining time? Partial completion?

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
        result.set_type(_messageType);

        if(!_statistics.empty()) {
            auto& last = _statistics.back();
            // set metrics (num rows etc.)
            result.set_taskexecutiontime(last.totalTime);
            result.set_numrowswritten(last.numNormalOutputRows);
            result.set_numexceptions(last.numExceptionOutputRows);
        }

        // message specific results
        if(_messageType == tuplex::messages::MessageType::MT_WARMUP) {
            for(const auto& c_info : _invokedContainers) {
                auto element = result.add_invokedcontainers();
                c_info.fill(element);
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
