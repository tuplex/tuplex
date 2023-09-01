//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifdef BUILD_WITH_AWS

#include <ee/aws/AWSLambdaBackend.h>
#include <ee/local/LocalBackend.h>

#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/ListFunctionsRequest.h>
#include <aws/lambda/model/UpdateFunctionConfigurationRequest.h>
#include <aws/lambda/model/UpdateFunctionConfigurationResult.h>
#include <aws/lambda/model/GetFunctionConcurrencyRequest.h>
#include <aws/lambda/model/GetFunctionConcurrencyResult.h>
#include <aws/lambda/model/PutFunctionConcurrencyRequest.h>
#include <aws/lambda/model/PutFunctionConcurrencyResult.h>
#include <aws/lambda/model/GetAccountSettingsRequest.h>
#include <aws/lambda/model/GetAccountSettingsResult.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include <JSONUtils.h>

// only exists in newer SDKs...
// #include <aws/lambda/model/Architecture.h>

// protobuf header
#include <Lambda.pb.h>

#include <third_party/base64/base64.h>

#include <google/protobuf/util/json_util.h>
#include <iomanip>

#include <utility>
#include "ee/worker/WorkerBackend.h"

namespace tuplex {

    AwsLambdaBackend::~AwsLambdaBackend() {
        // stop http requests
        if (_client)
            _client->DisableRequestProcessing();

        // delete scratch dir?
        if (_deleteScratchDirOnShutdown) {
            auto vfs = VirtualFileSystem::fromURI(_scratchDir);
            vfs.remove(_scratchDir); // TODO: could optimize this by keeping track of temp files and issuing on shutdown
            // a single multiobject delete request...
        }
    }

    std::shared_ptr<Aws::Lambda::LambdaClient> AwsLambdaBackend::makeClient() {
        // Note: should have only a SINGLE context with Lambda backend...

        // init Lambda client
        Aws::Client::ClientConfiguration clientConfig;

        clientConfig.requestTimeoutMs = _options.AWS_REQUEST_TIMEOUT() * 1000; // conv seconds to ms
        clientConfig.connectTimeoutMs = _options.AWS_CONNECT_TIMEOUT() * 1000; // connection timeout

        // tune client, according to https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/client-config.html
        // note: max connections should not exceed max concurrency if it is below 100, else aws lambda
        // will return toomanyrequestsexception
        clientConfig.maxConnections = std::max(32ul, _options.AWS_MAX_CONCURRENCY());

        // to avoid thread exhaust of system, use pool thread executor with 8 threads
        clientConfig.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(_tag.c_str(),
                                                                                             _options.AWS_NUM_HTTP_THREADS());
        if (_options.AWS_REGION().empty())
            clientConfig.region = _credentials.default_region.c_str();
        else
            clientConfig.region = _options.AWS_REGION().c_str(); // hard-coded here

        // verify zone
        if (!isValidAWSZone(clientConfig.region.c_str())) {
            logger().warn("Specified AWS zone '" + std::string(clientConfig.region.c_str()) +
                          "' is not a valid AWS zone. Defaulting to " + _credentials.default_region + " zone.");
            clientConfig.region = _credentials.default_region.c_str();
        }

        //clientConfig.userAgent = "tuplex"; // should be perhaps set as well.
        auto ns = _options.AWS_NETWORK_SETTINGS();
        applyNetworkSettings(ns, clientConfig);

        // change aws settings here
        Aws::Auth::AWSCredentials cred(_credentials.access_key.c_str(),
                                       _credentials.secret_key.c_str(),
                                       _credentials.session_token.c_str());
        auto client = Aws::MakeShared<Aws::Lambda::LambdaClient>(_tag.c_str(), cred, clientConfig);

        Aws::Lambda::Model::ListFunctionsRequest list_req;
        const Aws::Lambda::Model::FunctionConfiguration *fc = nullptr; // holds lambda conf
        Aws::String fc_json_str;
        auto outcome = client->ListFunctions(list_req);
        if (!outcome.IsSuccess()) {
            std::stringstream ss;
            ss << outcome.GetError().GetExceptionName().c_str() << ", "
               << outcome.GetError().GetMessage().c_str();

            throw std::runtime_error("LAMBDA failed to list functions, details: " + ss.str());
        } else {
            // check whether function is contained
            auto funcs = outcome.GetResult().GetFunctions();

            // search for the function of interest
            for (const auto &f: funcs) {
                if (f.GetFunctionName().c_str() == _functionName) {
                    fc_json_str = f.Jsonize().View().WriteCompact();
                    fc = new Aws::Lambda::Model::FunctionConfiguration(Aws::Utils::Json::JsonValue(fc_json_str));
                    break;
                }
            }

            if (!fc)
                throw std::runtime_error("could not find lambda function '" + _functionName + "'");
            else {
                logger().info(
                        "Found AWS Lambda function " + _functionName + " (" + std::to_string(fc->GetMemorySize()) +
                        "MB)");
            }
        }

        // check architecture of function (only newer AWS SDKs support this...)
        {
            using namespace Aws::Utils::Json;
            using namespace Aws::Utils;
            auto fc_json = Aws::Utils::Json::JsonValue(fc_json_str);
            if (fc_json.View().ValueExists("Architectures")) {
                Array<JsonView> architecturesJsonList = fc_json.View().GetArray("Architectures");
                std::vector<std::string> architectures;
                for (unsigned architecturesIndex = 0;
                     architecturesIndex < architecturesJsonList.GetLength(); ++architecturesIndex)
                    architectures.push_back(std::string(architecturesJsonList[architecturesIndex].AsString().c_str()));
                // there should be one architecture
                if (architectures.size() != 1) {
                    logger().warn(
                            "AWS Lambda changed specification, update how to deal with mulit-architecture functions");
                    if (!architectures.empty())
                        _functionArchitecture = architectures.front();
                } else {
                    _functionArchitecture = architectures.front();
                }
            } else {
                _functionArchitecture = "x86_64";
            }
        }

        logger().info("Using Lambda running on " + _functionArchitecture);
#ifdef BUILD_WITH_CEREAL
        logger().info("Lambda client uses Cereal AST serialization format.");
#else
        logger().info("Lambda client uses JSON AST serialization format.");
#endif

        // could also check account limits in case:
        // https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.get_function_configuration

        checkAndUpdateFunctionConcurrency(client, _options.AWS_MAX_CONCURRENCY(), _functionName,
                                          false); // no provisioned concurrency!

        // limit concurrency + mem of function manually (TODO: uncomment for faster speed!), if it doesn't fit options
        // i.e. aws lambda put-function-concurrency --function-name tplxlam --reserved-concurrent-executions $MAX_CONCURRENCY
        //      aws lambda update-function-configuration --function-name tplxlam --memory-size $MEM_SIZE --timeout 60
        // update required?
        bool needToUpdateConfig = false;
        Aws::Lambda::Model::UpdateFunctionConfigurationRequest update_req;
        update_req.SetFunctionName(_functionName.c_str());
        if (fc->GetTimeout() != _lambdaTimeOut) {
            update_req.SetTimeout(_lambdaTimeOut);
            needToUpdateConfig = true;
        }

        if (fc->GetMemorySize() != _lambdaSizeInMB) {
            update_req.SetMemorySize(_lambdaSizeInMB);
            needToUpdateConfig = true;
        }
        if (needToUpdateConfig) {
            logger().info("updating Lambda settings to timeout: " + std::to_string(_lambdaTimeOut) + "s memory: " +
                          std::to_string(_lambdaSizeInMB) + " MB");
            auto outcome = client->UpdateFunctionConfiguration(update_req);
            if (!outcome.IsSuccess()) {
                std::stringstream ss;
                ss << outcome.GetError().GetExceptionName().c_str()
                   << outcome.GetError().GetMessage().c_str();

                throw std::runtime_error("LAMBDA failed update configuration, details: " + ss.str());
            }
            logger().info("Updated Lambda configuration successfully.");
        }

        delete fc;

        return client;
    }

    void AwsLambdaBackend::checkAndUpdateFunctionConcurrency(const std::shared_ptr<Aws::Lambda::LambdaClient> &client,
                                                             size_t concurrency,
                                                             const std::string &functionName,
                                                             bool provisioned) {
        if (provisioned) {
            throw std::runtime_error("Provisioned concurrency not yet supported...");
        } else {
            // check concurrency & adjust (may fail with account limits...)
            Aws::Lambda::Model::GetFunctionConcurrencyRequest req;
            req.SetFunctionName(functionName.c_str());
            auto outcome = client->GetFunctionConcurrency(req);
            if (!outcome.IsSuccess()) {
                logger().error("Failed to retrieve function concurrency");
            } else {
                auto &result = outcome.GetResult();
                _functionConcurrency = result.GetReservedConcurrentExecutions();

                // different than what is desired?
                if (_functionConcurrency != concurrency) {
                    auto concurrency_description = std::to_string(_functionConcurrency);
                    if (0 == _functionConcurrency)
                        concurrency_description = "(no reserved concurrency)";
                    logger().info("Adjusting reserved concurrency from " + concurrency_description + " to " +
                                  std::to_string(concurrency));

                    // update function
                    Aws::Lambda::Model::PutFunctionConcurrencyRequest u_req;
                    u_req.SetFunctionName(functionName.c_str());
                    u_req.SetReservedConcurrentExecutions(concurrency);

                    auto outcome = client->PutFunctionConcurrency(u_req);
                    if (!outcome.IsSuccess()) {
                        // check error
                        auto &error = outcome.GetError();
                        auto statusCode = static_cast<int>(error.GetResponseCode());
                        auto errorMessage = std::string(error.GetMessage().c_str());
                        auto errorName = std::string(error.GetExceptionName().c_str());

                        // check if it was invalid param
                        if (error.GetErrorType() == Aws::Lambda::LambdaErrors::INVALID_PARAMETER_VALUE) {
                            // get max allowed concurrency from account & adjust
                            //Aws::Lambda::Model::
                            Aws::Lambda::Model::GetAccountSettingsRequest a_req;
                            auto outcome = client->GetAccountSettings(a_req);
                            if (!outcome.IsSuccess()) {
                                auto &error = outcome.GetError();
                                logger().error(
                                        "Failed to retrieve account settings, can not adjust invalid concurrency configuration. Details: " +
                                        std::string(error.GetMessage().c_str()));
                                return;
                            } else {
                                // check what the limit is, adjust accordingly
                                auto &result = outcome.GetResult();
                                auto unreserved_capacity = result.GetAccountLimit().GetUnreservedConcurrentExecutions();

                                // unreserved must be at least 100
                                if (unreserved_capacity > AWS_MINIMUM_UNRESERVED_CONCURRENCY)
                                    unreserved_capacity -= AWS_MINIMUM_UNRESERVED_CONCURRENCY;
                                else {
                                    throw std::runtime_error(
                                            "No account capacity remaining, can't assign any concurrency to LAMBDA function " +
                                            _functionName);
                                }

                                auto max_account_concurrency = result.GetAccountLimit().GetConcurrentExecutions();
                                auto concurrency_to_assign = unreserved_capacity + _functionConcurrency;
                                std::stringstream ss;
                                ss << """Account has overall concurrency limit of " << max_account_concurrency
                                   << ", remaining capacity of " << unreserved_capacity
                                   << ": Can assign maximum of " << concurrency_to_assign << " to function "
                                   << _functionName << ".";
                                logger().info(ss.str());

                                // assign concurrency to assign
                                Aws::Lambda::Model::PutFunctionConcurrencyRequest u_req;
                                u_req.SetFunctionName(functionName.c_str());
                                u_req.SetReservedConcurrentExecutions(concurrency_to_assign);
                                auto outcome = client->PutFunctionConcurrency(u_req);
                                if (!outcome.IsSuccess()) {
                                    auto &error = outcome.GetError();
                                    auto statusCode = static_cast<int>(error.GetResponseCode());
                                    auto errorMessage = std::string(error.GetMessage().c_str());
                                    auto errorName = std::string(error.GetExceptionName().c_str());
                                    throw std::runtime_error(
                                            "internal error assigning maximum, remaining capacity to Lambda runner.");
                                } else {
                                    logger().info("Used maximum available concurrency of "
                                                  + std::to_string(concurrency_to_assign) + " for Lambda runner.");
                                    _functionConcurrency = concurrency_to_assign;
                                }
                            }
                        }

                        logger().error(
                                "Failed to update concurrency with error " + errorName + ", details: " + errorMessage);
                    } else {
                        auto &result = outcome.GetResult();
                        _functionConcurrency = result.GetReservedConcurrentExecutions();
                        logger().info("Function concurrency adjusted to " + std::to_string(_functionConcurrency));
                    }
                }
            }
        }

        if (0 == _functionConcurrency) {
            // issue message & set dummy limit of 100
            logger().info("Function is treated as unreserved concurrency, thus AWS Limit of " +
                          std::to_string(AWS_MINIMUM_UNRESERVED_CONCURRENCY) + " applies.");
            _functionConcurrency = AWS_MINIMUM_UNRESERVED_CONCURRENCY;
        }
    }

    void AwsLambdaBackend::invokeAsync(const AwsLambdaRequest &req) {

      assert(_service);

      // invoke using callbacks!
      _service->invokeAsync(req, [this](const AwsLambdaRequest& req, const AwsLambdaResponse& resp) { onLambdaSuccess(req, resp); },
                            [this](const AwsLambdaRequest& req, LambdaErrorCode code, const std::string& msg) { onLambdaFailure(req, code, msg); },
                            [this](const AwsLambdaRequest& req, LambdaErrorCode code, const std::string& msg, bool b) { onLambdaRetry(req, code, msg, b); });
    }

    void AwsLambdaBackend::onLambdaFailure(const AwsLambdaRequest &req, LambdaErrorCode err_code,
                                           const std::string &err_msg) {
        // abort all requests
        _service->abortAllRequests(false);

        // print failure as last message!
        logger().error(err_msg);
    }

    void AwsLambdaBackend::onLambdaRetry(const AwsLambdaRequest &req, LambdaErrorCode retry_code,
                                         const std::string &retry_msg, bool decreasesRetryCount) {

        auto retries_left_str = req.retriesLeft == 1 ? "1 retry left" : std::to_string(req.retriesLeft) + " retries left";

        if(decreasesRetryCount) {
            logger().info("LAMBDA retrying task (" + retries_left_str + "), details: " + retry_msg);
        }

        // do not display silent retries unless in debug mode
        else {
            logger().debug("LAMBDA retrying task (" + retries_left_str + "), because of " + retry_msg);
        }
    }

    void AwsLambdaBackend::onLambdaSuccess(const AwsLambdaRequest &req, const AwsLambdaResponse &resp) {

        // message
        std::stringstream  ss;

        auto statusCode = 200; // should be that?

         // this here should go into the success callback!
         ss << "LAMBDA task done in " << resp.response.taskexecutiontime() << "s ";
         std::string container_status = resp.response.container().reused() ? "reused" : "new";
         ss << "[" << statusCode << ", " << pluralize(resp.response.numrowswritten(), "row")
            << ", " << pluralize(resp.response.numexceptions(), "exception") << ", "
            << container_status << ", id: " << resp.response.container().uuid() << "] ";

        // compute cost and print out
        ss << "Cost so far: $";
        double price = lambdaCost();
        if (price < 0.01)
            ss.precision(4);
        if (price < 0.0001)
            ss.precision(6);
        ss << std::fixed << price;

        logger().info(ss.str());

        // lock & save response!
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _tasks.push_back(resp);
        }
    }

//    std::set<std::string> AwsLambdaBackend::performWarmup(const std::vector<int> &countsToInvoke,
//                                                          size_t timeOutInMs,
//                                                          size_t baseDelayInMs) {
//
//        //            size_t numWarmingRequests = 50;
//        std::set<std::string> containerIds;
//        logger().info("Warming up containers...");
//        // do a single synced request (else reuse will occur!)
//        // Tuplex request
//        messages::InvocationRequest req;
//        req.set_type(messages::MessageType::MT_WARMUP);
//
//        // specific warmup message contents
//        auto wm = std::make_unique<messages::WarmupMessage>();
//        wm->set_timeoutinms(timeOutInMs);
//        wm->set_basedelayinms(baseDelayInMs);
//        for (auto count: countsToInvoke)
//            wm->add_invocationcount(count);
//        req.set_allocated_warmup(wm.release());
//
//        // construct req object
//        Aws::Lambda::Model::InvokeRequest invoke_req;
//        invoke_req.SetFunctionName(_functionName.c_str());
//        invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
//        // logtype to extract log data??
//        //req.SetLogtype(Aws::Lambda::Model::LogType::None);
//        invoke_req.SetLogType(Aws::Lambda::Model::LogType::Tail);
//        std::string json_buf;
//        google::protobuf::util::MessageToJsonString(req, &json_buf);
//        invoke_req.SetBody(stringToAWSStream(json_buf));
//        invoke_req.SetContentType("application/javascript");
//
//        // perform synced (!) invoke.
//        auto outcome = _client->Invoke(invoke_req);
//        if (outcome.IsSuccess()) {
//
//            // write response
//            auto &result = outcome.GetResult();
//            auto statusCode = result.GetStatusCode();
//            std::string version = result.GetExecutedVersion().c_str();
//            auto response = parsePayload(result);
//
//            auto log = result.GetLogResult();
//
//            if (response.status() == messages::InvocationResponse_Status_SUCCESS) {
//                // extract info
//                auto info = RequestInfo::parseFromLog(log.c_str());
//                info.fillInFromResponse(response);
//                std::stringstream ss;
//                auto &task = response;
//                if (task.type() == messages::MessageType::MT_WARMUP) {
//                    containerIds.insert(task.container().uuid());
//                    for (auto info: task.invokedcontainers())
//                        containerIds.insert(info.uuid());
//                }
//                ss << "Warmup request took " << response.taskexecutiontime() << " s, " << "initialized "
//                   << containerIds.size();
//                logger().info(ss.str());
//            } else {
//                logger().info("Message returned was weird.");
//            }
//
//            logger().info("Warming succeeded.");
//        } else {
//            // failed
//            logger().error("Warming request failed.");
//
//            auto &error = outcome.GetError();
//            auto statusCode = static_cast<int>(error.GetResponseCode());
//            std::string exceptionName = outcome.GetError().GetExceptionName().c_str();
//            std::string errorMessage = outcome.GetError().GetMessage().c_str();
//            // rate limit? => reissue request
//            if (statusCode == static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS) || // i.e. 429
//                statusCode == static_cast<int>(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR)) {  // i.e. 500
//            } else {
//            }
//        }
////            for(unsigned i = 0; i < numWarmingRequests; ++i) {
////
////                // Tuplex request
////                messages::InvocationRequest req;
////                req.set_type(messages::MessageType::MT_WARMUP);
////
////                // specific warmup message contents
////                auto wm = std::make_unique<messages::WarmupMessage>();
////                wm->set_timeoutinms(timeOutInMs);
////                wm->set_invocationcount(numLambdasToInvoke);
////                req.set_allocated_warmup(wm.release());
////
////                invokeAsync(req);
////            }
////            waitForRequests();
//        logger().info("warmup done");
//
//        return containerIds;
//    }

    std::string remove_non_digits(const std::string &s) {
        std::string res;

        std::copy_if(s.begin(), s.end(), std::back_inserter(res), [](char c) {
            return '0' <= c && c <= '9';
        });
        return res;
    }

    std::string nextJobDumpPath(const std::string &root_path) {
        // create root path if not exists
        auto vfs = VirtualFileSystem::fromURI(root_path);
        vfs.create_dir(root_path);

        // check whether job_0001.json etc. exists
        auto uris = vfs.globAll(URI(root_path).join_path("job_*.json").toString());

        if (uris.empty())
            return URI(root_path).join_path("job_0000.json").toString();
        unsigned max_job_no = 0;
        for (auto uri: uris) {
            auto name = uri.basename();
            unsigned num = std::stoi(remove_non_digits(name));
            max_job_no = std::max(num, max_job_no);
        }
        return URI(root_path).join_path("job_" + fixedLength(max_job_no + 1, 4) + ".json").toString();
    }

    void AwsLambdaBackend::execute(PhysicalStage *stage) {
        using namespace std;

        _startTimestamp = current_utc_timestamp();

        reset();

        if (_options.USE_WEBUI()) {
            // add job to WebUI
            _historyServer.reset();
            _historyServer = HistoryServerConnector::registerNewJob(_historyConn,
                                                                    "AWS Lambda backend", stage->plan(), _options);
            if (_historyServer) {
                logger().info("track job under " + _historyServer->trackURL());
                _historyServer->sendStatus(JobStatus::STARTED);
            }
        }

        // Notes:
        // ==> could use the warm up events for sampling & speed detection
        // ==> helps to plan the query more efficiently!

        // perform warmup phase if desired (only for first stage?)
//        if(_options.AWS_LAMBDA_SELF_INVOCATION()) {
//            // issue a couple self-invoke requests...
//            Timer timer;
//
//            // warmup in multiple steps (for a maximum time...)
//            std::set<std::string> containerIds;
////            for(int i = 0; i < 10; ++i) {
//                logger().info("Performing warmup.");
//                auto before_count = containerIds.size();
//                auto ids = performWarmup({20, 10, 4}); // 800 total invocations??
//                for(auto id : ids)
//                    containerIds.insert(id);
//                auto after_count = containerIds.size();
//                logger().info("Warmup gave " + std::to_string(after_count - before_count) + " new IDs (" + std::to_string(after_count) + " total)");
////            }
//
//            logger().info("Warmup yielded " + pluralize(containerIds.size(), "container id"));
//            logger().info("Warmup took: " + std::to_string(timer.time()));
//
//            reset();
//            exit(0);
//        }

        auto tstage = dynamic_cast<TransformStage *>(stage);
        if (!tstage)
            throw std::runtime_error("only transform stage from AWS Lambda backend yet supported");

        vector<tuple<std::string, size_t>> uri_infos;

        // decode data from stage
        // -> i.e. could be memory or file, so far only files are supported!
        if (tstage->inputMode() != EndPointMode::FILE) {
            // the data needs to get somehow transferred from the local driver to the cloud
            // Either it could be passed directly via the request OR via S3.

            // For now, use S3 for simplicity...
            auto s3tmp_uri = scratchDir(hintsFromTransformStage(tstage));
            if (s3tmp_uri == URI::INVALID) {
                throw std::runtime_error("could not find/create AWS Lambda scratch dir.");

            }
            // need to transfer the Tuplex partitions to S3
            // -> which format?
            switch (tstage->inputMode()) {
                case EndPointMode::MEMORY: {
                    // simply save to S3!
                    // @TODO: larger/smaller files?
                    Timer timer;
                    int partNo = 0;
                    auto num_partitions = tstage->inputPartitions().size();
                    auto num_digits = ilog10c(num_partitions);
                    size_t total_uploaded = 0;
                    for (auto p: tstage->inputPartitions()) {
                        // lock each and write to S3!
                        // save input URI and size!
                        auto part_uri = s3tmp_uri.join_path("input_part_" + fixedLength(partNo, num_digits) + ".mem");
                        auto vfs = VirtualFileSystem::fromURI(part_uri);
                        auto vf = vfs.open_file(part_uri, VirtualFileMode::VFS_OVERWRITE);

                        // @TODO: setMIMEtype?

                        if (!vf)
                            throw std::runtime_error("could not open file " + part_uri.toString());
                        auto buf = p->lockRaw();
                        auto buf_size = p->bytesWritten();
                        if (!buf_size)
                            buf_size = p->size();
                        vf->write(buf, buf_size);
                        logger().info("Uploading " + sizeToMemString(buf_size) + " to AWS Lambda cache dir");
                        vf->close();
                        p->unlock();
                        p->invalidate();
                        total_uploaded += buf_size;
                        uri_infos.push_back(make_tuple(part_uri.toString(), buf_size));
                        partNo++;
                    }

                    logger().info("Upload done, " + sizeToMemString(total_uploaded) +
                                  " in total transferred to " + s3tmp_uri.toString() + ", took " +
                                  std::to_string(timer.time()) + "s");
                    break;
                }
                default: {
                    throw std::runtime_error("unsupported endpoint inputmode in AWS Lambda Backend, not supported yet");
                }
            }
        } else {
            // simply decode uris from input partitions...
            uri_infos = decodeFileURIs(tstage->inputPartitions());
        }

        std::string optimizedBitcode = "";
        // optimize at client @TODO: optimize for target triple?
        if (_options.USE_LLVM_OPTIMIZER()) {
            Timer timer;
            bool optimized_ir = false;
            // optimize in parallel???
            if (!tstage->fastPathCode().empty()) {

                assert(tstage->fastPathCodeFormat() == codegen::CodeFormat::LLVM_IR_BITCODE);
                llvm::LLVMContext ctx;
                LLVMOptimizer opt;
                auto mod = codegen::bitCodeToModule(ctx, tstage->fastPathCode());
                opt.optimizeModule(*mod);
                optimizedBitcode = codegen::moduleToBitCodeString(*mod);
                optimized_ir = true;
            } else if (!tstage->slowPathCode().empty()) {
                // todo...
            }
            if (optimized_ir)
                logger().info("client-side LLVM IR optimization of took " + std::to_string(timer.time()) + "s");
        } else {
            optimizedBitcode = tstage->fastPathCode();
        }

        if (stage->outputMode() == EndPointMode::MEMORY) {
            // check whether scratch dir exists.
            auto scratch = scratchDir(hintsFromTransformStage(tstage));
            if (scratch == URI::INVALID) {
                throw std::runtime_error(
                        "temporaty AWS Lambda scratch dir required to write output, please specify via tuplex.aws.scratchDir key");
                return;
            }
        }

        // Worker config variables
        size_t numThreads = 1;
        // check what setting is given for threads
        if (_options.AWS_LAMBDA_THREAD_COUNT() == "auto") {
            numThreads = core::ceilToMultiple(_options.AWS_LAMBDA_MEMORY(), 1792ul) /
                         1792ul; // 1792MB is one vCPU. Use the 200+ for rounding.
            logger().debug("Given Lambda size of " + std::to_string(_options.AWS_LAMBDA_MEMORY()) + "MB, use " +
                           pluralize(numThreads, "thread"));
        } else {
            numThreads = std::stoi(_options.AWS_LAMBDA_THREAD_COUNT());
        }
        logger().info("Lambda executors each use " + pluralize(numThreads, "thread"));
        auto spillURI = _options.AWS_SCRATCH_DIR() + "/spill_folder";
        // perhaps also use:  - 64 * numThreads ==> smarter buffer scaling necessary.
        size_t buf_spill_size = (_options.AWS_LAMBDA_MEMORY() - 256) / numThreads * 1000 * 1024;

        // limit to 128mb each
        if (buf_spill_size > 128 * 1000 * 1024)
            buf_spill_size = 128 * 1000 * 1024;

        logger().info("Setting buffer size for each thread to " + sizeToMemString(buf_spill_size));

        Timer timer;

        // create requests depending on execution strategy
        vector<AwsLambdaRequest> requests;
        switch (stringToAwsExecutionStrategy(_options.AWS_LAMBDA_INVOCATION_STRATEGY())) {
            case AwsLambdaExecutionStrategy::DIRECT: {
                requests = createSingleFileRequests(tstage, optimizedBitcode, numThreads, uri_infos, spillURI,
                                                    buf_spill_size);
                break;
            }
            case AwsLambdaExecutionStrategy::TREE: {

                // configure
                RequestConfig conf;

                // check how large input data is in total
                size_t total_size = 0;
                for (auto info: uri_infos) {
                    total_size += std::get<1>(info);
                }

                size_t max_lambda_parallelism = _options.AWS_MAX_CONCURRENCY();
                size_t max_parallelism = numThreads * max_lambda_parallelism;
                size_t chunk_lambda_size = total_size / max_lambda_parallelism;
                size_t chunk_size = total_size / max_parallelism;
                {
                    std::stringstream ss;
                    ss<<"found "<<sizeToMemString(total_size)
                      <<" to process in total, with maximum parallelism of "<<max_parallelism
                      <<" ("<<pluralize(max_lambda_parallelism, "lambda")
                      <<") split into chunks of "<<sizeToMemString(chunk_size)<<" ("<<sizeToMemString(chunk_lambda_size)<<" per lambda).";
                    logger().info(ss.str());
                }

                conf.maximum_lambda_process_size = chunk_lambda_size;
                conf.spillURI = spillURI;
                conf.specialization_unit_size = _options.EXPERIMENTAL_SPECIALIZATION_UNIT_SIZE();
                conf.minimum_size_to_specialize = _options.EXPERIMENTAL_MINIMUM_SIZE_TO_SPECIALIZE();
                conf.buf_spill_size = buf_spill_size;
                auto requests = createSpecializingSelfInvokeRequests(tstage, optimizedBitcode, numThreads, uri_infos, conf);

                throw std::runtime_error("not yet supported");
//                requests = createSelfInvokingRequests(tstage, optimizedBitcode, numThreads, uri_infos, spillURI,
//                                                      buf_spill_size);
                break;
            }
            default:
                logger().error("Unknown execution strategy");
                throw std::runtime_error("unknown invocation strategy '" + _options.AWS_LAMBDA_INVOCATION_STRATEGY() + "', abort");
                break;
        }
        if (!requests.empty()) {
            logger().info("Invoking " + pluralize(requests.size(), "request") + " ...");

#ifndef NDEBUG
            logger().debug("Emitting request files for easier debugging...");
            for(unsigned i = 0; i < requests.size(); ++i) {
                std::string json_buf;
                google::protobuf::util::MessageToJsonString(requests[i].body, &json_buf);
                stringToFile(URI("request_" + std::to_string(i) + ".json"), json_buf);
            }
            logger().debug("Debug files written (" + pluralize(requests.size(), "file") + ").");
#endif

            for (const auto &req: requests)
                invokeAsync(req);
            logger().info("LAMBDA requesting took " + std::to_string(timer.time()) + "s");
        } else {
            logger().warn("No requests generated, skipping stage.");
        }

        // TODO: check signals, allow abort...

        // wait till everything finished computing
        assert(_service);
        _service->waitForRequests();
        gatherStatistics();

        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> ecounts;

        // collect exception counts from stage
        {
            std::lock_guard<std::mutex> lock(_mutex);

            // aggregate stats over responses
            for (const auto &task: _tasks) {
                // @TODO: response needs to contain exception info incl. traceback (?)
                for (const auto &keyval: task.response.exceptioncounts()) {
                    // decode from message
                    // auto key = std::get<0>(keyval.first) << 32 | std::get<1>(keyval.first);
                    // auto key = std::make_tuple(exceptionOperatorID, exceptionCode);
                    int64_t opID = keyval.first >> 32;
                    int64_t ecCode = keyval.first & 0xFFFFFFFF;
                    auto key = std::make_tuple(opID, i64ToEC(ecCode));
                    ecounts[key] += keyval.second;

                    std::cout << "opID=" << opID << " ecCode=" << ecCode << " #: " << keyval.second << std::endl;
                }
            }
        }

        // check here whether all files where successfully processed or not!
        // -> reissue requests for missing files...!

        // save request end! --> i.e. synchronization points!
        _endTimestamp = current_utc_timestamp();


        // check if any remote files are required to be downloaded
        if(!_remoteToLocalURIMapping.empty()) {
            Timer fetchTimer;
            logger().info("Downloading remote files to local machine.");

            for(auto kv : _remoteToLocalURIMapping) {
                logger().info("remote -> local: " + kv.first.toPath() + " -> " + kv.second.toPath());
            }

            for(auto task : _tasks) {
                // check output uris and their mapping
                for(auto output_uri : task.response.outputuris()) {
                    logger().info("output uri: " + output_uri);

                    // fetch file
                    auto it = _remoteToLocalURIMapping.find(output_uri);
                    if(it == _remoteToLocalURIMapping.end()) {
                        logger().warn("could not find output uri " + output_uri + " in remote -> local mapping, skipping.");
                    } else {
                        // get basename of remote uri and map to local (join with parent)
                        auto remote_basename = URI(it->first).basename();
                        auto local_parent = it->second.parent();
                        auto target_path = local_parent.join(remote_basename).toPath();
                        logger().info("storing " + it->first.toPath() + " to " + target_path);
                        VirtualFileSystem::s3DownloadFile(it->first, target_path);
                    }
                }
            }
            logger().info("Fetching files from remote took " + std::to_string(fetchTimer.time()) + "s");
        }

        auto path = nextJobDumpPath("job");
        dumpAsJSON(path);
        logger().info("dumped job info as JSON to " + path);

        {
            std::stringstream ss;
            ss << "LAMBDA compute took " << timer.time() << "s";
            double cost = lambdaCost();
            if (cost < 0.01)
                ss << ", cost < $0.01";
            else
                ss << std::fixed << std::setprecision(2) << ", cost $" << cost;
            logger().info(ss.str());
        }

        // generate here csv with information per file
        {
            auto csv_info = csvPerFileInfo();
            logger().info("\nper-file information:\n" + csv_info + "\n");
        }

        // @TODO: results sets etc.
        switch (tstage->outputMode()) {
            case EndPointMode::FILE: {
                tstage->setFileResult(ecounts);
                break;
            }

            case EndPointMode::MEMORY: {
                // fetch from outputs, alloc partitions and set
                // tstage->setMemoryResult()
                Timer timer;
                vector<URI> output_uris;
                for (const auto &task: _tasks) {
                    for (const auto &uri: task.response.outputuris())
                        output_uris.push_back(uri);
                }
                // sort after part no @TODO
                std::sort(output_uris.begin(), output_uris.end(), [](const URI &a, const URI &b) {
                    return a.toString() < b.toString();
                });

                // download and store each part in one partition (TODO: resize etc.)
                vector<Partition *> output_partitions;
                int partNo = 0;
                int num_digits = ilog10c(output_uris.size());
                vector<URI> local_paths;
                for (const auto &uri: output_uris) {
                    // download to local scratch dir
                    auto local_path = _options.SCRATCH_DIR().join_path("aws-part" + fixedLength(partNo, num_digits));
                    VirtualFileSystem::copy(uri.toString(), local_path);
                    local_paths.push_back(local_path);
                    partNo++;
                }
                logger().info(
                        "fetching results from " + scratchDir().toString() + " took " + std::to_string(timer.time()) +
                        "s");

                // convert to partitions
                timer.reset();
                for (auto path: local_paths) {
                    auto vf = VirtualFileSystem::fromURI(path).open_file(path, VirtualFileMode::VFS_READ);
                    if (!vf) {
                        throw std::runtime_error("could not read locally cached file " + path.toString());
                    }

                    auto file_size = vf->size();
                    size_t bytesRead = 0;

                    // alloc new driver partition
                    Partition *partition = _driver->allocWritablePartition(file_size, tstage->outputSchema(),
                                                                           tstage->outputDataSetID(),
                                                                           stage->context().id());
                    auto ptr = partition->lockWrite();
                    int64_t bytesWritten = file_size;
                    int64_t numRows = 0;
                    vf->read(&bytesWritten, sizeof(int64_t), &bytesRead);
                    vf->read(&numRows, sizeof(int64_t), &bytesRead);
                    vf->read(ptr, file_size, &bytesRead);
                    partition->unlockWrite();
                    partition->setNumRows(numRows);
                    partition->setBytesWritten(bytesWritten);
                    vf->close();

                    logger().debug("read " + sizeToMemString(bytesRead) + " to a single partition");
                    output_partitions.push_back(partition);

                    // remove local file @TODO: could be done later to be more efficient, faster...
                    VirtualFileSystem::remove(path);
                }
                logger().info("Loading S3 results into driver took " + std::to_string(timer.time()) + "s");
                tstage->setMemoryResult(output_partitions);

                break;
            }

            default: {
                throw std::runtime_error("other end points then memory/file via S3 not yet implemented");
            }
        }


        // if webui, send job end (single stage right now only supported)
        if (_historyServer)
            _historyServer->sendStatus(JobStatus::FINISHED);
    }


    // recurse depth:
    // 2^n?
    // 3^n?

    std::vector<messages::InvocationRequest>
    AwsLambdaBackend::createSelfInvokingRequests(const TransformStage *tstage, const std::string &bitCode,
                                                 const size_t numThreads,
                                                 const std::vector<std::tuple<std::string, std::size_t>> &uri_infos,
                                                 const std::string &spillURI, const size_t buf_spill_size) {

        // how many files are there? What's the total size?
        std::vector<messages::InvocationRequest> requests;

        size_t total_size = 0;
        for (auto info: uri_infos) {
            total_size += std::get<1>(info);
        }
        logger().info("Creating self-invoking requests for " + pluralize(uri_infos.size(), "file") + " - " +
                      sizeToMemString(total_size));

        // create a request which invokes Lambdas recursively?
        // for now simply let one Lambda invoke all the others
        std::vector<size_t> recursive_invocations;

        // // Strategy I:
        // // one lambda per file?
        // if(uri_infos.size() > 2)
        //     recursive_invocations.push_back(uri_infos.size() - 1);

        // Strategy II: use concurrency setting!
        //recursive_invocations.push_back(_functionConcurrency - 1);

        // just use  2 -> 2 -> 2
        recursive_invocations = std::vector<size_t>{1, 1,
                                                    1}; // 2 * 2 * 2 -> 8 invocations. still need to figure out the part naming.

        // test, use 5 x 4 --> spawns 512 Lambdas.
        recursive_invocations = std::vector<size_t>{4, 4, 4, 4, 4}; // 512 Lambdas?

        recursive_invocations = std::vector<size_t>{200, 4};

        // when concurrency is < 200, use simple invocation strategy.
        // AWS EMR compatible setting.

        // always should split into MORE parts than function concurrency in order
        // to max out everything...
        recursive_invocations = std::vector<size_t>{2 * _functionConcurrency - 1};

        // transform to request
        messages::InvocationRequest req;
        req.set_type(messages::MessageType::MT_TRANSFORM);
        auto pb_stage = tstage->to_protobuf();
        for (auto count: recursive_invocations)
            pb_stage->add_invocationcount(count);

        req.set_allocated_stage(pb_stage.release());

        // add request for this
        for (auto info: uri_infos) {
            auto inputURI = std::get<0>(info);
            auto inputSize = std::get<1>(info);
            req.add_inputuris(inputURI);
            req.add_inputsizes(inputSize);
        }

        // worker config
        auto ws = std::make_unique<messages::WorkerSettings>();
        throw std::runtime_error("ERROR: need unique spill URIS");
        config_worker(ws.get(), _options, numThreads, spillURI, buf_spill_size);
        req.set_allocated_settings(ws.release());

        // partNo offset
        req.set_partnooffset(0); // single request!

        // output uri of job? => final one? parts?
        // => create temporary if output is local! i.e. to memory etc.
        int taskNo = 0;
        int num_digits = 5;
        if (tstage->outputMode() == EndPointMode::MEMORY) {
            // create temp file in scratch dir!
            req.set_baseoutputuri(scratchDir(hintsFromTransformStage(tstage)).join_path(
                    "output.part" + fixedLength(taskNo, num_digits)).toString());
        } else if (tstage->outputMode() == EndPointMode::FILE) {
            // create output URI based on taskNo
            auto uri = outputURI(tstage->outputPathUDF(), tstage->outputURI(), taskNo, tstage->outputFormat());
            req.set_baseoutputuri(uri.toPath());
        } else if (tstage->outputMode() == EndPointMode::HASHTABLE) {
            throw std::runtime_error("join, aggregate not yet supported in lambda backend");
        } else throw std::runtime_error("unknown output endpoint in lambda backend");
        requests.push_back(req);

        logger().info("Created " + pluralize(requests.size(), "LAMBDA request") + +".");
        return requests;
    }

    static AwsLambdaRequest create_tree_based_hyperspecialization_request(const TransformStage* tstage,
                                                                          const std::string& bitCode,
                                                                          const URI& uri, size_t uri_size,
                                                                          size_t rangeStart, size_t rangeEnd,
                                                                          const ContextOptions& options,
                                                                          std::unordered_map<URI,URI>& remoteToLocalURIMapping
                                                                          ) {
//        assert(tstage);
//
//        messages::InvocationRequest req;
//        req.set_type(messages::MessageType::MT_TRANSFORM);
//        auto pb_stage = tstage->to_protobuf();
//
//        // auto rangeStart = cur_size;
//        // auto rangeEnd = std::min(cur_size + splitSize, uri_size);
//
//        // clamp rangeEnd
//        rangeEnd = std::min(rangeEnd, uri_size);
//
//        pb_stage->set_bitcode(bitCode);
//
//        req.set_allocated_stage(pb_stage.release());
//
//        // add request for this
//        auto inputURI = uri.toString();
//        auto inputSize = uri_size;
//        inputURI += ":" + std::to_string(rangeStart) + "-" + std::to_string(rangeEnd);
//        req.add_inputuris(inputURI);
//        req.add_inputsizes(inputSize);
//
//        // worker config
//        auto ws = std::make_unique<messages::WorkerSettings>();
//        auto worker_spill_uri = spillURI + "/lam" + fixedPoint(i, num_digits) + "/" + fixedPoint(part_no, num_digits_part);
//        config_worker(ws.get(), options, numThreads, worker_spill_uri, buf_spill_size);
//        req.set_allocated_settings(ws.release());
//        req.set_verboselogging(options.AWS_VERBOSE_LOGGING());
//
//        // output uri of job? => final one? parts?
//        // => create temporary if output is local! i.e. to memory etc.
//        int taskNo = i;
//        if (tstage->outputMode() == EndPointMode::MEMORY) {
//            // create temp file in scratch dir!
//            req.set_baseoutputuri(scratchDir(hintsFromTransformStage(tstage)).join_path(
//                    "output.part" + fixedLength(taskNo, num_digits) + "_" +
//                    fixedLength(part_no, num_digits_part)).toString());
//        } else if (tstage->outputMode() == EndPointMode::FILE) {
//            // create output URI based on taskNo
//            auto uri = outputURI(tstage->outputPathUDF(), tstage->outputURI(), taskNo, tstage->outputFormat());
//            auto remote_output_uri = uri.toPath();
//
//            // is it a local file URI as target? if so, generate dummy uri under spill folder for this job & add mapping
//            if(uri.isLocal()) {
//                auto tmp_uri = scratchDir(hintsFromTransformStage(tstage)).join_path(
//                        "output.part" + fixedLength(taskNo, num_digits) + "_" +
//                        fixedLength(part_no, num_digits_part)).toString();
//
//                remote_output_uri = tmp_uri;
//
//                // add extension to mapping
//                auto output_fmt = tstage->outputFormat();
//                auto ext = defaultFileExtension(output_fmt);
//                tmp_uri += "." + ext; // done in worker app, fix in the future.
//                remoteToLocalURIMapping[tmp_uri] = uri;
//            }
//
//            req.set_baseoutputuri(remote_output_uri);
//        } else if (tstage->outputMode() == EndPointMode::HASHTABLE) {
//            // there's two options now, either this is an end-stage (i.e., unique/aggregateByKey/...)
//            // or an intermediate stage where a temp hash-table is required.
//            // in any case, because compute is done on Lambda materialize hash-table as temp file.
//            auto temp_uri = tempStageURI(tstage->number());
//            req.set_baseoutputuri(temp_uri.toString());
//        } else throw std::runtime_error("unknown output endpoint in lambda backend");

        AwsLambdaRequest aws_req;
//        aws_req.body = req;
        aws_req.retriesLeft = 5;

        return aws_req;
    }

    std::vector<AwsLambdaRequest>
    AwsLambdaBackend::createSpecializingSelfInvokeRequests(const TransformStage *tstage, const std::string &bitCode,
                                                           const size_t numThreads,
                                                           const std::vector<std::tuple<std::string, std::size_t>> &uri_infos,
                                                           const RequestConfig &conf) {
        // check config is valid
        if(!conf.valid()) {
            throw std::runtime_error("given request config is not valid.");
        }

        std::vector<AwsLambdaRequest> requests;
        std::vector<URI> request_uris;

        // now generation is quite simple. Go over files, and then check wrt to specialization size if they're large enough to get specialized on. Then issue appropriate number of requests.
        for(auto uri_info : uri_infos) {
            auto input_uri = std::get<0>(uri_info);
            auto input_uri_size = std::get<1>(uri_info);

            if(input_uri_size >= conf.minimum_size_to_specialize) {
                // issue specialization query!

                // how many queries to issue? could be that input uri needs to be split up into multiple files...!
                // -> make sure each part is at least specialization unit size
                assert(conf.specialization_unit_size >= conf.minimum_size_to_specialize);

                size_t bytes_so_far = 0;
                while(bytes_so_far < input_uri_size) {
                    // part from bytes_so_far, bytes_so_far + specialization_unit_size

                    // is this is the last part?
                    if(bytes_so_far + 2 * conf.specialization_unit_size < input_uri_size) {
                        request_uris.push_back(encodeRangeURI(input_uri, bytes_so_far, bytes_so_far + conf.specialization_unit_size));

                        auto uri = input_uri;
                        auto uri_size = input_uri_size;
                        auto range_start = bytes_so_far;
                        auto range_end = bytes_so_far + conf.specialization_unit_size;

                        create_tree_based_hyperspecialization_request(tstage, bitCode, uri, uri_size,
                                                                      range_start, range_end,
                                                                      _options,
                                                                      _remoteToLocalURIMapping);
                        bytes_so_far += conf.specialization_unit_size;
                    } else {
                        // last part, issue and then that's it
                        request_uris.push_back(encodeRangeURI(input_uri, bytes_so_far, input_uri_size));
                        // create_tree_based_hyperspecialization_request(...)
#warning "need to implement stuff here..."
                        break;
                        bytes_so_far = input_uri_size;
                    }

                    bytes_so_far += conf.specialization_unit_size;
                }
            } else {
                // not large enough to warrant specialization. process in parallel
                // i.e. just keep query as is, do not issue specialization request.
                // create_tree_based_regular_request(...)
#warning "need to implement stuff here..."
                throw std::runtime_error("not yet supported");
            }
        }

        logger().info("Created " + std::to_string(requests.size()) + " LAMBDA requests.");
        // how many thereof are regular? how many hperspecialziing?
        // "thereof {} regular / {} specializing"

        return {};
    }

    std::vector<AwsLambdaRequest>
    AwsLambdaBackend::createSingleFileRequests(const TransformStage *tstage,
                                               const std::string &bitCode,
                                               const size_t numThreads,
                                               const std::vector<std::tuple<std::string, std::size_t>> &uri_infos,
                                               const std::string &spillURI,
                                               const size_t buf_spill_size) {

        std::vector<AwsLambdaRequest> requests;

        size_t splitSize = _options.INPUT_SPLIT_SIZE();
        size_t total_size = 0;
        for (auto info: uri_infos) {
            total_size += std::get<1>(info);
        }

        logger().info("Found " + pluralize(uri_infos.size(), "uri")
                      + " with total size = " + sizeToMemString(total_size) + " (split size=" +
                      sizeToMemString(splitSize) + ")");

        // new: part based
        // Note: for now, super simple: 1 request per file (this is inefficient, but whatever)
        // @TODO: more sophisticated splitting of workload!
        Timer timer;
        int num_digits = ilog10c(uri_infos.size());
        for (int i = 0; i < uri_infos.size(); ++i) {
            auto info = uri_infos[i];

            // the smaller
            auto uri_size = std::get<1>(info);
            auto num_parts_per_file = uri_size / splitSize;

            auto num_digits_part = ilog10c(num_parts_per_file + 1);
            int part_no = 0;
            size_t cur_size = 0;
            while (cur_size < uri_size) {
                messages::InvocationRequest req;
                req.set_type(messages::MessageType::MT_TRANSFORM);
                auto pb_stage = tstage->to_protobuf();

                auto rangeStart = cur_size;
                auto rangeEnd = std::min(cur_size + splitSize, uri_size);

                req.set_allocated_stage(pb_stage.release());

                // add request for this
                auto inputURI = std::get<0>(info);
                auto inputSize = std::get<1>(info);
                inputURI += ":" + std::to_string(rangeStart) + "-" + std::to_string(rangeEnd);
                req.add_inputuris(inputURI);
                req.add_inputsizes(inputSize);

                // worker config
                auto ws = std::make_unique<messages::WorkerSettings>();
                auto worker_spill_uri = spillURI + "/lam" + fixedPoint(i, num_digits) + "/" + fixedPoint(part_no, num_digits_part);
                config_worker(ws.get(), _options, numThreads, worker_spill_uri, buf_spill_size);
                req.set_allocated_settings(ws.release());
                req.set_verboselogging(_options.AWS_VERBOSE_LOGGING());

                // output uri of job? => final one? parts?
                // => create temporary if output is local! i.e. to memory etc.
                int taskNo = i;
                if (tstage->outputMode() == EndPointMode::MEMORY) {
                    // create temp file in scratch dir!
                    req.set_baseoutputuri(scratchDir(hintsFromTransformStage(tstage)).join_path(
                            "output.part" + fixedLength(taskNo, num_digits) + "_" +
                            fixedLength(part_no, num_digits_part)).toString());
                } else if (tstage->outputMode() == EndPointMode::FILE) {
                    // create output URI based on taskNo
                    auto uri = outputURI(tstage->outputPathUDF(), tstage->outputURI(), taskNo, tstage->outputFormat());
                    auto remote_output_uri = uri.toPath();

                    // is it a local file URI as target? if so, generate dummy uri under spill folder for this job & add mapping
                    if(uri.isLocal()) {
                        auto tmp_uri = scratchDir(hintsFromTransformStage(tstage)).join_path(
                                "output.part" + fixedLength(taskNo, num_digits) + "_" +
                                fixedLength(part_no, num_digits_part)).toString();

                        remote_output_uri = tmp_uri;

                        // add extension to mapping
                        auto output_fmt = tstage->outputFormat();
                        auto ext = defaultFileExtension(output_fmt);
                        tmp_uri += "." + ext; // done in worker app, fix in the future.
                        _remoteToLocalURIMapping[tmp_uri] = uri;
                    }

                    req.set_baseoutputuri(remote_output_uri);
                } else if (tstage->outputMode() == EndPointMode::HASHTABLE) {
                    // there's two options now, either this is an end-stage (i.e., unique/aggregateByKey/...)
                    // or an intermediate stage where a temp hash-table is required.
                    // in any case, because compute is done on Lambda materialize hash-table as temp file.
                    auto temp_uri = tempStageURI(tstage->number());
                    req.set_baseoutputuri(temp_uri.toString());
                } else throw std::runtime_error("unknown output endpoint in lambda backend");



                AwsLambdaRequest aws_req;
                aws_req.body = req;
                aws_req.retriesLeft = 5;
                requests.push_back(aws_req);

                part_no++;
                cur_size += splitSize;
            }
        }

        logger().info("Created " + std::to_string(requests.size()) + " LAMBDA requests.");

        return requests;
    }

    AwsLambdaBackend::AwsLambdaBackend(const Context &context,
                                       const AWSCredentials &credentials,
                                       const std::string &functionName) : IBackend(context), _credentials(credentials),
                                                                          _functionName(functionName),
                                                                          _options(context.getOptions()),
                                                                          _logger(Logger::instance().logger(
                                                                                  "aws-lambda")), _tag("tuplex"),
                                                                          _client(nullptr),
                                                                          _startTimestamp(0),
                                                                          _endTimestamp(0) {


        _deleteScratchDirOnShutdown = false;
        _scratchDir = URI::INVALID;

        // // check that scratch dir is s3 path!
        // if(options.SCRATCH_DIR().prefix() != "s3://") // @TODO: check further it's a dir...
        //     throw std::runtime_error("need to provide as scratch dir an s3 path to Lambda backend");

        initAWS(credentials, _options.AWS_NETWORK_SETTINGS(), _options.AWS_REQUESTER_PAY());

        // several options are NOT supported currently in AWS Lambda Backend, hence
        // force them to what works
        if (_options.OPT_GENERATE_PARSER()) {
            logger().warn(
                    "using generated CSV parser not yet supported in AWS Lambda backend, defaulting back to original parser");
            _options.set("tuplex.optimizer.generateParser", "false");
        }
//        if(_options.OPT_NULLVALUE_OPTIMIZATION()) {
//            logger().warn("null value optimization not yet available for AWS Lambda backend, deactivating.");
//            _options.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
//        }

        _driver.reset(new Executor(_options.DRIVER_MEMORY(),
                                   _options.PARTITION_SIZE(),
                                   _options.RUNTIME_MEMORY(),
                                   _options.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE(),
                                   _options.SCRATCH_DIR(), "aws-local-driver"));

        _lambdaSizeInMB = _options.AWS_LAMBDA_MEMORY();
        _lambdaTimeOut = _options.AWS_LAMBDA_TIMEOUT();

        logger().info("Execution over lambda with " + std::to_string(_lambdaSizeInMB) + "MB");

        // Lambda supports 1MB increments. Hence, no adjustment to 64MB granularity anymore necessary as in prior to Dec 2020.
        _lambdaSizeInMB = std::min(std::max(AWS_MINIMUM_LAMBDA_MEMORY_MB, _lambdaSizeInMB),
                                   AWS_MAXIMUM_LAMBDA_MEMORY_MB);
        logger().info("Adjusted lambda size to " + std::to_string(_lambdaSizeInMB) + "MB");

        if (_lambdaTimeOut < 10 || _lambdaTimeOut > 15 * 60) {
            _lambdaTimeOut = std::min(std::max(AWS_MINIMUM_TUPLEX_TIMEOUT_REQUIREMENT, _lambdaTimeOut),
                                      AWS_MAXIMUM_LAMBDA_TIMEOUT); // min 5s, max 15min
            logger().info("Adjusted lambda timeout to " + std::to_string(_lambdaTimeOut));
        }

        // init lambda client (Note: must be called AFTER aws init!)
        _client = makeClient();

        // init invocation service
        _service.reset(new AwsLambdaInvocationService(_client, functionName));

        if (_options.USE_WEBUI()) {

            TUPLEX_TRACE("initializing REST/Curl interface");
            // init rest interface if required (check if already done by AWS!)
            RESTInterface::init();
            TUPLEX_TRACE("creating history server connector");
            _historyConn = HistoryServerConnector::connect(_options.WEBUI_HOST(),
                                                           _options.WEBUI_PORT(),
                                                           _options.WEBUI_DATABASE_HOST(),
                                                           _options.WEBUI_DATABASE_PORT());
            TUPLEX_TRACE("connection established");
        }
    }

    static void
    printBreakdowns(const std::map<std::string, RollingStats<double>> &breakdownTimings, std::stringstream &ss) {
        ss << "{";

        size_t prefix_offset = 3;
        std::string found_prefixes[2] = {"", ""};
        std::string prefixes[2] = {"process_mem_", "process_file_"};
        std::map<std::string, RollingStats<double>> m[2];

        bool first_breakdown = true;
        auto print_breakdown = [&](const std::pair<std::string, RollingStats<double>> &keyval) {
            if (first_breakdown) {
                first_breakdown = false;
            } else {
                ss << ", ";
            }
            ss << "\"" << keyval.first << "\": { \"mean\": " << keyval.second.mean() << ", \"std\": "
               << keyval.second.std() << "}";
        };

        for (const auto &keyval: breakdownTimings) {
            auto is_prefix = [prefix_offset](const std::string &a, const std::string &b) {
                // check if a is a prefix of b
                if (a.size() + prefix_offset <= b.size()) {
                    auto res = std::mismatch(a.begin(), a.end(), b.begin() + prefix_offset);
                    return res.first == a.end();
                }
                return false;
            };
            if (is_prefix(prefixes[0], keyval.first) || is_prefix(prefixes[1], keyval.first)) {
                auto prefix_idx = is_prefix(prefixes[0], keyval.first) ? 0 : 1;
                found_prefixes[prefix_idx] = keyval.first.substr(0, prefixes[prefix_idx].length() + prefix_offset);
                auto suffix = keyval.first.substr(found_prefixes[prefix_idx].length());
                m[prefix_idx][suffix] = keyval.second;
            } else {
                print_breakdown(keyval);
            }
        }

        for (int i = 0; i < 2; i++) {
            if (found_prefixes[i].empty()) continue;
            ss << ", ";
            ss << "\"" << found_prefixes[i] << "\": {";
            first_breakdown = true;
            for (const auto &keyval: m[i]) {
                print_breakdown(keyval);
            }
            ss << "}";
        }

        ss << "}\n";
    }

    struct PerFileInfo {
        std::string requestId;
        size_t in_normal;
        size_t in_general;
        size_t in_fallback;
        size_t in_unresolved;
        size_t out_normal;
        size_t out_unresolved;

        // for convenience
        std::string log;

        inline std::string to_csv() const {
            std::stringstream ss;
            ss<<requestId<<","
              <<in_normal<<","
              <<in_general<<","
              <<in_fallback<<","
              <<in_unresolved<<","
              <<out_normal<<","
              <<out_unresolved;
            return ss.str();
        }

        inline std::string header() const {
            return "requestId,in_normal,in_general,in_fallback,in_unresolved,out_normal,out_unresolved";
        }
    };

    std::string AwsLambdaBackend::csvPerFileInfo() {
        using namespace std;

        // settings etc.
        bool hyper_mode_str = (_options.USE_EXPERIMENTAL_HYPERSPECIALIZATION() ? "true" : "false");

        std::unordered_map<std::string, PerFileInfo> uri_map;

        // 1. tasks
        {
            std::lock_guard<std::mutex> lock(_mutex);

            int task_counter = 0;
            for (const auto &task: _tasks) {
                ContainerInfo info = task.response.container();

                std::string log = "";
                // log
                for (const auto &r: task.response.resources()) {
                    if (r.type() == static_cast<uint32_t>(ResourceType::LOG)) {
                        log = decompress_string(r.payload());
                        break;
                    }
                }

               // invoked input uris
                for (unsigned i = 0; i < task.response.inputuris_size(); ++i) {
                    auto input_uri = task.response.inputuris(i);

                    // clean from part
                    URI uri;
                    size_t rangeStart = 0, rangeEnd = 0;
                    decodeRangeURI(input_uri, uri, rangeStart, rangeEnd);

                    auto it = uri_map.find(uri.toPath());
                    if(it != uri_map.end())
                        uri_map[uri.toPath()] = PerFileInfo();

                    auto& f_info = uri_map[uri.toPath()];
                    f_info.requestId = info.requestId;
                    f_info.log = log;

                }

                task_counter++;
            }
        }


        // 2. requests & responses?
        {
            std::lock_guard<std::mutex> lock(_mutex);
            for (unsigned i = 0; i < _tasks.size(); ++i) {
                auto &info = _tasks[i].info;
                // info has information as well

                // find in map the info with requestid
                for(auto& kv : uri_map) {
                    if(kv.second.requestId == info.requestId) {
                        // save now in normal etc.
                        kv.second.in_normal = info.in_normal;
                        kv.second.in_general = info.in_general;
                        kv.second.in_fallback = info.in_fallback;
                        kv.second.in_unresolved = info.in_unresolved;
                        kv.second.out_normal = info.out_normal;
                        kv.second.out_unresolved = info.out_unresolved;
                    }
                }
            }
        }

        // create version (sort after first entry!)
        std::vector<std::tuple<std::string, std::string>> entries;
        std::string header;
        std::stringstream ss;
        bool first_time = true;
        for(const auto& kv : uri_map) {
            if(first_time) {
                first_time = false;
                header = "uri," + kv.second.header();
            }
            entries.push_back(std::make_tuple(kv.first, kv.second.to_csv()));

            // write to local dir for convenience
            auto log_path = "./logs/" + URI(kv.first).basename()  + ".log.txt";
            stringToFile(log_path, kv.second.log);
        }

        std::sort(entries.begin(), entries.end(), [](const std::tuple<std::string, std::string>& rhs, const std::tuple<std::string, std::string>& lhs) {
           return std::get<0>(rhs) < std::get<0>(lhs);
        });

        ss<<header<<"\n";
        for(auto entry : entries)
            ss<<std::get<0>(entry)<<","<<std::get<1>(entry)<<"\n";

        return ss.str();
    }

    void AwsLambdaBackend::dumpAsJSON(const std::string &json_path) {
        using namespace std;
        stringstream ss;

        ss << "{";

        // 0. general info
        ss << "\"stageStartTimestamp\":" << _startTimestamp << ",";
        ss << "\"stageEndTimestamp\":" << _endTimestamp << ",";

        // settings etc.
        ss << "\"hyper_mode\":" << (_options.USE_EXPERIMENTAL_HYPERSPECIALIZATION() ? "true" : "false") << ",";
        ss << "\"cost\":" << _info.cost << ",";
        ss << "\"input_paths_taken\":{"
           << "\"normal\":" << _info.total_input_normal_path << ","
           << "\"general\":" << _info.total_input_general_path << ","
           << "\"fallback\":" << _info.total_input_fallback_path << ","
           << "\"unresolved\":" << _info.total_input_unresolved
           << "},";
        ss << "\"output_paths_taken\":{"
           << "\"normal\":" << _info.total_output_rows << ","
           << "\"unresolved\":" << _info.total_output_exceptions
           << "},";


        // 1. tasks
        ss << "\"tasks\":[";
        {
            std::lock_guard<std::mutex> lock(_mutex);

            int task_counter = 0;
            for (const auto &task: _tasks) {
                ContainerInfo info = task.response.container();
                ss << "{\"container\":" << info.asJSON() << ",\"invoked_containers\":[";
                for (unsigned i = 0; i < task.response.invokedcontainers_size(); ++i) {
                    info = task.response.invokedcontainers(i);
                    ss << info.asJSON();
                    if (i != task.response.invokedcontainers_size() - 1)
                        ss << ",";
                }
                ss << "]";

                // log
                for (const auto &r: task.response.resources()) {
                    if (r.type() == static_cast<uint32_t>(ResourceType::LOG)) {
                        auto log = decompress_string(r.payload());
                        ss << ",\"log\":" << escape_for_json(log) << "";
                        break;
                    }
                }


                ss << ",\"invoked_requests\":[";
                RequestInfo r_info;
                for (unsigned i = 0; i < task.response.invokedrequests_size(); ++i) {
                    r_info = task.response.invokedrequests(i);
                    ss << r_info.asJSON();
                    if (i != task.response.invokedrequests_size() - 1)
                        ss << ",";
                }
                ss << "]";

                // invoked input uris
                ss << ",\"input_uris\":[";
                for (unsigned i = 0; i < task.response.inputuris_size(); ++i) {
                    ss << "\"" << task.response.inputuris(i) << "\"";
                    if (i != task.response.inputuris_size() - 1)
                        ss << ",";
                }

                ss << "]";

                // end container.
                ss << "}";
                if (task_counter != _tasks.size() - 1)
                    ss << ",";
                task_counter++;
            }
        }
        ss << "],";


        // 2. requests & responses?
        // 1. tasks
        ss << "\"requests\":[";
        {
            std::lock_guard<std::mutex> lock(_mutex);
            for (unsigned i = 0; i < _tasks.size(); ++i) {
                auto &info = _tasks[i].info;
                ss << info.asJSON();
                if (i != _tasks.size() - 1)
                    ss << ",";
            }
        }
        ss << "]";

        ss << "}";

        stringToFile(json_path, ss.str());
    }

    void AwsLambdaBackend::gatherStatistics() {
        std::stringstream ss;

        _info.cost = lambdaCost();

        {
            std::lock_guard<std::mutex> lock(_mutex);

            RollingStats<double> awsInitTime;
            RollingStats<double> taskExecutionTime;
            std::map<std::string, RollingStats<size_t>> s3Stats;
            std::map<std::string, RollingStats<double>> breakdownTimings;
            std::set<std::string> containerIDs;
            size_t numReused = 0;
            size_t numNew = 0;

            size_t total_num_output_rows = 0;
            size_t total_num_exceptions = 0;
            size_t total_bytes_written = 0;

            // paths rows took
            size_t total_normal_path = 0;
            size_t total_general_path = 0;
            size_t total_interpreter_path = 0;
            size_t total_unresolved = 0;

            // aggregate stats over responses
            for (const auto &task: _tasks) {
                total_num_output_rows += task.response.numrowswritten();
                total_num_exceptions += task.response.numexceptions();
                total_bytes_written += task.response.numbyteswritten();

                total_normal_path += task.response.rowstats().normal();
                total_general_path += task.response.rowstats().general();
                total_interpreter_path += task.response.rowstats().interpreter();
                total_unresolved += task.response.rowstats().unresolved();

                awsInitTime.update(task.response.awsinittime());
                taskExecutionTime.update(task.response.taskexecutiontime());
                for (const auto &keyval: task.response.s3stats()) {
                    auto key = keyval.first;
                    auto val = keyval.second;

                    auto it = s3Stats.find(key);
                    if (it == s3Stats.end())
                        s3Stats[key] = RollingStats<size_t>();
                    s3Stats[key].update(val);
                }
                for (const auto &keyval: task.response.breakdowntimes()) {
                    auto key = keyval.first;
                    auto val = keyval.second;

                    auto it = breakdownTimings.find(key);
                    if (it == breakdownTimings.end())
                        breakdownTimings[key] = RollingStats<double>();
                    breakdownTimings[key].update(val);
                }

                containerIDs.insert(task.response.container().uuid());
                numReused += task.response.container().reused();
                numNew += !task.response.container().reused();
            }

            // print stage stats
            logger().info("LAMBDA Stage output: " + pluralize(total_num_output_rows, "row") + ", "
                          + pluralize(total_num_exceptions, "exception"));
            std::string general_info = _options.RESOLVE_WITH_INTERPRETER_ONLY() ? "(deactivated)" : std::to_string(
                    total_general_path);
            logger().info("LAMBDA paths input rows took: normal: " + std::to_string(total_normal_path)
                          + " general: " + general_info + " interpreter: "
                          + std::to_string(total_interpreter_path));

            // save to internal
            _info.total_input_normal_path = total_normal_path;
            _info.total_input_general_path = total_general_path;
            _info.total_input_fallback_path = total_interpreter_path;
            _info.total_input_unresolved = total_unresolved; // <-- this doesn't really make sense, unresolved as input?? -> only from previous caching... | this is a check more or less.

            _info.total_output_rows = total_num_output_rows;
            _info.total_output_exceptions = total_num_exceptions;

//            // print exception summary if any occurred
//            if(total_num_exceptions > 0) {
//                printErrorTreeHelper
//            }

            // compute cost of s3 + Lambda
            ss << "Lambda #containers used: " << containerIDs.size() << " reused: " << numReused
               << " newly initialized: " << numNew << "\n";
            ss << "Lambda init time: " << awsInitTime.mean() << " +- " << awsInitTime.std() << " min: "
               << awsInitTime.min() << " max: " << awsInitTime.max() << "\n";
            ss << "Lambda execution time: " << taskExecutionTime.mean() << " +- " << taskExecutionTime.std() << " min: "
               << taskExecutionTime.min() << " max: " << taskExecutionTime.max() << "\n";
            // compute S3 cost => this is more complicated, i.e. postpone
            // for(auto keyval : s3Stats) {
            //     ss<<"s3: "<<keyval.first<<" sum: "<<keyval.second.mean()<<"\n";
            // }

            // breakdown timings
            ss << "\n----- BREAKDOWN TIMINGS -----\n";
            printBreakdowns(breakdownTimings, ss);
        }

        ss << "Lambda cost: $" << lambdaCost();

        logger().info("LAMBDA statistics: \n" + ss.str());
    }

    size_t AwsLambdaBackend::getMB100Ms() {
        std::lock_guard<std::mutex> lock(_mutex);

        // sum up billed mb ms
        size_t billed = 0;
        for (const auto task: _tasks) {
            size_t billedDurationInMs = task.info.billedDurationInMs;
            size_t memorySizeInMb = task.info.memorySizeInMb;
            billed += billedDurationInMs / 100 * memorySizeInMb;
        }
        return billed;
    }

    size_t AwsLambdaBackend::getMBMs() {
        std::lock_guard<std::mutex> lock(_mutex);

        // sum up billed mb ms
        size_t billed = 0;
        for (const auto& task: _tasks) {
            size_t billedDurationInMs = task.info.billedDurationInMs;
            size_t memorySizeInMb = task.info.memorySizeInMb;
            billed += billedDurationInMs * memorySizeInMb;
        }
        return billed;
    }

    URI AwsLambdaBackend::scratchDir(const std::vector<URI> &hints) {
        // is URI valid? return
        if (_scratchDir != URI::INVALID)
            return _scratchDir;

        // fetch dir from options
        auto ctx_scratch_dir = _options.AWS_SCRATCH_DIR();
        if (!ctx_scratch_dir.empty()) {
            _scratchDir = URI(ctx_scratch_dir);
            if (_scratchDir.prefix() != "s3://") // force S3
                _scratchDir = URI("s3://" + ctx_scratch_dir);
            _deleteScratchDirOnShutdown = false; // if given externally, do not delete per default
            return _scratchDir;
        }

        auto cache_folder = ".tuplex-cache";

        // check hints
        for (const auto &hint: hints) {
            if (hint.prefix() != "s3://") {
                logger().warn("AWS scratch dir hint given, but is no S3 URI: " + hint.toString());
                continue;
            }

            // check whether a file exists, if so skip, else valid dir found!
            auto dir = hint.join_path(cache_folder);
            if (!dir.exists()) {
                _scratchDir = dir;
                _deleteScratchDirOnShutdown = true;
                logger().info("Using " + dir.toString() +
                              " as temporary AWS S3 scratch dir, will be deleted on tuplex context shutdown.");
                return _scratchDir;
            }
        }

        // invalid, no aws scratch dir available
        logger().error(
                "requesting AWS S3 scratch dir, but none configured. Please set a AWS S3 scratch dir for the context by setting the config key tuplex.aws.scratchDir to a valid S3 URI");
        return URI::INVALID;
    }

    std::vector<URI> AwsLambdaBackend::hintsFromTransformStage(const TransformStage *stage) {
        std::vector<URI> hints;

        // take input and output folder as hints
        // prefer output folder hint over input folder hint
        if (stage->outputMode() == EndPointMode::FILE) {
            auto uri = stage->outputURI();
            if (uri.prefix() == "s3://")
                hints.push_back(uri);
        }

        if (stage->inputMode() == EndPointMode::FILE) {
            // TODO
            // get S3 uris, etc.
        }

        return hints;
    }

    void AwsLambdaBackend::reset() {
        _tasks.clear();

        // reset path mapping
        _remoteToLocalURIMapping.clear();

        // make sure to reset service and cost calculations there!
        if(_service)
            _service->reset();

        // other reset? @TODO.
    }

//    void AwsLambdaBackend::abortRequestsAndFailWith(int returnCode, const std::string &errorMessage) {
//        logger().error("LAMBDA execution failed due to exit code " + std::to_string(returnCode) +
//                       " on one executor, details: " + errorMessage);
//
//        int numPending = std::max((int) _numPendingRequests, 0);
//        if (numPending > 0)
//            logger().info("Aborting " + pluralize(numPending, " pending request"));
//        else
//            logger().info("Aborting.");
//
//        _numPendingRequests = 0;
//        _client->DisableRequestProcessing();
//        logger().info("Shutdown remote execution.");
//        _client->EnableRequestProcessing();
//    }
}
#endif