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
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/json/JsonSerializer.h>

// only exists in newer SDKs...
// #include <aws/lambda/model/Architecture.h>

// protobuf header
#include <Lambda.pb.h>

#include <third_party/base64/base64.h>

#include <google/protobuf/util/json_util.h>
#include <iomanip>

#include <utility>

namespace tuplex {

    // helper class to provide backend in callback
    class AwsLambdaBackendCallerContext : public Aws::Client::AsyncCallerContext {
    private:
        AwsLambdaBackend *_backend;
        std::chrono::high_resolution_clock::time_point _ts;
        std::string _payload;
        uniqueid_t _taskID;
    public:
        AwsLambdaBackendCallerContext() = delete;

        AwsLambdaBackendCallerContext(AwsLambdaBackend *backend, const std::string& payload, uniqueid_t taskID) : _backend(backend),
                                                                                      _ts(std::chrono::high_resolution_clock::now()),
                                                                                      _payload(payload),
                                                                                      _taskID(taskID) {
        }

        AwsLambdaBackend *getBackend() const { return _backend; }

        std::string payload() const { return _payload; }

        double time() const {
            auto stop = std::chrono::high_resolution_clock::now();
            double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - _ts).count() / 1000000000.0;
            return duration;
        }

        uniqueid_t getTaskID() const { return _taskID; }
    };

    AwsLambdaBackend::~AwsLambdaBackend() {
        // stop http requests
        if (_client)
            _client->DisableRequestProcessing();

        // delete scratch dir?
        if(_deleteScratchDirOnShutdown) {
            auto vfs = VirtualFileSystem::fromURI(_scratchDir);
            vfs.remove(_scratchDir); // TODO: could optimize this by keeping track of temp files and issuing on shutdown
            // a single multiobject delete request...
        }
    }

    std::shared_ptr<Aws::Lambda::LambdaClient> AwsLambdaBackend::makeClient() {
        // init Lambda client
        Aws::Client::ClientConfiguration clientConfig;

        clientConfig.requestTimeoutMs = _options.AWS_REQUEST_TIMEOUT() * 1000; // conv seconds to ms
        clientConfig.connectTimeoutMs = _options.AWS_CONNECT_TIMEOUT() * 1000; // connection timeout

        // tune client, according to https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/client-config.html
        // note: max connections should not exceed max concurrency if it is below 100, else aws lambda
        // will return toomanyrequestsexception
        clientConfig.maxConnections = _options.AWS_MAX_CONCURRENCY();

        // to avoid thread exhaust of system, use pool thread executor with 8 threads
        clientConfig.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(_tag.c_str(), _options.AWS_NUM_HTTP_THREADS());
        if(_options.AWS_REGION().empty())
            clientConfig.region = _credentials.default_region.c_str();
        else
            clientConfig.region = _options.AWS_REGION().c_str(); // hard-coded here

        // verify zone
        if(!isValidAWSZone(clientConfig.region.c_str())) {
            logger().warn("Specified AWS zone '" + std::string(clientConfig.region.c_str()) + "' is not a valid AWS zone. Defaulting to " + _credentials.default_region + " zone.");
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
        if(!outcome.IsSuccess()) {
            std::stringstream ss;
            ss << outcome.GetError().GetExceptionName().c_str()
               << outcome.GetError().GetMessage().c_str();

            throw std::runtime_error("LAMBDA failed to list functions, details: " + ss.str());
        } else {
            // check whether function is contained
            auto funcs = outcome.GetResult().GetFunctions();

            // search for the function of interest
            for(const auto& f : funcs) {
                if(f.GetFunctionName().c_str() == _functionName) {
                    fc_json_str = f.Jsonize().View().WriteCompact();
                    fc = new Aws::Lambda::Model::FunctionConfiguration(Aws::Utils::Json::JsonValue(fc_json_str));
                    break;
                }
            }

            if(!fc)
                throw std::runtime_error("could not find lambda function '" + _functionName + "'");
            else {
                logger().info("Found AWS Lambda function " + _functionName + " (" + std::to_string(fc->GetMemorySize()) + "MB)");
            }
        }

        // check architecture of function (only newer AWS SDKs support this...)
        {
            using namespace Aws::Utils::Json;
            using namespace Aws::Utils;
            auto fc_json = Aws::Utils::Json::JsonValue(fc_json_str);
            if(fc_json.View().ValueExists("Architectures")) {
                Array<JsonView> architecturesJsonList = fc_json.View().GetArray("Architectures");
                std::vector<std::string> architectures;
                for(unsigned architecturesIndex = 0; architecturesIndex < architecturesJsonList.GetLength(); ++architecturesIndex)
                   architectures.push_back(std::string(architecturesJsonList[architecturesIndex].AsString().c_str()));
                // there should be one architecture
                if(architectures.size() != 1) {
                    logger().warn(
                            "AWS Lambda changed specification, update how to deal with mulit-architecture functions");
                    if(!architectures.empty())
                        _functionArchitecture = architectures.front();
                }
                else {
                    _functionArchitecture = architectures.front();
                }
            } else {
                _functionArchitecture = "x86_64";
            }
        }

        logger().info("Using Lambda running on " + _functionArchitecture);

        // limit concurrency + mem of function manually (TODO: uncomment for faster speed!), if it doesn't fit options
        // i.e. aws lambda put-function-concurrency --function-name tplxlam --reserved-concurrent-executions $MAX_CONCURRENCY
        //      aws lambda update-function-configuration --function-name tplxlam --memory-size $MEM_SIZE --timeout 60
        // update required?
        bool needToUpdateConfig = false;
        Aws::Lambda::Model::UpdateFunctionConfigurationRequest update_req;
        update_req.SetFunctionName(_functionName.c_str());
        if(fc->GetTimeout() != _lambdaTimeOut) {
            update_req.SetTimeout(_lambdaTimeOut);
            needToUpdateConfig = true;
        }

        if(fc->GetMemorySize() != _lambdaSizeInMB) {
            update_req.SetMemorySize(_lambdaSizeInMB);
            needToUpdateConfig = true;
        }
        if(needToUpdateConfig) {
            logger().info("updating Lambda settings to timeout: " + std::to_string(_lambdaTimeOut) + "s memory: " + std::to_string(_lambdaSizeInMB) + " MB");
            auto outcome = client->UpdateFunctionConfiguration(update_req);
            if(!outcome.IsSuccess()) {
                std::stringstream ss;
                ss << outcome.GetError().GetExceptionName().c_str()
                   << outcome.GetError().GetMessage().c_str();

                throw std::runtime_error("LAMBDA failed update configuration, details: " + ss.str());
            }
            logger().info("Updated Lambda configuration successfully.");
        }

        // concurrency?
        // PutFunctionConcurrency

        // Also need to check that Lambda is in ready status...

        // print out timing info...


        delete fc;

        return client;
    }

    std::vector<std::tuple<std::string, size_t> >
    AwsLambdaBackend::decodeFileURIs(const std::vector<Partition *> &partitions, bool invalidate) {
        using namespace std;
        vector<std::tuple<std::string, size_t> > infos;

        auto fileSchema = Schema(Schema::MemoryLayout::ROW,
                                 python::Type::makeTupleType({python::Type::STRING, python::Type::I64}));

        for (auto partition : partitions) {
            // get num
            auto numFiles = partition->getNumRows();
            const uint8_t *ptr = partition->lock();
            size_t bytesRead = 0;
            // found
            for (int i = 0; i < numFiles; ++i) {
                // found file -> create task / split into multiple tasks
                Row row = Row::fromMemory(fileSchema, ptr, partition->capacity() - bytesRead);
                auto path = row.getString(0);
                size_t file_size = row.getInt(1);

                infos.push_back(make_tuple(path, file_size));
                ptr += row.serializedLength();
                bytesRead += row.serializedLength();
            }

            partition->unlock();

            if (invalidate)
                partition->invalidate();
        }

        return infos;
    }

    void AwsLambdaBackend::invokeAsync(const messages::InvocationRequest &req) {

        // @TODO: refactor using old model of lambda requests
        // => save req and response
        // fallback when rate limit is reached
        // i.e. checkout https://github.com/StanfordSNR/gg/blob/master/src/execution/engine_lambda.cc
        // https://github.com/StanfordSNR/gg/blob/62579e141a96f30312cd9a1a2d6f91302e3899d5/src/execution/reductor.cc

        auto taskID = getUniqueID();

        // Note: If message too large, will receive RequestEntityTooLargeException. => need to deal with that!
        // async limit is 128K, requestresponse is 6MB.

        // https://www.stackery.io/blog/RequestEntityTooLargeException-aws-lambda-message-invocation-limits/
        // Note: ObjectExpiration date for temp objects and can also use multiobject delete request
        // https://aws.amazon.com/blogs/aws/amazon-s3-multi-object-deletion/

        // construct req object
        Aws::Lambda::Model::InvokeRequest invoke_req;
        invoke_req.SetFunctionName(_functionName.c_str());
        // note: may redesign lambda backend to work async, however then response only yields status code
        // i.e., everything regarding state needs to be managed explicitly...
        invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
        // logtype to extract log data??
        //req.SetLogtype(Aws::Lambda::Model::LogType::None);
        invoke_req.SetLogType(Aws::Lambda::Model::LogType::Tail);
        // qualifier to specify a version to be invoked... ==> critical for multiple users!

        // this doesn't work, use json instead...
        // invoke_req.SetBody(stringToAWSStream(req.SerializeAsString()));
        // invoke_req.SetContentType("application/x-protobuf");

        std::string json_buf;
        google::protobuf::util::MessageToJsonString(req, &json_buf);
        invoke_req.SetBody(stringToAWSStream(json_buf));
        invoke_req.SetContentType("application/javascript");

        // sent to client
        _numPendingRequests.fetch_add(1, std::memory_order_release); // inc task number by one
        _numRequests.fetch_add(1, std::memory_order_release);
        _client->InvokeAsync(invoke_req, AwsLambdaBackend::asyncLambdaCallback,
                             Aws::MakeShared<AwsLambdaBackendCallerContext>(_tag.c_str(), this, req.SerializeAsString(), taskID));
    }

    std::set<std::string> AwsLambdaBackend::performWarmup(size_t concurrency, size_t timeOutInMs, size_t delayInMs) {

        //            size_t numWarmingRequests = 50;
        std::set<std::string> containerIds;
        size_t numLambdasToInvoke = concurrency - 1;
        logger().info("Warming up containers...");
        // do a single synced request (else reuse will occur!)
        // Tuplex request
        messages::InvocationRequest req;
        req.set_type(messages::MessageType::MT_WARMUP);

        // specific warmup message contents
        auto wm = std::make_unique<messages::WarmupMessage>();
        wm->set_timeoutinms(timeOutInMs);
        wm->set_invocationcount(numLambdasToInvoke);
        req.set_allocated_warmup(wm.release());

        // construct req object
        Aws::Lambda::Model::InvokeRequest invoke_req;
        invoke_req.SetFunctionName(_functionName.c_str());
        invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
        // logtype to extract log data??
        //req.SetLogtype(Aws::Lambda::Model::LogType::None);
        invoke_req.SetLogType(Aws::Lambda::Model::LogType::Tail);
        std::string json_buf;
        google::protobuf::util::MessageToJsonString(req, &json_buf);
        invoke_req.SetBody(stringToAWSStream(json_buf));
        invoke_req.SetContentType("application/javascript");

        // perform synced (!) invoke.
        auto outcome = _client->Invoke(invoke_req);
        if(outcome.IsSuccess()) {

            // write response
            auto& result = outcome.GetResult();
            auto statusCode = result.GetStatusCode();
            std::string version = result.GetExecutedVersion().c_str();
            auto response = parsePayload(result);

            auto log = result.GetLogResult();

            if(response.status() == messages::InvocationResponse_Status_SUCCESS) {
                // extract info
                AwsLambdaBackend::InvokeInfo info = parseFromLog(log.c_str());
                std::stringstream ss;
                auto& task = response;
                if(task.type() == messages::MessageType::MT_WARMUP) {
                    containerIds.insert(task.container().uuid());
                    for(auto info: task.invokedcontainers())
                        containerIds.insert(info.uuid());
                }
                ss<<"Warmup request took "<<response.taskexecutiontime()<<" s, "<<"initialized "<<containerIds.size();
                logger().info(ss.str());
            } else {
                logger().info("Message returned was weird.");
            }

            logger().info("Warming succeeded.");
        } else {
            // failed
            logger().error("Warming request failed.");

            auto &error = outcome.GetError();
            auto statusCode = static_cast<int>(error.GetResponseCode());
            std::string exceptionName = outcome.GetError().GetExceptionName().c_str();
            std::string errorMessage= outcome.GetError().GetMessage().c_str();
            // rate limit? => reissue request
            if(statusCode == static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS) || // i.e. 429
                statusCode == static_cast<int>(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR)) {  // i.e. 500
            } else {
            }
        }
//            for(unsigned i = 0; i < numWarmingRequests; ++i) {
//
//                // Tuplex request
//                messages::InvocationRequest req;
//                req.set_type(messages::MessageType::MT_WARMUP);
//
//                // specific warmup message contents
//                auto wm = std::make_unique<messages::WarmupMessage>();
//                wm->set_timeoutinms(timeOutInMs);
//                wm->set_invocationcount(numLambdasToInvoke);
//                req.set_allocated_warmup(wm.release());
//
//                invokeAsync(req);
//            }
//            waitForRequests();
        logger().info("warmup done");

        return containerIds;
    }

    void AwsLambdaBackend::execute(PhysicalStage *stage) {
        using namespace std;

        reset();

        // Notes:
        // ==> could use the warm up events for sampling & speed detection
        // ==> helps to plan the query more efficiently!

        // perform warmup phase if desired (only for first stage?)
        if(_options.AWS_LAMBDA_SELF_INVOCATION()) {
            // issue a couple self-invoke requests...
            Timer timer;

            auto containerIds = performWarmup();

            logger().info("Warmup yielded " + pluralize(containerIds.size(), "container id"));
            logger().info("Warmup took: " + std::to_string(timer.time()));

            reset();
        }

        auto tstage = dynamic_cast<TransformStage *>(stage);
        if (!tstage)
            throw std::runtime_error("only trafo stage from AWSLambdda backend yet supported");

        vector <tuple<std::string, size_t>> uri_infos;

        // decode data from stage
        // -> i.e. could be memory or file, so far only files are supported!
        if (tstage->inputMode() != EndPointMode::FILE) {
            // the data needs to get somehow transferred from the local driver to the cloud
            // Either it could be passed directly via the request OR via S3.

            // For now, use S3 for simplicity...
            auto s3tmp_uri = scratchDir(hintsFromTransformStage(tstage));
            if(s3tmp_uri == URI::INVALID) {
                throw std::runtime_error("could not find/create AWS Lambda scratch dir.");

            }
            // need to transfer the Tuplex partitions to S3
            // -> which format?
            switch(tstage->inputMode()) {
                case EndPointMode::MEMORY: {
                    // simply save to S3!
                    // @TODO: larger/smaller files?
                    Timer timer;
                    int partNo = 0;
                    auto num_partitions = tstage->inputPartitions().size();
                    auto num_digits = ilog10c(num_partitions);
                    size_t total_uploaded = 0;
                    for(auto p : tstage->inputPartitions()) {
                        // lock each and write to S3!
                        // save input URI and size!
                        auto part_uri = s3tmp_uri.join_path("input_part_" + fixedLength(partNo, num_digits) + ".mem");
                        auto vfs = VirtualFileSystem::fromURI(part_uri);
                        auto vf = vfs.open_file(part_uri, VirtualFileMode::VFS_OVERWRITE);

                        // @TODO: setMIMEtype?

                        if(!vf)
                            throw std::runtime_error("could not open file " + part_uri.toString());
                        auto buf = p->lockRaw();
                        auto buf_size = p->bytesWritten();
                        if(!buf_size)
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
                    " in total transferred to " + s3tmp_uri.toString() + ", took "+ std::to_string(timer.time()) + "s");
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
        if(_options.USE_LLVM_OPTIMIZER()) {
            Timer timer;
            llvm::LLVMContext ctx;
            LLVMOptimizer opt;
            auto mod = codegen::bitCodeToModule(ctx, tstage->bitCode());
            opt.optimizeModule(*mod);
            optimizedBitcode = codegen::moduleToBitCodeString(*mod);
            logger().info("client-side LLVM IR optimization took " + std::to_string(timer.time()) + "s");
        }

        if(stage->outputMode() == EndPointMode::MEMORY) {
            // check whether scratch dir exists.
            auto scratch = scratchDir(hintsFromTransformStage(tstage));
            if(scratch == URI::INVALID) {
                throw std::runtime_error("temporaty AWS Lambda scratch dir required to write output, please specify via tuplex.aws.scratchDir key");
                return;
            }
        }

        // Worker config variables
        size_t numThreads = 1;
        // check what setting is given for threads
        if(_options.AWS_LAMBDA_THREAD_COUNT() == "auto") {
            numThreads = core::ceilToMultiple(_options.AWS_LAMBDA_MEMORY(), 1792ul) / 1792ul; // 1792MB is one vCPU. Use the 200+ for rounding.
            logger().debug("Given Lambda size of " + std::to_string(_options.AWS_LAMBDA_MEMORY()) + "MB, use " + pluralize(numThreads, "thread"));
        } else {
            numThreads = std::stoi(_options.AWS_LAMBDA_THREAD_COUNT());
        }
        auto spillURI = _options.AWS_SCRATCH_DIR() + "/spill_folder";
        // perhaps also use:  - 64 * numThreads ==> smarter buffer scaling necessary.
        size_t buf_spill_size = (_options.AWS_LAMBDA_MEMORY() - 256) / numThreads * 1000 * 1024;

        // Note: for now, super simple: 1 request per file (this is inefficient, but whatever)
        // @TODO: more sophisticated splitting of workload!
        Timer timer;
        int num_digits = ilog10c(uri_infos.size());
        for (int i = 0; i < uri_infos.size(); ++i) {
            auto info = uri_infos[i];
            messages::InvocationRequest req;
            req.set_type(messages::MessageType::MT_TRANSFORM);
            auto pb_stage = tstage->to_protobuf();

            if(_options.USE_LLVM_OPTIMIZER() && !optimizedBitcode.empty())
                pb_stage->set_bitcode(optimizedBitcode);
            else
                pb_stage->set_bitcode(tstage->bitCode());

            req.set_allocated_stage(pb_stage.release());

            // add request for this
            auto inputURI = std::get<0>(info);
            auto inputSize = std::get<1>(info);
            req.add_inputuris(inputURI);
            req.add_inputsizes(inputSize);

            // worker config
            auto ws = std::make_unique<messages::WorkerSettings>();
            ws->set_numthreads(numThreads);
            ws->set_normalbuffersize(buf_spill_size);
            ws->set_exceptionbuffersize(buf_spill_size);
            ws->set_spillrooturi(spillURI);
            req.set_allocated_settings(ws.release());

            // output uri of job? => final one? parts?
            // => create temporary if output is local! i.e. to memory etc.
            int taskNo = i;
            if (tstage->outputMode() == EndPointMode::MEMORY) {
                // create temp file in scratch dir!
                req.set_outputuri(scratchDir(hintsFromTransformStage(tstage)).join_path("output.part" + fixedLength(taskNo, num_digits)).toString());
            } else if (tstage->outputMode() == EndPointMode::FILE) {
                // create output URI based on taskNo
                auto uri = outputURI(tstage->outputPathUDF(), tstage->outputURI(), taskNo, tstage->outputFormat());
                req.set_outputuri(uri.toPath());
            } else if (tstage->outputMode() == EndPointMode::HASHTABLE) {
                throw std::runtime_error("join, aggregate not yet supported in lambda backend");
            } else throw std::runtime_error("unknown output endpoint in lambda backend");

            // make invocation
            std::stringstream ss;
            ss<<"LAMBDA request "<<(i+1)<<"/"<<uri_infos.size()<<" on "<<sizeToMemString(inputSize);
            logger().info(ss.str());

            // debug, save to protobuf!
#ifndef NDEBUG
            stringToFile("zillow.pb", req.SerializeAsString());
#endif

            invokeAsync(req);
        }
        logger().info("LAMBDA requesting took "+ std::to_string(timer.time()) + "s");

        // TODO: check signals, allow abort...

        // wait till everything finished computing
        waitForRequests();
        printStatistics();
        {
            std::stringstream ss;
            ss<<"LAMBDA compute took "<<timer.time()<<"s";
            double cost = lambdaCost();
            if(cost < 0.01)
                ss<<", cost < $0.01";
            else
                ss<<std::fixed<<std::setprecision(2)<<", cost $"<<cost;
            logger().info(ss.str());
        }

        // @TODO: results sets etc.
        switch(tstage->outputMode()) {
            case EndPointMode::FILE: {
                std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> ecounts; // Todo: fill in from lambda
                tstage->setFileResult(ecounts);
                break;
            }

            case EndPointMode::MEMORY: {
                // fetch from outputs, alloc partitions and set
                // tstage->setMemoryResult()
                Timer timer;
                vector<URI> output_uris;
                for(auto task : _tasks) {
                    for(auto uri : task.outputuris())
                        output_uris.push_back(uri);
                }
                // sort after part no @TODO
                std::sort(output_uris.begin(), output_uris.end(), [](const URI& a, const URI& b) {
                    return a.toString() < b.toString();
                });

                // download and store each part in one partition (TODO: resize etc.)
                vector<Partition*> output_partitions;
                int partNo = 0;
                int num_digits = ilog10c(output_uris.size());
                vector<URI> local_paths;
                for(auto uri : output_uris) {
                    // download to local scratch dir
                    auto local_path = _options.SCRATCH_DIR().join_path("aws-part" + fixedLength(partNo, num_digits));
                    VirtualFileSystem::copy(uri.toString(), local_path);
                    local_paths.push_back(local_path);
                    partNo++;
                }
                logger().info("fetching results from " + scratchDir().toString() + " took " + std::to_string(timer.time()) + "s");

                // convert to partitions
                timer.reset();
                for(auto path : local_paths) {
                    auto vf = VirtualFileSystem::fromURI(path).open_file(path, VirtualFileMode::VFS_READ);
                    if(!vf) {
                        throw std::runtime_error("could not read locally cached file " + path.toString());
                    }

                    auto file_size = vf->size();
                    size_t bytesRead = 0;

                    // alloc new driver partition
                    Partition *partition = _driver->allocWritablePartition(file_size, tstage->outputSchema(), tstage->outputDataSetID(), stage->context().id());
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
    }

    void AwsLambdaBackend::asyncLambdaCallback(const Aws::Lambda::LambdaClient *client,
                                               const Aws::Lambda::Model::InvokeRequest &req,
                                               const Aws::Lambda::Model::InvokeOutcome &outcome,
                                               const std::shared_ptr<const Aws::Client::AsyncCallerContext> &ctx) {
        using namespace std;
        stringstream ss;

        auto lctx = dynamic_cast<const AwsLambdaBackendCallerContext*>(ctx.get());
        assert(lctx);

        auto backend = lctx->getBackend();
        assert(backend);

        // Note: lambda needs to be explicitly configured for async invocation
        // -> https://docs.aws.amazon.com/lambda/latest/dg/lambda-dg.pdf, unhandled

        // recreate the original message
        messages::InvocationRequest invoke_req;
        invoke_req.ParseFromString(lctx->payload());

//        backend->lambdaCallback(req, outcome, lctx->time(), lctx->getTaskID());
        int statusCode = 0;

        if (!outcome.IsSuccess()) {
            auto &error = outcome.GetError();
            statusCode = static_cast<int>(error.GetResponseCode());

            // rate limit? => reissue request
            if(statusCode == static_cast<int>(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS) || // i.e. 429
               statusCode == static_cast<int>(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR)) {  // i.e. 500

                // invoke again
                backend->invokeAsync(invoke_req);
                backend->_numPendingRequests.fetch_add(-1, std::memory_order_release);

                backend->logger().info("LAMBDA task failed with [" + std::to_string(statusCode) + "], invoking again.");
                return;
            } else {
                ss << "LAMBDA task failed with ["<<statusCode<<"]" << outcome.GetError().GetExceptionName().c_str()
                   << outcome.GetError().GetMessage().c_str();
            }
        } else {
            // write response
            auto& result = outcome.GetResult();
            statusCode = result.GetStatusCode();
            string version = result.GetExecutedVersion().c_str();
            auto response = parsePayload(result);

            auto log = result.GetLogResult();

            if(response.status() == messages::InvocationResponse_Status_SUCCESS) {
                ss << "LAMBDA task done in " << response.taskexecutiontime() << "s ";
                string container_status = response.container().reused() ? "reused" : "new";
                ss << "[" << statusCode << ", " << pluralize(response.numrowswritten(), "row")
                   << ", " << pluralize(response.numexceptions(), "exception") << ", "
                   << container_status << ", id: " << response.container().uuid() << "] ";

                // extract info
                AwsLambdaBackend::InvokeInfo info = backend->parseFromLog(log.c_str());

                // lock and move to vector
                {
                    std::lock_guard<std::mutex> lock(backend->_mutex);
                    backend->_tasks.push_back(response);
                    backend->_infos.push_back(info);
                }

                // did request fail on Lambda?
                if(info.returnCode != 0) {
                    // stop execution
                    backend->_numPendingRequests.fetch_add(-1, std::memory_order_release);
                    backend->abortRequestsAndFailWith(info.returnCode, info.errorMessage);
                    return;
                }

                // compute cost and print out
                ss<<"Cost so far: $";
                double price = backend->lambdaCost();
                if(price < 0.01)
                    ss.precision(4);
                if(price < 0.0001)
                    ss.precision(6);
                ss<<std::fixed<<price;
            } else {
                // TODO: maybe still track the response info (e.g. reused, cost, etc.)
                ss<<"Lambda task failed, details: "<<response.errormessage();
            }
        }

        // log out message
        backend->logger().info(ss.str());

        // decrease wait counter
        backend->_numPendingRequests.fetch_add(-1, std::memory_order_release);
    }

    messages::InvocationResponse AwsLambdaBackend::parsePayload(const Aws::Lambda::Model::InvokeResult &result) {
        using namespace std;
        stringstream ss;
        auto& stream = const_cast<Aws::Lambda::Model::InvokeResult&>(result).GetPayload();
        ss<<stream.rdbuf();
        string data = ss.str();
        messages::InvocationResponse response;
        google::protobuf::util::JsonStringToMessage(data, &response);
        return response;
    }

    AwsLambdaBackend::AwsLambdaBackend(const Context& context,
                                       const AWSCredentials &credentials,
                                       const std::string &functionName) : IBackend(context), _credentials(credentials),
                                       _functionName(functionName), _options(context.getOptions()),
                                       _logger(Logger::instance().logger("aws-lambda")), _tag("tuplex"),
                                       _client(nullptr), _numPendingRequests(0), _numRequests(0) {


        _deleteScratchDirOnShutdown = false;
        _scratchDir = URI::INVALID;

        // // check that scratch dir is s3 path!
        // if(options.SCRATCH_DIR().prefix() != "s3://") // @TODO: check further it's a dir...
        //     throw std::runtime_error("need to provide as scratch dir an s3 path to Lambda backend");

        initAWS(credentials, _options.AWS_NETWORK_SETTINGS(), _options.AWS_REQUESTER_PAY());

        // several options are NOT supported currently in AWS Lambda Backend, hence
        // force them to what works
        if(_options.OPT_GENERATE_PARSER()) {
            logger().warn("using generated CSV parser not yet supported in AWS Lambda backend, defaulting back to original parser");
            _options.set("tuplex.optimizer.generateParser", "false");
        }
        if(_options.OPT_NULLVALUE_OPTIMIZATION()) {
            logger().warn("null value optimization not yet available for AWS Lambda backend, deactivating.");
            _options.set("tuplex.optimizer.nullValueOptimization", "false");
        }

        _driver.reset(new Executor(_options.DRIVER_MEMORY(),
                                   _options.PARTITION_SIZE(),
                                   _options.RUNTIME_MEMORY(),
                                   _options.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE(),
                                   _options.SCRATCH_DIR(), "aws-local-driver"));

        _lambdaSizeInMB = _options.AWS_LAMBDA_MEMORY();
        _lambdaTimeOut = _options.AWS_LAMBDA_TIMEOUT();

        logger().info("Execution over lambda with " + std::to_string(_lambdaSizeInMB) + "MB");

        // Lambda supports 1MB increments. Hence, no adjustment to 64MB granularity anymore necessary as in prior to Dec 2020.
        _lambdaSizeInMB = std::min(std::max(AWS_MINIMUM_LAMBDA_MEMORY_MB, _lambdaSizeInMB), AWS_MAXIMUM_LAMBDA_MEMORY_MB);
        logger().info("Adjusted lambda size to " + std::to_string(_lambdaSizeInMB) + "MB");

        if(_lambdaTimeOut < 10 || _lambdaTimeOut > 15 * 60) {
            _lambdaTimeOut = std::min(std::max(AWS_MINIMUM_TUPLEX_TIMEOUT_REQUIREMENT, _lambdaTimeOut), AWS_MAXIMUM_LAMBDA_TIMEOUT); // min 5s, max 15min
            logger().info("Adjusted lambda timeout to " + std::to_string(_lambdaTimeOut));
        }

        // init lambda client (Note: must be called AFTER aws init!)
        _client = makeClient();
    }

    void AwsLambdaBackend::waitForRequests(size_t sleepInterval) {
        // wait for requests to be finished & check periodically PyErrCheckSignals for Ctrl+C

        size_t pendingTasks = 0;
        while((pendingTasks = _numPendingRequests.load(std::memory_order_acquire)) > 0) {
            // sleep
            usleep(sleepInterval);

            python::lockGIL();

            if(PyErr_CheckSignals() != 0) {
               // stop requests & cleanup @TODO: cleanup on S3 with requests...
               if(_client)
                   _client->DisableRequestProcessing();
               _numPendingRequests.store(0, std::memory_order_acq_rel);
            }

            python::unlockGIL();
        }
    }

    static void printBreakdowns(const std::map<std::string, RollingStats<double>> &breakdownTimings, std::stringstream &ss) {
        ss << "{";

        size_t prefix_offset = 3;
        std::string found_prefixes[2] = {"", ""};
        std::string prefixes[2] = {"process_mem_", "process_file_"};
        std::map<std::string, RollingStats<double>> m[2];

        bool first_breakdown = true;
        auto print_breakdown = [&](const std::pair<std::string, RollingStats<double>> &keyval) {
            if(first_breakdown) {
                first_breakdown = false;
            } else {
                ss << ", ";
            }
            ss << "\"" << keyval.first << "\": { \"mean\": " << keyval.second.mean() << ", \"std\": "
               << keyval.second.std() << "}";
        };

        for(const auto& keyval: breakdownTimings) {
            auto is_prefix = [prefix_offset](const std::string &a, const std::string &b) {
                // check if a is a prefix of b
                if(a.size() + prefix_offset <= b.size()) {
                    auto res = std::mismatch(a.begin(), a.end(), b.begin() + prefix_offset);
                    return res.first == a.end();
                }
                return false;
            };
            if(is_prefix(prefixes[0], keyval.first) || is_prefix(prefixes[1], keyval.first)) {
                auto prefix_idx = is_prefix(prefixes[0], keyval.first) ? 0 : 1;
                found_prefixes[prefix_idx] = keyval.first.substr(0, prefixes[prefix_idx].length() + prefix_offset);
                auto suffix = keyval.first.substr(found_prefixes[prefix_idx].length());
                m[prefix_idx][suffix] = keyval.second;
            } else {
                print_breakdown(keyval);
            }
        }

        for(int i =0; i < 2; i++) {
            if(found_prefixes[i].empty()) continue;
            ss << ", ";
            ss << "\"" << found_prefixes[i] << "\": {";
            first_breakdown = true;
            for(const auto& keyval : m[i]) {
                print_breakdown(keyval);
            }
            ss << "}";
        }

        ss << "}\n";
    }

    void AwsLambdaBackend::printStatistics() {
        std::stringstream ss;

        {
            std::lock_guard<std::mutex> lock(_mutex);

            RollingStats<double> awsInitTime;
            RollingStats<double> taskExecutionTime;
            std::map<std::string, RollingStats<size_t>> s3Stats;
            std::map<std::string, RollingStats<double>> breakdownTimings;
            std::set<std::string> containerIDs;
            size_t numReused = 0;
            size_t numNew = 0;

            // aggregate stats over responses
            for (const auto &task : _tasks) {
                awsInitTime.update(task.awsinittime());
                taskExecutionTime.update(task.taskexecutiontime());
                for (const auto &keyval : task.s3stats()) {
                    auto key = keyval.first;
                    auto val = keyval.second;

                    auto it = s3Stats.find(key);
                    if (it == s3Stats.end())
                        s3Stats[key] = RollingStats<size_t>();
                    s3Stats[key].update(val);
                }
                for (const auto &keyval : task.breakdowntimes()) {
                    auto key = keyval.first;
                    auto val = keyval.second;

                    auto it = breakdownTimings.find(key);
                    if (it == breakdownTimings.end())
                        breakdownTimings[key] = RollingStats<double>();
                    breakdownTimings[key].update(val);
                }

                containerIDs.insert(task.container().uuid());
                numReused += task.container().reused();
                numNew += !task.container().reused();
            }

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

        ss<<"Lambda cost: $"<<lambdaCost();

        logger().info("LAMBDA statistics: \n" + ss.str());
    }

    AwsLambdaBackend::InvokeInfo AwsLambdaBackend::parseFromLog(const std::string& log) {
        InvokeInfo info;

        std::stringstream ss;
        // Decode the result header to see requested log information
        auto byteLogResult = Aws::Utils::HashingUtils::Base64Decode(log.c_str());
        for (unsigned i = 0; i < byteLogResult.GetLength(); i++)
            ss << byteLogResult.GetItem(i);
        auto logTail =  ss.str();

        // fetch RequestID, Duration, BilledDuration, MemorySize, MaxMemoryUsed from last line
        auto reportLine = logTail.substr(logTail.rfind("\nREPORT") + strlen("\nREPORT"), logTail.rfind('\n'));

        std::vector<std::string> tabCols;
        splitString(reportLine, '\t', [&](const std::string& s) { tabCols.emplace_back(s); });

        // extract parts
        for(auto col : tabCols) {
            trim(col);

            if (info.requestID.empty() && strStartsWith(col, "RequestId: ")) {
                // extract ID and store it
                info.requestID = col.substr(strlen("RequestId: "));
            }

            if (strStartsWith(col, "Duration: ") && strEndsWith(col, " ms")) {
                // extract ID and store it
                auto r = col.substr(strlen("Duration: "), col.length() - 3 - strlen("Duration: "));
                info.durationInMs = std::stod(r);
            }

            if (strStartsWith(col, "Billed Duration: ") && strEndsWith(col, " ms")) {
                // extract ID and store it
                auto r = col.substr(strlen("Billed Duration: "), col.length() - 3 - strlen("Billed Duration: "));
                info.billedDurationInMs = std::stoi(r);
            }

            if (strStartsWith(col, "Memory Size: ") && strEndsWith(col, " MB")) {
                // extract ID and store it
                auto r = col.substr(strlen("Memory Size: "), col.length() - 3 - strlen("Memory Size: "));
                info.memorySizeInMb = std::stoi(r);
            }

            if (strStartsWith(col, "Max Memory Used: ") && strEndsWith(col, " MB")) {
                // extract ID and store it
                auto r = col.substr(strlen("Max Memory Used: "), col.length() - 3 - strlen("Max Memory Used: "));
                info.maxMemoryUsedInMb = std::stoi(r);
            }

            // error message is formatted using RequestId: .... Error: ...
            // i.e., this here is the regex: RequestId:\s+([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\s+Error:.*
            if(strStartsWith(col, "RequestId: ") && col.find("Error: ") != std::string::npos) {
                // extract error message
                info.errorMessage = col.substr(col.find("Error: "));

                // per default assign retcode -1
                info.returnCode = -1;

                // find exit status via regex
                // exit status (\d+)
                std::regex re_exit_status("exit status (\\d+)");
                std::smatch base_match;
                if(regex_search(col, base_match, re_exit_status)) {
                    // sub_match is the first parenthesized expression.
                    if (base_match.size() == 2) {
                        std::ssub_match base_sub_match = base_match[1];
                        std::string base = base_sub_match.str();
                        info.returnCode = std::stoi(base);
                    }
                }
            }
        }
        return info;
    }

    size_t AwsLambdaBackend::getMB100Ms() {
        std::lock_guard<std::mutex> lock(_mutex);

        // sum up billed mb ms
        size_t billed = 0;
        for(auto info : _infos) {
            size_t billedDurationInMs = info.billedDurationInMs;
            size_t memorySizeInMb = info.memorySizeInMb;
            billed += billedDurationInMs / 100 * memorySizeInMb;
        }
        return billed;
    }

    size_t AwsLambdaBackend::getMBMs() {
        std::lock_guard<std::mutex> lock(_mutex);

        // sum up billed mb ms
        size_t billed = 0;
        for(auto info : _infos) {
            size_t billedDurationInMs = info.billedDurationInMs;
            size_t memorySizeInMb = info.memorySizeInMb;
            billed += billedDurationInMs * memorySizeInMb;
        }
        return billed;
    }

    URI AwsLambdaBackend::scratchDir(const std::vector<URI> &hints) {
        // is URI valid? return
        if(_scratchDir != URI::INVALID)
            return _scratchDir;

        // fetch dir from options
        auto ctx_scratch_dir = _options.AWS_SCRATCH_DIR();
        if(!ctx_scratch_dir.empty()) {
            _scratchDir = URI(ctx_scratch_dir);
            if(_scratchDir.prefix() != "s3://") // force S3
                _scratchDir = URI("s3://" + ctx_scratch_dir);
            _deleteScratchDirOnShutdown = false; // if given externally, do not delete per default
            return _scratchDir;
        }

        auto cache_folder = ".tuplex-cache";

        // check hints
        for(const auto &hint : hints) {
            if(hint.prefix() != "s3://") {
                logger().warn("AWS scratch dir hint given, but is no S3 URI: "+hint.toString());
                continue;
            }

            // check whether a file exists, if so skip, else valid dir found!
            auto dir = hint.join_path(cache_folder);
            if(!dir.exists()) {
                _scratchDir = dir;
                _deleteScratchDirOnShutdown = true;
                logger().info("Using " + dir.toString() + " as temporary AWS S3 scratch dir, will be deleted on tuplex context shutdown.");
                return _scratchDir;
            }
        }

        // invalid, no aws scratch dir available
        logger().error("requesting AWS S3 scratch dir, but none configured. Please set a AWS S3 scratch dir for the context by setting the config key tuplex.aws.scratchDir to a valid S3 URI");
        return URI::INVALID;
    }

    std::vector<URI> AwsLambdaBackend::hintsFromTransformStage(const TransformStage* stage) {
        std::vector<URI> hints;

        // take input and output folder as hints
        // prefer output folder hint over input folder hint
        if(stage->outputMode() == EndPointMode::FILE) {
            auto uri = stage->outputURI();
            if(uri.prefix() == "s3://")
                hints.push_back(uri);
        }

        if(stage->inputMode() == EndPointMode::FILE) {
            // TODO
            // get S3 uris, etc.
        }

        return hints;
    }

    void AwsLambdaBackend::reset() {
        _tasks.clear();
        _infos.clear();

        // other reset? @TODO.
    }

    void AwsLambdaBackend::abortRequestsAndFailWith(int returnCode, const std::string &errorMessage) {
        logger().error("LAMBDA execution failed due to exit code " + std::to_string(returnCode) + " on one executor, details: " + errorMessage);

        int numPending = std::max((int)_numPendingRequests, 0);
        if(numPending > 0)
            logger().info("Aborting " + pluralize(numPending, " pending request"));
        else
            logger().info("Aborting.");

        _numPendingRequests = 0;
        _client->DisableRequestProcessing();
        logger().info("Shutdown remote execution.");
        _client->EnableRequestProcessing();
    }
}
#endif