//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 12/2/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LAMBDAWORKERAPP_H
#define TUPLEX_LAMBDAWORKERAPP_H

#include "WorkerApp.h"

#ifdef BUILD_WITH_AWS

#include <AWSCommon.h>
#include <ee/aws/ContainerInfo.h>
#include <ee/aws/RequestInfo.h>
#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/ListFunctionsRequest.h>
#include <aws/lambda/LambdaClient.h>

// safety time to allocate for creating proper response
#define AWS_LAMBDA_SAFETY_DURATION_IN_MS 1000

namespace tuplex {

    // externally link, i.e. in testing need dummy to make linking work!
    extern ContainerInfo getThisContainerInfo();

    struct LambdaWorkerSettings : public WorkerSettings {
        // add here specific Lambda settings
    };

    /// AWS
    /// Lambda specific Worker, inherits from base WorkerApp class
    class LambdaWorkerApp : public WorkerApp {
    public:
        LambdaWorkerApp(const LambdaWorkerSettings& ws) : WorkerApp(ws), _outstandingRequests(0) {

            // for Lambda install sighandler for curl
            _aws_options.httpOptions.installSigPipeHandler = true;
        }

        tuplex::messages::InvocationResponse generateResponse();

        int globalInit() override;

    protected:
        /// put here Lambda specific constants to easily update them
        static const std::string caFile;
        static const std::string tuplexRuntimePath;
        static const bool verifySSL;

        int processMessage(const tuplex::messages::InvocationRequest& req) override;

        MessageHandler& logger() const override {
            return Logger::instance().logger("Lambda worker");
        }

        std::string _functionName;
        NetworkSettings _networkSettings;
        tuplex::AWSCredentials _credentials;
    private:

        struct Metrics {
            double global_init_time;
        };
        Metrics metrics;

        // @TODO: redesign this...
        messages::MessageType _messageType;
        std::vector<ContainerInfo> _invokedContainers;
        std::vector<RequestInfo> _requests;
        std::vector<std::string> _output_uris;
        std::vector<std::string> _input_uris;

        inline void resetResult() {
            _messageType = messages::MT_UNKNOWN;
            _invokedContainers.clear();
            _requests.clear();
            _output_uris.clear();
            _input_uris.clear();
        }

        // self-invocation to scale-out
        struct SelfInvokeRequest {
            int requestIdx; // for self reference
            size_t max_retries;
            size_t retries;
            std::string payload; // json payload
            uint64_t tsStart; // timestamp for this request.

            struct Result {
                int returnCode;
                ContainerInfo container;
                RequestInfo invoke_desc;

                // all the containers any subinvocations invoked
                std::vector<ContainerInfo> invoked_containers;
                std::vector<RequestInfo> invoked_requests;

                std::vector<std::string> output_uris;
                std::vector<std::string> input_uris; // which parts succeeded processing

                inline bool success() const {
                    return returnCode == (int)messages::InvocationResponse_Status_SUCCESS;
                }
            };

            Result response; // response (to be overwritten on callback)

            SelfInvokeRequest() : retries(0), max_retries(0) {}
        };

        std::mutex _invokeRequestMutex;
        std::vector<SelfInvokeRequest> _invokeRequests;
        std::atomic_int _outstandingRequests;


        inline int addRequest(const SelfInvokeRequest& req) {
            std::unique_lock<std::mutex> lock(_invokeRequestMutex);
            _invokeRequests.push_back(req);
            auto requestNo = _invokeRequests.size() - 1;
            _invokeRequests.back().requestIdx = requestNo;
            _outstandingRequests++;
            return requestNo;
        }

        inline void decRequests() {
            _outstandingRequests--;
        }

        // helper function to check whether time is left on worker or not!
        std::chrono::high_resolution_clock::time_point _timeOutRefPoint;
        std::chrono::milliseconds _timeOutInMs;
        inline bool timeLeftOnLambda() const {
            auto now = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - _timeOutRefPoint);
            return duration < _timeOutInMs;
        }

        void setLambdaTimeout(std::chrono::milliseconds timeOutInMs) {
            _timeOutRefPoint = std::chrono::high_resolution_clock::now();
            _timeOutInMs = timeOutInMs;
        }

        /*!
         * invoke another Lambda function
         * @param timeout how many seconds to allow this Lambda invocation max
         * @param parts on which parts to run this Lambda invocation
         * @param base_output_uri where to save results for that particular lambda invocation.
         * @param partNoOffset which offset to use when generating part numbers
         * @param original_message original message (copy will be created and params overwritten)
         * @param max_retries how often to retry each request at most
         * @param invocation_counts recursive invocation counts
         */
        void invokeLambda(double timeout,
                          const std::vector<FilePart>& parts,
                          const URI& base_output_uri,
                          uint32_t partNoOffset,
                          const tuplex::messages::InvocationRequest& original_message,
                          size_t max_retries = 3,
                          const std::vector<size_t>& invocation_counts={});


        std::shared_ptr<Aws::Lambda::LambdaClient> _lambdaClient;
        std::shared_ptr<Aws::Lambda::LambdaClient> createClient(double timeout, size_t max_connections);


        struct LambdaRequestContext : public Aws::Client::AsyncCallerContext {
            LambdaWorkerApp *app;
            int requestIdx;

            LambdaRequestContext() = delete;
            LambdaRequestContext(LambdaWorkerApp* the_app, int idx) : app(the_app), requestIdx(idx) {}
        };

        // static functions/callbacks for invocation
        static void lambdaCallback(const Aws::Lambda::LambdaClient* client,
                                   const Aws::Lambda::Model::InvokeRequest& req,
                                   const Aws::Lambda::Model::InvokeOutcome& outcome,
                                   const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx);


        // callback
        void lambdaOnSuccess(SelfInvokeRequest& request, const messages::InvocationResponse& response,
                             const RequestInfo& desc);

        void lambdaOnFailure(SelfInvokeRequest& request,
                             int statusCode,
                             const std::string& errorName,
                             const std::string& errorMessage);

        void prepareResponseFromSelfInvocations();

    };

    extern std::vector<ContainerInfo> selfInvoke(const std::string& functionName,
                                               size_t count,
                                               const std::vector<size_t>& recursive_counts,
                                               size_t timeOutInMs,
                                               size_t baseDelayInMs,
                                               const tuplex::AWSCredentials& credentials,
                                               const NetworkSettings& ns,
                                               std::string tag="lambda");

    inline std::vector<ContainerInfo> selfInvoke(const std::string& functionName,
                                                 size_t count,
                                                 size_t timeOutInMs,
                                                 size_t baseDelayInMs,
                                                 const tuplex::AWSCredentials& credentials,
                                                 const NetworkSettings& ns,
                                                 std::string tag="lambda") {
        return selfInvoke(functionName, count, {}, timeOutInMs, baseDelayInMs, credentials, ns, tag);
    }

    // in invoked containers the same uuid may be present multiple timnes,
    // this functions cleans the member var
    extern std::vector<ContainerInfo> normalizeInvokedContainers(const std::vector<ContainerInfo>& containers);

    // check whether AWS env is set
    extern bool checkIfOptionIsSetInEnv(const std::string& option_name);

}

#endif
#endif //TUPLEX_LAMBDAWORKERAPP_H
