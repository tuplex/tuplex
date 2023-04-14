//
// Created by leonhards on 4/13/23.
//

#ifndef TUPLEX_AWSLAMBDAINVOCATIONSERVICE_H
#define TUPLEX_AWSLAMBDAINVOCATIONSERVICE_H

#ifdef BUILD_WITH_AWS

#include <vector>
#include <physical/execution/TransformStage.h>
#include <physical/execution/HashJoinStage.h>
#include <physical/execution/AggregateStage.h>
#include <physical/codegen/BlockBasedTaskBuilder.h>
#include <physical/execution/IExceptionableTask.h>
#include <numeric>
#include <physical/execution/TransformTask.h>
#include <physical/execution/ResolveTask.h>

#include <utils/Messages.h>

#include "AWSCommon.h"
#include "ContainerInfo.h"
#include "RequestInfo.h"
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
#include <regex>

namespace tuplex {
     enum class LambdaErrorCode {
        ERROR_UNKNOWN,
        ERROR_TIMEOUT,
        ERROR_RATE_LIMIT,
        ERROR_RETRIES_EXHAUSTED,
        ERROR_TASK
    };

    struct AwsLambdaRequest {
        uniqueid_t id;
        messages::InvocationRequest body;
        size_t retriesLeft;
        std::vector<LambdaErrorCode> retryErrors;

        AwsLambdaRequest() : id(getUniqueID()), retriesLeft(5) {}

        // get input description
        std::string input_desc() const {
            if(body.inputuris_size() == 0)
                return "no files";
            if(body.inputuris_size() == 1)
                return body.inputuris(0);

            std::vector<std::string> files;
            for(unsigned i = 0; i < body.inputuris_size(); ++i) {
                files.push_back(body.inputuris(i));
            }
            std::stringstream ss;
            ss<<files;
            return ss.str();
        }
    };

    struct AwsLambdaResponse {
        messages::InvocationResponse response;
        RequestInfo info;
    };

    class AwsLambdaInvocationService {
    private:
        std::mutex _mutex;
        std::shared_ptr<Aws::Lambda::LambdaClient> _client;
        std::string _functionName;
        std::atomic_int32_t _numPendingRequests; // how many tasks are open (atomic used for multi threaded execution)
        std::atomic_int32_t _numRequests;

        // cost tracking (do it here!)
        std::atomic_uint64_t _mbms; // megabyte milliseconds.

        //std::vector<AwsLambdaRequest> _pendingRequests; // <-- todo, check this that the task is removed.
        static void asyncLambdaCallback(const Aws::Lambda::LambdaClient *client,
                                        const Aws::Lambda::Model::InvokeRequest &aws_req,
                                        const Aws::Lambda::Model::InvokeOutcome &aws_outcome,
                                        const std::shared_ptr<const Aws::Client::AsyncCallerContext> &ctx);

    public:
        AwsLambdaInvocationService() = delete;
        AwsLambdaInvocationService(const std::shared_ptr<Aws::Lambda::LambdaClient>& client,
                                   const std::string& function_name) : _client(client),
                                   _functionName(function_name),
                                   _numPendingRequests(0), _numRequests(0), _mbms(0) { assert(client); }

        void reset();

        inline double usedGBSeconds() const { return (double)_mbms / 1000000.0; }

        /*!
         * stops all active requests and return number of requests that have been aborted
         * @param print whether to print out failure or not.
         * @return number of aborted requests
         */
        size_t abortAllRequests(bool print);

        MessageHandler& logger() const { return Logger::instance().logger("LAMBDA"); }

        size_t numRequests() const { return _numRequests; }

        inline void addCost(size_t billedDurationInMs, size_t memorySizeInMb) {
            _mbms += billedDurationInMs * memorySizeInMb;
        }

        void waitForRequests(size_t sleepInterval=100*1000);

        bool invokeAsync(const AwsLambdaRequest& req,
                         std::function<void(const AwsLambdaRequest&, const AwsLambdaResponse&)> onSuccess=[](const AwsLambdaRequest& req, const AwsLambdaResponse& resp) {},
                         std::function<void(const AwsLambdaRequest&, LambdaErrorCode, const std::string&)> onFailure=[](const AwsLambdaRequest& req,
                                                                                                                        LambdaErrorCode err_code,
                                                                                                                        const std::string& err_msg) {},
                         std::function<void(const AwsLambdaRequest&, LambdaErrorCode, const std::string&, bool)> onRetry=[](const AwsLambdaRequest& req,
                                                                                                                            LambdaErrorCode retry_code,
                                                                                                                            const std::string& retry_reason,
                                                                                                                            bool willDecreaseRetryCount) {});
    };

    extern messages::InvocationResponse AwsParseRequestPayload(const Aws::Lambda::Model::InvokeResult &result);
}

#endif
#endif //TUPLEX_AWSLAMBDAINVOCATIONSERVICE_H
