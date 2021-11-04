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

#ifndef TUPLEX_AWSLAMBDABACKEND_H
#define TUPLEX_AWSLAMBDABACKEND_H

// this bucket is used for testing (change to personal testing bucket in case)
// @TODO: make this an AWS option
#define S3_TEST_BUCKET "tuplex-test"

#include "../IBackend.h"
#include <vector>
#include <physical/TransformStage.h>
#include <physical/HashJoinStage.h>
#include <physical/AggregateStage.h>
#include <physical/BlockBasedTaskBuilder.h>
#include <physical/IExceptionableTask.h>
#include <numeric>
#include <physical/TransformTask.h>
#include <physical/ResolveTask.h>

#include "AWSCommon.h"
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

namespace tuplex {

    class LogicalPlan;
    class PhysicalPlan;
    class PhysicalStage;

    class AwsLambdaBackend : public IBackend {
    public:
        AwsLambdaBackend() = delete;
        ~AwsLambdaBackend();

        AwsLambdaBackend(const AWSCredentials& credentials, const std::string& functionName,
                const ContextOptions& options);

        Executor* driver() override { return _driver.get(); }
        void execute(PhysicalStage* stage) override;

    private:
        AWSCredentials _credentials;
        std::string _functionName; // name of the AWS lambda func.
        ContextOptions _options;
        std::unique_ptr<Executor> _driver;

        // AWS Lambda interface
        std::string _tag;
        std::shared_ptr<Aws::Lambda::LambdaClient> _client;

        // fetch values via options
        size_t _lambdaSizeInMB;
        size_t _lambdaTimeOut;

        struct InvokeInfo {
            std::string requestID;
            double durationInMs;
            size_t billedDurationInMs;
            size_t memorySizeInMb;
            size_t maxMemoryUsedInMb;

            InvokeInfo() : durationInMs(0), billedDurationInMs(100), memorySizeInMb(0), maxMemoryUsedInMb(0) {}
        };

        InvokeInfo parseFromLog(const std::string& log);

        void reset();

        URI _scratchDir;
        bool _deleteScratchDirOnShutdown;
        /*!
         * returns a scratch dir. If none is stored/found, abort
         * @param hints one or more directories (typically buckets) where a temporary cache region could be stored.
         * @return URI or URI::INVALID
         */
        URI scratchDir(const std::vector<URI>& hints=std::vector<URI>{});

        // store for tasks done
        std::mutex _mutex;
        std::vector<messages::InvocationResponse> _tasks;
        std::vector<InvokeInfo> _infos;

        std::shared_ptr<Aws::Lambda::LambdaClient> makeClient();

        std::atomic_int32_t _numPendingRequests; // how many tasks are open (atomic used for multi threaded execution)
        std::atomic_int32_t _numRequests;
        static void asyncLambdaCallback(const Aws::Lambda::LambdaClient* client,
                                        const Aws::Lambda::Model::InvokeRequest& req,
                                        const Aws::Lambda::Model::InvokeOutcome& outcome,
                                        const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx);
        void invokeAsync(const messages::InvocationRequest& req);

        // decode fileURIS
        std::vector<std::tuple<std::string, size_t>> decodeFileURIs(const std::vector<Partition*>& partitions, bool invalidate=true);

        std::vector<URI> hintsFromTransformStage(const TransformStage* stage);

        inline MessageHandler& logger() const { return Logger::instance().logger("aws-lambda"); }

        void waitForRequests(size_t sleepInterval=100*1000);

        static messages::InvocationResponse parsePayload(const Aws::Lambda::Model::InvokeResult &result);

        /*!
         * print extended lambda statistics out
         */
        void printStatistics();

        size_t getMB100Ms();
        double usedGBSeconds() {return (double)getMB100Ms() / 10000.0;}
        size_t numRequests() { return _numRequests; }

        double lambdaCost() {
            double cost_per_request = 0.0000002;
            double cost_per_gb_second = 0.0000166667;
            return usedGBSeconds() * cost_per_gb_second + numRequests() * cost_per_request;
        }
    };
}

#endif //TUPLEX_AWSLAMBDABACKEND_H

#endif