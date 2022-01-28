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
#include "ContainerInfo.h"
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

    class LogicalPlan;
    class PhysicalPlan;
    class PhysicalStage;

    class AwsLambdaBackend : public IBackend {
    public:
        AwsLambdaBackend() = delete;
        ~AwsLambdaBackend() override;

        AwsLambdaBackend(const Context& context, const AWSCredentials& credentials, const std::string& functionName);

        Executor* driver() override { return _driver.get(); }
        void execute(PhysicalStage* stage) override;

    private:
        AWSCredentials _credentials;
        std::string _functionName; // name of the AWS lambda func.
        std::string _functionArchitecture; // x86_64 | arm64
        ContextOptions _options;
        std::unique_ptr<Executor> _driver;

        MessageHandler& _logger;

        // AWS Lambda interface
        std::string _tag;
        std::shared_ptr<Aws::Lambda::LambdaClient> _client;

        // fetch values via options
        size_t _lambdaSizeInMB;
        size_t _lambdaTimeOut;

        int64_t _startTimestamp;


        void reset();

        void checkAndUpdateFunctionConcurrency(const std::shared_ptr<Aws::Lambda::LambdaClient>& client,
                                               size_t concurrency,
                                               const std::string& functionName,
                                               bool provisioned=false);
        size_t _functionConcurrency;

        std::vector<messages::InvocationRequest> createSingleFileRequests(const TransformStage* tstage,
                                                                          const std::string& bitCode,
                                                                          const size_t numThreads,
                                                                          const std::vector<std::tuple<
                                                                          std::string, std::size_t>>& uri_infos,
                                                                          const std::string& spillURI, const size_t buf_spill_size);

        std::vector<messages::InvocationRequest> createSelfInvokingRequests(const TransformStage* tstage,
                                                                          const std::string& bitCode,
                                                                          const size_t numThreads,
                                                                          const std::vector<std::tuple<
                                                                                  std::string, std::size_t>>& uri_infos,
                                                                          const std::string& spillURI, const size_t buf_spill_size);


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
        std::vector<LambdaInvokeDescription> _infos;

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

        inline MessageHandler logger() const { return _logger; }

        void waitForRequests(size_t sleepInterval=100*1000);

        static messages::InvocationResponse parsePayload(const Aws::Lambda::Model::InvokeResult &result);

        void abortRequestsAndFailWith(int returnCode, const std::string& errorMessage);

        std::set<std::string> performWarmup(const std::vector<int>& countsToInvoke, size_t timeOutInMs=4000, size_t baseDelayInMs=75);

        /*!
         * print extended lambda statistics out
         */
        void printStatistics();

        /*!
         * outputs all the stats etc. as json file for analysis
         * @param json_path
         */
        void dumpAsJSON(const std::string& json_path);

        size_t getMB100Ms();
        size_t getMBMs();

        double usedGBSeconds() {
            // 1ms = 0.001s
            // 1mb = 0.001Gb
            return (double)getMBMs() / 1000000.0;

            // old: Before Dec 2020, now costs changed.
            // return (double)getMB100Ms() / 10000.0;
        }
        size_t numRequests() { return _numRequests; }

        double lambdaCost() {
            double cost_per_request = 0.0000002;

            // depends on ARM or x86 architecture
            // values from https://aws.amazon.com/lambda/pricing/ (Nov 2021)
            double cost_per_gb_second_x86 = 0.0000166667;
            double cost_per_gb_second_arm = 0.0000133334;

            double cost_per_gb_second = cost_per_gb_second_x86;

            // check architecture, else assume x86 cost
            if(_functionArchitecture == "arm64")
                cost_per_gb_second = cost_per_gb_second_arm;

            return usedGBSeconds() * cost_per_gb_second + (double)numRequests() * cost_per_request;
        }
    };
}

#endif //TUPLEX_AWSLAMBDABACKEND_H

#endif