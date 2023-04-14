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
#include "AWSLambdaInvocationService.h"
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


    enum class AwsLambdaExecutionStrategy {
        UNKNOWN=0,
        DIRECT,
        TREE
    };

    inline AwsLambdaExecutionStrategy stringToAwsExecutionStrategy(const std::string& str) {
        std::string name = str;
        trim(name);
        for(auto& c : name)
             c = std::tolower(c);

        if("direct" == name)
            return AwsLambdaExecutionStrategy::DIRECT;
        if("tree" == name)
            return AwsLambdaExecutionStrategy::TREE;

        throw std::runtime_error("Unknown Lambda execution strategy " + str);

        return AwsLambdaExecutionStrategy::UNKNOWN;
    }



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
        std::unique_ptr<AwsLambdaInvocationService> _service; // <-- use this to invoke lambdas

        // fetch values via options
        size_t _lambdaSizeInMB;
        size_t _lambdaTimeOut;

        uint64_t _startTimestamp;
        uint64_t _endTimestamp;

        // statistics/info
        struct JobInfo {
            size_t total_input_normal_path;
            size_t total_input_general_path;
            size_t total_input_fallback_path;
            size_t total_input_unresolved;

            size_t total_output_rows;
            size_t total_output_exceptions;

            double cost;

            JobInfo() {}
        };

        JobInfo _info;

        // mapping of remote to local paths for result collection.
        std::unordered_map<URI, URI> _remoteToLocalURIMapping;

        // web ui
        HistoryServerConnection _historyConn;
        std::shared_ptr<HistoryServerConnector> _historyServer;

        void reset();

        void checkAndUpdateFunctionConcurrency(const std::shared_ptr<Aws::Lambda::LambdaClient>& client,
                                               size_t concurrency,
                                               const std::string& functionName,
                                               bool provisioned=false);
        size_t _functionConcurrency;

        std::vector<AwsLambdaRequest> createSingleFileRequests(const TransformStage* tstage,
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

        // Lambda request callbacks
        void onLambdaSuccess(const AwsLambdaRequest& req, const AwsLambdaResponse& resp);
        void onLambdaFailure(const AwsLambdaRequest& req, LambdaErrorCode err_code, const std::string& err_msg);
        void onLambdaRetry(const AwsLambdaRequest& req, LambdaErrorCode retry_code, const std::string& retry_msg, bool decreasesRetryCount);

        URI _scratchDir;
        bool _deleteScratchDirOnShutdown;

        /*!
         * returns a scratch dir. If none is stored/found, abort
         * @param hints one or more directories (typically buckets) where a temporary cache region could be stored.
         * @return URI or URI::INVALID
         */
        URI scratchDir(const std::vector<URI>& hints=std::vector<URI>{});

        // // store for tasks done
        std::mutex _mutex;
        std::vector<AwsLambdaResponse> _tasks;
        std::shared_ptr<Aws::Lambda::LambdaClient> makeClient();

        void invokeAsync(const AwsLambdaRequest& req);

        std::vector<URI> hintsFromTransformStage(const TransformStage* stage);

        inline MessageHandler logger() const { return _logger; }

        // void abortRequestsAndFailWith(int returnCode, const std::string& errorMessage);

        // std::set<std::string> performWarmup(const std::vector<int>& countsToInvoke, size_t timeOutInMs=4000, size_t baseDelayInMs=75);

        /*!
         * print extended lambda statistics out
         */
        void gatherStatistics();

        /*!
         * outputs all the stats etc. as json file for analysis
         * @param json_path
         */
        void dumpAsJSON(const std::string& json_path);

        std::string csvPerFileInfo();

        size_t getMB100Ms();
        size_t getMBMs();

        double usedGBSeconds() {
            // 1ms = 0.001s
            // 1mb = 0.001Gb
            return (double)getMBMs() / 1000000.0;

            // old: Before Dec 2020, now costs changed.
            // return (double)getMB100Ms() / 10000.0;
        }
        size_t numRequests() { assert(_service); return _service->numRequests(); }

        inline double lambdaCost() const {
            double cost_per_request = 0.0000002;

            // depends on ARM or x86 architecture
            // values from https://aws.amazon.com/lambda/pricing/ (Nov 2021)
            double cost_per_gb_second_x86 = 0.0000166667;
            double cost_per_gb_second_arm = 0.0000133334;

            double cost_per_gb_second = cost_per_gb_second_x86;

            // check architecture, else assume x86 cost
            if(_functionArchitecture == "arm64")
                cost_per_gb_second = cost_per_gb_second_arm;

            auto usedGBSeconds = _service->usedGBSeconds();
            auto numRequests = _service->numRequests();

            return usedGBSeconds * cost_per_gb_second + (double)numRequests * cost_per_request;
        }

        /*!
         * generate a baseURI for a temporary file.
         * @param stageNo
         * @return URI
         */
        inline URI tempStageURI(int stageNo) const {
            return URI(_options.AWS_SCRATCH_DIR() + "/temporary_stage_output/" + "stage_" + std::to_string(stageNo));
        }
    };
}

#endif //TUPLEX_AWSLAMBDABACKEND_H

#endif