//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_HISTORYSERVERCONNECTOR_H
#define TUPLEX_HISTORYSERVERCONNECTOR_H

#include <chrono>
#include <Logger.h>
#include <logical/LogicalOperator.h>
#include "ContextOptions.h"
#include "HistoryServerClasses.h"
#include "RESTInterface.h"
#include <mt/ConcurrentCountingMap.h>
#include <physical/SampleProcessor.h>
#include <ExceptionCodes.h>
#include <set>
#include <TransformStageExceptionReservoir.h>

namespace tuplex {

    class LogicalOperator;
    class Executor;
    class SampleProcessor;
    class Partition;
    class PhysicalPlan;
    class TransformStageExceptionReservoir;

    using exception_map=std::map<int64_t, std::unordered_map<int64_t, std::vector<Partition*>>>;

    class HistoryServerConnector {
    private:
        std::string _jobID;
        HistoryServerConnection _conn;
        size_t _exceptionDisplayLimit;
        std::string _contextName;
        std::string _trackURL;

        // each trafo stage has an exception sample reservoir
        // --> can be updated fast if necessary
        std::vector<std::shared_ptr<TransformStageExceptionReservoir>> _reservoirs;
        // map to store which reservoir corresponds to which operator
        std::unordered_map<int64_t, std::shared_ptr<TransformStageExceptionReservoir>> _reservoirLookup;

        /*!
         * simulates (lineage!) traceback, exception sample & Co to display for operator/ec combo.
         * @param operatorID
         * @param ec
         * @param exceptions
         * @param limit
         * @return true if new sample was added, false else.
         */
        bool addExceptionSamples(int64_t operatorID, const ExceptionCode& ec, size_t ecCount, const std::vector<Partition*>& exceptions, size_t limit, bool excludeAvailableResolvers);

        void initResolverLookupMap(const PhysicalPlan* plan);

        void sendExceptionCounts(int64_t opID, const std::unordered_map<int64_t, size_t>& ecounts);

        HistoryServerConnector(const HistoryServerConnection& conn,
                               const std::string& jobID,
                               const std::string& contextName,
                               const std::string& trackURL,
                               size_t exceptionDisplayLimit,
                               const PhysicalPlan* plan,
                               unsigned maxExceptions=512);

        std::vector<std::string> getParentsColumnNames(int64_t operatorID);


        /*!
         * check whether a resolver exists for given opID and exceptioncode
         * @param opID
         * @param ec
         * @return
         */
        bool resolverExists(int64_t opID, ExceptionCode ec);

        std::shared_ptr<LogicalOperator> getOperator(int64_t opID);
        int64_t getOperatorIndex(int64_t opID); // get index within stage of operator
    public:

        HistoryServerConnector() = delete;


        /*!
         * connect to running Tuplex History server instance.
         * @param host host, i.e. localhost
         * @param port port to use
         * @param mongo_host host of mongodb database for history server
         * @param mongo_port port of mongodb database for history server
         * @return connection object. If connection failed, connected field will be set to false
         */
        static HistoryServerConnection connect(const std::string& host,
                                        uint16_t port,
                                                      const std::string& mongo_host="localhost",
                                                      uint16_t mongo_port=27017);

        /*!
         * create a new connector object used to send messages to history server
         * @param conn  connection to use
         * @param contextName name of context to use to register job
         * @param plan physical plan to send along (allows access to logical plan as well!)
         * @param options options of context object for this job
         * @param maxExceptions maximum number of exceptions to be tracked within the WebUI
         * @return connector object
         */
        static std::shared_ptr<HistoryServerConnector> registerNewJob(const HistoryServerConnection& conn,
                                                     const std::string& contextName,
                                                     const PhysicalPlan* plan,
                                                     const ContextOptions& options,
                                                     unsigned maxExceptions=512);


        /*!
         * send a quick status update
         * @param status Started, Running, Scheduled, Finished
         * @param num_open_tasks
         * @param num_finished_tasks
         */
        void sendStatus(JobStatus status, unsigned num_open_tasks, unsigned num_finished_tasks);


        void sendStatus(JobStatus status);

        void sendTrafoTask(int stageID,
                int64_t num_input_rows,
                int64_t num_output_rows,
                const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts,
                const std::vector<Partition*>& exceptions,
                bool excludeAvailableResolvers=true);

        /*!
         * get the url under which to track the job associated with the connector object
         * @return string, i.e. a url
         */
        std::string trackURL() const { return _trackURL; }



        void sendStagePlan(const std::string& stageName,
                           const std::string& unoptimizedIR,
                           const std::string& optimizedIR,
                           const std::string& assemblyCode);

        void sendStageResult(int64_t stageNumber,
                             size_t numInputRows,
                             size_t numOutputRows,
                             const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts);

    };
}

#endif //TUPLEX_HISTORYSERVERCONNECTOR_H