//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LOCALBACKEND_H
#define TUPLEX_LOCALBACKEND_H

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

namespace tuplex {

    class LogicalPlan;
    class PhysicalPlan;
    class PhysicalStage;

    class LocalBackend : public IBackend {
    public:

        LocalBackend() = delete;

        // note: must be virtual destructor when used in make_unique
        virtual ~LocalBackend();

        /*!
         * constructor for convenience
         * @param context
         */
        explicit LocalBackend(const Context& context);

        Executor* driver() override; // for local execution

        void execute(PhysicalStage* stage) override;
    private:
        Executor *_driver; //! driver from local backend...
        std::vector<Executor*> _executors; //! drivers to be used
        std::unique_ptr<JITCompiler> _compiler;

        HistoryServerConnection _historyConn;
        std::shared_ptr<HistoryServerConnector> _historyServer;

        ContextOptions _options;

        /*!
         * init or retrieve driver + as many executors as demanded from the Local execution engine
         */
        void initExecutors(const ContextOptions& options);

        /*!
         * release driver + executors from the local execution engine. Local Exec Engine does the final cleanup.
         */
        void freeExecutors();


        std::vector<IExecutorTask*> createLoadAndTransformToMemoryTasks(TransformStage* tstage, const ContextOptions& options, const std::shared_ptr<TransformStage::JITSymbols>& syms);
        void executeTransformStage(TransformStage* tstage);


        /*!
         * Create the final hashmap from all of the input [tasks] (e.g. either merge them (join) or combine them (aggregate)
         * @param tasks
         * @param hashtableKeyByteWidth The width of the keys in the hashtables (e.g. differentiate between i64 and str hashtable)
         * @param combine whether this is an aggregate (e.g. if we should call the aggregate combiner, rather than simply merging the hashtables)
         * @return the final hashtable sink
         */
        HashTableSink createFinalHashmap(const std::vector<const IExecutorTask*>& tasks,
                                         int hashtableKeyByteWidth,
                                         bool combine,
                                         codegen::agg_init_f init_aggregate,
                                         codegen::agg_combine_f combine_aggregate);

        // hash join stage
        void executeHashJoinStage(HashJoinStage* hstage);

        // aggregate stage
        void executeAggregateStage(AggregateStage* astage);


        MessageHandler& logger() const { return Logger::instance().logger("local ee"); }

        // write output (may be already in correct format!)
        void writeOutput(TransformStage* tstage, std::vector<IExecutorTask*>& sortedTasks);

        std::vector<IExecutorTask*> performTasks(std::vector<IExecutorTask*>& tasks, std::function<void()> driverCallback=[](){});

        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> calcExceptionCounts(const std::vector<IExecutorTask*>& tasks);

        inline size_t totalExceptionCounts(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> & counts) {
            return std::accumulate(counts.begin(), counts.end(), 0, [](size_t acc, std::pair<std::tuple<int64_t, ExceptionCode>, size_t> val) { return acc + val.second; });
        }

        inline std::vector<Partition*> getNormalPartitions(IExecutorTask* task) const {
            if(!task)
                return std::vector<Partition*>();

            if(task->type() == TaskType::UDFTRAFOTASK)
                return dynamic_cast<TransformTask*>(task)->getOutputPartitions();

            if(task->type() == TaskType::RESOLVE)
                return dynamic_cast<ResolveTask*>(task)->getOutputPartitions();

            throw std::runtime_error("unknown task type seen");
            return std::vector<Partition*>();
        }

        inline std::vector<Partition*> getExceptionPartitions(IExecutorTask* task) const {
            if(!task)
                return std::vector<Partition*>();

            if(task->type() == TaskType::UDFTRAFOTASK)
                return dynamic_cast<TransformTask*>(task)->getExceptionPartitions();

            if(task->type() == TaskType::RESOLVE)
                return dynamic_cast<ResolveTask*>(task)->getExceptions();

            throw std::runtime_error("unknown task type seen in " + std::string(__FILE_NAME__) + ":" + std::to_string(__LINE__));
            return std::vector<Partition*>();
        }

        inline std::vector<Partition*> getGeneralPartitions(IExecutorTask* task) const {
            if(!task)
                return std::vector<Partition*>();

            if(task->type() == TaskType::UDFTRAFOTASK)
                return std::vector<Partition*>();

            if(task->type() == TaskType::RESOLVE)
                return dynamic_cast<ResolveTask *>(task)->exceptionsFromTargetSchema();

            throw std::runtime_error("unknown task type seen in " + std::string(__FILE_NAME__) + ":" + std::to_string(__LINE__));
            return std::vector<Partition*>();
        }

        inline std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> getExceptionCounts(IExecutorTask* task) const {
            if(!task)
                return std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>();

            if(task->type() == TaskType::UDFTRAFOTASK)
                return dynamic_cast<TransformTask*>(task)->exceptionCounts();

            if(task->type() == TaskType::RESOLVE)
                return dynamic_cast<ResolveTask*>(task)->exceptionCounts();

            throw std::runtime_error("unknown task type seen in " + std::string(__FILE_NAME__) + ":" + std::to_string(__LINE__));
            return std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>();
        }

        inline std::vector<Partition*> getFallbackPartitions(IExecutorTask* task) const {
            if(!task)
                return std::vector<Partition*>();

            if(task->type() == TaskType::UDFTRAFOTASK)
                return std::vector<Partition*>(); // none here, can be only result from ResolveTask.

            if(task->type() == TaskType::RESOLVE)
                return dynamic_cast<ResolveTask*>(task)->getOutputFallbackPartitions();

            throw std::runtime_error("unknown task type seen in " + std::string(__FILE_NAME__) + ":" + std::to_string(__LINE__));
            return std::vector<Partition*>();
        }

        std::vector<IExecutorTask*> resolveViaSlowPath(std::vector<IExecutorTask*>& tasks,
                bool merge_rows_in_order,
                codegen::resolve_f functor,
                TransformStage* tstage, bool combineHashmaps,
               codegen::agg_init_f init_aggregate,
               codegen::agg_combine_f combine_aggregate);
    };

    /*!
     * construct output path based either on a base URI or via a udf
     * @param udf
     * @param baseURI
     * @param partNo
     * @param fmt
     * @return
     */
    extern URI outputURI(const UDF& udf, const URI& baseURI, int64_t partNo, FileFormat fmt);

    /*!
     * converts python fallback path into runnable func object
     * @param py_code python code as string
     * @param pipeline_name the name of the pipeline function
     * @return nullptr or PyObject* referring to the function named pipeline_name
     */
    extern PyObject* preparePythonPipeline(const std::string& py_code, const std::string& pipeline_name);

    /*!
     * helper function for debugging which dumps exceptions as python objects to file.
     * @param local_path local path where to store exceptions
     * @param exceptions which exceptions
     */
    extern void dumpExceptionsForFallback(const std::string& local_path,
                                          const Schema& exceptionInputSchema,
                                          const std::vector<Partition*>& exceptions,
                                          bool invalidate_exceptions=false);
}

#endif //TUPLEX_LOCALBACKEND_H