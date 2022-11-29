//
// Created by Leonhard Spiegelberg on 11/29/22.
//

#ifndef TUPLEX_WORKERBACKEND_H
#define TUPLEX_WORKERBACKEND_H


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
#include <regex>

namespace tuplex {

    class LogicalPlan;
    class PhysicalPlan;
    class PhysicalStage;

    /*!
     * check that worker exists or try to find worker. throws exception on failure
     * @param exe_path
     * @return (absolute) path to worker
     */
    extern std::string ensure_worker_path(const std::string& exe_path);

    class WorkerBackend : public IBackend {
    public:
        WorkerBackend() = delete;
        ~WorkerBackend() override;

        WorkerBackend(const Context& context, const std::string& exe_path="");

        Executor* driver() override { return _driver.get(); }
        void execute(PhysicalStage* stage) override;
    protected:
        ContextOptions _options;
        std::unique_ptr<Executor> _driver;

        MessageHandler& _logger;

        std::vector<messages::InvocationRequest> createSingleFileRequests(const TransformStage* tstage,
                                                                          const std::string& bitCode,
                                                                          const size_t numThreads,
                                                                          const std::vector<std::tuple<std::string, std::size_t>>& uri_infos,
                                                                          const std::string& spillURI, const size_t buf_spill_size);

        // decode fileURIS
        std::vector<std::tuple<std::string, size_t>> decodeFileURIs(const std::vector<Partition*>& partitions, bool invalidate=true);

        std::vector<URI> hintsFromTransformStage(const TransformStage* stage);

        inline MessageHandler logger() const { return _logger; }

        void abortRequestsAndFailWith(int returnCode, const std::string& errorMessage);

        void config_worker(messages::WorkerSettings* ws, size_t numThreads, const URI& spillURI, size_t buf_spill_size);
    private:
        URI _scratchDir;
        bool _deleteScratchDirOnShutdown;
        std::string _worker_exe_path;

        /*!
         * returns a scratch dir. If none is stored/found, abort
         * @param hints one or more directories (typically buckets) where a temporary cache region could be stored.
         * @return URI or URI::INVALID
         */
        URI scratchDir(const std::vector<URI>& hints=std::vector<URI>{});
    };
}

#endif //TUPLEX_WORKERBACKEND_H
