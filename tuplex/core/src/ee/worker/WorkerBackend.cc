//
// Created by Leonhard Spiegelberg on 11/29/22.
//

#include <ee/worker/WorkerBackend.h>
#include <procinfo.h>
#include <FileHelperUtils.h>

namespace tuplex {
    WorkerBackend::WorkerBackend(const tuplex::Context &context,
                                 const std::string &exe_path) : IBackend(context),
                                                                _worker_exe_path(exe_path),
                                                                _options(context.getOptions()),
                                                                _deleteScratchDirOnShutdown(false),
                                                                _logger(Logger::instance().logger("worker")),
                                                                _scratchDir(URI::INVALID) {
        _worker_exe_path = ensure_worker_path(exe_path);
    }

    WorkerBackend::~WorkerBackend() {

    }

    void WorkerBackend::execute(PhysicalStage* stage) {
        using namespace std;

        auto tstage = dynamic_cast<TransformStage *>(stage);
        if (!tstage)
            throw std::runtime_error("only transform stage from AWS Lambda backend yet supported");

        vector<tuple<std::string, size_t>> uri_infos;
        if(tstage->inputMode() != EndPointMode::FILE) {
            throw std::runtime_error("only file mode yet supported");
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
            if (!tstage->fastPathBitCode().empty()) {
                llvm::LLVMContext ctx;
                LLVMOptimizer opt;
                auto mod = codegen::bitCodeToModule(ctx, tstage->fastPathBitCode());
                opt.optimizeModule(*mod);
                optimizedBitcode = codegen::moduleToBitCodeString(*mod);
                optimized_ir = true;
            } else if (!tstage->slowPathBitCode().empty()) {
                // todo...
            }
            if (optimized_ir)
                logger().info("client-side LLVM IR optimization of took " + std::to_string(timer.time()) + "s");
        } else {
            optimizedBitcode = tstage->fastPathBitCode();
        }

        if(stage->outputMode() != EndPointMode::FILE) {
            throw std::runtime_error("only file mode yet supported");
        }

        size_t numThreads = 1;
        logger().info("executors each use " + pluralize(numThreads, "thread"));
        auto spillURI = _options.AWS_SCRATCH_DIR() + "/spill_folder";
        // perhaps also use:  - 64 * numThreads ==> smarter buffer scaling necessary.
        size_t buf_spill_size = (_options.AWS_LAMBDA_MEMORY() - 256) / numThreads * 1000 * 1024;

        // limit to 128mb each
        if (buf_spill_size > 128 * 1000 * 1024)
            buf_spill_size = 128 * 1000 * 1024;

        logger().info("Setting buffer size for each thread to " + sizeToMemString(buf_spill_size));
    }

    std::string find_worker() {
        using namespace tuplex;

        // find worker executable (tuplex-worker)
        static const std::string exec_name = "tuplex-worker";

        // check current working dir
        auto exec_dir = dir_from_pid(pid_from_self());

        auto path = exec_dir + "/" + exec_name;

        if(!fileExists(path))
            throw std::runtime_error("Could not find worker under " + path);

        return eliminateSeparatorRuns(path);
    }

    std::string ensure_worker_path(const std::string& exe_path) {
        if(exe_path.empty()) {
            // find relative to current path
            auto path =  find_worker();
            if(!path.empty())
                return path;
        } else {
            // check it exists, is local and executable
            //if(fil)
            assert(false);
        }
        throw std::runtime_error("failed to ensure a worker path for path '" + exe_path + "'");
        return "";
    }

}