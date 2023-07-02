//
// Created by Leonhard Spiegelberg on 11/29/22.
//

#include <ee/worker/WorkerBackend.h>
#include <FileHelperUtils.h>
#include <procinfo.h>
#include <google/protobuf/util/json_util.h>
#include "ee/worker/WorkerApp.h"
#include "ee/local/LocalBackend.h"

namespace tuplex {
    WorkerBackend::WorkerBackend(const tuplex::Context &context,
                                 const std::string &exe_path) : IBackend(context),
                                                                _worker_exe_path(exe_path),
                                                                _options(context.getOptions()),
                                                                _deleteScratchDirOnShutdown(false),
                                                                _logger(Logger::instance().logger("worker")),
                                                                _scratchDir(URI::INVALID) {
        _worker_exe_path = ensure_worker_path(exe_path);

        _driver.reset(new Executor(_options.DRIVER_MEMORY(),
                                   _options.PARTITION_SIZE(),
                                   _options.RUNTIME_MEMORY(),
                                   _options.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE(),
                                   _options.SCRATCH_DIR(), "worker-local-driver"));
    }

    WorkerBackend::~WorkerBackend() {

    }


    void WorkerBackend::prepareTransformStage(tuplex::TransformStage &stage, const std::string &target_triple,
                                              const std::string &cpu) {
        // convert if necessary bitcode -> object via compilation & update everything.

        // optimize if desired
        if(_options.USE_LLVM_OPTIMIZER()) {
            Timer timer;
            LLVMOptimizer opt;
            stage.optimizeBitCode(opt);
            std::stringstream ss;
            ss<<"Client-side optimization via LLVM took "<<timer.time()<<"s";
            logger().info(ss.str());
        }

        // now convert to object file if format is bitcode (or ir)
        if(_options.EXPERIMENTAL_INTERCHANGE_CODE_VIA_OBJECT_FILES()) {
            Timer timer;
            stage.compileToObjectCode(target_triple, cpu);
            std::stringstream ss;
            ss<<"Client-side compilation to native code for cpu="<<cpu<<", triple="<<target_triple<<" took "<<timer.time()<<"s";
            logger().info(ss.str());
        }
    }

    void WorkerBackend::execute(PhysicalStage* stage) {
        using namespace std;

        auto tstage = dynamic_cast<TransformStage *>(stage);
        if (!tstage)
            throw std::runtime_error("only transform stage from AWS Lambda backend yet supported");

        vector<tuple<std::string, size_t>> uri_infos;
        if(tstage->inputMode() != EndPointMode::FILE) {
            // throw std::runtime_error("only file mode yet supported");
        } else {
            // simply decode uris from input partitions...
            uri_infos = decodeFileURIs(tstage->inputPartitions());
        }

        std::string target_triple = llvm::sys::getDefaultTargetTriple();
        std::string cpu = "native"; // <-- native target cpu
        prepareTransformStage(*tstage, target_triple, cpu);

        if(stage->outputMode() != EndPointMode::FILE) {
            // throw std::runtime_error("only file mode yet supported");
        }

        size_t numThreads = 1;
        logger().info("executors each use " + pluralize(numThreads, "thread"));
        auto scratchDir = _options.AWS_SCRATCH_DIR();
        if(scratchDir.empty())
            scratchDir = _options.SCRATCH_DIR().toPath();
        auto spillURI = scratchDir + "/spill_folder";
        // perhaps also use:  - 64 * numThreads ==> smarter buffer scaling necessary.
        size_t buf_spill_size = (_options.AWS_LAMBDA_MEMORY() - 256) / numThreads * 1000 * 1024;

        // limit to 128mb each
        if (buf_spill_size > 128 * 1000 * 1024)
            buf_spill_size = 128 * 1000 * 1024;

        logger().info("Setting buffer size for each thread to " + sizeToMemString(buf_spill_size));

        auto requests = createSingleFileRequests(tstage,
                                                 numThreads, uri_infos, spillURI,
                                            buf_spill_size);

        if (!requests.empty()) {
            logger().info("Invoking " + pluralize(requests.size(), "request") + " ...");

#ifndef NDEBUG
            logger().debug("Emitting request files for easier debugging...");
            for(unsigned i = 0; i < requests.size(); ++i) {
                std::string json_buf;
                google::protobuf::util::MessageToJsonString(requests[i], &json_buf);
                stringToFile(URI("request_" + std::to_string(i) + ".json"), json_buf);
            }
            logger().debug("Debug files written (" + pluralize(requests.size(), "file") + ").");
#endif
            // process using local app
            auto app = make_unique<WorkerApp>(WorkerSettings());
            app->globalInit(true);
            auto stats_array = nlohmann::json::array();
            auto req_array = nlohmann::json::array();
            size_t total_input_rows = 0;
            size_t total_num_output_rows = 0;
            for (const auto &req: requests) {
                Timer timer;
                std::string json_buf;
                google::protobuf::util::MessageToJsonString(req, &json_buf);
                auto rc = app->processJSONMessage(json_buf);
                // fetch result
                auto stats = app->jsonStats();

                if(stats.empty()) {
                    throw std::runtime_error("no statistics available, internal error?");
                }

                auto j_stats = nlohmann::json::parse(stats);
                nlohmann::json j;
                auto j_req = nlohmann::json::parse(json_buf);
                j["request"] = req.inputuris();
                j["stats"] = j_stats;
                stats_array.push_back(j);
                total_num_output_rows += j_stats["output"]["normal"].get<size_t>() + j_stats["output"]["except"].get<size_t>();
                total_input_rows += j_stats["input"]["total_input_row_count"].get<size_t>();
                req_array.push_back(j_req);

                logger().info("Processed request in " + std::to_string(timer.time()) + "s, rc=" + std::to_string(rc));
            }

            logger().info("total input row count: " + std::to_string(total_input_rows));
            logger().info("total output row count: " + std::to_string(total_num_output_rows));

            // dump
            logger().info("JSON:\n" + stats_array.dump(2));

            // write output to file
            nlohmann::json j_all;
            j_all["responses"] = stats_array;
            j_all["requests"] = req_array;
            j_all["total_input_row_count"] = total_input_rows;
            auto job_uri = URI("worker_app_job.json");
            stringToFile(job_uri, j_all.dump());
            logger().info("Saved job to " + job_uri.toPath());

        } else {
            logger().warn("No requests generated, skipping stage.");
        }
        tstage->setMemoryResult({});
    }

    std::vector<messages::InvocationRequest>
    WorkerBackend::createSingleFileRequests(const TransformStage *tstage,
                                            const size_t numThreads,
                                            const std::vector<std::tuple<std::string, std::size_t>> &uri_infos,
                                            const std::string &spillURI, const size_t buf_spill_size) {
        std::vector<messages::InvocationRequest> requests;

        size_t splitSize = 256 * 1024 * 1024; // 256MB split size for now.
        splitSize = _options.INPUT_SPLIT_SIZE();
        size_t total_size = 0;
        for (auto info: uri_infos) {
            total_size += std::get<1>(info);
        }

        logger().info("Found " + pluralize(uri_infos.size(), "uri")
                      + " with total size = " + sizeToMemString(total_size) + " (split size=" +
                      sizeToMemString(splitSize) + ")");

        // new: part based
        // Note: for now, super simple: 1 request per file (this is inefficient, but whatever)
        // @TODO: more sophisticated splitting of workload!
        Timer timer;
        int num_digits = ilog10c(uri_infos.size());
        for (int i = 0; i < uri_infos.size(); ++i) {
            auto info = uri_infos[i];

            // the smaller
            auto uri_size = std::get<1>(info);
            auto num_parts_per_file = uri_size / splitSize;

            auto num_digits_part = ilog10c(num_parts_per_file + 1);
            int part_no = 0;
            size_t cur_size = 0;
            while (cur_size < uri_size) {
                messages::InvocationRequest req;
                req.set_type(messages::MessageType::MT_TRANSFORM);
                auto pb_stage = tstage->to_protobuf();

                auto rangeStart = cur_size;
                auto rangeEnd = std::min(cur_size + splitSize, uri_size);

                req.set_allocated_stage(pb_stage.release());

                // add request for this
                auto inputURI = std::get<0>(info);
                auto inputSize = std::get<1>(info);
                inputURI += ":" + std::to_string(rangeStart) + "-" + std::to_string(rangeEnd);
                req.add_inputuris(inputURI);
                req.add_inputsizes(inputSize);

                // worker config
                auto ws = std::make_unique<messages::WorkerSettings>();
                auto worker_spill_uri = spillURI + "/lam" + fixedPoint(i, num_digits) + "/" + fixedPoint(part_no, num_digits_part);
                config_worker(ws.get(), _options, numThreads, worker_spill_uri, buf_spill_size);
                req.set_allocated_settings(ws.release());
                req.set_verboselogging(_options.AWS_VERBOSE_LOGGING());

                // output uri of job? => final one? parts?
                // => create temporary if output is local! i.e. to memory etc.
                int taskNo = i;
                if (tstage->outputMode() == EndPointMode::MEMORY) {
                    // create temp file in scratch dir!
                    req.set_baseoutputuri(scratchDir(hintsFromTransformStage(tstage)).join_path(
                            "output.part" + fixedLength(taskNo, num_digits) + "_" +
                            fixedLength(part_no, num_digits_part)).toString());
                } else if (tstage->outputMode() == EndPointMode::FILE) {
                    // create output URI based on taskNo
                    auto uri = outputURI(tstage->outputPathUDF(), tstage->outputURI(), taskNo, tstage->outputFormat());
                    req.set_baseoutputuri(uri.toPath());
                } else if (tstage->outputMode() == EndPointMode::HASHTABLE) {
                    // there's two options now, either this is an end-stage (i.e., unique/aggregateByKey/...)
                    // or an intermediate stage where a temp hash-table is required.
                    // in any case, because compute is done on Lambda materialize hash-table as temp file.
                    auto temp_uri = tempStageURI(tstage->number());
                    req.set_baseoutputuri(temp_uri.toString());
                } else throw std::runtime_error("unknown output endpoint in lambda backend");
                requests.push_back(req);

                part_no++;
                cur_size += splitSize;
            }
        }

        return requests;
    }

    std::vector<URI> WorkerBackend::hintsFromTransformStage(const TransformStage *stage) {
        std::vector<URI> hints;

        // take input and output folder as hints
        // prefer output folder hint over input folder hint
        if (stage->outputMode() == EndPointMode::FILE) {
            auto uri = stage->outputURI();
            if (uri.prefix() == "s3://")
                hints.push_back(uri);
        }

        if (stage->inputMode() == EndPointMode::FILE) {
            // TODO
            // get S3 uris, etc.
        }

        return hints;
    }

    URI WorkerBackend::scratchDir(const std::vector<URI> &hints) {
        // is URI valid? return
        if (_scratchDir != URI::INVALID)
            return _scratchDir;

        // fetch dir from options
        auto ctx_scratch_dir = _options.AWS_SCRATCH_DIR();
        if (!ctx_scratch_dir.empty()) {
            _scratchDir = URI(ctx_scratch_dir);
            if (_scratchDir.prefix() != "s3://") // force S3
                _scratchDir = URI("s3://" + ctx_scratch_dir);
            _deleteScratchDirOnShutdown = false; // if given externally, do not delete per default
            return _scratchDir;
        }

        auto cache_folder = ".tuplex-cache";

        // check hints
        for (const auto &hint: hints) {
            if (hint.prefix() != "s3://") {
                logger().warn("AWS scratch dir hint given, but is no S3 URI: " + hint.toString());
                continue;
            }

            // check whether a file exists, if so skip, else valid dir found!
            auto dir = hint.join_path(cache_folder);
            if (!dir.exists()) {
                _scratchDir = dir;
                _deleteScratchDirOnShutdown = true;
                logger().info("Using " + dir.toString() +
                              " as temporary AWS S3 scratch dir, will be deleted on tuplex context shutdown.");
                return _scratchDir;
            }
        }

        // invalid, no aws scratch dir available
        logger().error(
                "requesting AWS S3 scratch dir, but none configured. Please set a AWS S3 scratch dir for the context by setting the config key tuplex.aws.scratchDir to a valid S3 URI");
        return URI::INVALID;
    }

    void config_worker(messages::WorkerSettings *ws,
                                      const ContextOptions& options,
                                      size_t numThreads,
                                      const URI &spillURI,
                                      size_t buf_spill_size) {
        if(!ws)
            return;
        ws->set_numthreads(numThreads);
        ws->set_normalbuffersize(buf_spill_size);
        ws->set_exceptionbuffersize(buf_spill_size);
        ws->set_useinterpreteronly(options.PURE_PYTHON_MODE());
        ws->set_usecompiledgeneralcase(!options.RESOLVE_WITH_INTERPRETER_ONLY());
        ws->set_normalcasethreshold(options.NORMALCASE_THRESHOLD());
        ws->set_spillrooturi(spillURI.toString());

        // set other fields for some other option settings
        std::vector<std::string> other_keys({"tuplex.experimental.opportuneCompilation",
                                             "tuplex.experimental.s3PreCacheSize",
                                             "tuplex.useLLVMOptimizer",
                                             // "tuplex.sample.maxDetectionRows",
                                             // "tuplex.sample.strataSize",
                                             // "tuplex.sample.samplesPerStrata",
                                             "tuplex.optimizer.constantFoldingOptimization",
                                             "tuplex.optimizer.filterPromotion",
                                             "tuplex.experimental.forceBadParseExceptFormat",
                                             "tuplex.experimental.specializationUnitSize",
                                             "tuplex.experimental.interchangeWithObjectFiles"});
        auto& m_map = *(ws->mutable_other());
        for(const auto& key : other_keys)
            m_map[key] = options.get(key);

        // special case, remap sampling numbers
        m_map["tuplex.sample.maxDetectionMemory"] = std::to_string(options.AWS_LAMBDA_SAMPLE_MAX_DETECTION_MEMORY());
        m_map["tuplex.sample.maxDetectionRows"] = std::to_string(options.AWS_LAMBDA_SAMPLE_MAX_DETECTION_ROWS());
        m_map["tuplex.sample.strataSize"] = std::to_string(options.AWS_LAMBDA_SAMPLE_STRATA_SIZE());
        m_map["tuplex.sample.samplesPerStrata"] = std::to_string(options.AWS_LAMBDA_SAMPLE_SAMPLES_PER_STRATA());
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