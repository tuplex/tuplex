//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 11/22/2021                                                               //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#include "ee/worker/WorkerApp.h"
#include <physical/execution/TransformTask.h>
#include "ee/local/LocalBackend.h"
#include "visitors/TypeAnnotatorVisitor.h"
#include "AWSCommon.h"
#include "bucket.h"
#include <physical/execution/JsonReader.h>
#include <S3FileSystemImpl.h>
#include <S3Cache.h>

namespace tuplex {

    int64_t dummy_cell_functor(void* userData, int64_t num_cells, char **cells , int64_t* cell_sizes) {
        return 0;
    }

    int WorkerApp::globalInit(bool skip) {
        // skip if already initialized.
        if(_globallyInitialized)
            return WORKER_OK;

        if(skip) {
            _globallyInitialized = true;
            return WORKER_OK;
        }

        logger().info("WorkerAPP globalInit");
#ifdef BUILD_WITH_CEREAL
        logger().info("Using Cereal AST serialization");
#else
        logger().info("Using JSON AST serializastion");
#endif
        // runtime library path
        auto runtime_path = ContextOptions::defaults().RUNTIME_LIBRARY().toPath();

        // std::string python_home_dir = python::find_stdlib_location();
        // if(python_home_dir.empty()) {
        //     logger().error("Could not detect python stdlib location");
        //     return WORKER_ERROR_NO_PYTHON_HOME;
        // }

        NetworkSettings ns;
        ns.verifySSL = false;

#ifdef BUILD_WITH_AWS
        auto credentials = AWSCredentials::get();
        initAWS(credentials, ns, true);
#endif

        if(!runtime::init(runtime_path)) {
            logger().error("runtime specified as " + std::string(runtime_path) + " could not be found.");
            return WORKER_ERROR_NO_TUPLEX_RUNTIME;
        }

        // // init python home dir to point to directory where stdlib files are installed, cf.
        // // https://www.python.org/dev/peps/pep-0405/ for meaning of it. Per default, just use default behavior.
        // python::python_home_setup(python_home_dir);

        logger().info("Initializing python interpreter version " + python::python_version(true, true));
        python::initInterpreter();
        python::unlockGIL();

        _globallyInitialized = true;
        return WORKER_OK;
    }

    bool WorkerApp::reinitialize(const WorkerSettings &settings) {
        _settings = settings;

        // general init here...
        // compiler already active? Else init
        logger().info("performing global initialization (Worker App)");
        if(WORKER_OK != globalInit())
            return false;

        // before initializing compiler, make sure runtime has been loaded
        assert(runtime::loaded());

        if(!_compiler)
            _compiler = std::make_shared<JITCompiler>();

        if(!_fastCompiler)
            _fastCompiler = std::make_shared<JITCompiler>(llvm::CodeGenOpt::None); // <-- no codegen opt to circumvent bug.

        initThreadEnvironments(_settings.numThreads);

        // init s3 cache if required
        if(_settings.s3PreCacheSize != 0) {
            // make sure s3 system is initialized
            auto s3impl = VirtualFileSystem::getS3FileSystemImpl();
            if(!s3impl) {
                logger().error("required S3 cache, but S3 file system not initialized.");
                return false;
            }
            s3impl->activateReadCache(_settings.s3PreCacheSize);
        } else {
            auto s3impl = VirtualFileSystem::getS3FileSystemImpl();
            if(!s3impl) {
                logger().error("required S3 cache, but S3 file system not initialized.");
                return false;
            }
            s3impl->disableReadCache();
        }

        return true;
    }

    void WorkerApp::initThreadEnvironments(size_t numThreads) {
        // initialize thread buffers (which get passed to functions)
        _numThreads = std::max(1ul, numThreads);
        if(_threadEnvs) {
            // check if the same, if not reinit?

            // @TODO: release hashmap memory
            delete [] _threadEnvs;
        }
        _threadEnvs = new ThreadEnv[_numThreads];
        // init buffers
        for(int i = 0; i < _numThreads; ++i) {
            _threadEnvs[i].threadNo = i;
            _threadEnvs[i].app = this;
            _threadEnvs[i].normalBuf.provideSpace(_settings.normalBufferSize);
            _threadEnvs[i].exceptionBuf.provideSpace(_settings.exceptionBufferSize);

            // TODO: if other hashmaps are supported, init them here as well...
            _threadEnvs[i].hashMap = hashmap_new();
            _threadEnvs[i].nullBucket = nullptr; // empty bucket.
        }
    }

    void WorkerApp::resetThreadEnvironments() {
        // reset codepath stats
        _codePathStats.reset();
        for(int i = 0; i < _numThreads; ++i) {
            _threadEnvs[i].reset();
        }

        _output_uris.clear();

        _timeDict.clear();

        // reset default compile policy based on worker settings...
        codegen::DEFAULT_COMPILE_POLICY.allowNumericTypeUnification = _settings.allowNumericTypeUnification;
        codegen::DEFAULT_COMPILE_POLICY.normalCaseThreshold = _settings.normalCaseThreshold;
        codegen::DEFAULT_COMPILE_POLICY.allowUndefinedBehavior = false; // not avail
        codegen::DEFAULT_COMPILE_POLICY.sharedObjectPropagation = true; // not avail, but activate.
    }

    void WorkerApp::shutdown() {

        // join slow path compile thread
        if(_resolverCompileThread && _resolverCompileThread->joinable()) {
            _resolverCompileThread->join();
            _resolverCompileThread.reset();
        }

        if(python::isInterpreterRunning()) {
            python::lockGIL();
            python::closeInterpreter();
        }

        // do not call free, but instead releaseRunTimeMemory
         runtime::releaseRunTimeMemory();

#ifdef BUILD_WITH_AWS
        // causes error!!!
        // cf. https://github.com/aws/aws-sdk-cpp/issues/456 --> implement something like this.
        // shutdownAWS();
#endif
    }

    int WorkerApp::messageLoop() {
        return 0;
    }

    int WorkerApp::processJSONMessage(const std::string &message) {
        auto& logger = this->logger();

        // do this first.
        resetThreadEnvironments();

        // logger.info("JSON request: " + message);

        // parse JSON into protobuf
        tuplex::messages::InvocationRequest req;
        auto rc = google::protobuf::util::JsonStringToMessage(message, &req);
        if(!rc.ok()) {
            logger.error("could not parse json into protobuf message, bad parse for request - invalid format?");
            return WORKER_ERROR_INVALID_JSON_MESSAGE;
        }

        _currentMessage = req;

        // get worker settings from message, if they differ from current setup -> reinitialize worker!
        {
            std::stringstream ss;
            ss<<"current worker settings: "<<_settings;
            logger.info(ss.str());
        }
        auto settings = settingsFromMessage(req);
        if(settings != _settings) {
            // reinit with new settings...
            reinitialize(settings);
            _settings = settings; // make sure the new ones are now used!!!
            std::stringstream ss;
            ss<<"settings from message are different, reinitialized with: "<<_settings;
            logger.info(ss.str());
        }

        // currently message represents merely a transformstage/transformtask
        // @TODO: in the future this could change!
        auto tstage = TransformStage::from_protobuf(req.stage());

        for(unsigned i = 0; i < req.inputuris_size(); ++i) {
            logger.debug("input uri: " + req.inputuris(i) + " size: " + std::to_string(req.inputsizes(i)));
        }

        // reset compiled symbols & wait for old slow thread in case
        if(_resolverCompileThread && _resolverCompileThread->joinable())
            _resolverCompileThread->join(); // wait till compile thread finishes...
        _resolverCompileThread.reset(nullptr);
        {
            std::lock_guard<std::mutex> lock(_symsMutex);
            _syms.reset();
        }

        return processMessage(req);
    }

    int64_t WorkerApp::initTransformStage(const TransformStage::InitData& initData,
                                          const std::shared_ptr<TransformStage::JITSymbols> &syms) {
        if(!syms->_fastCodePath.initStageFunctor) {
            logger().info("skip init_trafo_stage, b.c. symbol not found");
            return 0;
        }

        // initialize stage
        int64_t init_rc = 0;
        logger().info("calling initStageFunctor with " + std::to_string(initData.numArgs) + " args");
        if((init_rc = syms->_fastCodePath.initStageFunctor(initData.numArgs,
                                             reinterpret_cast<void**>(initData.hash_maps),
                                             reinterpret_cast<void**>(initData.null_buckets))) != 0) {
            logger().error("initStage() failed for stage with code " + std::to_string(init_rc));
            return WORKER_ERROR_STAGE_INITIALIZATION;
        }

        // init aggregate by key
        if(syms->aggAggregateFunctor) {
            initThreadLocalAggregateByKey(syms->aggInitFunctor, syms->aggCombineFunctor, syms->aggAggregateFunctor);
        }
        else {
            if (syms->aggInitFunctor && syms->aggCombineFunctor) {
                initThreadLocalAggregates(_numThreads + 1, syms->aggInitFunctor, syms->aggCombineFunctor);
            }
        }
        return WORKER_OK;
    }

    int64_t WorkerApp::releaseTransformStage(const std::shared_ptr<TransformStage::JITSymbols>& syms) {
        if(!syms->_fastCodePath.initStageFunctor || !syms->_fastCodePath.releaseStageFunctor) {
            logger().info("skip release trafo stage, b.c. symbols not found");
            return 0;
        }

        // call release func for stage globals
        if(syms->_fastCodePath.releaseStageFunctor() != 0) {
            logger().error("releaseStage() failed for stage ");
            return WORKER_ERROR_STAGE_CLEANUP;
        }

        return WORKER_OK;
    }

    std::vector<FilePart> WorkerApp::partsFromMessage(const tuplex::messages::InvocationRequest& req, bool silent) {
        std::vector<FilePart> parts;

        // decode parts from URIs

        // input uris
        std::vector<URI> input_uris;
        std::vector<size_t> input_sizes;

        for(const auto& path : req.inputuris())
            input_uris.emplace_back(URI(path));
        for(auto file_size : req.inputsizes())
            input_sizes.emplace_back(file_size);

        auto num_input_files = input_uris.size();

        if(input_uris.size() != input_sizes.size())
            throw std::runtime_error("Invalid JSON message, need to have same number of sizes/uris");

        if(!silent)
            logger().info("Found " + std::to_string(num_input_files) + " input URIs to process");

        // push back full part?
        for(unsigned i = 0; i < input_uris.size(); ++i) {
            FilePart fp;
            fp.partNo = i;
            fp.rangeStart = 0;
            fp.rangeEnd = 0;

            // decode uri
            decodeRangeURI(input_uris[i].toString(), fp.uri, fp.rangeStart, fp.rangeEnd);

            fp.size = input_sizes[i];
            parts.push_back(fp);
        }

        return parts;
    }

    int WorkerApp::processMessage(const tuplex::messages::InvocationRequest& req) {
        _messageCount++;

        // reset buffers
        resetThreadEnvironments();

        // only transform stage yet supported, in the future support other stages as well!
        auto tstage = TransformStage::from_protobuf(req.stage());

        // update initial received schemas for stage
        _stage_normal_input_type = tstage->normalCaseInputSchema().getRowType();
        _stage_normal_output_type = tstage->normalCaseOutputSchema().getRowType();
        _stage_general_input_type = tstage->inputSchema().getRowType();
        _stage_general_output_type = tstage->outputSchema().getRowType();

        URI outputURI = outputURIFromReq(req);
        auto parts = partsFromMessage(req);

        if(0 != _settings.s3PreCacheSize)
            preCacheS3(parts);

        // reset types
        _normalCaseRowType = tstage->normalCaseInputSchema().getRowType();
        _hyperspecializedNormalCaseRowType = python::Type::UNKNOWN;
        _ncAndHyperNCIncompatible = false;

        // check settings, pure python mode?
        if(req.settings().has_useinterpreteronly() && req.settings().useinterpreteronly()) {
            logger().info("WorkerApp is processing everything in single-threaded python/fallback mode.");
            return processTransformStageInPythonMode(tstage, parts, outputURI);
        }

        // using hyper-specialization?
        if(useHyperSpecialization(req)) {

            // check if encoded AST is compatible...
            if(!astFormatCompatible(req))
                return WORKER_ERROR_INCOMPATIBLE_AST_FORMAT;

            logger().info("*** hyperspecialization active ***");
            Timer timer;
            // use first input file
            std::string uri = req.inputuris(0);
            size_t file_size = req.inputsizes(0);
            logger().info("-- specializing to " + uri);

            std::string irCodeBefore = tstage->fastPathBitCode();
            logger().info("fast code path size before hyperspecialization: " + sizeToMemString(irCodeBefore.size()));

            // check if specialized normal-case type is different from current normal case type
            _normalCaseRowType = tstage->normalCaseInputSchema().getRowType(); // needed when fastcode path is missing?
            auto normalCaseCols = tstage->normalCaseInputColumnsToKeep();
            // note that this function may or may not succeed. If it fails, original fast code path is used.
            codegen::StageBuilderConfiguration conf;
            conf.policy.normalCaseThreshold = _settings.normalCaseThreshold;
            conf.constantFoldingOptimization = _settings.useConstantFolding;
            conf.exceptionSerializationMode = _settings.exceptionSerializationMode;
            conf.filterPromotion = _settings.useFilterPromotion;
            bool hyper_rc = hyperspecialize(tstage,
                                            uri,
                                            file_size,
                                            _settings.sampleLimitCount,
                                            _settings.strataSize,
                                            _settings.samplesPerStrata,
                                            conf);
            _hyperspecializedNormalCaseRowType = tstage->normalCaseInputSchema().getRowType(); // refactor?
            if(hyper_rc) {
                auto hyperspecializedNormalCaseCols = tstage->normalCaseInputColumnsToKeep();

                // note: types could be identical but projected columns different!
                if(_hyperspecializedNormalCaseRowType != _normalCaseRowType ||
                   !vec_equal(normalCaseCols, hyperspecializedNormalCaseCols)) {
                    logger().info("specialized normal-case type " + _hyperspecializedNormalCaseRowType.desc() + " is different than given normal-case type " + _normalCaseRowType.desc() + ".");
                    _ncAndHyperNCIncompatible = true;
                }

                // update normal in / out
                _stage_normal_input_type = tstage->normalCaseInputSchema().getRowType();
                _stage_normal_output_type = tstage->normalCaseOutputSchema().getRowType();

                // !!! need to use LLVM optimizers !!! Else, there's no difference.
                logger().info("-- hyperspecialization took " + std::to_string(timer.time()) + "s");
            } else {
                logger().warn("-- hyperspecialization failed in " + std::to_string(timer.time())
                              + "s, using original, provided fast code path.");
            }
            logger().info("fast code path size after hyperspecialization: " + sizeToMemString(tstage->fastPathBitCode().size()));
            markTime("hyperspecialization_time", timer.time());
        }

        // wait for old compile thread...
        if(_resolverCompileThread && _resolverCompileThread->joinable()) {
            _resolverCompileThread->join(); // <-- need to join old existing thread or else program terminates...
        }

        // reset internal syms
        _syms.reset(new TransformStage::JITSymbols());

        // if not, compile given code & process using both compile code & fallback
        // optimize via LLVM when in hyper mode.
        Timer compileTimer;
        auto syms = compileTransformStage(*tstage, useHyperSpecialization(req));
        if(!syms)
            return WORKER_ERROR_COMPILATION_FAILED;
        markTime("compile_time", compileTimer.time());

        // opportune compilation? --> do this here b.c. lljit is not thread-safe yet?
        // kick off general case compile then
        if(_settings.opportuneGeneralPathCompilation && _settings.useCompiledGeneralPath) {
            // create new thread to compile slow path (in parallel to running fast path)
            _resolverCompileThread.reset(new std::thread([this, tstage]() {
                auto resolver = getCompiledResolver(tstage);
            }));
            //_resolverFuture = std::async(std::launch::async, [this, tstage]() {
            //    return getCompiledResolver(tstage);
            //});
        }

        auto rc = processTransformStage(tstage, syms, parts, outputURI);
        _lastStat = jsonStat(req, tstage); // generate stats before returning.
        return rc;
    }

    void WorkerApp::preCacheS3(const std::vector<FilePart> &parts) {
        // pre-cache in S3 file cache all the parts!
        Timer timer;
        auto& cache = S3FileCache::instance();
        cache.reset(_settings.s3PreCacheSize);
        std::vector<std::future<size_t>> futures;
        for(const auto& part : parts) {
            if(part.uri.prefix() == "s3://")
                futures.emplace_back(cache.putAsync(part.uri, part.rangeStart, part.rangeEnd));
        }

        size_t total_cached = 0;
        for(auto& f : futures) {
            total_cached += f.get();
        }
        std::stringstream ss;
        auto cache_time = timer.time();
        double s3ReadSpeed = (total_cached / (1024.0 * 1024.0)) / cache_time;
        ss<<"Cached "<<total_cached<<" bytes in "<<cache_time<<"s from S3 ( "<<s3ReadSpeed<<" MB/s)";
        logger().info(ss.str());
    }

    int
    WorkerApp::processTransformStageInPythonMode(const TransformStage *tstage, const std::vector<FilePart> &input_parts,
                                                 const URI &output_uri) {
        // make sure python code exists
        assert(tstage);
        auto pythonCode = tstage->purePythonCode();
        auto pythonPipelineName = tstage->pythonPipelineName();
        if(pythonCode.empty() || pythonPipelineName.empty())
            return WORKER_ERROR_MISSING_PYTHON_CODE;

        logger().info("Invoking processTransformStage in Python mode");

        // compile func
        auto pipelineFunctionObj = preparePythonPipeline(pythonCode, pythonPipelineName);

        logger().info("pipeline prepared");

        // now go through input parts (files) and read them into python!
        // (single-threaded), could do multi-processing...

        // init single-threaded env
        initThreadEnvironments(1);

        logger().info("Thread environment (single-thread) prepared");

        runtime::setRunTimeMemory(_settings.runTimeMemory, _settings.runTimeMemoryDefaultBlockSize);

        logger().info("runtime memory initialized, attempting to lock GIL");

        int64_t numInputRowsProcessed = 0;

        // loop over parts & process
        python::lockGIL();
        logger().info("GIL locked, processing " + pluralize(input_parts.size(), "part"));
        for(const auto& part : input_parts) {
            size_t inputRowCount = 0;
            auto rc = processSourceInPython(0, tstage->fileInputOperatorID(),
                                            part, tstage, pipelineFunctionObj, false, &inputRowCount);
            logger().info("part processed, rc=" + std::to_string(rc));
            numInputRowsProcessed += inputRowCount;
            if(rc != WORKER_OK) {
                python::unlockGIL();
                runtime::releaseRunTimeMemory();
                return rc;
            }
        }

        python::unlockGIL();
        runtime::releaseRunTimeMemory();

        // all sources are processed, because fallback path was used no exception resolution necessary.
        // Exceptions are "true" exceptions
        logger().info("Writing parts out to destination file");

        // write output parts (incl. spilled parts) to output file
        auto rc = writeAllPartsToOutput(output_uri, tstage->outputFormat(), tstage->outputOptions());

        // compute number of successful normal-case rows -> rest is unresolved
        auto exception_row_count = get_exception_row_count();
        assert(numInputRowsProcessed >= exception_row_count);
        _codePathStats.rowsOnNormalPathCount += numInputRowsProcessed - exception_row_count;
        _codePathStats.unresolvedRowsCount += exception_row_count;
        _codePathStats.inputRowCount += numInputRowsProcessed;

        if(rc != WORKER_OK)
            return rc;
        return WORKER_OK;
    }

    int WorkerApp::processTransformStage(TransformStage *tstage,
                                         const std::shared_ptr<TransformStage::JITSymbols> &syms,
                                         const std::vector<FilePart> &input_parts, const URI &output_uri) {
        Timer timer;
        size_t minimumPartSize = 1024 * 1024; // 1MB.

        // init stage, abort on error
        auto rc = initTransformStage(tstage->initData(), syms);
        if(rc != WORKER_OK)
            return rc;

        size_t numInputRowsProcessed = 0;
        auto numCodes = std::max(1ul, _numThreads);
        auto processCodes = new int[numCodes];
        memset(processCodes, WORKER_OK, sizeof(int) * numCodes);
        Timer fastPathTimer;
        // process data (single-threaded or via thread pool!)
        if(_numThreads <= 1) {
            logger().info("setting runtime memory for single-threaded execution");
            runtime::setRunTimeMemory(_settings.runTimeMemory, _settings.runTimeMemoryDefaultBlockSize);

            try {
                // single-threaded
                logger().info("Single-threaded worker starting fast-path execution.");
                for(unsigned i = 0; i < input_parts.size(); ++i) {
                   const auto& fp = input_parts[i];
                    size_t inputRowCount = 0;
                    processCodes[0] = processSource(0, tstage->fileInputOperatorID(), fp, tstage, syms, &inputRowCount);
                    logger().info("processed file " + std::to_string(i + 1) + "/" + std::to_string(input_parts.size()));
                    if(processCodes[0] != WORKER_OK)
                        break;
                    numInputRowsProcessed += inputRowCount;
                }
            } catch(const std::exception& e) {
                logger().error("exception occurred in single-threaded mode: " + std::string(e.what()));
                processCodes[0] = WORKER_ERROR_EXCEPTION;
            } catch(...) {
                logger().error("unknown exception occurred in single-threaded mode.");
                processCodes[0] = WORKER_ERROR_EXCEPTION;
            }
            runtime::releaseRunTimeMemory();

            _codePathStats.inputRowCount += numInputRowsProcessed;
        } else {
            // multi-threaded
            // -> split into parts according to size and distribute between threads!
            auto parts = splitIntoEqualParts(_numThreads, input_parts, minimumPartSize);
            auto num_parts = 0;
            for(auto part : parts)
                num_parts += part.size();
            logger().debug("split input data into " + pluralize(num_parts, "part"));

#ifndef NDEBUG
            {
                std::stringstream ss;
                ss<<"Parts overview:\n";
                for(auto part : parts) {
                    for(auto p : part) {
                        ss<<"Part "<<p.partNo<<": "<<p.uri.toString()<<":"<<p.rangeStart<<"-"<<p.rangeEnd<<"\n";
                    }
                }
                logger().debug(ss.str());
            }
#endif

            // launch threads & process in each assigned parts
            std::vector<std::thread> threads;
            threads.reserve(_numThreads);

            std::vector<size_t> v_inputRowCount(_numThreads, 0);

            for(int i = 1; i < _numThreads; ++i) {
                threads.emplace_back([this, tstage, &syms, &processCodes, &v_inputRowCount](int threadNo, const std::vector<FilePart>& parts) {
                    logger().debug("thread (" + std::to_string(threadNo) + ") started.");

                    runtime::setRunTimeMemory(_settings.runTimeMemory, _settings.runTimeMemoryDefaultBlockSize);

                    try {
                        for(const auto& part : parts) {
                            logger().debug("thread (" + std::to_string(threadNo) + ") processing part via fast-path");
                            size_t inputRowCount = 0;
                            processCodes[threadNo] = processSource(threadNo, tstage->fileInputOperatorID(), part, tstage, syms, &inputRowCount);
                            if(processCodes[threadNo] != WORKER_OK)
                                break;
                            v_inputRowCount[threadNo] += inputRowCount;
                        }
                    } catch(const std::exception& e) {
                        logger().error(std::string("exception recorded: ") + e.what());
                        processCodes[threadNo] = WORKER_ERROR_EXCEPTION;
                    } catch(...) {
                        logger().error("unknown exception encountered, abort.");
                        processCodes[threadNo] = WORKER_ERROR_EXCEPTION;
                    }

                    logger().debug("thread (" + std::to_string(threadNo) + ") done.");

                    // release here runtime memory...
                    runtime::releaseRunTimeMemory();

                }, i, std::cref(parts[i]));
            }

            // process with this thread data as well!
            logger().debug("thread (main) processing started");
            runtime::setRunTimeMemory(_settings.runTimeMemory, _settings.runTimeMemoryDefaultBlockSize);

            try {
                for(const auto& part : parts[0]) {
                    logger().debug("thread (main) processing part");
                    size_t inputRowCount = 0;
                    processCodes[0] = processSource(0, tstage->fileInputOperatorID(), part, tstage, syms, &inputRowCount);
                    if(processCodes[0] != WORKER_OK)
                        break;
                    v_inputRowCount[0] += inputRowCount;
                }
            } catch(const std::exception& e) {
                logger().error(std::string("exception recorded: ") + e.what());
                processCodes[0] = WORKER_ERROR_EXCEPTION;
            } catch(...) {
                logger().error("unknown exception encountered, abort.");
                processCodes[0] = WORKER_ERROR_EXCEPTION;
            }

            logger().debug("thread (main) processing done, waiting for others to finish.");
            // release here runtime memory...
            runtime::releaseRunTimeMemory();

            // wait for all threads to finish (what about interrupt signal?)
            // std::for_each(threads.begin(), threads.end(), std::mem_fn(&std::thread::join));
            for(auto& thread : threads) {
                if(thread.joinable())
                    thread.join();
                logger().debug("Thread joined");
            }

            // calc total count
            numInputRowsProcessed = 0;
            for(auto el : v_inputRowCount)
                numInputRowsProcessed += el;
            _codePathStats.inputRowCount += numInputRowsProcessed;
            logger().debug("All threads joined, processing done. Processed "
                            + pluralize(numInputRowsProcessed, "input row") + ".");
        }

        // check return codes of threads...
        bool failed = false;
        for(unsigned i = 0; i < numCodes; ++i) {
            if(processCodes[i] != WORKER_OK) {
                logger().error("Thread " + std::to_string(i) + " failed processing with code " + std::to_string(processCodes[i]));
                failed = true;
            }
        }
        delete [] processCodes;
        if(failed) {
            // save invoke message to scratch
            storeInvokeRequest();

            return WORKER_ERROR_PIPELINE_FAILED;
        }
        logger().info("fast path took: " + std::to_string(fastPathTimer.time()) + "s");
        markTime("fast_path_execution_time", fastPathTimer.time());

        // update paths
        // compute number of successful normal-case rows -> rest is unresolved
        auto exception_row_count = get_exception_row_count();

        logger().debug("Of " + pluralize(numInputRowsProcessed, "input row")
                       + " there are " + pluralize(exception_row_count, "exception row") + ".");

        // limiting, fix later.
        exception_row_count = std::min(exception_row_count, numInputRowsProcessed);
        assert(numInputRowsProcessed >= exception_row_count);
        _codePathStats.rowsOnNormalPathCount += numInputRowsProcessed - exception_row_count;
        _codePathStats.unresolvedRowsCount += exception_row_count;

        logger().info("Input rows processed: normal: " + std::to_string(_codePathStats.rowsOnNormalPathCount)
                        + " unresolved: " + std::to_string(_codePathStats.unresolvedRowsCount));
        // display exception info:
        size_t num_exceptions = 0;
        size_t num_normal = 0;
        for(unsigned i = 0; i < _numThreads; ++i) {
            auto& env = _threadEnvs[i];
            num_exceptions += env.numExceptionRows;
            num_normal += env.numNormalRows;
        }

        // @TODO: write exceptions in order...
        auto stats_before_resolve = get_row_stats(tstage);
        logger().info("Normal rows before resolve: " + std::to_string(std::get<0>(stats_before_resolve)) + ", except rows before resolve: " + std::to_string(std::get<1>(stats_before_resolve)));

        // for now, everything out of order
        logger().info("Starting exception resolution/slow path execution");

        // print out whether compiled/interpreter paths are available
        {
            std::stringstream ss;
            auto pythonCode = tstage->purePythonCode();
            auto pythonPipelineName = tstage->pythonPipelineName();
            ss<<"has interpreter path: "<<((pythonCode.empty() || pythonPipelineName.empty()) ? "no" : "yes")
              <<" has compiled fallback path: "
              <<(tstage->slowPathBitCode().empty() ? "no" : "yes");
            logger().info(ss.str());
        }

        // start resolution, therefore reset unresolvedRowsCount
        _codePathStats.unresolvedRowsCount = 0;
        _codePathStats.rowsOnGeneralPathCount = 0;
        _codePathStats.rowsOnInterpreterPathCount = 0;

        // reset for all envs the exception counts, they will be newly counted in exception resolution.
        for(unsigned i = 0; i < _numThreads; ++i) {
            _threadEnvs[i].exceptionCounts.clear();
        }

        Timer resolveTimer;
        for(unsigned i = 0; i < _numThreads; ++i) {
            resolveOutOfOrder(i, tstage, syms); // note: this func is NOT thread-safe yet!!!
        }
        logger().info("Exception resolution/slow path done. Took " + std::to_string(resolveTimer.time()) + "s");
        markTime("general_and_interpreter_time", resolveTimer.time());
        auto row_stats = get_row_stats(tstage);

        auto numNormalRows = std::get<0>(row_stats);
        auto numExceptionRows = std::get<1>(row_stats);
        auto numHashRows = std::get<2>(row_stats);
        auto normalBufSize = std::get<3>(row_stats);
        auto exceptionBufSize = std::get<4>(row_stats);
        auto hashMapSize = std::get<5>(row_stats);
        auto task_duration = timer.time();
        std::stringstream ss;
        ss<<"in "<<task_duration<<"s "<<sizeToMemString(normalBufSize)
          <<" ("<<pluralize(numNormalRows, "normal row")<<") "
          <<sizeToMemString(exceptionBufSize)<<" ("<<pluralize(numExceptionRows, "exception row")<<")  "
          <<sizeToMemString(hashMapSize)<<" ("<<pluralize(numHashRows, "hash row")<<")";

        logger().info("Data processed " + ss.str());
        ss.clear();

        rc = writeAllPartsToOutput(output_uri, tstage->outputFormat(), tstage->outputOptions());
        if(rc != WORKER_OK)
            return rc;

        // release stage
        rc = releaseTransformStage(syms);
        if(rc != WORKER_OK)
            return rc;

        MessageStatistic stat;
        stat.totalTime = timer.time();
        stat.numNormalOutputRows = numNormalRows;
        stat.numExceptionOutputRows = numExceptionRows;
        stat.codePathStats = _codePathStats;
        _statistics.push_back(stat);

        logger().info("Took " + std::to_string(timer.time()) + "s in total");
        logger().info("Paths rows took: normal: " + std::to_string(_codePathStats.rowsOnNormalPathCount)
        + " general: " + std::to_string(_codePathStats.rowsOnGeneralPathCount)
        + " interpreter: " + std::to_string(_codePathStats.rowsOnInterpreterPathCount)
        + " unresolved: " + std::to_string(_codePathStats.unresolvedRowsCount));
        return WORKER_OK;
    }

    int64_t WorkerApp::writeAllPartsToOutput(const URI& output_uri, const FileFormat& output_format, const std::unordered_map<std::string, std::string>& output_options) {

        std::stringstream ss;
        std::vector<WriteInfo> reorganized_normal_parts;
        // @TODO: same for exceptions... => need to correct numbers??
        //  @Ben Givertz will know how to do this...
        std::vector<SpillInfo> exceptionFiles;

        for(unsigned i = 0; i < _numThreads; ++i) {
            auto& env = _threadEnvs[i];

            // save only when incremental is active...
            //            if (env.exceptionBuf.size() > 0) {
            //                spillExceptionBuffer(i);
            //            }

            // first come all the spill parts, then the remaining buffer...
            // add write info...
            // !!! stable sort necessary after this !!!
            WriteInfo info;
            for(const auto& spill_info : env.spillFiles) {
                if(!spill_info.isExceptionBuf) {
                    info.partNo = info.partNo;
                    info.use_buf = false;
                    info.threadNo = i;
                    info.spill_info = spill_info;
                    info.num_rows = info.spill_info.num_rows;
                    reorganized_normal_parts.push_back(info);
                } else {
                    // @TODO...
                    // --> these should be saved somewhere separate?
                    exceptionFiles.push_back(spill_info);
                }
            }

            // now add buffer (if > 0)
            if(env.normalBuf.size() > 0) {
                info.partNo = env.normalOriginalPartNo;
                info.buf = static_cast<uint8_t*>(env.normalBuf.buffer());
                info.buf_size = env.normalBuf.size();
                info.num_rows = env.numNormalRows;
                info.use_buf = true;
                info.threadNo = i;
                reorganized_normal_parts.push_back(info);
            }
        }

        // sort parts (!!! STABLE SORT !!!)
        std::stable_sort(reorganized_normal_parts.begin(),
                         reorganized_normal_parts.end(), [](const WriteInfo& a, const WriteInfo& b) {
                    // bufs come last!
                    return a.use_buf < b.use_buf && a.partNo < b.partNo;
                });

        // debug: print out parts
#ifndef NDEBUG
        ss.str("");
        for(auto info : reorganized_normal_parts) {
            ss<<"Part ("<<info.partNo<<") produced by thread "<<info.threadNo;
            if(info.use_buf)
                ss<<" (main memory, "<<info.buf_size<<" bytes, "<<pluralize(info.num_rows, "row")<<")\n";
            else
                ss<<" (spilled to "<<info.spill_info.path<<")\n";
        }
        auto res_msg = "Result overview: \n" + ss.str();
        trim(res_msg);
        logger().debug(res_msg);
#endif

        // no write everything to final output_uri out in order!
        logger().info("Writing data to " + output_uri.toString());
        writePartsToFile(output_uri, output_format, reorganized_normal_parts, output_options);
        logger().info("Data fully materialized");

        return WORKER_OK;
    }


    void WorkerApp::writePartsToFile(const URI &outputURI,
                                     const FileFormat &fmt,
                                     const std::vector<WriteInfo> &parts,
                                     const std::unordered_map<std::string, std::string>& output_options) {
        logger().info("file output initiated...");

        // need to potentially accumulate some info!
        // csv has no amount of rows, ORC doesn't care either as well as Text
        size_t totalRows = 0;
        size_t totalBytes = 0;
        for(auto part_info : parts) {
            if(part_info.use_buf) {
                totalRows += part_info.num_rows;
                totalBytes += part_info.buf_size;
            } else {
                totalRows += part_info.spill_info.num_rows;
                totalBytes += part_info.spill_info.file_size;
            }
        }

        logger().info("Writing output from " + pluralize(parts.size(), "part")
        + " (" + sizeToMemString(totalBytes) + ", " + pluralize(totalRows, "row") + ")");


        // open file
        auto vfs = VirtualFileSystem::fromURI(outputURI);
        auto mode = VirtualFileMode::VFS_OVERWRITE | VirtualFileMode::VFS_WRITE;
        if(fmt == FileFormat::OUTFMT_CSV || fmt == FileFormat::OUTFMT_TEXT)
            mode |= VirtualFileMode::VFS_TEXTMODE;

        logger().info("Merging output parts together into " + outputURI.toString());

        auto file = tuplex::VirtualFileSystem::open_file(outputURI, mode);
        if(!file)
            throw std::runtime_error("could not open " + outputURI.toPath() + " to write output");

        // & write header
        if(FileFormat::OUTFMT_TUPLEX == fmt) {
            //  1. numbytes 2. numrows
            int64_t tmp = totalBytes;
            file->write(&tmp, sizeof(int64_t));
            tmp = totalRows;
            file->write(&tmp, sizeof(int64_t));
        } else if(FileFormat::OUTFMT_CSV == fmt) {
            // header if desired...
            // create CSV header if desired
            uint8_t *header = nullptr;
            size_t header_length = 0;

            // write header if desired...
            bool writeHeader = stringToBool(get_or(output_options, "header", "false"));
            if(writeHeader) {
                // fetch special var csvHeader
                auto headerLine = output_options.at("csvHeader");
                header_length = headerLine.length();
                header = new uint8_t[header_length+1];
                memset(header, 0, header_length + 1 );
                memcpy(header, (uint8_t *)headerLine.c_str(), header_length);

                file->write(header, header_length);
                delete [] header; // delete temp buffer.

                std::cout<<"wrote header to: "<<outputURI.toString()<<std::endl;
            }
        }

        // use multi-threading for S3!!! --> that makes faster upload! I.e., multi-part-upload.
        for(const auto& part_info : parts) {
            // is it buf or file?
            if(part_info.use_buf) {
                // write the buffer...
                // -> directly to file!
                assert(part_info.buf);

                std::cout<<"wrote buf of size "<<part_info.buf_size<<" to "<<outputURI.toString()<<std::endl;
                if(part_info.buf)
                    file->write(part_info.buf, part_info.buf_size);
                else
                    std::cerr<<"invalid buffer stored?"<<std::endl;
            } else {
                // read & copy back the file contents of the spill file!
                logger().debug("opening spilled part file");
                auto part_file = VirtualFileSystem::open_file(part_info.spill_info.path, VirtualFileMode::VFS_READ);
                if(!part_file) {
                    auto err_msg = "part file could not be found under " + part_info.spill_info.path + ", output corrupted.";
                    logger().error(err_msg);
                    throw std::runtime_error(err_msg);
                }

                // read contents in from spill file...
                if(part_file->size() != part_info.spill_info.file_size) {
                    logger().warn("part_file: " + std::to_string(part_file->size()) + " part_info: " + std::to_string(part_info.spill_info.file_size));
                }
                // assert(part_file->size() == part_info.spill_info.file_size);
                assert(part_file->size() >= 2 * sizeof(int64_t));

                part_file->seek(2 * sizeof(int64_t)); // skip first two bytes representing bytes/rows
                size_t part_buffer_size = part_info.spill_info.file_size - 2 * sizeof(int64_t);
                uint8_t* part_buffer = new uint8_t[part_buffer_size];
                size_t bytes_read = 0;
                part_file->readOnly(part_buffer, part_buffer_size, &bytes_read);
                logger().debug("read from parts file " + sizeToMemString(bytes_read));

                part_file->close();

                std::cout<<"write (part) buf of size "<<part_buffer_size<<" to "<<outputURI.toString()<<std::endl;

                // copy part buffer to output file!
                file->write(part_buffer, part_buffer_size);
                delete [] part_buffer;
                logger().debug("copied contents from part back to output buffer");
                logger().debug("TODO: delete file? Add to job cleanup queue?");
            }
        }

        file->close();
        _output_uris.push_back(outputURI.toString());
        logger().info("file output done.");
    }


    int64_t WorkerApp::pythonCellFunctor(void *userData, int64_t row_number, char **cells, int64_t *cell_sizes) {
        assert(userData);
        auto ctx = static_cast<WorkerApp::PythonExecutionContext*>(userData);
        if(row_number < 10)
		Logger::instance().defaultLogger().info("processing row " + std::to_string(row_number));
        if(row_number > 0 && row_number % 100000 == 0)
            Logger::instance().defaultLogger().info("processed 100k rows...");

        return ctx->app->processCellsInPython(ctx->threadNo,
                                              ctx->pipelineObject,
                                              ctx->py_intermediates,row_number,
                                              ctx->numInputColumns,
                                              cells,
                                              cell_sizes);
    }


    int64_t
    WorkerApp::processCellsInPython(int threadNo, PyObject *pipelineObject,
                                    const std::vector<PyObject*>& py_intermediates,
                                    int64_t row_number, size_t num_cells,
                                    char **cells, int64_t *cell_sizes) {

        assert(pipelineObject);
        assert(num_cells > 0 && cells);
        PyObject* tuple = PyTuple_New(num_cells);
        for(unsigned i = 0; i < num_cells; ++i) {
            // zero terminated? if not, need to copy
            PyObject* str_obj = nullptr;
            assert(cells[i]);
            auto cell = cells[i];
            auto cell_size = cell_sizes[i];
            if(cell_size > 0 && cell[cell_size - 1] == '\0') {
                str_obj = python::PyString_FromString(cell);
            } else {
                // need to memcpy
                auto buffer = new char[cell_size + 1];
                buffer[cell_size] = '\0';
                memcpy(buffer, cell, cell_size);
                str_obj = python::PyString_FromString(buffer);
                delete [] buffer;
            }

            PyTuple_SET_ITEM(tuple, i, str_obj);
        }

        // call pipFunctor
        PyObject* args = PyTuple_New(1 + py_intermediates.size());
        PyTuple_SET_ITEM(args, 0, tuple);
        for(unsigned i = 0; i < py_intermediates.size(); ++i) {
            Py_XINCREF(py_intermediates[i]);
            PyTuple_SET_ITEM(args, i + 1, py_intermediates[i]);
        }

        auto kwargs = PyDict_New(); PyDict_SetItemString(kwargs, "parse_cells", Py_True);
        auto pcr = python::callFunctionEx(pipelineObject, args, kwargs);

        auto err_stream = &std::cerr;
        int64_t ecOperatorID = 0;
        int64_t ecCode = 0;
        std::vector<PyObject*> resultObjects;
        bool returnAllAsPyObjects = false;
        bool outputAsNormalRow = false;
        bool outputAsGeneralRow = false;

        if(pcr.exceptionCode != ExceptionCode::SUCCESS) {
            // this should not happen, bad internal error. codegen'ed python should capture everything.
            if(err_stream)
                *err_stream<<"bad internal python error: " + pcr.exceptionMessage<<std::endl;
        } else {
            // all good, row is fine. exception occured?
            assert(pcr.res);

            // type check: save to regular rows OR save to python row collection
            if(!pcr.res) {
                if(err_stream)
                    *err_stream<<"bad internal python error, NULL object returned"<<std::endl;
            } else {

#ifndef NDEBUG
                // // uncomment to print res obj
                // Py_XINCREF(pcr.res);
                // PyObject_Print(pcr.res, stdout, 0);
                // std::cout<<std::endl;
#endif
                auto exceptionObject = PyDict_GetItemString(pcr.res, "exception");
                if(exceptionObject) {

                    // overwrite operatorID which is throwing.
                    auto exceptionOperatorID = PyDict_GetItemString(pcr.res, "exceptionOperatorID");
                    ecOperatorID = PyLong_AsLong(exceptionOperatorID);
                    auto exceptionType = PyObject_Type(exceptionObject);
                    // can ignore input row.
                    ecCode = ecToI64(python::translatePythonExceptionType(exceptionType));
                } else {
                    // normal, check type and either merge to normal set back OR onto python set together with row number?
                    auto resultRows = PyDict_GetItemString(pcr.res, "outputRows");
                    assert(PyList_Check(resultRows));
                    for(int i = 0; i < PyList_Size(resultRows); ++i) {
                        // type check w. output schema
                        // cf. https://pythonextensionpatterns.readthedocs.io/en/latest/refcount.html
                        auto rowObj = PyList_GetItem(resultRows, i);
                        Py_XINCREF(rowObj);

                        // returnAllAsPyObjects makes especially sense when hashtable is used!
                        if(returnAllAsPyObjects) {
                            // res.pyObjects.push_back(rowObj);
                            continue;
                        }

                        auto rowType = python::mapPythonClassToTuplexType(rowObj);

                        // special case output schema is str (fileoutput!)
                        if(rowType == python::Type::STRING) {
                            // write to file, no further type check necessary b.c.
                            // if it was the object string it would be within a tuple!
                            auto cptr = PyUnicode_AsUTF8(rowObj);
                            Py_XDECREF(rowObj);

                            auto size = strlen(cptr);

                            // this i.e. the output of tocsv
                            // note that because it's a zero-terminated string, do not write everything!
                            int64_t rc = 0;
                            if((rc = writeRow(threadNo, reinterpret_cast<const uint8_t *>(cptr), size)) != ecToI64(ExceptionCode::SUCCESS))
                                return rc;

//                            res.buf.provideSpace(size);
//                            memcpy(res.buf.ptr(), reinterpret_cast<const uint8_t *>(cptr), size);
//                            res.buf.movePtr(size);
//                            res.bufRowCount++;
                            //mergeRow(reinterpret_cast<const uint8_t *>(cptr), strlen(cptr), BUF_FORMAT_NORMAL_OUTPUT); // don't write '\0'!
                        } else {
                            throw std::runtime_error("not yet supported in pure python mode");
                            // there are three options where to store the result now
//                            // 1. fits targetOutputSchema (i.e. row becomes normalcase row)
//                            bool outputAsNormalRow = python::Type::UNKNOWN != unifyTypes(rowType, specialized_target_schema.getRowType(), allowNumericTypeUnification)
//                                                     && canUpcastToRowType(rowType, specialized_target_schema.getRowType());
//                            // 2. fits generalCaseOutputSchema (i.e. row becomes generalcase row)
//                            bool outputAsGeneralRow = python::Type::UNKNOWN != unifyTypes(rowType,
//                                                                                          general_target_schema.getRowType(), allowNumericTypeUnification)
//                                                      && canUpcastToRowType(rowType, general_target_schema.getRowType());

                            // 3. doesn't fit, store as python object. => we should use block storage for this as well. Then data can be shared.

                            // can upcast? => note that the && is necessary because of cases where outputSchema is
                            // i64 but the given row type f64. We can cast up i64 to f64 but not the other way round.
                            if(outputAsNormalRow) {
                                // Row resRow = python::pythonToRow(rowObj).upcastedRow(specialized_target_schema.getRowType());
                                // assert(resRow.getRowType() == specialized_target_schema.getRowType());
                                // auto serialized_length = resRow.serializedLength();
                                // res.buf.provideSpace(serialized_length);
                                // auto actual_length = resRow.serializeToMemory(static_cast<uint8_t *>(res.buf.ptr()), res.buf.capacity() - res.buf.size());
                                // assert(serialized_length == actual_length);
                                // res.buf.movePtr(serialized_length);
                                //  res.bufRowCount++;
                            } else if(outputAsGeneralRow) {
                                // Row resRow = python::pythonToRow(rowObj).upcastedRow(general_target_schema.getRowType());
                                // assert(resRow.getRowType() == general_target_schema.getRowType());

                                throw std::runtime_error("not yet supported");

//                                // write to buffer & perform callback
//                                auto buf_size = 2 * resRow.serializedLength();
//                                uint8_t *buf = new uint8_t[buf_size];
//                                memset(buf, 0, buf_size);
//                                auto serialized_length = resRow.serializeToMemory(buf, buf_size);
//                                // call row func!
//                                // --> merge row distinguishes between those two cases. Distinction has to be done there
//                                //     because of compiled functor who calls mergeRow in the write function...
//                                mergeRow(buf, serialized_length, BUF_FORMAT_GENERAL_OUTPUT);
//                                delete [] buf;
                            } else {
                                // res.pyObjects.push_back(rowObj);
                            }
                            // Py_XDECREF(rowObj);
                        }
                    }

#ifndef NDEBUG
                    if(PyErr_Occurred()) {
                        // print out the otber objects...
                        std::cout<<__FILE__<<":"<<__LINE__<<" python error not cleared properly!"<<std::endl;
                        PyErr_Print();
                        std::cout<<std::endl;
                        PyErr_Clear();
                    }
#endif
                    // // everything was successful, change resCode to 0!
                    // res.code = ecToI64(ExceptionCode::SUCCESS);
                }
            }
        }

        return 0;
    }

    int64_t WorkerApp::processSourceInPython(int threadNo, int64_t inputNodeID, const FilePart& part,
                                             const TransformStage* tstage, PyObject* pipelineObject,
                                             bool acquireGIL,
                                             size_t* inputRowCount) {
        using namespace std;
        assert(tstage);
        assert(pipelineObject);

        // reset counter
        if(inputRowCount)*inputRowCount = 0;

        if(acquireGIL)
            python::lockGIL();

        size_t rangeStart=0, rangeEnd=0;
        auto inputURI = part.uri;
        decodeRangeURI(part.uri.toString(), inputURI, rangeStart, rangeEnd);

        auto normalCaseEnabled = false;

        // input reader
        std::unique_ptr<FileInputReader> reader;
        ThreadEnv* env = &_threadEnvs[threadNo];

        // spill files if they do not add up!
        if(env->normalOriginalPartNo != part.partNo) {
            if(env->normalBuf.size() > 0)
                spillNormalBuffer(threadNo);
            env->normalOriginalPartNo = part.partNo;
        }

        // do the same for the exception buffer...
        if(env->exceptionOriginalPartNo != part.partNo) {
            if(env->exceptionBuf.size() > 0)
                spillExceptionBuffer(threadNo);
            env->exceptionOriginalPartNo = part.partNo;
        }
        // same for hash buf as well
        if(env->hashOriginalPartNo != part.partNo) {
            if(hashmap_bucket_count((map_t)env->hashMap) > 0)
                spillHashMap(threadNo);
            env->hashOriginalPartNo = part.partNo;
        }

        // use try/catch b.c. of GIL
        try {

            PythonExecutionContext ctx;
            ctx.app = this;
            ctx.threadNo = 0;
            ctx.pipelineObject = pipelineObject;
            void* userData = reinterpret_cast<void*>(&ctx);

            logger().info("Starting python processing...");

            switch(tstage->inputMode()) {
                case EndPointMode::FILE: {

                    // there should be a couple input uris in this request!
                    // => decode using optional fileinput params from the
                    // @TODO: ranges

                    // only csv + text so far supported!
                    if(tstage->inputFormat() == FileFormat::OUTFMT_CSV) {

                        // decode from file input params everything
                        auto numColumns = tstage->csvNumFileInputColumns();
                        vector<std::string> header;
                        if(tstage->csvHasHeader())
                            header = tstage->csvHeader();
                        auto delimiter = tstage->csvInputDelimiter();
                        auto quotechar = tstage->csvInputQuotechar();
                        auto colsToKeep = indicesToBoolArray(tstage->generalCaseInputColumnsToKeep(), tstage->inputColumnCount());

                        ctx.numInputColumns = numColumns;

                        auto csv = new CSVReader(userData, reinterpret_cast<codegen::cells_row_f>(pythonCellFunctor),
                                                 normalCaseEnabled,
                                                 inputNodeID,
                                                 reinterpret_cast<codegen::exception_handler_f>(exceptRowCallback),
                                                 numColumns, delimiter,
                                                 quotechar, colsToKeep);
                        // fetch full file for now, later make this optional!
                        // csv->setRange(rangeStart, rangeStart + rangeSize);
                        csv->setHeader(header);
                        csv->setRange(part.rangeStart, part.rangeEnd);
                        reader.reset(csv);
                    } else if(tstage->inputFormat() == FileFormat::OUTFMT_TEXT) {
                        ctx.numInputColumns = 1;
                        auto text = new TextReader(userData,
                                                   reinterpret_cast<codegen::cells_row_f>(pythonCellFunctor),
                                                   inputNodeID,
                                                   reinterpret_cast<codegen::exception_handler_f>(exceptRowCallback));
                        // fetch full range for now, later make this optional!
                        // text->setRange(rangeStart, rangeStart + rangeSize);
                        text->setRange(part.rangeStart, part.rangeEnd);
                        reader.reset(text);
                    } else if(tstage->inputFormat() == FileFormat::OUTFMT_JSON) {
                        auto json = new JsonReader(userData,
                                                   reinterpret_cast<codegen::read_block_f>(pythonCellFunctor),
                                                   _readerBufferSize);
                        json->setRange(part.rangeStart, part.rangeEnd);
                        reader.reset(json);
                    } else throw std::runtime_error("unsupported input file format given");

                    // Note: ORC reader does not support parts yet... I.e., function needs to read FULL file!

                    // read assigned file...
                    logger().info("Calling read func on reader...");
                    reader->read(inputURI);
                    if(inputRowCount)
                        *inputRowCount += reader->inputRowCount();
                    runtime::rtfree_all();
                    break;
                }
//                case EndPointMode::MEMORY: {
//                    // not supported yet
//                    // => simply read in partition from file (tuplex in memory format)
//                    // load file into partition, then call functor on the partition.
//
//                    // TODO: parts? -> for tuplex reader maybe also important!
//                    // Tuplex reader doesn't support chunking yet either...
//
//
//                    // TODO: Could optimize this by streaming in data & calling compute over blocks of data!
//                    auto vf = VirtualFileSystem::open_file(inputURI, VirtualFileMode::VFS_READ);
//                    if(vf) {
//                        auto file_size = vf->size();
//                        size_t bytes_read = 0;
//                        auto input_buffer = new uint8_t[file_size];
//                        vf->read(input_buffer, file_size, &bytes_read);
//                        logger().info("Read " + std::to_string(bytes_read) + " bytes from " + inputURI.toString());
//
//                        int64_t normal_row_output_count = 0;
//                        int64_t bad_row_output_count = 0;
//                        auto response_code = syms->functor(userData, input_buffer, bytes_read, &normal_row_output_count, &bad_row_output_count, false);
//                        {
//                            std::stringstream ss;
//                            ss << "RC=" << response_code << " ,computed " << normal_row_output_count << " normal rows, "
//                               << bad_row_output_count << " bad rows" << endl;
//                            logger().debug(ss.str());
//                        }
//
//                        delete [] input_buffer;
//                        vf->close();
//                    } else {
//                        logger().error("Error reading " + inputURI.toString());
//                        if(acquireGIL)
//                            python::unlockGIL();
//                        return WORKER_ERROR_IO;
//                    }
//                    break;
//                }
                default: {
                    logger().error("unsupported input mode found");
                    if(acquireGIL)
                        python::unlockGIL();
                    return WORKER_ERROR_UNSUPPORTED_INPUT;
                    break;
                }
            }

        } catch(const std::exception& e) {
            logger().error("Exception occurred while processing part " + encodeRangeURI(part.uri, part.rangeStart, part.rangeEnd) + ": " + e.what());
        } catch(...) {
            logger().error("Unknown exception occurred while processing part " + encodeRangeURI(part.uri, part.rangeStart, part.rangeEnd));
        }

        if(acquireGIL)
            python::unlockGIL();

        return WORKER_OK;
    }


    int64_t WorkerApp::processSource(int threadNo, int64_t inputNodeID, const FilePart& part, const TransformStage *tstage,
                                     const std::shared_ptr<TransformStage::JITSymbols>& syms, size_t* inputRowCount) {
        using namespace std;

        // couple checks
        assert(tstage);
        assert(syms->functor);
        assert(threadNo >= 0 && threadNo < _numThreads);

        // perform check
        if(!tstage || !syms->functor || !(threadNo >= 0 && threadNo < _numThreads)) {
            std::stringstream err_stream;
            err_stream<<"tstage: "<<tstage<<" functor: "<<syms->functor<<" _numThreads: "<<_numThreads<<" threadNo: "<<threadNo;
            logger().error(err_stream.str());
            return WORKER_ERROR_UNSUPPORTED_INPUT;
        }

        // reset optional output counter
        if(inputRowCount)
            *inputRowCount = 0;

        size_t rangeStart=0, rangeEnd=0;
        auto inputURI = part.uri;
        decodeRangeURI(part.uri.toString(), inputURI, rangeStart, rangeEnd);

        // input reader
        std::unique_ptr<FileInputReader> reader;
        bool normalCaseEnabled = false;
        void* userData = reinterpret_cast<void*>(&_threadEnvs[threadNo]);
        ThreadEnv* env = &_threadEnvs[threadNo];

        // spill files if they do not add up!
        if(env->normalOriginalPartNo != part.partNo) {
            if(env->normalBuf.size() > 0)
                spillNormalBuffer(threadNo);
            env->normalOriginalPartNo = part.partNo;
        }

        // do the same for the exception buffer...
        if(env->exceptionOriginalPartNo != part.partNo) {
            if(env->exceptionBuf.size() > 0)
                spillExceptionBuffer(threadNo);
            env->exceptionOriginalPartNo = part.partNo;
        }
        // same for hash buf as well
        if(env->hashOriginalPartNo != part.partNo) {
            if(hashmap_bucket_count((map_t)env->hashMap) > 0)
                spillHashMap(threadNo);
            env->hashOriginalPartNo = part.partNo;
        }

        switch(tstage->inputMode()) {
            case EndPointMode::FILE: {

                // there should be a couple input uris in this request!
                // => decode using optional fileinput params from the
                // @TODO: ranges

                // only csv + text so far supported!
                if(tstage->inputFormat() == FileFormat::OUTFMT_CSV) {

                    // decode from file input params everything
                    auto numColumns = tstage->csvNumFileInputColumns();
                    vector<std::string> header;
                    if(tstage->csvHasHeader())
                        header = tstage->csvHeader();
                    auto delimiter = tstage->csvInputDelimiter();
                    auto quotechar = tstage->csvInputQuotechar();
                    auto colsToKeep = indicesToBoolArray(tstage->generalCaseInputColumnsToKeep(), tstage->inputColumnCount()); //tstage->columnsToKeep();
                    auto cell_functor = reinterpret_cast<codegen::cells_row_f>(syms->functor);

                    // for errors, use this to fix...
                    // cell_functor = dummy_cell_functor;

                    auto csv = new CSVReader(userData, cell_functor,
                                             normalCaseEnabled,
                                             inputNodeID,
                                             reinterpret_cast<codegen::exception_handler_f>(exceptRowCallback),
                                             numColumns, delimiter,
                                             quotechar, colsToKeep);
                    // fetch full file for now, later make this optional!
                    // csv->setRange(rangeStart, rangeStart + rangeSize);
                    csv->setHeader(header);
                    csv->setRange(part.rangeStart, part.rangeEnd);
                    reader.reset(csv);
                } else if(tstage->inputFormat() == FileFormat::OUTFMT_TEXT) {
                    auto text = new TextReader(userData,
                                               reinterpret_cast<codegen::cells_row_f>(syms->functor),
                                               inputNodeID,
                                               reinterpret_cast<codegen::exception_handler_f>(exceptRowCallback));
                    // fetch full range for now, later make this optional!
                    // text->setRange(rangeStart, rangeStart + rangeSize);
                    text->setRange(part.rangeStart, part.rangeEnd);
                    reader.reset(text);
                } else if(tstage->inputFormat() == FileFormat::OUTFMT_JSON) {
                    auto json = new JsonReader(userData,
                                               reinterpret_cast<codegen::read_block_f>(syms->functor),
                                               _readerBufferSize);
                    json->setRange(part.rangeStart, part.rangeEnd);
                    reader.reset(json);
                } else throw std::runtime_error("unsupported input file format given");

                // Note: ORC reader does not support parts yet... I.e., function needs to read FULL file!

                // read assigned file...
                if(reader)
                    reader->read(inputURI);

                // fetch row count
                if(inputRowCount)
                    *inputRowCount = reader->inputRowCount();

                // more detailed stats

                runtime::rtfree_all();
                break;
            }
            case EndPointMode::MEMORY: {
                // not supported yet
                // => simply read in partition from file (tuplex in memory format)
                // load file into partition, then call functor on the partition.

                // TODO: parts? -> for tuplex reader maybe also important!
                // Tuplex reader doesn't support chunking yet either...


                // TODO: Could optimize this by streaming in data & calling compute over blocks of data!
                auto vf = VirtualFileSystem::open_file(inputURI, VirtualFileMode::VFS_READ);
                if(vf) {
                    auto file_size = vf->size();
                    size_t bytes_read = 0;
                    auto input_buffer = new uint8_t[file_size];
                    vf->read(input_buffer, file_size, &bytes_read);
                    logger().info("Read " + std::to_string(bytes_read) + " bytes from " + inputURI.toString());

                    // tuplex file format has number of rows first?
                    int64_t num_rows = *(int64_t*)input_buffer;
                    if(inputRowCount)
                        *inputRowCount = num_rows;

                    assert(syms->functor);
                    int64_t normal_row_output_count = 0;
                    int64_t bad_row_output_count = 0;
                    auto response_code = syms->functor(userData, input_buffer, bytes_read, &normal_row_output_count, &bad_row_output_count, false);
                    {
                        std::stringstream ss;
                        ss << "RC=" << response_code << " ,computed " << normal_row_output_count << " normal rows, "
                             << bad_row_output_count << " bad rows" << endl;
                        logger().debug(ss.str());
                    }

                    delete [] input_buffer;
                    vf->close();
                } else {
                    logger().error("Error reading " + inputURI.toString());
                    return WORKER_ERROR_IO;
                }
                break;
            }
            default: {
                logger().error("unsupported input mode found");
                return WORKER_ERROR_UNSUPPORTED_INPUT;
                break;
            }
        }

        // print out some stats (??)
#ifndef NDEBUG
        std::cout<<"finished processing normal case: "<<"normal: "<<env->numNormalRows<<" exception: "<<env->numExceptionRows<<std::endl;
        std::cout<<"exception details:"<<std::endl;
        for(auto kv : env->exceptionCounts) {
            std::cout<<" -- "<<std::get<0>(kv.first)<<" "<<std::get<1>(kv.first)<<"   "<<kv.second<<std::endl;
        }
#endif
        // when processing is done, simply output everything to URI (should be an S3 one!)
        return WORKER_OK;
    }

    void WorkerApp::writeBufferToFile(const URI& outputURI,
                                      const FileFormat& fmt,
                                      const uint8_t* buf,
                                      const size_t buf_size,
                                      const size_t num_rows,
                                      const std::unordered_map<std::string, std::string>& output_options) {

        logger().info("writing " + sizeToMemString(buf_size) + " to " + outputURI.toPath());
        // write first the # rows, then the data
        auto vfs = VirtualFileSystem::fromURI(outputURI);
        auto mode = VirtualFileMode::VFS_OVERWRITE | VirtualFileMode::VFS_WRITE;
        if(fmt == FileFormat::OUTFMT_CSV || fmt == FileFormat::OUTFMT_TEXT)
            mode |= VirtualFileMode::VFS_TEXTMODE;
        auto file = tuplex::VirtualFileSystem::open_file(outputURI, mode);
        if(!file)
            throw std::runtime_error("could not open " + outputURI.toPath() + " to write output");


        // open file & write
        if(FileFormat::OUTFMT_CSV == fmt) {
            // header?
            // create CSV header if desired
            uint8_t *header = nullptr;
            size_t header_length = 0;

            // write header if desired...
            bool writeHeader = stringToBool(get_or(output_options, "header", "false"));
            if(writeHeader) {
                // fetch special var csvHeader
                auto headerLine = output_options.at("csvHeader");
                header_length = headerLine.length();
                header = new uint8_t[header_length + 1];
                memset(header, 0, header_length + 1 );
                memcpy(header, (uint8_t *)headerLine.c_str(), header_length);
                delete [] header;
            }
        }

        // write header for Tuplex fileformat (i.e. header!)
        else if(FileFormat::OUTFMT_TUPLEX == fmt) {
            // header of tuplex fmt is simply bytesWritten + numoutputrows
            assert(sizeof(int64_t) == sizeof(size_t));
            int64_t tmp = buf_size;
            file->write(&tmp, sizeof(int64_t));
            tmp = num_rows;
            file->write(&tmp, sizeof(int64_t));
        }
        else {
            // TODO: ORC file format??

            file->close();
            throw std::runtime_error("unknown file format " + std::to_string(static_cast<int64_t>(fmt)) + " found.");
        }

        // write buffer & close file
        file->write(buf, buf_size);
        file->close();
    }

    WorkerSettings WorkerApp::settingsFromMessage(const tuplex::messages::InvocationRequest& req) {
        WorkerSettings ws;

        // decode from message if certain settings are present
        if(req.settings().has_numthreads())
            ws.numThreads = std::max(1u, req.settings().numthreads());
        if(req.settings().has_normalbuffersize())
            ws.normalBufferSize = req.settings().normalbuffersize();
        if(req.settings().has_exceptionbuffersize())
            ws.exceptionBufferSize = req.settings().exceptionbuffersize();
        if(req.settings().has_spillrooturi())
            ws.spillRootURI = req.settings().spillrooturi();
        if(req.settings().has_runtimememoryperthread())
            ws.runTimeMemory = req.settings().runtimememoryperthread();
        if(req.settings().has_runtimememoryperthreadblocksize())
            ws.runTimeMemoryDefaultBlockSize = req.settings().runtimememoryperthreadblocksize();
        if(req.settings().has_allownumerictypeunification())
            ws.allowNumericTypeUnification = req.settings().allownumerictypeunification();
        if(req.settings().has_useinterpreteronly())
            ws.useInterpreterOnly = req.settings().useinterpreteronly();
        if(req.settings().has_usecompiledgeneralcase())
            ws.useCompiledGeneralPath = req.settings().usecompiledgeneralcase();
        if(req.settings().has_normalcasethreshold())
            ws.normalCaseThreshold = req.settings().normalcasethreshold();

        // set a couple settings from other field (map)
        // cf. AWSLambdaBackend::config_worker
        auto it = req.settings().other().find("tuplex.experimental.opportuneCompilation");
        if(it != req.settings().other().end()) {
            ws.opportuneGeneralPathCompilation = stringToBool(it->second);
        } else {
            ws.opportuneGeneralPathCompilation = false;
        }
        it = req.settings().other().find("tuplex.experimental.s3PreCacheSize");
        if(it != req.settings().other().end()) {
            ws.s3PreCacheSize = memStringToSize(it->second);
        } else {
            ws.s3PreCacheSize = 0; // 0 means deactivated
        }
        it = req.settings().other().find("tuplex.useLLVMOptimizer");
        if(it != req.settings().other().end()) {
            ws.useOptimizer = stringToBool(it->second);
        } else {
            ws.useOptimizer = true;
        }
        it = req.settings().other().find("tuplex.optimizer.filterPromotion");
        if(it != req.settings().other().end()) {
            ws.useFilterPromotion = stringToBool(it->second);
        } else {
            ws.useFilterPromotion = false;
        }
        it = req.settings().other().find("tuplex.sample.maxDetectionRows");
        if(it != req.settings().other().end()) {
            ws.sampleLimitCount = std::stoull(it->second);
        } else {
            ws.sampleLimitCount = std::numeric_limits<size_t>::max();
        }
        it = req.settings().other().find("tuplex.sample.strataSize");
        if(it != req.settings().other().end()) {
            ws.strataSize = std::stoull(it->second);
        } else {
            ws.strataSize = 1;
        }
        it = req.settings().other().find("tuplex.sample.samplesPerStrata");
        if(it != req.settings().other().end()) {
            ws.samplesPerStrata = std::stoull(it->second);
        } else {
            ws.samplesPerStrata = 1;
        }

        ws.strataSize = std::max(ws.strataSize, 1ul);
        ws.samplesPerStrata = std::min(ws.samplesPerStrata, ws.strataSize);

        it = req.settings().other().find("tuplex.optimizer.constantFoldingOptimization");
        if(it != req.settings().other().end()) {
            ws.useConstantFolding = stringToBool(it->second);
        } else {
            ws.useConstantFolding = false;
        }

        it = req.settings().other().find("tuplex.experimental.forceBadParseExceptFormat");
        if(it != req.settings().other().end()) {
            bool forceBadParse = stringToBool(it->second);
            if(forceBadParse)
                ws.exceptionSerializationMode = codegen::ExceptionSerializationMode::SERIALIZE_MISMATCH_ALWAYS_AS_BAD_PARSE;
            else
                ws.exceptionSerializationMode = codegen::ExceptionSerializationMode::SERIALIZE_AS_GENERAL_CASE;
        } else {
            ws.exceptionSerializationMode = codegen::ExceptionSerializationMode::SERIALIZE_AS_GENERAL_CASE;
        }


        ws.numThreads = std::max(1ul, ws.numThreads);
        return ws;
    }

    std::shared_ptr<TransformStage::JITSymbols> WorkerApp::compileTransformStage(TransformStage &stage, bool use_llvm_optimizer) {

        use_llvm_optimizer = _settings.useOptimizer;

        // 1. check fast code path
        auto bitCode = stage.fastPathBitCode() + stage.slowPathBitCode();

        if(bitCode.empty()) {
            logger().error("no bitcode found, empty stage?");
            return nullptr;
        }

        // disable cache, unfairly benefits no-hyper specialization case.
        // // use cache
        // auto it = _compileCache.find(bitCode);
        // if(it != _compileCache.end()) {
        //     logger().info("Using cached compiled code for fast/slow path");
        //     return it->second;
        // }

        try {
            // compile transform stage, depending on worker settings use optimizer before or not!
            // -> per default, don't.
            if(!_compiler)
                throw std::runtime_error("JITCompiler not initialized.");
            if(!_fastCompiler)
                throw std::runtime_error("fast compiler not initialized.");

            // register symbols for each compiler
            std::vector<JITCompiler*> compilers({_compiler.get(), _fastCompiler.get()});
            for(JITCompiler* compiler : compilers) {

                assert(compiler);

                // register functions
                // Note: only normal case for now, the other stuff is not interesting yet...
                if(!stage.writeMemoryCallbackName().empty())
                    compiler->registerSymbol(stage.writeMemoryCallbackName(), writeRowCallback);
                if(!stage.exceptionCallbackName().empty())
                    compiler->registerSymbol(stage.exceptionCallbackName(), exceptRowCallback);
                if(!stage.writeFileCallbackName().empty())
                    compiler->registerSymbol(stage.writeFileCallbackName(), writeRowCallback);
                if(!stage.writeHashCallbackName().empty())
                    compiler->registerSymbol(stage.writeHashCallbackName(), writeHashCallback);

                // slow path registration, for now dummies
                if(!stage.resolveWriteCallbackName().empty())
                    compiler->registerSymbol(stage.resolveWriteCallbackName(), slowPathRowCallback);
                if(!stage.resolveExceptionCallbackName().empty())
                    compiler->registerSymbol(stage.resolveExceptionCallbackName(), slowPathExceptCallback);
                // @TODO: hashing callbacks...
            }

            // in debug mode, validate module.
            llvm::LLVMContext ctx;
            auto mod = stage.fastPathBitCode().empty() ? nullptr : codegen::bitCodeToModule(ctx, stage.fastPathBitCode());
            if(!mod) {
                if(!stage.fastPathBitCode().empty()) {
                    logger().error("error parsing module for fast code path");
                }
                return nullptr;
            } else {
                logger().info("parsed llvm module from bitcode, " + mod->getName().str());

                // run verify pass on module and print out any errors, before attempting to compile it
                std::string moduleErrors;
                llvm::raw_string_ostream os(moduleErrors);
                if (verifyModule(*mod, &os)) {
                    os.flush();
                    logger().error("could not verify module from bitcode");
                    logger().error(moduleErrors);
                    logger().error(core::withLineNumbers(codegen::moduleToString(*mod)));
                } else {
    #ifndef NDEBUG
                    logger().info("module verified.");
                    // save
                    auto ir_code = codegen::moduleToString(*mod.get());
                    stringToFile("worker_fast_path.txt", ir_code);
    #endif
                }
            }

            // perform actual compilation
            // -> do not compile slow path for now.
            // -> do not register symbols, because that has been done manually above.

            LLVMOptimizer opt;
//            auto syms = stage.compile(*_compiler, use_llvm_optimizer ? &opt : nullptr,
//                                        true,
//                                        false);

            // for debugging enable tracing for the 2nd invocation!
            bool traceExecution = false;

            // uncomment to trace errors on 2nd invocation
            // if(numProcessedMessages() > 1 && _statistics.size() >= 1)
            //     traceExecution = true;
            // do not register symbols
            auto syms = stage.compileFastPath(*_compiler,
                                              use_llvm_optimizer ? &opt : nullptr,
                                              false, traceExecution);

            {
                // update internal symbols.
                std::lock_guard<std::mutex> lock(_symsMutex);
                _syms->update(syms);
            }

            // // cache symbols for reuse.
            // _compileCache[bitCode] = syms;

            return syms;
        } catch(std::runtime_error& e) {
            logger().error(std::string("compilation failed, details: ") + e.what());
            return nullptr;
        }

        // if there is no
    }

    int64_t WorkerApp::writeRow(size_t threadNo, const uint8_t *buf, int64_t bufSize) {

        assert(_threadEnvs);
        assert(threadNo < _numThreads);
        // check if enough space is available
        auto& out_buf = _threadEnvs[threadNo].normalBuf;
        if(out_buf.size() + bufSize <= out_buf.capacity()) {
            memcpy(out_buf.ptr(), buf, bufSize);
            out_buf.movePtr(bufSize);
            _threadEnvs[threadNo].numNormalRows++;
        } else {
            // check if bufSize exceeds limit, if so resize and call again!
            if(bufSize > out_buf.capacity()) {
                out_buf.provideSpace(bufSize);
                writeRow(threadNo, buf, bufSize); // call again, do not return yet. Instead, spill for sure after the resize...
                logger().debug("row size exceeded internal buffer size, forced resize.");
                // buffer is full, save to spill out path
                spillNormalBuffer(threadNo);
            } else {
                // buffer is full, save to spill out path
                spillNormalBuffer(threadNo);

                // now write the row
                writeRow(threadNo, buf, bufSize);
            }
        }

        return 0;
    }

    void WorkerApp::spillNormalBuffer(size_t threadNo) {

        assert(_threadEnvs);
        assert(threadNo < _numThreads);

        // spill (in Tuplex format!), i.e. write first #rows, #bytes written & the rest!

        // create file name (trivial:)
        auto name = "spill_normal_" + std::to_string(threadNo) + "_" + std::to_string(_threadEnvs[threadNo].spillFiles.size());
        std::string ext = ".tmp";
        auto rootURI = _settings.spillRootURI.toString().empty() ? "" : _settings.spillRootURI.toString() + "/";
        auto path = URI(rootURI + name + ext);

        // open & write
        auto vfs = VirtualFileSystem::fromURI(path);
        auto vf = vfs.open_file(path, VirtualFileMode::VFS_OVERWRITE);
        if(!vf) {
            auto err_msg = "Failed to spill buffer to path " + path.toString();
            logger().error(err_msg);
            throw std::runtime_error(err_msg);
        } else {
            int64_t tmp = _threadEnvs[threadNo].numNormalRows;
            vf->write(&tmp, sizeof(int64_t));
            tmp = _threadEnvs[threadNo].normalBuf.size();
            vf->write(&tmp, sizeof(int64_t));
            vf->write(_threadEnvs[threadNo].normalBuf.buffer(), _threadEnvs[threadNo].normalBuf.size());

            SpillInfo info;
            info.path = path.toString();
            info.isExceptionBuf = false;
            info.num_rows = _threadEnvs[threadNo].numNormalRows;
            info.originalPartNo = _threadEnvs[threadNo].normalOriginalPartNo;
            info.file_size =  _threadEnvs[threadNo].normalBuf.size() + 2 * sizeof(int64_t);
            _threadEnvs[threadNo].spillFiles.push_back(info);

            logger().info("Spilled " + sizeToMemString(info.file_size) + " to " + path.toString());
        }

        // reset
        _threadEnvs[threadNo].normalBuf.reset();
        _threadEnvs[threadNo].numNormalRows = 0;
        _threadEnvs[threadNo].normalOriginalPartNo = 0;
    }

    void WorkerApp::spillExceptionBuffer(size_t threadNo) {
        assert(_threadEnvs);
        assert(threadNo < _numThreads);

        // spill (in Tuplex format!), i.e. write first #rows, #bytes written & the rest!

        auto env = &_threadEnvs[threadNo];

        // no exceptions or empty buf? skip.
        if(0 == env->numExceptionRows || 0 == env->exceptionBuf.size()) {
            // reset
            env->exceptionBuf.reset();
            env->numExceptionRows = 0;
            env->exceptionOriginalPartNo = 0;
            return;
        }

        // create file name (trivial:)
        auto name = "spill_except_" + std::to_string(threadNo) + "_" + std::to_string(env->spillFiles.size());
        std::string ext = ".tmp";
        auto rootURI = _settings.spillRootURI.toString().empty() ? "" : _settings.spillRootURI.toString() + "/";
        auto path = URI(rootURI + name + ext);

        logger().info("Spilling " + std::to_string(env->exceptionBuf.size()) + "/" + std::to_string(env->exceptionBuf.capacity()) + " to " + path.toString());

        // open & write
        auto vfs = VirtualFileSystem::fromURI(path);
        auto vf = vfs.open_file(path, VirtualFileMode::VFS_OVERWRITE);
        if(!vf) {
            auto err_msg = "Failed to spill except buffer to path " + path.toString();
            logger().error(err_msg);
            throw std::runtime_error(err_msg);
        } else {
            int64_t tmp = env->numExceptionRows;
            vf->write(&tmp, sizeof(int64_t));
            tmp = env->exceptionBuf.size();
            vf->write(&tmp, sizeof(int64_t));
            vf->write(env->exceptionBuf.buffer(), env->exceptionBuf.size());

            SpillInfo info;
            info.path = path.toString();
            info.isExceptionBuf = true;
            info.num_rows = env->numExceptionRows;
            info.originalPartNo = env->exceptionOriginalPartNo;
            info.file_size =  env->exceptionBuf.size() + 2 * sizeof(int64_t);
            env->spillFiles.push_back(info);
            vf->close();
            logger().info("Spilled " + sizeToMemString(info.file_size) + " to " + path.toString());
        }

        // reset
        env->exceptionBuf.reset();
        env->numExceptionRows = 0;
        env->exceptionOriginalPartNo = 0;
    }

    void WorkerApp::spillHashMap(size_t threadNo) {
        throw std::runtime_error("spilling hashmap not yet supported");
    }

    void WorkerApp::writeException(size_t threadNo, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t *input,
                              int64_t dataLength) {
        // same here for exception buffers...
        assert(_threadEnvs);
        assert(threadNo < _numThreads);

        auto env = &_threadEnvs[threadNo];

        // check if enough space is available
        auto& out_buf = env->exceptionBuf;

#ifndef NDEBUG
        // use following code snippet to "debug" what kind of rows violate normal-case. Helpful for processing.
        // print row out when < 5
        size_t max_debug_rows_to_print = 5;
        if(env->numExceptionRows < max_debug_rows_to_print) {
            if(exceptionCode == ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)) {
                // serialization exception
                std::stringstream ss;
                ss<<"#"<<env->numExceptionRows<<": row="<<rowNumber<<" ec="<<exceptionCode<<"\n";
                ss<<pythonStringFromParseException(input, dataLength);
                logger().debug(ss.str());
            }
        }
#endif


        // for speed reasons, serialize exception directly!
        size_t bufSize = 0;
        auto buf = serializeExceptionToMemory(exceptionCode, exceptionOperatorID, rowNumber, input, dataLength, &bufSize);

        if(out_buf.size() + bufSize <= out_buf.capacity()) {
            memcpy(out_buf.ptr(), buf, bufSize);
            out_buf.movePtr(bufSize);

            // update type (TODO, first traceback sample??)
            auto key = std::make_tuple(exceptionOperatorID, exceptionCode);
            env->exceptionCounts[key]++;
            env->numExceptionRows++;
        } else {

            // check if bufSize exceeds limit, if so resize and call again!
            if(bufSize > out_buf.capacity()) {
                out_buf.provideSpace(bufSize);
                writeException(threadNo, exceptionCode, exceptionOperatorID, rowNumber, input, dataLength); // call again, do not return. Instead spill for sure after the resize...
                logger().debug("row size exceeded internal exception buffer size, forced resize.");
                // buffer is full, save to spill out path
                spillExceptionBuffer(threadNo);
            } else {
                // buffer is full, save to spill out path
                spillExceptionBuffer(threadNo);
                writeException(threadNo, exceptionCode, exceptionOperatorID, rowNumber, input, dataLength);
            }
        }

        free(buf);
    }

    void WorkerApp::writeHashedRow(size_t threadNo, const uint8_t *key, int64_t key_size, bool bucketize, uint8_t *buf, int64_t buf_size) {

        assert(0 <= threadNo && threadNo < _numThreads);

        // write to corresponding env
        auto& env = _threadEnvs[threadNo];
        assert(env.hashMap);

        // only bytes map so far supported...
        if(key != nullptr && key_size > 0) {
            // put into hashmap!
            uint8_t *bucket = nullptr;
            if(bucketize) { //@TODO: maybe get rid off this if by specializing pipeline better for unique case...
                hashmap_get(env.hashMap, reinterpret_cast<const char *>(key), key_size, (void **) (&bucket));
                // update or new entry
                bucket = extend_bucket(bucket, buf, buf_size);
            }
            hashmap_put(env.hashMap, reinterpret_cast<const char *>(key), key_size, bucket);
        } else {
            // goes into null bucket, no hash
            env.nullBucket = extend_bucket(env.nullBucket, buf, buf_size);
        }
    }

    // static helper functions/callbacks
    int64_t WorkerApp::writeRowCallback(ThreadEnv* env, const uint8_t *buf, int64_t bufSize) {
        assert(env);
        return env->app->writeRow(env->threadNo, buf, bufSize);
    }
    void WorkerApp::writeHashCallback(ThreadEnv* env, const uint8_t *key, int64_t key_size, bool bucketize, uint8_t *bucket, int64_t bucket_size) {
        assert(env);
        env->app->writeHashedRow(env->threadNo, key, key_size, bucketize, bucket, bucket_size);
    }
    void WorkerApp::exceptRowCallback(ThreadEnv* env, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t *input, int64_t dataLength) {
        assert(env);
        env->app->writeException(env->threadNo, exceptionCode, exceptionOperatorID, rowNumber, input, dataLength);
    }

    int64_t WorkerApp::slowPathRowCallback(ThreadEnv *env, uint8_t *buf, int64_t bufSize) {

        // slow path & fast path should have compatible output. => hence write it out regularly!
        env->app->writeRow(env->threadNo, buf, bufSize);
        // env->app->logger().warn("slowPath writeRow called, not yet implemented");
        return 0;
    }

    void WorkerApp::slowPathExceptCallback(ThreadEnv *env, int64_t exceptionCode, int64_t exceptionOperatorID,
                                           int64_t rowNumber, uint8_t *input, int64_t dataLength) {
        if(!env->app->has_python_resolver()) {
            // it's a true exception and processing needs to stop b.c. no python code path is available, else resolveBuffer will try to call the python path on this...
            env->app->logger().warn("slowPath writeException called, but there's no interprter path. not yet implemented.");
        }
    }

    URI WorkerApp::getNextOutputURI(int threadNo, const URI& baseURI, bool isBaseURIFolder, const std::string& extension) {
        using namespace std;

        // @TODO: maybe add global file number to worker!
        // -> maybe worker should be told directly where to put files? how to consolidate them etc.?
        // -> need to find smart strategy for S3!
        // -> multi-file upload?

        assert(0 <= threadNo && threadNo < _numThreads);

        int num_files = _threadEnvs[threadNo].spillFiles.size();

        // use 4 digit code for files?
        string path = baseURI.toPath();
        if(isBaseURIFolder) {
            path += "/";
        }

        path += fmt::format("part_{:02d}_{:04d}.", threadNo, num_files, extension);

        return path;
    }

    size_t WorkerApp::ThreadEnv::hashMapSize() const {
        // only works for regular bytes hashmap right now... --> specialize further!
        if(!this->hashMap)
            return 0;
        auto hm_size = hashmap_size(this->hashMap);

        // @TODO: in order to add size of data-elements, need to decode bucket entries!

        // for now just return the hashmap size...
        return hm_size;
    }

    codegen::resolve_f WorkerApp::getCompiledResolver(const TransformStage* stage) {
        logger().info("compiling slow code path.");

        if(_settings.useInterpreterOnly || stage->slowPathBitCode().empty())
            return nullptr;

        if(!_settings.useCompiledGeneralPath)
            return nullptr;

#ifndef NDEBUG
        {
            llvm::LLVMContext ctx;
            auto slow_path_bit_code = stage->slowPathBitCode();
            auto slow_path_mod = slow_path_bit_code.empty() ? nullptr : codegen::bitCodeToModule(ctx, slow_path_bit_code);
            auto ir_code = codegen::moduleToString(*slow_path_mod.get());
            std::string general_save_path = "worker_general_resolver.txt";
            logger().debug("saved compiled general code path to " + general_save_path);
            stringToFile(URI(general_save_path), ir_code);
            logger().debug("size of general-case IR code is: " + sizeToMemString(ir_code.size()));
        }
#endif

        // determine which compiler to use based on store instruction threshold ( or general bitcode size?)
        JITCompiler *compiler_to_use = nullptr;

        compiler_to_use = _compiler.get();

        // this is a hack/magic constant
        logger().debug("slow path bitcode size: " + sizeToMemString(stage->slowPathBitCode().size()));

        // more than 512KB? -> select fast (non-optimizing) compiler
        auto bitcode_threshold_size = 512 * 1024; // 512KB
        if(stage->slowPathBitCode().size() > bitcode_threshold_size) {
            auto bitcode_size = stage->slowPathBitCode().size();
            logger().info("large bitcode " + sizeToMemString(bitcode_size) + " encountered larger than threshold of "
            + sizeToMemString(bitcode_threshold_size) + " for slow path, using fast compiler instead of optimizing-one.");
            compiler_to_use = _fastCompiler.get();
        }

        if(!compiler_to_use)
            throw std::runtime_error("invalid compiler pointer in getCompiledResolver");

        // perform actual compilation.
        Timer timer;
        auto syms = stage->compileSlowPath(*compiler_to_use, nullptr, false); // symbols should be known already...

        // store syms internally (lock)
        {
            std::lock_guard<std::mutex> lock(_symsMutex);
            _syms->update(syms);
        }

        logger().info("Compilation of slow path took " + std::to_string(timer.time()) + "s");
        return syms->resolveFunctor;
    }

    int64_t WorkerApp::resolveOutOfOrder(int threadNo, TransformStage *stage,
                                         std::shared_ptr<TransformStage::JITSymbols> syms) {
        using namespace std;

        assert(threadNo >= 0 && threadNo < _numThreads);
        auto env = &_threadEnvs[threadNo];

        _has_python_resolver = false; // NOT THREADSAFE

        if(0 == env->numExceptionRows)
            return WORKER_OK; // nothing todo

//        // !!! HACK !!!
//        if(stage->use_hyper()) {
//            std::cout<<"Stage got "<<env->numExceptionRows<<std::endl;
//
//            std::cout<<"skipping resolution for now, b.c. buggy."<<std::endl;
//            return WORKER_OK;
//        }
//        /// end


        // if no compiled resolver & no interpreted resolver are present, simply return.
        if(!syms->resolveFunctor && stage->purePythonCode().empty()) {
            logger().info("No resolve code shipped. Nothing can't be resolved here.");
            return WORKER_OK;
        }

        // is exception output schema different than normal case schema? I.e., upgrade necessary?
        if(stage->normalCaseOutputSchema() != stage->outputSchema()) {
            logger().error("normal case output schema: " + stage->normalCaseOutputSchema().getRowType().desc() + " general case output schema: " + stage->outputSchema().getRowType().desc());
            throw std::runtime_error("different schemas between normal/exception case not yet supported!");
        }

        // now go through all exceptions & resolve them.
        // --> create a copy of the buffer & spill-files, then empty them!
        Buffer exceptionBuf(1024 * 4);
        if(env->exceptionBuf.size() > 0) {
            exceptionBuf.provideSpace(env->exceptionBuf.size());
            memcpy(exceptionBuf.ptr(), env->exceptionBuf.buffer(), env->exceptionBuf.size());
            exceptionBuf.movePtr(env->exceptionBuf.size());
        }

        vector<SpillInfo> exceptFiles;
        vector<SpillInfo> normalFiles;
        std::copy_if(env->spillFiles.begin(), env->spillFiles.end(),
                     std::back_inserter(exceptFiles),
                     [](const SpillInfo& info) {return info.isExceptionBuf; });
        std::copy_if(env->spillFiles.begin(), env->spillFiles.end(),
                     std::back_inserter(normalFiles),
                     [](const SpillInfo& info) {return !info.isExceptionBuf; });
        env->spillFiles = normalFiles; // only normal files

        // reset internal exception buf stats
        size_t numExceptionRows = env->numExceptionRows;
        env->numExceptionRows = 0;
        env->exceptionOriginalPartNo = 0;
        env->exceptionBuf.reset();

        // 1. buffer, then spill files
        int64_t rc = WORKER_OK;
        if(exceptionBuf.size() > 0) {
            rc = resolveBuffer(threadNo, exceptionBuf, numExceptionRows, stage, syms);
            if(rc != WORKER_OK)
                return rc;
        }

        // same story with exception spill files. Load them first to the temp buffer, and then resolve...
        if(!exceptFiles.empty()) {
            logger().info("Processing " + pluralize(exceptFiles.size(), "spilled except file"));
            for(const auto& part_info : exceptFiles) {

                // loading into buffer & resolving it.
                logger().debug("opening except part file");
                auto part_file = VirtualFileSystem::open_file(part_info.path, VirtualFileMode::VFS_READ);

                if(!part_file) {
                    auto err_msg = "part file could not be found under " + part_info.path + ", output corrupted.";
                    logger().error(err_msg);
                    throw std::runtime_error(err_msg);
                }

                // read contents in from spill file...
                if(part_file->size() != part_info.file_size) {
                    logger().warn("part_file: " + std::to_string(part_file->size()) + " part_info: " + std::to_string(part_info.file_size));
                }
                // assert(part_file->size() == part_info.spill_info.file_size);
                assert(part_file->size() >= 2 * sizeof(int64_t));

                part_file->seek(2 * sizeof(int64_t)); // skip first two bytes representing bytes/rows
                size_t part_buffer_size = part_info.file_size - 2 * sizeof(int64_t);
                Buffer part_buffer;
                part_buffer.provideSpace(part_buffer_size);
                size_t bytes_read = 0;
                part_file->readOnly(part_buffer.ptr(), part_buffer_size, &bytes_read);
                logger().debug("read from parts file " + sizeToMemString(bytes_read));
                part_file->close();

                // process now...
                auto rc = resolveBuffer(threadNo, part_buffer, part_info.num_rows, stage, syms);
                if(rc != WORKER_OK)
                    return rc;

            }
            logger().info("except spill files done.");
        }

        return WORKER_OK;
    }

    static std::atomic_int rbuf_counter(0);


    std::string
    WorkerApp::exceptRowToString(int64_t ecRowNumber, const ExceptionCode &ecCode, const uint8_t *ecBuf, size_t ecBufSize, const python::Type& general_case_input_type) {
        std::stringstream ss;

        ss<<"normal -> general except | row: "<<ecRowNumber<<" ecCode: "<< ecToI64(ecCode)<<" size: "<<sizeToMemString(ecBufSize)<<" content: ";

        // what exception code is it? -> special types require certain conversions...
        switch(ecCode) {
            case ExceptionCode::BADPARSE_STRING_INPUT: {
                // bad parse row
                ss<<pythonStringFromParseException(ecBuf, ecBufSize);
                break;
            }
            default: {
                // must be in general-case except format -> decode via that
                Row row = Row::fromMemory(Schema(Schema::MemoryLayout::ROW, general_case_input_type), ecBuf, ecBufSize);
                ss<<row.toPythonString();
                break;
            }
        }

        return ss.str();
    }

    void
    WorkerApp::storeExceptSample(ThreadEnv *env, int64_t ecRowNumber, const ExceptionCode &ecCode, const uint8_t *ecBuf,
                                 size_t ecBufSize, const python::Type &general_case_input_type) {
        // space still there?
        if(env->normalToGeneralExceptCountPerEC[ecToI64(ecCode)] < ThreadEnv::MAX_EXCEPT_SAMPLE) {
            auto except_row_str = exceptRowToString(ecRowNumber, ecCode, ecBuf, ecBufSize, general_case_input_type);
            logger().debug(except_row_str);
            env->normalToGeneralExceptSample.push_back(except_row_str);
            env->normalToGeneralExceptCountPerEC[ecToI64(ecCode)]++;
        }
    }

    int64_t WorkerApp::resolveBuffer(int threadNo, Buffer &buf, size_t numRows, const TransformStage *stage,
                                     const std::shared_ptr<TransformStage::JITSymbols> &syms) {

        if(0 == numRows)
            return WORKER_OK;

        // counters for debugging
        size_t resolved_via_compiled_slow_path = 0;
        size_t resolved_via_interpreter = 0;
        size_t exception_count = 0;

        // fetch functors
        codegen::resolve_f compiledResolver = nullptr;
        if(_settings.opportuneGeneralPathCompilation) {

            // check if available in syms already, if not wait till thread completes...
            {
                std::lock_guard<std::mutex> lock(_symsMutex);
                compiledResolver = _syms->resolveFunctor;
            }

            if(!compiledResolver && !_settings.useInterpreterOnly && _settings.useCompiledGeneralPath) {
                logger().info("waiting for slow path compilation to complete...");
                if(_resolverCompileThread && _resolverCompileThread->joinable())
                    _resolverCompileThread->join(); // wait till compile thread finishes...
                compiledResolver = _syms->resolveFunctor; // <-- no sync here necessary, b.c. thread elapsed.
                logger().info("slow path retrieved!");
                _resolverCompileThread.reset(nullptr);
            }
        } else {

            // check if in syms, if not compile and update!
            {
                std::lock_guard<std::mutex> lock(_symsMutex);
                compiledResolver = _syms->resolveFunctor;
            }
            if(!compiledResolver)
                compiledResolver = getCompiledResolver(stage); // syms->resolveFunctor;

            {
                std::lock_guard<std::mutex> lock(_symsMutex);
                _syms->resolveFunctor = compiledResolver;
            }
        }
        auto interpretedResolver = preparePythonPipeline(stage->purePythonCode(), stage->pythonPipelineName());
        _has_python_resolver = true;

        // deactivate compiled resolver according to setting
        if(!_settings.useCompiledGeneralPath)
            compiledResolver = nullptr;

        // debug:
#ifndef NDEBUG
        Logger::instance().defaultLogger().debug("saved interpreted code path (" + stage->pythonPipelineName() + ") to interpreted_resolver.py");
        stringToFile(URI("worker_interpreted_resolver.py"), stage->purePythonCode());
#endif

        bool is_first_unresolved_interpreter = true;

        // when both compiled resolver & interpreted resolver are invalid, this means basically that all exceptions stay...
        const auto* ptr = static_cast<const uint8_t*>(buf.buffer());
        auto env = &_threadEnvs[threadNo];
        for(unsigned i = 0; i < numRows; ++i) {
            auto rc = -1;

            // // debug
            // std::cout<<"processing row "<<(i+1)<<"/"<<numRows<<std::endl;

            // most of the code here is similar to ResolveTask.cc --> maybe avoid redundant code!
            // deserialize exception
            const uint8_t *ecBuf = nullptr;
            int64_t ecCode = -1, ecOperatorID = -1;
            int64_t ecRowNumber = -1;
            size_t ecBufSize = 0;
            auto delta = deserializeExceptionFromMemory(ptr, &ecCode, &ecOperatorID, &ecRowNumber, &ecBuf,
                                                        &ecBufSize);

            // try to resolve using compiled resolver...
            if(compiledResolver) {

                // debug, can remove lines from here...
                // check if not enough samples stored, if so store row!
                {
                    auto general_case_input_type = stage->inputSchema().getRowType();
                    storeExceptSample(env, ecRowNumber, i64ToEC(ecCode), ecBuf, ecBufSize, general_case_input_type);
                }
                // ... to here


                // for hyper, force onto general case format.
                 rc = compiledResolver(env, ecRowNumber, ecCode, ecBuf, ecBufSize);
                if(rc != ecToI32(ExceptionCode::SUCCESS)) {
                    // fallback is only required if normalcaseviolation or badparsestringinput, else it's considered a true exception
                    // to force reprocessing always onto fallback path, use rc = -1 here
                    if(rc == ecToI32(ExceptionCode::NORMALCASEVIOLATION)
                       || rc == ecToI32(ExceptionCode::BADPARSE_STRING_INPUT)
                       || rc == ecToI32(ExceptionCode::NULLERROR)
                       || rc == ecToI32(ExceptionCode::GENERALCASEVIOLATION)
                       || rc == ecToI32(ExceptionCode::PYTHON_PARALLELIZE))
                        rc = -1;
                    else {
                        // it's a true exception the resolver won't be able to handle.
                        // can short circuit here. => i.e. for key error.

                        // b.c. this requires resolver to write exception row (not done yet) -> defer to interpreter. (??) maybe it's done ??
                        rc = -1;
                    }
                } else {
                    // resolved if rc == success
                    assert(rc == ecToI32(ExceptionCode::SUCCESS));
                    resolved_via_compiled_slow_path++;
                }
            }

            // try to resolve using interpreted resolver
            if(-1 == rc && interpretedResolver) {
                bool output_is_hashtable = stage->outputMode() == EndPointMode::HASHTABLE;
                Schema exception_input_schema = stage->inputSchema();
                Schema specialized_output_schema = stage->normalCaseOutputSchema();
                Schema general_output_schema = stage->outputSchema();

                std::stringstream err_stream;

                FallbackPathResult fallbackRes;
                fallbackRes.code = ecToI64(ExceptionCode::UNKNOWN);
                python::lockGIL();
                try {
#ifdef NDEBUG
                    auto err_stream_pointer = &err_stream;
                    // only stream out first row as long as there is no unresolved.
                    if(!is_first_unresolved_interpreter)
                        err_stream_pointer = nullptr;
#else
                    auto err_stream_pointer = &err_stream;
#endif

                    processRowUsingFallback(fallbackRes, interpretedResolver,
                                                               ecCode,
                                                               ecOperatorID,
                                                               stage->normalCaseInputSchema(),
                                                               stage->inputSchema(),
                                                               ecBuf, ecBufSize,
                                                               specialized_output_schema,
                                                               general_output_schema,
                                                               {},
                                                               _settings.allowNumericTypeUnification,
                                                               output_is_hashtable,
                                                               err_stream_pointer);
                } catch(const std::exception& e) {
                    logger().error(std::string("while processing fallback path, an internal exception occurred: ") + e.what());
                } catch(...) {
                    logger().error(std::string("while processing fallback path, an unknown internal exception occurred"));
                }
                python::unlockGIL();

                auto err = err_stream.str();
                if(!err.empty())
                    logger().error(err);

                // check whether output succeeded
                if(fallbackRes.code == ecToI64(ExceptionCode::SUCCESS)) {
                    // worked, take buffer result and serialize!
                    if(!fallbackRes.pyObjects.empty() || fallbackRes.generalBuf.size() > 0) {
                        throw std::runtime_error("not yet supported!");
                    }

                    // take normal buf and add to output (i.e. simple copy)!
                    if(fallbackRes.bufRowCount > 0) {
                        env->normalBuf.provideSpace(fallbackRes.buf.size());
                        memcpy(env->normalBuf.ptr(), fallbackRes.buf.buffer(), fallbackRes.buf.size());
                        env->normalBuf.movePtr(fallbackRes.buf.size());
                        env->numNormalRows += fallbackRes.bufRowCount;
                    }
                    rc = 0; // all good, next row!
                    resolved_via_interpreter++;
                } else {
                    // didn't work, keep as original exception or partially resolved path?
                    // -> for now keep as it is...

                    // debug: print out result for first unresolved row.
                    if(is_first_unresolved_interpreter) {

                        // #ifndef NDEBUG
                        bool parse_cells = false;
                        PyObject* tuple = nullptr;
                        python::lockGIL();
                        std::tie(parse_cells, tuple) = decodeFallbackRow(i64ToEC(ecCode), ecBuf, ecBufSize,  stage->normalCaseInputSchema(),
                                                                         stage->inputSchema());
                        //PyObject_Print(tuple, stdout, 0);
                        // std::cout<<std::endl;
                        auto row_as_str = python::PyString_AsString(tuple);
                        python::unlockGIL();
// #endif

                        std::stringstream ss;
                        ss<<"first row passed to interpreter, after ec="<<ecCode<<" has rc="<<fallbackRes.code<<"\n"<<row_as_str<<"\n";
                        is_first_unresolved_interpreter = false;
                        logger().info(ss.str());
                    }

                    // need to update ecCode and operator ID from fallback!
                    ecCode = fallbackRes.code;
                    ecOperatorID = fallbackRes.operatorID;
                    // rc == -1 indicates write it as exception.
                    rc = -1;
                }
            }

            // check if not success, then also exception (i.e. fallthrough from compiled resolver
            if(rc != ecToI32(ExceptionCode::SUCCESS))
                rc = -1;

            // else, save as exception!
            if(-1 == rc) {
                exception_count++;
                writeException(threadNo, ecCode, ecOperatorID, ecRowNumber, const_cast<uint8_t *>(ecBuf), ecBufSize);
            }

            ptr += delta;
        }

        // sanity check: (applies for input row counts!)
        assert(numRows == exception_count + resolved_via_compiled_slow_path + resolved_via_interpreter);

        // update stats
        _codePathStats.rowsOnGeneralPathCount += resolved_via_compiled_slow_path;
        _codePathStats.rowsOnInterpreterPathCount += resolved_via_interpreter;
        _codePathStats.unresolvedRowsCount += numRows - resolved_via_compiled_slow_path - resolved_via_interpreter;

        std::stringstream ss;
        ss<<"Resolved buffer, compiled: "<<resolved_via_compiled_slow_path<<" interpreted: "
          <<resolved_via_interpreter<<" unresolved: "<<(numRows - resolved_via_compiled_slow_path - resolved_via_interpreter);
        env->app->logger().info(ss.str());

        return WORKER_OK;
    }

    URI WorkerApp::outputURIFromReq(const messages::InvocationRequest &request) {

        URI baseURI(request.baseoutputuri());
        auto output_fmt = proto_toFileFormat(request.stage().outputformat());
        auto ext = defaultFileExtension(output_fmt);
        if(request.has_partnooffset())
            return baseURI.join("part" + std::to_string(request.partnooffset()) + "." + ext);
        else
            return URI(request.baseoutputuri() + "." + ext);
    }

    PyObject* fallbackTupleFromParseException(const uint8_t* buf, size_t buf_size) {
        // cf.  char* serializeParseException(int64_t numCells,
        //            char **cells,
        //            int64_t* sizes,
        //            size_t *buffer_size,
        //            std::vector<bool> colsToSerialize,
        //            decltype(malloc) allocator)
        int64_t num_cells = *(int64_t*)buf; buf += sizeof(int64_t);
        PyObject* tuple = PyTuple_New(num_cells);
        for(unsigned j = 0; j < num_cells; ++j) {
            auto info = *(int64_t*)buf;
            auto offset = info & 0xFFFFFFFF;
            const char* cell = reinterpret_cast<const char *>(buf + offset);
            auto cell_size = info >> 32u;

            // @TODO: quicker conversion from str cell?
            PyTuple_SET_ITEM(tuple, j, python::PyString_FromString(cell));
            buf += sizeof(int64_t);
        }
        return tuple;
    }

//    FallbackPathResult processRowUsingFallback(PyObject* func,
//                                               int64_t ecCodeWithFmt,
//                                               int64_t ecOperatorID,
//                                               const Schema& normal_input_schema,
//                                               const Schema& general_input_schema,
//                                               const uint8_t* buf,
//                                               size_t buf_size,
//                                               const Schema& specialized_target_schema,
//                                               const Schema& general_target_schema,
//                                               const std::vector<PyObject*>& py_intermediates,
//                                               bool allowNumericTypeUnification,
//                                               bool returnAllAsPyObjects,
//                                               std::ostream *err_stream) {
//
//        assert(func && PyCallable_Check(func));
//
//        FallbackPathResult res;
//
//        // holds the pythonized data
//        PyObject* tuple = nullptr;
//
//        bool parse_cells = false;
//
//        // extract serialization format from code
//        auto exFmt = static_cast<ExceptionSerializationFormat>(ecCodeWithFmt >> 32);
//        auto ecCode = ecCodeWithFmt & 0xFFFFFFFF;
//
//        // fmt needs to be either generalcase or cells
//        if(exFmt != ExceptionSerializationFormat::GENERALCASE && exFmt != ExceptionSerializationFormat::STRING_CELLS) {
//            // whe normal == general, ok
//            if(normal_input_schema != general_input_schema)
//                throw std::runtime_error("not yet supported, fallback only allows for general case exception format or string cells");
//        }
//
//        // there are different data reps for certain error codes.
//        // => decode the correct object from memory & then feed it into the pipeline...
//        if(exFmt == ExceptionSerializationFormat::STRING_CELLS) {
//            // it's a string!
//            tuple = fallbackTupleFromParseException(buf, buf_size);
//            parse_cells = true; // need to parse cells in python mode.
//        } else if(exFmt == ExceptionSerializationFormat::NORMALCASE) {
//            // changed, why are these names so random here? makes no sense...
//            auto row = Row::fromMemory(normal_input_schema, buf, buf_size);
//
//            // upcast to general-case schema b.c. fallback will only accept genral-case rows?
//            throw std::runtime_error("not yet implemented, fallback will only accept general-case rows currently!");
//            // maybe generate upcast into python code as well?
//
//            tuple = python::rowToPython(row, true);
//            parse_cells = false;
//            // called below...
//        } else if(exFmt == ExceptionSerializationFormat::GENERALCASE) {
//            // normal case, i.e. an exception occurred somewhere.
//            // --> this means if pipeline is using string as input, we should convert
//            auto row = Row::fromMemory(general_input_schema, buf, buf_size);
//            // cell source automatically takes input, i.e. no need to convert. simply get tuple from row object
//            tuple = python::rowToPython(row, true);
//            parse_cells = false;
//        } else {
//            throw std::runtime_error("unknown serialization format.");
//        }
//
//        // compute
//        // @TODO: we need to encode the hashmaps as these hybrid objects!
//        // ==> for more efficiency we prob should store one per executor!
//        //     the same goes for any hashmap...
//
//        assert(tuple);
//#ifndef NDEBUG
//        if(!tuple) {
//            if(err_stream)
//                *err_stream<<"bad decode, using () as dummy..."<<std::endl;
//            tuple = PyTuple_New(0); // empty tuple.
//        }
//#endif
//
//
//        // note: current python pipeline always expects a tuple arg. hence pack current element.
//        if(PyTuple_Check(tuple) && PyTuple_Size(tuple) > 1) {
//            // nothing todo...
//        } else {
//            auto tmp_tuple = PyTuple_New(1);
//            PyTuple_SET_ITEM(tmp_tuple, 0, tuple);
//            tuple = tmp_tuple;
//        }
//
//#ifndef NDEBUG
//        // // to print python object
//        // Py_XINCREF(tuple);
//        // PyObject_Print(tuple, stdout, 0);
//        // std::cout<<std::endl;
//#endif
//
//        // call pipFunctor
//        PyObject* args = PyTuple_New(1 + py_intermediates.size());
//        PyTuple_SET_ITEM(args, 0, tuple);
//        for(unsigned i = 0; i < py_intermediates.size(); ++i) {
//            Py_XINCREF(py_intermediates[i]);
//            PyTuple_SET_ITEM(args, i + 1, py_intermediates[i]);
//        }
//
//        auto kwargs = PyDict_New(); PyDict_SetItemString(kwargs, "parse_cells", parse_cells ? Py_True : Py_False);
//        auto pcr = python::callFunctionEx(func, args, kwargs);
//
//        if(pcr.exceptionCode != ExceptionCode::SUCCESS) {
//            // this should not happen, bad internal error. codegen'ed python should capture everything.
//            if(err_stream)
//                *err_stream<<"bad internal python error: " + pcr.exceptionMessage<<std::endl;
//        } else {
//            // all good, row is fine. exception occured?
//            assert(pcr.res);
//
//            // type check: save to regular rows OR save to python row collection
//            if(!pcr.res) {
//                if(err_stream)
//                    *err_stream<<"bad internal python error, NULL object returned"<<std::endl;
//            } else {
//
//#ifndef NDEBUG
//                // // uncomment to print res obj
//                // Py_XINCREF(pcr.res);
//                // PyObject_Print(pcr.res, stdout, 0);
//                // std::cout<<std::endl;
//#endif
//                auto exceptionObject = PyDict_GetItemString(pcr.res, "exception");
//                if(exceptionObject) {
//
//                    // overwrite operatorID which is throwing.
//                    auto exceptionOperatorID = PyDict_GetItemString(pcr.res, "exceptionOperatorID");
//                    ecOperatorID = PyLong_AsLong(exceptionOperatorID);
//                    auto exceptionType = PyObject_Type(exceptionObject);
//                    // can ignore input row.
//                    ecCode = ecToI64(python::translatePythonExceptionType(exceptionType));
//
//#ifndef NDEBUG
//                    // // debug printing of exception and what the reason is...
//                    // // print res obj
//                    // Py_XINCREF(pcr.res);
//                    // std::cout<<"exception occurred while processing using python: "<<std::endl;
//                    // PyObject_Print(pcr.res, stdout, 0);
//                    // std::cout<<std::endl;
//#endif
//                    // deliver that result is exception
//                    res.code = ecCode;
//                    res.operatorID = ecOperatorID;
//                } else {
//                    // normal, check type and either merge to normal set back OR onto python set together with row number?
//                    auto resultRows = PyDict_GetItemString(pcr.res, "outputRows");
//                    assert(PyList_Check(resultRows));
//                    for(int i = 0; i < PyList_Size(resultRows); ++i) {
//                        // type check w. output schema
//                        // cf. https://pythonextensionpatterns.readthedocs.io/en/latest/refcount.html
//                        auto rowObj = PyList_GetItem(resultRows, i);
//                        Py_XINCREF(rowObj);
//
//                        // returnAllAsPyObjects makes especially sense when hashtable is used!
//                        if(returnAllAsPyObjects) {
//                            res.pyObjects.push_back(rowObj);
//                            continue;
//                        }
//
//                        auto rowType = python::mapPythonClassToTuplexType(rowObj);
//
//                        // special case output schema is str (fileoutput!)
//                        if(rowType == python::Type::STRING) {
//                            // write to file, no further type check necessary b.c.
//                            // if it was the object string it would be within a tuple!
//                            auto cptr = PyUnicode_AsUTF8(rowObj);
//                            Py_XDECREF(rowObj);
//
//                            auto size = strlen(cptr);
//                            res.buf.provideSpace(size);
//                            memcpy(res.buf.ptr(), reinterpret_cast<const uint8_t *>(cptr), size);
//                            res.buf.movePtr(size);
//                            res.bufRowCount++;
//                            //mergeRow(reinterpret_cast<const uint8_t *>(cptr), strlen(cptr), BUF_FORMAT_NORMAL_OUTPUT); // don't write '\0'!
//                        } else {
//
//                            // there are three options where to store the result now
//                            // 1. fits targetOutputSchema (i.e. row becomes normalcase row)
//                            bool outputAsNormalRow = python::Type::UNKNOWN != unifyTypes(rowType, specialized_target_schema.getRowType(), allowNumericTypeUnification)
//                                                     && canUpcastToRowType(rowType, specialized_target_schema.getRowType());
//                            // 2. fits generalCaseOutputSchema (i.e. row becomes generalcase row)
//                            bool outputAsGeneralRow = python::Type::UNKNOWN != unifyTypes(rowType,
//                                                                                          general_target_schema.getRowType(), allowNumericTypeUnification)
//                                                      && canUpcastToRowType(rowType, general_target_schema.getRowType());
//
//                            // 3. doesn't fit, store as python object. => we should use block storage for this as well. Then data can be shared.
//
//                            // can upcast? => note that the && is necessary because of cases where outputSchema is
//                            // i64 but the given row type f64. We can cast up i64 to f64 but not the other way round.
//                            if(outputAsNormalRow) {
//                                Row resRow = python::pythonToRow(rowObj).upcastedRow(specialized_target_schema.getRowType());
//                                assert(resRow.getRowType() == specialized_target_schema.getRowType());
//
//                                // write to buffer & perform callback
//                                // auto buf_size = 2 * resRow.serializedLength();
//                                // uint8_t *buf = new uint8_t[buf_size];
//                                // memset(buf, 0, buf_size);
//                                // auto serialized_length = resRow.serializeToMemory(buf, buf_size);
//                                // // call row func!
//                                // // --> merge row distinguishes between those two cases. Distinction has to be done there
//                                // //     because of compiled functor who calls mergeRow in the write function...
//                                // mergeRow(buf, serialized_length, BUF_FORMAT_NORMAL_OUTPUT);
//                                // delete [] buf;
//                                auto serialized_length = resRow.serializedLength();
//                                res.buf.provideSpace(serialized_length);
//                                auto actual_length = resRow.serializeToMemory(static_cast<uint8_t *>(res.buf.ptr()), res.buf.capacity() - res.buf.size());
//                                assert(serialized_length == actual_length);
//                                res.buf.movePtr(serialized_length);
//                                res.bufRowCount++;
//                            } else if(outputAsGeneralRow) {
//                                Row resRow = python::pythonToRow(rowObj).upcastedRow(general_target_schema.getRowType());
//                                assert(resRow.getRowType() == general_target_schema.getRowType());
//
//                                throw std::runtime_error("not yet supported");
//
////                                // write to buffer & perform callback
////                                auto buf_size = 2 * resRow.serializedLength();
////                                uint8_t *buf = new uint8_t[buf_size];
////                                memset(buf, 0, buf_size);
////                                auto serialized_length = resRow.serializeToMemory(buf, buf_size);
////                                // call row func!
////                                // --> merge row distinguishes between those two cases. Distinction has to be done there
////                                //     because of compiled functor who calls mergeRow in the write function...
////                                mergeRow(buf, serialized_length, BUF_FORMAT_GENERAL_OUTPUT);
////                                delete [] buf;
//                            } else {
//                                res.pyObjects.push_back(rowObj);
//                            }
//                            // Py_XDECREF(rowObj);
//                        }
//                    }
//
//#ifndef NDEBUG
//                    if(PyErr_Occurred()) {
//                        // print out the otber objects...
//                        std::cout<<__FILE__<<":"<<__LINE__<<" python error not cleared properly!"<<std::endl;
//                        PyErr_Print();
//                        std::cout<<std::endl;
//                        PyErr_Clear();
//                    }
//#endif
//                    // everything was successful, change resCode to 0!
//                    res.code = ecToI64(ExceptionCode::SUCCESS);
//                }
//            }
//        }
//
//        return res;
//    }


    void processRowUsingFallback(FallbackPathResult& res, PyObject* func,
                                               int64_t ecCodeWithFmt,
                                               int64_t ecOperatorID,
                                               const Schema& normal_input_schema,
                                               const Schema& general_input_schema,
                                               const uint8_t* buf,
                                               size_t buf_size,
                                               const Schema& specialized_target_schema,
                                               const Schema& general_target_schema,
                                               const std::vector<PyObject*>& py_intermediates,
                                               bool allowNumericTypeUnification,
                                               bool returnAllAsPyObjects,
                                               std::ostream *err_stream) {

        assert(func && PyCallable_Check(func));

        // holds the pythonized data
        PyObject* tuple = nullptr;
        bool parse_cells = false;

        // extract serialization format from code
        auto exFmt = static_cast<ExceptionSerializationFormat>(ecCodeWithFmt >> 32);
        auto ecCode = ecCodeWithFmt & 0xFFFFFFFF;

        std::tie(parse_cells, tuple) = decodeFallbackRow(i64ToEC(ecCode), buf, buf_size, normal_input_schema, general_input_schema);

        // compute
        // @TODO: we need to encode the hashmaps as these hybrid objects!
        // ==> for more efficiency we prob should store one per executor!
        //     the same goes for any hashmap...

        assert(tuple);
#ifndef NDEBUG
        if(!tuple) {
            if(err_stream)
                *err_stream<<"bad decode, using () as dummy..."<<std::endl;
            tuple = PyTuple_New(0); // empty tuple.
        }
#endif

#ifndef NDEBUG
         // // to print python object
         // Py_XINCREF(tuple);
         // PyObject_Print(tuple, stdout, 0);
         // std::cout<<std::endl;
#endif

        // call pipFunctor
        PyObject* args = PyTuple_New(1 + py_intermediates.size());
        PyTuple_SET_ITEM(args, 0, tuple);
        for(unsigned i = 0; i < py_intermediates.size(); ++i) {
            Py_XINCREF(py_intermediates[i]);
            PyTuple_SET_ITEM(args, i + 1, py_intermediates[i]);
        }

        auto kwargs = PyDict_New();
        PyDict_SetItemString(kwargs, "parse_cells", python::boolToPython(parse_cells));
        auto pcr = python::callFunctionEx(func, args, kwargs);

        if(pcr.exceptionCode != ExceptionCode::SUCCESS) {
            // this should not happen, bad internal error. codegen'ed python should capture everything.
            if(err_stream)
                *err_stream<<"bad internal python error: " + pcr.exceptionMessage<<std::endl;
        } else {
            // all good, row is fine. exception occured?
            assert(pcr.res);

            // type check: save to regular rows OR save to python row collection
            if(!pcr.res) {
                if(err_stream)
                    *err_stream<<"bad internal python error, NULL object returned"<<std::endl;
            } else {

#ifndef NDEBUG
                // // uncomment to print res obj
                // Py_XINCREF(pcr.res);
                // PyObject_Print(pcr.res, stdout, 0);
                // std::cout<<std::endl;
#endif
                auto exceptionObject = PyDict_GetItemString(pcr.res, "exception");
                if(exceptionObject) {

                    // overwrite operatorID which is throwing.
                    auto exceptionOperatorID = PyDict_GetItemString(pcr.res, "exceptionOperatorID");
                    ecOperatorID = PyLong_AsLong(exceptionOperatorID);
                    auto exceptionType = PyObject_Type(exceptionObject);
                    // can ignore input row.
                    ecCode = ecToI64(python::translatePythonExceptionType(exceptionType));

                    // debug print first true exception row
                    if(err_stream) {
                        // incref and convert to string
                        Py_XINCREF(pcr.res);
                        auto res_as_str = python::PyString_AsString(pcr.res);
                        *err_stream<<"first failing interpreter row exception details: \n"<<res_as_str;
                    }

                    // @TODO: add here tooling to expose this better...
                    // // just print everything for debugging:
                    // {
                    //     Py_XINCREF(pcr.res);
                    //     auto res_as_str = python::PyString_AsString(pcr.res);
                    //     auto& logger = Logger::instance().logger("python");
                    //     logger.warn("pcr res: " + res_as_str);
                    //     Py_XINCREF(exceptionObject);
                    //     auto exc_as_str = python::PyString_AsString(exceptionObject);
                    //     logger.warn("exception obj: " + exc_as_str);
                    // }

#ifndef NDEBUG
                     // debug printing of exception and what the reason is...
                     // print res obj
                     Py_XINCREF(pcr.res);
                     std::cout<<"exception occurred while processing using python: "<<std::endl;
                     PyObject_Print(pcr.res, stdout, 0);
                     std::cout<<std::endl;
#endif
                    // deliver that result is exception
                    res.code = ecCode;
                    res.operatorID = ecOperatorID;
                } else {
                    // normal, check type and either merge to normal set back OR onto python set together with row number?
                    auto resultRows = PyDict_GetItemString(pcr.res, "outputRows");
                    assert(PyList_Check(resultRows));
                    for(int i = 0; i < PyList_Size(resultRows); ++i) {
                        // type check w. output schema
                        // cf. https://pythonextensionpatterns.readthedocs.io/en/latest/refcount.html
                        auto rowObj = PyList_GetItem(resultRows, i);
                        Py_XINCREF(rowObj);

                        // returnAllAsPyObjects makes especially sense when hashtable is used!
                        if(returnAllAsPyObjects) {
                            res.pyObjects.push_back(rowObj);
                            continue;
                        }

                        auto rowType = python::mapPythonClassToTuplexType(rowObj);

                        // special case output schema is str (fileoutput!)
                        if(rowType == python::Type::STRING) {
                            // write to file, no further type check necessary b.c.
                            // if it was the object string it would be within a tuple!
                            auto cptr = PyUnicode_AsUTF8(rowObj);
                            Py_XDECREF(rowObj);

                            auto size = strlen(cptr);
                            res.buf.provideSpace(size);
                            memcpy(res.buf.ptr(), reinterpret_cast<const uint8_t *>(cptr), size);
                            res.buf.movePtr(size);
                            res.bufRowCount++;
                            //mergeRow(reinterpret_cast<const uint8_t *>(cptr), strlen(cptr), BUF_FORMAT_NORMAL_OUTPUT); // don't write '\0'!
                        } else {

                            // there are three options where to store the result now
                            // 1. fits targetOutputSchema (i.e. row becomes normalcase row)
                            bool outputAsNormalRow = python::Type::UNKNOWN != unifyTypes(rowType, specialized_target_schema.getRowType(), allowNumericTypeUnification)
                                                     && canUpcastToRowType(rowType, specialized_target_schema.getRowType());
                            // 2. fits generalCaseOutputSchema (i.e. row becomes generalcase row)
                            bool outputAsGeneralRow = python::Type::UNKNOWN != unifyTypes(rowType,
                                                                                          general_target_schema.getRowType(), allowNumericTypeUnification)
                                                      && canUpcastToRowType(rowType, general_target_schema.getRowType());

                            // 3. doesn't fit, store as python object. => we should use block storage for this as well. Then data can be shared.

                            // can upcast? => note that the && is necessary because of cases where outputSchema is
                            // i64 but the given row type f64. We can cast up i64 to f64 but not the other way round.
                            if(outputAsNormalRow) {
                                Row resRow = python::pythonToRow(rowObj).upcastedRow(specialized_target_schema.getRowType());
                                assert(resRow.getRowType() == specialized_target_schema.getRowType());

                                // write to buffer & perform callback
                                // auto buf_size = 2 * resRow.serializedLength();
                                // uint8_t *buf = new uint8_t[buf_size];
                                // memset(buf, 0, buf_size);
                                // auto serialized_length = resRow.serializeToMemory(buf, buf_size);
                                // // call row func!
                                // // --> merge row distinguishes between those two cases. Distinction has to be done there
                                // //     because of compiled functor who calls mergeRow in the write function...
                                // mergeRow(buf, serialized_length, BUF_FORMAT_NORMAL_OUTPUT);
                                // delete [] buf;
                                auto serialized_length = resRow.serializedLength();
                                res.buf.provideSpace(serialized_length);
                                auto actual_length = resRow.serializeToMemory(static_cast<uint8_t *>(res.buf.ptr()), res.buf.capacity() - res.buf.size());
                                assert(serialized_length == actual_length);
                                res.buf.movePtr(serialized_length);
                                res.bufRowCount++;
                            } else if(outputAsGeneralRow) {
                                Row resRow = python::pythonToRow(rowObj).upcastedRow(general_target_schema.getRowType());
                                assert(resRow.getRowType() == general_target_schema.getRowType());

                                throw std::runtime_error("not yet supported");

//                                // write to buffer & perform callback
//                                auto buf_size = 2 * resRow.serializedLength();
//                                uint8_t *buf = new uint8_t[buf_size];
//                                memset(buf, 0, buf_size);
//                                auto serialized_length = resRow.serializeToMemory(buf, buf_size);
//                                // call row func!
//                                // --> merge row distinguishes between those two cases. Distinction has to be done there
//                                //     because of compiled functor who calls mergeRow in the write function...
//                                mergeRow(buf, serialized_length, BUF_FORMAT_GENERAL_OUTPUT);
//                                delete [] buf;
                            } else {
                                res.pyObjects.push_back(rowObj);
                            }
                            // Py_XDECREF(rowObj);
                        }
                    }

#ifndef NDEBUG
                    if(PyErr_Occurred()) {
                        // print out the otber objects...
                        std::cout<<__FILE__<<":"<<__LINE__<<" python error not cleared properly!"<<std::endl;
                        PyErr_Print();
                        std::cout<<std::endl;
                        PyErr_Clear();
                    }
#endif
                    // everything was successful, change resCode to 0!
                    res.code = ecToI64(ExceptionCode::SUCCESS);
                }
            }
        }
    }


    std::vector<FilePart> mergeParts(const std::vector<FilePart>& parts, size_t startPartNo) {
        std::vector<FilePart> merged;

        // create copy & sort after partNo
        std::vector<FilePart> copy_parts(parts.begin(), parts.end());
        std::sort(copy_parts.begin(), copy_parts.end(), [](const FilePart& a, const FilePart& b) {

            auto end_a = a.rangeEnd;
            auto end_b = b.rangeEnd;
            // correct for full file
            if(a.rangeStart == 0 && a.rangeEnd == 0)
                end_a = a.size;
            if(b.rangeStart == 0 && b.rangeEnd == 0)
                end_b = b.size;

            return a.partNo < b.partNo && end_a < end_b;
        });

        for(const auto& part : copy_parts) {
            if(merged.empty()) {
                merged.push_back(part);
                continue;
            }

            // check whether this part can be merged with current one (same uri and consecutive partNo!)
            assert(!merged.empty());
            if((merged.back().partNo == part.partNo || merged.back().partNo + 1 == part.partNo)
            && (merged.back().uri == part.uri)) {
                merged.back().rangeEnd = part.rangeEnd;
                merged.back().partNo = part.partNo;

                // correction for full file
                if(merged.back().rangeStart == 0 && merged.back().rangeEnd == merged.back().size)
                    merged.back().rangeEnd = 0;
            } else {
                merged.push_back(part);
            }
        }

        // overwrite part numbers
        for(unsigned i = 0; i < merged.size(); ++i) {
            merged[i].partNo = startPartNo + i;
        }

        return merged;
    }

    std::string
    WorkerApp::jsonStat(const tuplex::messages::InvocationRequest &req, tuplex::TransformStage *stage) const {
        std::stringstream ss;

        ss<<"{";
        auto num_input_files = req.inputuris_size();

        // input path breakdown:
        ss<<"\"input\":{";
        ss<<"\"input_file_count\":"<<num_input_files<<",";
        ss<<"\"total_input_row_count\":"<<_codePathStats.inputRowCount<<",";
        ss<<"\"normal\":"<<_codePathStats.rowsOnNormalPathCount<<",";
        ss<<"\"general\":"<<_codePathStats.rowsOnGeneralPathCount<<",";
        ss<<"\"fallback\":"<<_codePathStats.rowsOnInterpreterPathCount<<",";
        ss<<"\"unresolved\":"<<_codePathStats.unresolvedRowsCount;
        ss<<"}";

        // output path breakdown
        ss<<",\"output\":{";
        ss<<"\"normal\":"<<_statistics.back().numNormalOutputRows;
        ss<<",\"except\":"<<_statistics.back().numExceptionOutputRows;
        ss<<"}";

        // go over timing dict (should be reset)
        ss<<",\"timings\":{";
        unsigned counter = 0;
        for(auto kv : _timeDict) {
            ss<<"\""<<kv.first<<"\":"<<kv.second;
            if(counter != _timeDict.size() - 1)
                ss<<",";
            counter++;
        }
        ss<<"}";

        // save whether hyper was active or not
        std::string hyper_active = useHyperSpecialization(req) ? "true" : "false";
        ss<<",\"hyper_active\":"<<hyper_active;

        ss<<"}";

        // basic checks: do line counts add up correctly?
#ifndef NDEBUG
        using namespace std;
        auto noexcept_in = _codePathStats.rowsOnNormalPathCount + _codePathStats.rowsOnGeneralPathCount + _codePathStats.rowsOnInterpreterPathCount;
        auto except_in = _codePathStats.unresolvedRowsCount.load();
        cout<<"input row count: "<<_codePathStats.inputRowCount.load()<<" = "
        <<noexcept_in<<" (normal) + "<<except_in<<" (except) . is this true? "
        <<boolalpha<<(_codePathStats.inputRowCount.load() == noexcept_in + except_in)<<endl;
#endif

        return ss.str();
    }

    WorkerApp::~WorkerApp() {
        // wait for compile thread to end
        if(_resolverCompileThread && _resolverCompileThread->joinable()) {
            _resolverCompileThread->join();
            _resolverCompileThread.reset(nullptr);
        }

        shutdown();
    }
}
