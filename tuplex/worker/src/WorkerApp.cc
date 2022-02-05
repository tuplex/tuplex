//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 11/22/2021                                                               //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#include <WorkerApp.h>
#include <physical/TransformTask.h>
#include "ee/local/LocalBackend.h"
#include "TypeAnnotatorVisitor.h"
#include "AWSCommon.h"
#include "bucket.h"

namespace tuplex {

    int WorkerApp::globalInit() {
        // skip if already initialized.
        if(_globallyInitialized)
            return WORKER_OK;

        logger().info("WorkerAPP globalInit");

        // runtime library path
        auto runtime_path = ContextOptions::defaults().RUNTIME_LIBRARY().toPath();
        std::string python_home_dir = python::find_stdlib_location();

        if(python_home_dir.empty()) {
            logger().error("Could not detect python stdlib location");
            return WORKER_ERROR_NO_PYTHON_HOME;
        }

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
        _compiler = std::make_shared<JITCompiler>();

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

        initThreadEnvironments(_settings.numThreads);
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

    void WorkerApp::shutdown() {
        if(python::isInterpreterRunning()) {
            python::lockGIL();
            python::closeInterpreter();
        }

        runtime::freeRunTimeMemory();

#ifdef BUILD_WITH_AWS
        shutdownAWS();
#endif
    }

    int WorkerApp::messageLoop() {
        return 0;
    }

    int WorkerApp::processJSONMessage(const std::string &message) {
        auto& logger = this->logger();

        // logger.info("JSON request: " + message);

        // parse JSON into protobuf
        tuplex::messages::InvocationRequest req;
        auto rc = google::protobuf::util::JsonStringToMessage(message, &req);
        if(!rc.ok()) {
            logger.error("could not parse json into protobuf message, bad parse for request - invalid format?");
            return WORKER_ERROR_INVALID_JSON_MESSAGE;
        }

        // get worker settings from message, if they differ from current setup -> reinitialize worker!
        auto settings = settingsFromMessage(req);
        if(settings != _settings)
            reinitialize(settings);

        // currently message represents merely a transformstage/transformtask
        // @TODO: in the future this could change!
        auto tstage = TransformStage::from_protobuf(req.stage());

        for(unsigned i = 0; i < req.inputuris_size(); ++i) {
            logger.debug("input uri: " + req.inputuris(i) + " size: " + std::to_string(req.inputsizes(i)));
        }

        return processMessage(req);
    }

    int64_t WorkerApp::initTransformStage(const TransformStage::InitData& initData,
                                          const std::shared_ptr<TransformStage::JITSymbols> &syms) {
        // initialize stage
        int64_t init_rc = 0;
        if((init_rc = syms->initStageFunctor(initData.numArgs,
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
        // call release func for stage globals
        if(syms->releaseStageFunctor() != 0) {
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

        // only transform stage yet supported, in the future support other stages as well!
        auto tstage = TransformStage::from_protobuf(req.stage());
        URI outputURI = outputURIFromReq(req);
        auto parts = partsFromMessage(req);

        // check settings, pure python mode?
        if(req.settings().has_useinterpreteronly() && req.settings().useinterpreteronly()) {
            logger().info("WorkerApp is processing everything in single-threaded python/fallback mode.");
            return processTransformStageInPythonMode(tstage, parts, outputURI);
        }

        // if not, compile given code & process using both compile code & fallback
        auto syms = compileTransformStage(*tstage);
        if(!syms)
            return WORKER_ERROR_COMPILATION_FAILED;

        return processTransformStage(tstage, syms, parts, outputURI);
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

        // loop over parts & process
        python::lockGIL();
        logger().info("GIL locked, processing " + pluralize(input_parts.size(), "part"));
        for(const auto& part : input_parts) {
            auto rc = processSourceInPython(0, tstage->fileInputOperatorID(),
                                            part, tstage, pipelineFunctionObj, false);
            logger().info("part processed, rc=" + std::to_string(rc));
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
        if(rc != WORKER_OK)
            return rc;
        return WORKER_OK;
    }

    int WorkerApp::processTransformStage(const TransformStage *tstage,
                                         const std::shared_ptr<TransformStage::JITSymbols> &syms,
                                         const std::vector<FilePart> &input_parts, const URI &output_uri) {

        size_t minimumPartSize = 1024 * 1024; // 1MB.

        Timer timer;

        // init stage, abort on error
        auto rc = initTransformStage(tstage->initData(), syms);
        if(rc != WORKER_OK)
            return rc;

        auto numCodes = std::max(1ul, _numThreads);
        auto processCodes = new int[numCodes];
        memset(processCodes, WORKER_OK, sizeof(int) * numCodes);

        // process data (single-threaded or via thread pool!)
        if(_numThreads <= 1) {
            runtime::setRunTimeMemory(_settings.runTimeMemory, _settings.runTimeMemoryDefaultBlockSize);

            try {
                // single-threaded
                for(unsigned i = 0; i < input_parts.size(); ++i) {
                   auto fp = input_parts[i];

                    processCodes[0] = processSource(0, tstage->fileInputOperatorID(), fp, tstage, syms);
                    logger().debug("processed file " + std::to_string(i + 1) + "/" + std::to_string(input_parts.size()));
                    if(processCodes[0] != WORKER_OK)
                        break;
                }
            } catch(...) {
                processCodes[0] = WORKER_ERROR_EXCEPTION;
            }
            runtime::releaseRunTimeMemory();
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
            for(int i = 1; i < _numThreads; ++i) {
                threads.emplace_back([this, tstage, &syms, &processCodes](int threadNo, const std::vector<FilePart>& parts) {
                    logger().debug("thread (" + std::to_string(threadNo) + ") started.");

                    runtime::setRunTimeMemory(_settings.runTimeMemory, _settings.runTimeMemoryDefaultBlockSize);

                    try {
                        for(const auto& part : parts) {
                            logger().debug("thread (" + std::to_string(threadNo) + ") processing part");
                            processCodes[threadNo] = processSource(threadNo, tstage->fileInputOperatorID(), part, tstage, syms);
                            if(processCodes[threadNo] != WORKER_OK)
                                break;
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
                for(auto part : parts[0]) {
                    logger().debug("thread (main) processing part");
                    processCodes[0] = processSource(0, tstage->fileInputOperatorID(), part, tstage, syms);
                    if(processCodes[0] != WORKER_OK)
                        break;
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

            logger().debug("All threads joined, processing done.");
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
            return WORKER_ERROR_PIPELINE_FAILED;
        }

        // print out info

        size_t numNormalRows = 0;
        size_t numExceptionRows = 0;
        size_t numHashRows = 0;
        size_t normalBufSize = 0;
        size_t exceptionBufSize = 0;
        size_t hashMapSize = 0;
        for(unsigned i = 0; i < _numThreads; ++i) {
            numNormalRows += _threadEnvs[i].numNormalRows;
            numExceptionRows += _threadEnvs[i].numExceptionRows;
            normalBufSize += _threadEnvs[i].normalBuf.size();
            exceptionBufSize += _threadEnvs[i].exceptionBuf.size();
            hashMapSize += _threadEnvs[i].hashMapSize();
            for(auto info : _threadEnvs[i].spillFiles) {
                if(info.isExceptionBuf)
                    numExceptionRows += info.num_rows;
                else
                    numNormalRows += info.num_rows;
            }
        }

        if(tstage->outputMode() == EndPointMode::HASHTABLE) {
            numHashRows = numNormalRows;
            numNormalRows = 0;
        }

        // @TODO: write exceptions in order...

        // for now, everything out of order
        logger().info("Starting exception resolution/slow path execution");
        for(unsigned i = 0; i < _numThreads; ++i) {
            resolveOutOfOrder(i, tstage, syms);
        }
        logger().info("Exception resolution/slow path done.");


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
        _statistics.push_back(stat);

        logger().info("Took " + std::to_string(timer.time()) + "s in total");
        return WORKER_OK;
    }

    int64_t WorkerApp::writeAllPartsToOutput(const URI& output_uri, const FileFormat& output_format, const std::unordered_map<std::string, std::string>& output_options) {

        std::stringstream ss;
        std::vector<WriteInfo> reorganized_normal_parts;
        // @TODO: same for exceptions... => need to correct numbers??
        //  @Ben Givertz will know how to do this...
        for(unsigned i = 0; i < _numThreads; ++i) {
            auto& env = _threadEnvs[i];

            // first come all the spill parts, then the remaining buffer...
            // add write info...
            // !!! stable sort necessary after this !!!
            WriteInfo info;
            for(auto spill_info : env.spillFiles) {
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
            }
        }

        // use multi-threading for S3!!! --> that makes faster upload! I.e., multi-part-upload.
        for(auto part_info : parts) {
            // is it buf or file?
            if(part_info.use_buf) {
                // write the buffer...
                // -> directly to file!
                assert(part_info.buf);
                file->write(part_info.buf, part_info.buf_size);
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
                // copy part buffer to output file!
                file->write(part_buffer, part_buffer_size);
                delete [] part_buffer;
                logger().debug("copied contents from part back to output buffer");
                logger().debug("TODO: delete file? Add to job cleanup queue?");
            }
        }

        file->close();
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
                                             bool acquireGIL) {
        using namespace std;
        assert(tstage);
        assert(pipelineObject);

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
                        auto colsToKeep = tstage->columnsToKeep();

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
                        auto text = new TextReader(userData, reinterpret_cast<codegen::cells_row_f>(pythonCellFunctor));
                        // fetch full range for now, later make this optional!
                        // text->setRange(rangeStart, rangeStart + rangeSize);
                        text->setRange(part.rangeStart, part.rangeEnd);
                        reader.reset(text);
                    } else throw std::runtime_error("unsupported input file format given");

                    // Note: ORC reader does not support parts yet... I.e., function needs to read FULL file!

                    // read assigned file...
                    logger().info("Calling read func on reader...");
                    reader->read(inputURI);
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
                                     const std::shared_ptr<TransformStage::JITSymbols>& syms) {
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
                    auto colsToKeep = tstage->columnsToKeep();

                    auto csv = new CSVReader(userData, reinterpret_cast<codegen::cells_row_f>(syms->functor),
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
                    auto text = new TextReader(userData, reinterpret_cast<codegen::cells_row_f>(syms->functor));
                    // fetch full range for now, later make this optional!
                    // text->setRange(rangeStart, rangeStart + rangeSize);
                    text->setRange(part.rangeStart, part.rangeEnd);
                    reader.reset(text);
                } else throw std::runtime_error("unsupported input file format given");

                // Note: ORC reader does not support parts yet... I.e., function needs to read FULL file!

                // read assigned file...

                reader->read(inputURI);
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
        ws.numThreads = std::max(1ul, ws.numThreads);
        return ws;
    }

    std::shared_ptr<TransformStage::JITSymbols> WorkerApp::compileTransformStage(TransformStage &stage) {

        // use cache
        auto it = _compileCache.find(stage.bitCode());
        if(it != _compileCache.end()) {
            logger().info("Using cached compiled code");
            return it->second;
        }

        try {
            // compile transform stage, depending on worker settings use optimizer before or not!
            // -> per default, don't.

            // register functions
            // Note: only normal case for now, the other stuff is not interesting yet...
            if(!stage.writeMemoryCallbackName().empty())
                _compiler->registerSymbol(stage.writeMemoryCallbackName(), writeRowCallback);
            if(!stage.exceptionCallbackName().empty())
                _compiler->registerSymbol(stage.exceptionCallbackName(), exceptRowCallback);
            if(!stage.writeFileCallbackName().empty())
                _compiler->registerSymbol(stage.writeFileCallbackName(), writeRowCallback);
            if(!stage.writeHashCallbackName().empty())
                _compiler->registerSymbol(stage.writeHashCallbackName(), writeHashCallback);

            // slow path registration, for now dummies
            if(!stage.resolveWriteCallbackName().empty())
                _compiler->registerSymbol(stage.resolveWriteCallbackName(), slowPathRowCallback);
            if(!stage.resolveExceptionCallbackName().empty())
                _compiler->registerSymbol(stage.resolveExceptionCallbackName(), slowPathExceptCallback);
            // @TODO: hashing callbacks...

            // in debug mode, validate module.
#ifndef NDEBUG
            llvm::LLVMContext ctx;
        auto mod = codegen::bitCodeToModule(ctx, stage.bitCode());
        if(!mod)
            logger().error("error parsing module");
        else {
            logger().info("parsed llvm module from bitcode, " + mod->getName().str());

            // run verify pass on module and print out any errors, before attempting to compile it
            std::string moduleErrors;
            llvm::raw_string_ostream os(moduleErrors);
            if (verifyModule(*mod, &os)) {
                os.flush();
                logger().error("could not verify module from bitcode");
                logger().error(moduleErrors);
                logger().error(core::withLineNumbers(codegen::moduleToString(*mod)));
            } else
            logger().info("module verified.");
        }
#endif

            // perform actual compilation
            // -> do not compile slow path for now.
            // -> do not register symbols, because that has been done manually above.
            auto syms = stage.compile(*_compiler, nullptr,
                                        true,
                                        false);

            // cache symbols for reuse.
            _compileCache[stage.bitCode()] = syms;

            return syms;
        } catch(std::runtime_error& e) {
            logger().error(std::string("compilation failed, details: ") + e.what());
            return nullptr;
        }
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

        // create file name (trivial:)
        auto name = "spill_except_" + std::to_string(threadNo) + "_" + std::to_string(env->spillFiles.size());
        std::string ext = ".tmp";
        auto rootURI = _settings.spillRootURI.toString().empty() ? "" : _settings.spillRootURI.toString() + "/";
        auto path = URI(rootURI + name + ext);

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

        // for speed reasons, serialize exception directly!
        size_t bufSize = 0;
        auto buf = serializeExceptionToMemory(exceptionCode, exceptionOperatorID, rowNumber, input, dataLength, &bufSize);

        if(out_buf.size() + bufSize <= out_buf.capacity()) {
            memcpy(out_buf.ptr(), buf, bufSize);
            out_buf.movePtr(bufSize);
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
        env->app->logger().warn("slowPath writeRow called, not yet implemented");
        return 0;
    }

    void WorkerApp::slowPathExceptCallback(ThreadEnv *env, int64_t exceptionCode, int64_t exceptionOperatorID,
                                           int64_t rowNumber, uint8_t *input, int64_t dataLength) {
        env->app->logger().warn("slowPath writeException called, not yet implemented");
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

    int64_t WorkerApp::resolveOutOfOrder(int threadNo, const TransformStage *stage,
                                         const std::shared_ptr<TransformStage::JITSymbols> &syms) {
        using namespace std;

        assert(threadNo >= 0 && threadNo < _numThreads);
        auto env = &_threadEnvs[threadNo];

        if(0 == env->numExceptionRows)
            return WORKER_OK; // nothing todo

        // if no compiled resolver & no interpreted resolver are present, simply return.
        if(!syms->resolveFunctor && stage->purePythonCode().empty()) {
            logger().info("No resolve code shipped. Nothing can't be resolved here.");
            return WORKER_OK;
        }

        // is exception output schema different than normal case schema? I.e., upgrade necessary?
        if(stage->normalCaseOutputSchema() != stage->outputSchema()) {
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
        for(auto info : exceptFiles) {

        }

        return WORKER_OK;
    }

    int64_t WorkerApp::resolveBuffer(int threadNo, Buffer &buf, size_t numRows, const TransformStage *stage,
                                     const std::shared_ptr<TransformStage::JITSymbols> &syms) {

        if(0 == numRows)
            return WORKER_OK;

        // fetch functors
        auto compiledResolver = syms->resolveFunctor;
        auto interpretedResolver = preparePythonPipeline(stage->purePythonCode(), stage->pythonPipelineName());

        // when both compiled resolver & interpreted resolver are invalid, this means basically that all exceptions stay...
        const auto* ptr = static_cast<const uint8_t*>(buf.buffer());
        auto env = &_threadEnvs[threadNo];
        for(unsigned i = 0; i < numRows; ++i) {
            auto rc = -1;
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
                rc = compiledResolver(env, ecRowNumber, ecCode, ecBuf, ecBufSize);
                if(rc == ecToI32(ExceptionCode::NORMALCASEVIOLATION))
                    rc = -1;
            }

            // try to resolve using interpreted resolver
            if(-1 == rc && interpretedResolver) {

                bool output_is_hashtable = stage->outputMode() == EndPointMode::HASHTABLE;
                Schema exception_input_schema = stage->inputSchema();
                Schema specialized_output_schema = stage->normalCaseOutputSchema();
                Schema general_output_schema = stage->outputSchema();

                python::lockGIL();
                auto fallbackRes = processRowUsingFallback(interpretedResolver, ecCode, ecOperatorID,
                                                           exception_input_schema,
                                                           ecBuf, ecBufSize,
                                                           specialized_output_schema,
                                                           general_output_schema,
                                                           {},
                                                           _settings.allowNumericTypeUnification,
                                                           output_is_hashtable);
                python::unlockGIL();

                // check whether output succeeded
                if(fallbackRes.code == ecToI64(ExceptionCode::SUCCESS)) {
                    // worked, take buffer result and serialize!
                    if(!fallbackRes.pyObjects.empty() || fallbackRes.generalBuf.size() > 0) {
                        throw std::runtime_error("not yet supported!");
                    }

                    // take normal buf and add to output (i.e. simple copy)!
                    env->normalBuf.provideSpace(fallbackRes.buf.size());
                    memcpy(env->normalBuf.ptr(), fallbackRes.buf.buffer(), fallbackRes.buf.size());
                    env->normalBuf.movePtr(fallbackRes.buf.size());
                    env->numNormalRows += fallbackRes.bufRowCount;
                    rc = 0; // all good, next row!
                } else {
                    // didn't work, keep as original exception or partially resolved path?
                    // -> for now keep as it is...
                    rc = -1;
                }
            }

            // else, save as exception!
            if(-1 == rc) {
                writeException(threadNo, ecCode, ecOperatorID, ecRowNumber, const_cast<uint8_t *>(ecBuf), ecBufSize);
            }

            ptr += delta;
        }

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

            // @TODO: quicker conversion from str cell?
            PyTuple_SET_ITEM(tuple, j, python::PyString_FromString(cell));

            auto cell_size = info >> 32u;
            buf += sizeof(int64_t);
        }
        return tuple;
    }

    FallbackPathResult processRowUsingFallback(PyObject* func,
                                               int64_t ecCode,
                                               int64_t ecOperatorID,
                                               const Schema& input_schema,
                                               const uint8_t* buf,
                                               size_t buf_size,
                                               const Schema& specialized_target_schema,
                                               const Schema& general_target_schema,
                                               const std::vector<PyObject*>& py_intermediates,
                                               bool allowNumericTypeUnification,
                                               bool returnAllAsPyObjects,
                                               std::ostream *err_stream) {

        assert(func && PyCallable_Check(func));

        FallbackPathResult res;

        // holds the pythonized data
        PyObject* tuple = nullptr;

        bool parse_cells = false;

        // there are different data reps for certain error codes.
        // => decode the correct object from memory & then feed it into the pipeline...
        if(ecCode == ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)) {
            // it's a string!
            tuple = fallbackTupleFromParseException(buf, buf_size);
            parse_cells = true; // need to parse cells in python mode.
        } else if(ecCode == ecToI64(ExceptionCode::NORMALCASEVIOLATION)) {
            // changed, why are these names so random here? makes no sense...
            auto row = Row::fromMemory(input_schema, buf, buf_size);

            tuple = python::rowToPython(row, true);
            parse_cells = false;
            // called below...
        } else {
            // normal case, i.e. an exception occurred somewhere.
            // --> this means if pipeline is using string as input, we should convert
            auto row = Row::fromMemory(input_schema, buf, buf_size);

            // cell source automatically takes input, i.e. no need to convert. simply get tuple from row object
            tuple = python::rowToPython(row, true);

#ifndef NDEBUG
            if(PyTuple_Check(tuple)) {
                // make sure tuple is valid...
                for(unsigned i = 0; i < PyTuple_Size(tuple); ++i) {
                    auto elemObj = PyTuple_GET_ITEM(tuple, i);
                    assert(elemObj);
                }
            }
#endif
            parse_cells = false;
        }

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


        // note: current python pipeline always expects a tuple arg. hence pack current element.
        if(PyTuple_Check(tuple) && PyTuple_Size(tuple) > 1) {
            // nothing todo...
        } else {
            auto tmp_tuple = PyTuple_New(1);
            PyTuple_SET_ITEM(tmp_tuple, 0, tuple);
            tuple = tmp_tuple;
        }

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

        auto kwargs = PyDict_New(); PyDict_SetItemString(kwargs, "parse_cells", parse_cells ? Py_True : Py_False);
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

#ifndef NDEBUG
                    // // debug printing of exception and what the reason is...
                    // // print res obj
                    // Py_XINCREF(pcr.res);
                    // std::cout<<"exception occurred while processing using python: "<<std::endl;
                    // PyObject_Print(pcr.res, stdout, 0);
                    // std::cout<<std::endl;
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

        return res;
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
}
