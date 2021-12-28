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

namespace tuplex {

    int WorkerApp::globalInit() {
        auto& logger = Logger::instance().defaultLogger();

        // skip if already initialized.
        if(_globallyInitialized)
            return WORKER_OK;

        // runtime library path
        auto runtime_path = ContextOptions::defaults().RUNTIME_LIBRARY().toPath();
        std::string python_home_dir = python::find_stdlib_location();

        if(python_home_dir.empty()) {
            logger.error("Could not detect python stdlib location");
            return WORKER_ERROR_NO_PYTHON_HOME;
        }

        NetworkSettings ns;
        ns.verifySSL = false;

#ifdef BUILD_WITH_AWS
        Aws::InitAPI(_aws_options);
#endif

        if(!runtime::init(runtime_path)) {
            logger.error("runtime specified as " + std::string(runtime_path) + " could not be found.");
            return WORKER_ERROR_NO_TUPLEX_RUNTIME;
        }
        _compiler = std::make_shared<JITCompiler>();

        // // init python home dir to point to directory where stdlib files are installed, cf.
        // // https://www.python.org/dev/peps/pep-0405/ for meaning of it. Per default, just use default behavior.
        // python::python_home_setup(python_home_dir);

        logger.info("Initializing python interpreter version " + python::python_version(true, true));
        python::initInterpreter();
        python::unlockGIL();

        _globallyInitialized = true;
        return WORKER_OK;
    }

    bool WorkerApp::reinitialize(const WorkerSettings &settings) {
        _settings = settings;

        // general init here...
        // compiler already active? Else init
        globalInit();

        // initialize thread buffers (which get passed to functions)
        _numThreads = std::max(1ul, settings.numThreads);
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
            _threadEnvs[i].normalBuf.provideSpace(settings.normalBufferSize);
            _threadEnvs[i].exceptionBuf.provideSpace(settings.exceptionBufferSize);
            // hashmap init?
            // @TODO
        }

        return true;
    }

    void WorkerApp::shutdown() {
        if(python::isInterpreterRunning()) {
            python::lockGIL();
            python::closeInterpreter();
        }

        runtime::freeRunTimeMemory();

#ifdef BUILD_WITH_AWS
        Aws::ShutdownAPI(_aws_options);
#endif
    }

    int WorkerApp::messageLoop() {
        return 0;
    }

    int WorkerApp::processJSONMessage(const std::string &message) {
        auto& logger = this->logger();

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

        // check number of input files
        using namespace std;
        vector<URI> inputURIs;
        vector<size_t> inputSizes;
        auto num_input_files = inputURIs.size();
        logger.debug("Found " + to_string(num_input_files) + " input URIs to process");

        return processMessage(req);

//        auto response = executeTransformTask(tstage);

        return WORKER_OK;
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

    int WorkerApp::processMessage(const tuplex::messages::InvocationRequest& req) {

        // only transform stage yet supported, in the future support other stages as well!
        auto tstage = TransformStage::from_protobuf(req.stage());

        // check what type of message it is & then start processing it.
        auto syms = compileTransformStage(*tstage);
        if(!syms)
            return WORKER_ERROR_COMPILATION_FAILED;

        // init stage, abort on error
        auto rc = initTransformStage(tstage->initData(), syms);
        if(rc != WORKER_OK)
            return rc;

        // input uris
        std::vector<URI> input_uris;
        std::vector<size_t> input_sizes;
        for(auto path : req.inputuris())
            input_uris.emplace_back(URI(path));
        for(auto file_size : req.inputsizes())
            input_sizes.emplace_back(file_size);

        URI outputURI(req.outputuri());

        // process data (single-threaded or via thread pool!)
        if(_numThreads <= 1) {
            runtime::setRunTimeMemory(_settings.runTimeMemory, _settings.runTimeMemoryDefaultBlockSize);

            // single-threaded
            for(unsigned i = 0; i < input_uris.size(); ++i) {
                FilePart fp;
                fp.rangeStart = 0;
                fp.rangeEnd = 0;
                fp.uri = input_uris[i];
                fp.partNo = i;
                processSource(0, tstage->fileInputOperatorID(), fp, tstage, syms);
                logger().debug("processed file " + std::to_string(i + 1) + "/" + std::to_string(input_uris.size()));
            }

            runtime::releaseRunTimeMemory();
        } else {
            // multi-threaded
            // -> split into parts according to size and distribute between threads!
            auto parts = splitIntoEqualParts(_numThreads, input_uris, input_sizes);
            auto num_parts = 0;
            for(auto part : parts)
                num_parts += part.size();
            logger().debug("split files into " + pluralize(num_parts, "part"));

            // launch threads & process in each assigned parts
            std::vector<std::thread> threads;
            for(int i = 1; i < _numThreads; ++i) {

                runtime::setRunTimeMemory(_settings.runTimeMemory, _settings.runTimeMemoryDefaultBlockSize);

                threads.emplace_back(std::thread([this, tstage, syms](int threadNo, const std::vector<FilePart>& parts) {
                    logger().debug("thread (" + std::to_string(threadNo) + ") started.");
                    for(auto part : parts) {
                        logger().debug("thread (" + std::to_string(threadNo) + ") processing part");
                        processSource(threadNo, tstage->fileInputOperatorID(), part, tstage, syms);
                    }
                    logger().debug("thread (" + std::to_string(threadNo) + ") done.");

                    // release here runtime memory...
                    runtime::releaseRunTimeMemory();

                }, i, parts[i]));
            }

            // process with this thread data as well!
            logger().debug("thread (main) processing started");
            runtime::setRunTimeMemory(_settings.runTimeMemory, _settings.runTimeMemoryDefaultBlockSize);
            for(auto part : parts[0]) {
                logger().debug("thread (main) processing part");
                processSource(0, tstage->fileInputOperatorID(), part, tstage, syms);
            }
            logger().debug("thread (main) processing done, waiting for others to finish.");
            // release here runtime memory...
            runtime::releaseRunTimeMemory();

            // wait for all threads to finish (what about interrupt signal?)
            for(auto& thread : threads)
                thread.join();
            logger().debug("All threads joined, processing done.");
        }

        // file reorganizing part...
        //   //        auto outputURI = getNextOutputURI(threadNo, tstage->outputURI(),
        //        //                                          strEndsWith(tstage->outputURI().toPath(), "/"),
        //        //                                          defaultFileExtension(tstage->outputFormat()));
        //        auto outputURI = tstage->outputURI();

        // save buffers now if desired (i.e. data exchange)
        // => need order for this from original parts + spill parts
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
            return a.partNo < b.partNo;
        });

        // debug: print out parts
#ifndef NDEBUG
        for(auto info : reorganized_normal_parts) {
            ss<<"Part ("<<info.partNo<<") produced by thread "<<info.threadNo<<" ";
            if(info.use_buf)
                ss<<" (main memory, "<<info.buf_size<<" bytes, "<<pluralize(info.num_rows, "row")<<")\n";
            else
                ss<<" (spilled to "<<info.spill_info.path<<")\n";
        }
        logger().debug("Result overview: \n" + ss.str());
#endif

        // no write everything to final output_uri out in order!
        logger().info("Writing data to " + outputURI.toString());
        writePartsToFile(outputURI, tstage->outputFormat(), reorganized_normal_parts, tstage);
        logger().info("Data fully materialized");

        // @TODO: write exceptions in order...


        // release stage
        rc = releaseTransformStage(syms);
        if(rc != WORKER_OK)
            return rc;

        return WORKER_OK;
    }

    void WorkerApp::writePartsToFile(const URI &outputURI, const FileFormat &fmt,
                                     const std::vector<WriteInfo> &parts,
                                     const TransformStage *stage) {
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
            auto outOptions = stage->outputOptions();
            bool writeHeader = stringToBool(get_or(outOptions, "header", "false"));
            if(writeHeader) {
                // fetch special var csvHeader
                auto headerLine = outOptions["csvHeader"];
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

                // read contents in...
                assert(part_file->size() == part_info.spill_info.file_size);
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

    int64_t WorkerApp::processSource(int threadNo, int64_t inputNodeID, const FilePart& part, const TransformStage *tstage,
                                     const std::shared_ptr<TransformStage::JITSymbols>& syms) {
        using namespace std;

        // couple checks
        assert(tstage);
        assert(syms->functor);
        assert(threadNo >= 0 && threadNo < _numThreads);

        auto inputURI = part.uri;


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
        stringstream ss;
        ss<<"Task resulted in: "<<sizeToMemString(env->normalBuf.size())<<" (normal)  "
          <<sizeToMemString(env->exceptionBuf.size())<<" (except)  "
          <<sizeToMemString(env->hashMapSize())<<" (hash)";

        // lazy write each threadEnv back to S3!
        // @TODO: file reorg at the end???
        // --> need to specify that!
        // --> should files be partitioned after certain things??

        logger().info("Task done!");

        return WORKER_OK;
    }

    void WorkerApp::writeBufferToFile(const URI& outputURI,
                                      const FileFormat& fmt,
                                      const uint8_t* buf,
                                      const size_t buf_size,
                                      const size_t num_rows,
                                      const TransformStage* tstage) {

        logger().info("writing " + sizeToMemString(buf_size) + " to " + outputURI.toPath());
        // write first the # rows, then the data
        auto vfs = VirtualFileSystem::fromURI(outputURI);
        auto mode = VirtualFileMode::VFS_OVERWRITE | VirtualFileMode::VFS_WRITE;
        if(tstage->outputFormat() == FileFormat::OUTFMT_CSV || tstage->outputFormat() == FileFormat::OUTFMT_TEXT)
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
            auto outOptions = tstage->outputOptions();
            bool writeHeader = stringToBool(get_or(outOptions, "header", "false"));
            if(writeHeader) {
                // fetch special var csvHeader
                auto headerLine = outOptions["csvHeader"];
                header_length = headerLine.length();
                header = new uint8_t[header_length+1];
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
            file->close();
            throw std::runtime_error("unknown file format " + std::to_string(static_cast<int64_t>(fmt)) + " found.");
        }


        // write buffer & close file
        file->write(buf, buf_size);
        file->close();

        // ORC?
        if(FileFormat::OUTFMT_ORC == fmt) {
            // @TODO:
        }

    }

//    tuplex::messages::InvocationResponse WorkerApp::executeTransformTask(const TransformStage* tstage);

    WorkerSettings WorkerApp::settingsFromMessage(const tuplex::messages::InvocationRequest& req) {
        WorkerSettings ws;

        // decode from message if certain settings are present
        if(req.settings().has_numthreads())
            ws.numThreads = req.settings().numthreads();
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

        return ws;
    }

    std::shared_ptr<TransformStage::JITSymbols> WorkerApp::compileTransformStage(TransformStage &stage) {

        // use cache
        auto it = _compileCache.find(stage.bitCode());
        if(it != _compileCache.end()) {
            logger().debug("Using cached compiled code");
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
#ifdef NDEBUG
            llvm::LLVMContext ctx;
        auto mod = codegen::bitCodeToModule(ctx, stage.bitCode());
        if(!mod)
            logger.error("error parsing module");
        else {
            logger.info("parsed llvm module from bitcode, " + mod->getName().str());

            // run verify pass on module and print out any errors, before attempting to compile it
            std::string moduleErrors;
            llvm::raw_string_ostream os(moduleErrors);
            if (verifyModule(*mod, &os)) {
                os.flush();
                logger.error("could not verify module from bitcode");
                logger.error(moduleErrors);
                logger.error(core::withLineNumbers(codegen::moduleToString(*mod)));
            } else
            logger.info("module verified.");

        }
#endif

            // perform actual compilation
            // -> do not compile slow path for now.
            // -> do not register symbols, because that has been done manually above.
            auto syms = stage.compile(*_compiler, nullptr, true, false);

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
                writeRow(threadNo, buf, bufSize); // call again, do not return. Instead spill for sure after the resize...
                logger().debug("row size exceeded internal buffer size, forced resize.");
            }

            // buffer is full, save to spill out path
            spillNormalBuffer(threadNo);
        }

        return 0;
    }

    void WorkerApp::spillNormalBuffer(size_t threadNo) {

        assert(_threadEnvs);
        assert(threadNo < _numThreads);

        // spill (in Tuplex format!), i.e. write first #rows, #bytes written & the rest!

        // create file name (trivial:)
        auto name = "spill_normal_" + std::to_string(threadNo) + "_" + std::to_string(_threadEnvs->spillFiles.size());

        auto path = URI(_settings.spillRootURI.toString() + "/" + name);

        // open & write
        auto vfs = VirtualFileSystem::fromURI(path);
        auto vf = vfs.open_file(path, VirtualFileMode::VFS_OVERWRITE);
        if(!vf) {
            logger().error("Failed to spill buffer to path " + path.toString());
        } else {
            int64_t tmp = _threadEnvs[threadNo].numNormalRows;
            vf->write(&tmp, sizeof(int64_t));
            tmp = _threadEnvs[threadNo].normalBuf.size();
            vf->write(&tmp, sizeof(int64_t));
            vf->write(_threadEnvs[threadNo].normalBuf.buffer(), _threadEnvs[threadNo].normalBuf.size());
            logger().info("Spilled " + sizeToMemString(_threadEnvs[threadNo].normalBuf.size() + 2 * sizeof(int64_t)) + " to  " + path.toString());

            SpillInfo info;
            info.path = path.toString();
            info.isExceptionBuf = false;
            info.num_rows = _threadEnvs[threadNo].numNormalRows;
            info.originalPartNo = _threadEnvs[threadNo].normalOriginalPartNo;
            _threadEnvs[threadNo].spillFiles.push_back(info);
        }

        // reset
        _threadEnvs[threadNo].normalBuf.reset();
        _threadEnvs[threadNo].numNormalRows = 0;
        _threadEnvs[threadNo].normalOriginalPartNo = 0;
    }

    void WorkerApp::spillExceptionBuffer(size_t threadNo) {

    }

    void WorkerApp::spillHashMap(size_t threadNo) {

    }

    void WorkerApp::writeException(size_t threadNo, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t *input,
                              int64_t dataLength) {
        // same here for exception buffers...
        logger().info("Thread " + std::to_string(threadNo) + " produced Exception " + exceptionCodeToString(i64ToEC(exceptionCode)));
    }

    void WorkerApp::writeHashedRow(size_t threadNo, const uint8_t *key, int64_t key_size, const uint8_t *bucket, int64_t bucket_size) {

    }

    // static helper functions/callbacks
    int64_t WorkerApp::writeRowCallback(ThreadEnv* env, const uint8_t *buf, int64_t bufSize) {
        assert(env);
        return env->app->writeRow(env->threadNo, buf, bufSize);
    }
    void WorkerApp::writeHashCallback(ThreadEnv* env, const uint8_t *key, int64_t key_size, const uint8_t *bucket, int64_t bucket_size) {
        assert(env);
        env->app->writeHashedRow(env->threadNo, key, key_size, bucket, bucket_size);
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


    extern std::vector<std::vector<FilePart>> splitIntoEqualParts(size_t numThreads,
                                                                  const std::vector<URI>& uris,
                                                                  const std::vector<size_t>& file_sizes) {
        using namespace std;

        assert(uris.size() == file_sizes.size());

        size_t partNo = 0;

        auto vv = vector<vector<FilePart>>(numThreads, vector<FilePart>{});


        // check how many bytes each vector should get
        size_t totalBytes = 0;
        for(auto fs : file_sizes)
            totalBytes += fs;

        auto bytesPerThread = totalBytes / numThreads;

        // set bytes per thread to a minimum
        // bytesPerThread = std::max(bytesPerThread, 256 * 1024ul); // min 256KB per thread...

        assert(bytesPerThread != 0);

        // do not process empty files...
        size_t curThreadSize = 0;
        unsigned curThread = 0;
        for(unsigned i = 0; i < std::min(uris.size(), file_sizes.size()); ++i) {
            // skip empty files...
            if(0 == file_sizes[i])
                continue;

            // does current thread get the whole file?
            if(curThreadSize + file_sizes[i] <= bytesPerThread && file_sizes[i] > 0) {
                FilePart fp;
                fp.uri = uris[i];
                fp.rangeStart = 0;
                fp.rangeEnd = 0; // full file.
                fp.partNo = partNo++;
                vv[curThread].emplace_back(fp);
                curThreadSize += file_sizes[i];
            } else {
                // no, current thread gets part of the file only
                // rest of file needs to get distributed among other threads!

                // split into parts & inc curThread
                size_t remaining_bytes = file_sizes[i];
                size_t cur_start = 0;

                size_t bytes_this_thread_gets = bytesPerThread > curThreadSize ? bytesPerThread - curThreadSize : 0;

                if(bytes_this_thread_gets > 0) {
                    // split into part (depending on how many bytes remain)
                    FilePart fp;
                    fp.uri = uris[i];
                    fp.rangeStart = cur_start;
                    fp.rangeEnd = std::min(bytes_this_thread_gets, remaining_bytes);
                    fp.partNo = partNo++;
                    cur_start += fp.rangeEnd - fp.rangeStart;
                    vv[curThread].emplace_back(fp);
                }

                // go to next thread
                // check how many bytes are left, distribute large file...
                curThread = (curThread + 1) % numThreads;
                curThreadSize = 0;

                // split into parts...
                while(cur_start < remaining_bytes) {

                    bytes_this_thread_gets = remaining_bytes - cur_start;

                    // now current thread gets the current file (or a part of it)
                    if(curThreadSize + bytes_this_thread_gets <= bytesPerThread && bytes_this_thread_gets > 0) {
                        FilePart fp;
                        fp.uri = uris[i];
                        fp.rangeStart = cur_start;
                        fp.rangeEnd = cur_start + bytes_this_thread_gets;
                        fp.partNo = partNo++;
                        vv[curThread].emplace_back(fp);
                        curThreadSize += fp.rangeEnd - fp.rangeStart;
                        cur_start += fp.rangeEnd - fp.rangeStart;
                    } else {
                        // thread only gets a part, inc thread!
                        bytes_this_thread_gets = std::min(bytes_this_thread_gets, bytesPerThread);
                        FilePart fp;
                        fp.uri = uris[i];
                        fp.rangeStart = cur_start;
                        fp.rangeEnd = cur_start + bytes_this_thread_gets;
                        fp.partNo = partNo++;
                        vv[curThread].emplace_back(fp);
                        curThreadSize += bytes_this_thread_gets;
                        cur_start += bytes_this_thread_gets;

                        // next thread!
                        curThread = (curThread + 1) % numThreads;
                        curThreadSize = 0;
                    }
                }
            }
        }

        return vv;
    }
}