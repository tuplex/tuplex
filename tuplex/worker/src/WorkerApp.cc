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

namespace tuplex {

    int WorkerApp::globalInit() {
        auto& logger = Logger::instance().defaultLogger();

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
            python::unlockGIL();
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

    int WorkerApp::processMessage(const tuplex::messages::InvocationRequest& req) {
        // @TODO:
        return WORKER_OK;
    }

//    tuplex::messages::InvocationResponse WorkerApp::executeTransformTask(const TransformStage* tstage);

    WorkerSettings WorkerApp::settingsFromMessage(const tuplex::messages::InvocationRequest& req) {
        WorkerSettings ws;

        return ws;
    }

    std::shared_ptr<TransformStage::JITSymbols> WorkerApp::compileTransformStage(TransformStage &stage) {
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

        return syms;
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
            _threadEnvs[threadNo].spillFiles.push_back(path.toString());
        }

        // reset
        _threadEnvs[threadNo].normalBuf.reset();
        _threadEnvs[threadNo].numNormalRows = 0;
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
}