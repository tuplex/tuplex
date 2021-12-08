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
        auto runtime_path = "tuplex-runtime.so";
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
        auto& logger = Logger::instance().defaultLogger();

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

    int64_t WorkerApp::writeRow(const uint8_t *buf, int64_t bufSize) {
        return 0;
    }

    void WorkerApp::writeException(int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t *input,
                              int64_t dataLength) {

    }

    void WorkerApp::writeHashedRow(const uint8_t *key, int64_t key_size, const uint8_t *bucket, int64_t bucket_size) {

    }

    // static helper functions/callbacks
    int64_t WorkerApp::writeRowCallback(WorkerApp *app, const uint8_t *buf, int64_t bufSize) {
        assert(app);
        return app->writeRow(buf, bufSize);
    }
    void WorkerApp::writeHashCallback(WorkerApp *app, const uint8_t *key, int64_t key_size, const uint8_t *bucket, int64_t bucket_size) {
        assert(app);
        app->writeHashedRow(key, key_size, bucket, bucket_size);
    }
    void WorkerApp::exceptRowCallback(WorkerApp *app, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t *input, int64_t dataLength) {
        assert(app);
        app->writeException(exceptionCode, exceptionOperatorID, rowNumber, input, dataLength);
    }
}