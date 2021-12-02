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
        auto tstage = TransformStage::from_protobuf(req.stage());

        // check number of input files
        using namespace std;
        vector<URI> inputURIs;
        vector<size_t> inputSizes;
        auto num_input_files = inputURIs.size();
        logger.debug("Found " + to_string(num_input_files) + " input URIs to process");


        return WORKER_OK;
    }

    WorkerSettings WorkerApp::settingsFromMessage(const tuplex::messages::InvocationRequest& req) {
        WorkerSettings ws;

        return ws;
    }
}