//
// Created by Leonhard Spiegelberg on 11/22/21.
//

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

        runtime::init(runtime_path);
        _compiler = std::make_shared<JITCompiler>();

        // init python home dir.
        python::python_home_setup(python_home_dir);
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
        python::closeInterpreter();
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