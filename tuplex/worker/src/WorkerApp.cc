//
// Created by Leonhard Spiegelberg on 11/22/21.
//

#include <WorkerApp.h>

namespace tuplex {

    bool WorkerApp::reinitialize(const WorkerSettings &settings) {
        _settings = settings;

        return true;
    }

    void WorkerApp::shutdown() {

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



        return WORKER_OK;
    }

    WorkerSettings WorkerApp::settingsFromMessage(const tuplex::messages::InvocationRequest& req) {
        WorkerSettings ws;

        return ws;
    }
}