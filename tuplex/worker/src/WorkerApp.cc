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

}