//
// Created by Leonhard Spiegelberg on 11/22/21.
//

#ifndef TUPLEX_WORKERAPP_H
#define TUPLEX_WORKERAPP_H




#include <string>

// error codes
#define WORKER_OK 0
#define WORKER_ERROR_INVALID_JSON_MESSAGE 100

// protobuf
#include <Lambda.pb.h>
#include <physical/TransformStage.h>
#include <physical/CSVReader.h>
#include <physical/TextReader.h>
#include <google/protobuf/util/json_util.h>

namespace tuplex {

    /// settings to use to initialize a worker application. Helpful to tune depending on
    /// deployment target.
    struct WorkerSettings {
        // Settings:
        // -> thread-pool, how many threads to use for tasks!
        // -> local file cache -> how much main memory, which disk dir, how much memory available on disk dir
        // executor in total how much memory available to use
        //

        bool operator == (const WorkerSettings& other) const = default;

        bool operator != (const WorkerSettings& other) const {
            return !(*this == other);
        }
    };

    /// main class to represent a running worker application
    /// i.e., this is an applicaton which performs some task and returns it in some way
    /// exchange could be via request response, files, shared memory? etc.
    class WorkerApp {
    public:
        WorkerApp() = delete;
        WorkerApp(const WorkerApp& other) =  delete;

        // create WorkerApp from settings
        WorkerApp(const WorkerSettings& settings) { reinitialize(settings); }

        bool reinitialize(const WorkerSettings& settings);

        int messageLoop();

        /*!
         * processes a single message given as JSON
         * @param message JSON string
         * @return 0 if successful or error code depending on circumstances
         */
        int processJSONMessage(const std::string& message);

        void shutdown();

    protected:
        WorkerSettings settingsFromMessage(const tuplex::messages::InvocationRequest& req);

    private:
        WorkerSettings _settings;
    };
}

#endif //TUPLEX_WORKERAPP_H
