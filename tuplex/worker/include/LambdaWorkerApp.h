//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 12/2/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LAMBDAWORKERAPP_H
#define TUPLEX_LAMBDAWORKERAPP_H

#include "WorkerApp.h"

#ifdef BUILD_WITH_AWS

namespace tuplex {

    struct LambdaWorkerSettings : public WorkerSettings {
        // add here specific Lambda settings
    };

    /// AWS Lambda specific Worker, inherits from base WorkerApp class
    class LambdaWorkerApp : public WorkerApp {
    public:
        LambdaWorkerApp(const LambdaWorkerSettings& ws) : WorkerApp(ws) {
            initThreadEnvironments();
        }

        tuplex::messages::InvocationResponse generateResponse();

        int globalInit() override;

    protected:
        /// put here Lambda specific constants to easily update them
        static const std::string caFile;
        static const std::string tuplexRuntimePath;
        static const bool verifySSL;

        int processMessage(const tuplex::messages::InvocationRequest& req) override;

        MessageHandler& logger() const {
            return Logger::instance().logger("Lambda worker");
        }
    private:

        struct Metrics {
            double global_init_time;
        };
        Metrics metrics;
    };
}

#endif
#endif //TUPLEX_LAMBDAWORKERAPP_H
