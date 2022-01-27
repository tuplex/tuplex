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

#include <AWSCommon.h>

namespace tuplex {

    struct ContainerInfo {
        bool reused; //! whether container has been reused or not
        std::string requestId; //! Lambda Request ID
        std::string uuid; //! uuid of container
        uint32_t msRemaining; //! how many milliseconds remain of this container when info was added
        uint32_t requestsServed; //! how many requests did this container already serve? (incl. the current one)
        uint64_t startTimestamp; //! when container was started
        uint64_t deadlineTimestamp; //! when container will shutdown/expire

        ContainerInfo() = default;

        ContainerInfo(const messages::ContainerInfo& info) : reused(info.reused()),
                                                             requestId(info.requestid().c_str()),
                                                             uuid(info.uuid().c_str()),
                                                             msRemaining(info.msremaining()),
                                                             requestsServed(info.requestsserved()),
                                                             startTimestamp(info.start()),
                                                             deadlineTimestamp(info.deadline()) {

        }

        inline void fill(messages::ContainerInfo* c) const {
            if(!c)
                return;

            c->set_reused(reused);
            c->set_requestid(requestId.c_str());
            c->set_uuid(uuid.c_str());
            c->set_msremaining(msRemaining);
            c->set_requestsserved(requestsServed);
            c->set_start(startTimestamp);
            c->set_deadline(deadlineTimestamp);
        }

        inline messages::ContainerInfo* to_protobuf() const {
            auto c = new messages::ContainerInfo();
            fill(c);
            return c;
        }
    };

    // externally link, i.e. in testing need dummy to make linking work!
    extern ContainerInfo getThisContainerInfo();

    struct LambdaWorkerSettings : public WorkerSettings {
        // add here specific Lambda settings
    };

    /// AWS Lambda specific Worker, inherits from base WorkerApp class
    class LambdaWorkerApp : public WorkerApp {
    public:
        LambdaWorkerApp(const LambdaWorkerSettings& ws) : WorkerApp(ws) {
        }

        tuplex::messages::InvocationResponse generateResponse();

        int globalInit() override;

    protected:
        /// put here Lambda specific constants to easily update them
        static const std::string caFile;
        static const std::string tuplexRuntimePath;
        static const bool verifySSL;

        int processMessage(const tuplex::messages::InvocationRequest& req) override;

        MessageHandler& logger() const override {
            return Logger::instance().logger("Lambda worker");
        }

        std::string _functionName;
        NetworkSettings _networkSettings;
        tuplex::AWSCredentials _credentials;
    private:

        struct Metrics {
            double global_init_time;
        };
        Metrics metrics;

        // @TODO: redesign this...
        messages::MessageType _messageType;
        std::vector<ContainerInfo> _invokedContainers;


        // self-invocation to scale-out
        struct SelfInvokeRequest {

        };

        /*!
         * invoke another Lambda function
         * @param timeout how many seconds to allow this Lambda invocation max
         * @param parts on which parts to run this Lambda invocation
         * @param original_message original message (copy will be created and params overwritten)
         * @param invocation_counts recursive invocation counts
         */
        void invokeLambda(double timeout, const std::vector<FilePart>& parts,
                          const tuplex::messages::InvocationRequest& original_message, const std::vector<size_t>& invocation_counts={});
    };

    extern std::vector<ContainerInfo> selfInvoke(const std::string& functionName,
                                               size_t count,
                                               const std::vector<size_t>& recursive_counts,
                                               size_t timeOutInMs,
                                               size_t baseDelayInMs,
                                               const tuplex::AWSCredentials& credentials,
                                               const NetworkSettings& ns,
                                               std::string tag="lambda");

    inline std::vector<ContainerInfo> selfInvoke(const std::string& functionName,
                                                 size_t count,
                                                 size_t timeOutInMs,
                                                 size_t baseDelayInMs,
                                                 const tuplex::AWSCredentials& credentials,
                                                 const NetworkSettings& ns,
                                                 std::string tag="lambda") {
        return selfInvoke(functionName, count, {}, timeOutInMs, baseDelayInMs, credentials, ns, tag);
    }
}

#endif
#endif //TUPLEX_LAMBDAWORKERAPP_H
