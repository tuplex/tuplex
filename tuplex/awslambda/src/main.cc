//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "main.h"
#include "sighandler.h"
#include <nlohmann/json.hpp>
#include <google/protobuf/util/json_util.h>


using namespace nlohmann;
using namespace aws::lambda_runtime;

static bool g_reused = false;
static tuplex::uniqueid_t g_id = tuplex::getUniqueID();
bool container_reused() { return g_reused; }
extern tuplex::uniqueid_t container_id() { return g_id; }


std::string proto_to_json(const tuplex::messages::InvocationResponse& r) {
    std::string json_buf;
    google::protobuf::util::MessageToJsonString(r, &json_buf);
    return json_buf;
}

static invocation_response lambda_handler(invocation_request const& req) {

    // for signals, do jmp_buf
    // why is this important?
    // ==> because ELSE we get billed for the additional lambda retries -.-
    // DON'T USE invocation_response::failure because this leads to retries...
    int sig;
    if((sig = setjmp(sig_buf)) == 0) {
        // normal code
        // this is a cleaner approach, then the one before using Cxx exceptions
        try {
            // always return success...
            auto result = lambda_main(req);
            // do error handling in master...
            g_reused = true;
            return invocation_response::success(proto_to_json(result),
                                                "application/json");
        } catch(const std::exception& e) {
            g_reused = true;
            return invocation_response::success(proto_to_json(make_exception(std::string("lambda_handler caught an exception! ") + e.what())),
                                                "application/json");
        } catch(...) {
            g_reused = true;
            return invocation_response::success(proto_to_json(make_exception("Unknown exception encountered in catch(...) block.")),
                                                "application/json");
        }
    } else {
        // special exception code
        g_reused = true;
        return invocation_response::success(proto_to_json(make_exception("SIGSEV encountered")),
                                            "application/json");
    }
}

// main function which setups error handling & invocation of custom lambda function
int main() {

    // TODO: determine whether this is needed for the new AWS C++ Runtime
    using namespace aws::lambda_runtime;
    // install sigsev handler to throw C++ exception which is caught in handler...
    struct sigaction sigact;
    sigact.sa_sigaction = sigsev_handler;
    sigact.sa_flags = SA_RESTART | SA_SIGINFO;


    // set sigabort too
    sigaction(SIGABRT, &sigact, nullptr);

    global_init();
    reset_executor_setup();

    // signal(SIGSEGV, sigsev_handler);
    if(sigaction(SIGSEGV, &sigact, nullptr) != 0) {

        run_handler([](invocation_request const& req) {
            return invocation_response::success(proto_to_json(make_exception("could not add sigsev handler")),
                                                "application/json");
        });

    } else {

        // Lambda basically invokes multiple times the handler, hence can use this to cache results
        // i.e. compiled code...

        // init here globally things
        // idea is to use a global class, LambdaApplication
        // which has init, shutdown and invocation request...
        // ==> need this too for correct stats & Co


        // i.e. create the following way a class:
        // Constructor setups apis, compiler, runtime etc.
        // then, use a LRU cache (i.e. when putting a new unseen ir code function in)
        // for compiled functions
        // this avoids costly recompilation of functions!

        // also, don't forget to reset stats counters for each invocation

        run_handler(lambda_handler);
    }

    // flush buffers
    std::cout.flush();
    std::cerr.flush();

    global_cleanup();

    return 0;
}