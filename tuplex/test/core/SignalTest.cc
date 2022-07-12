//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <csignal>

#include "TestUtils.h"
#include "FullPipelines.h"

class SigTest : public PyTest {};


// @TODO: in the shell, Ctrl+C should work well too...
// i.e. just aborting the current line. Not destroying everything!
// check https://android.googlesource.com/platform/bionic/+/master/tests/signal_test.cpp on how to test this...
TEST_F(SigTest, FlightInterrupt) {
    // test pipeline over several context configurations
    using namespace tuplex;
    using namespace std;
    std::string bts_path="../resources/pipelines/flights/flights_on_time_performance_2019_01.10k-sample.csv";
    std::string carrier_path="../resources/pipelines/flights/L_CARRIER_HISTORY.csv";
    std::string airport_path="../resources/pipelines/flights/GlobalAirportDatabase.txt";

    // for reference deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.csv.selectionPushdown", "false");
    opt_ref.set("tuplex.optimizer.generateParser", "false");
    opt_ref.set("tuplex.optimizer.mergeExceptionsInOrder", "false");

    // Tuplex thread
    Context c_ref(opt_ref);
    auto& ds = flightPipeline(c_ref, bts_path, carrier_path, airport_path, false);

    // Note: Could run this twice: Once to detect the delay time & then with a signal interrupt.
    // launch thread to issue signal in 250ms
    std::thread t([]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // use 500ms per default.
        std::raise(SIGINT);
    });
    auto ref = pipelineAsStrs(ds);
    t.join();
}