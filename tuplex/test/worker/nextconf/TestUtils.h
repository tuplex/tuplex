//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2023, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on  3/5/2023                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifndef TEST_UTILS_HEADER_
#define TEST_UTILS_HEADER_
#include <gtest/gtest.h>
#include <unistd.h>
#include <Utils.h>
#include <Timer.h>
#include <physical/codegen/StageBuilder.h>
#include <logical/FileInputOperator.h>
#include <logical/FileOutputOperator.h>
#include <logical/MapOperator.h>
#include <google/protobuf/util/json_util.h>
#include <ee/worker/WorkerApp.h>
#include <ee/aws/LambdaWorkerApp.h>

#include <boost/filesystem.hpp>

#ifdef BUILD_WITH_AWS
#include <ee/aws/AWSLambdaBackend.h>
#endif

#ifndef S3_TEST_BUCKET
// define dummy to compile
#ifdef SKIP_AWS_TESTS
#define S3_TEST_BUCKET "tuplex-test"
#endif

#warning "need S3 Test bucket to run these tests"
#endif


//   constexpr double nan = std::numeric_limits<double>::quiet_NaN();

#include <CSVUtils.h>
#include <procinfo.h>
#include <FileHelperUtils.h>
#include <physical/codegen/StagePlanner.h>
#include <logical/LogicalOptimizer.h>

#include "PerfEvent.hpp"
#include "logical/LogicalPlan.h"
#include "physical/execution/csvmonkey.h"
#include <boost/filesystem/operations.hpp>

#include <spdlog/sinks/ostream_sink.h>

namespace tuplex {
    class TuplexTest : public ::testing::Test {
    protected:
        std::string testName;
        std::string scratchDir;

        void SetUp() override {
            testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
            auto user = tuplex::getUserName();
            if(user.empty()) {
                std::cerr<<"could not retrieve user name, setting to user"<<std::endl;
                user = "user";
            }
            scratchDir = "/tmp/" + user + "/" + testName;
        }

        inline void remove_temp_files() {
            tuplex::Timer timer;
            boost::filesystem::remove_all(scratchDir.c_str());
            std::cout<<"removed temp files in "<<timer.time()<<"s"<<std::endl;
        }

        ~TuplexTest() override {
            remove_temp_files();
        }

        inline tuplex::ContextOptions testOptions() {
            using namespace tuplex;
            ContextOptions co = ContextOptions::defaults();
            co.set("tuplex.executorCount", "4");
            co.set("tuplex.partitionSize", "512KB");
            co.set("tuplex.executorMemory", "8MB");
            co.set("tuplex.useLLVMOptimizer", "true");
            co.set("tuplex.allowUndefinedBehavior", "false");
            co.set("tuplex.webui.enable", "false");
            co.set("tuplex.scratchDir", "file://" + scratchDir);
#ifdef BUILD_FOR_CI
            co.set("tuplex.aws.httpThreadCount", "0");
#else
            co.set("tuplex.aws.httpThreadCount", "1");
#endif
            return co;
        }

        inline tuplex::ContextOptions microTestOptions() {
            using namespace tuplex;
            ContextOptions co = ContextOptions::defaults();
            co.set("tuplex.executorCount", "4");
            co.set("tuplex.partitionSize", "256B");
            co.set("tuplex.executorMemory", "4MB");
            co.set("tuplex.useLLVMOptimizer", "true");
//    co.set("tuplex.useLLVMOptimizer", "false");
            co.set("tuplex.allowUndefinedBehavior", "false");
            co.set("tuplex.webui.enable", "false");
            co.set("tuplex.optimizer.mergeExceptionsInOrder", "true"); // force exception resolution for single stages to occur in order
            co.set("tuplex.scratchDir", "file://" + scratchDir);

            // disable schema pushdown
            co.set("tuplex.optimizer.selectionPushdown", "true");

#ifdef BUILD_FOR_CI
            co.set("tuplex.aws.httpThreadCount", "0");
#else
            co.set("tuplex.aws.httpThreadCount", "1");
#endif
            return co;
        }

#ifdef BUILD_WITH_AWS
        inline tuplex::ContextOptions microLambdaOptions() {
            auto co = microTestOptions();
            co.set("tuplex.backend", "lambda");

            // enable requester pays
            co.set("tuplex.aws.requesterPay", "true");
#ifndef SKIP_AWS_TESTS
#ifndef S3_TEST_BUCKET
#error "no S3 test bucket specified, can't really run AWS queries..."
#endif
            // scratch dir
            co.set("tuplex.aws.scratchDir", std::string("s3://") + S3_TEST_BUCKET + "/.tuplex-cache");
#else
            co.set("tuplex.aws.scratchDir", std::string("s3://tuplex-test-") + tuplex::getUserName() + "/.tuplex-cache");
#endif
#ifdef BUILD_FOR_CI
            co.set("tuplex.aws.httpThreadCount", "1");
#else
            co.set("tuplex.aws.httpThreadCount", "4");
#endif

            return co;
        }
#endif
    };

// helper class to not have to write always the python interpreter startup stuff
// need for these tests a running python interpreter, so spin it up
    class PyTest : public TuplexTest {
    protected:
        PyThreadState *saveState;
        std::stringstream logStream;

        void SetUp() override {
            TuplexTest::SetUp();
            // reset global static variables, i.e. whether to use UDF compilation or not!
            tuplex::UDF::enableCompilation();

            // init logger to write to both stream as well as stdout
            // ==> searching the stream can be used to validate err Messages
            Logger::init({std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>(),
                          std::make_shared<spdlog::sinks::ostream_sink_mt>(logStream)});

            python::initInterpreter();
            // release GIL
            python::unlockGIL();
        }

        void TearDown() override {
            python::lockGIL();
            // important to get GIL for this
            python::closeInterpreter();

            // release runtime memory
            tuplex::runtime::releaseRunTimeMemory();

            // remove all loggers ==> note: this crashed because of multiple threads not being done yet...
            // call only AFTER all threads/threadpool is terminated from Context/LocalBackend/LocalEngine...
            Logger::instance().reset();

            tuplex::UDF::enableCompilation(); // reset

            // check whether exceptions work, LLVM9 has a bug which screws up C++ exception handling in ORCv2 APIs
            try {
                throw std::exception();
            } catch(...) {
                std::cout << "test done." << std::endl;
            }
        }
    };
}

#endif