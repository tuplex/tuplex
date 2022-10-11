//
// Created by Leonhard Spiegelberg on 9/20/22.
//

#ifndef TUPLEX_HYPERUTILS_H
#define TUPLEX_HYPERUTILS_H

#include "gtest/gtest.h"
#ifndef GTEST_IS_THREADSAFE
#error "need threadsafe version of google test"
#else
#if (GTEST_IS_THREADSAFE != 1)
#error "need threadsafe version of google test"
#endif
#endif


#include <Row.h>
#include <UDF.h>
#include <ee/local/Executor.h>
#include <ContextOptions.h>
#include <spdlog/sinks/ostream_sink.h>
#include <ee/local/LocalEngine.h>
#include <physical/codegen/CodeDefs.h>
#include <jit/RuntimeInterface.h>
#include <symbols/ClosureEnvironment.h>

#ifdef BUILD_WITH_AWS
#include <ee/aws/AWSLambdaBackend.h>
#endif

#include <boost/filesystem/operations.hpp>

inline void compareStrArrays(std::vector<std::string> arr_A, std::vector<std::string> arr_B, bool ignore_order) {
    if(ignore_order) {
        std::sort(arr_A.begin(), arr_A.end());
        std::sort(arr_B.begin(), arr_B.end());
    }

    for (int i = 0; i < std::min(arr_A.size(), arr_B.size()); ++i) {
        EXPECT_EQ(arr_A[i], arr_B[i]);
    }
    ASSERT_EQ(arr_A.size(), arr_B.size());
}

class HyperTest : public ::testing::Test {
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

    ~HyperTest() override {
        remove_temp_files();
    }
};

class HyperPyTest : public HyperTest {
protected:
    PyThreadState *saveState;
    std::stringstream logStream;

    void SetUp() override {
        HyperTest::SetUp();
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
};

#endif //TUPLEX_HYPERUTILS_H
