//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TESTUTILS_H
#define TUPLEX_TESTUTILS_H

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
#include <Executor.h>
#include <ContextOptions.h>
#include <spdlog/sinks/ostream_sink.h>
#include <LocalEngine.h>
#include <physical/CodeDefs.h>
#include <RuntimeInterface.h>
#include <ClosureEnvironment.h>
#include <Environment.h>

#ifdef BUILD_WITH_AWS
#include <ee/aws/AWSLambdaBackend.h>
#endif

#include <boost/filesystem/operations.hpp>

// declare dummy S3 Test Bucket if skip tests is enabled
#ifdef SKIP_AWS_TESTS
#ifndef S3_TEST_BUCKET
#define S3_TEST_BUCKET "tuplex-test"
#endif
#endif


// helper functions to faciliate test writing
extern tuplex::Row execRow(const tuplex::Row& input, tuplex::UDF udf=tuplex::UDF("lambda x: x"));

extern std::unique_ptr<tuplex::Executor> testExecutor();

inline void printRows(const std::vector<tuplex::Row>& v) {
    for(auto r : v)
        std::cout<<"type: "<<r.getRowType().desc()<<" content: "<<r.toPythonString()<<std::endl;
}

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

class TuplexTest : public ::testing::Test {
protected:
    std::string testName;
    std::string scratchDir;

    void SetUp() override {
        testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
        scratchDir = "/tmp/" + testName;
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
        co.set("tuplex.csv.selectionPushdown", "true");

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



/// a simple wrapper class to call closeInterpreter on destruction
class PyInterpreterGuard {
public:
    PyInterpreterGuard() {
        python::initInterpreter();
        printf("*** CALLED initInterpreter ***\n");
    }
    ~PyInterpreterGuard() {
        printf("*** CALLED closeInterpreter ***\n");
        python::closeInterpreter();
    }
};



// other test macros (https://stackoverflow.com/questions/1460703/comparison-of-arrays-in-google-test)
template<typename T> void EXPECT_IN_VECTOR(const std::vector<T>& v, const T& x) {
    auto it = std::find(v.begin(), v.end(), x);
    ASSERT_TRUE(it != v.end());
}

#endif //TUPLEX_TESTUTILS_H