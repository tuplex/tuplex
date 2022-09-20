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

#endif //TUPLEX_HYPERUTILS_H
