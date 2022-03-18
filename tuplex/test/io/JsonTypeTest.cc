//
// Created by Leonhard Spiegelberg on 3/18/22.
//

// @TODO: March, add to this file tests regarding JSON types

#include <gtest/gtest.h>
#include <VirtualFileSystem.h>
#include <JsonStatistic.h>

inline std::string testFolderName() {
    return "test_" + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
}

TEST(JsonTypes, FlatTypes) {
    using namespace tuplex;
    using namespace std;

    // get unique directory for this test --> allows to run tests in parallel!
    auto folder_name = testFolderName();

    // write a test file
    auto test_uri = URI("./" + folder_name + "/flat_typesI.json");
    stringToFile(test_uri, "{\"num_bedrooms\":10,\"price\":80.9}\n{\"num_bedrooms\":9,\"price\":20.9}");

    // want to use something similar to CSVStat -> JsonStat
    // reuse code from https://github.com/LeonhardFS/Tuplex/pull/82/files
    // write some more tests, add more test files
    // use simdjson instead of rapidjson (replace there!)

    // files: utils/src/JsonStatistic.cc and utils/include/JsonStatistic.h
    auto buf = fileToString(test_uri);

    // hand over to JSON Statistic, use some offset to make it a bit more challenging
    JsonStatistic stat(0.9);

    stat.estimate(buf.c_str() + 5, buf.size() - 5);

    EXPECT_EQ(stat.columns(), {"num_bedrooms", "price"}); // fix this!
    EXPECT_EQ(stat.type(), stat.superType()); // <-- no specialization here
    EXPECT_EQ(stat.type().desc(), "(i64,f64)");

}

// @March: TODO, more tests...