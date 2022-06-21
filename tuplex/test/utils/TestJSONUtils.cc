//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include <StringUtils.h>
#include <JSONUtils.h>
#include <JsonStatistic.h>

TEST(JSONUtils, Chunker) {
    using namespace std;
    using namespace tuplex;
    // test over json files the chunking

    string test_str;

    // reference is SIMDJSON.
    test_str="{}";
    EXPECT_EQ(findNLJsonStart(test_str.c_str(), test_str.size()), 0); // this should work!
    test_str = "{}}\n{}"; // this should not give 0
    EXPECT_NE(findNLJsonStart(test_str.c_str(), test_str.size()), 0); // this should work!
    test_str = "abc{},\n{\"hello world\"}";
    EXPECT_EQ(findNLJsonStart(test_str.c_str(), test_str.size()), strlen("abc{},\n")); // this should work!

    test_str = " world\"}";
    EXPECT_EQ(findNLJsonStart(test_str.c_str(), test_str.size()), -1);
}


// files to test: some with empty lines, etc.

TEST(JSONUtils, arrayConv) {

    using namespace std;
    using namespace tuplex;
    vector<string> v{"a", "abc", "\"hello\""};

    EXPECT_EQ(stringArrayToJSON(v), "[\"a\",\"abc\",\"\\\"hello\\\"\"]");

    auto res = jsonToStringArray(stringArrayToJSON(v));
    ASSERT_EQ(res.size(), v.size());
    for(int i = 0; i < v.size(); ++i)
        EXPECT_EQ(res[i], v[i]);
}

TEST(JSONUtils, mapArbitraryTypes) {
    using namespace std;
    using namespace tuplex;

    auto m = jsonToMap("{\"test\": \"string\", \"number\": 42, \"boolean\": true, \"None\": null}");

    EXPECT_EQ(m["test"], "string");
    EXPECT_EQ(m["number"], "42");
    EXPECT_EQ(m["boolean"], "true");
    EXPECT_EQ(m["None"], "null");
}