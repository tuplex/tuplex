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
#include <fstream>

static std::string fileToString(const std::string& path) {
    std::ifstream t(path);
    std::stringstream buffer;
    buffer << t.rdbuf();
    return buffer.str();
}

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

TEST(JSONUtils, SIMDJSONFieldParse) {
    using namespace tuplex;

    // super slow parse into tuplex structure using SIMDJSON
    std::string test_path = "../resources/ndjson/github.json";
    std::string data = fileToString(test_path);

    simdjson::padded_string ps(data);

    // https://simdjson.org/api/2.0.0/md_doc_iterate_many.html
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream stream;
    auto error = parser.iterate_many(data).get(stream);
    // dom parser allows more??
    // auto error = parser.parse_many(data).get(stream);

    if(error) {
        std::cerr << error << std::endl; return;
    }

    // break up into Rows and detect things along the way.
    std::vector<Row> rows;

    // first: detect column names (ordered? as they are?)
    std::vector<std::string> column_names;
    std::set<std::string> column_names_set;

    // anonymous row? I.e., simple value?
    for(auto it = stream.begin(); it != stream.end(); ++it) {
        auto doc = (*it);
        // type of doc
        switch(doc.type().value()) {
            case simdjson::ondemand::json_type::object: {
                auto obj = doc.get_object();
                // objects per line
                for(auto field : obj) {
                    //std::cout<<field.unescaped_key().value()<<" ";

                    // add to names
                    auto sv_key = field.unescaped_key().value();
                    std::string key = {sv_key.begin(), sv_key.end()};
                    auto jt = column_names_set.find(key);
                    if(jt == column_names_set.end()) {
                        column_names.push_back(key);
                        column_names_set.insert(key);
                    }

                }
                std::cout<<std::endl;
                break;
            }
            case simdjson::ondemand::json_type::array: {
                // array per line
                doc.count_elements();
                break;
            }
            default: {
                // unknown, i.e. error line.
                break;
            }
        }

//        std::cout << it.source() << std::endl;
    }
    std::cout << stream.truncated_bytes() << " bytes "<< std::endl; // returns 39 bytes

    std::cout<<"Found columns: "<<column_names<<std::endl;

    // how can everything be represented? I.e., nested struct? -> dict?

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