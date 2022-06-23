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


namespace tuplex {

    // non-recursive mapping
    python::Type jsonTypeToPythonTypeNonRecursive(const simdjson::ondemand::json_type& jtype, const std::string_view& value) {
        switch(jtype) {
            case simdjson::ondemand::json_type::array: {
                return python::Type::GENERICLIST;
            }
            case simdjson::ondemand::json_type::object: {
                return python::Type::GENERICDICT;
            }
            case simdjson::ondemand::json_type::string: {
                return python::Type::STRING;
            }
            case simdjson::ondemand::json_type::boolean: {
                return python::Type::BOOLEAN;
            }
            case simdjson::ondemand::json_type::null: {
                return python::Type::NULLVALUE;
            }
            case simdjson::ondemand::json_type::number: {
                // if a . can be found -> floating point, else integer (if length is not too large!)
                if(value.find('.') == std::string_view::npos)
                    return python::Type::I64;
                else
                    return python::Type::F64;
            }
            default: {
                return python::Type::UNKNOWN;
            }
        }
    }
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

    // pass I: detect column names
    // first: detect column names (ordered? as they are?)
    std::vector<std::string> column_names;
    std::unordered_map<std::string, size_t> column_index_lookup_table;
    std::set<std::string> column_names_set;

    // @TODO: test with corrupted files & make sure this doesn't fail...
    std::unordered_map<simdjson::ondemand::json_type, size_t> line_types;

    // counting tables for types
    std::unordered_map<std::tuple<size_t, python::Type>, size_t> type_counts;

    bool first_row = false;

    // anonymous row? I.e., simple value?
    for(auto it = stream.begin(); it != stream.end(); ++it) {
        auto doc = (*it);

        auto line_type = doc.type().value();
        line_types[line_type]++;

        // type of doc
        switch(line_type) {
            case simdjson::ondemand::json_type::object: {
                auto obj = doc.get_object();
                // objects per line
                for(auto field : obj) {
                    // add to names
                    auto sv_key = field.unescaped_key().value();
                    std::string key = {sv_key.begin(), sv_key.end()};
                    auto jt = column_names_set.find(key);
                    if(jt == column_names_set.end()) {
                        size_t cur_index = column_index_lookup_table.size();
                        column_index_lookup_table[key] = cur_index;
                        column_names.push_back(key);
                        column_names_set.insert(key);
                    }

                    // perform type count (lookups necessary because can be ANY order)
                    auto py_type = jsonTypeToPythonTypeNonRecursive(field.value().type(), field.value().raw_json_token());
                    // add to count array
                    type_counts[std::make_tuple(column_index_lookup_table[key], py_type)]++;
                }
                break;
            }
            case simdjson::ondemand::json_type::array: {
                // unknown, i.e. error line.
                // header? -> first line?
                if(first_row) {
                    bool all_elements_strings = true;
                    auto arr = doc.get_array();
                    size_t pos = 0;
                    for(auto field : arr) {
                        if(field.type() != simdjson::ondemand::json_type::string)
                            all_elements_strings = false;
                        else {
                            auto sv = field.get_string().value();
                            auto name = std::string{sv.begin(), sv.end()};
                            column_names.push_back(name);
                        }

                        // perform type count (lookups necessary because can be ANY order)
                        auto py_type = jsonTypeToPythonTypeNonRecursive(field.value().type(), field.value().raw_json_token());
                        // add to count array
                        type_counts[std::make_tuple(pos, py_type)]++;
                        pos++;
                    }
                }

                // todo, parse types???

                break;
            }
            default: {
                break;
            }
        }
        first_row = true;

//        std::cout << it.source() << std::endl;
    }
    std::cout << stream.truncated_bytes() << " bytes "<< std::endl; // returns 39 bytes
    std::cout<<"Found columns: "<<column_names<<std::endl;
    if(line_types.find(simdjson::ondemand::json_type::object) != line_types.end())
        std::cout<<"Rows in object notation"<<std::endl;
    if(line_types.find(simdjson::ondemand::json_type::array) != line_types.end())
        std::cout<<"Rows in array notation"<<std::endl;
    if(line_types.size() > 1) {
        std::cerr<<"Found mix of array [...] and object row notation"<<std::endl;
        std::cerr<<line_types[simdjson::ondemand::json_type::array]<<"x array, "<<line_types[simdjson::ondemand::json_type::object]<<std::endl;
    }

    // print type table
    // 1st, gather all available types
    std::set<python::Type> found_types;
    size_t max_column_idx = 0;
    for(const auto& keyval : type_counts) {
        found_types.insert(std::get<1>(keyval.first));
        max_column_idx = std::max(max_column_idx, std::get<0>(keyval.first));
    }
    // now go through column names
    // @TODO: retrieve column count statistics?
    std::cout<<"Found at most "<<pluralize(max_column_idx + 1, "column")<<std::endl;

    // types for each column
    // iterate over column (names)
    for(unsigned i = 0; i <= max_column_idx; ++i) {
        // name present? else, use dummy
        auto column_name = i < column_names.size() ? column_names[i] : "col(" + std::to_string(i) + ")";

        std::cout<<column_name<<": ";
        for(auto t : found_types) {
            auto it = type_counts.find(std::make_tuple(i, t));
            if(it != type_counts.end()) {
                std::cout<<t.desc()<<": "<<it->second<<" ";
            } else {
                // std::cout<<t.desc()<<": 0 ";
            }
        }
        std::cout<<std::endl;
    }

    // @TODO: when number of objects doesn't add up, fill in null values if that behavior is desired!
    // @TODO: run majority detect function from lambda-exp over the result, after ordering types per column.
    // @TODO: add header detection mode. (ignored in object setting)

    // @TODO: add structtype to type system with string keys typed explicitly (keytype), (value_type).

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