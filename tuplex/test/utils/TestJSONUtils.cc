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
#include <TypeHelper.h>
#include <Utils.h>

// other util files
#include <Timer.h>
#include <compression.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

bool create_dir(const std::string& path) {
    struct stat st = {0};

    if (stat(path.c_str(), &st) == -1) {
        mkdir(path.c_str(), 0700);
        return true;
    }
    return false;
}

static std::string fileToString(const std::string& path) {
    std::ifstream t(path);
    std::stringstream buffer;
    buffer << t.rdbuf();
    return buffer.str();
}

static bool stringToFile(const std::string& data, const std::string& path) {
    //std::ofstream ofs(path);
    //ofs << data;
    //ofs.flush();
    //ofs.close();
    //
    FILE *f = fopen(path.c_str(), "w");
    if(!f) { 
	    std::cerr<<"failed to open file "<<path<<" for writing"<<std::endl;
	    return false;
    }
    //fprintf(f, "%s", data.c_str()); 
    fwrite(data.c_str(), data.size(), 1, f);
    fclose(f);
    std::cout<<"wrote "<<data.size()<<" bytes to "<<path<<std::endl;
    return true;
}


//TEST(JSONUtils, CParse) {
//    using namespace tuplex;
//    using namespace std;
//
//    string sample_path = "/Users/leonhards/Downloads/github_sample";
//    string sample_file = sample_path + "/2011-11-26-13.json.gz";
//
//    auto path = sample_file;
//
//    path = "../resources/2011-11-26-13.json.gz";
//
//    auto raw_data = fileToString(path);
//
//    const char * pointer = raw_data.data();
//    std::size_t size = raw_data.size();
//
//    // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
//    std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;
//
//
//    // parse code starts here...
//    auto buf = decompressed_data.data();
//    auto buf_size = decompressed_data.size();
//
//
//    // detect (general-case) type here:
////    ContextOptions co = ContextOptions::defaults();
////    auto sample_size = co.SAMPLE_MAX_DETECTION_MEMORY();
////    auto nc_th = co.NORMALCASE_THRESHOLD();
//    auto sample_size = 256 * 1024ul; // 256kb
//    auto nc_th = 0.9;
//    auto rows = parseRowsFromJSON(buf, std::min(buf_size, sample_size));
//    auto row_type = detectMajorityRowType(rows, nc_th);
//    std::cout<<"detected: "<<row_type.desc()<<std::endl;
//
//    // C-version of parsing
//    uint64_t row_number = 0;
//
//    auto j = JsonParser_init();
//    if(!j)
//        throw std::runtime_error("failed to initialize parser");
//    JsonParser_open(j, buf, buf_size);
//    while(JsonParser_hasNextRow(j)) {
//        if(JsonParser_getDocType(j) != JsonParser_objectDocType()) {
//            // BADPARSE_STRINGINPUT
//            auto line = JsonParser_getMallocedRow(j);
//            free(line);
//        }
//
//        // line ok, now extract something from the object!
//        // => basically need to traverse...
//
//
//        row_number++;
//        JsonParser_moveToNextRow(j);
//    }
//    JsonParser_close(j);
//    JsonParser_free(j);
//
//    std::cout<<"Parsed "<<pluralize(row_number, "row")<<std::endl;
//}

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

    // for estimation a tree structure is required
    struct JSONTypeNode {
        std::string key;
        std::unordered_map<python::Type, size_t> types;
        std::vector<std::unique_ptr<JSONTypeNode>> children;

        inline bool isLeaf() const { return children.empty(); }

        inline void inc_type(const python::Type& type) {
            types[type]++;
        }
    };

    //
    /*!
     * check whether strings in needle adheres to order of reference array. Each element of needle must be contained within reference,
     * but there may be elements in reference that aren't contained. No duplicates allowed in either needle or reference!
     */
    bool adheresToRelativeOrder(const std::vector<std::string>& needle, const std::vector<std::string>& reference) {
        assert(std::set<std::string>(needle.begin(), needle.end()).size() == needle.size());
        assert(std::set<std::string>(reference.begin(), reference.end()).size() == reference.size());
        assert(needle.size() <= reference.size());

        if(needle.empty())
            return true;

        // slow algorithm, could be improved to be linear -> postponed for time reasons

        int last_idx = -1;
        for(unsigned i = 0; i < needle.size(); ++i) {
            // needle has to be contained within reference!
            auto idx = indexInVector(needle[i], reference);
            if(idx < 0)
                return false; // needle not contained within reference

            // idx has to be larger than last idx!
            if(idx <= last_idx)
                return false;

            last_idx = idx;
        }
        return true;
    }


    bool columnsAdheringAllToSameOrder(const std::vector<std::vector<std::string>>& v, std::vector<std::string>* detectedOrderedUniqueNames) {

        using namespace std;
        if(v.empty())
            return true;

        // pass 1: get default order of how column names are encountered.
        vector<string> column_names = v.front();
        set<string> unique_names(column_names.begin(), column_names.end());
        for(unsigned i = 1; i < v.size(); ++i) {
            // go through names and check whether they are all contained within the collected names
            for(unsigned j = 0; j < v[i].size(); ++j) {
                auto name = v[i][j];
                if(unique_names.find(name) == unique_names.end()) {
                    // not contained! Therefore insert at the current position and shift the rest of the vector further up!
                    column_names.insert(column_names.begin() + j, name);
                    unique_names.insert(name);
                } else {
                    // check if index j is smaller equal than detected index in already encountered column names
                    size_t idx = 0;
                    while(idx < column_names.size() && column_names[idx] != name)
                        idx++;
                    if(idx < j)
                        return false; // wrong!
                }
            }
        }

        if(detectedOrderedUniqueNames)
            *detectedOrderedUniqueNames = column_names;

        // pass 2: check whether the columns adhere to the order computed.

        return true;
    }

    template<typename T> bool vec_set_eq(const std::vector<T>& lhs, const std::vector<T>& rhs) {
        std::set<T> L(lhs.begin(), lhs.end());
        std::set<T> R(rhs.begin(), rhs.end());

        auto lsize = L.size();
        auto rsize = R.size();

        if(lsize != rsize)
            return false;

        // merge sets
        for(auto el : rhs)
            L.insert(el);
        return L.size() == lsize;
    }

    void reorder_row(Row& row,
                     const std::vector<std::string>& row_column_names,
                     const std::vector<std::string>& dest_column_names) {

        assert(row_column_names.size() == dest_column_names.size());
        assert(vec_set_eq(row_column_names, dest_column_names)); // expensive check

        // for each name, figure out where it has to be moved to!
        std::unordered_map<unsigned, unsigned> map;
        std::vector<Field> fields(row_column_names.size());
        for(unsigned i = 0; i < row_column_names.size(); ++i) {
            map[i] = indexInVector(row_column_names[i], dest_column_names);
        }
        for(unsigned i = 0; i < row_column_names.size(); ++i)
            fields[map[i]] = row.get(i);
        row = Row::from_vector(fields);
    }

}

TEST(JSONUtils, relativeOrderTest) {
    EXPECT_TRUE(tuplex::adheresToRelativeOrder({}, {"a", "b", "c"}));
    EXPECT_TRUE(tuplex::adheresToRelativeOrder({"a"}, {"a", "b", "c"}));
    EXPECT_TRUE(tuplex::adheresToRelativeOrder({"b"}, {"a", "b", "c"}));
    EXPECT_TRUE(tuplex::adheresToRelativeOrder({"c"}, {"a", "b", "c"}));
    EXPECT_TRUE(tuplex::adheresToRelativeOrder({"a", "b"}, {"a", "b", "c"}));
    EXPECT_TRUE(tuplex::adheresToRelativeOrder({"a", "c"}, {"a", "b", "c"}));
    EXPECT_TRUE(tuplex::adheresToRelativeOrder({"b", "c"}, {"a", "b", "c"}));
    // false
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"b", "a"}, {"a", "b", "c"})); // wrong order
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"c", "a"}, {"a", "b", "c"})); // wrong order
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"c", "b"}, {"a", "b", "c"})); // wrong order
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"d"}, {"a", "b", "c"})); // not contained
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"a", "d"}, {"a", "b", "c"})); // partially not contained
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"d", "a"}, {"a", "b", "c"})); // partially not contained
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"b", "d"}, {"a", "b", "c"})); // partially not contained
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"d", "b"}, {"a", "b", "c"})); // partially not contained
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"c", "d"}, {"a", "b", "c"})); // partially not contained
    EXPECT_FALSE(tuplex::adheresToRelativeOrder({"d", "c"}, {"a", "b", "c"})); // partially not contained
}

TEST(JSONUtils, ReorderRow) {
    using namespace std;
    using namespace tuplex;

    Row r(10, 20, 30);
    reorder_row(r, {"a", "b", "c"}, {"b", "c", "a"});
    EXPECT_EQ(r.toPythonString(), "(20,30,10)");
}

TEST(JSONUtils, Escaping) {
    // test helper functions for escaping/unescaping strings using full ASCII set
    using namespace std;
    using namespace tuplex;

    // UTF-8 not supported yet...

    string test(128, '\0');
    for(int i = 0; i < 128; ++i)
        test[i] = 128 - i - 1;

    auto escaped = escape_json_string(test);
    auto unescaped = unescape_json_string(escaped);

    cout<<"escaped:   "<<escaped<<endl;
    cout<<"unescaped: "<<unescaped<<endl;

    EXPECT_GE(escaped.size(), test.size());
    EXPECT_EQ(unescaped, test);

    // use nlohmann to check as well
    nlohmann::json j;
    j["test"] = test;
    auto dump = j.dump();
    dump = dump.substr(8, dump.length() - 9);
    auto unescaped_test = unescape_json_string(dump);
    EXPECT_EQ(dump, escaped);
    EXPECT_EQ(unescaped_test, test);
}

namespace tuplex {

    nlohmann::json sampleToJSON(const std::vector<Row>& rows) {
        nlohmann::json j = nlohmann::json::array();

        for(auto row : rows) {
            nlohmann::json jrow;
            jrow = row.toPythonString();
            j.push_back(jrow);
        }

        return j;
    }


    std::string process_path(const std::string& path, std::string event_type="", size_t max_samples_per_path=100) {
        using namespace std;

        auto& logger = Logger::instance().defaultLogger();
        std::stringstream ss;

        // step 1: decode file
        Timer timer;

        ss<<"start processing "<<path<<"::"<<endl;
        logger.info(ss.str()); ss.str("");
        auto raw_data = fileToString(path);

        const char * pointer = raw_data.data();
        std::size_t size = raw_data.size();

        // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
        std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;
        ss<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "<<"loaded "<<path<<" "<<sizeToMemString(raw_data.size())<<" -> "<<sizeToMemString(decompressed_data.size())<<" (decompressed)"<<endl;
        logger.info(ss.str()); ss.str("");

        // step 2: load json from decompressed data
        std::vector<std::vector<std::string>> column_names;
        timer.reset();
        auto rows = parseRowsFromJSON(decompressed_data, &column_names);
        ss<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "<<"parsed "<<pluralize(rows.size(), "row")<<" from "<<path<<" ("<<(rows.size() / timer.time())<<" rows/s)"<<endl;
        logger.info(ss.str()); ss.str("");

        if(!event_type.empty()) {
            // parse rows by event type
            std::vector<Row> filtered_rows;
            std::vector<std::vector<std::string>> filtered_names;
            for(unsigned i = 0; i < rows.size(); ++i) {
                // find type field
                // "type"
                auto idx = indexInVector(std::string("type"), column_names[i]);
                if(idx < 0)
                    continue;
                auto value = rows[i].getString(idx);
                if(value == event_type)  {
                    filtered_rows.push_back(rows[i]);
                    filtered_names.push_back(column_names[i]);
                }
            }
            ss<<"filtered down to "<<pluralize(filtered_rows.size(), "row")<<" of type "<<event_type<<" from "<<pluralize(rows.size(), "row")<<endl;
            logger.info(ss.str()); ss.str("");
            rows = filtered_rows;
            column_names = filtered_names;

            if(filtered_rows.empty()) {
                logger.error("no rows");
                return "";
            }

        }

        // step 3: info about column names
        // fetch stat about column names, i.e. which ones occur how often?
        // ==> per row stat?
        // ==> replacing missing values with nulls or not?
        std::set<std::string> unique_column_names;
        size_t min_column_name_count = std::numeric_limits<size_t>::max();
        size_t max_column_name_count = std::numeric_limits<size_t>::min();

        for(auto names: column_names) {
            for(auto name : names)
                unique_column_names.insert(name);
            min_column_name_count = std::min(min_column_name_count, names.size());
            max_column_name_count = std::max(max_column_name_count, names.size());
        }

        ss<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
          <<"sample contains "<<min_column_name_count<<" - "<<max_column_name_count<<" columns"<<std::endl;
        logger.info(ss.str()); ss.str("");

        // step 4: determine type counts & majority types
        std::vector<std::string> column_names_ordered;
        ss<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
            <<"column adhering to same order within file: "<<std::boolalpha<<columnsAdheringAllToSameOrder(column_names, &column_names_ordered)<<std::endl;
        logger.info(ss.str()); ss.str("");
        // if not same column order -> need to resort rows!!!
        bool same_column_order = columnsAdheringAllToSameOrder(column_names, &column_names_ordered);

        // the detection results
        size_t detected_column_count = 0;
        std::vector<std::string> detected_column_names;

        // detection conf variables
        double conf_nc_threshold = 0.9;
        bool conf_independent_columns=true;
        bool conf_use_nvo=true;
        bool conf_treatMissingDictKeysAsNone = false;
        bool conf_autoupcast_numbers = false;
        bool conf_allowUnifyWithPyObject = false;
        auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
        conf_general_case_type_policy.unifyMissingDictKeys = true;
        conf_general_case_type_policy.allowUnifyWithPyObject = true;


        TypeUnificationPolicy conf_type_policy;
        conf_type_policy.unifyMissingDictKeys = true;
        std::vector<Row> sample;

        // if fill-in with missing null-values is ok, then can use maximum order of columns, if not need to first detect maximum order
        if(conf_treatMissingDictKeysAsNone) {
            // detect majority type of rows (individual columns (?) )
            detected_column_count = column_names_ordered.size();
            detected_column_names = column_names_ordered;

            // fix up columns and add them to sample
            throw std::runtime_error("nyimpl");
        } else {
            ss<<"   -- detecting majority case column count and names"<<std::endl;
            logger.info(ss.str()); ss.str("");
            std::unordered_map<std::vector<std::string>, size_t> column_count_counts;
            for(auto names : column_names) {
                column_count_counts[names]++;
            }

            // majority case
            ss<<"   -- found "<<pluralize(column_count_counts.size(), "unique column name constellation")<<std::endl;
            logger.info(ss.str()); ss.str("");

            auto most_frequent_count = 0;
            std::vector<std::string> most_frequent_names;
            for(const auto& el : column_count_counts)
                if(el.second > most_frequent_count) {
                    most_frequent_count = el.second;
                    most_frequent_names = el.first;
                }
            ss<<"   -- most common column names are: "<<most_frequent_names<<" ("
              <<pluralize(most_frequent_names.size(), "column")<<", "
              <<(100.0 * most_frequent_count / (1.0 * column_names.size()))<<"%)"<<std::endl;
            logger.info(ss.str()); ss.str("");

            // now compute majority row type by first filtering on the columns adhering to that column count
            detected_column_count = most_frequent_names.size();
            detected_column_names = most_frequent_names;

            // create sample by scanning
            assert(column_names.size() == rows.size());
            for(unsigned i = 0; i < column_names.size(); ++i) {
                if(rows[i].getNumColumns() != detected_column_count)
                    continue;
                if(column_names[i] == detected_column_names) {
                    // add to sample
                    sample.push_back(rows[i]);
                } else {
                    // skip for now, later implement here order-invariant => i.e. reorder columns!
                    if(vec_set_eq(column_names[i], detected_column_names)) {
                        Row row = rows[i];
                        reorder_row(row, column_names[i], detected_column_names);
                        sample.push_back(row);
                    } else {
                        continue;
                    }
                }
            }
        }

        std::unordered_map<python::Type, size_t> type_counts;
        timer.reset();

        // step 5: detect over ALL rows by forming corresponding type
        vector<python::Type> row_types;
        row_types.reserve(rows.size());
        for(unsigned i = 0; i < rows.size(); ++i) {
            auto num_columns = rows[i].getNumColumns();
            assert(num_columns == column_names[i].size());

            vector<python::StructEntry> kv_pairs; kv_pairs.reserve(num_columns);
            for(unsigned j = 0; j < num_columns; ++j) {
                python::StructEntry entry;
                entry.alwaysPresent = true;
                entry.key = escape_to_python_str(column_names[i][j]);
                entry.keyType = python::Type::STRING;
                entry.valueType = rows[i].getType(j);
                kv_pairs.push_back(entry);
            }

            // create struct type
            auto s_type = python::Type::makeStructuredDictType(kv_pairs);
            row_types.push_back(s_type);

            // hash type
            type_counts[s_type]++;
        }

        ss<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
          <<"found "<<pluralize(type_counts.size(), "different type")<<" in "<<path<<endl;
        logger.info(ss.str()); ss.str("");

        // what's the most common type?
        // order pairs!
        std::vector<std::pair<python::Type, size_t>> type_count_pairs(type_counts.begin(), type_counts.end());
        std::sort(type_count_pairs.begin(), type_count_pairs.end(),
                  [](const std::pair<python::Type, size_t>& lhs,
                     const std::pair<python::Type, size_t>& rhs) {
                      return lhs.second > rhs.second;
                  });

        double most_likely_pct = 100.0 * (type_count_pairs.front().second / (1.0 * rows.size()));
        double least_likely_pct = 100.0 * (type_count_pairs.back().second / (1.0 * rows.size()));
        ss<<"   -- most likely type is ("<<most_likely_pct<<"%, "<<type_count_pairs.front().second<<"x): "<<type_count_pairs.front().first.desc()<<endl;
        ss<<"   -- least likely type is ("<<least_likely_pct<<"%, "<<type_count_pairs.back().second<<"x): "<<type_count_pairs.back().first.desc()<<endl;
        logger.info(ss.str()); ss.str("");

        auto general_case_max_type = maximizeTypeCover(type_count_pairs, conf_nc_threshold, true, conf_general_case_type_policy);
        auto normal_case_max_type = maximizeTypeCover(type_count_pairs, conf_nc_threshold, true, TypeUnificationPolicy::defaultPolicy());

        double num_rows_d = column_names.size() * 1.0;
        double normal_pct = normal_case_max_type.second / num_rows_d * 100.0;
        double general_pct = general_case_max_type.second / num_rows_d * 100.0;
        ss<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
            <<"maximizing type cover results in:"<<path<<endl;
        ss<<"   -- normal  case max type ("<<normal_pct<<"%, "<<normal_case_max_type.second<<"x): "<<normal_case_max_type.first.desc()<<std::endl;
        ss<<"   -- general case max type ("<<general_pct<<"%, "<<general_case_max_type.second<<"x): "<<general_case_max_type.first.desc()<<std::endl;
        logger.info(ss.str()); ss.str("");

        // pretty print
        ss<<"normal  case max type: "<<prettyPrintStructType(normal_case_max_type.first)<<std::endl;
        ss<<"general case max type: "<<prettyPrintStructType(general_case_max_type.first)<<std::endl;
        logger.info(ss.str()); ss.str("");

        // check how many rows would be on each path (normal, general, fallback)
        // check how many (of the original) rows adhere to this detected normal-case type
        // this also requires column name checking!
        size_t nc_count = 0;
        size_t gc_count = 0;
        size_t fb_count = 0;

        std::vector<Row> normal_case_sample;
        std::vector<Row> general_case_sample;
        std::vector<Row> fallback_case_sample;
        for(unsigned i = 0; i < row_types.size(); ++i) {
            if(python::canUpcastType(row_types[i], normal_case_max_type.first)) {
                nc_count++;

                if(normal_case_sample.size() < max_samples_per_path) {
                    assert(rows.size() >= max_samples_per_path);
                    normal_case_sample.push_back(rows[i]);
                }

            } else if(python::canUpcastType(row_types[i], general_case_max_type.first)) {

                if(general_case_sample.size() < max_samples_per_path) {
                    assert(rows.size() >= max_samples_per_path);
                    general_case_sample.push_back(rows[i]);
                }

                gc_count++;
            } else {
                if(fallback_case_sample.size() < max_samples_per_path) {
                    assert(rows.size() >= max_samples_per_path);
                    fallback_case_sample.push_back(rows[i]);
                }

                fb_count++;
            }


        }
        ss<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
            <<"execution would result in the following paths taken:"<<path<<endl;
        ss<<"   -- "<<nc_count<<" / "<<row_types.size()<<" normal path ("<<(nc_count / (0.01 * row_types.size()))<<"%)"<<endl;
        ss<<"   -- "<<gc_count<<" / "<<row_types.size()<<" general path ("<<(gc_count / (0.01 * row_types.size()))<<"%)"<<endl;
        ss<<"   -- "<<fb_count<<" / "<<row_types.size()<<" fallback path ("<<(fb_count / (0.01 * row_types.size()))<<"%)"<<endl;
        logger.info(ss.str()); ss.str("");

        // create json string with all the data so nothing is wasted.
        std::string json_string;
        {
            nlohmann::json j;
            j["path"] = path;
            j["size"] = decompressed_data.size();
            j["nrows"] = rows.size();
            j["min_columns"] = min_column_name_count;
            j["max_columns"] = max_column_name_count;
            j["normal_case_type"] = normal_case_max_type.first.desc();
            j["normal_case_path_count"] = nc_count;
            j["general_case_type"] = general_case_max_type.first.desc();
            j["general_case_path_count"] = gc_count;
            j["fallback_case_path_count"] = fb_count;

            // pretty print types
            j["normal_case_type_pretty"] = prettyPrintStructType(normal_case_max_type.first);
            j["general_case_type_pretty"] = prettyPrintStructType(general_case_max_type.first);

            // type counts (detailed)
            auto j_counts = nlohmann::json::array();
            for(const auto& kv : type_counts) {
                nlohmann::json tmp;
                tmp["type"] = kv.first.desc();
                tmp["count"] = kv.second;
                j_counts.push_back(tmp);
            }
            j["type_counts"] = j_counts;

            // add sample of detected paths
            j["normal_sample"] = sampleToJSON(normal_case_sample);
            j["general_sample"] = sampleToJSON(general_case_sample);
            j["fallback_sample"] = sampleToJSON(fallback_case_sample);

            // to string
            json_string = j.dump();
        }

        return json_string;
    }
}


TEST(JSONUtils, CheckFiles) {
    using namespace tuplex;
    using namespace std;

    auto& logger = Logger::instance().defaultLogger();

    // for each file, run sampling stats
    string root_path = "/hot/data/flights_all/";
    string pattern = root_path + "flights_on_time_performance_*.csv";


    // test
    pattern = "../resources/*.json.gz";

    // bbsn00 test
    //pattern = "/disk/download/data/*2021*.json.gz";

    // test file /disk/download/data/2021-01-05-11.json.gz

    // where to output stats...
    string output_path = "./stats";
    output_path = "/home/lspiegel/tuplex-public/tuplex/build/stats";

    pattern = "../resources/ndjson/github-hyper/*.json";

    cout<<"Saving detailed stats in "<<output_path<<endl;

    create_dir(output_path); 

    size_t num_files_found = 0;
    auto paths = glob(pattern);
    std::sort(paths.begin(), paths.end());
    std::reverse(paths.begin(), paths.end());
    num_files_found = paths.size();
    cout<<"Found "<<pluralize(num_files_found, "file")<<" to analyze schema for."<<endl;

    auto concurrency = std::thread::hardware_concurrency();
    cout<<"Detected hardware concurrency: "<<concurrency<<endl;

    // process in parallel...
    for(const auto& path : paths) {
        auto json_string = process_path(path, "PushEvent");
        auto fname = base_file_name(path.c_str());
        auto save_path = output_path + "/" + fname + "_stats.json";
        //cout<<"saving stats data to "<<save_path<<endl;
        stringToFile(json_string, save_path);
        logger.info("processed file " + std::string(fname));
        break;
    }
}

TEST(JSONUtils, CheckPushEventFiles) {
    using namespace tuplex;
    using namespace std;

//    // for each file, run sampling stats
//    string pattern = "../resources/*.json.gz";
//
//    // bbsn00 test
//    //pattern = "/disk/download/data/*2021*.json.gz";
//
//    // test file /disk/download/data/2021-01-05-11.json.gz
//    //pattern = "/Users/leonhards/Downloads/2021-01-05-11.json.gz";
//
//    // where to output stats...
//    string output_path = "stats";
//    cout<<"Saving detailed stats in "<<"./"<<output_path<<endl;
//
//    size_t num_files_found = 0;
//    auto paths = glob(pattern);
//    std::sort(paths.begin(), paths.end());
//    num_files_found = paths.size();
//    cout<<"Found "<<pluralize(num_files_found, "file")<<" to analyze schema for."<<endl;
//
//    for(const auto& path : paths) {
//        auto json_string = process_path(path);
//        auto fname = base_file_name(path.c_str());
//        auto save_path = output_path + "/" + fname + "_stats.json";
//        cout<<"saving stats data to "<<save_path<<endl;
//        stringToFile(json_string, save_path);
//        break;
//    }
}


TEST(JSONUtils, SIMDJSONFieldParse) {
    using namespace tuplex;

    // super slow parse into tuplex structure using SIMDJSON
//    std::string test_path = "../resources/ndjson/github.json";
    std::string test_path = "../resources/ndjson/example1.json";
    std::string data = fileToString(test_path);

    std::vector<std::vector<std::string>> column_names;
    auto rows = parseRowsFromJSON(data, &column_names);

    // fetch stat about column names, i.e. which ones occur how often?
    // ==> per row stat?
    // ==> replacing missing values with nulls or not?
    std::set<std::string> unique_column_names;
    size_t min_column_name_count = std::numeric_limits<size_t>::max();
    size_t max_column_name_count = std::numeric_limits<size_t>::min();

    for(auto names: column_names) {
        for(auto name : names)
            unique_column_names.insert(name);
        min_column_name_count = std::min(min_column_name_count, names.size());
        max_column_name_count = std::max(max_column_name_count, names.size());
    }

    std::cout<<"sample contains "<<min_column_name_count<<" - "<<max_column_name_count<<" columns"<<std::endl;
    std::vector<std::string> column_names_ordered;
    std::cout<<"Do all rows adhere to same column order?: "<<std::boolalpha<<columnsAdheringAllToSameOrder(column_names, &column_names_ordered)<<std::endl;

    // @TODO: might need to resort columns after names b.c. JSON order is NOT unique...
    // other option is to use struct type to find majority types...

    // if not same column order -> need to resort rows!!!
    bool same_column_order = columnsAdheringAllToSameOrder(column_names, &column_names_ordered);

    // the detection results
    size_t detected_column_count = 0;
    std::vector<std::string> detected_column_names;

    // detection conf variables
    double conf_nc_threshold = 0.9;
    bool conf_independent_columns=true;
    bool conf_use_nvo=true;
    bool conf_treatMissingDictKeysAsNone = false;
    bool conf_autoupcast_numbers = false;
    bool conf_allowUnifyWithPyObject = false;

    TypeUnificationPolicy conf_type_policy;
    conf_type_policy.unifyMissingDictKeys = true;


    if(!same_column_order) {
        throw std::runtime_error("need to resort/reorder column");
    } else {

        std::vector<Row> sample;

        // if fill-in with missing null-values is ok, then can use maximum order of columns, if not need to first detect maximum order
        if(conf_treatMissingDictKeysAsNone) {
            // detect majority type of rows (individual columns (?) )
            detected_column_count = column_names_ordered.size();
            detected_column_names = column_names_ordered;

            // fix up columns and add them to sample
            throw std::runtime_error("nyimpl");
        } else {
            std::cout<<"detecting majority case column count and names"<<std::endl;
            std::unordered_map<std::vector<std::string>, size_t> column_count_counts;
            for(auto names : column_names) {
                column_count_counts[names]++;
            }

            // majority case
            std::cout<<"found "<<pluralize(column_count_counts.size(), "unique column name constellation")<<std::endl;

            auto most_frequent_count = 0;
            std::vector<std::string> most_frequent_names;
            for(const auto& el : column_count_counts)
                if(el.second > most_frequent_count) {
                    most_frequent_count = el.second;
                    most_frequent_names = el.first;
                }
            std::cout<<"most common column names are: "<<most_frequent_names<<" ("
                     <<pluralize(most_frequent_names.size(), "column")<<", "
                     <<(100.0 * most_frequent_count / (1.0 * column_names.size()))<<"%)"<<std::endl;

            // now compute majority row type by first filtering on the columns adhering to that column count
            detected_column_count = most_frequent_names.size();
            detected_column_names = most_frequent_names;

            // create sample by scanning
            assert(column_names.size() == rows.size());
            for(unsigned i = 0; i < column_names.size(); ++i) {
                if(rows[i].getNumColumns() != detected_column_count)
                    continue;
                if(column_names[i] == detected_column_names) {
                    // add to sample
                    sample.push_back(rows[i]);
                } else {
                    // skip for now, later implement here order-invariant => i.e. reorder columns!
                    if(vec_set_eq(column_names[i], detected_column_names)) {
                        Row row = rows[i];
                        reorder_row(row, column_names[i], detected_column_names);
                        sample.push_back(row);
                    } else {
                        continue;
                    }
                }
            }
        }

        std::cout<<"sample has "<<pluralize(sample.size(), "row")<<std::endl;

        // detect type based on sample...
        auto majorityRowType = detectMajorityRowType(sample, conf_nc_threshold, conf_independent_columns, conf_use_nvo);
        std::cout<<"detected majority column type is: "<<majorityRowType.desc()<<std::endl;

        // type cover maximization
        std::vector<std::pair<python::Type, size_t>> type_counts;
        for(unsigned i = 0; i < column_names.size(); ++i) {
            // row check:
            std::cout<<"row: "<<rows[i].toPythonString()<<" type: "<<rows[i].getRowType().desc()<<std::endl;
            type_counts.emplace_back(std::make_pair(rows[i].getRowType(), 1));
        }

        auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
        conf_general_case_type_policy.unifyMissingDictKeys = true;
        conf_general_case_type_policy.allowUnifyWithPyObject = true;


        auto general_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
        auto normal_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, TypeUnificationPolicy::defaultPolicy());

        double num_rows_d = column_names.size() * 1.0;
        double normal_pct = normal_case_max_type.second / num_rows_d * 100.0;
        double general_pct = general_case_max_type.second / num_rows_d * 100.0;
        std::cout<<"normal  case max type ("<<normal_pct<<"%): "<<normal_case_max_type.first.desc()<<std::endl;
        std::cout<<"general case max type ("<<general_pct<<"%): "<<general_case_max_type.first.desc()<<std::endl;

        // check how many (of the original) rows adhere to this detected normal-case type
        // this also requires column name checking!
        size_t nc_count = 0;
        for(unsigned i = 0; i < column_names.size(); ++i) {

            // row check:
            std::cout<<"row: "<<rows[i].toPythonString()<<" type: "<<rows[i].getRowType().desc()<<std::endl;

            if(rows[i].getNumColumns() != detected_column_count)
                continue;
            if(column_names[i] == detected_column_names) {
               if(!python::canUpcastToRowType(rows[i].getRowType(), majorityRowType))
                   continue;
            } else {
                // skip for now, later implement here order-invariant => i.e. reorder columns!
                if(vec_set_eq(column_names[i], detected_column_names)) {
                    Row row = rows[i];
                    reorder_row(row, column_names[i], detected_column_names);
                    if(!python::canUpcastToRowType(row.getRowType(), majorityRowType))
                        continue;
                } else {
                    continue;
                }
            }
            nc_count++;
        }
        std::cout<<"Of the original sample, "<<nc_count<<"/"<<pluralize(rows.size(), "row")
                 <<" ("<<(100.0 * nc_count / (1.0 * rows.size()))<<"%) adhere to the normal case"<<std::endl;

        // how many can be processed when allowing for parsing into a tree like structure?
        // @TODO
        // ==> basically create max-struct type for this case! => in physical layer this requires a key-present check
        //     at each level! => could be done e.g. with tree like struct to save space? => is this always a good idea?
        // i.e., sparsity of json tree dictates the physical representation.

        // how many rows require fallback because they do not fit normal nor general case?
        // @TODO


        // a sample query that helps for specializing:
        // internal representation when rewriting data (?) => could also use original strings, so this could be stupid.
        // however, different when partial data is extracted. => could apply to github. E.g., partially rewrite commit message (?)


        // for parser: 1.) check normal-case 2.) check general-case -> NV violation! 3.) badparse input.


        // next steps: TODO run large-scale analysis over github data, for each file detect how many
        // data points would confirm to a) normal-case b) general-case c) fallback
        // and how many different cases are detected!

    }

    return;



//
//    simdjson::padded_string ps(data);
//
//    // https://simdjson.org/api/2.0.0/md_doc_iterate_many.html
//    simdjson::ondemand::parser parser;
//    simdjson::ondemand::document_stream stream;
//    auto error = parser.iterate_many(data).get(stream);
//    // dom parser allows more??
//    // auto error = parser.parse_many(data).get(stream);
//
//    if(error) {
//        std::cerr << error << std::endl; return;
//    }
//
//    // break up into Rows and detect things along the way.
//    std::vector<Row> rows;
//
//    // pass I: detect column names
//    // first: detect column names (ordered? as they are?)
//    std::vector<std::string> column_names;
//    std::unordered_map<std::string, size_t> column_index_lookup_table;
//    std::set<std::string> column_names_set;
//
//    // @TODO: test with corrupted files & make sure this doesn't fail...
//    std::unordered_map<simdjson::ondemand::json_type, size_t> line_types;
//
//    // counting tables for types
//    std::unordered_map<std::tuple<size_t, python::Type>, size_t> type_counts;
//
//    bool first_row = false;
//
//    // cf. Tree Walking and JSON Element Types in https://github.com/simdjson/simdjson/blob/master/doc/basics.md
//    // use scalar() => for simple numbers etc.
//
//    // anonymous row? I.e., simple value?
//    for(auto it = stream.begin(); it != stream.end(); ++it) {
//        auto doc = (*it);
//
//        auto line_type = doc.type().value();
//        line_types[line_type]++;
//
//        // type of doc
//        switch(line_type) {
//            case simdjson::ondemand::json_type::object: {
//                auto obj = doc.get_object();
//                // objects per line
//                for(auto field : obj) {
//                    // add to names
//                    auto sv_key = field.unescaped_key().value();
//                    std::string key = {sv_key.begin(), sv_key.end()};
//                    auto jt = column_names_set.find(key);
//                    if(jt == column_names_set.end()) {
//                        size_t cur_index = column_index_lookup_table.size();
//                        column_index_lookup_table[key] = cur_index;
//                        column_names.push_back(key);
//                        column_names_set.insert(key);
//                    }
//
//                    // perform type count (lookups necessary because can be ANY order)
//                    auto py_type = jsonTypeToPythonTypeNonRecursive(field.value().type(), field.value().raw_json_token());
//
//                    // generic types? -> recurse!
//                    if(py_type == python::Type::GENERICDICT || py_type == python::Type::GENERICLIST) {
//                        py_type = jsonTypeToPythonTypeRecursive(field.value());
//                    }
//
//                    // add to count array
//                    type_counts[std::make_tuple(column_index_lookup_table[key], py_type)]++;
//                }
//                break;
//            }
//            case simdjson::ondemand::json_type::array: {
//                // unknown, i.e. error line.
//                // header? -> first line?
//                if(first_row) {
//                    bool all_elements_strings = true;
//                    auto arr = doc.get_array();
//                    size_t pos = 0;
//                    for(auto field : arr) {
//                        if(field.type() != simdjson::ondemand::json_type::string)
//                            all_elements_strings = false;
//                        else {
//                            auto sv = field.get_string().value();
//                            auto name = std::string{sv.begin(), sv.end()};
//                            column_names.push_back(name);
//                        }
//
//                        // perform type count (lookups necessary because can be ANY order)
//                        auto py_type = jsonTypeToPythonTypeNonRecursive(field.value().type(), field.value().raw_json_token());
//                        // add to count array
//                        type_counts[std::make_tuple(pos, py_type)]++;
//                        pos++;
//                    }
//                }
//                break;
//            }
//            default: {
//                // basic element -> directly map type!
//                auto py_type = jsonTypeToPythonTypeNonRecursive(doc.type().value(), doc.raw_json_token());
//                break;
//            }
//        }
//        first_row = true;
//
////        std::cout << it.source() << std::endl;
//    }
//    std::cout << stream.truncated_bytes() << " bytes "<< std::endl; // returns 39 bytes
//    std::cout<<"Found columns: "<<column_names<<std::endl;
//    if(line_types.find(simdjson::ondemand::json_type::object) != line_types.end())
//        std::cout<<"Rows in object notation"<<std::endl;
//    if(line_types.find(simdjson::ondemand::json_type::array) != line_types.end())
//        std::cout<<"Rows in array notation"<<std::endl;
//    if(line_types.size() > 1) {
//        std::cerr<<"Found mix of array [...] and object row notation"<<std::endl;
//        std::cerr<<line_types[simdjson::ondemand::json_type::array]<<"x array, "<<line_types[simdjson::ondemand::json_type::object]<<std::endl;
//    }
//
//    // print type table
//    // 1st, gather all available types
//    std::set<python::Type> found_types;
//    size_t max_column_idx = 0;
//    for(const auto& keyval : type_counts) {
//        found_types.insert(std::get<1>(keyval.first));
//        max_column_idx = std::max(max_column_idx, std::get<0>(keyval.first));
//    }
//    // now go through column names
//    // @TODO: retrieve column count statistics?
//    std::cout<<"Found at most "<<pluralize(max_column_idx + 1, "column")<<std::endl;
//
//    // types for each column
//    // iterate over column (names)
//    for(unsigned i = 0; i <= max_column_idx; ++i) {
//        // name present? else, use dummy
//        auto column_name = i < column_names.size() ? column_names[i] : "col(" + std::to_string(i) + ")";
//
//        std::cout<<column_name<<": ";
//        for(auto t : found_types) {
//            auto it = type_counts.find(std::make_tuple(i, t));
//            if(it != type_counts.end()) {
//                std::cout<<t.desc()<<": "<<it->second<<" ";
//            } else {
//                // std::cout<<t.desc()<<": 0 ";
//            }
//        }
//        std::cout<<std::endl;
//    }
//
//    // @TODO: when number of objects doesn't add up, fill in null values if that behavior is desired!
//    // @TODO: run majority detect function from lambda-exp over the result, after ordering types per column.
//    // @TODO: add header detection mode. (ignored in object setting)
//
//    // @TODO: add structtype to type system with string keys typed explicitly (keytype), (value_type).
//
//    // how can everything be represented? I.e., nested struct? -> dict?
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
