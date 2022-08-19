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

namespace tuplex {
    /*!
     * maximizeTypeCover computes the most likely type by potentially fusing types together.
     * @param counts
     * @param threshold
     * @param use_nvo
     * @param t_policy
     * @return
     */
    std::pair<python::Type, size_t> maximizeTypeCover(const std::vector<std::pair<python::Type, size_t>>& counts,
                                                      double threshold,
                                                      bool use_nvo,
                                                      const TypeUnificationPolicy& t_policy) {
        using namespace std;

        if(counts.empty()) {
            return make_pair(python::Type::UNKNOWN, 0);
        }

        // @TODO: implement this here! incl. recursive null checking for structured dict types...

        // sort desc after count pairs. Note: goal is to have maximum type cover!
        auto t_counts = counts;
        std::sort(t_counts.begin(), t_counts.end(), [](const pair<python::Type, size_t>& lhs,
                const pair<python::Type, size_t>& rhs) { return lhs.second > rhs.second; });

        // this is a dynamic programming problem and should be solved alike...

        // Malte: thresholding for gc? i.e. get rid off super rare formats?


        // for now, use a super simple solution. I.e., combine till combination doesn't work anymore.
        std::pair<python::Type, size_t> best_pair = t_counts.front();
        for(unsigned i = 1; i < t_counts.size(); ++i) {
            auto combined_type = unifyTypes(best_pair.first, t_counts[i].first, t_policy);
            if(combined_type != python::Type::UNKNOWN) {
                best_pair.first = combined_type;
                best_pair.second += t_counts[i].second;
            } else {
                // std::cout<<"can't unify:\n"<<best_pair.first.desc()<<"\nwith\n"<<t_counts[i].first.desc()<<std::endl;
                return best_pair;
            }
        }
        return best_pair;
    }


    std::string process_path(const std::string& path) {
        using namespace std;

        // step 1: decode file
        Timer timer;

        cout<<"start processing "<<path<<"::"<<endl;
        auto raw_data = fileToString(path);

        const char * pointer = raw_data.data();
        std::size_t size = raw_data.size();

        // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
        std::string decompressed_data = gzip::decompress(pointer, size);
        cout<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "<<"loaded "<<path<<" "<<sizeToMemString(raw_data.size())<<" -> "<<sizeToMemString(decompressed_data.size())<<" (decompressed)"<<endl;

        // step 2: load json from decompressed data
        std::vector<std::vector<std::string>> column_names;
        timer.reset();
        auto rows = parseRowsFromJSON(decompressed_data, &column_names);
        cout<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "<<"parsed "<<pluralize(rows.size(), "row")<<" from "<<path<<" ("<<(rows.size() / timer.time())<<" rows/s)"<<endl;

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

        cout<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
            <<"sample contains "<<min_column_name_count<<" - "<<max_column_name_count<<" columns"<<std::endl;

        // step 4: determine type counts & majority types
        std::vector<std::string> column_names_ordered;
        cout<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
            <<"column adhering to same order within file: "<<std::boolalpha<<columnsAdheringAllToSameOrder(column_names, &column_names_ordered)<<std::endl;
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
            std::cout<<"   -- detecting majority case column count and names"<<std::endl;
            std::unordered_map<std::vector<std::string>, size_t> column_count_counts;
            for(auto names : column_names) {
                column_count_counts[names]++;
            }

            // majority case
            std::cout<<"   -- found "<<pluralize(column_count_counts.size(), "unique column name constellation")<<std::endl;

            auto most_frequent_count = 0;
            std::vector<std::string> most_frequent_names;
            for(const auto& el : column_count_counts)
                if(el.second > most_frequent_count) {
                    most_frequent_count = el.second;
                    most_frequent_names = el.first;
                }
            std::cout<<"   -- most common column names are: "<<most_frequent_names<<" ("
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
                entry.key = column_names[i][j];
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

        cout<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
            <<"found "<<pluralize(type_counts.size(), "different type")<<" in "<<path<<endl;

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
        cout<<"   -- most likely type is ("<<most_likely_pct<<"%, "<<type_count_pairs.front().second<<"x): "<<type_count_pairs.front().first.desc()<<endl;
        cout<<"   -- least likely type is ("<<least_likely_pct<<"%, "<<type_count_pairs.back().second<<"x): "<<type_count_pairs.back().first.desc()<<endl;

        auto general_case_max_type = maximizeTypeCover(type_count_pairs, conf_nc_threshold, true, conf_general_case_type_policy);
        auto normal_case_max_type = maximizeTypeCover(type_count_pairs, conf_nc_threshold, true, TypeUnificationPolicy::defaultPolicy());

        double num_rows_d = column_names.size() * 1.0;
        double normal_pct = normal_case_max_type.second / num_rows_d * 100.0;
        double general_pct = general_case_max_type.second / num_rows_d * 100.0;
        cout<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
            <<"maximizing type cover results in:"<<path<<endl;
        std::cout<<"   -- normal  case max type ("<<normal_pct<<"%, "<<normal_case_max_type.second<<"x): "<<normal_case_max_type.first.desc()<<std::endl;
        std::cout<<"   -- general case max type ("<<general_pct<<"%, "<<general_case_max_type.second<<"x): "<<general_case_max_type.first.desc()<<std::endl;

        // check how many rows would be on each path (normal, general, fallback)
        // check how many (of the original) rows adhere to this detected normal-case type
        // this also requires column name checking!
        size_t nc_count = 0;
        size_t gc_count = 0;
        size_t fb_count = 0;
        for(unsigned i = 0; i < row_types.size(); ++i) {
            if(python::canUpcastType(row_types[i], normal_case_max_type.first))
                nc_count++;
            else if(python::canUpcastType(row_types[i], general_case_max_type.first))
                gc_count++;
            else
                fb_count++;

        }
        cout<<"  "<<fixed<<setprecision(2)<<timer.time()<<"s: "
            <<"execution would result in the following paths taken:"<<path<<endl;
        cout<<"   -- "<<nc_count<<" / "<<row_types.size()<<" normal path ("<<(nc_count / (0.01 * row_types.size()))<<"%)"<<endl;
        cout<<"   -- "<<gc_count<<" / "<<row_types.size()<<" general path ("<<(gc_count / (0.01 * row_types.size()))<<"%)"<<endl;
        cout<<"   -- "<<fb_count<<" / "<<row_types.size()<<" fallback path ("<<(fb_count / (0.01 * row_types.size()))<<"%)"<<endl;

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

            // type counts (detailed)
            auto j_counts = nlohmann::json::array();
            for(auto kv : type_counts) {
                nlohmann::json tmp;
                tmp["type"] = kv.first.desc();
                tmp["count"] = kv.second;
                j_counts.push_back(tmp);
            }
            j["type_counts"] = j_counts;

            // to string
            json_string = j.dump();
        }

        return json_string;
    }

}

TEST(JSONUtils, CheckFiles) {
    using namespace tuplex;
    using namespace std;

    // for each file, run sampling stats
    string root_path = "/hot/data/flights_all/";
    string pattern = root_path + "flights_on_time_performance_*.csv";


    // test
    pattern = "../resources/*.json.gz";

    size_t num_files_found = 0;
    auto paths = glob(pattern);
    num_files_found = paths.size();
    cout<<"Found "<<pluralize(num_files_found, "file")<<" to analyze schema for."<<endl;

    auto path = paths[0];

    auto json_string = process_path(path);
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