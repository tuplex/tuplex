//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_JSONUTILS_H
#define TUPLEX_JSONUTILS_H

#ifdef BUILD_WITH_AWS
#include <aws/core/external/cjson/cJSON.h>
#else
#include <cJSON.h>
#endif
#include <vector>
#include "Base.h"
#include "Utils.h"
#include <nlohmann/json.hpp>

#include <TypeSystem.h>
#include <TypeHelper.h>

namespace tuplex {

    /*!
     * converts an array of strings to a string in JSON representation
     * @param array
     * @return JSON string containing the string array
     */
    inline std::string stringArrayToJSON(const std::vector<std::string>& array) {

        // special case: empty
        if(array.empty())
            return "[]";

        nlohmann::json j;
        for(auto s: array) {
            j.push_back(s);
        }
        return j.dump();
    }

    /*!
     * convert string to vector of strings
     * @param s
     * @return
     */
    inline std::vector<std::string> jsonToStringArray(const std::string& s) {
        auto j = nlohmann::json::parse(s);

        if(!j.is_array())
            throw std::runtime_error("json object is not an array!");

        std::vector<std::string> v;
        for(auto it = j.begin(); it != j.end(); ++it) {
            if(!it->is_string()) {
                throw std::runtime_error("array element is not string");
            }

            v.push_back(it->get<std::string>());
        }
        return v;
    }

    inline std::unordered_map<std::string, std::string> jsonToMap(const std::string &s) {
        std::unordered_map<std::string, std::string> m;

        // empty string? return empty map!
        if(s.empty())
            return m;

        auto j = nlohmann::json::parse(s);

        for (auto &el : j.items()) {

            if(el.value().is_null())
                m[el.key()] = "null";
            else if(el.value().is_string())
                m[el.key()] = el.value().get<std::string>();
            else if(el.value().is_boolean())
                m[el.key()] = boolToString(el.value().get<bool>());
            else if(el.value().is_number_integer())
                m[el.key()] = std::to_string(el.value().get<int64_t>());
            else if(el.value().is_number_float())
                m[el.key()] = std::to_string(el.value().get<double>());
            else {
                m[el.key()] = el.value().dump();
            }
        }

        return m;
    }

    /*!
     * decodes the string value of a JSON escaped string
     * @param json_string JSON escaped string (must start with ")
     * @return decoded string
     */
    extern std::string unescape_json_string(const std::string& json_string);

    /*!
     * encodes a string as JSON escaped string
     * @param string
     * @return JSON escaped string.
     */
    extern std::string escape_json_string(const std::string& string);


    ///////// experimental JSON features ///////////////////////
    /*!
    * maximizeTypeCover computes the most likely type by potentially fusing types together.
    * @param counts
    * @param threshold
    * @param use_nvo
    * @param t_policy
    * @return
    */
    inline std::pair<python::Type, size_t> maximizeTypeCover(const std::vector<std::pair<python::Type, size_t>>& counts,
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

    /*!
     * prints json struct type neatly out
     * @param type
     * @param spaces_per_level
     * @return string
     */
    inline std::string prettyPrintStructType(const python::Type& type, const size_t spaces_per_level=2) {
        std::stringstream ss;

        std::string indent;
        for(unsigned i = 0; i < spaces_per_level; ++i) {
            indent += " ";
        }

        // is it struct type?
        if(!type.isStructuredDictionaryType())
            return type.desc();
        else {
            // go through pairs, and create JSON structure
            ss<<"{\n";
            for(unsigned i = 0; i < type.get_struct_pairs().size(); ++i) {
                auto kv  = type.get_struct_pairs()[i];
                if(kv.keyType == python::Type::STRING) {
                    auto unescaped_key = str_value_from_python_raw_value(kv.key);
                    auto json_key = escape_json_string(unescaped_key);

                    ss<<indent<<json_key<<":";
                } else {
                    ss<<indent<<kv.key<<": ";
                }

                if(!kv.alwaysPresent)
                    ss<<" (maybe) ";

                // check what type the other stuff is
                auto subtype = prettyPrintStructType(kv.valueType);
                trim(subtype);
                subtype = core::prefixLines(subtype, indent);
                trim(subtype);

                // prefix lines with indent (except first one?)
                auto comma = i != type.get_struct_pairs().size() - 1 ? "," : "";
                ss<<subtype<<comma<<"\n";
            }
            ss<<"}";
        }
        return ss.str();
    }


}

#endif //TUPLEX_JSONUTILS_H