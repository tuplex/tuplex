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
     * creates an escaped version of a JSON string.
     * @param str
     * @return escaped version.
     */
    extern std::string escape_for_json(const std::string& str);

}

#endif //TUPLEX_JSONUTILS_H