//
// Created by Leonhard Spiegelberg on 3/18/22.
//

#ifndef TUPLEX_JSONSTATISTIC_H
#define TUPLEX_JSONSTATISTIC_H

#include "CSVUtils.h"
#include <vector>
#include "TypeSystem.h"

#include <simdjson.h>
#include <TypeHelper.h>

namespace tuplex {

    //@March implement: finding Json Offset
    //@March + write tests.
    // we need this function to chunk JSON files later.
    /*!
     * finds the start of a valid newline-delimited JSON entry.
     * @param buf buffer
     * @param buf_size size in bytes of buffer (not necessarily '\0' terminated)
     * @return offset from start of buf to first valid line entry, -1 if not found.
     */
    int64_t findNLJsonStart(const char* buf, size_t buf_size);

    /*!
     * maps a single primitive value to a python type (non-recursing_
     * @param jtype
     * @param value
     * @return tuplex python type
     */
    extern python::Type jsonTypeToPythonTypeNonRecursive(const simdjson::ondemand::json_type& jtype, const std::string_view& value);

    /*!
     * recursively maps a json field to a python type value, recurses on arrays and objects. Arrays are
     * mapped to List[PyObject] if they're not homogenous (i.e. all elements have the same type)
     * @param obj
     * @return python type
     */
    extern python::Type jsonTypeToPythonTypeRecursive(simdjson::simdjson_result<simdjson::fallback::ondemand::value> obj);

    /*!
     * parses rows from buf (newline delimited json) as tuplex rows,
     * assigning detected type (StructType) per individual row.
     * @param buf
     * @param buf_size
     * @param outColumnNames vector of string vectors storing the column names for each individual row if desired. If top-level is given as [...] takes either the first rows names or empty string.
     * @return vector of Rows with types assigned.
     */
    extern std::vector<Row> parseRowsFromJSON(const char* buf, size_t buf_size, std::vector<std::vector<std::string>>* outColumnNames=nullptr);

    inline std::vector<Row> parseRowsFromJSON(const std::string& s, std::vector<std::vector<std::string>>* outColumnNames=nullptr) {
        return parseRowsFromJSON(s.c_str(), s.size() + 1, outColumnNames);
    }

    // --> put implementation of this into JsonStatistic.cc file in utils/src/JsonStatistic.cc

    // for this https://github.com/LeonhardFS/Tuplex/pull/82/files
    // may be helpful
    class JsonStatistic {
    public:
        JsonStatistic(double threshold, const std::vector<std::string>& null_values=std::vector<std::string>{""}) : _threshold(threshold), _null_values(null_values) {}

        // This function basically takes a buffer, needs to find the start of a valid JSON (i.e. \n{, \r\n{ if start[0] is not {)
        void estimate(const char* start, size_t size, bool disableNullColumns=false);

        //@March: implement, columns present in json file
        std::vector<std::string> columns() const;

        //@March: implement, how many full rows are contained in the sample
        size_t rowCount() const;

        //@March: implement, normal-case type. I.e. specialized according to threshold
        python::Type type() const;
        //@March: implement, general-case type. I.e. type has to be a subtyoe of superType, i.e. canUpcastRowType(type(), superType()) must always hold
        python::Type superType() const;
    private:
        double _threshold;
        std::vector<std::string> _null_values;

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

        std::unique_ptr<JSONTypeNode> _root;

        void walkJSONTree(std::unique_ptr<JSONTypeNode>& node, cJSON* json);

    };
}

#endif //TUPLEX_JSONSTATISTIC_H