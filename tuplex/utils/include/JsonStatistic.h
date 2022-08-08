//
// Created by Leonhard Spiegelberg on 3/18/22.
//

#ifndef TUPLEX_JSONSTATISTIC_H
#define TUPLEX_JSONSTATISTIC_H

#include "CSVUtils.h"
#include <vector>
#include "TypeSystem.h"

#include <simdjson.h>

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
