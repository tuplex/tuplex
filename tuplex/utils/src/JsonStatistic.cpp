//
// Created by kboonyap on 4/11/22.
//

#include "JsonStatistic.h"

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
    int64_t findNLJsonStart(const char *buf, size_t buf_size) {

    }

    //@March: Implement, this function basically takes a buffer, needs to find the start of a valid JSON (i.e. \n{, \r\n{ if start[0] is not {)
    void JsonStatistic::estimate(const char *start, size_t size, bool disableNullColumns = false) {

    }

    //@March: implement, columns present in json file
    std::vector<std::string> JsonStatistic::columns() const {

    }

    //@March: implement, how many full rows are contained in the sample
    size_t JsonStatistic::rowCount() const {

    }

    //@March: implement, normal-case type. I.e. specialized according to threshold
    python::Type JsonStatistic::type() const {

    }

    //@March: implement, general-case type. I.e. type has to be a subtyoe of superType, i.e. canUpcastRowType(type(), superType()) must always hold
    python::Type JsonStatistic::superType() const {

    }
}