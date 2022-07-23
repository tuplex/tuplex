//
// Created by kboonyap on 4/11/22.
//

#include <JsonStatistic.h>
#ifdef BUILD_WITH_AWS
#include <aws/core/external/cjson/cJSON.h>
#else
#include <cJSON.h>
#endif

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
        // we use ndjson, so finding a new line is as simple as searching forr '\n' or '\r\n', yet we can also be at
        // the beginning of a valid json object. Hence, basically try to parse and see whether it works!
        int64_t pos = 0;

        char ws[256];
        memset(ws, 0, sizeof(char) * 256);
        ws[' '] = 1;
        ws['\n'] = 1;
        ws['\t'] = 1;
        ws['\r'] = 1;

        // parse possible from the start?
        const char *end_ptr = nullptr;
        auto json_obj = cJSON_ParseWithLengthOpts(buf, buf_size, &end_ptr, false);
        if(json_obj) {
            cJSON_free(json_obj);

            // needs to newline (else partial parse to end...)
            if(*end_ptr == '\n' || *end_ptr == '\r' || *end_ptr == '\0')
                return 0;
        }

        while(pos < buf_size && buf[pos] != '\0') {
            if(buf[pos] == '\n' || buf[pos] == '\r') {
                // can we parse from the start (minus whitespace?)
                auto ptr = buf;
                auto buf_size_to_parse = pos + 1;
                while(ptr < buf + buf_size && ptr < buf + pos && ws[*ptr]) {
                    ptr++;
                    buf_size_to_parse--;
                }

                // parse cJSON from start of ptr
                json_obj = cJSON_ParseWithLengthOpts(ptr, buf_size_to_parse, &end_ptr, false);
                if(json_obj) {
                    cJSON_free(json_obj);
                    if(*end_ptr == '\n' || *end_ptr == '\r')
                        return buf - ptr;
                }

                // consume as much whitespace as possible
                ptr = buf + pos;
                while(ptr < buf + buf_size && ws[*ptr]) {
                    pos++;
                    ptr = buf + pos;
                }

                // can we parse now?
                json_obj = cJSON_ParseWithLengthOpts(ptr, buf_size - pos + 1, &end_ptr, false);
                if(json_obj) {
                    cJSON_free(json_obj);
                    if(*end_ptr == '\n' || *end_ptr == '\r' || *end_ptr == '\0')
                        return buf - ptr;
                }
                return pos;
            }

            pos++;
        }
        // not found
        return -1;
    }

    //@March: Implement, this function basically takes a buffer, needs to find the start of a valid JSON (i.e. \n{, \r\n{ if start[0] is not {)
    void JsonStatistic::estimate(const char *start, size_t size, bool disableNullColumns) {
        throw std::runtime_error("not yet implemented");
    }

    //@March: implement, columns present in json file
    std::vector<std::string> JsonStatistic::columns() const {
        throw std::runtime_error("not yet implemented");
    }

    //@March: implement, how many full rows are contained in the sample
    size_t JsonStatistic::rowCount() const {
        throw std::runtime_error("not yet implemented");
    }

    //@March: implement, normal-case type. I.e. specialized according to threshold
    python::Type JsonStatistic::type() const {
        throw std::runtime_error("not yet implemented");
    }

    //@March: implement, general-case type. I.e. type has to be a subtyoe of superType, i.e. canUpcastRowType(type(), superType()) must always hold
    python::Type JsonStatistic::superType() const {
        throw std::runtime_error("not yet implemented");
    }
}