//
// Created by kboonyap on 4/11/22.
//

#include <JsonStatistic.h>
#ifdef BUILD_WITH_AWS
#include <aws/core/external/cjson/cJSON.h>
#else
#include <cJSON.h>
#endif

#include <JSONUtils.h>

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

    python::Type jsonTypeToPythonTypeRecursive(simdjson::simdjson_result<simdjson::fallback::ondemand::value> obj) {
        using namespace std;

        // is a nested field?
        if(obj.type() == simdjson::ondemand::json_type::object) {

            // json is always string keys -> value
            vector<python::StructEntry> kv_pairs;
            for(auto field : obj.get_object()) {
                // add to names
                auto sv_key = field.unescaped_key().value();
                std::string key = {sv_key.begin(), sv_key.end()};

                python::StructEntry entry;
                entry.key = escape_to_python_str(key);
                entry.keyType = python::Type::STRING;
                entry.valueType = jsonTypeToPythonTypeRecursive(field.value()); // recurse if necessary!
                kv_pairs.push_back(entry);
            }

            return python::Type::makeStructuredDictType(kv_pairs);
        }
        // is it an array? => only homogenous arrays supported!
        if(obj.type() == simdjson::ondemand::json_type::array) {

            // homogenous type?
            auto token = obj.raw_json_token().value();
            auto arr = obj.get_array();
            size_t arr_size = 0;

            // go through array and check that types are the same
            python::Type element_type;
            bool first = true;
            for(auto el : arr) {
                auto next_type = jsonTypeToPythonTypeRecursive(el);
                if(first) {
                    element_type = next_type;
                    first = false;
                }
                auto uni_type = unifyTypes(element_type, next_type);
                if(uni_type == python::Type::UNKNOWN)
                    return python::Type::makeListType(python::Type::PYOBJECT); // non-homogenous list
                element_type = uni_type;
                arr_size++;
            }

            if(arr_size == 0)
                return python::Type::EMPTYLIST;

            return python::Type::makeListType(element_type);
        }

        return jsonTypeToPythonTypeNonRecursive(obj.type(), obj.raw_json_token());
    }


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
                        return ptr - buf;
                }
                return pos;
            }

            pos++;
        }
        // not found
        return -1;
    }

    static Field stringToField(const std::string& s, const python::Type& type) {
        if(type == python::Type::NULLVALUE)
            return Field::null();
        if(type == python::Type::BOOLEAN) {
            assert(s == "true" || s == "false");
            return Field((bool)(s == "true"));
        }
        if(type == python::Type::I64) {
            int64_t i = 0;
            if(ecToI32(ExceptionCode::SUCCESS) == fast_atoi64(s.c_str(), s.c_str() + s.length(), &i)) {
                return Field(i);
            } else
                throw std::runtime_error("integer parse error for field " + s);
        }
        if(type == python::Type::F64) {
            double d = 0.0;
            if(ecToI32(ExceptionCode::SUCCESS) == fast_atod(s.c_str(), s.c_str() + s.length(), &d)) {
                return Field(d);
            } else
                throw std::runtime_error("double parse error for field " + s);
        }
        if(type == python::Type::STRING)
            return Field(s);

        if(type.isDictionaryType())
            return Field::from_str_data(s, type);

        throw std::runtime_error("unsupported primitive type " + type.desc());
    }


    std::string jsonDictFromBuffer(const char* buf, size_t buf_size, int numBraces=0) {
        // parse till either buf is exhausted or numStartBraces hits 0
        size_t pos = 0;
        auto startBraces = std::max(0, numBraces);
        while(pos < buf_size) {
            char c = buf[pos];

            // what field is char?
            if(c == '\"') {
                // start of escaped field, skip till end of it!
                pos++;
                while(pos < buf_size && buf[pos] != '\"') {
                    if(pos + 1 < buf_size && buf[pos] == '\\' && buf[pos + 1] == '\"')
                        pos += 2;
                    else
                        pos++;
                }
                pos++;
            } else if(c == '{') {
                numBraces++;
                pos++;
            } else if(c == '}') {
                numBraces--;
                pos++;
            } else {
                pos++;
            }

            // braces done?
            if(0 == numBraces) {
                // return string!
                char *rc = new char[startBraces + pos + 1];
                memset(rc, 0, startBraces + pos + 1);
                for(unsigned i = 0; i < startBraces; ++i)
                    rc[i] = '{';
                memcpy(rc + startBraces, buf, pos);

                std::string res(rc);
                delete [] rc;
                return res;
            }
        }
        // exhausted, no positive result...

        return "";
    }

    std::vector<Row> parseRowsFromJSON(const char* buf,
                                       size_t buf_size,
                                       std::vector<std::vector<std::string>>* outColumnNames,
                                       bool unwrap_rows) {
        using namespace std;

        assert(buf[buf_size - 1] == '\0');

        auto SIMDJSON_BATCH_SIZE=simdjson::dom::DEFAULT_BATCH_SIZE;

        // use simdjson as parser b.c. cJSON has issues with integers/floats.
        // https://simdjson.org/api/2.0.0/md_doc_iterate_many.html
        simdjson::ondemand::parser parser;
        simdjson::ondemand::document_stream stream;
        auto error = parser.iterate_many(buf, buf_size, std::min(buf_size, SIMDJSON_BATCH_SIZE)).get(stream);
        if(error) {
            stringstream err_stream; err_stream<<error;
            throw std::runtime_error(err_stream.str());
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

        // cf. Tree Walking and JSON Element Types in https://github.com/simdjson/simdjson/blob/master/doc/basics.md
        // use scalar() => for simple numbers etc.

        // anonymous row? I.e., simple value?
        for(auto it = stream.begin(); it != stream.end(); ++it) {
            auto doc = (*it);

            // the full json string of a row can be obtained via
            // std::cout << it.source() << std::endl;
            string full_row;
            {
                 stringstream ss;
                 ss<<it.source()<<std::endl;
                 full_row = ss.str();
            }


            // error? stop parse, return partial results
            if(doc.type().error())
                break;

            auto line_type = doc.type().value();
            line_types[line_type]++;

            std::vector<Field> fields;

            // type of doc
            switch(line_type) {
                case simdjson::ondemand::json_type::object: {
                    auto obj = doc.get_object();

                    // key names == column names
                    vector<string> row_column_names;
                    vector<string> row_json_strings;
                    vector<python::Type> row_field_types;

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

                        // generic types? -> recurse!
                        if(py_type == python::Type::GENERICDICT || py_type == python::Type::GENERICLIST) {
                            py_type = jsonTypeToPythonTypeRecursive(field.value());
                        }

                        // add to count array
                        type_counts[std::make_tuple(column_index_lookup_table[key], py_type)]++;

                        // save raw data for more info
                        row_column_names.push_back(key);

                        // fetch data for field (i.e., raw string)
                        auto sv_value = simdjson::to_json_string(obj[key]).value();

                        std::string str_value(sv_value.begin(), sv_value.end());

                        row_json_strings.push_back(str_value);
                        // cf. https://github.com/simdjson/simdjson/blob/master/doc/basics.md#direct-access-to-the-raw-string
                        // --> however this works only for non-array/non-object data.
                        row_field_types.push_back(py_type);
                    }

                    // output column names if desired & save row
                    if(outColumnNames) {
                        outColumnNames->push_back(row_column_names);
                    }
                    vector<Field> fields;
                    for(unsigned i = 0; i < row_field_types.size(); ++i) {
                        // can't be opt type -> primitives are parsed!
                        assert(!row_field_types[i].isOptionType());
                        if(row_field_types[i].isDictionaryType()) {
                            // dict field?
                            // simple, use json dict
                            fields.push_back(Field::from_str_data(row_json_strings[i], row_field_types[i]));
                        } else if(row_field_types[i].isListType()) {
                            // list field?
                            // TODO
                            throw std::runtime_error("field type list not yet implemented, todo");
                        } else {
                            // convert primitive string to field
                            fields.push_back(stringToField(unescape_json_string(row_json_strings[i]), row_field_types[i]));
                        }

                    }

                    if(unwrap_rows)
                        rows.push_back(Row::from_vector(fields));
                    else {
                        // push back as struct type
                        std::string json_line = full_row;
                        std::vector<python::StructEntry> kv_pairs;
                        assert(fields.size() == row_column_names.size());
                        for(unsigned i = 0; i < fields.size(); ++i) {
                            python::StructEntry entry;
                            entry.key = escape_to_python_str(row_column_names[i]);
                            entry.keyType = python::Type::STRING;
                            entry.valueType = fields[i].getType();
                            kv_pairs.push_back(entry);
                        }
                        python::Type struct_type = python::Type::makeStructuredDictType(kv_pairs);
                        Row struct_row({Field::from_str_data(json_line, struct_type)});
                        rows.push_back(struct_row);
                    }

                    break;
                }
                case simdjson::ondemand::json_type::array: {
                    // unknown, i.e. error line.
                    // header? -> first line?

                    // @TODO: not yet fully implemented!!!
                    throw std::runtime_error("not yet fully implemented...");

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
                    break;
                }
                default: {
                    // basic element -> directly map type!
                    auto py_type = jsonTypeToPythonTypeNonRecursive(doc.type().value(), doc.raw_json_token());
                    break;
                }
            }
            first_row = true;
        }

        return rows;
    }

    void JsonStatistic::walkJSONTree(std::unique_ptr<JSONTypeNode>& node, cJSON* json) {
        if(!json || !node)
            return;

        // check type of JSON, recurse if necessary
        if(cJSON_IsNull(json)) {
           node->inc_type(python::Type::NULLVALUE);
        } else if(cJSON_IsBool(json)) {
            node->inc_type(python::Type::BOOLEAN);
        } else if(cJSON_IsNumber(json)) {
            // parse...

        } else if(cJSON_IsString(json)) {
            node->inc_type(python::Type::STRING);
        } else if(cJSON_IsArray(json)) {
            // only homogenous arrays supported!
        } else if(cJSON_IsObject(json)) {
          // recurse..
        }
    }

    //@March: Implement, this function basically takes a buffer, needs to find the start of a valid JSON (i.e. \n{, \r\n{ if start[0] is not {)
    void JsonStatistic::estimate(const char *start, size_t size, bool disableNullColumns) {

        // find start offset (limited to size)
        auto start_offset = findNLJsonStart(start, size);
        if(start_offset < 0)
            throw std::runtime_error("Could not find start of valid JSON document in buffer of size " + std::to_string(size));

        // start parsing at start + offset -> count then types/fields in tree structure
        const char *end_ptr = nullptr;
        auto buf = start + start_offset;
        auto buf_size = size - start_offset;
        auto json_obj = cJSON_ParseWithLengthOpts(buf, buf_size, &end_ptr, false);
        while(json_obj) {
            // count types...

            cJSON_free(json_obj);

            // parse next object
            auto parsed_bytes = end_ptr - buf;
            buf += parsed_bytes;
            buf_size -= parsed_bytes;
            json_obj = cJSON_ParseWithLengthOpts(buf, buf_size, &end_ptr, false);
        }
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