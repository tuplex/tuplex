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


    python::Type jsonTypeToPythonTypeRecursive(simdjson::dom::element obj,
                                               bool interpret_heterogenous_lists_as_tuples) {
        switch(obj.type()) {
            case simdjson::dom::element_type::STRING: {
                return python::Type::STRING;
            }
            case simdjson::dom::element_type::NULL_VALUE: {
                return python::Type::NULLVALUE;
            }
            case simdjson::dom::element_type::BOOL: {
                return python::Type::BOOLEAN;
            }
            case simdjson::dom::element_type::UINT64:
            case simdjson::dom::element_type::INT64: {
                return python::Type::I64;
            }
            case simdjson::dom::element_type::DOUBLE: {
                return python::Type::F64;
            }
            case simdjson::dom::element_type::OBJECT: {
                // json is always string keys -> value
                std::vector<python::StructEntry> kv_pairs;
                auto object = obj.get_object().value();
                for(auto kv : object) {
                    // add to names
                    auto sv_key = kv.key;
                    auto field = kv.value;
                    std::string key = {sv_key.begin(), sv_key.end()};

                    python::StructEntry entry;
                    entry.key = escape_to_python_str(key);
                    entry.keyType = python::Type::STRING;
                    entry.valueType = jsonTypeToPythonTypeRecursive(field, interpret_heterogenous_lists_as_tuples); // recurse if necessary!
                    kv_pairs.push_back(entry);
                }

                return python::Type::makeStructuredDictType(kv_pairs);
                break;
            }
            case simdjson::dom::element_type::ARRAY: {
                auto arr = obj.get_array().value();

                 // go through array and check that types are the same
                python::Type element_type;
                bool first = true;
                std::vector<python::Type> element_types;
                unsigned arr_size = 0;
                for(const auto& el : arr) {
                    auto next_type = jsonTypeToPythonTypeRecursive(el, interpret_heterogenous_lists_as_tuples);
                    element_types.push_back(next_type);
                    if(first) {
                        element_type = next_type;
                        first = false;
                    }
                    auto uni_type = unifyTypes(element_type, next_type);
                    if(uni_type == python::Type::UNKNOWN) {
                        // special case: tuple?
                        if(!interpret_heterogenous_lists_as_tuples)
                            return python::Type::makeListType(python::Type::PYOBJECT); // non-homogenous list
                    }

                    element_type = uni_type;
                    arr_size++;
                }

                if(arr_size == 0)
                    return python::Type::EMPTYLIST;

                // solve special case tuple for heterogenous list
                if(interpret_heterogenous_lists_as_tuples && element_type == python::Type::UNKNOWN) {
                    return python::Type::makeTupleType(element_types);
                } else {
                    assert(element_type != python::Type::UNKNOWN);
                    return python::Type::makeListType(element_type);
                }

                // tuple or list
                break;
            }
        }
        return python::Type::UNKNOWN;
    }

    python::Type jsonTypeToPythonTypeRecursive(simdjson::simdjson_result<simdjson::ondemand::value> obj,
                                               bool interpret_heterogenous_lists_as_tuples) {
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
                entry.valueType = jsonTypeToPythonTypeRecursive(field.value(), interpret_heterogenous_lists_as_tuples); // recurse if necessary!
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
            std::vector<python::Type> element_types;
            for(auto el : arr) {
                auto next_type = jsonTypeToPythonTypeRecursive(el, interpret_heterogenous_lists_as_tuples);
                element_types.push_back(next_type);
                if(first) {
                    element_type = next_type;
                    first = false;
                }
                auto uni_type = unifyTypes(element_type, next_type);
                if(uni_type == python::Type::UNKNOWN) {
                    // special case: tuple?
                    if(!interpret_heterogenous_lists_as_tuples)
                        return python::Type::makeListType(python::Type::PYOBJECT); // non-homogenous list
                }

                element_type = uni_type;
                arr_size++;
            }

            if(arr_size == 0)
                return python::Type::EMPTYLIST;

            // solve special case tuple for heterogenous list
            if(interpret_heterogenous_lists_as_tuples && element_type == python::Type::UNKNOWN) {
                return python::Type::makeTupleType(element_types);
            } else {
                assert(element_type != python::Type::UNKNOWN);
                return python::Type::makeListType(element_type);
            }
        }

        return jsonTypeToPythonTypeNonRecursive(obj.type(), obj.raw_json_token());
    }

    int64_t findNLJsonOffsetToNextLine(const char *buf, size_t buf_size) {
        // we use ndjson, so finding a new line is as simple as searching forr '\n' or '\r\n', yet we can also be at
        // the beginning of a valid json object. Hence, basically try to parse and see whether it works!
        int64_t pos = 0;

        char ws[256];
        memset(ws, 0, sizeof(char) * 256);
        ws[' '] = 1;
        ws['\n'] = 1;
        ws['\t'] = 1;
        ws['\r'] = 1;

        // skip whitespace?
        while(ws[buf[pos]] && pos < buf_size)
            pos++;


        // perform two step parsing process. First, limit buf size to 1MB - if that fails, use full buffer...
        size_t limit_size = 1024 * 1024; // 1MB
        if(buf_size - pos > limit_size) {
            // check first using snippet.

            const char *end_ptr = nullptr;
            auto json_obj = cJSON_ParseWithLengthOpts(buf + pos, limit_size, &end_ptr, false);
            if (json_obj) {
                cJSON_free(json_obj);

                // needs to newline (else partial parse to end...)
                if (*end_ptr == '\n' || *end_ptr == '\r' || *end_ptr == '\0')
                    return end_ptr - buf;
            } else {
                // full buffer parse (may conflict with rt runtime memory!)
                end_ptr = nullptr;
                auto json_obj = cJSON_ParseWithLengthOpts(buf + pos, buf_size - pos, &end_ptr, false);
                if (json_obj) {
                    cJSON_free(json_obj);

                    // needs to newline (else partial parse to end...)
                    if (*end_ptr == '\n' || *end_ptr == '\r' || *end_ptr == '\0')
                        return end_ptr - buf;
                }
                return -1;
            }
            return -1;

        } else {
            // parse direct
            // parse possible from the start?
            const char *end_ptr = nullptr;
            auto json_obj = cJSON_ParseWithLengthOpts(buf + pos, buf_size - pos, &end_ptr, false);
            if (json_obj) {
                cJSON_free(json_obj);

                // needs to newline (else partial parse to end...)
                if (*end_ptr == '\n' || *end_ptr == '\r' || *end_ptr == '\0')
                    return end_ptr - buf;
            }
            return -1;
        }
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
            return Field(unescape_json_string(s));

        if(type.isDictionaryType()) {
            if(type == python::Type::EMPTYDICT)
                return Field::empty_dict();

            return Field::from_str_data(s, type);
        }

        if(type.isTupleType()) {
            if(type == python::Type::EMPTYTUPLE)
                return Field::empty_tuple();

            // parse using simdjson and then transform
            simdjson::dom::parser parser;
            auto arr = parser.parse(s);
            if(!arr.is_array())
                throw std::runtime_error("given string is not an array, can't convert json str to field.");
            std::vector<Field> elements;
            unsigned pos = 0;
            for(const auto& el : arr) {
                auto element_type = type.parameters()[pos];
                auto el_str = simdjson::minify(el);
                elements.push_back(stringToField(el_str, element_type));
                pos++;
            }
            return Field(Tuple::from_vector(elements));
        }

        if(type.isListType()) {
            if(type == python::Type::EMPTYLIST)
                return Field::empty_list();

            // parse using simdjson and then transform
            simdjson::dom::parser parser;
            auto arr = parser.parse(s);
            if(!arr.is_array())
                throw std::runtime_error("given string " + s + "is not an array, can't convert json str to field.");
            std::vector<Field> elements;
            unsigned pos = 0;

            auto element_type = type.elementType();
            for(const auto& el : arr) {
                auto el_str = simdjson::minify(el);
                auto el_field = stringToField(el_str, element_type);
                elements.push_back(el_field);
                pos++;
            }
            return Field(List::from_vector(elements));
        }

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

    /*!
     * parse into homogenous list. If failure, returns null
     */
    Field json_array_to_list(const std::string& json_str, const python::Type& list_type, bool interpret_heterogenous_lists_as_tuples) {
        auto& logger = Logger::instance().logger("json");

        assert(list_type.isListType());
        if(list_type == python::Type::EMPTYLIST)
            return Field::empty_list();

        simdjson::dom::parser parser;
        auto arr = parser.parse(json_str);
        if(!arr.is_array()) {
            logger.error("given json str is not an array.");
            return Field::null();
        }
        std::vector<Field> elements;

        for(auto field : arr) {
            auto raw_json_str = simdjson::minify(field);
            auto py_type =  jsonTypeToPythonTypeRecursive(field, interpret_heterogenous_lists_as_tuples);
            if(py_type != list_type.elementType()) {
                logger.error("type of element does not match homogenous list's element type");
                return Field::null();
            }
            elements.push_back(stringToField(raw_json_str, py_type));
        }

        return Field(List::from_vector(elements));
    }

    std::vector<Row> parseRowsFromJSON(const char* buf,
                                       size_t buf_size,
                                       std::vector<std::vector<std::string>>* outColumnNames,
                                       bool unwrap_rows,
                                       bool interpret_heterogenous_lists_as_tuples,
                                       size_t max_rows) {
        using namespace std;

        if(0 == max_rows)
            return {};

        // assert(buf[buf_size - 1] == '\0');

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
        for(auto it = stream.begin(); it != stream.end() && rows.size() <= max_rows; ++it) {
            auto doc = (*it);

            // error? stop parse, return partial results
            simdjson::ondemand::json_type doc_type;
            doc.type().tie(doc_type, error);
            if(error)
                break;

            // the full json string of a row can be obtained via
            // std::cout << it.source() << std::endl;
            string full_row;
            {
                 stringstream ss;
                 ss<<it.source()<<std::endl;
                 full_row = ss.str();
            }

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
                            py_type = jsonTypeToPythonTypeRecursive(field.value(), interpret_heterogenous_lists_as_tuples);
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
                            // list field -> need to parse into individual fields (homogenous) and then create proper List out of this.
                            auto list_field = json_array_to_list(row_json_strings[i], row_field_types[i], interpret_heterogenous_lists_as_tuples);
                            if(list_field.getType() == python::Type::NULLVALUE) {
                                Logger::instance().defaultLogger().warn("Found non-conforming list, adding null as dummy-value.");
                            }
                            fields.push_back(list_field);
                        } else {
                            // convert primitive string to field
                            fields.push_back(stringToField(row_json_strings[i], row_field_types[i]));
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

    std::tuple<std::vector<Row>, std::vector<std::string>> sortRowsAndIdentifyColumns(const std::vector<Row> &rows,
                                                                                      const std::vector<std::vector<std::string>> &columnNames) {
        if(rows.empty()) {
            return std::make_tuple(std::vector<Row>(), std::vector<std::string>());
        }

        std::vector<std::string> ordered_names;
        std::set<std::string> set_names;
        std::vector<Row> sorted_rows;
        for (const auto &names: columnNames) {
            for (const auto &name: names) {
                auto it = set_names.find(name);
                if (it == set_names.end()) {
                    set_names.insert(name);
                    ordered_names.push_back(name);
                }
            }
        }

        sorted_rows.reserve(rows.size());

        // go through rows and sort them after the ordered_names array!
        std::unordered_map<std::string, unsigned> m;
        for(unsigned i = 0; i < ordered_names.size(); ++i)
            m[ordered_names[i]] = i;

        std::vector<std::pair<unsigned, Field>> fields_with_prio;
        std::vector<Field> fields;
        fields_with_prio.reserve(ordered_names.size());
        fields.reserve(ordered_names.size());
        for(unsigned i = 0; i < rows.size(); ++i) {
            const auto& row = rows[i];
            assert(row.getNumColumns() == columnNames[i].size());

            // resorted field
            fields_with_prio.clear(); // reuse mem
            fields.clear();
            for(unsigned j = 0; j < row.getNumColumns(); ++j) {
                auto name = columnNames[i][j];
                auto idx = m.at(name); // check field idx
                fields_with_prio.push_back(std::make_pair(idx, row.get(j)));
            }
            // sort after prio
            std::sort(fields_with_prio.begin(), fields_with_prio.end(), [](const std::pair<unsigned, Field>& lhs,
                    const std::pair<unsigned, Field>& rhs) {
                return lhs.first < rhs.first;
            });

            // fill in with Nulls if missing...
            auto jt = fields_with_prio.begin();
            for(unsigned j = 0; j < ordered_names.size(); ++j) {
                if(jt != fields_with_prio.end() && jt->first == j) {
                    fields.push_back(jt->second);
                    ++jt;
                } else {
                    // push back NULL
                    fields.push_back(Field::null());
                }
            }

            // add to sorted rows
            sorted_rows.push_back(Row::from_vector(fields));
        }

        return std::make_tuple(sorted_rows, ordered_names);
    }
}