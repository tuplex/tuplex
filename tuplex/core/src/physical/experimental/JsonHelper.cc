//
// Created by leonhard on 9/23/22.
//
#include <physical/experimental/JsonHelper.h>
#include <RuntimeInterface.h>
#include <JITCompiler.h>

namespace tuplex {
    namespace codegen {
        // C-APIs to use in codegen
        JsonParser *JsonParser_init() {
            // can't malloc, or can malloc but then need to call inplace C++ constructors!
            return new JsonParser();
        }

        void JsonParser_free(JsonParser *parser) {
            if (parser)
                delete parser;
            parser = nullptr;
        }

        int64_t JsonParser_TruncatedBytes(JsonParser* parser) {
            assert(parser);
            return parser->stream.truncated_bytes();
        }

        uint64_t JsonParser_open(JsonParser *j, const char *buf, size_t buf_size) {
            assert(j);

            simdjson::error_code error;
            // // ondemand
            // j->parser.iterate_many(buf, buf_size, std::min(buf_size, SIMDJSON_BATCH_SIZE)).tie(j->stream, error);

            // dom
            j->parser.parse_many(buf, buf_size, std::min(buf_size, SIMDJSON_BATCH_SIZE)).tie(j->stream, error);

            if (error) {
                std::stringstream err_stream;
                err_stream << error;
                j->lastError = err_stream.str();
                return ecToI64(ExceptionCode::JSONPARSER_ERROR);
            }

            // set internal iterator
            j->it = j->stream.begin();

            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonParser_close(JsonParser *j) {
            assert(j);

            j->it = j->stream.end();

            return ecToI64(ExceptionCode::SUCCESS);
        }

        bool JsonParser_hasNextRow(JsonParser *j) {
            assert(j);
            return j->stream.end() != j->it;
        }

        bool JsonParser_moveToNextRow(JsonParser *j) {
            assert(j);
            ++j->it;
            return j->stream.end() != j->it;
        }

        /*!
         * get current row (malloc copy) (could also have rtmalloc copy).
         * Whoever requests this row, has to free it then. --> this function is required for badparsestringinput.
         */
        char *JsonParser_getMallocedRow(JsonParser *j) {
            using namespace std;

            assert(j);
            string full_row;
            stringstream ss;
            ss << j->it.source();
            ss.flush();
            full_row = ss.str();
            char *buf = (char *) malloc(full_row.length() + 1);
            if (buf) {
                memcpy(buf, full_row.c_str(), full_row.length());
                buf[full_row.length()] = '\0';
            }
            return buf;
        }

        char *JsonParser_getMallocedRowAndSize(JsonParser *j, int64_t* size) {
            using namespace std;

            assert(j);
            string full_row;
            stringstream ss;
            ss << j->it.source();
            ss.flush();
            full_row = ss.str();
            char *buf = (char *) malloc(full_row.length() + 1);
            if (buf) {
                memcpy(buf, full_row.c_str(), full_row.length());
                buf[full_row.length()] = '\0';
            }

            if(size && buf)
                *size = full_row.length() + 1;
            return buf;
        }

        uint64_t JsonParser_getDocType(JsonParser *j) {
            assert(j);
            // i.e. simdjson::ondemand::json_type::object:
            // or simdjson::ondemand::json_type::array:
            // => if it doesn't conform, simply use badparse string input?
            if (!(j->it != j->stream.end()))
                return 0xFFFFFFFF;

            // some weird error may happen IFF batch size is too small.
            // -> out of capacity. Could be detected here and then by manually hacking the iterator
            // @TODO.

            simdjson::error_code error;

            auto doc = *j->it;
            simdjson::dom::element_type line_type;
            doc.type().tie(line_type, error);
            if(error) {

                // special case: empty line
                if(simdjson::error_code::TAPE_ERROR == error) {
                    // can line be retrieved?
                    // -> do so.
                    // make sure 0 is not used...
                    return 0;
                }

                std::stringstream ss;
                ss<<error; // <-- can get error description like this

                // // can line retrieved?
                // std::string full_row;
                // {
                //     std::stringstream ss;
                //     ss << j->it.source() << std::endl;
                //     full_row = ss.str();
                // }
                throw std::runtime_error(ss.str());
            }

            return static_cast<uint64_t>(line_type);
        }


        // C API
        void JsonItem_Free(JsonItem *i) {
            delete i; //--> bad: error here!
            i = nullptr;
        }

        uint64_t JsonParser_getObject(JsonParser *j, JsonItem **out) {
            assert(j);
            assert(out);

            auto doc = *j->it;

            // on demand
            // assert(doc.value().type().take_value() == simdjson::ondemand::json_type::object);

            // check type
            simdjson::error_code error;
            simdjson::dom::element_type t;
            simdjson::dom::object object;
            doc.get_object().tie(object, error); // no error check here (for speed reasons).
            if(error) {
                return ecToI64(ExceptionCode::BADPARSE_STRING_INPUT);
            }

            auto o = new JsonItem();

            // debug checks:
            assert(j->it != j->stream.end());

            //dom
            assert(doc.value().type() == simdjson::dom::element_type::OBJECT);

            o->o = std::move(object);

            *out = o;

            return ecToI64(ExceptionCode::SUCCESS);
        }

        inline uint64_t translate_simdjson_error(const simdjson::error_code &error) {
            if (simdjson::NO_SUCH_FIELD == error)
                return ecToI64(ExceptionCode::KEYERROR);
            if (simdjson::INCORRECT_TYPE == error)
                return ecToI64(ExceptionCode::TYPEERROR);
            //if(simdjson::N)
            return ecToI64(ExceptionCode::JSONPARSER_ERROR);
        }

        // get string item and save to rtmalloced string!
        uint64_t JsonItem_getString(JsonItem *item, const char *key, char **out) {
            assert(item);
            assert(key);
            assert(out);

            simdjson::error_code error;
            std::string_view sv_value;
            item->o[key].get_string().tie(sv_value, error);
            if (error)
                return translate_simdjson_error(error);

            auto str_size = 1 + sv_value.size();
            char *buf = (char *) runtime::rtmalloc(str_size);
            for (unsigned i = 0; i < sv_value.size(); ++i)
                buf[i] = sv_value.at(i);
            buf[sv_value.size()] = '\0';
            *out = buf;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonItem_getStringAndSize(JsonItem *item, const char *key, char **out, int64_t *size) {
            assert(item);
            assert(key);
            assert(out);

            simdjson::error_code error;
            std::string_view sv_value;
            item->o[key].get_string().tie(sv_value, error);
            if (error)
                return translate_simdjson_error(error);

            auto str_size = 1 + sv_value.size();
            char *buf = (char *) runtime::rtmalloc(str_size);
            for (unsigned i = 0; i < sv_value.size(); ++i)
                buf[i] = sv_value.at(i);
            buf[sv_value.size()] = '\0';
            *out = buf;
            *size = sv_value.size() + 1;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonItem_getObject(JsonItem *item, const char *key, JsonItem **out) {
            assert(item);
            assert(key);
            assert(out);

            simdjson::error_code error;
            // on demand
            // simdjson::ondemand::object o;
            // dom
            simdjson::dom::object o;

            item->o[key].get_object().tie(o, error);
            if (error)
                return translate_simdjson_error(error);
            // ONLY allocate if ok. else, leave how it is.
            auto obj = new JsonItem();
            obj->o = std::move(o);
            *out = obj;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonItem_getArray(JsonItem *item, const char *key, JsonArray **out) {
            assert(item);
            assert(key);
            assert(out);

            simdjson::error_code error;
            // on demand -> out of order execution unless objects are then directly traversed!
            // simdjson::ondemand::array a;

            // dom
            simdjson::dom::array a;
            item->o[key].get_array().tie(a, error);
            if (error)
                return translate_simdjson_error(error);

            // ONLY allocate if ok. else, leave how it is.
            auto arr = new JsonArray();

            // decode array
            for(auto element : a) {
                // arr->elements.emplace_back(item);
                arr->elements.emplace_back(element);
            }
            *out = arr;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        void JsonArray_Free(JsonArray* arr) {
            if(arr)
                delete arr;
            arr = nullptr;
        }

        uint64_t JsonArray_Size(JsonArray* arr) {
            assert(arr);
            return arr->elements.size(); // this should NOT yield any errors.
        }

        uint64_t JsonArray_IsNull(JsonArray* arr, size_t i) {
            assert(arr);
            simdjson::error_code error = simdjson::NO_SUCH_FIELD;
            assert(i < arr->elements.size()); // this should hold...
            return arr->elements[i].is_null() ? ecToI64(ExceptionCode::SUCCESS) : ecToI64(ExceptionCode::TYPEERROR);
        }

        uint64_t JsonArray_getInt(JsonArray *arr, size_t i, int64_t *out) {
            assert(arr);
            assert(out);

            simdjson::error_code error = simdjson::NO_SUCH_FIELD;
            int64_t value;

            // this is slow -> maybe better to replace complex iteration with custom decode routines!
            assert(i < arr->elements.size());

            arr->elements[i].get_int64().tie(value, error);

            if (error)
                return translate_simdjson_error(error);

            *out = value;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonArray_getDouble(JsonArray *arr, size_t i, double *out) {
            assert(arr);
            assert(out);

            simdjson::error_code error = simdjson::NO_SUCH_FIELD;
            double value;

            // this is slow -> maybe better to replace complex iteration with custom decode routines!
            assert(i < arr->elements.size());

            arr->elements[i].get_double().tie(value, error);

            if (error)
                return translate_simdjson_error(error);

            *out = value;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonArray_getBoolean(JsonArray *arr, size_t i, int64_t *out) {
            assert(arr);
            assert(out);

            simdjson::error_code error = simdjson::NO_SUCH_FIELD;
            bool value;

            // this is slow -> maybe better to replace complex iteration with custom decode routines!
            assert(i < arr->elements.size());

            arr->elements[i].get_bool().tie(value, error);

            if (error)
                return translate_simdjson_error(error);

            *out = value;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonArray_getStringAndSize(JsonArray *arr, size_t i, char **out, int64_t *size) {
            assert(arr);
            assert(out);

            simdjson::error_code error = simdjson::NO_SUCH_FIELD;

            // this is slow -> maybe better to replace complex iteration with custom decode routines!
            assert(i < arr->elements.size());

            std::string_view sv_value;
            arr->elements[i].get_string().tie(sv_value, error);
            if (error)
                return translate_simdjson_error(error);

            auto str_size = 1 + sv_value.size();
            char *buf = (char *) runtime::rtmalloc(str_size);
            for (unsigned i = 0; i < sv_value.size(); ++i)
                buf[i] = sv_value.at(i);
            buf[sv_value.size()] = '\0';
            *out = buf;
            *size = sv_value.size() + 1;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonArray_getObject(JsonArray *arr, size_t i, JsonItem **out) {
            assert(arr);
            assert(out);

            simdjson::error_code error = simdjson::NO_SUCH_FIELD;

            // this is slow -> maybe better to replace complex iteration with custom decode routines!
            assert(i < arr->elements.size());

            // on demand
            // simdjson::ondemand::object o;
            // dom
            simdjson::dom::object o;

            arr->elements[i].get_object().tie(o, error);
            if (error)
                return translate_simdjson_error(error);

            // ONLY allocate if ok. else, leave how it is.
            auto obj = new JsonItem();
            obj->o = std::move(o);
            *out = obj;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonArray_getArray(JsonArray *arr, size_t i, JsonArray **out) {
            assert(arr);
            assert(out);

            simdjson::error_code error = simdjson::NO_SUCH_FIELD;

            // this is slow -> maybe better to replace complex iteration with custom decode routines!
            assert(i < arr->elements.size());

            // on demand
            // simdjson::ondemand::array a;
            // dom
            simdjson::dom::array a;

            arr->elements[i].get_array().tie(a, error);
            if (error)
                return translate_simdjson_error(error);

            // ONLY allocate if ok. else, leave how it is.
            auto sub_arr = new JsonArray();
            // decode subarray!
            for(auto element : a) {
                // sub_arr->elements.emplace_back(item);
                sub_arr->elements.emplace_back(element);
            }
            *out = sub_arr;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonArray_getEmptyArray(JsonArray *arr, size_t i) {
            assert(arr);

            simdjson::error_code error = simdjson::NO_SUCH_FIELD;

            // this is slow -> maybe better to replace complex iteration with custom decode routines!
            assert(i < arr->elements.size());

            // on demand
            // simdjson::ondemand::array a;
            // dom
            simdjson::dom::array a;

            arr->elements[i].get_array().tie(a, error);
            if (error)
                return translate_simdjson_error(error);

            return 0 == a.size() ? ecToI64(ExceptionCode::SUCCESS) : ecToI64(ExceptionCode::TYPEERROR);
            // for ondemand this is prob important
            // // ONLY allocate if ok. else, leave how it is.
            // // decode subarray!
            // for(auto element : a) {
            //     return ecToI64(ExceptionCode::TYPEERROR); // not an empty list!
            // }
            // // ok, no element found but array found!
            // return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonItem_getDouble(JsonItem *item, const char *key, double *out) {
            assert(item);
            assert(key);
            assert(out);

            simdjson::error_code error;
            double value;
            item->o[key].get_double().tie(value, error);
            if (error)
                return translate_simdjson_error(error);

            *out = value;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonItem_getInt(JsonItem *item, const char *key, int64_t *out) {
            assert(item);
            assert(key);
            assert(out);

            simdjson::error_code error;
            int64_t value;
            item->o[key].get_int64().tie(value, error);
            if (error)
                return translate_simdjson_error(error);

            *out = value;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        uint64_t JsonItem_getBoolean(JsonItem *item, const char *key, bool *out) {
            assert(item);
            assert(key);
            assert(out);

            simdjson::error_code error;
            bool value;
            item->o[key].get_bool().tie(value, error);
            if (error)
                return translate_simdjson_error(error);

            *out = value;
            return ecToI64(ExceptionCode::SUCCESS);
        }

        // returns 0 if it is null!
        uint64_t JsonItem_IsNull(JsonItem *item, const char *key) {
            assert(item);
            simdjson::error_code error;
            error = item->o[key].error();
            if (error == simdjson::NO_SUCH_FIELD)
                return ecToI64(ExceptionCode::KEYERROR);
            return item->o[key].is_null() ? ecToI64(ExceptionCode::SUCCESS) : ecToI64(ExceptionCode::TYPEERROR);
        }

        bool JsonItem_hasKey(JsonItem *item, const char *key) {
            assert(item);
            auto error = item->o[key].error();
            return (error != simdjson::NO_SUCH_FIELD);
        }

        uint64_t JsonItem_numberOfKeys(JsonItem *item) {
            assert(item);
            size_t value;
            simdjson::error_code error;

            // on demand
            // item->o.count_fields().tie(value, error);

            // dom
            return item->o.size();

            assert(!error);
            return value;
        }


        // need an algorithm to verify schema match against set of keys
        // -> there's must have keys and optional keys.
        // all must have keys must be contained.
        // maybe keys may be contained, but can't be the other way round.
        // basically iterate over all keys present, check all must have keys are in there
        // and THEN check that remaining keys are not present.
        // basically check
        // keys n must_have_keys = must_have_keys
        // keys \ (must_have_keys u maybe_keys) = emptyset

        // helper function to perform a set of keys to a buffer
        std::string makeKeySetBuffer(const std::vector<std::string> &keys) {
            size_t total_size = sizeof(uint64_t) + sizeof(uint32_t) * keys.size();
            for (const auto &key: keys)
                total_size += key.size() + 1;
            std::string buf(total_size, '\0');

            // internal format is basically
            // | count (i64) | str_size (i32) | str_content ...| str_size(i32) | str_content ...| ... |

            // write to buffer
            auto ptr = (uint8_t *) &buf[0];
            *(uint64_t *) ptr = keys.size(); // maybe save size as well?
            ptr += sizeof(int64_t);
            for (const auto &key: keys) {
                *(uint32_t *) ptr = key.size() + 1;
                ptr += sizeof(uint32_t);
                memcpy(ptr, key.data(), key.size() + 1);
                ptr += key.size() + 1;
            }

            return buf;
        }



        // use a helper function for this and specially encoded buffers
        uint64_t JsonItem_keySetMatch(JsonItem *item, uint8_t *always_keys_buf, uint8_t *maybe_keys_buf) {

            assert(item);
            assert(always_keys_buf);
            assert(maybe_keys_buf);

            // check always_keys_buf
            // => they all need to be there!
            uint64_t num_always_keys = *(uint64_t *) always_keys_buf;
            uint64_t num_maybe_keys = *(uint64_t *) maybe_keys_buf;

            // fetch all keys and check then off.
            size_t num_fields = 0;
            // note: looking up string views does work for C++20+
            std::unordered_map<std::string, unsigned> lookup;
            for (auto field: item->o) {

                // ondemand
                // auto key = field.unescaped_key().take_value();

                // dom
                auto key = field.key;

                lookup[view_to_string(key)] = num_fields++;
            }

            // quick check
            if (num_fields < num_always_keys)
                return ecToI64(ExceptionCode::TYPEERROR); // not enough fields

            std::vector<bool> field_seen(num_fields, false);

            // go through the two buffers and mark whatever has been seen
            auto ptr = always_keys_buf + sizeof(int64_t);
            unsigned num_always_fields_seen = 0;
            for (unsigned i = 0; i < num_always_keys; ++i) {
                auto str_size = *(uint32_t *) ptr;
                ptr += sizeof(uint32_t);
                std::string key = (char *) ptr;
                if (lookup.end() != lookup.find(key)) {
                    field_seen[lookup[key]] = true; // mark as seen
                    num_always_fields_seen++; // must be there, i.e. count
                }
                ptr += str_size;
            }

            // check always fields quick check
            if (num_always_fields_seen != num_always_keys)
                return ecToI64(ExceptionCode::TYPEERROR); // not all always fields are there

            // another shortcut: if number of keys is num_always_keys, it's ok - all keys have been seen
            if (num_always_keys == num_fields)
                return ecToI64(ExceptionCode::SUCCESS);

            // are there maybe fields?
            if (num_maybe_keys > 0) {
                // expensive check.

                // go through the two buffers and mark whatever has been seen
                ptr = maybe_keys_buf + sizeof(int64_t);
                for (unsigned i = 0; i < num_maybe_keys; ++i) {
                    auto str_size = *(uint32_t *) ptr;
                    ptr += sizeof(uint32_t);
                    std::string key = (char *) ptr;
                    if (lookup.end() != lookup.find(key))
                        field_seen[lookup[key]] = true; // mark as seen
                    ptr += str_size;
                }

                // now go through bool array. if there is a single false => failure!
                auto num_seen = 0; // usually faster to sum everythin up...
                for (auto seen: field_seen) {
                    num_seen += seen;
                }
                if (num_seen != field_seen.size())
                    return ecToI64(ExceptionCode::TYPEERROR);
            }

            return ecToI64(ExceptionCode::SUCCESS); // ok.
        }

        bool Json_is_whitespace(const char* str, size_t size) {

            //assert(strlen(str) <= size); // won't hold, because string can be a string view!

            for(unsigned i = 0; i < size; ++i) {
                auto c = str[i];
                switch(c) {
                    case ' ':
                    case '\t':
                    case '\n':
                    case '\r':
                        continue;
                    default:
                        return false;
                }
            }
            return true;
        }

        void addJsonSymbolsToJIT(JITCompiler& jit) {
            // register symbols
            jit.registerSymbol("JsonParser_Init", JsonParser_init);
            jit.registerSymbol("JsonParser_Free", JsonParser_free);
            jit.registerSymbol("JsonParser_moveToNextRow", JsonParser_moveToNextRow);
            jit.registerSymbol("JsonParser_hasNextRow", JsonParser_hasNextRow);
            jit.registerSymbol("JsonParser_open", JsonParser_open);
            jit.registerSymbol("JsonParser_getDocType", JsonParser_getDocType);
            jit.registerSymbol("JsonParser_getMallocedRow", JsonParser_getMallocedRow);
            jit.registerSymbol("JsonParser_getMallocedRowAndSize", JsonParser_getMallocedRowAndSize);
            jit.registerSymbol("JsonParser_getObject", JsonParser_getObject);
            jit.registerSymbol("JsonItem_Free", JsonItem_Free);
            jit.registerSymbol("JsonItem_getStringAndSize", JsonItem_getStringAndSize);
            jit.registerSymbol("JsonItem_getObject", JsonItem_getObject);
            jit.registerSymbol("JsonItem_getDouble", JsonItem_getDouble);
            jit.registerSymbol("JsonItem_getInt", JsonItem_getInt);
            jit.registerSymbol("JsonItem_getBoolean", JsonItem_getBoolean);
            jit.registerSymbol("JsonItem_IsNull", JsonItem_IsNull);
            jit.registerSymbol("JsonItem_numberOfKeys", JsonItem_numberOfKeys);
            jit.registerSymbol("JsonItem_keySetMatch", JsonItem_keySetMatch);
            jit.registerSymbol("JsonItem_hasKey", JsonItem_hasKey);
            jit.registerSymbol("JsonItem_getArray", JsonItem_getArray);
            jit.registerSymbol("JsonArray_Free", JsonArray_Free);
            jit.registerSymbol("JsonArray_Size", JsonArray_Size);
            jit.registerSymbol("JsonArray_getBoolean", JsonArray_getBoolean);
            jit.registerSymbol("JsonArray_getInt", JsonArray_getInt);
            jit.registerSymbol("JsonArray_getDouble", JsonArray_getDouble);
            jit.registerSymbol("JsonArray_getStringAndSize", JsonArray_getStringAndSize);
            jit.registerSymbol("JsonArray_getObject", JsonArray_getObject);
            jit.registerSymbol("JsonArray_getArray", JsonArray_getArray);
            jit.registerSymbol("JsonArray_IsNull", JsonArray_IsNull);
            jit.registerSymbol("JsonArray_getEmptyArray", JsonArray_getEmptyArray);
            jit.registerSymbol("Json_is_whitespace", Json_is_whitespace);
            jit.registerSymbol("JsonParser_TruncatedBytes", JsonParser_TruncatedBytes);
        }

        bool JsonContainsAtLeastOneDocument(const char* buf, size_t buf_size) {
            if(buf_size == 0 || !buf)
                return true;

            const char* ptr = buf;
            size_t pos = 0;
            int64_t brace_counter = 0;
            int64_t bracket_counter = 0;
            bool in_escaped_string = false;
            while(pos < buf_size) {
                auto c = *ptr;
                switch(c) {
                    case '\0': {
                        if(pos > 0 && 0 == brace_counter && 0 == bracket_counter)
                            return true;
                        else
                            return false;
                        break;
                    }
                    case '\"': {
                        in_escaped_string = !in_escaped_string;
                        break;
                    }
                    case '\\': {
                        // are we in escaped string? => then \\\" can be skipped!
                        if(buf_size - pos - 1 >= 0) {
                            ptr++;
                            pos++; // skip, do not change...
                        } else {
                            return false; // invalid string
                        }
                        break;
                    }
                    case '\n':
                    case '\r': {
                        // in escaped string? -> doesn't matter, else check if there's a completed doc!
                        if(!in_escaped_string) {
                            return pos > 0 && 0 == brace_counter && 0 == bracket_counter;
                        }
                        break;
                    }
                    case '{': {
                        if(!in_escaped_string)
                            brace_counter++;
                        break;
                    }
                    case '}': {
                        if(!in_escaped_string)
                            brace_counter--;
                        break;
                    }
                    case '[': {
                        if(!in_escaped_string)
                            bracket_counter++;
                        break;
                    }
                    case ']': {
                        if(!in_escaped_string)
                            bracket_counter--;
                        break;
                    }
                }
                // end of document? check!
                ptr++;

                // when is a document found? when { and } count matches! Or when [ and ] count matches.

                pos++;
            }
            return 0 == brace_counter && 0 == bracket_counter;
        }
    }
}