//
// Created by leonhard on 9/23/22.
//
#include <physical/experimental/JsonSourceTaskBuilder.h>
#include <RuntimeInterface.h>
#include <JITCompiler.h>

namespace tuplex {
    namespace codegen {
        // parse using simdjson
        static const auto SIMDJSON_BATCH_SIZE = simdjson::dom::DEFAULT_BATCH_SIZE;

        // C-APIs to use in codegen

        JsonParser *JsonParser_init() {
            // can't malloc, or can malloc but then need to call inplace C++ constructors!
            return new JsonParser();
        }

        void JsonParser_free(JsonParser *parser) {
            if (parser)
                delete parser;
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
            ss << j->it.source() << std::endl;
            full_row = ss.str();
            char *buf = (char *) malloc(full_row.size());
            if (buf)
                memcpy(buf, full_row.c_str(), full_row.size());
            return buf;
        }

        uint64_t JsonParser_getDocType(JsonParser *j) {
            assert(j);
            // i.e. simdjson::ondemand::json_type::object:
            // or simdjson::ondemand::json_type::array:
            // => if it doesn't conform, simply use badparse string input?
            if (!(j->it != j->stream.end()))
                return 0xFFFFFFFF;

            auto doc = *j->it;
            auto line_type = doc.type().value();
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

            auto o = new JsonItem();

            // debug checks:
            assert(j->it != j->stream.end());

            auto doc = *j->it;

            // on demand
            // assert(doc.value().type().take_value() == simdjson::ondemand::json_type::object);

            //dom
            assert(doc.value().type() == simdjson::dom::element_type::OBJECT);

            o->o = doc.get_object().take_value(); // no error check here (for speed reasons).

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

        void addJsonSymbolsToJIT(JITCompiler& jit) {
            // register symbols
            jit.registerSymbol("JsonParser_Init", JsonParser_init);
            jit.registerSymbol("JsonParser_Free", JsonParser_free);
            jit.registerSymbol("JsonParser_moveToNextRow", JsonParser_moveToNextRow);
            jit.registerSymbol("JsonParser_hasNextRow", JsonParser_hasNextRow);
            jit.registerSymbol("JsonParser_open", JsonParser_open);
            jit.registerSymbol("JsonParser_getDocType", JsonParser_getDocType);
            jit.registerSymbol("JsonParser_getMallocedRow", JsonParser_getMallocedRow);
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
            jit.registerSymbol("JsonArray_getInt", JsonArray_getInt);
            jit.registerSymbol("JsonArray_getObject", JsonArray_getObject);
        }
    }
}