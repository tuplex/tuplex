//
// Created by leonhard on 9/23/22.
//

#ifndef TUPLEX_JSONSOURCETASKBUILDER_H
#define TUPLEX_JSONSOURCETASKBUILDER_H

#include <StringUtils.h>
#include <JSONUtils.h>
#include <JsonStatistic.h>

#include <experimental/ListHelper.h>
#include <experimental/StructDictHelper.h>
#include "../../JITCompiler.h"

namespace tuplex {
    namespace codegen {
        inline std::string view_to_string(const std::string_view &v) {
            return std::string{v.begin(), v.end()};
        }

        // helper C-struct holding simdjson parser
        struct JsonParser {

            // this will have issues with out of order access (happening when arrays are present...) -> more complex to implement.
            //// use simdjson as parser b.c. cJSON has issues with integers/floats.
            //// https://simdjson.org/api/2.0.0/md_doc_iterate_many.html
            //simdjson::ondemand::parser parser;
            //simdjson::ondemand::document_stream stream;
            //
            //// iterators
            //simdjson::ondemand::document_stream::iterator it;

            simdjson::dom::parser parser;
            simdjson::dom::document_stream stream;
            simdjson::dom::document_stream::iterator it;

            std::string lastError;
        };

        struct JsonItem {
            // on demand
            // simdjson::ondemand::object o;

            simdjson::dom::object o;
        };

        struct JsonArray {
            // require decoding of full array always b.c. simdjson has some issues with the array
            // std::vector<simdjson::simdjson_result<simdjson::ondemand::value>> elements;
            std::vector<simdjson::dom::element> elements;
        };

        inline uint64_t JsonParser_objectDocType() {

            // on demand
            // return static_cast<uint64_t>(simdjson::ondemand::json_type::object);

            // dom
            return static_cast<uint64_t>(simdjson::dom::element_type::OBJECT);
        }

        extern  std::string makeKeySetBuffer(const std::vector<std::string> &keys);
        extern uint64_t JsonItem_keySetMatch(JsonItem *item, uint8_t *always_keys_buf, uint8_t *maybe_keys_buf);

        // symbols
        extern void addJsonSymbolsToJIT(JITCompiler& jit);

        extern JsonParser *JsonParser_init();
        extern void JsonParser_free(JsonParser *parser);
        extern void JsonItem_Free(JsonItem *i);
        extern uint64_t JsonParser_open(JsonParser *j, const char *buf, size_t buf_size);
        extern uint64_t JsonParser_close(JsonParser *j);
        extern bool JsonParser_hasNextRow(JsonParser *j);
        extern bool JsonParser_moveToNextRow(JsonParser *j);
        extern char *JsonParser_getMallocedRow(JsonParser *j);
        extern uint64_t JsonParser_getDocType(JsonParser *j);
        extern uint64_t JsonParser_getObject(JsonParser *j, JsonItem **out);
        extern uint64_t JsonItem_getString(JsonItem *item, const char *key, char **out);
        extern uint64_t JsonItem_getStringAndSize(JsonItem *item, const char *key, char **out, int64_t *size);
        extern uint64_t JsonItem_getObject(JsonItem *item, const char *key, JsonItem **out);
        extern uint64_t JsonItem_getArray(JsonItem *item, const char *key, JsonArray **out);
        extern void JsonArray_Free(JsonArray* arr);
        extern uint64_t JsonArray_Size(JsonArray* arr);
        extern uint64_t JsonArray_getBoolean(JsonArray *arr, size_t i, int64_t *out);
        extern uint64_t JsonArray_getInt(JsonArray *arr, size_t i, int64_t *out);
        extern uint64_t JsonArray_getDouble(JsonArray *arr, size_t i, double *out);
        extern uint64_t JsonArray_getStringAndSize(JsonArray *arr, size_t i, char **out, int64_t *size);
        extern uint64_t JsonArray_getObject(JsonArray *arr, size_t i, JsonItem **out);
        extern uint64_t JsonItem_getDouble(JsonItem *item, const char *key, double *out);
        extern uint64_t JsonItem_getInt(JsonItem *item, const char *key, int64_t *out);
        extern uint64_t JsonItem_getBoolean(JsonItem *item, const char *key, bool *out);
        extern uint64_t JsonItem_IsNull(JsonItem *item, const char *key);
        extern bool JsonItem_hasKey(JsonItem *item, const char *key);
        extern uint64_t JsonItem_numberOfKeys(JsonItem *item);
    }
}


#endif //TUPLEX_JSONSOURCETASKBUILDER_H