//
// Created by Leonhard Spiegelberg on 9/20/22.
//

#include "HyperUtils.h"
#include "LLVM_wip.h"

// bindings
#include <StringUtils.h>
#include <JSONUtils.h>
#include <JsonStatistic.h>
#include <fstream>
#include <TypeHelper.h>
#include <Utils.h>
#include <compression.h>
#include <RuntimeInterface.h>

// NOTES:
// for concrete parser implementation with pushdown etc., use
// https://github.com/simdjson/simdjson/blob/master/doc/basics.md#json-pointer
// => this will allow to extract field...

namespace tuplex {

    // parse using simdjson
    static const auto SIMDJSON_BATCH_SIZE=simdjson::dom::DEFAULT_BATCH_SIZE;

    // helper C-struct holding simdjson parser
    struct JsonParser {
         // use simdjson as parser b.c. cJSON has issues with integers/floats.
         // https://simdjson.org/api/2.0.0/md_doc_iterate_many.html
         simdjson::ondemand::parser parser;
         simdjson::ondemand::document_stream stream;

         // iterators
         simdjson::ondemand::document_stream::iterator it;

        // simdjson::dom::parser parser;
        // simdjson::dom::document_stream stream;
        // simdjson::dom::document_stream::iterator it;

        std::string lastError;
    };

    // C-APIs to use in codegen

    JsonParser* JsonParser_init() {
        // can't malloc, or can malloc but then need to call inplace C++ constructors!
        return new JsonParser();
    }

    void JsonParser_free(JsonParser *parser) {
        if(parser)
            delete parser;
    }

    uint64_t JsonParser_open(JsonParser* j, const char* buf, size_t buf_size) {
        assert(j);

        simdjson::error_code error;
        // ondemand
        j->parser.iterate_many(buf, buf_size, std::min(buf_size, SIMDJSON_BATCH_SIZE)).tie(j->stream, error);

        // dom
        // j->parser.parse_many(buf, buf_size, std::min(buf_size, SIMDJSON_BATCH_SIZE)).tie(j->stream, error);
        if(error) {
            std::stringstream err_stream; err_stream<<error;
            j->lastError = err_stream.str();
            return ecToI64(ExceptionCode::JSONPARSER_ERROR);
        }

        // set internal iterator
        j->it = j->stream.begin();

        return ecToI64(ExceptionCode::SUCCESS);
    }

    uint64_t JsonParser_close(JsonParser* j) {
        assert(j);

        j->it = j->stream.end();

        return ecToI64(ExceptionCode::SUCCESS);
    }

    bool JsonParser_hasNextRow(JsonParser* j) {
        assert(j);
        return j->stream.end() != j->it;
    }

    bool JsonParser_moveToNextRow(JsonParser* j) {
        assert(j);
        ++j->it;
        return j->stream.end() != j->it;
    }

    /*!
     * get current row (malloc copy) (could also have rtmalloc copy).
     * Whoever requests this row, has to free it then. --> this function is required for badparsestringinput.
     */
    char* JsonParser_getMallocedRow(JsonParser* j) {
        using namespace std;

        assert(j);
        string full_row;
        stringstream ss;
        ss<<j->it.source()<<std::endl;
        full_row = ss.str();
        char* buf = (char*)malloc(full_row.size());
        if(buf)
            memcpy(buf, full_row.c_str(), full_row.size());
        return buf;
    }

    uint64_t JsonParser_getDocType(JsonParser* j) {
        assert(j);
        // i.e. simdjson::ondemand::json_type::object:
        // or simdjson::ondemand::json_type::array:
        // => if it doesn't conform, simply use badparse string input?
        if(!(j->it != j->stream.end()))
            return 0xFFFFFFFF;

        auto doc = *j->it;
        auto line_type = doc.type().value();
        return static_cast<uint64_t>(line_type);
    }

    inline uint64_t JsonParser_objectDocType() { return static_cast<uint64_t>(simdjson::ondemand::json_type::object); }

//    struct JsonItem {
//        simdjson::ondemand::
//    };

    struct JsonItem {
        simdjson::ondemand::object o;

    };

    // C API
    void JsonItem_Free(JsonItem* i) {
        delete i;
        i = nullptr;
    }

    uint64_t JsonParser_getObject(JsonParser* j, JsonItem** out) {
        assert(j);
        assert(out);

        auto o = new JsonItem();

        // debug checks:
        assert(j->it != j->stream.end());

        auto doc = *j->it;

        assert(doc.value().type().take_value() == simdjson::ondemand::json_type::object);

        o->o = doc.get_object().take_value(); // no error check here (for speed reasons).

        *out = o;

        return ecToI64(ExceptionCode::SUCCESS);
    }

    inline uint64_t translate_simdjson_error(const simdjson::error_code& error) {
        if(simdjson::NO_SUCH_FIELD == error)
            return ecToI64(ExceptionCode::KEYERROR);
        if(simdjson::INCORRECT_TYPE == error)
            return ecToI64(ExceptionCode::TYPEERROR);
        //if(simdjson::N)
        return ecToI64(ExceptionCode::JSONPARSER_ERROR);
    }

    // get string item and save to rtmalloced string!
    uint64_t JsonItem_getString(JsonItem *item, const char* key, char **out) {
        assert(item);
        assert(key);
        assert(out);

        simdjson::error_code error;
        std::string_view sv_value;
        item->o[key].get_string().tie(sv_value, error);
        if(error)
           return translate_simdjson_error(error);

        auto str_size = 1 + sv_value.size();
        char* buf = (char*)runtime::rtmalloc(str_size);
        for(unsigned i = 0; i < sv_value.size(); ++i)
            buf[i] = sv_value.at(i);
        buf[sv_value.size()] = '\0';
        *out = buf;
        return ecToI64(ExceptionCode::SUCCESS);
    }

    uint64_t JsonItem_getObject(JsonItem *item, const char* key, JsonItem **out) {
        assert(item);
        assert(key);
        assert(out);

        simdjson::error_code error;
        simdjson::ondemand::object o;
        item->o[key].get_object().tie(o, error);
        if(error)
            return translate_simdjson_error(error);

        auto obj = new JsonItem();
        obj->o = std::move(o);
        *out = obj;
        return ecToI64(ExceptionCode::SUCCESS);
    }

    uint64_t JsonItem_getDouble(JsonItem *item, const char* key, double *out) {
        assert(item);
        assert(key);
        assert(out);

        simdjson::error_code error;
        double value;
        item->o[key].get_double().tie(value, error);
        if(error)
            return translate_simdjson_error(error);

        *out = value;
        return ecToI64(ExceptionCode::SUCCESS);
    }

    uint64_t JsonItem_getInt(JsonItem *item, const char* key, int64_t *out) {
        assert(item);
        assert(key);
        assert(out);

        simdjson::error_code error;
        int64_t value;
        item->o[key].get_int64().tie(value, error);
        if(error)
            return translate_simdjson_error(error);

        *out = value;
        return ecToI64(ExceptionCode::SUCCESS);
    }

    uint64_t JsonItem_getBoolean(JsonItem *item, const char* key, bool *out) {
        assert(item);
        assert(key);
        assert(out);

        simdjson::error_code error;
        bool value;
        item->o[key].get_bool().tie(value, error);
        if(error)
            return translate_simdjson_error(error);

        *out = value;
        return ecToI64(ExceptionCode::SUCCESS);
    }

}

// notes: type of line can be

TEST_F(HyperTest, BasicStructLoad) {
    using namespace tuplex;
    using namespace std;

    string sample_path = "/Users/leonhards/Downloads/github_sample";
    string sample_file = sample_path + "/2011-11-26-13.json.gz";

    auto path = sample_file;

    path = "../resources/2011-11-26-13.json.gz";

    auto raw_data = fileToString(path);

    const char * pointer = raw_data.data();
    std::size_t size = raw_data.size();

    // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
    std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;


    // parse code starts here...
    auto buf = decompressed_data.data();
    auto buf_size = decompressed_data.size();


    // detect (general-case) type here:
//    ContextOptions co = ContextOptions::defaults();
//    auto sample_size = co.CSV_MAX_DETECTION_MEMORY();
//    auto nc_th = co.NORMALCASE_THRESHOLD();
    auto sample_size = 256 * 1024ul; // 256kb
    auto nc_th = 0.9;
    auto rows = parseRowsFromJSON(buf, std::min(buf_size, sample_size), nullptr, false);
    auto row_type = detectMajorityRowType(rows, nc_th);
    std::cout<<"detected: "<<row_type.desc()<<std::endl;

    // runtime init
    ContextOptions co = ContextOptions::defaults();
    runtime::init(co.RUNTIME_LIBRARY(false).toPath());

    // C-version of parsing
    uint64_t row_number = 0;

    auto j = JsonParser_init();
    if(!j)
        throw std::runtime_error("failed to initialize parser");
    JsonParser_open(j, buf, buf_size);
    while(JsonParser_hasNextRow(j)) {
        if(JsonParser_getDocType(j) != JsonParser_objectDocType()) {
            // BADPARSE_STRINGINPUT
            auto line = JsonParser_getMallocedRow(j);
            free(line);
        }

        // line ok, now extract something from the object!
        // => basically need to traverse...
        auto doc = *j->it;

//        auto obj = doc.get_object().take_value();

        // get type
        JsonItem *obj = nullptr;
        uint64_t rc = JsonParser_getObject(j, &obj);
        if(rc != 0)
            break; // --> don't forget to release stuff here!
        char* type_str = nullptr;
        rc = JsonItem_getString(obj, "type", &type_str);
        if(rc != 0)
            continue; // --> don't forget to release stuff here
        JsonItem *sub_obj = nullptr;
        rc = JsonItem_getObject(obj, "repo", &sub_obj);
        if(rc != 0)
            continue; // --> don't forget to release stuff here!

        // check wroong type
        int64_t val_i = 0;
        rc = JsonItem_getInt(obj, "repo", &val_i);
        EXPECT_EQ(rc, ecToI64(ExceptionCode::TYPEERROR));
        if(rc != 0) {
            row_number++;
            JsonParser_moveToNextRow(j);
            continue; // --> next
        }

        char* url_str = nullptr;
        rc = JsonItem_getString(sub_obj, "url", &url_str);

        // error handling: KeyError?
        rc = JsonItem_getString(sub_obj, "key that doesn't exist", &type_str);
        EXPECT_EQ(rc, ecToI64(ExceptionCode::KEYERROR));

        // release all allocated things
        JsonItem_Free(obj);
        JsonItem_Free(sub_obj);

        row_number++;
        JsonParser_moveToNextRow(j);
    }
    JsonParser_close(j);
    JsonParser_free(j);

    std::cout<<"Parsed "<<pluralize(row_number, "row")<<std::endl;
}