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

#include <AccessPathVisitor.h>

#include <llvm/IR/TypeFinder.h>

#include "ListHelper.h"

// NOTES:
// for concrete parser implementation with pushdown etc., use
// https://github.com/simdjson/simdjson/blob/master/doc/basics.md#json-pointer
// => this will allow to extract field...

namespace tuplex {

    // parse using simdjson
    static const auto SIMDJSON_BATCH_SIZE = simdjson::dom::DEFAULT_BATCH_SIZE;

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
        // ondemand
        j->parser.iterate_many(buf, buf_size, std::min(buf_size, SIMDJSON_BATCH_SIZE)).tie(j->stream, error);

        // dom
        // j->parser.parse_many(buf, buf_size, std::min(buf_size, SIMDJSON_BATCH_SIZE)).tie(j->stream, error);
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

    inline uint64_t JsonParser_objectDocType() { return static_cast<uint64_t>(simdjson::ondemand::json_type::object); }

//    struct JsonItem {
//        simdjson::ondemand::
//    };

    struct JsonItem {
        simdjson::ondemand::object o;

    };

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

        assert(doc.value().type().take_value() == simdjson::ondemand::json_type::object);

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
        simdjson::ondemand::object o;
        item->o[key].get_object().tie(o, error);
        if (error)
            return translate_simdjson_error(error);
        // ONLY allocate if ok. else, leave how it is.
        auto obj = new JsonItem();
        obj->o = std::move(o);
        *out = obj;
        return ecToI64(ExceptionCode::SUCCESS);
    }

    struct JsonArray {
        // require decoding of full array always b.c. simdjson has some issues with the array
        std::vector<simdjson::simdjson_result<simdjson::ondemand::value>> elements;
    };

    uint64_t JsonItem_getArray(JsonItem *item, const char *key, JsonArray **out) {
        assert(item);
        assert(key);
        assert(out);

        simdjson::error_code error;
        simdjson::ondemand::array a;
        item->o[key].get_array().tie(a, error);
        if (error)
            return translate_simdjson_error(error);

        // ONLY allocate if ok. else, leave how it is.
        auto arr = new JsonArray();

        // decode array
        for(auto item : a) {
            arr->elements.emplace_back(item);
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
        item->o.count_fields().tie(value, error);
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

    inline std::string view_to_string(const std::string_view &v) {
        return std::string{v.begin(), v.end()};
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
            auto key = field.unescaped_key().take_value();
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


//    struct JsonKeyView
//
//    uint64_t JsonItem_keysToStringList(JsonItem *item, uint8_t** out_buf, int64_t *out_buf_size) {
//        assert(item);
//
//        // iterates keys and writes them to rtmalloced array in Tuplex list struct (for strings?)
//        simdjson::error_code error;
//        std::vector<std::string_view> keys;
//        for(auto field : item->o) {
//            auto sv_key = field.unescaped_key().value();
//            keys.pu
//        }
//
//    }

}


namespace tuplex {
    namespace codegen {

        // recursive function to decode data, similar to flattening the type below
        // each item should be access_path | value_type | alwaysPresent |  value : SerializableValue | present : i1
        using access_path_t = std::vector<std::pair<std::string, python::Type>>;
        using flattened_struct_dict_decoded_entry_t = std::tuple<std::vector<std::pair<std::string, python::Type>>, python::Type, bool, SerializableValue, llvm::Value*>;
        using flattened_struct_dict_decoded_entry_list_t = std::vector<flattened_struct_dict_decoded_entry_t>;

        // forward declaration
        SerializableValue struct_dict_load_from_values(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const python::Type& dict_type, flattened_struct_dict_decoded_entry_list_t entries, llvm::Value* value=nullptr);
        SerializableValue struct_dict_type_serialized_memory_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type);
        SerializableValue struct_dict_serialize_to_memory(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, llvm::Value* dest_ptr);

        // zero all size fields. Important! especially for maybe elements.
        void struct_dict_mem_zero(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type);

        llvm::Type* create_structured_dict_type(LLVMEnvironment &env, const std::string &name, const python::Type &dict_type);

        // store functions
        void struct_dict_store_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_present);
        void struct_dict_store_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* value);
        void struct_dict_store_isnull(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_null);
        void struct_dict_store_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* size);

        inline llvm::Constant *cbool_const(llvm::LLVMContext &ctx, bool b) {
            auto type = ctypeToLLVM<bool>(ctx);
            return llvm::ConstantInt::get(llvm::Type::getIntNTy(ctx, type->getIntegerBitWidth()), b);
        }

        class JSONSourceTaskBuilder {
        public:
            JSONSourceTaskBuilder(LLVMEnvironment &env,
                                  const python::Type &rowType,
                                  const std::string &functionName = "parseJSON", bool unwrap_first_level = true) : _env(
                    env), _rowType(rowType),
                                                                                                                   _functionName(
                                                                                                                           functionName),
                                                                                                                   _unwrap_first_level(
                                                                                                                           unwrap_first_level),
                                                                                                                   _rowNumberVar(
                                                                                                                           nullptr),
                                                                                                                   _badParseCountVar(
                                                                                                                           nullptr),
                                                                                                                   _freeStart(
                                                                                                                           nullptr),
                                                                                                                   _freeEnd(
                                                                                                                           _freeStart) {}

            void build();

        private:
            LLVMEnvironment &_env;
            python::Type _rowType;
            std::string _functionName;
            bool _unwrap_first_level;

            // helper values
            llvm::Value *_rowNumberVar;
            llvm::Value *_badParseCountVar; // stores count of bad parse emits.

            // blocks to hold start/end of frees --> called before going to next row.
            llvm::BasicBlock *_freeStart;
            llvm::BasicBlock *_freeEnd;


            // helper functions

            void generateParseLoop(llvm::IRBuilder<> &builder, llvm::Value *bufPtr, llvm::Value *bufSize);

            llvm::Value *initJsonParser(llvm::IRBuilder<> &builder);

            void freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j);

            llvm::Value *
            openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf, llvm::Value *buf_size);

            void
            exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition, llvm::Value *exitCode);

            llvm::Value *hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);

            void moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j);


            llvm::BasicBlock *
            emitBadParseInputAndMoveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *condition);

            inline llvm::Value *rowNumber(llvm::IRBuilder<> &builder) {
                assert(_rowNumberVar);
                assert(_rowNumberVar->getType() == _env.i64ptrType());
                return builder.CreateLoad(_rowNumberVar);
            }

            llvm::Value *isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j);

            void parseAndPrintStructuredDictFromObject(llvm::IRBuilder<> &builder, llvm::Value *j,
                                                       llvm::BasicBlock *bbSchemaMismatch);

            void freeObject(llvm::IRBuilder<> &builder, llvm::Value *obj);
            void freeObject(llvm::Value *obj);
            void freeArray(llvm::IRBuilder<> &builder, llvm::Value *arr);
            void freeArray(llvm::Value *arr);
            llvm::Value* arraySize(llvm::IRBuilder<>& builder, llvm::Value* arr);

            llvm::Value *numberOfKeysInObject(llvm::IRBuilder<> &builder, llvm::Value *j);

            /*!
             *
             * @param builder
             * @param obj
             * @param t
             * @param check_that_all_keys_are_present if true, then row must contain exact keys for struct dict. Else, it's parsed whatever is specified in the schema.
             * @param bbSchemaMismatch
             */
            void parseAndPrint(llvm::IRBuilder<> &builder, llvm::Value *obj, const std::string &debug_path,
                               bool alwaysPresent, const python::Type &t, bool check_that_all_keys_are_present,
                               llvm::BasicBlock *bbSchemaMismatch);

            llvm::Value *decodeFieldFromObject(llvm::IRBuilder<> &builder,
                                               llvm::Value *obj,
                                               const std::string &debug_path,
                                               SerializableValue *out,
                                               bool alwaysPresent,
                                               llvm::Value *key,
                                               const python::Type &keyType,
                                               const python::Type &valueType,
                                               bool check_that_all_keys_are_present,
                                               llvm::BasicBlock *bbSchemaMismatch);

            llvm::Value *
            decodeFieldFromObject(llvm::IRBuilder<> &builder, llvm::Value *obj, const std::string &debug_path,
                                  SerializableValue *out, bool alwaysPresent, const std::string &key,
                                  const python::Type &keyType, const python::Type &valueType,
                                  bool check_that_all_keys_are_present, llvm::BasicBlock *bbSchemaMismatch) {
                return decodeFieldFromObject(builder, obj, debug_path, out, alwaysPresent, _env.strConst(builder, key),
                                             keyType, valueType, check_that_all_keys_are_present, bbSchemaMismatch);
            }

            void printValueInfo(llvm::IRBuilder<> &builder, const std::string &key, const python::Type &valueType,
                                llvm::Value *keyPresent, const SerializableValue &value);

            void checkRC(llvm::IRBuilder<> &builder, const std::string &key, llvm::Value *rc);

            struct DecodeOptions {
                bool verifyExactKeySetMatch;
            };

            void decode(llvm::IRBuilder<>& builder,
                        llvm::Value* dict_ptr,
                        const python::Type& dict_ptr_type, // <- the type of the top-level project where to store stuff
                        llvm::Value* object,
                        llvm::BasicBlock* bbSchemaMismatch,
                        const python::Type &dict_type, // <-- the type of object (which must be a structured dict)
                        std::vector<std::pair<std::string, python::Type>> prefix = {},
                        bool include_maybe_structs = true,
                        const DecodeOptions& options={});


            std::tuple<llvm::Value*, llvm::Value*, SerializableValue> decodePrimitiveFieldFromObject(llvm::IRBuilder<>& builder,
                                                                                                     llvm::Value* obj,
                                                                                                     llvm::Value* key,
                                                                                                     const python::StructEntry& entry,
                                                                                                     const DecodeOptions& options,
                                                                                                     llvm::BasicBlock *bbSchemaMismatch);


            // various decoding functions (object)
            std::tuple<llvm::Value*, SerializableValue> decodeString(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeBoolean(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeI64(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeF64(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeEmptyDict(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeNull(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key);

            // similarly, decoding functions (array)
            std::tuple<llvm::Value*, SerializableValue> decodeI64FromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index);

            // complex compound types
            std::tuple<llvm::Value*, SerializableValue> decodeEmptyList(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeList(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value *key, const python::Type& listType);

            // helper function to create the loop for the array
            llvm::Value* generateDecodeListItemsLoop(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* num_elements);


            inline void badParseCause(const std::string& cause) {
                // helper function, called to describe cause. probably useful later...
            }
        };

        std::string json_access_path_to_string(const std::vector<std::pair<std::string, python::Type>>& path,
                                               const python::Type& value_type,
                                               bool always_present) {
            std::stringstream ss;
            // first the path:
            for (auto atom: path) {
                ss << atom.first << " (" << atom.second.desc() << ") -> ";
            }
            auto presence = !always_present ? "  (maybe)" : "";
            auto v_type = value_type;
            auto value_desc = v_type.isStructuredDictionaryType() ? "Struct[...]" : v_type.desc();
            ss << value_desc << presence;
            return ss.str();
        }


        std::tuple<llvm::Value *, SerializableValue>
        JSONSourceTaskBuilder::decodeString(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            // decode using string
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getStringAndSize", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0), _env.i64ptrType());
            auto str_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr(), "s");
            auto str_size_var = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "s_size");
            llvm::Value* rc = builder.CreateCall(F, {obj, key, str_var, str_size_var});
            SerializableValue v;
            v.val = builder.CreateLoad(str_var);
            v.size = builder.CreateLoad(str_size_var);
            v.is_null = _env.i1Const(false);
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONSourceTaskBuilder::decodeBoolean(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            // decode using string
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getBoolean", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(),
                                         ctypeToLLVM<bool>(_env.getContext())->getPointerTo());
            auto b_var = _env.CreateFirstBlockVariable(builder, cbool_const(_env.getContext(), false));
            llvm::Value* rc = builder.CreateCall(F, {obj, key, b_var});
            SerializableValue v;
            v.val = _env.upcastToBoolean(builder, builder.CreateLoad(b_var));
            v.size = _env.i64Const(sizeof(int64_t));
            v.is_null = _env.i1Const(false);
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONSourceTaskBuilder::decodeI64(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            // decode using string
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getInt", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(),
                                         _env.i64ptrType());
            auto i_var = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            llvm::Value* rc = builder.CreateCall(F, {obj, key, i_var});
            SerializableValue v;
            v.val = builder.CreateLoad(i_var);
            v.size = _env.i64Const(sizeof(int64_t));
            v.is_null = _env.i1Const(false);
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONSourceTaskBuilder::decodeF64(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            // decode using string
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getDouble", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(),
                                         _env.doublePointerType());
            auto f_var = _env.CreateFirstBlockVariable(builder, _env.f64Const(0));
            llvm::Value* rc = builder.CreateCall(F, {obj, key, f_var});
            SerializableValue v;
            v.val = builder.CreateLoad(f_var);
            v.size = _env.i64Const(sizeof(int64_t));
            v.is_null = _env.i1Const(false);
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONSourceTaskBuilder::decodeEmptyDict(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            // query sub-object and call count keys!
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getObject", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
            auto sub_obj_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr());

            // create call, recurse only if ok!
            BasicBlock *bbCurrent = builder.GetInsertBlock();
            BasicBlock *bbObjectFound = BasicBlock::Create(_env.getContext(), "found_object", builder.GetInsertBlock()->getParent());
            BasicBlock *bbNext = BasicBlock::Create(_env.getContext(), "empty_check_done", builder.GetInsertBlock()->getParent());
            llvm::Value* rc_A = builder.CreateCall(F, {obj, key, sub_obj_var});
            auto found_object = builder.CreateICmpEQ(rc_A, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));

            builder.CreateCondBr(found_object, bbObjectFound, bbNext);

            // --- object found ---
            builder.SetInsertPoint(bbObjectFound);
            auto sub_obj = builder.CreateLoad(sub_obj_var);

            // check how many entries
            auto num_keys = numberOfKeysInObject(builder, sub_obj);
            auto is_empty = builder.CreateICmpEQ(num_keys, _env.i64Const(0));
            llvm::Value* rc_B = builder.CreateSelect(is_empty, _env.i64Const(
                                              ecToI64(ExceptionCode::SUCCESS)),
                                      _env.i64Const(
                                              ecToI64(ExceptionCode::TYPEERROR)));
            // add object to free list...
            freeObject(sub_obj_var);
            // go to done block.
            builder.CreateBr(bbNext);

            builder.SetInsertPoint(bbNext);
            // use phi instruction. I.e., if found_object => call count keys
            // if it's not an object keep rc and
            auto phi = builder.CreatePHI(_env.i64Type(), 2);
            phi->addIncoming(rc_A, bbCurrent);
            phi->addIncoming(rc_B, bbObjectFound);
            llvm::Value* rc = phi;

            SerializableValue v; // dummy value for empty dict.
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value*, SerializableValue>
        JSONSourceTaskBuilder::decodeNull(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            // special case!
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_IsNull", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType());
            llvm::Value* rc = builder.CreateCall(F, {obj, key});
            SerializableValue v;
            v.is_null = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONSourceTaskBuilder::decodeEmptyList(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
            // similar to empty dict

            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            // query for array and determine array size, if 0 => match!
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getArray", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
            auto item_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr());

            // create call, recurse only if ok!
            BasicBlock *bbCurrent = builder.GetInsertBlock();
            BasicBlock *bbObjectFound = BasicBlock::Create(_env.getContext(), "found_array", builder.GetInsertBlock()->getParent());
            BasicBlock *bbNext = BasicBlock::Create(_env.getContext(), "empty_check_done", builder.GetInsertBlock()->getParent());
            llvm::Value* rc_A = builder.CreateCall(F, {obj, key, item_var});
            auto found_array = builder.CreateICmpEQ(rc_A, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));

            builder.CreateCondBr(found_array, bbObjectFound, bbNext);

            // --- object found ---
            builder.SetInsertPoint(bbObjectFound);
            auto item = builder.CreateLoad(item_var);

            // check how many entries
            auto num_elements = arraySize(builder, item);
            auto is_empty = builder.CreateICmpEQ(num_elements, _env.i64Const(0));
            llvm::Value* rc_B = builder.CreateSelect(is_empty, _env.i64Const(
                                                             ecToI64(ExceptionCode::SUCCESS)),
                                                     _env.i64Const(
                                                             ecToI64(ExceptionCode::TYPEERROR)));
            // add object to free list...
            freeObject(item_var);
            // go to done block.
            builder.CreateBr(bbNext);

            builder.SetInsertPoint(bbNext);
            // use phi instruction. I.e., if found_array => call count keys
            // if it's not an object keep rc and
            auto phi = builder.CreatePHI(_env.i64Type(), 2);
            phi->addIncoming(rc_A, bbCurrent);
            phi->addIncoming(rc_B, bbObjectFound);
            llvm::Value* rc = phi;

            SerializableValue v; // dummy value for empty list.
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONSourceTaskBuilder::decodeI64FromArray(llvm::IRBuilder<> &builder, llvm::Value *array, llvm::Value *index) {
            using namespace std;
            using namespace llvm;


            assert(array && index);
            assert(index->getType() == _env.i64Type());

            // decode using string
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonArray_getInt", _env.i64Type(), _env.i8ptrType(), _env.i64Type(),
                                         _env.i64ptrType());
            auto i_var = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            llvm::Value* rc = builder.CreateCall(F, {array, index, i_var});
            SerializableValue v;
            v.val = builder.CreateLoad(i_var);
            v.size = _env.i64Const(sizeof(int64_t));
            v.is_null = _env.i1Const(false);
            return make_tuple(rc, v);
        }

        llvm::Value* JSONSourceTaskBuilder::generateDecodeListItemsLoop(llvm::IRBuilder<> &builder, llvm::Value *array,
                                                                llvm::Value *list_ptr, const python::Type &list_type,
                                                                llvm::Value *num_elements) {
            using namespace llvm;

            auto& ctx = _env.getContext();
            assert(list_type.isListType());
            auto element_type = list_type.elementType();

            assert(array && array->getType() == _env.i8ptrType());
            assert(list_ptr && list_ptr->getType() == _env.getOrCreateListType(list_type)->getPointerTo());
            assert(num_elements && num_elements->getType() == _env.i64Type());

            // loop is basically:
            // for i = 0, ..., num_elements -1:
            //   v = decode(array, i)
            //   if err(v)
            //     break
            //   list_store(i, v)

            llvm::Value* rcVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));

            auto F = builder.GetInsertBlock()->getParent(); assert(F);
            BasicBlock* bLoopHeader = BasicBlock::Create(ctx, "array_loop_header", F);
            BasicBlock* bLoopBody = BasicBlock::Create(ctx, "array_loop_body", F);
            BasicBlock* bLoopDone = BasicBlock::Create(ctx, "array_loop_done", F);
            auto loop_i = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));

            builder.CreateStore(_env.i64Const(0), loop_i);
            builder.CreateBr(bLoopHeader);

            {
                // --- loop header ---
                // if i < num_elements:
                builder.SetInsertPoint(bLoopHeader);
                auto loop_i_val = builder.CreateLoad(loop_i);
                auto loop_cond = builder.CreateICmpULT(loop_i_val, num_elements);
                builder.CreateCondBr(loop_cond, bLoopBody, bLoopDone);
            }

            {
                // --- loop body ---
                builder.SetInsertPoint(bLoopBody);
                // // debug
                // _env.printValue(builder, builder.CreateLoad(loop_i), "decoding element ");

                llvm::Value* item_rc = nullptr;
                SerializableValue item;

                auto index = builder.CreateLoad(loop_i);

                // decode now element from array
                if(element_type == python::Type::I64) {
                    std::tie(item_rc, item) = decodeI64FromArray(builder, array, index);
                } else {
                    throw std::runtime_error("Decode of element type " + element_type.desc() + " in list not yet supported");
                }

                // check what the result is of item_rc -> can be combined with rc!
                BasicBlock* bDecodeOK = BasicBlock::Create(ctx, "array_item_decode_ok", F);
                BasicBlock* bDecodeFail = BasicBlock::Create(ctx, "array_item_decode_failed", F);

                auto is_item_decode_ok = builder.CreateICmpEQ(item_rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                builder.CreateCondBr(is_item_decode_ok, bDecodeOK, bDecodeFail);

                {
                    // fail block:
                    builder.SetInsertPoint(bDecodeFail);
                    builder.CreateStore(item_rc, rcVar);
                    builder.CreateBr(bLoopDone);
                }

                {
                    // ok block:
                    builder.SetInsertPoint(bDecodeOK);

                    // // next: store in list
                    // _env.printValue(builder, item.val, "decoded value: ");

                    auto loop_i_val = builder.CreateLoad(loop_i);
                    list_store_value(_env, builder, list_ptr, list_type, loop_i_val, item);

                    // inc.
                    builder.CreateStore(builder.CreateAdd(_env.i64Const(1), loop_i_val), loop_i);
                    builder.CreateBr(bLoopHeader);
                }
            }

            builder.SetInsertPoint(bLoopDone);

            return builder.CreateLoad(rcVar);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONSourceTaskBuilder::decodeList(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key,
                                          const python::Type &listType) {
            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            if(python::Type::EMPTYLIST == listType)
                return decodeEmptyList(builder, obj, key);

            // create list ptr (in any case!)
            auto list_llvm_type = _env.getOrCreateListType(listType);
            auto list_ptr = _env.CreateFirstBlockAlloca(builder, list_llvm_type);
            list_init_empty(_env, builder, list_ptr, listType);


            auto rc_var = _env.CreateFirstBlockAlloca(builder, _env.i64Type());
            builder.CreateStore(_env.i64Const(ecToI64(ExceptionCode::SUCCESS)), rc_var);

            // decode happens in two steps:
            // step 1: check if there's actually an array in the JSON data -> if not, type error!
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getArray", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
            auto item_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr());
            // add array free to step after parse row
            freeArray(item_var);

            // create call, recurse only if ok!
            BasicBlock *bbCurrent = builder.GetInsertBlock();
            BasicBlock *bbArrayFound = BasicBlock::Create(_env.getContext(), "found_array", builder.GetInsertBlock()->getParent());
            BasicBlock *bbDecodeDone = BasicBlock::Create(_env.getContext(), "array_decode_done", builder.GetInsertBlock()->getParent());
            llvm::Value* rc_A = builder.CreateCall(F, {obj, key, item_var});
            builder.CreateStore(rc_A, rc_var);
            auto found_array = builder.CreateICmpEQ(rc_A, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            builder.CreateCondBr(found_array, bbArrayFound, bbDecodeDone);


            // -----------------------------------------------------------
            // step 2: check that it is a homogenous list...
            auto elementType = listType.elementType();
            builder.SetInsertPoint(bbArrayFound);
            auto array = builder.CreateLoad(item_var);
            auto num_elements = arraySize(builder, array);

            // for now dummy...
            _env.printValue(builder, num_elements, "found for type " + listType.desc() + " elements: ");

            // reserve capacity for elements
            bool initialize_elements_as_null = true; //false;
            list_reserve_capacity(_env, builder, list_ptr, listType, num_elements, initialize_elements_as_null);

            // decoding happens in a loop...
            // -> basically get the data!
            auto list_rc = generateDecodeListItemsLoop(builder, array, list_ptr, listType, num_elements);
            builder.CreateStore(list_rc, rc_var);
            _env.printValue(builder, list_rc, "decode result is: ");

            // only if decode is ok, store list size!
            auto list_decode_ok = builder.CreateICmpEQ(list_rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            BasicBlock* bbListOK = BasicBlock::Create(_env.getContext(), "array_decode_ok", builder.GetInsertBlock()->getParent());
            builder.CreateCondBr(list_decode_ok, bbListOK, bbDecodeDone);

            {
                // --- set list size hwen ok ---
                builder.SetInsertPoint(bbListOK);
                list_store_size(_env, builder, list_ptr, listType, num_elements); // <-- now list is ok!
                builder.CreateBr(bbDecodeDone);
            }

            builder.SetInsertPoint(bbDecodeDone);
            llvm::Value* rc = builder.CreateLoad(rc_var);
            SerializableValue value;
            value.val = builder.CreateLoad(list_ptr); // retrieve the ptr representing the list
            return make_tuple(rc, value);
        }

        std::tuple<llvm::Value*, llvm::Value*, SerializableValue> JSONSourceTaskBuilder::decodePrimitiveFieldFromObject(llvm::IRBuilder<>& builder,
                                                                                                 llvm::Value* obj,
                                                                                                 llvm::Value* key,
                                                                                                 const python::StructEntry& entry,
                                                                                                 const DecodeOptions& options,
                                                                                                 llvm::BasicBlock *bbSchemaMismatch) {
            using namespace std;
            using namespace llvm;

            llvm::Value* rc = nullptr;
            llvm::Value* is_present = nullptr;
            SerializableValue value;

            // checks
            assert(obj);
            assert(key);
            assert(!entry.valueType.isStructuredDictionaryType()); // --> this function doesn't support nested decode.
            assert(entry.keyType == python::Type::STRING); // --> JSON decode ONLY supports string keys.

            auto value_type = entry.valueType;
            auto& ctx = _env.getContext();

            // special case: option => i.e. perform null check first. If it fails, decode element.
            if(value_type.isOptionType()) {

                BasicBlock* bbCurrent = builder.GetInsertBlock();
                BasicBlock* bbDecodeIsNull = BasicBlock::Create(ctx, "decode_option_null", bbCurrent->getParent());
                BasicBlock* bbDecodeNonNull = BasicBlock::Create(ctx, "decode_option_non_null", bbCurrent->getParent());
                BasicBlock* bbDecoded = BasicBlock::Create(ctx, "decoded_option", bbCurrent->getParent());

                // check if it is null
                llvm::Value* rcA = nullptr;
                std::tie(rcA, value) = decodeNull(builder, obj, key);
                auto successful_decode_cond = builder.CreateICmpEQ(rcA, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                auto is_null_cond = builder.CreateAnd(successful_decode_cond, value.is_null);

                // branch: if null -> got to bbDecodeIsNull, else decode value.
                BasicBlock* bbValueIsNull = nullptr, *bbValueIsNotNull = nullptr;
                builder.CreateCondBr(is_null_cond, bbDecodeIsNull, bbDecodeNonNull);

                // --- decode null ---
                builder.SetInsertPoint(bbDecodeIsNull);
                // _env.debugPrint(builder, "found null value for key=" + entry.key);
                bbValueIsNull = builder.GetInsertBlock();
                builder.CreateBr(bbDecoded);

                // --- decode value ---
                builder.SetInsertPoint(bbDecodeNonNull);
                // _env.debugPrint(builder, "found " + entry.valueType.getReturnType().desc() + " value for key=" + entry.key);
                llvm::Value* rcB = nullptr;
                llvm::Value* presentB = nullptr;
                SerializableValue valueB;
                python::StructEntry entryB = entry;
                entryB.valueType = entry.valueType.getReturnType(); // remove option
                std::tie(rcB, presentB, valueB) = decodePrimitiveFieldFromObject(builder, obj, key, entryB, options, bbSchemaMismatch);
                bbValueIsNotNull = builder.GetInsertBlock(); // <-- this is the block from where to jump to bbDecoded (phi entry block)
                builder.CreateBr(bbDecoded);

                // --- decode done ----
                builder.SetInsertPoint(bbDecoded);
                // finish decode by jumping into bbDecoded block.
                // fetch rc and value depending on block (phi node!)
                // => for null, create dummy values so phi works!
                assert(rcB && presentB);
                SerializableValue valueA;
                valueA.is_null = _env.i1Const(true); // valueA is null
                if(valueB.val)
                    valueA.val = _env.nullConstant(valueB.val->getType());
                if(valueB.size)
                    valueA.size = _env.nullConstant(valueB.size->getType());

                builder.SetInsertPoint(bbDecoded);
                assert(bbValueIsNotNull && bbValueIsNull);
                value = SerializableValue();
                if(valueB.val) {
                    auto phi = builder.CreatePHI(valueB.val->getType(), 2);
                    phi->addIncoming(valueA.val, bbValueIsNull);
                    phi->addIncoming(valueB.val, bbValueIsNotNull);
                    value.val = phi;
                }
                if(valueB.size) {
                    auto phi = builder.CreatePHI(valueB.size->getType(), 2);
                    phi->addIncoming(valueA.size, bbValueIsNull);
                    phi->addIncoming(valueB.size, bbValueIsNotNull);
                    value.size = phi;
                }
                value.is_null = is_null_cond; // trivial, no phi needed.

                // however, for rc a phi is needed.
                auto phi = builder.CreatePHI(_env.i64Type(), 2);
                phi->addIncoming(rcA, bbValueIsNull);
                phi->addIncoming(rcB, bbValueIsNotNull);
                rc = phi;
            } else {
                // decode non-option types
                auto v_type = value_type;
                assert(!v_type.isOptionType());

                if (v_type == python::Type::STRING) {
                    std::tie(rc, value) = decodeString(builder, obj, key);
                } else if (v_type == python::Type::BOOLEAN) {
                    std::tie(rc, value) = decodeBoolean(builder, obj, key);
                } else if (v_type == python::Type::I64) {
                    std::tie(rc, value) = decodeI64(builder, obj, key);
                } else if (v_type == python::Type::F64) {
                    std::tie(rc, value) = decodeF64(builder, obj, key);
                } else if (v_type == python::Type::NULLVALUE) {
                    std::tie(rc, value) = decodeNull(builder, obj, key);
                } else if (v_type == python::Type::EMPTYDICT) {
                    std::tie(rc, value) = decodeEmptyDict(builder, obj, key);
                } else if(v_type.isListType() || v_type == python::Type::EMPTYLIST) {
                    std::tie(rc, value) = decodeList(builder, obj, key, v_type);
                } else {
                    // for another nested object, utilize:
                    throw std::runtime_error("encountered unsupported value type " + value_type.desc());
                }
            }

            // perform now here depending on policy the present check etc.
            // basically if element should be always present - then a key error indicates it's missing
            // if it's a key error, change rc to success and return is_present as false
            if(entry.alwaysPresent) {
                // anything else than success? => go to schema mismatch
                auto is_not_ok = builder.CreateICmpNE(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                BasicBlock* bbOK = BasicBlock::Create(ctx, "extract_ok", builder.GetInsertBlock()->getParent());
                builder.CreateCondBr(is_not_ok, bbSchemaMismatch, bbOK);
                builder.SetInsertPoint(bbOK);
                is_present = _env.i1Const(true); // it's present, else there'd have been an error reported.
            } else {
                // is it a key error? => that's ok, element is simply not present.
                // is it a different error => issue!
                auto is_key_error = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::KEYERROR)));
                auto is_ok = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                auto is_not_ok = _env.i1neg(builder, builder.CreateOr(is_key_error, is_ok));
                BasicBlock* bbOK = BasicBlock::Create(ctx, "extract_ok", builder.GetInsertBlock()->getParent());
                builder.CreateCondBr(is_not_ok, bbSchemaMismatch, bbOK);
                builder.SetInsertPoint(bbOK);
                is_present = _env.i1neg(builder, is_key_error); // it's present if there is no key error.
            }

            // return rc (i.e., success or keyerror or whatever other error there is)
            // return is_present (indicates whether field was found or not)
            // return value => valid if rc == success AND is_present is true
            assert(rc->getType() == _env.i64Type());
            assert(is_present->getType() == _env.i1Type());
            return make_tuple(rc, is_present, value);
        }

        void JSONSourceTaskBuilder::decode(llvm::IRBuilder<> &builder,
                                           llvm::Value* dict_ptr,
                                           const python::Type& dict_ptr_type,
                                           llvm::Value *object,
                                           llvm::BasicBlock* bbSchemaMismatch,
                                           const python::Type &dict_type,
                                           std::vector<std::pair<std::string, python::Type>> prefix,
                                           bool include_maybe_structs,
                                           const DecodeOptions& options) {
            using namespace std;
            using namespace llvm;

            auto& logger = Logger::instance().logger("codegen");
            auto& ctx = _env.getContext();
            assert(dict_type.isStructuredDictionaryType());
            assert(dict_ptr && dict_ptr->getType()->isPointerTy());

            for (const auto& kv_pair: dict_type.get_struct_pairs()) {
                vector <pair<string, python::Type>> access_path = prefix; // = prefix
                access_path.push_back(make_pair(kv_pair.key, kv_pair.keyType));

                auto key_value = str_value_from_python_raw_value(kv_pair.key); // it's an encoded value, but query here for the real key.
                auto key = _env.strConst(builder, key_value);

                if (kv_pair.valueType.isStructuredDictionaryType()) {
                    logger.debug("parsing nested dict: " +
                                 json_access_path_to_string(access_path, kv_pair.valueType, kv_pair.alwaysPresent));

                    // check if an object exists under the given key.
                    auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getObject", _env.i64Type(),
                                                 _env.i8ptrType(),
                                                 _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
                    auto item_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr());
                    // create call, recurse only if ok!
                    llvm::Value *rc = builder.CreateCall(F, {object, key, item_var});

                    auto is_object = builder.CreateICmpEQ(rc, _env.i64Const(
                            ecToI64(ExceptionCode::SUCCESS))); // <-- indicates successful parse

                    // special case: if include maybe structs as well, add entry. (should not get serialized)
                    if (include_maybe_structs && !kv_pair.alwaysPresent) {

                        // store presence into struct dict ptr
                        struct_dict_store_present(_env, builder, dict_ptr, dict_ptr_type, access_path, is_object);
                        // present if is_object == true
                        // --> as for value, use a dummy.
                        // entries.push_back(
                        //        make_tuple(access_path, kv_pair.valueType, kv_pair.alwaysPresent, SerializableValue(),
                        //                   is_object));
                    }

                    // create now some basic blocks to decode ON demand.
                    BasicBlock *bbDecodeItem = BasicBlock::Create(ctx, "decode_object", builder.GetInsertBlock()->getParent());
                    BasicBlock *bbDecodeDone = BasicBlock::Create(ctx, "next_item", builder.GetInsertBlock()->getParent());
                    builder.CreateCondBr(is_object, bbDecodeItem, bbDecodeDone);

                    builder.SetInsertPoint(bbDecodeItem);
                    // load item!
                    auto item = builder.CreateLoad(item_var);
                    // recurse using new prefix
                    // --> similar to flatten_recursive_helper(entries, kv_pair.valueType, access_path, include_maybe_structs);
                    decode(builder, dict_ptr, dict_ptr_type, item, bbSchemaMismatch, kv_pair.valueType, access_path, include_maybe_structs, options);
                    builder.CreateBr(bbDecodeDone); // whererver builder is, continue to decode done for this item.
                    builder.SetInsertPoint(bbDecodeDone); // continue from here...
                } else {

                     // // comment this, in order to invoke the list decoding (not completed yet...) -> requires serialization!
                     // // debug: skip list for now (more complex)
                     // if(kv_pair.valueType.isListType()) {
                     //     std::cerr<<"skipping array decode with type="<<kv_pair.valueType.desc()<<" for now."<<std::endl;
                     //     continue;
                     // }

                    // basically get the entry for the kv_pair.
                    logger.debug("generating code to decode " + json_access_path_to_string(access_path, kv_pair.valueType, kv_pair.alwaysPresent));
                    SerializableValue decoded_value;
                    llvm::Value* value_is_present = nullptr;
                    llvm::Value* rc = nullptr; // can ignore rc -> parse escapes to mismatch...
                    std::tie(rc, value_is_present, decoded_value) = decodePrimitiveFieldFromObject(builder, object, key, kv_pair, options, bbSchemaMismatch);

                     // // comment this, in order to invoke the list decoding (not completed yet...) -> requires serialization!
                     // if(kv_pair.valueType.isListType()) {
                     //     std::cerr<<"skipping array store in final struct with type="<<kv_pair.valueType.desc()<<" for now."<<std::endl;
                     //     continue;
                     // }

                    // store!
                    struct_dict_store_value(_env, builder, dict_ptr, dict_ptr_type, access_path, decoded_value.val);
                    struct_dict_store_size(_env, builder, dict_ptr, dict_ptr_type, access_path, decoded_value.size);
                    struct_dict_store_isnull(_env, builder, dict_ptr, dict_ptr_type, access_path, decoded_value.is_null);
                    struct_dict_store_present(_env, builder, dict_ptr, dict_ptr_type, access_path, value_is_present);

                    // optimized store using if logic... --> beneficial?

                    // entries.push_back(make_tuple(access_path, kv_pair.valueType, kv_pair.alwaysPresent, decoded_value, value_is_present));
                }
            }
        }

        llvm::Value *JSONSourceTaskBuilder::arraySize(llvm::IRBuilder<> &builder, llvm::Value *arr) {
            assert(arr);
            assert(arr->getType() == _env.i8ptrType());

            // call func
            using namespace llvm;
            auto &ctx = _env.getContext();

            auto F = getOrInsertFunction(_env.getModule().get(), "JsonArray_Size", _env.i64Type(),
                                                _env.i8ptrType());
            return builder.CreateCall(F, arr);
        }


        void JSONSourceTaskBuilder::checkRC(llvm::IRBuilder<> &builder, const std::string &key, llvm::Value *rc) {
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            BasicBlock *bbPrint = BasicBlock::Create(ctx, key + "_present", F);
            BasicBlock *bbNext = BasicBlock::Create(ctx, key + "_done", F);

            // check what the rc values are
            auto bad_value = builder.CreateICmpNE(rc, _env.i64Const(0));
            builder.CreateCondBr(bad_value, bbPrint, bbNext);
            builder.SetInsertPoint(bbPrint);

            // _env.printValue(builder, rc, "rc for key=" + key + " is: ");

            builder.CreateBr(bbNext);
            builder.SetInsertPoint(bbNext);
        }

        void JSONSourceTaskBuilder::printValueInfo(llvm::IRBuilder<> &builder,
                                                   const std::string &key,
                                                   const python::Type &valueType,
                                                   llvm::Value *keyPresent,
                                                   const tuplex::codegen::SerializableValue &value) {

            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            BasicBlock *bbPresent = BasicBlock::Create(ctx, key + "_present", F);
            BasicBlock *bbNotNull = BasicBlock::Create(ctx, key + "_notnull", F);
            BasicBlock *bbNext = BasicBlock::Create(ctx, key + "_done", F);

            builder.CreateCondBr(keyPresent, bbPresent, bbNext);

            builder.SetInsertPoint(bbPresent);
            _env.debugPrint(builder, "key " + key + " is present");
            auto is_null = value.is_null ? value.is_null : _env.i1Const(false);
            builder.CreateCondBr(is_null, bbNext, bbNotNull);

            builder.SetInsertPoint(bbNotNull);
             if(value.val && !valueType.isStructuredDictionaryType())
                _env.printValue(builder, value.val, "decoded key=" + key + " as " + valueType.desc());
            builder.CreateBr(bbNext);

            builder.SetInsertPoint(bbNext);
        }


        llvm::Value *JSONSourceTaskBuilder::numberOfKeysInObject(llvm::IRBuilder<> &builder, llvm::Value *j) {
            assert(j);

            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_numberOfKeys", _env.i64Type(),
                                         _env.i8ptrType());
            return builder.CreateCall(F, j);
        }

        llvm::Value *JSONSourceTaskBuilder::decodeFieldFromObject(llvm::IRBuilder<> &builder,
                                                                  llvm::Value *obj,
                                                                  const std::string &debug_path,
                                                                  tuplex::codegen::SerializableValue *out,
                                                                  bool alwaysPresent,
                                                                  llvm::Value *key,
                                                                  const python::Type &keyType,
                                                                  const python::Type &valueType,
                                                                  bool check_that_all_keys_are_present,
                                                                  llvm::BasicBlock *bbSchemaMismatch) {
            using namespace llvm;

            if (keyType != python::Type::STRING)
                throw std::runtime_error("so far only string type supported for decoding");

            assert(key && out);

            auto &ctx = _env.getContext();

            SerializableValue v;
            llvm::Value *rc = nullptr;

            // special case: option
            auto v_type = valueType.isOptionType() ? valueType.getReturnType() : valueType;
            llvm::Module *mod = _env.getModule().get();

            if (v_type == python::Type::STRING) {
                // decode using string
                auto F = getOrInsertFunction(mod, "JsonItem_getStringAndSize", _env.i64Type(), _env.i8ptrType(),
                                             _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0), _env.i64ptrType());
                auto str_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr(), "s");
                auto str_size_var = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "s_size");
                rc = builder.CreateCall(F, {obj, key, str_var, str_size_var});
                v.val = builder.CreateLoad(str_var);
                v.size = builder.CreateLoad(str_size_var);
                v.is_null = _env.i1Const(false);
            } else if (v_type == python::Type::BOOLEAN) {
                auto F = getOrInsertFunction(mod, "JsonItem_getBoolean", _env.i64Type(), _env.i8ptrType(),
                                             _env.i8ptrType(),
                                             ctypeToLLVM<bool>(ctx)->getPointerTo());
                auto b_var = _env.CreateFirstBlockVariable(builder, cbool_const(ctx, false));
                rc = builder.CreateCall(F, {obj, key, b_var});
                v.val = _env.upcastToBoolean(builder, builder.CreateLoad(b_var));
                v.size = _env.i64Const(sizeof(int64_t));
                v.is_null = _env.i1Const(false);
            } else if (v_type == python::Type::I64) {
                auto F = getOrInsertFunction(mod, "JsonItem_getInt", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(),
                                             _env.i64ptrType());
                auto i_var = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
                rc = builder.CreateCall(F, {obj, key, i_var});
                v.val = builder.CreateLoad(i_var);
                v.size = _env.i64Const(sizeof(int64_t));
                v.is_null = _env.i1Const(false);
            } else if (v_type == python::Type::F64) {
                auto F = getOrInsertFunction(mod, "JsonItem_getDouble", _env.i64Type(), _env.i8ptrType(),
                                             _env.i8ptrType(),
                                             _env.doublePointerType());
                auto f_var = _env.CreateFirstBlockVariable(builder, _env.f64Const(0));
                rc = builder.CreateCall(F, {obj, key, f_var});
                v.val = builder.CreateLoad(f_var);
                v.size = _env.i64Const(sizeof(int64_t));
                v.is_null = _env.i1Const(false);
            } else if (v_type.isStructuredDictionaryType()) {
                auto F = getOrInsertFunction(mod, "JsonItem_getObject", _env.i64Type(), _env.i8ptrType(),
                                             _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
                auto obj_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr());
                // create call, recurse only if ok!
                BasicBlock *bbOK = BasicBlock::Create(ctx, "is_object", builder.GetInsertBlock()->getParent());


                rc = builder.CreateCall(F, {obj, key, obj_var});
                auto is_object = builder.CreateICmpEQ(rc, _env.i64Const(
                        ecToI64(ExceptionCode::SUCCESS))); // <-- indicates successful parse

                // if the object is maybe present, then key-error is not a problem.
                // correct condition therefore
                if (!alwaysPresent) {
                    BasicBlock *bbParseSub = BasicBlock::Create(ctx, "parse_object",
                                                                builder.GetInsertBlock()->getParent());
                    BasicBlock *bbContinue = BasicBlock::Create(ctx, "continue_parse",
                                                                builder.GetInsertBlock()->getParent());

                    // ok, when either success OR keyerror => can continue to OK.
                    // continue parse of subobject, if it was success. If it was key error, directly go to bbOK
                    auto is_keyerror = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::KEYERROR)));
                    auto is_ok = builder.CreateOr(is_keyerror, is_object);
                    builder.CreateCondBr(is_ok, bbContinue, bbSchemaMismatch);
                    builder.SetInsertPoint(bbContinue);

                    builder.CreateCondBr(is_keyerror, bbOK, bbParseSub);
                    builder.SetInsertPoint(bbParseSub);

                    // continue parse if present
                    auto sub_obj = builder.CreateLoad(obj_var);
                    // recurse...
                    parseAndPrint(builder, sub_obj, debug_path + ".", alwaysPresent, v_type, true, bbSchemaMismatch);
                    builder.CreateBr(bbOK);

                    // continue on ok block.
                    builder.SetInsertPoint(bbOK);
                } else {
                    builder.CreateCondBr(is_object, bbOK, bbSchemaMismatch);
                    builder.SetInsertPoint(bbOK);

                    auto sub_obj = builder.CreateLoad(obj_var);

                    // recurse...
                    parseAndPrint(builder, sub_obj, debug_path + ".", alwaysPresent, v_type, true, bbSchemaMismatch);
                }

                // free in free block.
                freeObject(obj_var);
            } else if (v_type.isListType()) {
                std::cerr << "skipping for now type: " << v_type.desc() << std::endl;
                rc = _env.i64Const(0); // ok.
            } else if (v_type == python::Type::NULLVALUE) {
                // special case!
                auto F = getOrInsertFunction(mod, "JsonItem_IsNull", _env.i64Type(), _env.i8ptrType(),
                                             _env.i8ptrType());
                rc = builder.CreateCall(F, {obj, key});
                v.is_null = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            } else if (v_type == python::Type::EMPTYDICT) {
                // special case!
                // query subobject and call count keys!
                auto F = getOrInsertFunction(mod, "JsonItem_getObject", _env.i64Type(), _env.i8ptrType(),
                                             _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
                auto obj_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr());
                // create call, recurse only if ok!
                BasicBlock *bbOK = BasicBlock::Create(ctx, "is_object", builder.GetInsertBlock()->getParent());

                rc = builder.CreateCall(F, {obj, key, obj_var});
                auto is_object = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                builder.CreateCondBr(is_object, bbOK, bbSchemaMismatch);
                builder.SetInsertPoint(bbOK);

                auto sub_obj = builder.CreateLoad(obj_var);

                // check how many entries
                auto num_keys = numberOfKeysInObject(builder, sub_obj);
                auto is_empty = builder.CreateICmpEQ(num_keys, _env.i64Const(0));

                //// free! @TODO: add to free list... -> yet should be ok?
                freeObject(obj_var);

                rc = builder.CreateSelect(is_empty, _env.i64Const(
                                                  ecToI64(ExceptionCode::SUCCESS)),
                                          _env.i64Const(
                                                  ecToI64(ExceptionCode::TYPEERROR)));
            } else {



                // for another nested object, utilize:

                throw std::runtime_error("encountered unsupported value type " + valueType.desc());
            }
            *out = v;

            return rc;
        }


        void JSONSourceTaskBuilder::parseAndPrint(llvm::IRBuilder<> &builder, llvm::Value *obj,
                                                  const std::string &debug_path, bool alwaysPresent,
                                                  const python::Type &t, bool check_that_all_keys_are_present,
                                                  llvm::BasicBlock *bbSchemaMismatch) {
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            if (t.isStructuredDictionaryType()) {

                auto kv_pairs = t.get_struct_pairs();

                // check how many keys are contained. If all are present, quick check -> count of keys
                bool all_keys_always_present = true;
                for (auto kv_pair: kv_pairs)
                    if (!kv_pair.alwaysPresent) {
                        all_keys_always_present = false;
                        break;
                    }

                if (all_keys_always_present && check_that_all_keys_are_present) {
                    // quick key check
                    // note that the expensive check has to be only performed when maybe keys are present.
                    // else, querying each field automatically will perform a presence check.
                    BasicBlock *bbOK = BasicBlock::Create(ctx, "all_keys_present_passed", F);
                    auto num_keys = numberOfKeysInObject(builder, obj);
                    auto cond = builder.CreateICmpNE(num_keys, _env.i64Const(kv_pairs.size()));
#ifndef NDEBUG
                    {
                        // print out expected vs. found
                        BasicBlock *bb = BasicBlock::Create(ctx, "debug", F);
                        BasicBlock *bbn = BasicBlock::Create(ctx, "debug_ct", F);
                        builder.CreateCondBr(cond, bb, bbn);
                        builder.SetInsertPoint(bb);
                        // _env.printValue(builder, num_keys, "struct type expected  " + std::to_string(kv_pairs.size()) + " elements, got: ");
                        builder.CreateBr(bbn);
                        builder.SetInsertPoint(bbn);
                    }
#endif
                    builder.CreateCondBr(cond, bbSchemaMismatch, bbOK);
                    builder.SetInsertPoint(bbOK);
                } else if (check_that_all_keys_are_present) {
                    // perform check by generating appropriate constants
                    // this is the expensive key check.
                    // -> i.e. should be used to match only against general-case.
                    // generate constants
                    std::vector<std::string> alwaysKeys;
                    std::vector<std::string> maybeKeys;
                    for (const auto &kv_pair: kv_pairs) {
                        // for JSON should be always keyType == string!
                        assert(kv_pair.keyType == python::Type::STRING);
                        if (kv_pair.alwaysPresent)
                            alwaysKeys.push_back(str_value_from_python_raw_value(kv_pair.key));
                        else
                            maybeKeys.push_back(str_value_from_python_raw_value(kv_pair.key));
                    }

                    auto sconst_always_keys = _env.strConst(builder, makeKeySetBuffer(alwaysKeys));
                    auto sconst_maybe_keys = _env.strConst(builder, makeKeySetBuffer(maybeKeys));

                    // perform check using helper function on item.
                    BasicBlock *bbOK = BasicBlock::Create(ctx, "keycheck_passed", F);
                    // call uint64_t JsonItem_keySetMatch(JsonItem *item, uint8_t* always_keys_buf, uint8_t* maybe_keys_buf)
                    auto Fcheck = getOrInsertFunction(_env.getModule().get(), "JsonItem_keySetMatch", _env.i64Type(),
                                                      _env.i8ptrType(), _env.i8ptrType(), _env.i8ptrType());
                    auto rc = builder.CreateCall(Fcheck, {obj, sconst_always_keys, sconst_maybe_keys});
                    auto cond = builder.CreateICmpNE(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                    builder.CreateCondBr(cond, bbSchemaMismatch, bbOK);
                    builder.SetInsertPoint(bbOK);
                }

                for (const auto &kv_pair: kv_pairs) {
                    llvm::Value *keyPresent = _env.i1Const(true); // default to always present

                    SerializableValue value;
                    auto key_value = str_value_from_python_raw_value(
                            kv_pair.key); // it's an encoded value, but query here for the real key.
                    // _env.debugPrint(builder, "decoding now key=" + key_value + " of path " + debug_path);
                    if (key_value == "payload") {
                        std::cout << "debug" << std::endl;
                    }
                    auto rc = decodeFieldFromObject(builder, obj, debug_path + "." + key_value, &value,
                                                    kv_pair.alwaysPresent, key_value, kv_pair.keyType,
                                                    kv_pair.valueType, check_that_all_keys_are_present,
                                                    bbSchemaMismatch);
                    auto successful_lookup = rc ? builder.CreateICmpEQ(rc,
                                                                       _env.i64Const(ecToI64(ExceptionCode::SUCCESS)))
                                                : _env.i1Const(false);

                    // optional? or always there?
                    if (kv_pair.alwaysPresent) {
                        // needs to be present, i.e. key error is fatal error!
                        // --> add check, and jump to mismatch else
                        BasicBlock *bbOK = BasicBlock::Create(ctx, "key_present",
                                                              builder.GetInsertBlock()->getParent());

                        // if(key_value == "payload") {
                        //    _env.printValue(builder, rc, "rc for payload is: ");
                        // }
                        builder.CreateCondBr(successful_lookup, bbOK, bbSchemaMismatch);
                        builder.SetInsertPoint(bbOK);
                    } else {
                        // can or can not be present.
                        // => change variable meaning
                        keyPresent = successful_lookup;
                        successful_lookup = _env.i1Const(true);
                    }

                    // can now print the 4 values if need be or store them away.
                    // note: should be done by checking! --> this here is a debug function.
                    //printValueInfo(builder, key_value, kv_pair.valueType, keyPresent, value);
                    if (rc)
                        checkRC(builder, key_value, rc);
                }


            } else {
                // other types, parse with type check!
                throw std::runtime_error("unsupported type");
            }
        }

        void JSONSourceTaskBuilder::freeArray(llvm::Value *arr) {
            assert(arr);
            using namespace llvm;
            auto &ctx = _env.getContext();

            auto ptr_to_free = arr;

            // free in free block (last block!)
            assert(_freeEnd);
            IRBuilder<> b(_freeEnd);

            // what type is it?
            if (arr->getType() == _env.i8ptrType()->getPointerTo()) {
                ptr_to_free = b.CreateLoad(arr);
            }

            freeArray(b, ptr_to_free);

            if (arr->getType() == _env.i8ptrType()->getPointerTo()) {
                // store nullptr in debug mode
#ifndef NDEBUG
                b.CreateStore(_env.i8nullptr(), arr);
#endif
            }
            _freeEnd = b.GetInsertBlock();
            assert(_freeEnd);
        }

        void JSONSourceTaskBuilder::freeObject(llvm::Value *obj) {
            assert(obj);
            using namespace llvm;
            auto &ctx = _env.getContext();

            auto ptr_to_free = obj;

            // free in free block (last block!)
            assert(_freeEnd);
            IRBuilder<> b(_freeEnd);

            // what type is it?
            if (obj->getType() == _env.i8ptrType()->getPointerTo()) {
                ptr_to_free = b.CreateLoad(obj);
            }

            freeObject(b, ptr_to_free);

            if (obj->getType() == _env.i8ptrType()->getPointerTo()) {
                // store nullptr in debug mode
#ifndef NDEBUG
                b.CreateStore(_env.i8nullptr(), obj);
#endif
            }
            _freeEnd = b.GetInsertBlock();
            assert(_freeEnd);
        }

        void JSONSourceTaskBuilder::freeObject(llvm::IRBuilder<> &builder, llvm::Value *obj) {
            using namespace llvm;
            auto &ctx = _env.getContext();

            auto Ffreeobj = getOrInsertFunction(_env.getModule().get(), "JsonItem_Free", llvm::Type::getVoidTy(ctx),
                                                _env.i8ptrType());
            builder.CreateCall(Ffreeobj, obj);
        }

        void JSONSourceTaskBuilder::freeArray(llvm::IRBuilder<> &builder, llvm::Value *arr) {
            using namespace llvm;
            auto &ctx = _env.getContext();

            auto Ffreeobj = getOrInsertFunction(_env.getModule().get(), "JsonArray_Free", llvm::Type::getVoidTy(ctx),
                                                _env.i8ptrType());
            builder.CreateCall(Ffreeobj, arr);
        }

        void JSONSourceTaskBuilder::parseAndPrintStructuredDictFromObject(llvm::IRBuilder<> &builder, llvm::Value *j,
                                                                          llvm::BasicBlock *bbSchemaMismatch) {
            assert(j);
            using namespace llvm;
            auto &ctx = _env.getContext();

            // get initial object
            // => this is from parser
            auto Fgetobj = getOrInsertFunction(_env.getModule().get(), "JsonParser_getObject", _env.i64Type(),
                                               _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));

            auto obj_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr(), "row_object");
            builder.CreateCall(Fgetobj, {j, obj_var});

            // don't forget to free everything...

            // alloc variable
            auto struct_dict_type = create_structured_dict_type(_env, "dict_struct", _rowType);
            auto row_var = _env.CreateFirstBlockAlloca(builder, struct_dict_type);
            struct_dict_mem_zero(_env, builder, row_var, _rowType); // !!! important !!!


            // decode everything -> entries can be then used to store to a struct!
            decode(builder, row_var, _rowType, builder.CreateLoad(obj_var), bbSchemaMismatch, _rowType, {}, true);

            auto s = struct_dict_type_serialized_memory_size(_env, builder, row_var, _rowType);
             _env.printValue(builder, s.val, "size of row materialized in bytes is: ");

             // rtmalloc and serialize!
             auto mem_ptr = _env.malloc(builder, s.val);
             auto serialization_res = struct_dict_serialize_to_memory(_env, builder, row_var, _rowType, mem_ptr);
             _env.printValue(builder, serialization_res.size, "realized serialization size is: ");

            // now, load entries to struct type in LLVM
            // then calculate serialized size and print it.
            //auto v = struct_dict_load_from_values(_env, builder, _rowType, entries);


            //// => call with row type
            //parseAndPrint(builder, builder.CreateLoad(obj_var), "", true, _rowType, true, bbSchemaMismatch);

            // free obj_var...
            freeObject(builder, builder.CreateLoad(obj_var));
#ifndef NDEBUG
            builder.CreateStore(_env.i8nullptr(), obj_var);
#endif

            // build schema mismatch block.


        }

        llvm::Value *JSONSourceTaskBuilder::isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j) {
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_getDocType", _env.i64Type(),
                                         _env.i8ptrType());
            auto call_res = builder.CreateCall(F, j);
            auto cond = builder.CreateICmpEQ(call_res, _env.i64Const(JsonParser_objectDocType()));
            return cond;
        }

        llvm::BasicBlock *
        JSONSourceTaskBuilder::emitBadParseInputAndMoveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j,
                                                                 llvm::Value *condition) {
            using namespace llvm;
            auto &ctx = _env.getContext();

            auto F = builder.GetInsertBlock()->getParent();

            BasicBlock *bbOK = BasicBlock::Create(ctx, "ok", F);
            BasicBlock *bbEmitBadParse = BasicBlock::Create(ctx, "bad_parse", F);
            builder.CreateCondBr(condition, bbEmitBadParse, bbOK);

            // ---- bad parse blocks ----
            //            auto line = JsonParser_getMallocedRow(j);
            //            free(line);
            // --> i.e. call exception handler from here...
            builder.SetInsertPoint(bbEmitBadParse);
            auto Frow = getOrInsertFunction(_env.getModule().get(), "JsonParser_getMallocedRow", _env.i8ptrType(),
                                            _env.i8ptrType());
            auto line = builder.CreateCall(Frow, j);

            // simply print (later call with error)
            _env.printValue(builder, rowNumber(builder), "bad parse encountered for row number: ");

            // inc value
            auto count = builder.CreateLoad(_badParseCountVar);
            builder.CreateStore(builder.CreateAdd(count, _env.i64Const(1)), _badParseCountVar);

            //_env.printValue(builder, line, "bad-parse for row: ");
            // this is ok here, b.c. it's local.
            _env.cfree(builder, line);

            // go to free block -> that will then take care of moving back to header.
            builder.CreateBr(_freeStart);

            // ok block
            builder.SetInsertPoint(bbOK);
            return bbEmitBadParse;
        }

        llvm::Value *JSONSourceTaskBuilder::hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_hasNextRow", ctypeToLLVM<bool>(ctx),
                                         _env.i8ptrType());

            auto v = builder.CreateCall(F, {j});
            return builder.CreateICmpEQ(v, llvm::ConstantInt::get(
                    llvm::Type::getIntNTy(ctx, ctypeToLLVM<bool>(ctx)->getIntegerBitWidth()), 1));
        }

        void JSONSourceTaskBuilder::moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            // move
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_moveToNextRow", ctypeToLLVM<bool>(ctx),
                                         _env.i8ptrType());
            builder.CreateCall(F, {j});

            // update row number (inc +1)
            auto row_no = rowNumber(builder);
            builder.CreateStore(builder.CreateAdd(row_no, _env.i64Const(1)), _rowNumberVar);

            // @TODO: free everything so far??
        }

        void JSONSourceTaskBuilder::exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition,
                                                              llvm::Value *exitCode) {
            using namespace llvm;
            auto &ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            assert(exitCondition->getType() == _env.i1Type());
            assert(exitCode->getType() == _env.i64Type());

            // branch and exit
            BasicBlock *bbExit = BasicBlock::Create(ctx, "exit_with_error", F);
            BasicBlock *bbContinue = BasicBlock::Create(ctx, "no_error", F);
            builder.CreateCondBr(exitCondition, bbExit, bbContinue);
            builder.SetInsertPoint(bbExit);
            builder.CreateRet(exitCode);
            builder.SetInsertPoint(bbContinue);
        }

        llvm::Value *JSONSourceTaskBuilder::initJsonParser(llvm::IRBuilder<> &builder) {

            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_Init", _env.i8ptrType());

            auto j = builder.CreateCall(F, {});
            auto is_null = builder.CreateICmpEQ(j, _env.i8nullptr());
            exitMainFunctionWithError(builder, is_null, _env.i64Const(ecToI64(ExceptionCode::NULLERROR)));
            return j;
        }

        llvm::Value *JSONSourceTaskBuilder::openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf,
                                                        llvm::Value *buf_size) {
            assert(j);
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_open", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i64Type());
            return builder.CreateCall(F, {j, buf, buf_size});
        }

        void JSONSourceTaskBuilder::freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_Free", llvm::Type::getVoidTy(ctx),
                                         _env.i8ptrType());
            builder.CreateCall(F, j);
        }

        void JSONSourceTaskBuilder::generateParseLoop(llvm::IRBuilder<> &builder, llvm::Value *bufPtr,
                                                      llvm::Value *bufSize) {
            using namespace llvm;
            auto &ctx = _env.getContext();

            // this will be a loop
            auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "loop_exit", F);

            // init json parse
            // auto j = JsonParser_init();
            // if(!j)
            //     throw std::runtime_error("failed to initialize parser");
            // JsonParser_open(j, buf, buf_size);
            // while(JsonParser_hasNextRow(j)) {
            //     if(JsonParser_getDocType(j) != JsonParser_objectDocType()) {

            auto parser = initJsonParser(builder);

            // init row number
            _rowNumberVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "row_no");
            _badParseCountVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "badparse_count");

            // create single free block
            _freeStart = _freeEnd = BasicBlock::Create(ctx, "free_row_objects", F);

#ifndef NDEBUG
            {
                // debug: create an info statement for free block
                llvm::IRBuilder<> b(_freeStart);
                // _env.printValue(b, rowNumber(b), "entered free row objects for row no=");
            }
#endif
            llvm::Value *rc = openJsonBuf(builder, parser, bufPtr, bufSize);
            llvm::Value *rc_cond = _env.i1neg(builder,
                                              builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS))));
            exitMainFunctionWithError(builder, rc_cond, rc);
            builder.CreateBr(bLoopHeader);


            // ---- loop condition ---
            // go from current block to header
            builder.SetInsertPoint(bLoopHeader);
            // condition (i.e. hasNextDoc)
            auto cond = hasNextRow(builder, parser);
            builder.CreateCondBr(cond, bLoopBody, bLoopExit);




            // ---- loop body ----
            // body
            builder.SetInsertPoint(bLoopBody);
            // generate here...
            // _env.debugPrint(builder, "parsed row");

            // check whether it's of object type -> parse then as object (only supported type so far!)
            cond = isDocumentOfObjectType(builder, parser);
            auto bbSchemaMismatch = emitBadParseInputAndMoveToNextRow(builder, parser, _env.i1neg(builder, cond));

            // print out structure -> this is the parse
            parseAndPrintStructuredDictFromObject(builder, parser, bbSchemaMismatch);

            // go to free start
            builder.CreateBr(_freeStart);

            // free data..
            // --> parsing will generate there free statements per row

            builder.SetInsertPoint(_freeEnd); // free is done -> now move onto next row.
            // go to next row
            moveToNextRow(builder, parser);

            // this will only work when allocating everything local!
            // -> maybe better craft a separate process row function?

            // link back to header
            builder.CreateBr(bLoopHeader);

            // ---- post loop block ----
            // continue in loop exit.
            builder.SetInsertPoint(bLoopExit);

            // free JSON parse (global object)
            freeJsonParse(builder, parser);

            _env.printValue(builder, rowNumber(builder), "parsed rows: ");
            _env.printValue(builder, builder.CreateLoad(_badParseCountVar),
                            "thereof bad parse rows (schema mismatch): ");
        }

        void JSONSourceTaskBuilder::build() {
            using namespace llvm;
            auto &ctx = _env.getContext();

            // create main function (takes buffer and buf_size, later take the other tuplex stuff)
            FunctionType *FT = FunctionType::get(ctypeToLLVM<int64_t>(ctx),
                                                 {ctypeToLLVM<char *>(ctx), ctypeToLLVM<int64_t>(ctx)}, false);

            Function *F = Function::Create(FT, llvm::GlobalValue::ExternalLinkage, _functionName,
                                           *_env.getModule().get());
            auto m = mapLLVMFunctionArgs(F, {"buf", "buf_size"});

            auto bbEntry = BasicBlock::Create(ctx, "entry", F);
            IRBuilder<> builder(bbEntry);

            // dummy parse, simply print type and value with type checking.
            generateParseLoop(builder, m["buf"], m["buf_size"]);

            builder.CreateRet(_env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
        }


        void calculate_field_counts(const python::Type &type, size_t &field_count, size_t &option_count,
                                    size_t &maybe_count) {
            if (type.isStructuredDictionaryType()) {
                // recurse
                auto kv_pairs = type.get_struct_pairs();
                for (const auto &kv_pair: kv_pairs) {
                    maybe_count += !kv_pair.alwaysPresent;

                    // count optional key as well
                    if (kv_pair.keyType.isOptionType())
                        throw std::runtime_error("unsupported now");

                    calculate_field_counts(kv_pair.valueType, field_count, option_count, maybe_count);
                }
            } else {
                if (type.isOptionType()) {
                    option_count++;
                    calculate_field_counts(type.getReturnType(), field_count, option_count, maybe_count);
                } else {
                    // count as one field (true even for lists etc.) -> only unnest { { ...}, ... }
                    field_count++;
                }
            }
        }

        inline bool noNeedToSerializeType(const python::Type &t) {
            // some types do not need to get serialized. This function specifies this
            if (t.isConstantValued())
                return true; // no need to serialize constants!
            if (t.isSingleValued())
                return true; // no need to serialize special constant values (like null, empty dict, empty list, empty tuple, ...)
            return false;
        }

        // flatten struct dict.
        using flattened_struct_dict_entry_list_t = std::vector<std::tuple<std::vector<std::pair<std::string, python::Type>>, python::Type, bool>>;

        void flatten_recursive_helper(flattened_struct_dict_entry_list_t &entries,
                                      const python::Type &dict_type,
                                      std::vector<std::pair<std::string, python::Type>> prefix = {},
                                      bool include_maybe_structs = true) {
            using namespace std;

            assert(dict_type.isStructuredDictionaryType());

            for (auto kv_pair: dict_type.get_struct_pairs()) {
                vector<pair<string, python::Type>> access_path = prefix; // = prefix
                access_path.push_back(make_pair(kv_pair.key, kv_pair.keyType));

                if (kv_pair.valueType.isStructuredDictionaryType()) {

                    // special case: if include maybe structs as well, add entry. (should not get serialized)
                    if (include_maybe_structs && !kv_pair.alwaysPresent)
                        entries.push_back(make_tuple(access_path, kv_pair.valueType, kv_pair.alwaysPresent));

                    // recurse using new prefix
                    flatten_recursive_helper(entries, kv_pair.valueType, access_path, include_maybe_structs);
                } else {
                    entries.push_back(make_tuple(access_path, kv_pair.valueType, kv_pair.alwaysPresent));
                }
            }
        }

        void print_flatten_structured_dict_type(const python::Type &dict_type) {
            using namespace std;

            // each entry is {(key, key_type), ..., (key, key_type)}, value_type, alwaysPresent
            // only nested dicts are flattened. Tuples etc. are untouched. (would be too cumbersome)
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type, {});

            // now print out everything...
            std::stringstream ss;
            for (auto entry: entries) {
                // first the path:
                for (auto atom: std::get<0>(entry)) {
                    ss << atom.first << " (" << atom.second.desc() << ") -> ";
                }
                auto presence = !std::get<2>(entry) ? "  (maybe)" : "";
                auto v_type = std::get<1>(entry);
                auto value_desc = v_type.isStructuredDictionaryType() ? "Struct[...]" : v_type.desc();
                ss << value_desc << presence << endl;
            }

            cout << ss.str() << endl;
        }


        static std::unordered_map<python::Type, llvm::Type*> m;

        // creating struct type based on structured dictionary type
        llvm::Type *
        create_structured_dict_type(LLVMEnvironment &env, const std::string &name, const python::Type &dict_type) {
            using namespace llvm;
            auto &logger = Logger::instance().logger("codegen");
            llvm::LLVMContext &ctx = env.getContext();

            if (!dict_type.isStructuredDictionaryType()) {
                logger.error("provided type is not a structured dict type but " + dict_type.desc());
                return nullptr;
            }

            // need a hashmap for this
            if(m.find(dict_type) != m.end())
                return m[dict_type];


            print_flatten_structured_dict_type(dict_type);

            // --> flattening the dict like this will guarantee that each level is local to itself, simplifying access.
            // (could also organize in fixed_size fields or not, but this here works as well)

            // each entry is {(key, key_type), ..., (key, key_type)}, value_type, alwaysPresent
            // only nested dicts are flattened. Tuples etc. are untouched. (would be too cumbersome)
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type, {});


            // retrieve counts => i.e. how many fields are options? how many are maybe present?
            size_t field_count = 0, option_count = 0, maybe_count = 0;

            for (auto entry: entries) {
                bool is_always_present = std::get<2>(entry);
                maybe_count += !is_always_present;
                bool is_value_optional = std::get<1>(entry).isOptionType();
                option_count += is_value_optional;

                bool is_struct_type = std::get<1>(entry).isStructuredDictionaryType();
                field_count += !is_struct_type; // only count non-struct dict fields. -> yet the nested struct types may change the maybe count for the bitmap!
            }

            std::stringstream ss;
            ss << "computed following counts for structured dict type: " << pluralize(field_count, "field")
               << " " << pluralize(option_count, "option") << " " << pluralize(maybe_count, "maybe");
            logger.info(ss.str());


            // let's start by allocating bitmaps for optional AND maybe types
            size_t num_option_bitmap_bits = core::ceilToMultiple(option_count, 64ul); // multiples of 64bit
            size_t num_maybe_bitmap_bits = core::ceilToMultiple(maybe_count, 64ul);
            size_t num_option_bitmap_elements = num_option_bitmap_bits / 64;
            size_t num_maybe_bitmap_elements = num_maybe_bitmap_bits / 64;


            bool is_packed = false;
            std::vector<llvm::Type *> member_types;
            auto i64Type = llvm::Type::getInt64Ty(ctx);

            // adding bitmap fails type creation - super weird.
            // add bitmap elements (if needed)

            // 64 bit logic
            // if (num_option_bitmap_elements > 0)
            //      member_types.push_back(llvm::ArrayType::get(i64Type, num_option_bitmap_elements));
            // if (num_maybe_bitmap_elements > 0)
            //      member_types.push_back(llvm::ArrayType::get(i64Type, num_maybe_bitmap_elements));

            // i1 logic (similar to flattened tuple)
            if (num_option_bitmap_elements > 0)
                member_types.push_back(llvm::ArrayType::get(Type::getInt1Ty(ctx), num_option_bitmap_bits));
            if (num_maybe_bitmap_elements > 0)
                member_types.push_back(llvm::ArrayType::get(Type::getInt1Ty(ctx), num_maybe_bitmap_bits));

            // auto a = ArrayType::get(Type::getInt1Ty(ctx), num_option_bitmap_bits);
            // if(num_option_bitmap_bits > 0)
            //    member_types.emplace_back(a);


            // now add all the elements from the (flattened) struct type (skip lists and other struct entries, i.e. only primitives so far)
            // --> could use a different structure as well! --> which to use?
            for (const auto &entry: entries) {
                // value type
                python::Type t = std::get<1>(entry);

                // is it a struct type? => skip.
                if (t.isStructuredDictionaryType())
                    continue;

                // // skip list
                // if (t.isListType())
                //     continue;

                // we do not save the key (because it's statically known), but simply lay out the data
                if (t.isOptionType())
                    t = t.getReturnType(); // option is handled above

                // do we actually need to serialize the value?
                // if not, no problem.
                if (noNeedToSerializeType(t))
                    continue;

                // serialize. Check if it is a fixed size type -> no size field required, else add an i64 field to store the var_length size!
                auto mapped_type = env.pythonToLLVMType(t);
                if (!mapped_type)
                    throw std::runtime_error("could not map type " + t.desc());
                member_types.push_back(mapped_type);

                // special case: list -> skip size!
                if(t.isListType())
                    continue;

                if (!t.isFixedSizeType()) {
                    // not fixes size but var length?
                    // add a size field!
                    member_types.push_back(i64Type);
                }
            }

//            // convert to C++ to check if godbolt works -.-
//            std::stringstream cc_code;
//            cc_code<<"struct LargeStruct {\n";
//            int pos = 0;
//            for(auto t : member_types) {
//                if(t->isIntegerTy()) {
//                    cc_code<<"   int"<<t->getIntegerBitWidth()<<"_t x"<<pos<<";\n";
//                }
//                if(t->isPointerTy()) {
//                    cc_code<<"   uint8_t* x"<<pos<<";\n";
//                }
//                if(t->isArrayTy()) {
//                    cc_code<<"   int64_t x"<<pos<<"["<<t->getArrayNumElements()<<"];\n";
//                }
//                pos++;
//            }
//            cc_code<<"};\n";
//            std::cout<<"C++ code:\n\n"<<cc_code.str()<<std::endl;

            // finally, create type
            // Note: these types can get super large!
            // -> therefore identify using identified struct (not opaque one!)

            // // this would create a literal struct
            // return llvm::StructType::get(ctx, members, is_packed);

//            // this creates an identified one (identifier!)
//            auto stype = llvm::StructType::create(ctx, name);
//
//            // do not set body (too large)
//            llvm::ArrayRef<llvm::Type *> members(member_types); // !!! important !!!
//            stype->setBody(members, is_packed); // for info

            llvm::Type **type_array = new llvm::Type *[member_types.size()];
            for (unsigned i = 0; i < member_types.size(); ++i) {
                type_array[i] = member_types[i];
            }
            llvm::ArrayRef<llvm::Type *> members(type_array, member_types.size());

            llvm::Type *structType = llvm::StructType::create(ctx, members, name, false);
            llvm::StructType *STy = dyn_cast<StructType>(structType);

            // store in hashmap
            m[dict_type] = structType;

            return structType;
        }

        void retrieve_bitmap_counts(const flattened_struct_dict_entry_list_t& entries, size_t& bitmap_element_count, size_t& presence_map_element_count) {
            // retrieve counts => i.e. how many fields are options? how many are maybe present?
            size_t field_count = 0, option_count = 0, maybe_count = 0;

            for (const auto& entry: entries) {
                bool is_always_present = std::get<2>(entry);
                maybe_count += !is_always_present;
                bool is_value_optional = std::get<1>(entry).isOptionType();
                option_count += is_value_optional;

                bool is_struct_type = std::get<1>(entry).isStructuredDictionaryType();
                field_count += !is_struct_type; // only count non-struct dict fields. -> yet the nested struct types may change the maybe count for the bitmap!
            }


            // let's start by allocating bitmaps for optional AND maybe types
            size_t num_option_bitmap_bits = core::ceilToMultiple(option_count, 64ul); // multiples of 64bit
            size_t num_maybe_bitmap_bits = core::ceilToMultiple(maybe_count, 64ul);
            size_t num_option_bitmap_elements = num_option_bitmap_bits / 64;
            size_t num_maybe_bitmap_elements = num_maybe_bitmap_bits / 64;

            bitmap_element_count = num_option_bitmap_elements;
            presence_map_element_count = num_maybe_bitmap_elements;
        }

        // create 64bit bitmap from 1bit vector (ceil!)
        std::vector<llvm::Value*> create_bitmap(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const std::vector<llvm::Value*>& v) {
            using namespace std;

            auto numBitmapElements = core::ceilToMultiple(v.size(), 64ul) / 64ul; // make 64bit bitmaps

            // construct bitmap using or operations
            vector<llvm::Value*> bitmapArray;
            for(int i = 0; i < numBitmapElements; ++i)
                bitmapArray.emplace_back(env.i64Const(0));

            // go through values and add to respective bitmap
            for(int i = 0; i < v.size(); ++i) {
                // get index within bitmap
                auto bitmapPos = i;
                assert(v[i]->getType() == env.i1Type());
                bitmapArray[bitmapPos / 64] = builder.CreateOr(bitmapArray[bitmapPos / 64], builder.CreateShl(
                        builder.CreateZExt(v[i], env.i64Type()),
                        env.i64Const(bitmapPos % 64)));

            }

            return bitmapArray;
        }

        // generates a map mapping from access path to indices: 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
        // if index is not need/present it's -1
        std::unordered_map<access_path_t, std::tuple<int, int, int, int>> struct_dict_load_indices(const python::Type& dict_type) {
            std::unordered_map<access_path_t, std::tuple<int, int, int, int>> indices;

            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            size_t num_bitmap = 0, num_presence_map = 0;

            retrieve_bitmap_counts(entries, num_bitmap, num_presence_map);
            bool has_bitmap = num_bitmap > 0;
            bool has_presence_map = num_presence_map > 0;

            int current_always_present_idx = 0;
            int current_idx = has_bitmap + has_presence_map; // potentially offset, -> bitmap is realized as i1s.
            int current_bitmap_idx = 0;

            for(auto& entry : entries) {
                auto access_path = std::get<0>(entry);
                auto value_type = std::get<1>(entry);
                auto always_present = std::get<2>(entry);

                int bitmap_idx = -1;
                int maybe_idx = -1;
                int field_idx = -1;
                int size_idx = -1;

                // manipulate indices accordingly
                if(!always_present) {
                    // not always present
                    maybe_idx = current_always_present_idx++; // save cur always index, and then inc (post inc!)
                }

                if(value_type.isOptionType()) {
                    bitmap_idx = current_bitmap_idx++;
                    value_type = value_type.getReturnType();
                }

                // special case: nested struct type!
                if(value_type.isStructuredDictionaryType()) {
                    indices[access_path] = std::make_tuple(bitmap_idx, maybe_idx, field_idx, size_idx);
                    continue; // --> skip, need to store only presence/bitmap info.
                }

                // special case: list
                if(value_type != python::Type::EMPTYLIST && value_type.isListType()) {
                    // embed type -> no size field.
                    field_idx = current_idx++;
                    indices[access_path] = std::make_tuple(bitmap_idx, maybe_idx, field_idx, -1);
                    continue;
                }

                // check what kind of value_type is
                if(!noNeedToSerializeType(value_type)) {
                    // need to serialize, so check
                    if(value_type.isFixedSizeType()) {
                        // only value field necessary.
                        field_idx = current_idx++;
                    } else {
                        // both value and size necessary.
                        field_idx = current_idx++;
                        size_idx = current_idx++;
                    }
                }

                indices[access_path] = std::make_tuple(bitmap_idx, maybe_idx, field_idx, size_idx);
            }

            return indices;
        }

        // load entries to structure
        SerializableValue struct_dict_load_from_values(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const python::Type& dict_type, flattened_struct_dict_decoded_entry_list_t entries, llvm::Value* ptr) {
            using namespace llvm;

            auto& ctx = env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            // get the corresponding type
            auto stype = create_structured_dict_type(env, "dict_struct", dict_type);
            assert(ptr);

            // get indices for faster storage access (note: not all entries need to be present!)
            auto indices = struct_dict_load_indices(dict_type);

            std::vector<std::pair<int, llvm::Value*>> bitmap_entries;
            std::vector<std::pair<int, llvm::Value*>> presence_entries;

            size_t num_bitmap = 0, num_presence_map = 0;
            flattened_struct_dict_entry_list_t type_entries;
            flatten_recursive_helper(type_entries, dict_type);
            retrieve_bitmap_counts(type_entries, num_bitmap, num_presence_map);
            bool has_bitmap = num_bitmap > 0;
            bool has_presence_map = num_presence_map > 0;

            // go over entries and generate code to load them!
            for(const auto& entry : entries) {
                // each item should be access_path | value_type | alwaysPresent |  value : SerializableValue | present : i1
                access_path_t access_path;
                python::Type value_type;
                bool always_present;
                SerializableValue el;
                llvm::Value* present = nullptr;
                std::tie(access_path, value_type, always_present, el, present) = entry;


                // fetch indices
                // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
                int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
                std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(access_path);

                // special case: list not supported yet, skip entries
                if(value_type.isListType()) {
                    field_idx = -1;
                    size_idx = -1;
                }

                // is it an always present element?
                // => yes! then load it directly to the type.

                llvm::BasicBlock* bbPresenceDone = nullptr, *bbPresenceStore = nullptr;
                llvm::BasicBlock* bbStoreDone = nullptr, *bbStoreValue = nullptr;

                // presence check! store only if present
                if(present_idx >= 0) {
                    assert(present);
                    assert(!always_present);

                    // create blocks
                    bbPresenceDone = BasicBlock::Create(ctx, "present_check_done", F);
                    bbPresenceStore = BasicBlock::Create(ctx, "present_store", F);
                    builder.CreateCondBr(present, bbPresenceDone, bbPresenceStore);
                    builder.SetInsertPoint(bbPresenceStore);
                }

                // bitmap check! store only if NOT null...
                if(bitmap_idx >= 0) {
                    assert(el.is_null);
                    // create blocks
                    bbStoreDone = BasicBlock::Create(ctx, "store_done", F);
                    bbStoreValue = BasicBlock::Create(ctx, "store", F);
                    builder.CreateCondBr(el.is_null, bbStoreDone, bbStoreValue);
                    builder.SetInsertPoint(bbStoreValue);
                }

                // some checks
                if(field_idx >= 0) {
                    assert(el.val);
                    auto llvm_idx = CreateStructGEP(builder, ptr, field_idx);
                    builder.CreateStore(el.val, llvm_idx);
                }

                if(size_idx >= 0) {
                    assert(el.size);
                    auto llvm_idx = CreateStructGEP(builder, ptr, size_idx);
                    builder.CreateStore(el.size, llvm_idx);
                }

                if(bitmap_idx >= 0) {
                    builder.CreateBr(bbStoreDone);
                    builder.SetInsertPoint(bbStoreDone);
                    bitmap_entries.push_back(std::make_pair(bitmap_idx, el.is_null));
                }

                if(present_idx >= 0) {
                    builder.CreateBr(bbPresenceDone);
                    builder.SetInsertPoint(bbPresenceDone);
                    presence_entries.push_back(std::make_pair(bitmap_idx, present));
                }
            }

            // create bitmaps and store them away...
            // auto bitmap = create_bitmap(env, builder, bitmap_entries);
            // auto presence_map = create_bitmap(env, builder, presence_entries);

            //  // // 64 bit bitmap logic
            //                // // extract bit (pos)
            //                // auto structBitmapIdx = builder.CreateStructGEP(tuplePtr, 0); // bitmap comes first!
            //                // auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0, bitmapPos / 64);
            //
            //                // i1 array logic
            //                // auto structBitmapIdx = builder.CreateStructGEP(tuplePtr, 0); // bitmap comes first!
            //                auto structBitmapIdx = CreateStructGEP(builder, tuplePtr, 0ull); // bitmap comes first!
            //                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
            //                builder.CreateStore(value.is_null, bitmapIdx);

            // first comes bitmap, then presence map
            if(has_bitmap) {
                for(unsigned i = 0; i < bitmap_entries.size(); ++i) {
                    auto bitmapPos = bitmap_entries[i].first;
                    auto structBitmapIdx = CreateStructGEP(builder, ptr, 0ull); // bitmap comes first!
                    auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                    builder.CreateStore(bitmap_entries[i].second, bitmapIdx);
                }
            }

            if(has_bitmap) {
                for(unsigned i = 0; i < presence_entries.size(); ++i) {
                    auto bitmapPos = presence_entries[i].first;
                    auto structBitmapIdx = CreateStructGEP(builder, ptr, 1ull); // bitmap comes first!
                    auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                    builder.CreateStore(presence_entries[i].second, bitmapIdx);
                }
            }

            return SerializableValue(ptr, nullptr, nullptr);
        }

        std::string struct_dict_lookup_llvm(LLVMEnvironment& env, llvm::Type *stype, int i) {
            if(i < 0 || i > stype->getStructNumElements())
                return "[invalid index]";
            return "[" + env.getLLVMTypeName(stype->getStructElementType(i)) + "]";
        }

        void struct_dict_verify_storage(LLVMEnvironment& env, const python::Type& dict_type, std::ostream& os) {
            auto stype = create_structured_dict_type(env, "struct_dict", dict_type);
            auto indices = struct_dict_load_indices(dict_type);
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            for(const auto& entry : entries) {
                access_path_t access_path = std::get<0>(entry);
                python::Type value_type = std::get<1>(entry);
                bool always_present = std::get<2>(entry);
                auto key = json_access_path_to_string(access_path, value_type, always_present);

                // fetch indices
                // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
                int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
                std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(access_path);

                // generate new line
                std::stringstream ss;
                ss<<key<<" :: ";
                if(bitmap_idx >= 0)
                    ss<<" bitmap: "<<bitmap_idx;
                if(present_idx >= 0)
                    ss<<" presence: "<<present_idx;
                if(field_idx >= 0)
                    ss<<" value: "<<field_idx<<" "<<struct_dict_lookup_llvm(env, stype, field_idx);
                if(size_idx >= 0)
                    ss<<" size: "<<size_idx<<" "<<struct_dict_lookup_llvm(env, stype, size_idx);
                os<<ss.str()<<std::endl;

            }

        }


        // helper function re type
        int bitmap_field_idx(const python::Type& dict_type) {
            // if has bitmap then
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);
            for(auto& entry : entries)
                if(std::get<1>(entry).isOptionType())
                    return 0;
            return -1;
        }

        int presence_map_field_idx(const python::Type& dict_type) {
            // if has bitmap then
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            bool has_bitmap = false;
            bool has_presence = false;
            for(auto& entry : entries) {
                if(std::get<1>(entry).isOptionType())
                   has_bitmap = true;
                if(!std::get<2>(entry))
                    has_presence = true;
            }
            if(has_bitmap && has_presence)
                return 1;
            if(has_presence)
                return 0;
            return -1;
        }

        // --- store functions ---
        void struct_dict_store_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_present) {
           // return;

            auto indices = struct_dict_load_indices(dict_type);
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            // store only if valid present_idx
            if(present_idx >= 0) {
                // env.printValue(builder, is_present, "storing away is_present at index " + std::to_string(present_idx));

                // make sure type has presence map index
                auto p_idx = presence_map_field_idx(dict_type);
                assert(p_idx >= 0);
                assert(is_present && is_present->getType() == env.i1Type());
                // i1 store logic
                auto bitmapPos = present_idx;
                auto structBitmapIdx = CreateStructGEP(builder, ptr, (size_t)p_idx); // bitmap comes first!
                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                builder.CreateStore(is_present, bitmapIdx);
            }
        }

        void struct_dict_store_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* value) {
            auto indices = struct_dict_load_indices(dict_type);
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            // store only if valid present_idx
            if(field_idx >= 0) {
                // env.printValue(builder, value, "storing away value at index " + std::to_string(field_idx));

                // store
                auto llvm_idx = CreateStructGEP(builder, ptr, field_idx);
                builder.CreateStore(value, llvm_idx);
            }
        }

        void struct_dict_store_isnull(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_null) {
            auto indices = struct_dict_load_indices(dict_type);
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            // store only if valid present_idx
            if(bitmap_idx >= 0) {
                // env.printValue(builder, is_null, "storing away is_null at index " + std::to_string(bitmap_idx));

                // make sure type has presence map index
                auto b_idx = bitmap_field_idx(dict_type);
                assert(b_idx >= 0);
                assert(is_null && is_null->getType() == env.i1Type());
                // i1 store logic
                auto bitmapPos = bitmap_idx;
                auto structBitmapIdx = CreateStructGEP(builder, ptr, (size_t)b_idx); // bitmap comes first!
                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                builder.CreateStore(is_null, bitmapIdx);
            }
        }

        void struct_dict_store_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* size) {
            auto indices = struct_dict_load_indices(dict_type);

            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            // store only if valid present_idx
            if(size_idx >= 0) {
                // env.printValue(builder, size, "storing away size at index " + std::to_string(size_idx));

                // store
                auto llvm_idx = CreateStructGEP(builder, ptr, size_idx);
                builder.CreateStore(size, llvm_idx);
            }
        }

        size_t struct_dict_bitmap_size_in_bytes(const python::Type& dict_type) {
            size_t num_bitmap = 0, num_presence_map = 0;
            flattened_struct_dict_entry_list_t type_entries;
            flatten_recursive_helper(type_entries, dict_type);
            retrieve_bitmap_counts(type_entries, num_bitmap, num_presence_map);

            return sizeof(int64_t) * num_bitmap + sizeof(int64_t) * num_presence_map;
        }

        SerializableValue struct_dict_type_serialized_memory_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type) {
            // get the corresponding type
            auto stype = create_structured_dict_type(env, "dict_struct", dict_type);

            if(ptr->getType() != stype->getPointerTo())
                throw std::runtime_error("ptr has not correct type, must be pointer to " + stype->getStructName().str());

            // get flattened structure!
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            auto indices = struct_dict_load_indices(dict_type);

            // check bitmap size (i.e. multiples of 64bit)
            auto bitmap_size = struct_dict_bitmap_size_in_bytes(dict_type);

            llvm::Value* size = env.i64Const(bitmap_size);

            auto bytes8 = env.i64Const(sizeof(int64_t));

            // get indices to properly decode
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto value_type = std::get<1>(entry);
                bool always_present = std::get<2>(entry);
                auto t_indices = indices.at(access_path);

                // special case list: --> needs extra care
                if(value_type.isOptionType())
                    value_type = value_type.getReturnType();

                // skip nested struct dicts!
                if(value_type.isStructuredDictionaryType())
                    continue;

                if(value_type.isListType()) {
                    // call list specific function to determine length.
                    auto value_idx = std::get<2>(t_indices);
                    assert(value_idx >= 0);
                    auto list_ptr = CreateStructGEP(builder, ptr, value_idx);
                    auto s = list_serialized_size(env, builder, list_ptr, value_type);

                    // add 8 bytes for storing the info
                    s = builder.CreateAdd(s, env.i64Const(8));
                    size = builder.CreateAdd(size, s);
                    continue;
                }

                // depending on field, add size!
                auto value_idx = std::get<2>(t_indices);
                auto size_idx = std::get<3>(t_indices);
                // how to serialize everything?
                // -> use again the offset trick!
                // may serialize a good amount of empty fields... but so be it.
                if(value_idx >= 0) { // <-- value_idx >= 0 indicates it's a field that may/may not be serialized
                    // always add 8 bytes per field
                    size = builder.CreateAdd(size, bytes8);
                    if(size_idx >= 0) { // <-- size_idx >= 0 indicates a variable length field!
                        // add size field + data
                        auto llvm_idx = CreateStructGEP(builder, ptr, size_idx);
                        auto value_size = builder.CreateLoad(llvm_idx);
                        size = builder.CreateAdd(size, value_size);
                    }
                }

                // // debug print
                // auto path_desc = json_access_path_to_string(access_path, value_type, always_present);
                // env.printValue(builder, size, "size after serializing " + path_desc + ": ");
            }

            return SerializableValue(size, bytes8, nullptr);
        }


        llvm::Value* serializeBitmap(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* bitmap, llvm::Value* dest_ptr) {
            assert(bitmap && dest_ptr);
            assert(bitmap->getType()->isArrayTy());
            auto element_type = bitmap->getType()->getArrayElementType();
            assert(element_type == env.i1Type());
            assert(dest_ptr->getType() == env.i8ptrType());

            auto num_bitmap_bits = bitmap->getType()->getArrayNumElements();
            auto num_elements = core::ceilToMultiple(num_bitmap_bits, 64ul) / 64ul;


            // need to create a temp variable
            auto arr_ptr = env.CreateFirstBlockAlloca(builder, bitmap->getType());
//            env.lifetimeStart(builder, arr_ptr);
            builder.CreateStore(bitmap, arr_ptr);

            llvm::Value* bitmap_ptr = builder.CreateBitOrPointerCast(arr_ptr, env.i64ptrType());
            dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());

            // store now
            for(unsigned i = 0; i < num_elements; ++i) {
                auto element_value = builder.CreateLoad(bitmap_ptr);
                builder.CreateStore(element_value, dest_ptr);
                bitmap_ptr = builder.CreateGEP(bitmap_ptr, env.i64Const(1));
                dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(1));
            }

            // end lifetime of arr_ptr
//            env.lifetimeEnd(builder, arr_ptr);

            // cast back to i8 ptr
            dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i8ptrType());

            return dest_ptr;
        }

        bool struct_dict_has_bitmap(const python::Type& dict_type) {
            assert(dict_type.isStructuredDictionaryType());

            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            size_t num_bitmap = 0, num_presence_map = 0;

            retrieve_bitmap_counts(entries, num_bitmap, num_presence_map);
            bool has_bitmap = num_bitmap > 0;
            bool has_presence_map = num_presence_map > 0;
            return has_bitmap;
        }

        bool struct_dict_has_presence_map(const python::Type& dict_type) {
            assert(dict_type.isStructuredDictionaryType());

            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            size_t num_bitmap = 0, num_presence_map = 0;

            retrieve_bitmap_counts(entries, num_bitmap, num_presence_map);
            bool has_bitmap = num_bitmap > 0;
            bool has_presence_map = num_presence_map > 0;
            return has_presence_map;
        }

        void struct_dict_mem_zero(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type) {
            auto& logger = Logger::instance().logger("codegen");

            // get the corresponding type
            auto stype = create_structured_dict_type(env, "dict_struct", dict_type);

            if(ptr->getType() != stype->getPointerTo())
                throw std::runtime_error("ptr has not correct type, must be pointer to " + stype->getStructName().str());

            // get flattened structure!
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            auto indices = struct_dict_load_indices(dict_type);

            // also zero bitmaps? i.e. everything should be null and not present?

            if(struct_dict_has_bitmap(dict_type)) {
//                auto bitmap_idx = CreateStructGEP(builder, ptr, 0);
//                auto bitmap = builder.CreateLoad(bitmap_idx);
//                dest_ptr = serializeBitmap(env, builder, bitmap, dest_ptr);
            }
            // 2. presence-bitmap
            if(struct_dict_has_presence_map(dict_type)) {
//                auto presence_map_idx = CreateStructGEP(builder, ptr, 1);
//                auto presence_map = builder.CreateLoad(presence_map_idx);
//                dest_ptr = serializeBitmap(env, builder, presence_map, dest_ptr);
            }
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto t_indices = indices.at(access_path);
                auto value_idx = std::get<2>(t_indices);
                auto size_idx = std::get<3>(t_indices);
                auto value_type = std::get<1>(entry);

                if (value_type.isOptionType())
                    value_type = value_type.getReturnType();

                // skip list
                if(value_type.isListType()) {
                    // special case: use list zero function!
                    assert(value_idx >= 0);

                    auto list_ptr = CreateStructGEP(builder, ptr, value_idx);
                    list_init_empty(env, builder, list_ptr, value_type);
                    continue; // --> done, go to next one.
                }

                // skip nested struct dicts!
                if (value_type.isStructuredDictionaryType())
                    continue;

                if(size_idx >= 0) {
                    auto llvm_size_idx = CreateStructGEP(builder, ptr, size_idx);

                    assert(llvm_size_idx->getType() == env.i64ptrType());
                    // store 0!
                    builder.CreateStore(env.i64Const(0), llvm_size_idx);
                }
            }
        }

        SerializableValue struct_dict_serialize_to_memory(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, llvm::Value* dest_ptr) {
            auto& logger = Logger::instance().logger("codegen");

            llvm::Value* original_dest_ptr = dest_ptr;

            // get the corresponding type
            auto stype = create_structured_dict_type(env, "dict_struct", dict_type);

            if(ptr->getType() != stype->getPointerTo())
                throw std::runtime_error("ptr has not correct type, must be pointer to " + stype->getStructName().str());

            // get flattened structure!
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            auto indices = struct_dict_load_indices(dict_type);

            // check bitmap size (i.e. multiples of 64bit)
            auto bitmap_size = struct_dict_bitmap_size_in_bytes(dict_type);

            llvm::Value* size = env.i64Const(bitmap_size);

            auto bytes8 = env.i64Const(sizeof(int64_t));

            // start: serialize bitmaps
            // 1. null-bitmap
            if(struct_dict_has_bitmap(dict_type)) {
                auto bitmap_idx = CreateStructGEP(builder, ptr, 0);
                auto bitmap = builder.CreateLoad(bitmap_idx);
                dest_ptr = serializeBitmap(env, builder, bitmap, dest_ptr);
            }
            // 2. presence-bitmap
            if(struct_dict_has_presence_map(dict_type)) {
                auto presence_map_idx = CreateStructGEP(builder, ptr, 1);
                auto presence_map = builder.CreateLoad(presence_map_idx);
                dest_ptr = serializeBitmap(env, builder, presence_map, dest_ptr);
            }

            // count how many fields there are => important to compute offsets!
            size_t num_fields = 0;
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto t_indices = indices.at(access_path);
                auto value_idx = std::get<2>(t_indices);
                auto value_type = std::get<1>(entry);

                if(value_type.isOptionType())
                    value_type = value_type.getReturnType();
                // skip nested struct dicts!
                if(value_type.isStructuredDictionaryType())
                    continue;

                if(value_idx < 0)
                    continue; // can skip field, not necessary to serialize
                    num_fields++;
            }

            logger.debug("found " + pluralize(num_fields, "field") + " to serialize.");

            size_t field_index = 0; // used in order to compute offsets!
            llvm::Value* varLengthOffset = env.i64Const(0); // current offset from varfieldsstart ptr
            llvm::Value* varFieldsStartPtr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t) * num_fields)); // where in memory the variable field storage starts!

            // get indices to properly decode
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto value_type = std::get<1>(entry);
                auto t_indices = indices.at(access_path);

                // special case list: --> needs extra care
                if(value_type.isOptionType())
                    value_type = value_type.getReturnType();
                if(value_type.isListType()) {
                    // special case, perform it here, then skip:
                    // call list specific function to determine length.
                    auto value_idx = std::get<2>(t_indices);
                    assert(value_idx >= 0);
                    auto list_type = value_type;
                    auto list_ptr = CreateStructGEP(builder, ptr, value_idx);
                    auto list_size_in_bytes = list_serialized_size(env, builder, list_ptr, list_type);

                    // => list is ALWAYS a var length field, serialize like that.
                    // compute offset
                    // from current field -> varStart + varoffset
                    size_t cur_to_var_start_offset = (num_fields - field_index + 1) * sizeof(int64_t);
                    auto offset = builder.CreateAdd(env.i64Const(cur_to_var_start_offset), varLengthOffset);

                    auto varDest = builder.CreateGEP(varFieldsStartPtr, varLengthOffset);
                    // call list function
                    list_serialize_to(env, builder, list_ptr, list_type, varDest);

                    // pack offset and size into 64bit!
                    auto info = pack_offset_and_size(builder, offset, list_size_in_bytes);

                    // store info away
                    auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
                    builder.CreateStore(info, casted_dest_ptr);

                    dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t)));
                    varLengthOffset = builder.CreateAdd(varLengthOffset, list_size_in_bytes);
                    field_index++;
                    continue;
                }

                // skip nested struct dicts! --> they're taken care of.
                if(value_type.isStructuredDictionaryType())
                    continue;

                // depending on field, add size!
                auto value_idx = std::get<2>(t_indices);
                auto size_idx = std::get<3>(t_indices);

                if(value_idx < 0)
                    continue; // can skip field, not necessary to serialize

                // what kind of data is it that needs to be serialized?
                bool is_varlength_field = size_idx >= 0;
                assert(value_idx >= 0);

                // load value
                auto llvm_value_idx = CreateStructGEP(builder, ptr, value_idx);
                llvm::Value* value = builder.CreateLoad(llvm_value_idx);

                if(!is_varlength_field) {
                    // simple: just load data and copy!
                    // make sure it's bool/i64/64 -> these are the only fixed size fields!

                    assert(value_type == python::Type::BOOLEAN || value_type == python::Type::I64 || value_type == python::Type::F64);

                    if(value_type == python::Type::BOOLEAN)
                        value = builder.CreateZExt(value, env.i64Type());

                    // store with casting
                    auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, value->getType()->getPointerTo());
                    builder.CreateStore(value, casted_dest_ptr);
                    dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t)));
                } else {
                    // more complex:
                    // for now, only string supported... => load and fix!
                    if(value_type != python::Type::STRING)
                        throw std::runtime_error("unsupported type " + value_type.desc() + " encountered! ");

                    // add size field + data
                    auto llvm_size_idx = CreateStructGEP(builder, ptr, size_idx);
                    auto value_size = builder.CreateLoad(llvm_size_idx); // <-- serialized size.

                    // compute offset
                    // from current field -> varStart + varoffset
                    size_t cur_to_var_start_offset = (num_fields - field_index + 1) * sizeof(int64_t);
                    auto offset = builder.CreateAdd(env.i64Const(cur_to_var_start_offset), varLengthOffset);

                    auto varDest = builder.CreateGEP(varFieldsStartPtr, varLengthOffset);
                    builder.CreateMemCpy(varDest, 0, value, 0, value_size); // for string, simple value copy!

                    // pack offset and size into 64bit!
                    auto info = pack_offset_and_size(builder, offset, value_size);

                    // store info away
                    auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
                    builder.CreateStore(info, casted_dest_ptr);

                    dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t)));

                    varLengthOffset = builder.CreateAdd(varLengthOffset, value_size);
                }

                // serialized field -> inc index!
                field_index++;
            }

            // move dest ptr to end!
            dest_ptr = builder.CreateGEP(dest_ptr, varLengthOffset);


            llvm::Value* serialized_size = builder.CreatePtrDiff(dest_ptr, original_dest_ptr);
            return SerializableValue(original_dest_ptr, serialized_size, nullptr);
        }

        SerializableValue struct_dict_type_to_memory(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* value, const python::Type& dict_type);



        SerializableValue struct_dict_get_item(LLVMEnvironment &env, llvm::Value *obj, const python::Type &dict_type,
                                               const SerializableValue &key, const python::Type &key_type);

        void struct_dict_set_item(LLVMEnvironment &env, llvm::Value *obj, const python::Type &dict_type,
                                  const SerializableValue &key, const python::Type &key_type);


        std::vector<python::StructEntry>::iterator
        find_by_key(const python::Type &dict_type, const std::string &key_value, const python::Type &key_type) {
            // perform value compare of key depending on key_type
            auto kv_pairs = dict_type.get_struct_pairs();
            return std::find_if(kv_pairs.begin(), kv_pairs.end(), [&](const python::StructEntry &entry) {
                auto k_type = deoptimizedType(key_type);
                auto e_type = deoptimizedType(entry.keyType);
                if (k_type != e_type) {
                    // special case: option types ->
                    if (k_type.isOptionType() &&
                        (python::Type::makeOptionType(e_type) == k_type || e_type == python::Type::NULLVALUE)) {
                        // ok... => decide
                        return semantic_python_value_eq(k_type, entry.key, key_value);
                    }

                    // other way round
                    if (e_type.isOptionType() &&
                        (python::Type::makeOptionType(k_type) == e_type || k_type == python::Type::NULLVALUE)) {
                        // ok... => decide
                        return semantic_python_value_eq(e_type, entry.key, key_value);
                    }

                    return false;
                } else {
                    // is key_value the same as what is stored in the entry?
                    return semantic_python_value_eq(k_type, entry.key, key_value);
                }
                return false;
            });
        }

        llvm::Value *struct_dict_contains_key(LLVMEnvironment &env, llvm::Value *obj, const python::Type &dict_type,
                                              const SerializableValue &key, const python::Type &key_type) {
            assert(dict_type.isStructuredDictionaryType());

            auto &logger = Logger::instance().logger("codegen");

            // quick check
            // is key-type at all contained?
            auto kv_pairs = dict_type.get_struct_pairs();
            auto it = std::find_if(kv_pairs.begin(), kv_pairs.end(),
                                   [&key_type](const python::StructEntry &entry) { return entry.keyType == key_type; });
            if (it == kv_pairs.end())
                return env.i1Const(false);

            // is it a constant key? => can decide during compile time as well!
            if (key_type.isConstantValued()) {
                auto it = find_by_key(dict_type, key_type.constant(), key_type.underlying());
                return env.i1Const(it != dict_type.get_struct_pairs().end());
            }

            // is the value a llvm constant? => can optimize as well!
            if (key.val && llvm::isa<llvm::Constant>(key.val)) {
                // there are only a couple cases where this works...
                // -> string, bool, i64, f64...
                // if(key_type == python::Type::STRING && llvm::isa<llvm::Constant)
                // @TODO: skip for now
                logger.debug("optimization potential here... can decide this at compile time!");
            }

            // can't decide statically, use here LLVM IR code to decide whether the struct type contains the key or not!
            // => i.e. want to do semantic comparison.
            // only need to compare against all keys with key_type (or that are compatible to it (e.g. options))
            std::vector<python::StructEntry> pairs_to_compare_against;
            for (auto kv_pair: kv_pairs) {

            }

            return nullptr;
        }

    }
}

namespace tuplex {
    void create_dummy_function(codegen::LLVMEnvironment &env, llvm::Type *stype) {
        using namespace llvm;
        assert(stype);
        assert(stype->isStructTy());

        auto FT = FunctionType::get(env.i64Type(), {env.i64Type()}, false);
        auto F = Function::Create(FT, llvm::GlobalValue::ExternalLinkage, "dummy", *env.getModule().get());

        auto bb = BasicBlock::Create(env.getContext(), "entry", F);
        IRBuilder<> b(bb);
        b.CreateAlloca(stype);
        b.CreateRet(env.i64Const(0));

    }
}

// let's start with some simple tests for a basic dict struct type
namespace tuplex {
    Field get_representative_value(const python::Type &type) {
        using namespace tuplex;
        std::unordered_map<python::Type, Field> m{{python::Type::BOOLEAN,    Field(false)},
                                                  {python::Type::I64,        Field((int64_t) 42)},
                                                  {python::Type::F64,        Field(5.3)},
                                                  {python::Type::STRING,     Field("hello world!")},
                                                  {python::Type::NULLVALUE,  Field::null()},
                                                  {python::Type::EMPTYTUPLE, Field::empty_tuple()},
                                                  {python::Type::EMPTYLIST,  Field::empty_list()},
                                                  {python::Type::EMPTYDICT,  Field::empty_dict()}};

        if (type.isOptionType()) {
            // randomize:
            if (rand() % 1000 > 500)
                return Field::null();
            else
                return m.at(type.getReturnType());
        }

        return m.at(type);
    }
}

TEST_F(HyperTest, StructLLVMTypeContains) {
    using namespace tuplex;
    using namespace std;

    // create a struct type with everything in it.
    auto types = python::primitiveTypes(true);

    cout << "got " << pluralize(types.size(), "primitive type") << endl;

    // create a big struct type!
    std::vector<python::StructEntry> pairs;
    for (auto kt: types)
        for (auto vt: types) {
            auto key = escape_to_python_str(kt.desc());
            python::StructEntry entry;
            entry.key = key;
            entry.keyType = kt;
            entry.valueType = vt;
            pairs.push_back(entry);
        }

    // key type and value have to be unique!
    // -> i.e. remove any duplicates...

    auto stype = python::Type::makeStructuredDictType(pairs);

    cout << "created type: " << prettyPrintStructType(stype) << endl;
    cout << "type: " << stype.desc() << endl;
}


#include "DebugTypeFinder.h"

// test to generate a struct type
TEST_F(HyperTest, StructLLVMType) {
    using namespace tuplex;
    using namespace std;

    string sample_path = "/Users/leonhards/Downloads/github_sample";
    string sample_file = sample_path + "/2011-11-26-13.json.gz";

    auto path = sample_file;

    Logger::init();


    path = "../resources/2011-11-26-13.json.gz";

    // // smaller sample
    // path = "../resources/2011-11-26-13.sample.json";

    //   // payload removed, b.c. it's so hard to debug... // there should be one org => one exception row.
    //  path = "../resources/2011-11-26-13.sample2.json"; // -> so this works.

    //  path = "../resources/2011-11-26-13.sample3.json"; // -> single row, the parse should trivially work.

    // tiny json example to simplify things
    path = "../resources/ndjson/example1.json";


    auto raw_data = fileToString(path);

    const char *pointer = raw_data.data();
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

    // general case version
    auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
    conf_general_case_type_policy.unifyMissingDictKeys = true;
    conf_general_case_type_policy.allowUnifyWithPyObject = true;

    double conf_nc_threshold = 0.;
    // type cover maximization
    std::vector<std::pair<python::Type, size_t>> type_counts;
    for (unsigned i = 0; i < rows.size(); ++i) {
        // row check:
        //std::cout<<"row: "<<rows[i].toPythonString()<<" type: "<<rows[i].getRowType().desc()<<std::endl;
        type_counts.emplace_back(std::make_pair(rows[i].getRowType(), 1));
    }

    auto general_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
    auto normal_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true,
                                                  TypeUnificationPolicy::defaultPolicy());

    auto normal_case_type = normal_case_max_type.first.parameters().front();
    auto general_case_type = general_case_max_type.first.parameters().front();
    std::cout << "normal  case:  " << normal_case_type.desc() << std::endl;
    std::cout << "general case:  " << general_case_type.desc() << std::endl;

    auto row_type = normal_case_type;//general_case_type;
    row_type = general_case_type; // <-- this should match MOST of the rows...

    // codegen now here...
    codegen::LLVMEnvironment env;

    auto stype = codegen::create_structured_dict_type(env, "struct_dict", row_type);
    // create new func with this
    create_dummy_function(env, stype);

//    std::string err;
//    EXPECT_TRUE(codegen::verifyModule(*env.getModule(), &err));
//    std::cerr<<err<<std::endl;

    // TypeFinderDebug.run(*env.getModule());

    std::cout << "running manual typefinder" << std::endl;
    llvm::DebugTypeFinder tf;
    tf.run(*env.getModule(), true);

    std::cout << "running typefinder" << std::endl;
    //env.getModule()->dump();
    llvm::TypeFinder type_finder;
    type_finder.run(*env.getModule(), true);
    for (auto t: type_finder) {
        std::cout << t->getName().str() << std::endl;
    }

    std::cout << "type finder done, dumping module" << std::endl;
    // bitcode --> also fails.
    // codegen::moduleToBitCodeString(*env.getModule());

    auto ir_code = codegen::moduleToString(*env.getModule());
    std::cout << "generated code:\n" << core::withLineNumbers(ir_code) << std::endl;
}


// notes: type of line can be

TEST_F(HyperTest, BasicStructLoad) {
    using namespace tuplex;
    using namespace std;

    string sample_path = "/Users/leonhards/Downloads/github_sample";
    string sample_file = sample_path + "/2011-11-26-13.json.gz";

    auto path = sample_file;

    //


    path = "../resources/2011-11-26-13.json.gz";

    // // smaller sample
    // path = "../resources/2011-11-26-13.sample.json";

    //   // payload removed, b.c. it's so hard to debug... // there should be one org => one exception row.
    //  path = "../resources/2011-11-26-13.sample2.json"; // -> so this works.

    // path = "../resources/2011-11-26-13.sample3.json"; // -> single row, the parse should trivially work.


    // // tiny json example to simplify things
     // path = "../resources/ndjson/example1.json";


//     // mini example in order to analyze code
//     path = "test.json";
//     auto content = "{\"column1\": {\"a\": \"hello\", \"b\": 20, \"c\": 30}}\n"
//                    "{\"column1\": {\"a\": \"test\", \"b\": 20, \"c\": null}}\n"
//                    "{\"column1\": {\"a\": \"cat\",  \"c\": null}}";
//     stringToFile(path, content);

     // mini example in order to analyze code
     path = "test.json";

     // this here is a simple example of a list decode
     auto content = "{\"column1\": {\"a\": [1, 2, 3, 4]}}\n"
                    "{\"column1\": {\"a\": [1, 4]}}\n"
                    "{\"column1\": {\"a\": []}}";
     stringToFile(path, content);

     // steps: 1.) integer list decode
     //        2.) struct dict list decode (this is MORE involved)


    // now, regular routine...
    auto raw_data = fileToString(path);

    const char *pointer = raw_data.data();
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

    // general case version
    auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
    conf_general_case_type_policy.unifyMissingDictKeys = true;
    conf_general_case_type_policy.allowUnifyWithPyObject = true;

    double conf_nc_threshold = 0.;
    // type cover maximization
    std::vector<std::pair<python::Type, size_t>> type_counts;
    for (unsigned i = 0; i < rows.size(); ++i) {
        // row check:
        //std::cout<<"row: "<<rows[i].toPythonString()<<" type: "<<rows[i].getRowType().desc()<<std::endl;
        type_counts.emplace_back(std::make_pair(rows[i].getRowType(), 1));
    }

    auto general_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
    auto normal_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true,
                                                  TypeUnificationPolicy::defaultPolicy());

    auto normal_case_type = normal_case_max_type.first.parameters().front();
    auto general_case_type = general_case_max_type.first.parameters().front();
    std::cout << "normal  case:  " << normal_case_type.desc() << std::endl;
    std::cout << "general case:  " << general_case_type.desc() << std::endl;

    // modify here which type to use for the parsing...
    auto row_type = normal_case_type;//general_case_type;
    row_type = general_case_type; // <-- this should match MOST of the rows...
    // row_type = normal_case_type;

    // could do here a counter experiment: I.e., how many general case rows? how many normal case rows? how many fallback rows?
    // => then also measure how much memory is required!
    // => can perform example experiments for the 10 different files and plot it out.


    // codegen here
    codegen::LLVMEnvironment env;
    auto parseFuncName = "parseJSONCodegen";

    // verify storage architecture/layout
    codegen::struct_dict_verify_storage(env, row_type, std::cout);

    codegen::JSONSourceTaskBuilder jtb(env, row_type, parseFuncName);
    jtb.build();
    auto ir_code = codegen::moduleToString(*env.getModule());
    std::cout << "generated code:\n" << core::withLineNumbers(ir_code) << std::endl;

    // load runtime lib
    runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

    // init JITCompiler
    JITCompiler jit;
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
    jit.registerSymbol("rtmalloc", runtime::rtmalloc);

    // compile func
    auto rc_compile = jit.compile(ir_code);
    ASSERT_TRUE(rc_compile);

    // get func
    auto func = reinterpret_cast<int64_t(*)(const char *, size_t)>(jit.getAddrOfSymbol(parseFuncName));

    // runtime init
    ContextOptions co = ContextOptions::defaults();
    runtime::init(co.RUNTIME_LIBRARY(false).toPath());

    // call code generated function!
    Timer timer;
    auto rc = func(buf, buf_size);
    std::cout << "parsed rows in " << timer.time() << " seconds, (" << sizeToMemString(buf_size) << ")" << std::endl;
    std::cout << "done" << std::endl;
}

TEST_F(HyperTest, CParse) {
    using namespace tuplex;
    using namespace std;

    string sample_path = "/Users/leonhards/Downloads/github_sample";
    string sample_file = sample_path + "/2011-11-26-13.json.gz";

    auto path = sample_file;

    path = "../resources/2011-11-26-13.json.gz";

    auto raw_data = fileToString(path);

    const char *pointer = raw_data.data();
    std::size_t size = raw_data.size();

    // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
    std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;


    // parse code starts here...
    auto buf = decompressed_data.data();
    auto buf_size = decompressed_data.size();



    // C-version of parsing
    uint64_t row_number = 0;

    auto j = JsonParser_init();
    if (!j)
        throw std::runtime_error("failed to initialize parser");
    JsonParser_open(j, buf, buf_size);
    while (JsonParser_hasNextRow(j)) {
        if (JsonParser_getDocType(j) != JsonParser_objectDocType()) {
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
        if (rc != 0)
            break; // --> don't forget to release stuff here!
        char *type_str = nullptr;
        rc = JsonItem_getString(obj, "type", &type_str);
        if (rc != 0)
            continue; // --> don't forget to release stuff here
        JsonItem *sub_obj = nullptr;
        rc = JsonItem_getObject(obj, "repo", &sub_obj);
        if (rc != 0)
            continue; // --> don't forget to release stuff here!

        // check wroong type
        int64_t val_i = 0;
        rc = JsonItem_getInt(obj, "repo", &val_i);
        EXPECT_EQ(rc, ecToI64(ExceptionCode::TYPEERROR));
        if (rc != 0) {
            row_number++;
            JsonParser_moveToNextRow(j);
            continue; // --> next
        }

        char *url_str = nullptr;
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

    std::cout << "Parsed " << pluralize(row_number, "row") << std::endl;
}