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
        // delete i; //--> bad: error here!
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

    uint64_t JsonItem_getStringAndSize(JsonItem *item, const char* key, char **out, int64_t *size) {
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
        *size = sv_value.size() + 1;
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
        // ONLY allocate if ok. else, leave how it is.
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

    // returns 0 if it is null!
    uint64_t JsonItem_IsNull(JsonItem *item, const char* key) {
        assert(item);
        simdjson::error_code error;
        error = item->o[key].error();
        if(error == simdjson::NO_SUCH_FIELD)
            return ecToI64(ExceptionCode::KEYERROR);
        return item->o[key].is_null() ? ecToI64(ExceptionCode::SUCCESS) : ecToI64(ExceptionCode::TYPEERROR);
    }

    bool JsonItem_hasKey(JsonItem *item, const char* key) {
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
    std::string makeKeySetBuffer(const std::vector<std::string>& keys) {
        size_t total_size = sizeof(uint64_t) + sizeof(uint32_t) * keys.size();
        for(const auto& key : keys)
            total_size += key.size() + 1;
        std::string buf(total_size, '\0');

        // internal format is basically
        // | count (i64) | str_size (i32) | str_content ...| str_size(i32) | str_content ...| ... |

        // write to buffer
        auto ptr = (uint8_t*)&buf[0];
        *(uint64_t*)ptr = keys.size(); // maybe save size as well?
        ptr += sizeof(int64_t);
        for(const auto& key : keys) {
            *(uint32_t*)ptr = key.size() + 1;
            ptr += sizeof(uint32_t);
            memcpy(ptr, key.data(), key.size() + 1);
            ptr += key.size() + 1;
        }

        return buf;
    }

    inline std::string view_to_string(const std::string_view& v) {
        return std::string{v.begin(), v.end()};
    }

    // use a helper function for this and specially encoded buffers
    uint64_t JsonItem_keySetMatch(JsonItem *item, uint8_t* always_keys_buf, uint8_t* maybe_keys_buf) {

        assert(item);
        assert(always_keys_buf);
        assert(maybe_keys_buf);

        // check always_keys_buf
        // => they all need to be there!
        uint64_t num_always_keys = *(uint64_t*)always_keys_buf;
        uint64_t num_maybe_keys = *(uint64_t*)maybe_keys_buf;

        // fetch all keys and check then off.
        size_t num_fields = 0;
        // note: looking up string views does work for C++20+
        std::unordered_map<std::string, unsigned> lookup;
        for(auto field : item->o) {
            auto key = field.unescaped_key().take_value();
            lookup[view_to_string(key)] = num_fields++;
        }

        // quick check
        if(num_fields < num_always_keys)
            return ecToI64(ExceptionCode::TYPEERROR); // not enough fields

        std::vector<bool> field_seen(num_fields, false);

        // go through the two buffers and mark whatever has been seen
        auto ptr = always_keys_buf + sizeof(int64_t);
        unsigned num_always_fields_seen = 0;
        for(unsigned i = 0; i < num_always_keys; ++i) {
            auto str_size = *(uint32_t*)ptr;
            ptr += sizeof(uint32_t);
            std::string key = (char*)ptr;
            if(lookup.end() != lookup.find(key)) {
                field_seen[lookup[key]] = true; // mark as seen
                num_always_fields_seen++; // must be there, i.e. count
            }
            ptr += str_size;
        }

        // check always fields quick check
        if(num_always_fields_seen != num_always_keys)
             return ecToI64(ExceptionCode::TYPEERROR); // not all always fields are there

         // another shortcut: if number of keys is num_always_keys, it's ok - all keys have been seen
         if(num_always_keys == num_fields)
             return ecToI64(ExceptionCode::SUCCESS);

         // are there maybe fields?
         if(num_maybe_keys > 0) {
             // expensive check.

             // go through the two buffers and mark whatever has been seen
             ptr = maybe_keys_buf + sizeof(int64_t);
             for(unsigned i = 0; i < num_maybe_keys; ++i) {
                 auto str_size = *(uint32_t*)ptr;
                 ptr += sizeof(uint32_t);
                 std::string key = (char*)ptr;
                 if(lookup.end() != lookup.find(key))
                     field_seen[lookup[key]] = true; // mark as seen
                 ptr += str_size;
             }

             // now go through bool array. if there is a single false => failure!
             auto num_seen = 0; // usually faster to sum everythin up...
            for(auto seen : field_seen) {
               num_seen += seen;
            }
            if(num_seen != field_seen.size())
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

        inline llvm::Constant* cbool_const(llvm::LLVMContext& ctx, bool b) {
            auto type = ctypeToLLVM<bool>(ctx);
            return llvm::ConstantInt::get(llvm::Type::getIntNTy(ctx, type->getIntegerBitWidth()), b);
        }

        class JSONSourceTaskBuilder {
        public:
            JSONSourceTaskBuilder(LLVMEnvironment& env,
                                  const python::Type& rowType,
                                  const std::string& functionName="parseJSON", bool unwrap_first_level=true) : _env(env), _rowType(rowType),
            _functionName(functionName), _unwrap_first_level(unwrap_first_level), _rowNumberVar(nullptr), _badParseCountVar(nullptr), _freeStart(nullptr), _freeEnd(_freeStart) {}

            void build();
        private:
            LLVMEnvironment& _env;
            python::Type _rowType;
            std::string _functionName;
            bool _unwrap_first_level;

            // helper values
            llvm::Value* _rowNumberVar;
            llvm::Value* _badParseCountVar; // stores count of bad parse emits.

            // blocks to hold start/end of frees --> called before going to next row.
            llvm::BasicBlock* _freeStart;
            llvm::BasicBlock* _freeEnd;


            // helper functions

            void generateParseLoop(llvm::IRBuilder<> &builder, llvm::Value* bufPtr, llvm::Value* bufSize);

            llvm::Value* initJsonParser(llvm::IRBuilder<>& builder);
            void freeJsonParse(llvm::IRBuilder<>& builder, llvm::Value* j);

            llvm::Value* openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value* buf, llvm::Value* buf_size);

            void exitMainFunctionWithError(llvm::IRBuilder<>& builder, llvm::Value* exitCondition, llvm::Value* exitCode);

            llvm::Value* hasNextRow(llvm::IRBuilder<>& builder, llvm::Value* j);

            void moveToNextRow(llvm::IRBuilder<>& builder, llvm::Value* j);


            llvm::BasicBlock* emitBadParseInputAndMoveToNextRow(llvm::IRBuilder<>& builder, llvm::Value* j, llvm::Value* condition);

            inline llvm::Value* rowNumber(llvm::IRBuilder<>& builder) {
                assert(_rowNumberVar);
                assert(_rowNumberVar->getType() == _env.i64ptrType());
                return builder.CreateLoad(_rowNumberVar);
            }

            llvm::Value* isDocumentOfObjectType(llvm::IRBuilder<>& builder, llvm::Value* j);

            void parseAndPrintStructuredDictFromObject(llvm::IRBuilder<>& builder, llvm::Value* j, llvm::BasicBlock* bbSchemaMismatch);

            void freeObject(llvm::IRBuilder<>& builder, llvm::Value* obj);
            void freeObject(llvm::Value* obj);

            llvm::Value* numberOfKeysInObject(llvm::IRBuilder<>& builder, llvm::Value* j);

            /*!
             *
             * @param builder
             * @param obj
             * @param t
             * @param check_that_all_keys_are_present if true, then row must contain exact keys for struct dict. Else, it's parsed whatever is specified in the schema.
             * @param bbSchemaMismatch
             */
            void parseAndPrint(llvm::IRBuilder<>& builder, llvm::Value* obj, const std::string& debug_path, bool alwaysPresent, const python::Type& t, bool check_that_all_keys_are_present, llvm::BasicBlock* bbSchemaMismatch);

            llvm::Value* decodeFieldFromObject(llvm::IRBuilder<>& builder,
                                               llvm::Value* obj,
                                               const std::string& debug_path,
                                               SerializableValue* out,
                                               bool alwaysPresent,
                                               llvm::Value* key,
                                               const python::Type& keyType,
                                               const python::Type& valueType,
                                               bool check_that_all_keys_are_present,
                                               llvm::BasicBlock* bbSchemaMismatch);
            llvm::Value* decodeFieldFromObject(llvm::IRBuilder<>& builder, llvm::Value* obj, const std::string& debug_path, SerializableValue* out, bool alwaysPresent, const std::string& key, const python::Type& keyType, const python::Type& valueType, bool check_that_all_keys_are_present, llvm::BasicBlock* bbSchemaMismatch) {
                return decodeFieldFromObject(builder, obj, debug_path, out, alwaysPresent, _env.strConst(builder, key), keyType, valueType, check_that_all_keys_are_present, bbSchemaMismatch);
            }

            void printValueInfo(llvm::IRBuilder<>& builder, const std::string& key, const python::Type& valueType, llvm::Value* keyPresent, const SerializableValue& value);
            void checkRC(llvm::IRBuilder<>& builder, const std::string& key, llvm::Value* rc);
        };

        void JSONSourceTaskBuilder::checkRC(llvm::IRBuilder<> &builder, const std::string &key, llvm::Value *rc) {
            using namespace llvm;
            auto& ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            BasicBlock* bbPrint = BasicBlock::Create(ctx, key + "_present", F);
            BasicBlock* bbNext = BasicBlock::Create(ctx, key + "_done", F);

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
                                                   const python::Type& valueType,
                                                   llvm::Value *keyPresent,
                                                   const tuplex::codegen::SerializableValue &value) {

            using namespace llvm;
            auto& ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            BasicBlock* bbPresent = BasicBlock::Create(ctx, key + "_present", F);
            BasicBlock* bbNotNull = BasicBlock::Create(ctx, key + "_notnull", F);
            BasicBlock* bbNext = BasicBlock::Create(ctx, key + "_done", F);

            builder.CreateCondBr(keyPresent, bbPresent, bbNext);

            builder.SetInsertPoint(bbPresent);
            auto is_null = value.is_null ? value.is_null : _env.i1Const(false);
            builder.CreateCondBr(is_null, bbNext, bbNotNull);

            builder.SetInsertPoint(bbNotNull);
            // if(value.val && !valueType.isStructuredDictionaryType())
            //    _env.printValue(builder, value.val, "decoded key=" + key + " as " + valueType.desc());
            builder.CreateBr(bbNext);

            builder.SetInsertPoint(bbNext);
        }


        llvm::Value *JSONSourceTaskBuilder::numberOfKeysInObject(llvm::IRBuilder<> &builder, llvm::Value *j) {
            assert(j);

            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_numberOfKeys", _env.i64Type(), _env.i8ptrType());
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

            if(keyType != python::Type::STRING)
                throw std::runtime_error("so far only string type supported for decoding");

            assert(key && out);

            auto& ctx = _env.getContext();

            SerializableValue v;
            llvm::Value* rc = nullptr;

            // special case: option
            auto v_type = valueType.isOptionType() ? valueType.getReturnType() : valueType;
            llvm::Module* mod = _env.getModule().get();

            if(v_type == python::Type::STRING) {
                // decode using string
                auto F = getOrInsertFunction(mod, "JsonItem_getStringAndSize", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0), _env.i64ptrType());
                auto str_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr(), "s");
                auto str_size_var = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "s_size");
                rc = builder.CreateCall(F, {obj, key, str_var, str_size_var});
                v.val = builder.CreateLoad(str_var);
                v.size = builder.CreateLoad(str_size_var);
                v.is_null = _env.i1Const(false);
            } else if(v_type == python::Type::BOOLEAN) {
                auto F = getOrInsertFunction(mod, "JsonItem_getBoolean", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(),
                                             ctypeToLLVM<bool>(ctx)->getPointerTo());
                auto b_var = _env.CreateFirstBlockVariable(builder, cbool_const(ctx, false));
                rc = builder.CreateCall(F, {obj, key, b_var});
                v.val = _env.upcastToBoolean(builder, builder.CreateLoad(b_var));
                v.size = _env.i64Const(sizeof(int64_t));
                v.is_null = _env.i1Const(false);
            } else if(v_type == python::Type::I64) {
                auto F = getOrInsertFunction(mod, "JsonItem_getInt", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(),
                                             _env.i64ptrType());
                auto i_var = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
                rc = builder.CreateCall(F, {obj, key, i_var});
                v.val = builder.CreateLoad(i_var);
                v.size = _env.i64Const(sizeof(int64_t));
                v.is_null = _env.i1Const(false);
            } else if(v_type == python::Type::F64) {
                auto F = getOrInsertFunction(mod, "JsonItem_getDouble", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(),
                                             _env.doublePointerType());
                auto f_var = _env.CreateFirstBlockVariable(builder, _env.f64Const(0));
                rc = builder.CreateCall(F, {obj, key, f_var});
                v.val =  builder.CreateLoad(f_var);
                v.size = _env.i64Const(sizeof(int64_t));
                v.is_null = _env.i1Const(false);
            } else if(v_type.isStructuredDictionaryType()) {
                auto F = getOrInsertFunction(mod, "JsonItem_getObject", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
                auto obj_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr());
                // create call, recurse only if ok!
                BasicBlock* bbOK = BasicBlock::Create(ctx, "is_object", builder.GetInsertBlock()->getParent());


                rc = builder.CreateCall(F, {obj, key, obj_var});
                auto is_object = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS))); // <-- indicates successful parse

                // if the object is maybe present, then key-error is not a problem.
                // correct condition therefore
                if(!alwaysPresent) {
                    BasicBlock* bbParseSub = BasicBlock::Create(ctx, "parse_object", builder.GetInsertBlock()->getParent());
                    BasicBlock* bbContinue = BasicBlock::Create(ctx, "continue_parse", builder.GetInsertBlock()->getParent());

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
            } else if(v_type.isListType()) {
                std::cerr<<"skipping for now type: "<<v_type.desc()<<std::endl;
                rc =_env.i64Const(0); // ok.
            } else if(v_type == python::Type::NULLVALUE) {
                // special case!
                auto F = getOrInsertFunction(mod, "JsonItem_IsNull", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType());
                rc = builder.CreateCall(F, {obj, key});
                v.is_null =  builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            } else if(v_type == python::Type::EMPTYDICT) {
                // special case!
                // query subobject and call count keys!
                auto F = getOrInsertFunction(mod, "JsonItem_getObject", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
                auto obj_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr());
                // create call, recurse only if ok!
                BasicBlock* bbOK = BasicBlock::Create(ctx, "is_object", builder.GetInsertBlock()->getParent());

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
            auto& ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            if(t.isStructuredDictionaryType()) {

                auto kv_pairs = t.get_struct_pairs();

                // check how many keys are contained. If all are present, quick check -> count of keys
                bool all_keys_always_present = true;
                for(auto kv_pair : kv_pairs)
                    if(!kv_pair.alwaysPresent) {
                        all_keys_always_present = false;
                        break;
                    }

                if(all_keys_always_present && check_that_all_keys_are_present) {
                    // quick key check
                    // note that the expensive check has to be only performed when maybe keys are present.
                    // else, querying each field automatically will perform a presence check.
                    BasicBlock* bbOK = BasicBlock::Create(ctx, "all_keys_present_passed", F);
                    auto num_keys = numberOfKeysInObject(builder, obj);
                    auto cond = builder.CreateICmpNE(num_keys, _env.i64Const(kv_pairs.size()));
#ifndef NDEBUG
                    {
                        // print out expected vs. found
                        BasicBlock* bb = BasicBlock::Create(ctx, "debug", F);
                        BasicBlock* bbn = BasicBlock::Create(ctx, "debug_ct", F);
                        builder.CreateCondBr(cond, bb, bbn);
                        builder.SetInsertPoint(bb);
                        // _env.printValue(builder, num_keys, "struct type expected  " + std::to_string(kv_pairs.size()) + " elements, got: ");
                        builder.CreateBr(bbn);
                        builder.SetInsertPoint(bbn);
                    }
#endif
                    builder.CreateCondBr(cond, bbSchemaMismatch, bbOK);
                    builder.SetInsertPoint(bbOK);
                } else if(check_that_all_keys_are_present) {
                    // perform check by generating appropriate constants
                    // this is the expensive key check.
                    // -> i.e. should be used to match only against general-case.
                    // generate constants
                    std::vector<std::string> alwaysKeys;
                    std::vector<std::string> maybeKeys;
                    for(const auto& kv_pair : kv_pairs) {
                        // for JSON should be always keyType == string!
                        assert(kv_pair.keyType == python::Type::STRING);
                        if(kv_pair.alwaysPresent)
                            alwaysKeys.push_back(str_value_from_python_raw_value(kv_pair.key));
                        else
                            maybeKeys.push_back(str_value_from_python_raw_value(kv_pair.key));
                    }

                    auto sconst_always_keys = _env.strConst(builder, makeKeySetBuffer(alwaysKeys));
                    auto sconst_maybe_keys = _env.strConst(builder, makeKeySetBuffer(maybeKeys));

                    // perform check using helper function on item.
                    BasicBlock* bbOK = BasicBlock::Create(ctx, "keycheck_passed", F);
                    // call uint64_t JsonItem_keySetMatch(JsonItem *item, uint8_t* always_keys_buf, uint8_t* maybe_keys_buf)
                    auto Fcheck = getOrInsertFunction(_env.getModule().get(), "JsonItem_keySetMatch", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(), _env.i8ptrType());
                    auto rc = builder.CreateCall(Fcheck, {obj, sconst_always_keys, sconst_maybe_keys});
                    auto cond = builder.CreateICmpNE(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                    builder.CreateCondBr(cond, bbSchemaMismatch, bbOK);
                    builder.SetInsertPoint(bbOK);
                }

                for(const auto& kv_pair : kv_pairs) {
                    llvm::Value *keyPresent = _env.i1Const(true); // default to always present

                    SerializableValue value;
                    auto key_value = str_value_from_python_raw_value(kv_pair.key); // it's an encoded value, but query here for the real key.
                    // _env.debugPrint(builder, "decoding now key=" + key_value + " of path " + debug_path);
                    if(key_value == "payload") {
                        std::cout<<"debug"<<std::endl;
                    }
                    auto rc = decodeFieldFromObject(builder, obj, debug_path + "." + key_value, &value, kv_pair.alwaysPresent, key_value, kv_pair.keyType, kv_pair.valueType, check_that_all_keys_are_present, bbSchemaMismatch);
                    auto successful_lookup = rc ? builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS))) : _env.i1Const(false);

                    // optional? or always there?
                    if(kv_pair.alwaysPresent) {
                        // needs to be present, i.e. key error is fatal error!
                        // --> add check, and jump to mismatch else
                        BasicBlock* bbOK = BasicBlock::Create(ctx, "key_present", builder.GetInsertBlock()->getParent());

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
                    if(rc)
                        checkRC(builder, key_value, rc);
                }


            } else {
                // other types, parse with type check!
                throw std::runtime_error("unsupported type");
            }
        }

        void JSONSourceTaskBuilder::freeObject(llvm::Value *obj) {
            assert(obj);
            using namespace llvm;
            auto& ctx = _env.getContext();

            auto ptr_to_free = obj;

            // free in free block (last block!)
            assert(_freeEnd);
            IRBuilder<> b(_freeEnd);

            // what type is it?
            if(obj->getType() == _env.i8ptrType()->getPointerTo()) {
                ptr_to_free = b.CreateLoad(obj);
            }

            freeObject(b, ptr_to_free);

            if(obj->getType() == _env.i8ptrType()->getPointerTo()) {
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
            auto& ctx = _env.getContext();

            auto Ffreeobj = getOrInsertFunction(_env.getModule().get(), "JsonItem_Free", llvm::Type::getVoidTy(ctx), _env.i8ptrType());
            builder.CreateCall(Ffreeobj, obj);
        }

        void JSONSourceTaskBuilder::parseAndPrintStructuredDictFromObject(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::BasicBlock *bbSchemaMismatch) {
            assert(j);
            using namespace llvm;
            auto& ctx = _env.getContext();

            // get initial object
            // => this is from parser
            auto Fgetobj = getOrInsertFunction(_env.getModule().get(), "JsonParser_getObject", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));

            auto obj_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr(), "row_object");
            builder.CreateCall(Fgetobj, {j, obj_var});

            // don't forget to free everything...

            // => call with row type
            parseAndPrint(builder, builder.CreateLoad(obj_var), "", true, _rowType, true, bbSchemaMismatch);

            // free obj_var...
            freeObject(builder, builder.CreateLoad(obj_var));
#ifndef NDEBUG
            builder.CreateStore(_env.i8nullptr(), obj_var);
#endif

            // build schema mismatch block.


        }

        llvm::Value *JSONSourceTaskBuilder::isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j) {
            using namespace llvm;
            auto& ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_getDocType", _env.i64Type(), _env.i8ptrType());
            auto call_res = builder.CreateCall(F, j);
            auto cond = builder.CreateICmpEQ(call_res, _env.i64Const(JsonParser_objectDocType()));
            return cond;
        }

        llvm::BasicBlock* JSONSourceTaskBuilder::emitBadParseInputAndMoveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j,
                                                                      llvm::Value *condition) {
            using namespace llvm;
            auto& ctx = _env.getContext();

            auto F = builder.GetInsertBlock()->getParent();

            BasicBlock* bbOK = BasicBlock::Create(ctx, "ok", F);
            BasicBlock* bbEmitBadParse = BasicBlock::Create(ctx, "bad_parse", F);
            builder.CreateCondBr(condition, bbEmitBadParse, bbOK);

            // ---- bad parse blocks ----
            //            auto line = JsonParser_getMallocedRow(j);
            //            free(line);
            // --> i.e. call exception handler from here...
            builder.SetInsertPoint(bbEmitBadParse);
            auto Frow = getOrInsertFunction(_env.getModule().get(), "JsonParser_getMallocedRow", _env.i8ptrType(), _env.i8ptrType());
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
            auto& ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_hasNextRow", ctypeToLLVM<bool>(ctx), _env.i8ptrType());

            auto v = builder.CreateCall(F, {j});
            return builder.CreateICmpEQ(v, llvm::ConstantInt::get(llvm::Type::getIntNTy(ctx, ctypeToLLVM<bool>(ctx)->getIntegerBitWidth()), 1));
        }

        void JSONSourceTaskBuilder::moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            // move
            using namespace llvm;
            auto& ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_moveToNextRow", ctypeToLLVM<bool>(ctx), _env.i8ptrType());
            builder.CreateCall(F, {j});

            // update row number (inc +1)
            auto row_no = rowNumber(builder);
            builder.CreateStore(builder.CreateAdd(row_no, _env.i64Const(1)), _rowNumberVar);

            // @TODO: free everything so far??
        }

        void JSONSourceTaskBuilder::exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition,
                                                              llvm::Value *exitCode) {
            using namespace llvm;
            auto& ctx = _env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            assert(exitCondition->getType() == _env.i1Type());
            assert(exitCode->getType() == _env.i64Type());

            // branch and exit
            BasicBlock* bbExit = BasicBlock::Create(ctx, "exit_with_error", F);
            BasicBlock* bbContinue = BasicBlock::Create(ctx, "no_error", F);
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

        llvm::Value* JSONSourceTaskBuilder::openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value* buf, llvm::Value* buf_size) {
            assert(j);
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_open", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(), _env.i64Type());
            return builder.CreateCall(F, {j, buf, buf_size});
        }

        void JSONSourceTaskBuilder::freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto& ctx = _env.getContext();
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonParser_Free", llvm::Type::getVoidTy(ctx), _env.i8ptrType());
            builder.CreateCall(F, j);
        }

        void JSONSourceTaskBuilder::generateParseLoop(llvm::IRBuilder<> &builder, llvm::Value* bufPtr, llvm::Value* bufSize) {
            using namespace llvm;
            auto& ctx = _env.getContext();

            // this will be a loop
            auto F = builder.GetInsertBlock()->getParent();
            BasicBlock* bLoopHeader = BasicBlock::Create(ctx, "loop_header", F);
            BasicBlock* bLoopBody = BasicBlock::Create(ctx, "loop_body", F);
            BasicBlock* bLoopExit = BasicBlock::Create(ctx, "loop_exit", F);

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
                _env.printValue(b, rowNumber(b), "entered free row objects for row no=");
            }
#endif
            llvm::Value* rc = openJsonBuf(builder, parser, bufPtr, bufSize);
            llvm::Value* rc_cond = _env.i1neg(builder,builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS))));
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

            // link back to header
            builder.CreateBr(bLoopHeader);

            // ---- post loop block ----
            // continue in loop exit.
            builder.SetInsertPoint(bLoopExit);

            // free JSON parse (global object)
            freeJsonParse(builder, parser);

            _env.printValue(builder, rowNumber(builder), "parsed rows: ");
            _env.printValue(builder, builder.CreateLoad(_badParseCountVar), "thereof bad parse rows (schema mismatch): ");
        }

        void JSONSourceTaskBuilder::build() {
            using namespace llvm;
            auto& ctx = _env.getContext();

            // create main function (takes buffer and buf_size, later take the other tuplex stuff)
            FunctionType* FT = FunctionType::get(ctypeToLLVM<int64_t>(ctx), {ctypeToLLVM<char*>(ctx), ctypeToLLVM<int64_t>(ctx)}, false);

            Function *F = Function::Create(FT, llvm::GlobalValue::ExternalLinkage, _functionName, *_env.getModule().get());
            auto m = mapLLVMFunctionArgs(F, {"buf", "buf_size"});

            auto bbEntry = BasicBlock::Create(ctx, "entry", F);
            IRBuilder<> builder(bbEntry);

            // dummy parse, simply print type and value with type checking.
            generateParseLoop(builder, m["buf"], m["buf_size"]);

            builder.CreateRet(_env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
        }


        void calculate_field_counts(const python::Type& type, size_t& field_count, size_t& option_count, size_t& maybe_count) {
            if(type.isStructuredDictionaryType()) {
                // recurse
                auto kv_pairs = type.get_struct_pairs();
                for(const auto& kv_pair : kv_pairs) {
                    maybe_count += !kv_pair.alwaysPresent;

                    // count optional key as well
                    if(kv_pair.keyType.isOptionType())
                        throw std::runtime_error("unsupported now");

                    calculate_field_counts(kv_pair.valueType, field_count, option_count, maybe_count);
                }
            } else {
                if(type.isOptionType()) {
                    option_count++;
                    calculate_field_counts(type.getReturnType(), field_count, option_count, maybe_count);
                } else {
                    // count as one field (true even for lists etc.) -> only unnest { { ...}, ... }
                    field_count++;
                }
            }
        }

        inline bool noNeedToSerializeType(const python::Type& t) {
            // some types do not need to get serialized. This function specifies this
            if(t.isConstantValued())
                return true; // no need to serialize constants!
            if(t.isSingleValued())
                return true; // no need to serialize special constant values (like null, empty dict, empty list, empty tuple, ...)
            return false;
        }

        // flatten struct dict.

        void flatten_recursive_helper(std::vector<std::pair<std::vector<std::pair<std::string, python::Type>>, python::Type>>& entries,
                                      const python::Type& dict_type, std::vector<std::pair<std::string, python::Type>> prefix={}) {
            using namespace std;

            assert(dict_type.isStructuredDictionaryType());

            for(auto kv_pair : dict_type.get_struct_pairs()) {
                vector<pair<string, python::Type>> access_path = prefix; // = prefix
                access_path.push_back(make_pair(kv_pair.key, kv_pair.keyType));

                if(kv_pair.valueType.isStructuredDictionaryType()) {
                    // recurse using new prefix
                    flatten_recursive_helper(entries, kv_pair.valueType, access_path);
                } else {
                    entries.push_back(make_pair(access_path, kv_pair.valueType));
                }
            }
        }

        void flatten_structured_dict_type(const python::Type& dict_type) {
            using namespace std;


            // each entry is {(key, key_type), ..., (key, key_type)}, value_type
            // only nested dicts are flattened. Tuples etc. are untouched. (would be too cumbersome)
            vector<std::pair<vector<std::pair<std::string, python::Type>>, python::Type>> entries;
            flatten_recursive_helper(entries, dict_type, {});

            // now print out everything...
            std::stringstream  ss;
            for(auto entry : entries) {
                // first the path:
                for(auto atom : entry.first) {
                    ss<<atom.first<<" ("<<atom.second.desc()<<") -> ";
                }
                ss<<entry.second.desc()<<endl;
            }

            cout<<ss.str()<<endl;
        }



        // creating struct type based on structured dictionary type
        llvm::Type* create_structured_dict_type(LLVMEnvironment& env, const std::string& name, const python::Type& dict_type) {
            using namespace llvm;
            auto& logger = Logger::instance().logger("codegen");
            llvm::LLVMContext& ctx = env.getContext();

            if(!dict_type.isStructuredDictionaryType()) {
                logger.error("provided type is not a structured dict type but " + dict_type.desc());
                return nullptr;
            }

            flatten_structured_dict_type(dict_type);


            // retrieve counts => i.e. how many fields are options? how many are maybe present?
            size_t field_count=0, option_count=0, maybe_count=0;
            calculate_field_counts(dict_type, field_count, option_count, maybe_count);
            std::stringstream ss;
            ss<<"computed following counts for structured dict type: "<<pluralize(field_count, "field")
            <<" "<<pluralize(option_count, "option")<<" "<<pluralize(maybe_count, "maybe");
            logger.info(ss.str());


            // let's start by allocating bitmaps for optional AND maybe types
            size_t num_option_bitmap_bits = core::ceilToMultiple(option_count, 64ul); // multiples of 64bit
            size_t num_maybe_bitmap_bits = core::ceilToMultiple(maybe_count, 64ul);
            size_t num_option_bitmap_elements = num_option_bitmap_bits / 64;
            size_t num_maybe_bitmap_elements = num_maybe_bitmap_bits / 64;


            bool is_packed = false;
            std::vector<llvm::Type*> member_types;
            auto i64Type = llvm::Type::getInt64Ty(ctx);

            // add bitmap elements
            member_types.push_back(llvm::ArrayType::get(i64Type, num_option_bitmap_elements));
            member_types.push_back(llvm::ArrayType::get(i64Type, num_maybe_bitmap_elements));

            // now add all the elements from the struct type (skip lists and other struct entries, i.e. only primitives so far)
            auto kv_pairs = dict_type.get_struct_pairs();
            std::unordered_map<std::string, size_t> key_to_offset_map;
            unsigned offset = 0;
            unsigned bitmap_idx = 0;
            for(auto kv_pair : kv_pairs) {
                // => key is always known, so it's easy to do a quick lookup!
                key_to_offset_map[kv_pair.key] = offset;
                offset++;

                auto t = kv_pair.valueType;
                if(t.isOptionType()) {
                    // bitmap index!
                    bitmap_idx++;

                    // add

                } else {
                    // directly add ?
                    // primitive field or size field as well required?
                    if(t.isSingleValued() || t.isConstantValued()) {
                        // we can skip storing/serializing this. It can be directly read!
                    }
                }

                // we do not save the key (because it's statically known), but simply lay out the data
                auto element_type = t;
                if(t.isOptionType())
                    element_type = element_type.getReturnType(); // option is handled above
                // single valued type?
                // => skip!
                if(noNeedToSerializeType(t))
                    continue;

                // serialize. Check if it is a fixed size type -> no size field required, else add an i64 field to store the var_length size!
                auto mapped_type = env.pythonToLLVMType(t);
                if(!mapped_type)
                    throw std::runtime_error("could not map type " + t.desc());
                member_types.push_back(mapped_type);
                if(!t.isFixedSizeType()) {
                    member_types.push_back(i64Type);
                }
            }

            auto stype = llvm::StructType::create(ctx, member_types, name, is_packed);
            return stype;
        }


        SerializableValue struct_dict_get_item(LLVMEnvironment& env, llvm::Value* obj, const python::Type& dict_type, const SerializableValue& key, const python::Type& key_type);

        void struct_dict_set_item(LLVMEnvironment& env, llvm::Value* obj, const python::Type& dict_type, const SerializableValue& key, const python::Type& key_type);


        std::vector<python::StructEntry>::iterator find_by_key(const python::Type& dict_type, const std::string& key_value, const python::Type& key_type) {
            // perform value compare of key depending on key_type
            auto kv_pairs = dict_type.get_struct_pairs();
            return std::find_if(kv_pairs.begin(), kv_pairs.end(), [&](const python::StructEntry& entry) {
                auto k_type = deoptimizedType(key_type);
                auto e_type = deoptimizedType(entry.keyType);
                if(k_type != e_type) {
                    // special case: option types ->
                    if(k_type.isOptionType() && (python::Type::makeOptionType(e_type) == k_type || e_type == python::Type::NULLVALUE)) {
                        // ok... => decide
                        return semantic_python_value_eq(k_type, entry.key, key_value);
                    }

                    // other way round
                    if(e_type.isOptionType() && (python::Type::makeOptionType(k_type) == e_type || k_type == python::Type::NULLVALUE)) {
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

        llvm::Value* struct_dict_contains_key(LLVMEnvironment& env, llvm::Value* obj, const python::Type& dict_type, const SerializableValue& key, const python::Type& key_type) {
            assert(dict_type.isStructuredDictionaryType());

            auto& logger = Logger::instance().logger("codegen");

            // quick check
            // is key-type at all contained?
            auto kv_pairs = dict_type.get_struct_pairs();
            auto it = std::find_if(kv_pairs.begin(), kv_pairs.end(), [&key_type](const python::StructEntry& entry) { return entry.keyType == key_type; } );
            if(it == kv_pairs.end())
                return env.i1Const(false);

            // is it a constant key? => can decide during compile time as well!
            if(key_type.isConstantValued()) {
                auto it = find_by_key(dict_type, key_type.constant(), key_type.underlying());
                return env.i1Const(it != dict_type.get_struct_pairs().end());
            }

            // is the value a llvm constant? => can optimize as well!
            if(key.val && llvm::isa<llvm::Constant>(key.val)) {
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
            for(auto kv_pair : kv_pairs) {

            }

            return nullptr;
        }

    }
}

namespace tuplex {
    void create_dummy_function(codegen::LLVMEnvironment& env, llvm::Type *stype) {
        using namespace llvm;
        assert(stype);
        assert(stype->isStructTy());

        auto FT = FunctionType::get(env.i64Type(), {stype->getPointerTo()}, false);
        auto F = Function::Create(FT, llvm::GlobalValue::ExternalLinkage, "dummy", *env.getModule().get());

        auto bb = BasicBlock::Create(env.getContext(), "entry", F);
        IRBuilder<> b(bb);
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

        if(type.isOptionType()) {
            // randomize:
            if(rand() % 1000 > 500)
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

    cout<<"got "<<pluralize(types.size(), "primitive type")<<endl;

    // create a big struct type!
    std::vector<python::StructEntry> pairs;
    for(auto kt : types)
        for(auto vt : types) {
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

    cout<<"created type: "<<prettyPrintStructType(stype)<<endl;
    cout<<"type: "<<stype.desc()<<endl;
}


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

    // general case version
    auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
    conf_general_case_type_policy.unifyMissingDictKeys = true;
    conf_general_case_type_policy.allowUnifyWithPyObject = true;

    double conf_nc_threshold = 0.;
    // type cover maximization
    std::vector<std::pair<python::Type, size_t>> type_counts;
    for(unsigned i = 0; i < rows.size(); ++i) {
        // row check:
        //std::cout<<"row: "<<rows[i].toPythonString()<<" type: "<<rows[i].getRowType().desc()<<std::endl;
        type_counts.emplace_back(std::make_pair(rows[i].getRowType(), 1));
    }

    auto general_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
    auto normal_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, TypeUnificationPolicy::defaultPolicy());

    auto normal_case_type = normal_case_max_type.first.parameters().front();
    auto general_case_type = general_case_max_type.first.parameters().front();
    std::cout<<"normal  case:  "<<normal_case_type.desc()<<std::endl;
    std::cout<<"general case:  "<<general_case_type.desc()<<std::endl;

    auto row_type = normal_case_type;//general_case_type;
    row_type = general_case_type; // <-- this should match MOST of the rows...

    // codegen now here...
    codegen::LLVMEnvironment env;

    auto stype = codegen::create_structured_dict_type(env, "struct_dict", row_type);
    // create new func with this
    create_dummy_function(env, stype);

    auto ir_code = codegen::moduleToString(*env.getModule());
    std::cout<<"generated code:\n"<<core::withLineNumbers(ir_code)<<std::endl;
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

  //  path = "../resources/2011-11-26-13.sample3.json"; // -> single row, the parse should trivially work.


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

    // general case version
    auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
    conf_general_case_type_policy.unifyMissingDictKeys = true;
    conf_general_case_type_policy.allowUnifyWithPyObject = true;

    double conf_nc_threshold = 0.;
    // type cover maximization
    std::vector<std::pair<python::Type, size_t>> type_counts;
    for(unsigned i = 0; i < rows.size(); ++i) {
        // row check:
        //std::cout<<"row: "<<rows[i].toPythonString()<<" type: "<<rows[i].getRowType().desc()<<std::endl;
        type_counts.emplace_back(std::make_pair(rows[i].getRowType(), 1));
    }

    auto general_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
    auto normal_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, TypeUnificationPolicy::defaultPolicy());

    auto normal_case_type = normal_case_max_type.first.parameters().front();
    auto general_case_type = general_case_max_type.first.parameters().front();
    std::cout<<"normal  case:  "<<normal_case_type.desc()<<std::endl;
    std::cout<<"general case:  "<<general_case_type.desc()<<std::endl;

    auto row_type = normal_case_type;//general_case_type;
     row_type = general_case_type; // <-- this should match MOST of the rows...

    for(auto kv : row_type.get_struct_pairs()) {
        if(kv.key == "'payload'") {
            std::cout<<"general case payload: "<<kv.valueType.desc()<<std::endl;

            // check pairs in payload => should be all maybe
            for(auto xy : kv.valueType.get_struct_pairs()) {
                std::cout<<xy.key<<": "<<std::boolalpha<<xy.alwaysPresent<<std::endl;
            }
        }
    }

    // @TODO: single row parse to make this work...

    // pretty print
    std::cout<<prettyPrintStructType(row_type)<<std::endl;

    // row_type = normal_case_type;

    // codegen here
    codegen::LLVMEnvironment env;
    auto parseFuncName = "parseJSONCodegen";
    codegen::JSONSourceTaskBuilder jtb(env, row_type, parseFuncName);
    jtb.build();
    auto ir_code = codegen::moduleToString(*env.getModule());
    std::cout<<"generated code:\n"<<core::withLineNumbers(ir_code)<<std::endl;

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

    // compile func
    auto rc_compile = jit.compile(ir_code);
    ASSERT_TRUE(rc_compile);

    // get func
    auto func = reinterpret_cast<int64_t(*)(const char*, size_t)>(jit.getAddrOfSymbol(parseFuncName));

    // runtime init
    ContextOptions co = ContextOptions::defaults();
    runtime::init(co.RUNTIME_LIBRARY(false).toPath());

    // call code generated function!
    Timer timer;
    auto rc = func(buf, buf_size);
    std::cout<<"parsed rows in "<<timer.time()<<" seconds, ("<<sizeToMemString(buf_size)<<")"<<std::endl;
    std::cout<<"done"<<std::endl;
}

TEST_F(HyperTest, CParse) {
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