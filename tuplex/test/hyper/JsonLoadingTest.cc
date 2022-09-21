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

    uint64_t JsonItem_numberOfKeys(JsonItem *item) {
        assert(item);
        size_t value;
        simdjson::error_code error;
        item->o.count_fields().tie(value, error);
        assert(!error);
        return value;
    }

}


namespace tuplex {
    namespace codegen {

        inline llvm::Constant* cbool_const(llvm::LLVMContext& ctx, bool b) {
            auto type = ctypeToLLVM<bool>(ctx);
            return llvm::ConstantInt::get(llvm::Type::getIntNTy(ctx, type->getIntegerBitWidth()), b);
        }

        class JSONSourceTaskBuilder {
        public:
            JSONSourceTaskBuilder(LLVMEnvironment& env, const python::Type& rowType, const std::string& functionName="parseJSON", bool unwrap_first_level=true) : _env(env), _rowType(rowType), _functionName(functionName), _unwrap_first_level(unwrap_first_level), _rowNumberVar(nullptr) {}

            void build();
        private:
            LLVMEnvironment& _env;
            python::Type _rowType;
            std::string _functionName;
            bool _unwrap_first_level;

            // helper values
            llvm::Value* _rowNumberVar;


            void generateParseLoop(llvm::IRBuilder<> &builder, llvm::Value* bufPtr, llvm::Value* bufSize);

            llvm::Value* initJsonParser(llvm::IRBuilder<>& builder);
            void freeJsonParse(llvm::IRBuilder<>& builder, llvm::Value* j);

            llvm::Value* openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value* buf, llvm::Value* buf_size);

            void exitMainFunctionWithError(llvm::IRBuilder<>& builder, llvm::Value* exitCondition, llvm::Value* exitCode);

            llvm::Value* hasNextRow(llvm::IRBuilder<>& builder, llvm::Value* j);

            void moveToNextRow(llvm::IRBuilder<>& builder, llvm::Value* j);


            llvm::BasicBlock* emitBadParseInputAndMoveToNextRow(llvm::IRBuilder<>& builder, llvm::Value* j, llvm::Value* condition, llvm::BasicBlock* loop_start);

            inline llvm::Value* rowNumber(llvm::IRBuilder<>& builder) {
                assert(_rowNumberVar);
                assert(_rowNumberVar->getType() == _env.i64ptrType());
                return builder.CreateLoad(_rowNumberVar);
            }

            llvm::Value* isDocumentOfObjectType(llvm::IRBuilder<>& builder, llvm::Value* j);

            void parseAndPrintStructuredDictFromObject(llvm::IRBuilder<>& builder, llvm::Value* j, llvm::BasicBlock* bbSchemaMismatch);

            void freeObject(llvm::IRBuilder<>& builder, llvm::Value* obj);

            llvm::Value* numberOfKeysInObject(llvm::IRBuilder<>& builder, llvm::Value* j);

            /*!
             *
             * @param builder
             * @param obj
             * @param t
             * @param check_for_keys if true, then row must contain exact keys for struct dict. Else, it's parsed whatever is specified in the schema.
             * @param bbSchemaMismatch
             */
            void parseAndPrint(llvm::IRBuilder<>& builder, llvm::Value* obj, const python::Type& t, bool check_for_keys, llvm::BasicBlock* bbSchemaMismatch);

            llvm::Value* decodeFieldFromObject(llvm::IRBuilder<>& builder, llvm::Value* obj, SerializableValue* out, llvm::Value* key, const python::Type& keyType, const python::Type& valueType, llvm::BasicBlock* bbSchemaMismatch);
            llvm::Value* decodeFieldFromObject(llvm::IRBuilder<>& builder, llvm::Value* obj, SerializableValue* out, const std::string& key, const python::Type& keyType, const python::Type& valueType, llvm::BasicBlock* bbSchemaMismatch) {
                return decodeFieldFromObject(builder, obj, out, _env.strConst(builder, key), keyType, valueType, bbSchemaMismatch);
            }
        };


        llvm::Value *JSONSourceTaskBuilder::numberOfKeysInObject(llvm::IRBuilder<> &builder, llvm::Value *j) {
            assert(j);

            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_numberOfKeys", _env.i64Type(), _env.i8ptrType());
            return builder.CreateCall(F, j);
        }

        llvm::Value *JSONSourceTaskBuilder::decodeFieldFromObject(llvm::IRBuilder<> &builder,
                                                                  llvm::Value* obj,
                                                                  tuplex::codegen::SerializableValue *out,
                                                                  llvm::Value *key, const python::Type &keyType,
                                                                  const python::Type &valueType,
                                                                  llvm::BasicBlock* bbSchemaMismatch) {
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
                v.val = _env.upcastToBoolean(builder, builder.CreateLoad(i_var));
                v.size = _env.i64Const(sizeof(int64_t));
                v.is_null = _env.i1Const(false);
            } else if(v_type == python::Type::F64) {
                auto F = getOrInsertFunction(mod, "JsonItem_getDouble", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(),
                                             _env.doublePointerType());
                auto f_var = _env.CreateFirstBlockVariable(builder, _env.f64Const(0));
                rc = builder.CreateCall(F, {obj, key, f_var});
                v.val = _env.upcastToBoolean(builder, builder.CreateLoad(f_var));
                v.size = _env.i64Const(sizeof(int64_t));
                v.is_null = _env.i1Const(false);
            } else if(v_type.isStructuredDictionaryType()) {
                auto F = getOrInsertFunction(mod, "JsonItem_getObject", _env.i64Type(), _env.i8ptrType(), _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
                auto obj_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr());
                // create call, recurse only if ok!
                BasicBlock* bbOK = BasicBlock::Create(ctx, "is_object", builder.GetInsertBlock()->getParent());


                rc = builder.CreateCall(F, {obj, key, obj_var});
                auto is_object = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                builder.CreateCondBr(is_object, bbOK, bbSchemaMismatch);
                builder.SetInsertPoint(bbOK);

                auto obj = builder.CreateLoad(obj_var);

                // recurse...
                parseAndPrint(builder, obj, v_type, true, bbSchemaMismatch);

                // free! @TODO: add to free list... -> yet should be ok?
                freeObject(builder, builder.CreateLoad(obj_var));
            } else if(v_type.isListType()) {
                std::cerr<<"skipping for now type: "<<v_type.desc()<<std::endl;
            } else {



                // for another nested object, utilize:

                throw std::runtime_error("encountered unsupported value type " + valueType.desc());
            }
            *out = v;

            return rc;
        }


        void JSONSourceTaskBuilder::parseAndPrint(llvm::IRBuilder<> &builder, llvm::Value *obj, const python::Type &t, bool check_for_keys, llvm::BasicBlock* bbSchemaMismatch) {
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

                if(all_keys_always_present && check_for_keys) {
                    // quick key check
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
                        _env.printValue(builder, num_keys, "struct type expected  " + std::to_string(kv_pairs.size()) + " elements, got: ");
                        builder.CreateBr(bbn);
                        builder.SetInsertPoint(bbn);
                    }
#endif
                    builder.CreateCondBr(cond, bbSchemaMismatch, bbOK);
                    builder.SetInsertPoint(bbOK);
                }

                for(auto kv_pair : kv_pairs) {
                    // optional? or always there?
                    if(kv_pair.alwaysPresent) {
                        // needs to be present, i.e. key error is fatal error!
                        SerializableValue value;
                        //auto rc = decodeFieldFromObject(builder, obj, &value, kv_pair.key, kv_pair.keyType, kv_pair.valueType, bbSchemaMismatch);
                        // assert(rc);
                        if(value.val)
                            _env.printValue(builder, value.val, "decoded " + kv_pair.valueType.desc());
                        else if(kv_pair.valueType.isStructuredDictionaryType()) {
                            // _env.debugPrint(builder, "decoded object");
                        }
                    } else {
                        // can or can not be present.
                    }
                }


            } else {
                // other types, parse with type check!
                throw std::runtime_error("unsupported type");
            }
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
            parseAndPrint(builder, builder.CreateLoad(obj_var), _rowType, true, bbSchemaMismatch);


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
                                                                      llvm::Value *condition,
                                                                      llvm::BasicBlock *loop_start) {
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
            _env.printValue(builder, rowNumber(builder), "row number: ");
            //_env.printValue(builder, line, "bad-parse for row: ");

            _env.cfree(builder, line);

            moveToNextRow(builder, j);
            builder.CreateBr(loop_start);

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
            auto bbSchemaMismatch = emitBadParseInputAndMoveToNextRow(builder, parser, _env.i1neg(builder, cond), bLoopHeader);

            // print out structure -> this is the parse
            parseAndPrintStructuredDictFromObject(builder, parser, bbSchemaMismatch);


            // go to next row
            moveToNextRow(builder, parser);

            // link back to header
            builder.CreateBr(bLoopHeader);

            // ---- post loop block ----
            // continue in loop exit.
            builder.SetInsertPoint(bLoopExit);

            // free JSON parse
            freeJsonParse(builder, parser);

            _env.printValue(builder, rowNumber(builder), "parsed rows: ");

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
    row_type = row_type.parameters().front();
    std::cout<<"detected: "<<row_type.desc()<<std::endl;


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
    jit.registerSymbol("JsonItem_numberOfKeys", JsonItem_numberOfKeys);

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
    //return;




//    // C-version of parsing
//    uint64_t row_number = 0;
//
//    auto j = JsonParser_init();
//    if(!j)
//        throw std::runtime_error("failed to initialize parser");
//    JsonParser_open(j, buf, buf_size);
//    while(JsonParser_hasNextRow(j)) {
//        if(JsonParser_getDocType(j) != JsonParser_objectDocType()) {
//            // BADPARSE_STRINGINPUT
//            auto line = JsonParser_getMallocedRow(j);
//            free(line);
//        }
//
//        // line ok, now extract something from the object!
//        // => basically need to traverse...
//        auto doc = *j->it;
//
////        auto obj = doc.get_object().take_value();
//
//        // get type
//        JsonItem *obj = nullptr;
//        uint64_t rc = JsonParser_getObject(j, &obj);
//        if(rc != 0)
//            break; // --> don't forget to release stuff here!
//        char* type_str = nullptr;
//        rc = JsonItem_getString(obj, "type", &type_str);
//        if(rc != 0)
//            continue; // --> don't forget to release stuff here
//        JsonItem *sub_obj = nullptr;
//        rc = JsonItem_getObject(obj, "repo", &sub_obj);
//        if(rc != 0)
//            continue; // --> don't forget to release stuff here!
//
//        // check wroong type
//        int64_t val_i = 0;
//        rc = JsonItem_getInt(obj, "repo", &val_i);
//        EXPECT_EQ(rc, ecToI64(ExceptionCode::TYPEERROR));
//        if(rc != 0) {
//            row_number++;
//            JsonParser_moveToNextRow(j);
//            continue; // --> next
//        }
//
//        char* url_str = nullptr;
//        rc = JsonItem_getString(sub_obj, "url", &url_str);
//
//        // error handling: KeyError?
//        rc = JsonItem_getString(sub_obj, "key that doesn't exist", &type_str);
//        EXPECT_EQ(rc, ecToI64(ExceptionCode::KEYERROR));
//
//        // release all allocated things
//        JsonItem_Free(obj);
//        JsonItem_Free(sub_obj);
//
//        row_number++;
//        JsonParser_moveToNextRow(j);
//    }
//    JsonParser_close(j);
//    JsonParser_free(j);
//
//    std::cout<<"Parsed "<<pluralize(row_number, "row")<<std::endl;
}