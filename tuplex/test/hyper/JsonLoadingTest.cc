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

#include <experimental/ListHelper.h>
#include <experimental/StructDictHelper.h>
#include <physical/experimental/JsonSourceTaskBuilder.h>

#include "JSONParseRowGenerator.h"

// NOTES:
// for concrete parser implementation with pushdown etc., use
// https://github.com/simdjson/simdjson/blob/master/doc/basics.md#json-pointer
// => this will allow to extract field...

namespace tuplex {
    namespace codegen {


        // refactor for more flexibility to cover more of the experimental scenarios
        // -> a decoder/parser  class for JSON
        // -> gets hthe type, the object to decode and some basic blocks to store free objects (start & end block!)
        // and a variable/decode if decode was successful?
        // => use this then to hook it up with serialization logic etc.


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

            // output vars
            llvm::Value *_outTotalRowsVar;
            llvm::Value *_outTotalBadRowsVar;
            llvm::Value *_outTotalSerializationSize;

            void writeOutput(llvm::IRBuilder<>& builder, llvm::Value* var, llvm::Value* val);


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

            void printValueInfo(llvm::IRBuilder<> &builder, const std::string &key, const python::Type &valueType,
                                llvm::Value *keyPresent, const SerializableValue &value);

            void checkRC(llvm::IRBuilder<> &builder, const std::string &key, llvm::Value *rc);
        };

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
            auto struct_dict_type = create_structured_dict_type(_env, _rowType);
            auto row_var = _env.CreateFirstBlockAlloca(builder, struct_dict_type);
            struct_dict_mem_zero(_env, builder, row_var, _rowType); // !!! important !!!


            // create dict parser and store to row_var
            JSONParseRowGenerator gen(_env, _rowType,  _freeEnd, bbSchemaMismatch);
            gen.parseToVariable(builder, builder.CreateLoad(obj_var), row_var);

            auto s = struct_dict_type_serialized_memory_size(_env, builder, row_var, _rowType);
             _env.printValue(builder, s.val, "size of row materialized in bytes is: ");

             // rtmalloc and serialize!
             auto mem_ptr = _env.malloc(builder, s.val);
             auto serialization_res = struct_dict_serialize_to_memory(_env, builder, row_var, _rowType, mem_ptr);
             _env.printValue(builder, serialization_res.size, "realized serialization size is: ");

             // inc total size with serialization size!
             auto cur_total = builder.CreateLoad(_outTotalSerializationSize);
             auto new_total = builder.CreateAdd(cur_total, serialization_res.size);
             builder.CreateStore(new_total, _outTotalSerializationSize);

            // now, load entries to struct type in LLVM
            // then calculate serialized size and print it.
            //auto v = struct_dict_load_from_values(_env, builder, _rowType, entries);


            //// => call with row type
            //parseAndPrint(builder, builder.CreateLoad(obj_var), "", true, _rowType, true, bbSchemaMismatch);

            // free obj_var...
            json_freeObject(_env, builder, builder.CreateLoad(obj_var));
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
            _env.freeAll(builder); // -> call rtfree!
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

            // store in vars
            builder.CreateStore(rowNumber(builder), _outTotalRowsVar);
            builder.CreateStore(builder.CreateLoad(_badParseCountVar), _outTotalBadRowsVar);
        }


        void JSONSourceTaskBuilder::writeOutput(llvm::IRBuilder<> &builder, llvm::Value *var, llvm::Value *val) {
            using namespace llvm;

            assert(var && val);
            assert(var->getType() == val->getType()->getPointerTo());

            // if var != nullptr:
            // *var = val
            auto& ctx = _env.getContext();
            BasicBlock *bDone = BasicBlock::Create(ctx, "output_done", builder.GetInsertBlock()->getParent());
            BasicBlock *bWrite = BasicBlock::Create(ctx, "output_write", builder.GetInsertBlock()->getParent());

            auto cond = builder.CreateICmpNE(var, _env.nullConstant(var->getType()));
            builder.CreateCondBr(cond, bWrite, bDone);

            builder.SetInsertPoint(bWrite);
            builder.CreateStore(val, var);
            builder.CreateBr(bDone);

            builder.SetInsertPoint(bDone);
        }

        void JSONSourceTaskBuilder::build() {
            using namespace llvm;
            auto &ctx = _env.getContext();

            // create main function (takes buffer and buf_size, later take the other tuplex stuff)
            FunctionType *FT = FunctionType::get(ctypeToLLVM<int64_t>(ctx),
                                                 {ctypeToLLVM<char *>(ctx),
                                                         ctypeToLLVM<int64_t>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),
                                                  ctypeToLLVM<int64_t*>(ctx),}, false);

            Function *F = Function::Create(FT, llvm::GlobalValue::ExternalLinkage, _functionName,
                                           *_env.getModule().get());
            auto m = mapLLVMFunctionArgs(F, {"buf", "buf_size", "out_total_rows", "out_bad_parse_rows", "out_total_size"});

            auto bbEntry = BasicBlock::Create(ctx, "entry", F);
            IRBuilder<> builder(bbEntry);

            // allocate variables
            _outTotalRowsVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            _outTotalBadRowsVar = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            _outTotalSerializationSize = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));

            // dummy parse, simply print type and value with type checking.
            generateParseLoop(builder, m["buf"], m["buf_size"]);

            writeOutput(builder, m["out_total_rows"], builder.CreateLoad(_outTotalRowsVar));
            writeOutput(builder, m["out_bad_parse_rows"], builder.CreateLoad(_outTotalBadRowsVar));
            writeOutput(builder, m["out_total_size"], builder.CreateLoad(_outTotalSerializationSize));

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


namespace tuplex {
    std::tuple<python::Type, python::Type, size_t, size_t> detectTypes(const std::string& sample) {
        auto buf = sample.data();
        auto buf_size = sample.size();


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

        // check data layout
        codegen::LLVMEnvironment env;
        auto n_size = codegen::struct_dict_heap_size(env, normal_case_type);
        auto g_size = codegen::struct_dict_heap_size(env, general_case_type);

        return std::make_tuple(normal_case_type, general_case_type, n_size, g_size);
    }
}

TEST_F(HyperTest, PushEventPaperExample) {
    using namespace tuplex;
    using namespace std;


    auto content_before_filter_promote = "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 42}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 39}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 2}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 0}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20, \"C\" : null, \"D\" : 32}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : 0}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}";
    auto content_after_filter_promote = "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 42}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 39}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 2}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 0}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20, \"C\" : null, \"D\" : 32}}";

    python::Type normal, general;
    auto t_before = detectTypes(content_before_filter_promote);
    normal = std::get<0>(t_before); general = std::get<1>(t_before);

    std::cout<<"before filter promo:: normal: "<<normal.desc()<<"  general: "<<general.desc()<<endl;
    std::cout<<"              size :: normal: "<<std::get<2>(t_before)<<"  general: "<<std::get<3>(t_before)<<endl;

    auto t_after = detectTypes(content_after_filter_promote);
    normal = std::get<0>(t_after); general = std::get<1>(t_after);

    std::cout<<"after filter promo:: normal: "<<normal.desc()<<"  general: "<<general.desc()<<endl;
    std::cout<<"              size :: normal: "<<std::get<2>(t_after)<<"  general: "<<std::get<3>(t_after)<<endl;
    EXPECT_TRUE(normal != python::Type::UNKNOWN);

    // --- manual --- post filter pushdown + constant folding
    // Struct[type -> _Constant["PushEvent"], A -> Struct[B -> i64]]


    throw std::runtime_error("");
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

      path = "../resources/2011-11-26-13.sample3.json"; // -> single row, the parse should trivially work.

    // tiny json example to simplify things
    // path = "../resources/ndjson/example1.json";


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

    auto stype = codegen::create_structured_dict_type(env, row_type);
    // create new func with this
    create_dummy_function(env, stype);

//    std::string err;
//    EXPECT_TRUE(codegen::verifyModule(*env.getModule(), &err));
//    std::cerr<<err<<std::endl;

    // TypeFinderDebug.run(*env.getModule());

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


namespace tuplex {
    std::tuple<size_t, size_t, size_t> runCodegen(const python::Type& row_type, const uint8_t* buf, size_t buf_size) {
        using namespace tuplex;
        using namespace std;

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
        codegen::addJsonSymbolsToJIT(jit);
        jit.registerSymbol("rtmalloc", runtime::rtmalloc);


        // // optimize code (note: is in debug mode super slow)
        // LLVMOptimizer opt;
        // ir_code = opt.optimizeIR(ir_code);

        // compile func
        auto rc_compile = jit.compile(ir_code);
        //ASSERT_TRUE(rc_compile);

        // get func
        auto func = reinterpret_cast<int64_t(*)(const char *, size_t, size_t*, size_t*, size_t*)>(jit.getAddrOfSymbol(parseFuncName));

        // runtime init
        ContextOptions co = ContextOptions::defaults();
        runtime::init(co.RUNTIME_LIBRARY(false).toPath());

        // call code generated function!
        Timer timer;
        size_t total_rows = 0;
        size_t bad_rows = 0;
        size_t total_size = 0;
        auto rc = func(reinterpret_cast<const char*>(buf), buf_size, &total_rows, &bad_rows, &total_size);
        std::cout << "parsed rows in " << timer.time() << " seconds, (" << sizeToMemString(buf_size) << ")" << std::endl;
        std::cout << "done" << std::endl;

        return make_tuple(total_rows, bad_rows, total_size);
    }
}

TEST_F(HyperTest, LoadAllFiles) {
    using namespace tuplex;
    using namespace std;

    Logger::instance().init();
    auto& logger = Logger::instance().logger("experiment");

    string root_path = "/data/github_sample/*.json.gz";
    auto paths = glob(root_path);
    logger.info("Found " + pluralize(paths.size(), "path") + " under " + root_path);

    std::vector<std::string> bad_paths;

    // now perform detection & parse for EACH file.
    for(const auto& path : paths) {
        logger.info("Processing " + path);

        try {
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
            //row_type = general_case_type; // <-- this should match MOST of the rows...
            // row_type = normal_case_type;

            // could do here a counter experiment: I.e., how many general case rows? how many normal case rows? how many fallback rows?
            // => then also measure how much memory is required!
            // => can perform example experiments for the 10 different files and plot it out.
            std::cout<<"-----\nrunning using general case:\n-----\n"<<std::endl;
            auto normal_rc = runCodegen(normal_case_type, reinterpret_cast<const uint8_t*>(buf), buf_size);
            std::cout<<"-----\nrunning using normal case:\n-----\n"<<std::endl;
            auto general_rc = runCodegen(general_case_type, reinterpret_cast<const uint8_t*>(buf), buf_size);

            {
                std::vector<std::tuple<size_t, size_t, size_t>> rows({normal_rc, general_rc});
                std::vector<std::string> headers({"normal", "general"});
                for(int i = 0; i < 2; ++i) {
                    std::cout<<headers[i]<<":  "<<"#rows: "<<std::get<0>(rows[i])<<"  #bad-rows: "<<std::get<1>(rows[i])<<"  size(bytes): "<<std::get<2>(rows[i])<<std::endl;
                }
            }
        } catch (std::exception& e) {
            logger.error("path " + path + " failed processing with: " + e.what());
            bad_paths.push_back(path);
        }
    }

    if(!bad_paths.empty()) {
        for(const auto& path : bad_paths) {
            logger.error("failed to process: " + path);
        }
    }
}

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
      //path = "../resources/2011-11-26-13.sample2.json"; // -> so this works.

     // path = "../resources/2011-11-26-13.sample3.json"; // -> single row, the parse should trivially work.


    // // tiny json example to simplify things
     // path = "../resources/ndjson/example1.json";


//     // mini example in order to analyze code
//     path = "test.json";
//     auto content = "{\"column1\": {\"a\": \"hello\", \"b\": 20, \"c\": 30}}\n"
//                    "{\"column1\": {\"a\": \"test\", \"b\": 20, \"c\": null}}\n"
//                    "{\"column1\": {\"a\": \"cat\",  \"c\": null}}";
//     stringToFile(path, content);

//     // mini example in order to analyze code
//     path = "test.json";
//
//     // this here is a simple example of a list decode
//     auto content = "{\"column1\": {\"a\": [1, 2, 3, 4]}}\n"
//                    "{\"column1\": {\"a\": [1, 4]}}\n"
//                    "{\"column1\": {\"a\": []}}";
//     stringToFile(path, content);


//     // mini example in order with optional struct type!
//     path = "test.json";
//
//     // this here is a simple example of a list decode
//     auto content = "{\"column1\": {\"a\": [1, 2, 3, 4]}}\n"
//                    "{\"column1\": null}\n"
//                    "{\"column1\": null}";
//     stringToFile(path, content);

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
    //row_type = general_case_type; // <-- this should match MOST of the rows...
    // row_type = normal_case_type;

    // could do here a counter experiment: I.e., how many general case rows? how many normal case rows? how many fallback rows?
    // => then also measure how much memory is required!
    // => can perform example experiments for the 10 different files and plot it out.
    std::cout<<"-----\nrunning using general case:\n-----\n"<<std::endl;
    auto normal_rc = runCodegen(normal_case_type, reinterpret_cast<const uint8_t*>(buf), buf_size);
    std::cout<<"-----\nrunning using normal case:\n-----\n"<<std::endl;
    auto general_rc = runCodegen(general_case_type, reinterpret_cast<const uint8_t*>(buf), buf_size);

    {
        std::vector<std::tuple<size_t, size_t, size_t>> rows({normal_rc, general_rc});
        std::vector<std::string> headers({"normal", "general"});
        for(int i = 0; i < 2; ++i) {
        std::cout<<headers[i]<<":  "<<"#rows: "<<std::get<0>(rows[i])<<"  #bad-rows: "<<std::get<1>(rows[i])<<"  size(bytes): "<<std::get<2>(rows[i])<<std::endl;
        }
    }
}

TEST_F(HyperTest, CParse) {
    using namespace tuplex;
    using namespace tuplex::codegen;
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