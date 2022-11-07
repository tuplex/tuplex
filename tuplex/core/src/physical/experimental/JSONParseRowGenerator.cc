//
// Created by leonhard on 9/25/22.
//

#include <physical/experimental/JSONParseRowGenerator.h>
#include <codegen/FlattenedTuple.h>

namespace tuplex {
    namespace codegen {
        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeString(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
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
        JSONParseRowGenerator::decodeBoolean(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
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
        JSONParseRowGenerator::decodeI64(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
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
        JSONParseRowGenerator::decodeF64(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
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
        JSONParseRowGenerator::decodeEmptyDict(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            // query sub-object and call count keys!
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getObject", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
            auto sub_obj_var = addObjectVar(builder);
 #ifdef JSON_PARSER_TRACE_MEMORY
            // debug:
            _env.debugPrint(builder, "Using " + pointer2hex(sub_obj_var) + " in decodeEmptyDict");
#endif
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
        JSONParseRowGenerator::decodeNull(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
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
        JSONParseRowGenerator::decodeEmptyList(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key) {
            // similar to empty dict

            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            // query for array and determine array size, if 0 => match!
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getArray", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
            auto item_var = addArrayVar(builder);

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
        JSONParseRowGenerator::decodeBooleanFromArray(llvm::IRBuilder<> &builder, llvm::Value *array,
                                                      llvm::Value *index) {
            using namespace std;
            using namespace llvm;


            assert(array && index);
            assert(index->getType() == _env.i64Type());

            // decode using string
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonArray_getBoolean", _env.i64Type(), _env.i8ptrType(), _env.i64Type(),
                                         _env.i64ptrType());
            auto i_var = _env.CreateFirstBlockVariable(builder, _env.i64Const(0));
            llvm::Value* rc = builder.CreateCall(F, {array, index, i_var});
            SerializableValue v;
            v.val = builder.CreateZExtOrTrunc(builder.CreateLoad(i_var), _env.getBooleanType());
            v.size = _env.i64Const(sizeof(int64_t));
            v.is_null = _env.i1Const(false);
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeI64FromArray(llvm::IRBuilder<> &builder, llvm::Value *array, llvm::Value *index) {
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

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeF64FromArray(llvm::IRBuilder<> &builder, llvm::Value *array, llvm::Value *index) {
            using namespace std;
            using namespace llvm;


            assert(array && index);
            assert(index->getType() == _env.i64Type());

            // decode using string
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonArray_getDouble", _env.i64Type(), _env.i8ptrType(), _env.i64Type(),
                                         _env.doublePointerType());
            auto f_var = _env.CreateFirstBlockVariable(builder, _env.f64Const(0));
            llvm::Value* rc = builder.CreateCall(F, {array, index, f_var});
            SerializableValue v;
            v.val = builder.CreateLoad(f_var);
            v.size = _env.i64Const(sizeof(int64_t));
            v.is_null = _env.i1Const(false);
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeStringFromArray(llvm::IRBuilder<> &builder, llvm::Value *array,
                                                     llvm::Value *index) {
            using namespace std;
            using namespace llvm;

            assert(array && index);
            assert(index->getType() == _env.i64Type());

            // decode using string
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonArray_getStringAndSize", _env.i64Type(), _env.i8ptrType(),
                                         _env.i64Type(), _env.i8ptrType()->getPointerTo(0), _env.i64ptrType());
            auto str_var = _env.CreateFirstBlockVariable(builder, _env.i8nullptr(), "s");
            auto str_size_var = _env.CreateFirstBlockVariable(builder, _env.i64Const(0), "s_size");

            builder.CreateStore(_env.i8nullptr(), str_var);
            builder.CreateStore(_env.i64Const(0), str_size_var);
            llvm::Value* rc = builder.CreateCall(F, {array, index, str_var, str_size_var});
            SerializableValue v;
            v.val = builder.CreateLoad(str_var);
            v.size = builder.CreateLoad(str_size_var);
            v.is_null = _env.i1Const(false);
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeObjectFromArray(llvm::IRBuilder<> &builder,
                                                     llvm::Value *array,
                                                     llvm::Value *index,
                                                     const python::Type &dict_type) {
            // this is quite complex.
            using namespace std;
            using namespace llvm;

            auto& ctx = builder.getContext();

            assert(array && index);
            assert(index->getType() == _env.i64Type());

            assert(dict_type.isStructuredDictionaryType());

            auto rc_var = _env.CreateFirstBlockAlloca(builder, _env.i64Type());

            // alloc new struct dict value (heap-allocated!)
            auto llvm_dict_type = _env.getOrCreateStructuredDictType(dict_type);
            // calculate size of dictionary!
            auto dict_size = struct_dict_heap_size(_env, dict_type); // make sure this is correct! else, big issues.
            auto dict_ptr = builder.CreatePointerCast(_env.malloc(builder, dict_size), llvm_dict_type->getPointerTo());
            struct_dict_mem_zero(_env, builder, dict_ptr, dict_type);

            // check whether object exists, if so get the pointer. Then alloc, and store everything in it.
            // TODO: what about option types? ==> not handled yet!

            // create now some basic blocks to decode ON demand.
            BasicBlock *bbDecodeItem = BasicBlock::Create(ctx, "within_array_decode_object", builder.GetInsertBlock()->getParent());
            BasicBlock *bbDecodeDone = BasicBlock::Create(ctx, "array_next_element", builder.GetInsertBlock()->getParent());
            BasicBlock *bbSchemaMismatch = BasicBlock::Create(ctx, "within_array_schema_mismatch", builder.GetInsertBlock()->getParent());

            {
                auto F = getOrInsertFunction(_env.getModule().get(), "JsonArray_getObject", _env.i64Type(),
                                             _env.i8ptrType(),
                                             _env.i64Type(), _env.i8ptrType()->getPointerTo(0));
                auto item_var = addObjectVar(builder);
                #ifdef JSON_PARSER_TRACE_MEMORY
                _env.debugPrint(builder, "Using " + pointer2hex(item_var) + " in decodeObjectFromArray");
                #endif
                // start decode
                // create call, recurse only if ok!
                llvm::Value *rc = builder.CreateCall(F, {array, index, item_var});
                builder.CreateStore(rc, rc_var);
                auto is_object = builder.CreateICmpEQ(rc, _env.i64Const(
                        ecToI64(ExceptionCode::SUCCESS))); // <-- indicates successful parse

                builder.CreateCondBr(is_object, bbDecodeItem, bbDecodeDone);

                builder.SetInsertPoint(bbDecodeItem);
                // load item!
                auto item = builder.CreateLoad(item_var);
                // recurse using new prefix
                // --> similar to flatten_recursive_helper(entries, kv_pair.valueType, access_path, include_maybe_structs);
                decode(builder,
                       dict_ptr,
                       dict_type,
                       item, bbSchemaMismatch, dict_type, {}, true);
                builder.CreateBr(bbDecodeDone); // whererver builder is, continue to decode done for this item.
            }

            {
                builder.SetInsertPoint(bbSchemaMismatch);
                builder.CreateStore(_env.i64Const(ecToI64(ExceptionCode::TYPEERROR)), rc_var);
                // _env.debugPrint(builder, "element did not conform to struct schema " + dict_type.desc());
                badParseCause("element did not conform to struct schema " + dict_type.desc());
                builder.CreateBr(bbDecodeDone);
            }


            builder.SetInsertPoint(bbDecodeDone); // continue from here...
            llvm::Value* rc = builder.CreateLoad(rc_var); // <-- error
            SerializableValue v;
            v.val = builder.CreateLoad(dict_ptr);
            return make_tuple(rc, v);
        }


        llvm::Value* JSONParseRowGenerator::generateDecodeListItemsLoop(llvm::IRBuilder<> &builder, llvm::Value *array,
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
                std::tie(item_rc, item) = decodeFromArray(builder, array, index, element_type);

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
        JSONParseRowGenerator::decodeNullFromArray(llvm::IRBuilder<> &builder, llvm::Value *array, llvm::Value *index) {

            using namespace std;
            using namespace llvm;

            assert(array && index);
            assert(array->getType() == _env.i8ptrType());
            assert(index->getType() == _env.i64Type());

            // special case!
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonArray_IsNull", _env.i64Type(), _env.i8ptrType(), _env.i64Type());
            llvm::Value* rc = builder.CreateCall(F, {array, index});
            SerializableValue v;
            v.is_null = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeOptionFromArray(llvm::IRBuilder<> &builder, const python::Type &option_type,
                                                     llvm::Value *array, llvm::Value *index) {
            using namespace llvm;

            assert(option_type.isOptionType());

            auto& ctx = builder.getContext();

            llvm::Value* rc = nullptr;
            SerializableValue value;

            BasicBlock* bbCurrent = builder.GetInsertBlock();
            BasicBlock* bbDecodeIsNull = BasicBlock::Create(ctx, "array_decode_option_null", bbCurrent->getParent());
            BasicBlock* bbDecodeNonNull = BasicBlock::Create(ctx, "array_decode_option_non_null", bbCurrent->getParent());
            BasicBlock* bbDecoded = BasicBlock::Create(ctx, "array_decoded_option", bbCurrent->getParent());

            auto element_type = option_type.getReturnType();
            assert(!element_type.isOptionType()); // there can't be option of option...

            // special case: element to decode is struct type
            llvm::Value* null_struct = nullptr;
            if(element_type.isStructuredDictionaryType()) {
                auto llvm_element_type = _env.getOrCreateStructuredDictType(element_type);
                auto null_struct_var = _env.CreateFirstBlockAlloca(builder, llvm_element_type);
                struct_dict_mem_zero(_env, builder, null_struct_var, element_type);
                null_struct = builder.CreateLoad(null_struct_var);
            }

            // check if it is null
            llvm::Value* rcA = nullptr;
            std::tie(rcA, value) = decodeNullFromArray(builder, array, index);
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
            SerializableValue valueB;
            std::tie(rcB, valueB) = decodeFromArray(builder, array, index, element_type);
            bbValueIsNotNull = builder.GetInsertBlock(); // <-- this is the block from where to jump to bbDecoded (phi entry block)
            builder.CreateBr(bbDecoded);

            // --- decode done ----
            builder.SetInsertPoint(bbDecoded);
            // finish decode by jumping into bbDecoded block.
            // fetch rc and value depending on block (phi node!)
            // => for null, create dummy values so phi works!
            assert(rcB);
            SerializableValue valueA;
            valueA.is_null = _env.i1Const(true); // valueA is null
            if(valueB.val) {
                if(!element_type.isStructuredDictionaryType())
                    valueA.val = _env.nullConstant(valueB.val->getType());
                else {
                    assert(null_struct);
                    valueA.val = null_struct;
                }
            }
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

            return std::make_tuple(rc, value);
        }


        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeEmptyListFromArray(llvm::IRBuilder<> &builder, llvm::Value *array,
                                                        llvm::Value *index) {
            using namespace std;
            using namespace llvm;

            assert(array && index);
            assert(array->getType() == _env.i8ptrType());
            assert(index->getType() == _env.i64Type());

            // special case!
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonArray_getEmptyArray", _env.i64Type(), _env.i8ptrType(), _env.i64Type());
            llvm::Value* rc = builder.CreateCall(F, {array, index});
            SerializableValue v;
            v.is_null = builder.CreateICmpEQ(rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            return make_tuple(rc, v);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeListFromArray(llvm::IRBuilder<> &builder, const python::Type &list_type,
                                                   llvm::Value *array, llvm::Value *index) {
            using namespace llvm;
            using namespace std;

            assert(list_type.isListType());

            // special case: empty list?
            if(list_type == python::Type::EMPTYLIST) {
                return decodeEmptyListFromArray(builder, array, index);
            }

            auto element_type = list_type.elementType();

            // create list ptr (in any case!)
            auto list_llvm_type = _env.getOrCreateListType(list_type);
            auto list_ptr = _env.CreateFirstBlockAlloca(builder, list_llvm_type);
            list_init_empty(_env, builder, list_ptr, list_type);


            auto rc_var = _env.CreateFirstBlockAlloca(builder, _env.i64Type());
            builder.CreateStore(_env.i64Const(ecToI64(ExceptionCode::SUCCESS)), rc_var);

            // decode happens in two steps:
            // step 1: check if there's actually an array in the JSON data -> if not, type error!
            auto Fgetarray = getOrInsertFunction(_env.getModule().get(), "JsonArray_getArray", _env.i64Type(), _env.i8ptrType(),
                                                 _env.i64Type(), _env.i8ptrType()->getPointerTo(0));
            auto item_var = addArrayVar(builder);

            // create call, recurse only if ok!
            BasicBlock *bbCurrent = builder.GetInsertBlock();
            BasicBlock *bbArrayFound = BasicBlock::Create(_env.getContext(), "found_array", builder.GetInsertBlock()->getParent());
            BasicBlock *bbDecodeFailed = BasicBlock::Create(_env.getContext(), "array_decode_failed", builder.GetInsertBlock()->getParent());
            BasicBlock *bbDecodeDone = BasicBlock::Create(_env.getContext(), "array_decode_done", builder.GetInsertBlock()->getParent());
#ifdef JSON_PARSER_TRACE_MEMORY
            _env.debugPrint(builder, "calling JsonArray_getArray on line " + std::to_string(__LINE__) + " to decode " + list_type.desc());
#endif

            llvm::Value* rc_A = builder.CreateCall(Fgetarray, {array, index, item_var});
            builder.CreateStore(rc_A, rc_var);
            auto found_array = builder.CreateICmpEQ(rc_A, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            builder.CreateCondBr(found_array, bbArrayFound, bbDecodeFailed);

            builder.SetInsertPoint(bbDecodeFailed);
            badParseCause("no array found or array is object");
            builder.CreateBr(_badParseBlock);

            // -----------------------------------------------------------
            // step 2: check that it is a homogenous list...
            auto list_element_type = list_type.elementType();
            builder.SetInsertPoint(bbArrayFound);
            auto sub_array = builder.CreateLoad(item_var);
            auto num_elements = arraySize(builder, sub_array);

            // debug print here number of elements...
            // _env.printValue(builder, num_elements, "found for type " + listType.desc() + " elements: ");

            // reserve capacity for elements
            bool initialize_elements_as_null = true; //false;
            list_reserve_capacity(_env, builder, list_ptr, list_type, num_elements, initialize_elements_as_null);

            // decoding happens in a loop...
            // -> basically get the data!
            auto list_rc = generateDecodeListItemsLoop(builder, sub_array, list_ptr, list_type, num_elements);

            // debug print, checking what the list decode gives back...
            // _env.printValue(builder, list_rc, "decode result is: ");

            // only if decode is ok, store list size!
            auto list_decode_ok = builder.CreateICmpEQ(list_rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            BasicBlock* bbListOK = BasicBlock::Create(_env.getContext(), "sub_array_decode_ok", builder.GetInsertBlock()->getParent());
            builder.CreateCondBr(list_decode_ok, bbListOK, bbDecodeDone);

            {
                // --- set list size when ok ---
                builder.SetInsertPoint(bbListOK);
                list_store_size(_env, builder, list_ptr, list_type, num_elements); // <-- now list is ok!
                builder.CreateBr(bbDecodeDone);
            }

            builder.SetInsertPoint(bbDecodeDone);
            llvm::Value* rc = builder.CreateLoad(rc_var);
            SerializableValue value;
            value.val = builder.CreateLoad(list_ptr); // retrieve the ptr representing the list
            return make_tuple(rc, value);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeFromArray(llvm::IRBuilder<> &builder, llvm::Value *array, llvm::Value *index,
                                               const python::Type &element_type) {
            using namespace llvm;

            llvm::Value* item_rc = nullptr;
            SerializableValue item;
            // decode based on type
            if(element_type == python::Type::BOOLEAN) {
                return decodeBooleanFromArray(builder, array, index);
            } else if(element_type == python::Type::I64) {
                return decodeI64FromArray(builder, array, index);
            } else if(element_type == python::Type::F64) {
                return decodeF64FromArray(builder, array, index);
            } else if(element_type == python::Type::STRING) {
                return decodeStringFromArray(builder, array, index);
            } else if(element_type.isStructuredDictionaryType()) {
                // special case: decode nested object from array
                return decodeObjectFromArray(builder, array, index, element_type);
            } else if(element_type.isTupleType()) {
                return decodeTupleFromArray(builder, array, index, element_type, true); // !! must be true here
            } else if(element_type == python::Type::NULLVALUE) {
                return decodeNullFromArray(builder, array, index);
            } else if(element_type.isOptionType()) {
                // check if it's a null value -> if not decode value
                return decodeOptionFromArray(builder, element_type, array, index);
            } else if(element_type.isListType()) {
                // recursive decode...
                return decodeListFromArray(builder, element_type, array, index);
            } else {
                throw std::runtime_error("can not decode type " + element_type.desc() + " from array.");
            }

            return std::make_tuple(item_rc, item);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeTupleFromArray(llvm::IRBuilder<> &builder, llvm::Value *array, llvm::Value *index,
                                                    const python::Type &tuple_type, bool store_as_heap_ptr) {
            assert(tuple_type.isTupleType() || tuple_type == python::Type::EMPTYTUPLE);
            using namespace std;
            using namespace llvm;

            assert(array && index);
            assert(array->getType() == _env.i8ptrType());
            assert(index->getType() == _env.i64Type());

            auto& ctx = _env.getContext();

            // special case: () => i.e. same as empty list! just need to check for correct number of elements
            if(python::Type::EMPTYTUPLE == tuple_type)
                return decodeEmptyListFromArray(builder, array, index);

            // create flattened tuple
            FlattenedTuple ft(&_env);
            ft.init(tuple_type);

            auto rc_var = _env.CreateFirstBlockAlloca(builder, _env.i64Type());
            builder.CreateStore(_env.i64Const(ecToI64(ExceptionCode::SUCCESS)), rc_var);

            // decode happens in two steps:
            // step 1: check if there's actually an array in the JSON data -> if not, type error!
            auto Fgetarray = getOrInsertFunction(_env.getModule().get(), "JsonArray_getArray", _env.i64Type(), _env.i8ptrType(),
                                         _env.i64Type(), _env.i8ptrType()->getPointerTo(0));
            auto item_var = addArrayVar(builder);

            // create call, recurse only if ok!
            BasicBlock *bbCurrent = builder.GetInsertBlock();
            BasicBlock *bbArrayFound = BasicBlock::Create(_env.getContext(), "found_array", builder.GetInsertBlock()->getParent());
            BasicBlock *bbDecodeFailed = BasicBlock::Create(_env.getContext(), "array_decode_failed", builder.GetInsertBlock()->getParent());
#ifdef JSON_PARSER_TRACE_MEMORY
            _env.debugPrint(builder, "calling JsonArray_getArray on line " + std::to_string(__LINE__));
#endif
            llvm::Value* rc_A = builder.CreateCall(Fgetarray, {array, index, item_var});
            builder.CreateStore(rc_A, rc_var);
            auto found_array = builder.CreateICmpEQ(rc_A, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            builder.CreateCondBr(found_array, bbArrayFound, bbDecodeFailed);

            builder.SetInsertPoint(bbDecodeFailed);
            badParseCause("no array found or array is object");
            builder.CreateBr(_badParseBlock);


            // -----------------------------------------------------------
            // step 2: check that the array has the same size as the tuple! if not -> schema mismatch...
            builder.SetInsertPoint(bbArrayFound);
            auto sub_array = builder.CreateLoad(item_var);
            auto num_elements = arraySize(builder, sub_array);

            // debug print here number of elements...
            // _env.printValue(builder, num_elements, "found for type " + tupleType.desc() + " elements: ");
            auto num_tuple_elements = tuple_type.parameters().size();

            BasicBlock* bbMatchingSize = BasicBlock::Create(_env.getContext(), "array_element_count_matches", builder.GetInsertBlock()->getParent());
            auto match_cond = builder.CreateICmpEQ(num_elements, _env.i64Const(num_tuple_elements));
            builder.CreateCondBr(match_cond, bbMatchingSize, _badParseBlock);

            builder.SetInsertPoint(bbMatchingSize);
            // _env.debugPrint(builder, "decoding tuple " + tuple_type.desc());
            for(unsigned i = 0; i < num_tuple_elements; ++i) {
                // check type
                auto element_type = tuple_type.parameters()[i];
                llvm::Value* sub_index = _env.i64Const(i);

                llvm::Value* item_rc = nullptr;
                SerializableValue item;

                // check what the result is of item_rc -> can be combined with rc!
                BasicBlock* bDecodeOK = BasicBlock::Create(ctx, "item_" + std::to_string(i) + "_decode_ok", builder.GetInsertBlock()->getParent());
                BasicBlock* bDecodeFail = BasicBlock::Create(ctx, "item_" + std::to_string(i) + "_decode_failed", builder.GetInsertBlock()->getParent());

                std::tie(item_rc, item) = decodeFromArray(builder, sub_array, sub_index, element_type);

                auto is_item_decode_ok = builder.CreateICmpEQ(item_rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                builder.CreateCondBr(is_item_decode_ok, bDecodeOK, bDecodeFail);

                builder.SetInsertPoint(bDecodeFail);
                badParseCause("failed to decode tuple element " + std::to_string(i) + " (" + element_type.desc() + ")");
                builder.CreateBr(_badParseBlock);

                builder.SetInsertPoint(bDecodeOK);

                // // debug:
                // _env.printValue(builder, item.size, "size of tuple element " + std::to_string(i) + " of type " + element_type.desc() + " is: ");

                // add to tuple
                ft.set(builder, {(int)i}, item.val, item.size, item.is_null);
            }

            llvm::Value* rc = builder.CreateLoad(rc_var);
            SerializableValue value;
            value.val = store_as_heap_ptr ? ft.loadToHeapPtr(builder) : ft.loadToPtr(builder);

            return make_tuple(rc, value);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeTuple(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key,
                                           const python::Type &tupleType) {
            assert(tupleType.isTupleType() || tupleType == python::Type::EMPTYTUPLE);
            using namespace std;
            using namespace llvm;

            assert(obj && key);
            assert(obj->getType() == _env.i8ptrType());
            assert(key->getType() == _env.i8ptrType());

            auto& ctx = _env.getContext();

            // special case: () => i.e. same as empty list! just need to check for correct number of elements

            if(python::Type::EMPTYTUPLE == tupleType)
                return decodeEmptyList(builder, obj, key);

            // create flattened tuple
            FlattenedTuple ft(&_env);
            ft.init(tupleType);

            auto rc_var = _env.CreateFirstBlockAlloca(builder, _env.i64Type());
            builder.CreateStore(_env.i64Const(ecToI64(ExceptionCode::SUCCESS)), rc_var);

            // decode happens in two steps:
            // step 1: check if there's actually an array in the JSON data -> if not, type error!
            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getArray", _env.i64Type(), _env.i8ptrType(),
                                         _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
            auto item_var = addArrayVar(builder);

            // create call, recurse only if ok!
            BasicBlock *bbCurrent = builder.GetInsertBlock();
            BasicBlock *bbArrayFound = BasicBlock::Create(_env.getContext(), "found_array", builder.GetInsertBlock()->getParent());
            BasicBlock *bbDecodeDone = BasicBlock::Create(_env.getContext(), "array_decode_done", builder.GetInsertBlock()->getParent());
            llvm::Value* rc_A = builder.CreateCall(F, {obj, key, item_var});
            builder.CreateStore(rc_A, rc_var);
            auto found_array = builder.CreateICmpEQ(rc_A, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            builder.CreateCondBr(found_array, bbArrayFound, bbDecodeDone);


            // -----------------------------------------------------------
            // step 2: check that the array has the same size as the tuple! if not -> schema mismatch...
            builder.SetInsertPoint(bbArrayFound);
            auto array = builder.CreateLoad(item_var);
            auto num_elements = arraySize(builder, array);

            // debug print here number of elements...
            // _env.printValue(builder, num_elements, "found for type " + tupleType.desc() + " elements: ");
            auto num_tuple_elements = tupleType.parameters().size();

            BasicBlock* bbMatchingSize = BasicBlock::Create(_env.getContext(), "array_element_count_matches", builder.GetInsertBlock()->getParent());
            auto match_cond = builder.CreateICmpEQ(num_elements, _env.i64Const(num_tuple_elements));
            builder.CreateCondBr(match_cond, bbMatchingSize, _badParseBlock);

            builder.SetInsertPoint(bbMatchingSize);
            for(unsigned i = 0; i < num_tuple_elements; ++i) {
                // check type
                auto element_type = tupleType.parameters()[i];
                llvm::Value* index = _env.i64Const(i);

                llvm::Value* item_rc = nullptr;
                SerializableValue item;

                // check what the result is of item_rc -> can be combined with rc!
                BasicBlock* bDecodeOK = BasicBlock::Create(ctx, "item_" + std::to_string(i) + "_decode_ok", F);
                BasicBlock* bDecodeFail = BasicBlock::Create(ctx, "item_" + std::to_string(i) + "_decode_failed", F);

                std::tie(item_rc, item) = decodeFromArray(builder, array, index, element_type);

                auto is_item_decode_ok = builder.CreateICmpEQ(item_rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
                builder.CreateCondBr(is_item_decode_ok, bDecodeOK, bDecodeFail);

                builder.SetInsertPoint(bDecodeFail);
                badParseCause("failed to decode tuple element " + std::to_string(i) + " (" + element_type.desc() + ")");
                builder.CreateBr(_badParseBlock);

                builder.SetInsertPoint(bDecodeOK);
                // add to tuple
                ft.set(builder, {(int)i}, item.val, item.size, item.is_null);
            }

            llvm::Value* rc = builder.CreateLoad(rc_var);
            SerializableValue value;
            value.val = ft.loadToPtr(builder);
            return make_tuple(rc, value);
        }

        std::tuple<llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeList(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key,
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
            auto item_var = addArrayVar(builder);

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

            // debug print here number of elements...
            // _env.printValue(builder, num_elements, "found for type " + listType.desc() + " elements: ");

            // reserve capacity for elements
            bool initialize_elements_as_null = true; //false;
            list_reserve_capacity(_env, builder, list_ptr, listType, num_elements, initialize_elements_as_null);

            // decoding happens in a loop...
            // -> basically get the data!
            auto list_rc = generateDecodeListItemsLoop(builder, array, list_ptr, listType, num_elements);

            builder.CreateStore(list_rc, rc_var);
            // debug print, checking what the list decode gives back...
            // _env.printValue(builder, list_rc, "decode result is: ");

            // only if decode is ok, store list size!
            auto list_decode_ok = builder.CreateICmpEQ(list_rc, _env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            BasicBlock* bbListOK = BasicBlock::Create(_env.getContext(), "array_decode_ok", builder.GetInsertBlock()->getParent());
            builder.CreateCondBr(list_decode_ok, bbListOK, bbDecodeDone);

            {
                // --- set list size when ok ---
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

        std::tuple<llvm::Value *, llvm::Value *, SerializableValue>
        JSONParseRowGenerator::decodeStructDictFieldFromObject(llvm::IRBuilder<> &builder, llvm::Value *obj,
                                                               llvm::Value* key, const python::StructEntry &entry,
                                                               llvm::BasicBlock *bbSchemaMismatch) {
            using namespace std;
            using namespace llvm;

            llvm::Value* rc = nullptr;
            llvm::Value* is_present = nullptr;
            SerializableValue value;

            // checks
            assert(obj);
            assert(key);
            assert(entry.valueType.isStructuredDictionaryType()); // --> this function doesn't support nested decode.
            assert(entry.keyType == python::Type::STRING); // --> JSON decode ONLY supports string keys.

            auto value_type = entry.valueType;
            auto& ctx = _env.getContext();

            BasicBlock *bbDecodeItem = BasicBlock::Create(ctx, "decode_object", builder.GetInsertBlock()->getParent());
            BasicBlock *bbDecodeDone = BasicBlock::Create(ctx, "decode_done", builder.GetInsertBlock()->getParent());
            auto stype = _env.getOrCreateStructuredDictType(entry.valueType);
            auto value_item_var = _env.CreateFirstBlockAlloca(builder, stype); // <-- stored dict
            auto sub_object_var = addObjectVar(builder);  // <-- stores JsonObject*
            #ifdef JSON_PARSER_TRACE_MEMORY
            _env.debugPrint(builder, "Using " + pointer2hex(sub_object_var) + " in decodeStructFieldFromObject");
            #endif
            {
                auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getObject", _env.i64Type(),
                                             _env.i8ptrType(),
                                             _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));

                // zero dict (for safety)
                struct_dict_mem_zero(_env, builder, value_item_var, entry.valueType);

                auto rc_var = _env.CreateFirstBlockAlloca(builder, _env.i64Type());

                // start decode
                // create call, recurse only if ok!
                rc = builder.CreateCall(F, {obj, key, sub_object_var});
                builder.CreateStore(rc, rc_var);
                auto is_object = builder.CreateICmpEQ(rc, _env.i64Const(
                        ecToI64(ExceptionCode::SUCCESS))); // <-- indicates successful parse and that the item is present.

                is_present = builder.CreateICmpNE(rc, _env.i64Const(
                        ecToI64(ExceptionCode::KEYERROR))); // element is present iff not key error

                // // debug print:
                // _env.printValue(builder, rc, "rc for getObject: ");
                // _env.printValue(builder, is_object, "is object for key=" + entry.key);
                // _env.printValue(builder, is_present, "object present for key=" + entry.key);

                // --> only decode IFF is_ok (i.e., present and no type error (it's not an array or so)).
                builder.CreateCondBr(is_object, bbDecodeItem, bbDecodeDone);
            }

            {
                builder.SetInsertPoint(bbDecodeItem);
                // load json obj
                auto sub_object = builder.CreateLoad(sub_object_var);
                // recurse using new prefix
                // --> similar to flatten_recursive_helper(entries, kv_pair.valueType, access_path, include_maybe_structs);
                decode(builder,
                       value_item_var,
                       entry.valueType,
                       sub_object, bbSchemaMismatch, entry.valueType, {}, true);
                builder.CreateBr(bbDecodeDone); // whererver builder is, continue to decode done for this item.
            }

            builder.SetInsertPoint(bbDecodeDone);

            assert(is_present);
            assert(rc);
            value.val = builder.CreateLoad(value_item_var); // <-- loads struct!
            return make_tuple(rc, is_present, value);
        }


        std::tuple<llvm::Value*, SerializableValue> JSONParseRowGenerator::decodeOption(llvm::IRBuilder<>& builder,
                                                                 const python::Type& option_type,
                                                                 const python::StructEntry& entry,
                                                                 llvm::Value* obj,
                                                                 llvm::Value* key,
                                                                 llvm::BasicBlock* bbSchemaMismatch) {
            using namespace llvm;

            assert(option_type.isOptionType());

            auto& ctx = builder.getContext();

            llvm::Value* rc = nullptr;
            SerializableValue value;

            BasicBlock* bbCurrent = builder.GetInsertBlock();
            BasicBlock* bbDecodeIsNull = BasicBlock::Create(ctx, "decode_option_null", bbCurrent->getParent());
            BasicBlock* bbDecodeNonNull = BasicBlock::Create(ctx, "decode_option_non_null", bbCurrent->getParent());
            BasicBlock* bbDecoded = BasicBlock::Create(ctx, "decoded_option", bbCurrent->getParent());

            auto element_type = option_type.getReturnType();
            assert(!element_type.isOptionType()); // there can't be option of option...

            // special case: element to decode is struct type
            llvm::Value* null_struct = nullptr;
            if(element_type.isStructuredDictionaryType()) {
                auto llvm_element_type = _env.getOrCreateStructuredDictType(element_type);
                auto null_struct_var = _env.CreateFirstBlockAlloca(builder, llvm_element_type);
                struct_dict_mem_zero(_env, builder, null_struct_var, element_type);
                null_struct = builder.CreateLoad(null_struct_var);
            }

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

            // two cases: primitive field -> i.e., not a struct type
            if(!entryB.valueType.isStructuredDictionaryType())
                std::tie(rcB, presentB, valueB) = decodePrimitiveFieldFromObject(builder, obj, key, entryB, bbSchemaMismatch);
            else {
                // or struct type
                std::tie(rcB, presentB, valueB) = decodeStructDictFieldFromObject(builder, obj, key, entryB, bbSchemaMismatch);
            }


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
            if(valueB.val) {

                if(!entryB.valueType.isStructuredDictionaryType())
                    valueA.val = _env.nullConstant(valueB.val->getType());
                else {
                    assert(null_struct);
                    valueA.val = null_struct;
                }
            }
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

            return std::make_tuple(rc, value);
        }

        std::tuple<llvm::Value*, llvm::Value*, SerializableValue> JSONParseRowGenerator::decodePrimitiveFieldFromObject(llvm::IRBuilder<>& builder,
                                                                                                                        llvm::Value* obj,
                                                                                                                        llvm::Value* key,
                                                                                                                        const python::StructEntry& entry,
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
                std::tie(rc, value) = decodeOption(builder, value_type, entry, obj, key, bbSchemaMismatch);
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
                } else if(v_type.isTupleType() || v_type == python::Type::EMPTYTUPLE) {
                    // for another nested object, utilize:
                    std::tie(rc, value) = decodeTuple(builder, obj, key, v_type);
                } else {
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

        void JSONParseRowGenerator::decode(llvm::IRBuilder<> &builder,
                                           llvm::Value* dict_ptr,
                                           const python::Type& dict_ptr_type,
                                           llvm::Value *object,
                                           llvm::BasicBlock* bbSchemaMismatch,
                                           const python::Type &dict_type,
                                           std::vector<std::pair<std::string, python::Type>> prefix,
                                           bool include_maybe_structs) {
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
                    //logger.debug("parsing nested dict: " +
                    //             json_access_path_to_string(access_path, kv_pair.valueType, kv_pair.alwaysPresent));

                    // check if an object exists under the given key.
                    auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_getObject", _env.i64Type(),
                                                 _env.i8ptrType(),
                                                 _env.i8ptrType(), _env.i8ptrType()->getPointerTo(0));
                    auto item_var = addObjectVar(builder);

                    // do release
                    json_release_object(_env, builder, item_var);

                    #ifdef JSON_PARSER_TRACE_MEMORY
                    _env.debugPrint(builder, "Using " + pointer2hex(item_var) + " in decode");
                    #endif
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
                    } else {
                        if(kv_pair.alwaysPresent) {
                            // key MUST be present, hence it's a schema mismatch if not found.
                            BasicBlock* bKeyFound = BasicBlock::Create(ctx, "key_found", builder.GetInsertBlock()->getParent());
                            builder.CreateCondBr(is_object, bKeyFound, bbSchemaMismatch);
                            builder.SetInsertPoint(bKeyFound);
                        }
                    }

                    // create now some basic blocks to decode ON demand.
                    BasicBlock *bbDecodeItem = BasicBlock::Create(ctx, "decode_object", builder.GetInsertBlock()->getParent());
                    BasicBlock *bbDecodeDone = BasicBlock::Create(ctx, "next_item", builder.GetInsertBlock()->getParent());
                    builder.CreateCondBr(is_object, bbDecodeItem, bbDecodeDone);

                    builder.SetInsertPoint(bbDecodeItem);
                    // load item!
                    auto item = builder.CreateLoad(item_var);
#ifdef JSON_PARSER_TRACE_MEMORY
                    // debug:
                    _env.printValue(builder, item, "associated with " + pointer2hex(item_var) + " in decode is: ");
#endif
                    // recurse using new prefix
                    // --> similar to flatten_recursive_helper(entries, kv_pair.valueType, access_path, include_maybe_structs);
                    decode(builder, dict_ptr, dict_ptr_type, item, bbSchemaMismatch, kv_pair.valueType, access_path, include_maybe_structs);
                    builder.CreateBr(bbDecodeDone); // where ever builder is, continue to decode done for this item.
                    builder.SetInsertPoint(bbDecodeDone); // continue from here...
                } else {

                    // // comment this, in order to invoke the list decoding (not completed yet...) -> requires serialization!
                    // // debug: skip list for now (more complex)
                    // if(kv_pair.valueType.isListType()) {
                    //     std::cerr<<"skipping array decode with type="<<kv_pair.valueType.desc()<<" for now."<<std::endl;
                    //     continue;
                    // }

                    // basically get the entry for the kv_pair.
                    // logger.debug("generating code to decode " + json_access_path_to_string(access_path, kv_pair.valueType, kv_pair.alwaysPresent));
                    SerializableValue decoded_value;
                    llvm::Value* value_is_present = nullptr;
                    llvm::Value* rc = nullptr; // can ignore rc -> parse escapes to mismatch...
                    std::tie(rc, value_is_present, decoded_value) = decodePrimitiveFieldFromObject(builder, object, key, kv_pair, bbSchemaMismatch);

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

        llvm::Value *JSONParseRowGenerator::arraySize(llvm::IRBuilder<> &builder, llvm::Value *arr) {
            assert(arr);
            assert(arr->getType() == _env.i8ptrType());

            // call func
            using namespace llvm;
            auto &ctx = _env.getContext();

            auto F = getOrInsertFunction(_env.getModule().get(), "JsonArray_Size", _env.i64Type(),
                                         _env.i8ptrType());
            return builder.CreateCall(F, arr);
        }

        llvm::Value *JSONParseRowGenerator::numberOfKeysInObject(llvm::IRBuilder<> &builder, llvm::Value *j) {
            assert(j);

            auto F = getOrInsertFunction(_env.getModule().get(), "JsonItem_numberOfKeys", _env.i64Type(),
                                         _env.i8ptrType());
            return builder.CreateCall(F, j);
        }

        llvm::Value *JSONParseRowGenerator::decodeFieldFromObject(llvm::IRBuilder<> &builder,
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
                auto obj_var = addObjectVar(builder);
                #ifdef JSON_PARSER_TRACE_MEMORY
                _env.debugPrint(builder, "Using " + pointer2hex(obj_var) + " in decodeFieldFromObject (struct)");
                #endif
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
                    parseDict(builder, sub_obj, debug_path + ".", alwaysPresent, v_type, true, bbSchemaMismatch);
                    builder.CreateBr(bbOK);

                    // continue on ok block.
                    builder.SetInsertPoint(bbOK);
                } else {
                    builder.CreateCondBr(is_object, bbOK, bbSchemaMismatch);
                    builder.SetInsertPoint(bbOK);

                    auto sub_obj = builder.CreateLoad(obj_var);

                    // recurse...
                    parseDict(builder, sub_obj, debug_path + ".", alwaysPresent, v_type, true, bbSchemaMismatch);
                }
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
                auto obj_var = addObjectVar(builder);
                #ifdef JSON_PARSER_TRACE_MEMORY
                _env.debugPrint(builder, "Using " + pointer2hex(obj_var) + " in decodeFieldFromObject (emptydict)");
                #endif
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

//        void JSONParseRowGenerator::freeArray(llvm::Value *arr) {
//            assert(arr);
//            using namespace llvm;
//            auto &ctx = _env.getContext();
//
//            auto ptr_to_free = arr;
//
//            // free in free block (last block!)
//            assert(_freeEndBlock);
//            IRBuilder<> b(_freeEndBlock);
//
//            // what type is it?
//            if (arr->getType() == _env.i8ptrType()->getPointerTo()) {
//                ptr_to_free = b.CreateLoad(arr);
//            }
//#ifdef JSON_PARSER_TRACE_MEMORY
//            _env.printValue(b, ptr_to_free, "freeing array pointer: ");
//#endif
//            freeArray(b, ptr_to_free);
//            #ifdef JSON_PARSER_TRACE_MEMORY
//            _env.printValue(b, ptr_to_free, "--> pointer freed. ");
//            #endif
//
//            if (arr->getType() == _env.i8ptrType()->getPointerTo()) {
//                // store nullptr in debug mode
//#ifndef NDEBUG
//                b.CreateStore(_env.i8nullptr(), arr);
//#endif
//            }
//            _freeEndBlock = b.GetInsertBlock();
//            assert(_freeEndBlock);
//        }

//        void JSONParseRowGenerator::freeObject(llvm::Value *obj) {
//            assert(obj);
//            using namespace llvm;
//            auto &ctx = _env.getContext();
//
//            auto ptr_to_free = obj;
//
//            // free in free block (last block!)
//            assert(_freeEndBlock);
//            IRBuilder<> b(_freeEndBlock);
//
//            // what type is it?
//            if (obj->getType() == _env.i8ptrType()->getPointerTo()) {
//                ptr_to_free = b.CreateLoad(obj);
//            }
//
//#ifdef JSON_PARSER_TRACE_MEMORY
//             _env.printValue(b, ptr_to_free, "freeing ptr: ");
//#endif
//            json_freeObject(_env, b, ptr_to_free);
//
//            if (obj->getType() == _env.i8ptrType()->getPointerTo()) {
//                // store nullptr in debug mode
//#ifndef NDEBUG
//                b.CreateStore(_env.i8nullptr(), obj);
//#endif
//            }
//            _freeEndBlock = b.GetInsertBlock();
//            assert(_freeEndBlock);
//        }

//        void JSONParseRowGenerator::freeArray(llvm::IRBuilder<> &builder, llvm::Value *arr) {
//            using namespace llvm;
//            auto &ctx = _env.getContext();
//
//            auto Ffreeobj = getOrInsertFunction(_env.getModule().get(), "JsonArray_Free", llvm::Type::getVoidTy(ctx),
//                                                _env.i8ptrType());
//            builder.CreateCall(Ffreeobj, arr);
//        }

        void JSONParseRowGenerator::parseDict(llvm::IRBuilder<> &builder, llvm::Value *obj,
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
                }
            } else {
                // other types, parse with type check!
                throw std::runtime_error("unsupported type");
            }
        }

        llvm::Value *
        JSONParseRowGenerator::addVar(llvm::IRBuilder<> &builder, llvm::Type *type, llvm::Value *initial_value,
                                      const std::string &twine) {
            assert(type);
            // the trick here is to initialize in init block always.
            auto var = _env.CreateFirstBlockAlloca(builder, type, twine);

            if(!initial_value) {
                initial_value = _env.nullConstant(type);
            } else {
                assert(type == initial_value->getType());
            }
            assert(initial_value);

            // add storing to init block
            assert(_initBlock);
            llvm::IRBuilder<> b(_initBlock);
            b.CreateStore(initial_value, var);
            _initBlock = b.GetInsertBlock(); // update if there was complex storing.
            return var;
        }

        llvm::BasicBlock *JSONParseRowGenerator::generateFreeAllVars(llvm::BasicBlock *freeStart) {
            assert(freeStart);
            llvm::IRBuilder<> builder(freeStart);

            // got through all vars
#ifdef JSON_PARSER_TRACE_MEMORY
            _env.debugPrint(builder, "Freeing data of " + pluralize(_objectVars.size(), "object"));
            _env.debugPrint(builder, "Freeing data of " + pluralize(_arrayVars.size(), "array"));
#endif
            for(auto var : _objectVars)
                json_release_object(_env, builder, var);
            for(auto var : _arrayVars)
                json_release_array(_env, builder, var);

            return builder.GetInsertBlock();
        }
    }
}