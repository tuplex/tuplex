//
// Created by leonhard on 9/25/22.
//

#ifndef TUPLEX_JSONPARSEROWGENERATOR_H
#define TUPLEX_JSONPARSEROWGENERATOR_H

#include <codegen/LLVMEnvironment.h>
#include <CodegenHelper.h>

#include <experimental/ListHelper.h>
#include <experimental/StructDictHelper.h>
#include <physical/experimental/JsonHelper.h>

namespace tuplex {
    namespace codegen {

        struct JSONDecodeOptions {
            bool verifyExactKeySetMatch;
            bool unwrap_first_leve_to_tuple;
        };

        inline void json_freeObject(LLVMEnvironment& env, llvm::IRBuilder<> &builder, llvm::Value *obj) {
            using namespace llvm;
            auto &ctx = env.getContext();

            auto Ffreeobj = getOrInsertFunction(env.getModule().get(), "JsonItem_Free", llvm::Type::getVoidTy(ctx),
                                                env.i8ptrType());
            builder.CreateCall(Ffreeobj, obj);
        }

        inline void json_freeArray(LLVMEnvironment& env, llvm::IRBuilder<> &builder, llvm::Value *obj) {
            using namespace llvm;
            auto &ctx = env.getContext();

            auto Ffreearr = getOrInsertFunction(env.getModule().get(), "JsonArray_Free", llvm::Type::getVoidTy(ctx),
                                                env.i8ptrType());
            builder.CreateCall(Ffreearr, obj);
        }

        // free on demand
        inline void json_release_object(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* obj_var) {
            using namespace llvm;
            assert(obj_var && obj_var->getType() == env.i8ptrType()->getPointerTo());

            // generate code for the following:
            // if(obj != nullptr)
            //    JsonItem_Free(obj)
            // obj = nullptr
            auto is_null = builder.CreateICmpEQ(builder.CreateLoad(obj_var), env.i8nullptr());
            BasicBlock* bFree = BasicBlock::Create(env.getContext(), "free_object", builder.GetInsertBlock()->getParent());
            BasicBlock* bContinue = BasicBlock::Create(env.getContext(), "null_object", builder.GetInsertBlock()->getParent());
            builder.CreateCondBr(is_null, bContinue, bFree);
            builder.SetInsertPoint(bFree);

            // call free func
            json_freeObject(env, builder, builder.CreateLoad(obj_var));

            builder.CreateBr(bContinue);
            builder.SetInsertPoint(bContinue);
            builder.CreateStore(env.i8nullptr(), obj_var);
        }

        inline void json_release_array(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* arr_var) {
            using namespace llvm;
            assert(arr_var && arr_var->getType() == env.i8ptrType()->getPointerTo());

            // generate code for the following:
            // if(obj != nullptr)
            //    JsonItem_Free(obj)
            // obj = nullptr
            auto is_null = builder.CreateICmpEQ(builder.CreateLoad(arr_var), env.i8nullptr());
            BasicBlock* bFree = BasicBlock::Create(env.getContext(), "free_array", builder.GetInsertBlock()->getParent());
            BasicBlock* bContinue = BasicBlock::Create(env.getContext(), "null_array", builder.GetInsertBlock()->getParent());
            builder.CreateCondBr(is_null, bContinue, bFree);
            builder.SetInsertPoint(bFree);

            // call free func
            json_freeArray(env, builder, builder.CreateLoad(arr_var));

            builder.CreateBr(bContinue);
            builder.SetInsertPoint(bContinue);
            builder.CreateStore(env.i8nullptr(), arr_var);
        }

        class JSONParseRowGenerator {
        public:
            JSONParseRowGenerator(LLVMEnvironment& env,
                                  const python::Type& rowType,
                                  llvm::BasicBlock* bBadParse,
                                  JSONDecodeOptions={}) : _env(env), _rowType(rowType),
                                  _initBlock(nullptr), _afterInitBlock(nullptr), _badParseTargetBlock(bBadParse) {
                // generate free before badparse
                using namespace llvm;
                auto& ctx = env.getContext();
                assert(bBadParse);
                _badParseBlock = BasicBlock::Create(ctx, "free_before_badparse", bBadParse->getParent());
                // lazy generate (b.c. vars are not known yet!)
            }

            inline void parseToVariable(llvm::IRBuilder<>& builder, llvm::Value* object, llvm::Value* row_var) {
                using namespace llvm;

                if(_initBlock || _afterInitBlock)
                    throw std::runtime_error("call function only once!");

                // create init block from builder & then generate everything.
                auto& ctx = builder.getContext();
                auto parentF = builder.GetInsertBlock()->getParent();
                _initBlock = BasicBlock::Create(ctx, "init_json_parse", parentF);
                _afterInitBlock = BasicBlock::Create(ctx, "start_json_parse", parentF);
                builder.CreateBr(_initBlock);

                // set insert to after init block
                builder.SetInsertPoint(_afterInitBlock);

                // decode everything -> entries can be then used to store to a struct!
                decode(builder, row_var, _rowType, object, _badParseBlock, _rowType, {}, true);

                // generate free blocks and update builder
                auto lastFreeBlock = generateFreeAllVars(builder.GetInsertBlock());
                builder.SetInsertPoint(lastFreeBlock);

                // connect init to after init block
                IRBuilder<> b(_initBlock);
                b.CreateBr(_afterInitBlock);

                // call to create free sequence before bad parse is hit.
                lazyGenFreeAll();
            }

            /*!
             * generate code to free all allocated variables during a parse. (should be done after row is successfully parsed AND when it's a bad parse)
             * @param freeStart block onto which to attach free instructions
             * @return last block of the free sequence.
             */
            llvm::BasicBlock* generateFreeAllVars(llvm::BasicBlock* freeStart);

        private:
            LLVMEnvironment& _env;
            python::Type _rowType;

            llvm::BasicBlock* _badParseBlock;
            llvm::BasicBlock* _badParseTargetBlock; // where to end after bad parse happened!

            llvm::BasicBlock* _initBlock; // first block, where to add any init code
            llvm::BasicBlock* _afterInitBlock; // block after init (needs to be connected!)

            // helper functions
            void decode(llvm::IRBuilder<>& builder,
                        llvm::Value* dict_ptr,
                        const python::Type& dict_ptr_type, // <- the type of the top-level project where to store stuff
                        llvm::Value* object,
                        llvm::BasicBlock* bbSchemaMismatch,
                        const python::Type &dict_type, // <-- the type of object (which must be a structured dict)
                        std::vector<std::pair<std::string, python::Type>> prefix = {},
                        bool include_maybe_structs = true);


            // used for generating free blocks...
            std::vector<llvm::Value*> _objectVars;
            std::vector<llvm::Value*> _arrayVars;

            // use this instead of CreateFirstBlockAlloca/CreateFirstBlockVariable
            llvm::Value* addVar(llvm::IRBuilder<>& builder, llvm::Type* type, llvm::Value* initial_value=nullptr, const std::string& twine="");
//            inline llvm::Value* addI8PtrVar(llvm::IRBuilder<>& builder) {
//                return addVar(builder, _env.i8ptrType(), _env.i8nullptr());
//            }

            // array/object vars (incl. free)
            llvm::Value* addArrayVar(llvm::IRBuilder<>& builder) {
                auto var = addVar(builder, _env.i8ptrType(), _env.i8nullptr());
                _arrayVars.push_back(var);
                // add array free to step after parse row
                //freeArray(var);
                return var;
            }

            llvm::Value* addObjectVar(llvm::IRBuilder<>& builder) {
                auto var = addVar(builder, _env.i8ptrType(), _env.i8nullptr());
                _objectVars.push_back(var);
                // add array free to step after parse row
                //freeObject(var);
                return var;
            }

            std::tuple<llvm::Value*, llvm::Value*, SerializableValue> decodePrimitiveFieldFromObject(llvm::IRBuilder<>& builder,
                                                                                                     llvm::Value* obj,
                                                                                                     llvm::Value* key,
                                                                                                     const python::StructEntry& entry,
                                                                                                     llvm::BasicBlock *bbSchemaMismatch);

            std::tuple<llvm::Value*, llvm::Value*, SerializableValue> decodeStructDictFieldFromObject(llvm::IRBuilder<>& builder,
                                                                                                      llvm::Value* obj,
                                                                                                      llvm::Value* key,
                                                                                                      const python::StructEntry& entry,
                                                                                                      llvm::BasicBlock *bbSchemaMismatch);


            // various decoding functions (object)
            std::tuple<llvm::Value*, SerializableValue> decodeString(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeBoolean(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeI64(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeF64(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeEmptyDict(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeNull(llvm::IRBuilder<> &builder, llvm::Value *obj, llvm::Value *key);
            std::tuple<llvm::Value*, SerializableValue> decodeOption(llvm::IRBuilder<>& builder,
                                                                     const python::Type& option_type,
                                                                     const python::StructEntry& entry,
                                                                     llvm::Value* obj,
                                                                     llvm::Value* key,
                                                                     llvm::BasicBlock* bbSchemaMismatch);

            // similarly, decoding functions (array)
            std::tuple<llvm::Value*, SerializableValue> decodeFromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index, const python::Type& element_type);
            std::tuple<llvm::Value*, SerializableValue> decodeNullFromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index);
            std::tuple<llvm::Value*, SerializableValue> decodeBooleanFromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index);
            std::tuple<llvm::Value*, SerializableValue> decodeI64FromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index);
            std::tuple<llvm::Value*, SerializableValue> decodeF64FromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index);
            std::tuple<llvm::Value*, SerializableValue> decodeStringFromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index);
            std::tuple<llvm::Value*, SerializableValue> decodeObjectFromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index, const python::Type& dict_type);
            std::tuple<llvm::Value*, SerializableValue> decodeTupleFromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index, const python::Type& tuple_type, bool store_as_heap_ptr);
            std::tuple<llvm::Value*, SerializableValue> decodeOptionFromArray(llvm::IRBuilder<>& builder,
                                                                              const python::Type& option_type,
                                                                              llvm::Value* array,
                                                                              llvm::Value* index);
            std::tuple<llvm::Value*, SerializableValue> decodeEmptyListFromArray(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* index);
            std::tuple<llvm::Value*, SerializableValue> decodeListFromArray(llvm::IRBuilder<>& builder,
                                                                            const python::Type& list_type,
                                                                            llvm::Value* array,
                                                                            llvm::Value* index);

            // complex compound types
            std::tuple<llvm::Value*, SerializableValue> decodeEmptyList(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value* key);
            std::tuple<llvm::Value*, SerializableValue> decodeList(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value *key, const python::Type& listType);
            std::tuple<llvm::Value*, SerializableValue> decodeTuple(llvm::IRBuilder<>& builder, llvm::Value* obj, llvm::Value *key, const python::Type& tupleType);

            // helper function to create the loop for the array
            llvm::Value* generateDecodeListItemsLoop(llvm::IRBuilder<>& builder, llvm::Value* array, llvm::Value* list_ptr, const python::Type& list_type, llvm::Value* num_elements);

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

//            void freeObject(llvm::Value *obj);
//            void freeArray(llvm::IRBuilder<> &builder, llvm::Value *arr);
//            void freeArray(llvm::Value *arr);
            llvm::Value* arraySize(llvm::IRBuilder<>& builder, llvm::Value* arr);

            llvm::Value *numberOfKeysInObject(llvm::IRBuilder<> &builder, llvm::Value *j);

            inline void badParseCause(const std::string& cause) {
                // helper function, called to describe cause. probably useful later...
            }


            void parseDict(llvm::IRBuilder<> &builder, llvm::Value *obj,
                           const std::string &debug_path, bool alwaysPresent,
                           const python::Type &t, bool check_that_all_keys_are_present,
                           llvm::BasicBlock *bbSchemaMismatch);

            inline void lazyGenFreeAll() {
                assert(_badParseBlock && _badParseTargetBlock);
                llvm::IRBuilder<> builder(_badParseBlock);
                auto lastBlock = generateFreeAllVars(_badParseBlock);
                builder.SetInsertPoint(lastBlock);
                builder.CreateBr(_badParseTargetBlock);
            }
        };

    }
}
#endif //TUPLEX_JSONPARSEROWGENERATOR_H
