//
// Created by leonhard on 9/25/22.
//

#ifndef TUPLEX_JSONPARSEROWGENERATOR_H
#define TUPLEX_JSONPARSEROWGENERATOR_H

#include <LLVMEnvironment.h>
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

        class JSONParseRowGenerator {
        public:
            JSONParseRowGenerator(LLVMEnvironment& env,
                                  const python::Type& rowType,
                                  llvm::BasicBlock* bFreeBlock,
                                  llvm::BasicBlock* bBadParse,
                                  JSONDecodeOptions={}) : _env(env), _rowType(rowType), _freeStartBlock(bFreeBlock),
                                  _freeEndBlock(bFreeBlock), _badParseBlock(bBadParse),
                                  _initBlock(nullptr), _afterInitBlock(nullptr) {

            }

            llvm::BasicBlock* freeBlockEnd() const { return _freeEndBlock; }

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

                // connect init to after init block
                IRBuilder<> b(_initBlock);
                b.CreateBr(_afterInitBlock);
            }

        private:
            LLVMEnvironment& _env;
            python::Type _rowType;

            llvm::BasicBlock* _freeStartBlock;
            llvm::BasicBlock* _freeEndBlock;
            llvm::BasicBlock* _badParseBlock;

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

            // use this instead of CreateFirstBlockAlloca/CreateFirstBlockVariable
            llvm::Value* addVar(llvm::IRBuilder<>& builder, llvm::Type* type, llvm::Value* initial_value=nullptr, const std::string& twine="");
//            inline llvm::Value* addI8PtrVar(llvm::IRBuilder<>& builder) {
//                return addVar(builder, _env.i8ptrType(), _env.i8nullptr());
//            }

            // array/object vars (incl. free)
            llvm::Value* addArrayVar(llvm::IRBuilder<>& builder) {
                auto var = addVar(builder, _env.i8ptrType(), _env.i8nullptr());
                // add array free to step after parse row
                freeArray(var);
                return var;
            }
            llvm::Value* addObjectVar(llvm::IRBuilder<>& builder) {
                auto var = addVar(builder, _env.i8ptrType(), _env.i8nullptr());
                // add array free to step after parse row
                freeObject(var);
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

            void freeObject(llvm::Value *obj);
            void freeArray(llvm::IRBuilder<> &builder, llvm::Value *arr);
            void freeArray(llvm::Value *arr);
            llvm::Value* arraySize(llvm::IRBuilder<>& builder, llvm::Value* arr);

            llvm::Value *numberOfKeysInObject(llvm::IRBuilder<> &builder, llvm::Value *j);

            inline void badParseCause(const std::string& cause) {
                // helper function, called to describe cause. probably useful later...
            }


            void parseDict(llvm::IRBuilder<> &builder, llvm::Value *obj,
                           const std::string &debug_path, bool alwaysPresent,
                           const python::Type &t, bool check_that_all_keys_are_present,
                           llvm::BasicBlock *bbSchemaMismatch);
        };

    }
}
#endif //TUPLEX_JSONPARSEROWGENERATOR_H
