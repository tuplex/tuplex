//
// Created by leonhard on 9/23/22.
//

#ifndef TUPLEX_STRUCTDICTHELPER_H
#define TUPLEX_STRUCTDICTHELPER_H

#include <StructCommon.h>
#include <codegen/LLVMEnvironment.h>

namespace tuplex {
    namespace codegen {

        inline llvm::Constant *cbool_const(llvm::LLVMContext &ctx, bool b) {
            auto type = ctypeToLLVM<bool>(ctx);
            return llvm::ConstantInt::get(llvm::Type::getIntNTy(ctx, type->getIntegerBitWidth()), b);
        }

        using flattened_struct_dict_decoded_entry_t = std::tuple<std::vector<std::pair<std::string, python::Type>>, python::Type, bool, SerializableValue, llvm::Value*>;
        using flattened_struct_dict_decoded_entry_list_t = std::vector<flattened_struct_dict_decoded_entry_t>;

        extern llvm::Type *generate_structured_dict_type(LLVMEnvironment &env, const std::string &name, const python::Type &dict_type);

        inline llvm::Type *create_structured_dict_type(LLVMEnvironment &env, const python::Type &dict_type) {
            return env.getOrCreateStructuredDictType(dict_type);
        }

        extern std::vector<llvm::Value*> create_bitmap(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const std::vector<llvm::Value*>& v);
        extern SerializableValue struct_dict_load_from_values(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const python::Type& dict_type, flattened_struct_dict_decoded_entry_list_t entries, llvm::Value* ptr);

        extern void struct_dict_store_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* size);
        extern void struct_dict_store_isnull(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_null);
        extern void struct_dict_store_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* value);
        void struct_dict_store_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_present);

        extern SerializableValue struct_dict_serialized_memory_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type);

        extern llvm::Value* serializeBitmap(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* bitmap, llvm::Value* dest_ptr);

        extern void struct_dict_mem_zero(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type);

        extern SerializableValue struct_dict_serialize_to_memory(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, llvm::Value* dest_ptr);

        extern SerializableValue struct_dict_deserialize_from_memory(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type);

        extern void struct_dict_verify_storage(LLVMEnvironment& env, const python::Type& dict_type, std::ostream& os);

        extern size_t struct_dict_heap_size(LLVMEnvironment& env, const python::Type& dict_type);

        /*!
         * retrieve value stored under key from the structured dictionary. If it doesn't exist, go to bbKeyNotFound.
         * @param env
         * @param dict_type
         * @param key
         * @param ptr
         * @param bbKeyNotFound
         * @return the value (or dummies)
         */
        extern SerializableValue struct_dict_get_or_except(LLVMEnvironment& env,
                                                           llvm::IRBuilder<>& builder,
                                                           const python::Type& dict_type,
                                                           const std::string& key,
                                                           const python::Type& key_type,
                                                           llvm::Value* ptr,
                                                           llvm::BasicBlock* bbKeyNotFound);

        extern llvm::Value* struct_dict_load_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path);
        extern SerializableValue struct_dict_load_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path);

        extern SerializableValue struct_dict_upcast(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const SerializableValue& src, const python::Type& src_type, const python::Type& dest_type);
    }
}

#endif //TUPLEX_STRUCTDICTHELPER_H
