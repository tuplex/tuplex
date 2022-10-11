//
// Created by leonhard on 9/23/22.
//

#ifndef TUPLEX_STRUCTDICTHELPER_H
#define TUPLEX_STRUCTDICTHELPER_H

#include <codegen/LLVMEnvironment.h>

namespace tuplex {
    namespace codegen {

        inline llvm::Constant *cbool_const(llvm::LLVMContext &ctx, bool b) {
            auto type = ctypeToLLVM<bool>(ctx);
            return llvm::ConstantInt::get(llvm::Type::getIntNTy(ctx, type->getIntegerBitWidth()), b);
        }

        inline bool noNeedToSerializeType(const python::Type &t) {
            // some types do not need to get serialized. This function specifies this
            if (t.isConstantValued())
                return true; // no need to serialize constants!
            if (t.isSingleValued())
                return true; // no need to serialize special constant values (like null, empty dict, empty list, empty tuple, ...)
            return false;
        }

        // recursive function to decode data, similar to flattening the type below
        // each item should be access_path | value_type | alwaysPresent |  value : SerializableValue | present : i1
        using access_path_t = std::vector<std::pair<std::string, python::Type>>;
        using flattened_struct_dict_decoded_entry_t = std::tuple<std::vector<std::pair<std::string, python::Type>>, python::Type, bool, SerializableValue, llvm::Value*>;
        using flattened_struct_dict_decoded_entry_list_t = std::vector<flattened_struct_dict_decoded_entry_t>;

        // flatten struct dict.
        using flattened_struct_dict_entry_t = std::tuple<std::vector<std::pair<std::string, python::Type>>, python::Type, bool>;
        using flattened_struct_dict_entry_list_t = std::vector<flattened_struct_dict_entry_t>;

        extern std::string access_path_to_str(const access_path_t& path);

        inline std::string json_access_path_to_string(const std::vector<std::pair<std::string, python::Type>>& path,
                                                      const python::Type& value_type,
                                                      bool always_present) {
            std::stringstream ss;
            // first the path:
            ss<<access_path_to_str(path)<<" -> ";
            auto presence = !always_present ? "  (maybe)" : "";
            auto v_type = value_type;
            auto value_desc = v_type.isStructuredDictionaryType() ? "Struct[...]" : v_type.desc();
            ss << value_desc << presence;
            return ss.str();
        }

        extern void flatten_recursive_helper(flattened_struct_dict_entry_list_t &entries,
                                             const python::Type &dict_type,
                                             std::vector<std::pair<std::string, python::Type>> prefix = {},
                                             bool include_maybe_structs = true);

        extern void print_flatten_structured_dict_type(const python::Type &dict_type);

        extern llvm::Type *generate_structured_dict_type(LLVMEnvironment &env, const std::string &name, const python::Type &dict_type);

        inline llvm::Type *create_structured_dict_type(LLVMEnvironment &env, const python::Type &dict_type) {
            return env.getOrCreateStructuredDictType(dict_type);
        }

        extern std::vector<llvm::Value*> create_bitmap(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const std::vector<llvm::Value*>& v);

        extern std::unordered_map<access_path_t, std::tuple<int, int, int, int>> struct_dict_load_indices(const python::Type& dict_type);

        extern SerializableValue struct_dict_load_from_values(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const python::Type& dict_type, flattened_struct_dict_decoded_entry_list_t entries, llvm::Value* ptr);

        extern void struct_dict_store_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* size);
        extern void struct_dict_store_isnull(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_null);
        extern void struct_dict_store_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* value);
        void struct_dict_store_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_present);

        extern SerializableValue struct_dict_serialized_memory_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type);

        extern llvm::Value* serializeBitmap(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* bitmap, llvm::Value* dest_ptr);

        extern void struct_dict_mem_zero(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type);

        SerializableValue struct_dict_serialize_to_memory(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, llvm::Value* dest_ptr);

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

        /*!
         * retrieve the element type under access path.
         * @param dict_type
         * @param path
         * @return element type or UNKNOWN if access path is not found.
         */
        extern python::Type struct_dict_type_get_element_type(const python::Type& dict_type, const access_path_t& path);

        extern llvm::Value* struct_dict_load_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path);
        extern SerializableValue struct_dict_load_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path);
    }
}

#endif //TUPLEX_STRUCTDICTHELPER_H
