//
// Created by leonhard on 10/13/22.
//

#ifndef TUPLEX_STRUCTCOMMON_H
#define TUPLEX_STRUCTCOMMON_H

#include "Base.h"
#include <unordered_map>
#include <iostream>
#include "TypeSystem.h"
#include "Utils.h"
#include "TypeHelper.h"

// holds helper structures to decode struct dicts from memory e.g.
namespace tuplex {

    // typedefs for struct types
    // recursive function to decode data, similar to flattening the type below
    // each item should be access_path | value_type | alwaysPresent |  value : SerializableValue | present : i1
    using access_path_t = std::vector<std::pair<std::string, python::Type>>;

    // flatten struct dict.
    using flattened_struct_dict_entry_t = std::tuple<std::vector<std::pair<std::string, python::Type>>, python::Type, bool>;
    using flattened_struct_dict_entry_list_t = std::vector<flattened_struct_dict_entry_t>;

    inline bool noNeedToSerializeType(const python::Type &t) {
        // some types do not need to get serialized. This function specifies this
        if (t.isConstantValued())
            return true; // no need to serialize constants!
        if (t.isSingleValued())
            return true; // no need to serialize special constant values (like null, empty dict, empty list, empty tuple, ...)
        return false;
    }

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

    extern void print_flatten_structured_dict_type(const python::Type &dict_type, std::ostream& os=std::cout);

    extern bool struct_dict_field_present(const std::unordered_map<access_path_t, std::tuple<int, int, int, int>>& indices,
                                          const access_path_t& path, const std::vector<uint64_t>& presence_map);

    extern std::unordered_map<access_path_t, std::tuple<int, int, int, int>> struct_dict_load_indices(const python::Type& dict_type);

    extern bool struct_dict_has_bitmap(const python::Type& dict_type);
    extern bool struct_dict_has_presence_map(const python::Type& dict_type);

    /*!
     * retrieve the element type under access path.
     * @param dict_type
     * @param path
     * @return element type or UNKNOWN if access path is not found.
     */
    extern python::Type struct_dict_type_get_element_type(const python::Type& dict_type, const access_path_t& path);

    extern void retrieve_bitmap_counts(const flattened_struct_dict_entry_list_t& entries, size_t& bitmap_element_count, size_t& presence_map_element_count);
}

#endif //TUPLEX_STRUCTCOMMON_H
