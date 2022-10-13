//
// Created by leonhard on 10/13/22.
//

#include <StructCommon.h>

namespace tuplex {

    std::string access_path_to_str(const access_path_t& path) {
        std::stringstream ss;
        if(path.empty())
            return "*";
        for(unsigned i = 0; i < path.size(); ++i) {
            auto atom = path[i];
            ss << atom.first << " (" << atom.second.desc() << ")";
            if(i != path.size() -1)
                ss<<" -> ";
        }
        return ss.str();
    }

    void flatten_recursive_helper(flattened_struct_dict_entry_list_t &entries,
                                  const python::Type &dict_type,
                                  std::vector<std::pair<std::string, python::Type>> prefix,
                                  bool include_maybe_structs) {
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
            } else if(kv_pair.valueType.isOptionType() && kv_pair.valueType.getReturnType().isStructuredDictionaryType()) {
                entries.push_back(make_tuple(access_path, kv_pair.valueType, kv_pair.alwaysPresent));

                // recurse element field!
                flatten_recursive_helper(entries, kv_pair.valueType.getReturnType(), access_path, include_maybe_structs);
            } else {
                entries.push_back(make_tuple(access_path, kv_pair.valueType, kv_pair.alwaysPresent));
            }
        }
    }

    void print_flatten_structured_dict_type(const python::Type &dict_type, std::ostream& os) {
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

        os << ss.str() << endl;
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

    bool struct_dict_field_present(const std::unordered_map<access_path_t, std::tuple<int, int, int, int>>& indices,
                                   const access_path_t& path, const std::vector<uint64_t>& presence_map) {
        assert(!path.empty());

        // get present map index of current element.
        auto it = indices.find(path);
        // if not found, it's an always present (parent path)
        if(it == indices.end())
            return true;

        auto t = indices.at(path); // must exist.
        auto present_idx = std::get<1>(t);

        bool is_present = true;
        // >= 0? -> entry in bitmap exists.
        if(present_idx >= 0) {
            // make sure indices are valid
            auto el_idx = present_idx / 64;
            auto bit_idx = present_idx % 64;
            assert(el_idx < presence_map.size());
            is_present = (presence_map[el_idx] & (0x1ull << bit_idx));
        }

        // not present? -> return
        if(!is_present)
            return false;

        // else, check if parent is present
        if(path.size() > 1) {
            access_path_t parent_path(path.begin(), path.end() - 1);
            return struct_dict_field_present(indices, parent_path, presence_map);
        } else {
            assert(is_present);
            return is_present; // should be true here...
        }
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

            // helpful for debugging.
            auto path = json_access_path_to_string(access_path, value_type, always_present);

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

    python::Type struct_dict_type_get_element_type(const python::Type& dict_type, const access_path_t& path) {

        if(!dict_type.isStructuredDictionaryType())
            return python::Type::UNKNOWN;

        // fetch the type
        assert(dict_type.isStructuredDictionaryType());
        if(path.empty())
            return dict_type;

        auto p = path.front();
        // this is done recursively
        for(const auto& kv_pair : dict_type.get_struct_pairs()) {
            // compare
            if(p.second == kv_pair.keyType
               && semantic_python_value_eq(p.second, p.first, kv_pair.key)) {
                // match -> recurse!
                if(path.size() == 1)
                    return kv_pair.valueType;
                else {
                    assert(path.size() >= 2);
                    auto suffix_path = access_path_t(path.begin() + 1, path.end());

                    // special case: option[struct[...]] => search the non-option type
                    auto value_type = kv_pair.valueType;
                    if(value_type.isOptionType())
                        value_type = value_type.getReturnType();
                    return struct_dict_type_get_element_type(value_type, suffix_path);
                }
            }
        }
        return python::Type::UNKNOWN; // not found.
    }
}