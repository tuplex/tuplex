//
// Created by leonhard on 10/13/22.
//

#include <StructCommon.h>
#include <JSONUtils.h>

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

    class StrJsonTree {
    public:
        StrJsonTree() {}

        void add(const std::vector<std::string>& keys, const std::string& data) {
            if(keys.empty()) {
                _data = data;
                return;
            }

            auto key = keys.front();

            // key
            auto it = _children.find(key);
            if(it == _children.end()) {
                // add new child
                _children[key] = std::make_unique<StrJsonTree>();
            }
            it = _children.find(key);
            assert(it != _children.end());
            // modify child
            it->second->add(std::vector<std::string>(keys.begin() + 1, keys.end()), data);
        }

        // dump as json
        std::string to_json() const {
            std::stringstream ss;
            to_json(ss);
            return ss.str();
        }

        void to_json(std::ostream& os) const {
            // no children? simple
            if(_children.empty()) {
                os<<_data;
            } else {
                // children? struct
                os<<"{";
                unsigned i = 0;
                for(const auto& child : _children) {
                    os<<child.first<<":";
                    child.second->to_json(os);
                    if(i != _children.size() - 1)
                        os<<",";
                    ++i;
                }
                os<<"}";
            }
        }
    private:
        std::unordered_map<std::string, std::unique_ptr<StrJsonTree>> _children;
        std::string _data;
    };

    std::vector<std::string> access_path_to_json_keys(const access_path_t& path) {
        std::vector<std::string> keys;
        for(auto atom : path) {
            // what is the type?
            if(atom.second != python::Type::STRING)
                throw std::runtime_error("key type that is not string not yet supported in encoding.");

            // assume it's string -> the key is python str
            auto str_value = str_value_from_python_raw_value(atom.first);
            // escape to json string
            keys.push_back( escape_for_json(str_value));
        }
        return keys;
    }

    std::string decodeListAsJSON(const python::Type& list_type, const uint8_t* buf, size_t buf_size) {

        assert(list_type.isListType());

        // any list has in its first field the number of elements.
        int64_t num_elements = *(int64_t*)buf;

        // check what the element type is
        auto element_type = list_type.elementType();
        if(element_type.isListType()) {
            // recuse
            std::stringstream ss;
            ss<<"[";
            auto ptr = buf + sizeof(int64_t);
            for(unsigned i = 0; i < num_elements; ++i) {
                // fetch offset and size
                auto info = *(uint64_t*)ptr;

                uint32_t offset = info & 0xFFFFFFFF;
                uint32_t size = info >> 32u;

                // get start and decode
                ss<<decodeListAsJSON(element_type, ptr + offset, size);
                if(i != num_elements - 1)
                    ss<<",";

                ptr += sizeof(int64_t);
            }
            ss<<"]";
            return ss.str();
        } else if(element_type == python::Type::STRING) {
            std::stringstream ss;
            ss<<"[";
            auto ptr = buf + sizeof(int64_t);
            for(unsigned i = 0; i < num_elements; ++i) {
                // fetch offset and size
                auto info = *(uint64_t*)ptr;

                uint32_t offset = info & 0xFFFFFFFF;
                uint32_t size = info >> 32u;

                // get start and decode
                std::string str((const char*)(ptr + offset));
                ss<<escape_for_json(str);
                if(i != num_elements - 1)
                    ss<<",";

                ptr += sizeof(int64_t);
            }
            ss<<"]";
            return ss.str();
        } else {
            throw std::runtime_error("unsupported element type " + element_type.desc() + " in decodeListAsJSON");
        }
        return "[]"; // empty list as dummy default.
    }

    std::string decodeStructDictFromBinary(const python::Type& dict_type, const uint8_t* buf, size_t buf_size) {
        assert(dict_type.isStructuredDictionaryType());

        flattened_struct_dict_entry_list_t entries;
        flatten_recursive_helper(entries, dict_type, {});
        auto indices = struct_dict_load_indices(dict_type);

        // retrieve counts => i.e. how many fields are options? how many are maybe present?
        size_t field_count = 0, option_count = 0, maybe_count = 0;

        for (auto entry: entries) {
            bool is_always_present = std::get<2>(entry);
            maybe_count += !is_always_present;
            bool is_value_optional = std::get<1>(entry).isOptionType();
            option_count += is_value_optional;

            bool is_struct_type = std::get<1>(entry).isStructuredDictionaryType();
            field_count += !is_struct_type; // only count non-struct dict fields. -> yet the nested struct types may change the maybe count for the bitmap!
        }

        size_t num_option_bitmap_bits = option_count; // multiples of 64bit
        size_t num_maybe_bitmap_bits = maybe_count;
        size_t num_option_bitmap_elements = core::ceilToMultiple(option_count, 64ul) / 64ul;
        size_t num_maybe_bitmap_elements = core::ceilToMultiple(maybe_count, 64ul) / 64ul;

        std::vector<uint64_t> bitmap;
        std::vector<uint64_t> presence_map;
        if(num_option_bitmap_elements > 0) {
            bitmap = std::vector<uint64_t>(num_option_bitmap_elements, 0);
        }
        if(num_maybe_bitmap_elements > 0) {
            presence_map = std::vector<uint64_t>(num_maybe_bitmap_elements, 0);
        }

        // start decode:
        auto ptr = buf;
        size_t remaining_bytes = buf_size;

        // check if dict type has bitmaps, if so decode!
        if(struct_dict_has_bitmap(dict_type)) {
            // decode
            auto size_to_decode = sizeof(uint64_t) * num_option_bitmap_elements;
            assert(remaining_bytes >= size_to_decode);
            memcpy(&bitmap[0], ptr, size_to_decode);
            ptr += size_to_decode;
            remaining_bytes -= size_to_decode;
        }
        if(struct_dict_has_presence_map(dict_type)) {
            auto size_to_decode = sizeof(uint64_t) * num_maybe_bitmap_elements;
            assert(remaining_bytes >= size_to_decode);
            memcpy(&presence_map[0], ptr, size_to_decode);
            ptr += size_to_decode;
            remaining_bytes -= size_to_decode;

            // // go over elements and print bit
            // for(unsigned i = 0; i < presence_map.size(); ++i) {
            //     for(unsigned j = 0; j < 64; ++j) {
            //         if(presence_map[i] & (0x1ull << j))
            //             std::cout<<"presence bit "<<j + 64 * i<<" set."<<std::endl;
            //     }
            // }

            // special case: presence map all 0s -> return {}
            bool all_zero = true;
            for(auto p_el : presence_map)
                if(p_el)
                    all_zero =false;
            if(all_zero)
                return "{}";
        }

        // go through entries & decode!
        // get indices to properly decode
        // this func is basically modeled after struct_dict_serialize_to_menory
        unsigned field_index = 0;

        std::unordered_map<access_path_t, std::string> elements;

        auto tree = std::make_unique<StrJsonTree>();

        for(auto entry : entries) {
            auto access_path = std::get<0>(entry);
            auto value_type = std::get<1>(entry);
            auto always_present = std::get<2>(entry);
            auto t_indices = indices.at(access_path);

            auto path_str = json_access_path_to_string(access_path, value_type, always_present);

            int bitmap_idx = -1, present_idx = -1, value_idx = -1, size_idx = -1;
            std::tie(bitmap_idx, present_idx, value_idx, size_idx) = t_indices;

            assert(field_index < field_count);
            auto field_ptr = ptr + sizeof(uint64_t) * field_index;

            bool is_null = false;
            bool is_present = struct_dict_field_present(indices, access_path, presence_map);

            // std::cout<<"decoding "<<path_str<<" from field= "<<field_index<<" present= "<<std::boolalpha<<is_present<<std::endl;

            // if not present, do not decode. But do inc field_index!
            if(!is_present) {
                if(value_type.isOptionType())
                    value_type = value_type.getReturnType();
                if(value_type.isStructuredDictionaryType() || value_idx < 0) {
                    // nothing todo...
                } else {
                    field_index++;
                }
                continue;
            }

            // special case null: (--> same applies for constants as well...)
            if(value_type == python::Type::NULLVALUE) {
                elements[access_path] = "null";
                tree->add(access_path_to_json_keys(access_path), "null");
                continue;
            }

            // decode...
            if(value_type.isOptionType()) {
                assert(bitmap_idx >= 0);
                assert(!bitmap.empty());
                // check if null or not
                auto el_idx = bitmap_idx / 64;
                auto bit_idx = bitmap_idx % 64;
                is_null = bitmap[el_idx] & (0x1ull << bit_idx);
                if(is_null)
                    elements[access_path] = "null";
                value_type = value_type.getReturnType();
            }

            if(python::Type::EMPTYLIST != value_type && value_type.isListType()) {
                // special case list.

                // check size & offset
                uint32_t offset = *((uint64_t*)field_ptr) & 0xFFFFFFFF;
                uint32_t size = *((uint64_t*)field_ptr) >> 32u;

                // @TODO. for now, save empty list
                elements[access_path] = "[]";
                tree->add(access_path_to_json_keys(access_path), decodeListAsJSON(value_type, field_ptr + offset - sizeof(int64_t), size));

                field_index++;
                continue;
            }

            // skip nested struct dicts! --> they're taken care of.
            if(value_type.isStructuredDictionaryType())
                continue;

            // depending on field, add size!
            if(value_idx < 0) {
                // what's the type? add dummy field to string!
                if(!is_null)
                    throw std::runtime_error("decode type " + value_type.desc());
                continue; // can skip field, not necessary to serialize
            }

            // what kind of data is it that needs to be serialized?
            bool is_varlength_field = size_idx >= 0;
            assert(value_idx >= 0);

            if(!is_varlength_field) {
                // either double or i64 or bool
                if(value_type == python::Type::BOOLEAN) {
                    auto i = *(int64_t*)field_ptr;
                    if(!is_null) {
                        elements[access_path] = i != 0 ? "true" : "false";
                        tree->add(access_path_to_json_keys(access_path), i != 0 ? "true" : "false");
                    }
                } else if(value_type == python::Type::I64) {
                    auto i = *(int64_t*)field_ptr;
                    if(!is_null) {
                        elements[access_path] = std::to_string(i);
                        tree->add(access_path_to_json_keys(access_path), std::to_string(i));
                    }
                } else if(value_type == python::Type::F64) {
                    auto d = *(double*)field_ptr;
                    if(!is_null) {
                        elements[access_path] = std::to_string(d);
                        tree->add(access_path_to_json_keys(access_path), std::to_string(d));
                    }
                } else {
                    throw std::runtime_error("can't decode primitive field " + value_type.desc());
                }
            } else {
                // should be tuple or string or pyobject?
                if(value_type != python::Type::STRING)
                    throw std::runtime_error("unsupported type " + value_type.desc() + " encountered! ");

                // check size & offset
                uint32_t offset = *((uint64_t*)field_ptr) & 0xFFFFFFFF;
                uint32_t size = *((uint64_t*)field_ptr) >> 32u;

                if(!is_null) {
                    auto start_ptr = (char*)(field_ptr + offset - sizeof(int64_t));
                    auto end_ptr = start_ptr + std::min((uint32_t)strlen(start_ptr), std::min(size, 8 * 1024 * 1024u)); // 8MB as safeguard.
                    std::string sdata = fromCharPointers(start_ptr, end_ptr);
                    elements[access_path] = sdata;
                    tree->add(access_path_to_json_keys(access_path), escape_for_json(sdata));
                }
            }

            // serialized field -> inc index!
            field_index++;
        }

        // reorg into JSON string...
        return tree->to_json();
    }
}