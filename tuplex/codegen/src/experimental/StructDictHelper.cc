//
// Created by leonhard on 9/23/22.
//

#include <experimental/StructDictHelper.h>
#include <experimental/ListHelper.h>

namespace tuplex {
    namespace codegen {


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

        void print_flatten_structured_dict_type(const python::Type &dict_type) {
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

            cout << ss.str() << endl;
        }

        // creating struct type based on structured dictionary type
        llvm::Type *
        generate_structured_dict_type(LLVMEnvironment &env, const std::string &name, const python::Type &dict_type) {
            using namespace llvm;
            auto &logger = Logger::instance().logger("codegen");
            llvm::LLVMContext &ctx = env.getContext();

            if (!dict_type.isStructuredDictionaryType()) {
                logger.error("provided type is not a structured dict type but " + dict_type.desc());
                return nullptr;
            }

            print_flatten_structured_dict_type(dict_type);

            // --> flattening the dict like this will guarantee that each level is local to itself, simplifying access.
            // (could also organize in fixed_size fields or not, but this here works as well)

            // each entry is {(key, key_type), ..., (key, key_type)}, value_type, alwaysPresent
            // only nested dicts are flattened. Tuples etc. are untouched. (would be too cumbersome)
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type, {});


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

            // std::stringstream ss;
            // ss << "computed following counts for structured dict type: " << pluralize(field_count, "field")
            //    << " " << pluralize(option_count, "option") << " " << pluralize(maybe_count, "maybe");
            // logger.info(ss.str());

            // let's start by allocating bitmaps for optional AND maybe types
            size_t num_option_bitmap_bits = core::ceilToMultiple(option_count, 64ul); // multiples of 64bit
            size_t num_maybe_bitmap_bits = core::ceilToMultiple(maybe_count, 64ul);
            size_t num_option_bitmap_elements = num_option_bitmap_bits / 64;
            size_t num_maybe_bitmap_elements = num_maybe_bitmap_bits / 64;


            bool is_packed = false;
            std::vector<llvm::Type *> member_types;
            auto i64Type = llvm::Type::getInt64Ty(ctx);

            // adding bitmap fails type creation - super weird.
            // add bitmap elements (if needed)

            // 64 bit logic
            // if (num_option_bitmap_elements > 0)
            //      member_types.push_back(llvm::ArrayType::get(i64Type, num_option_bitmap_elements));
            // if (num_maybe_bitmap_elements > 0)
            //      member_types.push_back(llvm::ArrayType::get(i64Type, num_maybe_bitmap_elements));

            // i1 logic (similar to flattened tuple)
            if (num_option_bitmap_elements > 0)
                member_types.push_back(llvm::ArrayType::get(Type::getInt1Ty(ctx), num_option_bitmap_bits));
            if (num_maybe_bitmap_elements > 0)
                member_types.push_back(llvm::ArrayType::get(Type::getInt1Ty(ctx), num_maybe_bitmap_bits));

            // auto a = ArrayType::get(Type::getInt1Ty(ctx), num_option_bitmap_bits);
            // if(num_option_bitmap_bits > 0)
            //    member_types.emplace_back(a);


            // now add all the elements from the (flattened) struct type (skip lists and other struct entries, i.e. only primitives so far)
            // --> could use a different structure as well! --> which to use?
            for (const auto &entry: entries) {
                // value type
                auto access_path = std::get<0>(entry);
                python::Type value_type = std::get<1>(entry);
                bool always_present = std::get<2>(entry);

                // helpful for debugging.
                auto path = json_access_path_to_string(access_path, value_type, always_present);

                // we do not save the key (because it's statically known), but simply lay out the data
                if (value_type.isOptionType())
                    value_type = value_type.getReturnType(); // option is handled above

                // is it a struct type? => skip.
                if (value_type.isStructuredDictionaryType())
                    continue;

                // // skip list
                // if (value_type.isListType())
                //     continue;



                // do we actually need to serialize the value?
                // if not, no problem.
                if(noNeedToSerializeType(value_type))
                    continue;

                // serialize. Check if it is a fixed size type -> no size field required, else add an i64 field to store the var_length size!
                auto mapped_type = env.pythonToLLVMType(value_type);
                if (!mapped_type)
                    throw std::runtime_error("could not map type " + value_type.desc());
                member_types.push_back(mapped_type);

                // special case: list -> skip size!
                if(value_type.isListType())
                    continue;

                if (!value_type.isFixedSizeType()) {
                    // not fixes size but var length?
                    // add a size field!
                    member_types.push_back(i64Type);
                }
            }

//            // convert to C++ to check if godbolt works -.-
//            std::stringstream cc_code;
//            cc_code<<"struct LargeStruct {\n";
//            int pos = 0;
//            for(auto t : member_types) {
//                if(t->isIntegerTy()) {
//                    cc_code<<"   int"<<t->getIntegerBitWidth()<<"_t x"<<pos<<";\n";
//                }
//                if(t->isPointerTy()) {
//                    cc_code<<"   uint8_t* x"<<pos<<";\n";
//                }
//                if(t->isArrayTy()) {
//                    cc_code<<"   int64_t x"<<pos<<"["<<t->getArrayNumElements()<<"];\n";
//                }
//                pos++;
//            }
//            cc_code<<"};\n";
//            std::cout<<"C++ code:\n\n"<<cc_code.str()<<std::endl;

            // finally, create type
            // Note: these types can get super large!
            // -> therefore identify using identified struct (not opaque one!)

            // // this would create a literal struct
            // return llvm::StructType::get(ctx, members, is_packed);

//            // this creates an identified one (identifier!)
//            auto stype = llvm::StructType::create(ctx, name);
//
//            // do not set body (too large)
//            llvm::ArrayRef<llvm::Type *> members(member_types); // !!! important !!!
//            stype->setBody(members, is_packed); // for info

            llvm::Type **type_array = new llvm::Type *[member_types.size()];
            for (unsigned i = 0; i < member_types.size(); ++i) {
                type_array[i] = member_types[i];
            }
            llvm::ArrayRef<llvm::Type *> members(type_array, member_types.size());

            llvm::Type *structType = llvm::StructType::create(ctx, members, name, false);
            llvm::StructType *STy = dyn_cast<StructType>(structType);

            return structType;
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

        // create 64bit bitmap from 1bit vector (ceil!)
        std::vector<llvm::Value*> create_bitmap(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const std::vector<llvm::Value*>& v) {
            using namespace std;

            auto numBitmapElements = core::ceilToMultiple(v.size(), 64ul) / 64ul; // make 64bit bitmaps

            // construct bitmap using or operations
            vector<llvm::Value*> bitmapArray;
            for(int i = 0; i < numBitmapElements; ++i)
                bitmapArray.emplace_back(env.i64Const(0));

            // go through values and add to respective bitmap
            for(int i = 0; i < v.size(); ++i) {
                // get index within bitmap
                auto bitmapPos = i;
                assert(v[i]->getType() == env.i1Type());
                bitmapArray[bitmapPos / 64] = builder.CreateOr(bitmapArray[bitmapPos / 64], builder.CreateShl(
                        builder.CreateZExt(v[i], env.i64Type()),
                        env.i64Const(bitmapPos % 64)));

            }

            return bitmapArray;
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

        // load entries to structure
        SerializableValue struct_dict_load_from_values(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const python::Type& dict_type, flattened_struct_dict_decoded_entry_list_t entries, llvm::Value* ptr) {
            using namespace llvm;

            auto& ctx = env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            // get the corresponding type
            auto stype = create_structured_dict_type(env, dict_type);
            assert(ptr);

            // get indices for faster storage access (note: not all entries need to be present!)
            auto indices = struct_dict_load_indices(dict_type);

            std::vector<std::pair<int, llvm::Value*>> bitmap_entries;
            std::vector<std::pair<int, llvm::Value*>> presence_entries;

            size_t num_bitmap = 0, num_presence_map = 0;
            flattened_struct_dict_entry_list_t type_entries;
            flatten_recursive_helper(type_entries, dict_type);
            retrieve_bitmap_counts(type_entries, num_bitmap, num_presence_map);
            bool has_bitmap = num_bitmap > 0;
            bool has_presence_map = num_presence_map > 0;

            // go over entries and generate code to load them!
            for(const auto& entry : entries) {
                // each item should be access_path | value_type | alwaysPresent |  value : SerializableValue | present : i1
                access_path_t access_path;
                python::Type value_type;
                bool always_present;
                SerializableValue el;
                llvm::Value* present = nullptr;
                std::tie(access_path, value_type, always_present, el, present) = entry;


                // fetch indices
                // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
                int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
                std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(access_path);

                // special case: list not supported yet, skip entries
                if(value_type.isListType()) {
                    throw std::runtime_error("update this!");
                    field_idx = -1;
                    size_idx = -1;
                }

                // is it an always present element?
                // => yes! then load it directly to the type.

                llvm::BasicBlock* bbPresenceDone = nullptr, *bbPresenceStore = nullptr;
                llvm::BasicBlock* bbStoreDone = nullptr, *bbStoreValue = nullptr;

                // presence check! store only if present
                if(present_idx >= 0) {
                    assert(present);
                    assert(!always_present);

                    // create blocks
                    bbPresenceDone = BasicBlock::Create(ctx, "present_check_done", F);
                    bbPresenceStore = BasicBlock::Create(ctx, "present_store", F);
                    builder.CreateCondBr(present, bbPresenceDone, bbPresenceStore);
                    builder.SetInsertPoint(bbPresenceStore);
                }

                // bitmap check! store only if NOT null...
                if(bitmap_idx >= 0) {
                    assert(el.is_null);
                    // create blocks
                    bbStoreDone = BasicBlock::Create(ctx, "store_done", F);
                    bbStoreValue = BasicBlock::Create(ctx, "store", F);
                    builder.CreateCondBr(el.is_null, bbStoreDone, bbStoreValue);
                    builder.SetInsertPoint(bbStoreValue);
                }

                // some checks
                if(field_idx >= 0) {
                    assert(el.val);
                    auto llvm_idx = CreateStructGEP(builder, ptr, field_idx);
                    builder.CreateStore(el.val, llvm_idx);
                }

                if(size_idx >= 0) {
                    assert(el.size);
                    auto llvm_idx = CreateStructGEP(builder, ptr, size_idx);
                    builder.CreateStore(el.size, llvm_idx);
                }

                if(bitmap_idx >= 0) {
                    builder.CreateBr(bbStoreDone);
                    builder.SetInsertPoint(bbStoreDone);
                    bitmap_entries.push_back(std::make_pair(bitmap_idx, el.is_null));
                }

                if(present_idx >= 0) {
                    builder.CreateBr(bbPresenceDone);
                    builder.SetInsertPoint(bbPresenceDone);
                    presence_entries.push_back(std::make_pair(bitmap_idx, present));
                }
            }

            // create bitmaps and store them away...
            // auto bitmap = create_bitmap(env, builder, bitmap_entries);
            // auto presence_map = create_bitmap(env, builder, presence_entries);

            //  // // 64 bit bitmap logic
            //                // // extract bit (pos)
            //                // auto structBitmapIdx = builder.CreateStructGEP(tuplePtr, 0); // bitmap comes first!
            //                // auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0, bitmapPos / 64);
            //
            //                // i1 array logic
            //                // auto structBitmapIdx = builder.CreateStructGEP(tuplePtr, 0); // bitmap comes first!
            //                auto structBitmapIdx = CreateStructGEP(builder, tuplePtr, 0ull); // bitmap comes first!
            //                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
            //                builder.CreateStore(value.is_null, bitmapIdx);

            // first comes bitmap, then presence map
            if(has_bitmap) {
                for(unsigned i = 0; i < bitmap_entries.size(); ++i) {
                    auto bitmapPos = bitmap_entries[i].first;
                    auto structBitmapIdx = CreateStructGEP(builder, ptr, 0ull); // bitmap comes first!
                    auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                    builder.CreateStore(bitmap_entries[i].second, bitmapIdx);
                }
            }

            if(has_bitmap) {
                for(unsigned i = 0; i < presence_entries.size(); ++i) {
                    auto bitmapPos = presence_entries[i].first;
                    auto structBitmapIdx = CreateStructGEP(builder, ptr, 1ull); // bitmap comes first!
                    auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                    builder.CreateStore(presence_entries[i].second, bitmapIdx);
                }
            }

            return SerializableValue(ptr, nullptr, nullptr);
        }

        std::string struct_dict_lookup_llvm(LLVMEnvironment& env, llvm::Type *stype, int i) {
            if(i < 0 || i > stype->getStructNumElements())
                return "[invalid index]";
            return "[" + env.getLLVMTypeName(stype->getStructElementType(i)) + "]";
        }

        void struct_dict_verify_storage(LLVMEnvironment& env, const python::Type& dict_type, std::ostream& os) {
            auto stype = create_structured_dict_type(env, dict_type);
            auto indices = struct_dict_load_indices(dict_type);
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            for(const auto& entry : entries) {
                access_path_t access_path = std::get<0>(entry);
                python::Type value_type = std::get<1>(entry);
                bool always_present = std::get<2>(entry);
                auto key = json_access_path_to_string(access_path, value_type, always_present);

                // fetch indices
                // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
                int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
                std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(access_path);

                // generate new line
                std::stringstream ss;
                ss<<key<<" :: ";
                if(bitmap_idx >= 0)
                    ss<<" bitmap: "<<bitmap_idx;
                if(present_idx >= 0)
                    ss<<" presence: "<<present_idx;
                if(field_idx >= 0)
                    ss<<" value: "<<field_idx<<" "<<struct_dict_lookup_llvm(env, stype, field_idx);
                if(size_idx >= 0)
                    ss<<" size: "<<size_idx<<" "<<struct_dict_lookup_llvm(env, stype, size_idx);
                os<<ss.str()<<std::endl;

            }

        }


        // helper function re type
        int bitmap_field_idx(const python::Type& dict_type) {
            // if has bitmap then
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);
            for(auto& entry : entries)
                if(std::get<1>(entry).isOptionType())
                    return 0;
            return -1;
        }

        int presence_map_field_idx(const python::Type& dict_type) {
            // if has bitmap then
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            bool has_bitmap = false;
            bool has_presence = false;
            for(auto& entry : entries) {
                if(std::get<1>(entry).isOptionType())
                    has_bitmap = true;
                if(!std::get<2>(entry))
                    has_presence = true;
            }
            if(has_bitmap && has_presence)
                return 1;
            if(has_presence)
                return 0;
            return -1;
        }

        // --- load functions ---
        llvm::Value* struct_dict_load_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path) {
            // return;

            auto indices = struct_dict_load_indices(dict_type);
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            // load only if valid present_idx
            if(present_idx >= 0) {
                // env.printValue(builder, is_present, "storing away is_present at index " + std::to_string(present_idx));

                // make sure type has presence map index
                auto p_idx = presence_map_field_idx(dict_type);
                assert(p_idx >= 0);
                // i1 store logic
                auto bitmapPos = present_idx;
                auto structBitmapIdx = CreateStructGEP(builder, ptr, (size_t)p_idx); // bitmap comes first!
                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                return builder.CreateLoad(bitmapIdx);
            } else {
                // always present
                return env.i1Const(true);
            }
        }

        SerializableValue struct_dict_load_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path) {

#warning "need to fix this, also need to fix for store! i.e., when storing a structured dict to a structured dict."
            // some UDF examples that should work:
            // x = {}
            // x['test'] = 10 # <-- type of x is now Struct['test' -> i64]
            // x['blub'] = {'a' : 20, 'b':None} # <-- type of x is now Struct['test' -> i64, 'blub' -> Struct['a' -> i64, 'b' -> null]]
            auto indices = struct_dict_load_indices(dict_type);
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            SerializableValue val;
            val.size = env.i64Const(sizeof(int64_t));
            val.is_null = env.i1Const(false);

            // load only if valid field_idx
            if(field_idx >= 0) {
                // env.printValue(builder, value, "storing away value at index " + std::to_string(field_idx));

                // store
                auto llvm_idx = CreateStructGEP(builder, ptr, field_idx);
                val.val = builder.CreateLoad(llvm_idx);
            }

            // load only if valid size_idx
            if(size_idx >= 0) {
                // env.printValue(builder, size, "storing away size at index " + std::to_string(size_idx));

                // store
                auto llvm_idx = CreateStructGEP(builder, ptr, size_idx);
                val.size = builder.CreateLoad(llvm_idx);
            }

            // load only if valid bitmap_idx
            if(bitmap_idx >= 0) {
                // env.printValue(builder, is_null, "storing away is_null at index " + std::to_string(bitmap_idx));

                // make sure type has presence map index
                auto b_idx = bitmap_field_idx(dict_type);
                assert(b_idx >= 0);
                // i1 store logic
                auto bitmapPos = bitmap_idx;
                auto structBitmapIdx = CreateStructGEP(builder, ptr, (size_t)b_idx); // bitmap comes first!
                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                val.is_null = builder.CreateLoad(bitmapIdx);
            }

            return val;
        }

        // --- store functions ---
        void struct_dict_store_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_present) {
            // return;

            auto indices = struct_dict_load_indices(dict_type);
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            // store only if valid present_idx
            if(present_idx >= 0) {
                // env.printValue(builder, is_present, "storing away is_present at index " + std::to_string(present_idx));

                // make sure type has presence map index
                auto p_idx = presence_map_field_idx(dict_type);
                assert(p_idx >= 0);
                assert(is_present && is_present->getType() == env.i1Type());
                // i1 store logic
                auto bitmapPos = present_idx;
                auto structBitmapIdx = CreateStructGEP(builder, ptr, (size_t)p_idx); // bitmap comes first!
                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                builder.CreateStore(is_present, bitmapIdx);
            }
        }

        void struct_dict_store_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* value) {
            auto indices = struct_dict_load_indices(dict_type);
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            // store only if valid field_idx
            if(field_idx >= 0) {
                // env.printValue(builder, value, "storing away value at index " + std::to_string(field_idx));

                // store
                auto llvm_idx = CreateStructGEP(builder, ptr, field_idx);
                builder.CreateStore(value, llvm_idx);
            }
        }

        void struct_dict_store_isnull(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_null) {
            auto indices = struct_dict_load_indices(dict_type);
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            // store only if valid bitmap_idx
            if(bitmap_idx >= 0) {
                // env.printValue(builder, is_null, "storing away is_null at index " + std::to_string(bitmap_idx));

                // make sure type has presence map index
                auto b_idx = bitmap_field_idx(dict_type);
                assert(b_idx >= 0);
                assert(is_null && is_null->getType() == env.i1Type());
                // i1 store logic
                auto bitmapPos = bitmap_idx;
                auto structBitmapIdx = CreateStructGEP(builder, ptr, (size_t)b_idx); // bitmap comes first!
                auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                builder.CreateStore(is_null, bitmapIdx);
            }
        }

        void struct_dict_store_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* size) {
            auto indices = struct_dict_load_indices(dict_type);

            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices.at(path);

            // store only if valid size_idx
            if(size_idx >= 0) {
                // env.printValue(builder, size, "storing away size at index " + std::to_string(size_idx));

                // store
                auto llvm_idx = CreateStructGEP(builder, ptr, size_idx);
                builder.CreateStore(size, llvm_idx);
            }
        }

        size_t struct_dict_bitmap_size_in_bytes(const python::Type& dict_type) {
            size_t num_bitmap = 0, num_presence_map = 0;
            flattened_struct_dict_entry_list_t type_entries;
            flatten_recursive_helper(type_entries, dict_type);
            retrieve_bitmap_counts(type_entries, num_bitmap, num_presence_map);

            return sizeof(int64_t) * num_bitmap + sizeof(int64_t) * num_presence_map;
        }

        SerializableValue struct_dict_serialized_memory_size(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type) {
            // get the corresponding type
            auto stype = create_structured_dict_type(env, dict_type);

            // TODO: is this check warranted or not? if yes, then getTupleElement for struct/list should return the gep original...
#warning "is this check here good or not?"
            //if(ptr->getType() != stype->getPointerTo())
            //    throw std::runtime_error("ptr has not correct type, must be pointer to " + stype->getStructName().str());

            // get flattened structure!
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            auto indices = struct_dict_load_indices(dict_type);

            // check bitmap size (i.e. multiples of 64bit)
            auto bitmap_size = struct_dict_bitmap_size_in_bytes(dict_type);

            llvm::Value* size = env.i64Const(bitmap_size);

            auto bytes8 = env.i64Const(sizeof(int64_t));

            // get indices to properly decode
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto value_type = std::get<1>(entry);
                bool always_present = std::get<2>(entry);
                auto t_indices = indices.at(access_path);

                // special case list: --> needs extra care
                if(value_type.isOptionType())
                    value_type = value_type.getReturnType();

                // skip nested struct dicts!
                if(value_type.isStructuredDictionaryType())
                    continue;

                if(python::Type::EMPTYLIST != value_type && value_type.isListType()) {
                    // call list specific function to determine length.
                    auto value_idx = std::get<2>(t_indices);
                    assert(value_idx >= 0);
                    auto list_ptr = CreateStructGEP(builder, ptr, value_idx);
                    auto s = list_serialized_size(env, builder, list_ptr, value_type);

                    // add 8 bytes for storing the info
                    s = builder.CreateAdd(s, env.i64Const(8));
                    size = builder.CreateAdd(size, s);
                    continue;
                }

                // depending on field, add size!
                auto value_idx = std::get<2>(t_indices);
                auto size_idx = std::get<3>(t_indices);
                // how to serialize everything?
                // -> use again the offset trick!
                // may serialize a good amount of empty fields... but so be it.
                if(value_idx >= 0) { // <-- value_idx >= 0 indicates it's a field that may/may not be serialized
                    // always add 8 bytes per field
                    size = builder.CreateAdd(size, bytes8);
                    if(size_idx >= 0) { // <-- size_idx >= 0 indicates a variable length field!
                        // add size field + data
                        if(ptr->getType()->isPointerTy()) {
                            auto llvm_idx = CreateStructGEP(builder, ptr, size_idx);
                            auto value_size = builder.CreateLoad(llvm_idx);
                            size = builder.CreateAdd(size, value_size);
                        } else {
                            auto value_size = builder.CreateExtractValue(ptr, std::vector<unsigned>(1, size_idx));
                            size = builder.CreateAdd(size, value_size);
                        }
                    }
                }

                // // debug print
                // auto path_desc = json_access_path_to_string(access_path, value_type, always_present);
                // env.printValue(builder, size, "size after serializing " + path_desc + ": ");
            }

            return SerializableValue(size, bytes8, nullptr);
        }

        llvm::Value* serializeBitmap(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* bitmap, llvm::Value* dest_ptr) {
            assert(bitmap && dest_ptr);
            assert(bitmap->getType()->isArrayTy());
            auto element_type = bitmap->getType()->getArrayElementType();
            assert(element_type == env.i1Type());
            assert(dest_ptr->getType() == env.i8ptrType());

            auto num_bitmap_bits = bitmap->getType()->getArrayNumElements();
            auto num_elements = core::ceilToMultiple(num_bitmap_bits, 64ul) / 64ul;


            // need to create a temp variable
            auto arr_ptr = env.CreateFirstBlockAlloca(builder, bitmap->getType());
//            env.lifetimeStart(builder, arr_ptr);
            builder.CreateStore(bitmap, arr_ptr);

            llvm::Value* bitmap_ptr = builder.CreateBitOrPointerCast(arr_ptr, env.i64ptrType());
            dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());

            // store now
            for(unsigned i = 0; i < num_elements; ++i) {
                auto element_value = builder.CreateLoad(bitmap_ptr);
                builder.CreateStore(element_value, dest_ptr);
                bitmap_ptr = builder.CreateGEP(bitmap_ptr, env.i64Const(1));
                dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(1));
            }

            // end lifetime of arr_ptr
//            env.lifetimeEnd(builder, arr_ptr);

            // cast back to i8 ptr
            dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i8ptrType());

            return dest_ptr;
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

        void struct_dict_mem_zero(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type) {
            auto& logger = Logger::instance().logger("codegen");

            // get the corresponding type
            auto stype = create_structured_dict_type(env, dict_type);

            if(ptr->getType() != stype->getPointerTo())
                throw std::runtime_error("ptr has not correct type, must be pointer to " + stype->getStructName().str());

            // get flattened structure!
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            auto indices = struct_dict_load_indices(dict_type);

            // also zero bitmaps? i.e. everything should be null and not present?

            if(struct_dict_has_bitmap(dict_type)) {
//                auto bitmap_idx = CreateStructGEP(builder, ptr, 0);
//                auto bitmap = builder.CreateLoad(bitmap_idx);
//                dest_ptr = serializeBitmap(env, builder, bitmap, dest_ptr);
            }
            // 2. presence-bitmap
            if(struct_dict_has_presence_map(dict_type)) {
//                auto presence_map_idx = CreateStructGEP(builder, ptr, 1);
//                auto presence_map = builder.CreateLoad(presence_map_idx);
//                dest_ptr = serializeBitmap(env, builder, presence_map, dest_ptr);
            }
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto t_indices = indices.at(access_path);
                auto value_idx = std::get<2>(t_indices);
                auto size_idx = std::get<3>(t_indices);
                auto value_type = std::get<1>(entry);

                if (value_type.isOptionType())
                    value_type = value_type.getReturnType();

                // skip list
                if(python::Type::EMPTYLIST != value_type && value_type.isListType()) {
                    // special case: use list zero function!
                    assert(value_idx >= 0);

                    auto list_ptr = CreateStructGEP(builder, ptr, value_idx);
                    list_init_empty(env, builder, list_ptr, value_type);
                    continue; // --> done, go to next one.
                }

                // skip nested struct dicts!
                if (value_type.isStructuredDictionaryType())
                    continue;

                if(size_idx >= 0) {
                    auto llvm_size_idx = CreateStructGEP(builder, ptr, size_idx);

                    assert(llvm_size_idx->getType() == env.i64ptrType());
                    // store 0!
                    builder.CreateStore(env.i64Const(0), llvm_size_idx);
                }
            }
        }

        SerializableValue struct_dict_serialize_to_memory(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, llvm::Value* dest_ptr) {
            auto& logger = Logger::instance().logger("codegen");

            llvm::Value* original_dest_ptr = dest_ptr;

            // get the corresponding type
            auto stype = create_structured_dict_type(env, dict_type);

            // if(ptr->getType() != stype->getPointerTo())
            //    throw std::runtime_error("ptr has not correct type, must be pointer to " + stype->getStructName().str());

            // get flattened structure!
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            auto indices = struct_dict_load_indices(dict_type);

            // check bitmap size (i.e. multiples of 64bit)
            auto bitmap_size = struct_dict_bitmap_size_in_bytes(dict_type);

            llvm::Value* size = env.i64Const(bitmap_size);

            auto bytes8 = env.i64Const(sizeof(int64_t));

            // start: serialize bitmaps
            // 1. null-bitmap
            size_t bitmap_offset = 0;
            if(struct_dict_has_bitmap(dict_type)) {
                llvm::Value* bitmap = nullptr;
                if(ptr->getType()->isPointerTy()) {
                    auto bitmap_idx = CreateStructGEP(builder, ptr, bitmap_offset);
                    bitmap = builder.CreateLoad(bitmap_idx);
                } else {
                    bitmap = builder.CreateExtractValue(ptr, std::vector<unsigned>(1, bitmap_offset));
                }

                dest_ptr = serializeBitmap(env, builder, bitmap, dest_ptr);
                bitmap_offset++;
            }
            // 2. presence-bitmap
            if(struct_dict_has_presence_map(dict_type)) {
                auto presence_map_idx = CreateStructGEP(builder, ptr, bitmap_offset);
                auto presence_map = builder.CreateLoad(presence_map_idx);
                dest_ptr = serializeBitmap(env, builder, presence_map, dest_ptr);
                bitmap_offset++;
            }

            // count how many fields there are => important to compute offsets!
            size_t num_fields = 0;
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto t_indices = indices.at(access_path);
                auto value_idx = std::get<2>(t_indices);
                auto value_type = std::get<1>(entry);

                if(value_type.isOptionType())
                    value_type = value_type.getReturnType();
                // skip nested struct dicts!
                if(value_type.isStructuredDictionaryType())
                    continue;

                if(value_idx < 0)
                    continue; // can skip field, not necessary to serialize
                num_fields++;
            }

            logger.debug("found " + pluralize(num_fields, "field") + " to serialize.");

            size_t field_index = 0; // used in order to compute offsets!
            llvm::Value* varLengthOffset = env.i64Const(0); // current offset from varfieldsstart ptr
            llvm::Value* varFieldsStartPtr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t) * num_fields)); // where in memory the variable field storage starts!

            // get indices to properly decode
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto value_type = std::get<1>(entry);
                auto t_indices = indices.at(access_path);

                // special case list: --> needs extra care
                if(value_type.isOptionType())
                    value_type = value_type.getReturnType();
                if(python::Type::EMPTYLIST != value_type && value_type.isListType()) {
                    // special case, perform it here, then skip:
                    // call list specific function to determine length.
                    auto value_idx = std::get<2>(t_indices);
                    assert(value_idx >= 0);
                    auto list_type = value_type;
                    auto list_ptr = CreateStructGEP(builder, ptr, value_idx);
                    auto list_size_in_bytes = list_serialized_size(env, builder, list_ptr, list_type);

                    // => list is ALWAYS a var length field, serialize like that.
                    // compute offset
                    // from current field -> varStart + varoffset
                    size_t cur_to_var_start_offset = (num_fields - field_index + 1) * sizeof(int64_t);
                    auto offset = builder.CreateAdd(env.i64Const(cur_to_var_start_offset), varLengthOffset);

                    auto varDest = builder.CreateGEP(varFieldsStartPtr, varLengthOffset);
                    // call list function
                    list_serialize_to(env, builder, list_ptr, list_type, varDest);

                    // pack offset and size into 64bit!
                    auto info = pack_offset_and_size(builder, offset, list_size_in_bytes);

                    // store info away
                    auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
                    builder.CreateStore(info, casted_dest_ptr);

                    dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t)));
                    varLengthOffset = builder.CreateAdd(varLengthOffset, list_size_in_bytes);
                    field_index++;
                    continue;
                }

                // skip nested struct dicts! --> they're taken care of.
                if(value_type.isStructuredDictionaryType())
                    continue;

                // depending on field, add size!
                auto value_idx = std::get<2>(t_indices);
                auto size_idx = std::get<3>(t_indices);

                if(value_idx < 0)
                    continue; // can skip field, not necessary to serialize

                // what kind of data is it that needs to be serialized?
                bool is_varlength_field = size_idx >= 0;
                assert(value_idx >= 0);

                // load value
                llvm::Value* value = nullptr;
                if(ptr->getType()->isPointerTy()) {
                    auto llvm_value_idx = CreateStructGEP(builder, ptr, value_idx);
                    value = builder.CreateLoad(llvm_value_idx);
                } else {
                    value = builder.CreateExtractValue(ptr, std::vector<unsigned>(1, value_idx));
                }

                if(!is_varlength_field) {
                    // simple: just load data and copy!
                    // make sure it's bool/i64/64 -> these are the only fixed size fields!

                    assert(value_type == python::Type::BOOLEAN || value_type == python::Type::I64 || value_type == python::Type::F64);

                    if(value_type == python::Type::BOOLEAN)
                        value = builder.CreateZExt(value, env.i64Type());

                    // store with casting
                    auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, value->getType()->getPointerTo());
                    builder.CreateStore(value, casted_dest_ptr);
                    dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t)));
                } else {
                    // more complex:
                    // for now, only string supported... => load and fix!
                    if(value_type != python::Type::STRING)
                        throw std::runtime_error("unsupported type " + value_type.desc() + " encountered! ");

                    // add size field + data
                    auto llvm_size_idx = CreateStructGEP(builder, ptr, size_idx);
                    auto value_size = llvm_size_idx->getType()->isPointerTy() ? builder.CreateLoad(llvm_size_idx) : llvm_size_idx; // <-- serialized size.

                    // compute offset
                    // from current field -> varStart + varoffset
                    size_t cur_to_var_start_offset = (num_fields - field_index + 1) * sizeof(int64_t);
                    auto offset = builder.CreateAdd(env.i64Const(cur_to_var_start_offset), varLengthOffset);

                    auto varDest = builder.CreateGEP(varFieldsStartPtr, varLengthOffset);
                    builder.CreateMemCpy(varDest, 0, value, 0, value_size); // for string, simple value copy!

                    // pack offset and size into 64bit!
                    auto info = pack_offset_and_size(builder, offset, value_size);

                    // store info away
                    auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
                    builder.CreateStore(info, casted_dest_ptr);

                    dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t)));

                    varLengthOffset = builder.CreateAdd(varLengthOffset, value_size);
                }

                // serialized field -> inc index!
                field_index++;
            }

            // move dest ptr to end!
            dest_ptr = builder.CreateGEP(dest_ptr, varLengthOffset);


            llvm::Value* serialized_size = builder.CreatePtrDiff(dest_ptr, original_dest_ptr);
            return SerializableValue(original_dest_ptr, serialized_size, nullptr);
        }

        size_t struct_dict_heap_size(LLVMEnvironment& env, const python::Type& dict_type) {
            using namespace llvm;

            assert(dict_type.isStructuredDictionaryType());
            assert(env.getModule());
            auto& DL = env.getModule()->getDataLayout();
            auto llvm_type = env.getOrCreateStructuredDictType(dict_type);

            return DL.getTypeAllocSize(llvm_type);
        }

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

        bool access_paths_equal(const access_path_t& rhs, const access_path_t& lhs) {
            if(rhs.size() != lhs.size())
                return false;
            for(unsigned i = 0; i < rhs.size(); ++i) {
                if(rhs[i].second != lhs[i].second)
                    return false;
                if(!semantic_python_value_eq(rhs[i].second, rhs[i].first, lhs[i].first))
                    return false;
            }
            return true;
        }

        flattened_struct_dict_entry_list_t::const_iterator find_by_access_path(const flattened_struct_dict_entry_list_t& entries, const access_path_t& path) {
            // compare path exactly
            flattened_struct_dict_entry_list_t::const_iterator it = std::find_if(entries.begin(), entries.end(), [path](const flattened_struct_dict_entry_t& entry) {
                auto e_path = std::get<0>(entry);
                return access_paths_equal(e_path, path);
            });
            return it;
        }

        bool access_path_prefix_equal(const access_path_t& path, const access_path_t& prefix) {
            if(prefix.size() > path.size())
                return false;
            for(unsigned i = 0; i < prefix.size(); ++i) {
                if(path[i].second != prefix[i].second)
                    return false;
                if(!semantic_python_value_eq(path[i].second, path[i].first, prefix[i].first))
                    return false;
            }
            return true;
        }

        std::vector<unsigned> find_prefix_indices_by_access_path(const flattened_struct_dict_entry_list_t& entries, const access_path_t& path) {
            std::vector<unsigned> indices;
            unsigned idx = 0;
            for(const auto& entry : entries) {
                auto e_path = std::get<0>(entry);
                // compare prefixes
                if(access_path_prefix_equal(e_path, path))
                    indices.push_back(idx);
                idx++;
            }
            return indices;
        }

        python::Type struct_dict_type_get_element_type(const python::Type& dict_type, const access_path_t& path) {
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
                        return struct_dict_type_get_element_type(kv_pair.valueType, suffix_path);
                    }
                }
            }
            return python::Type::UNKNOWN; // not found.
        }


        SerializableValue struct_dict_get_or_except(LLVMEnvironment& env,
                                                    llvm::IRBuilder<>& builder,
                                                    const python::Type& dict_type,
                                                    const std::string& key,
                                                    const python::Type& key_type,
                                                    llvm::Value* ptr,
                                                    llvm::BasicBlock* bbKeyNotFound) {
            using namespace llvm;

            // check first that key_type is actually contained within dict type
            assert(dict_type.isStructuredDictionaryType());
            bool element_found = true;

            auto it = find_by_key(dict_type, key, key_type);
            if(it == dict_type.get_struct_pairs().end()) {
                // key needs to be known to dict structure!
                element_found = false;
                throw std::runtime_error("could not find key " + key + " (" + key_type.desc() + ") in struct type.");
            }

            // get indices to access element
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            // print out paths
            for(const auto& entry : entries)
                std::cout<<json_access_path_to_string(std::get<0>(entry), std::get<1>(entry), std::get<2>(entry));

            // find corresponding entry
            // flat access path
            access_path_t access_path;
            access_path.push_back(std::make_pair(key, key_type));

            // check all elements with that prefix
            auto prefix_indices = find_prefix_indices_by_access_path(entries, access_path);
            if(prefix_indices.empty()) {
                throw std::runtime_error("could not find entry under key " + key + " (" + key_type.desc() + ") in struct type.");
            }

            auto value_type = struct_dict_type_get_element_type(dict_type, access_path);
            if(python::Type::UNKNOWN == value_type) {
                throw std::runtime_error("fatal error, could not find element type for access path");
            }

            SerializableValue value = CreateDummyValue(env, builder, value_type);
            int bitmap_idx = -1, present_idx = -1, field_idx = -1, size_idx = -1;
            if(element_found) {
                auto struct_indices = struct_dict_load_indices(dict_type);

                if(prefix_indices.size() == 1) {
                    // note: following will only work for single element OR a nested struct that is maybe
                    auto indices = struct_indices.at(access_path);
                    std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices;
                }

                // check if present map indicates something
                if(present_idx >= 0) {
                    // need to check bit
                    auto element_present = struct_dict_load_present(env, builder, ptr, dict_type, access_path);
                    throw std::runtime_error("not yet supported");
                }

                // load value
                value = struct_dict_load_value(env, builder, ptr, dict_type, access_path);
            }

            return value;
        }

    }
}