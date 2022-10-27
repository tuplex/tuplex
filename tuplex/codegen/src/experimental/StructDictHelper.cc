//
// Created by leonhard on 9/23/22.
//

#include <experimental/StructDictHelper.h>
#include <experimental/ListHelper.h>
#include <codegen/FlattenedTuple.h>

namespace tuplex {
    namespace codegen {




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

            // print_flatten_structured_dict_type(dict_type);

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
            size_t num_option_bitmap_bits = option_count; // multiples of 64bit
            size_t num_maybe_bitmap_bits = maybe_count;
            size_t num_option_bitmap_elements = core::ceilToMultiple(option_count, 64ul) / 64ul;
            size_t num_maybe_bitmap_elements = core::ceilToMultiple(maybe_count, 64ul) / 64ul;


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

            // checks
#ifndef NDEBUG
            if(struct_dict_has_bitmap(dict_type))
                assert(num_option_bitmap_elements > 0 && num_option_bitmap_bits > 0);
            if(struct_dict_has_presence_map(dict_type))
                assert(num_maybe_bitmap_elements > 0 && num_maybe_bitmap_bits > 0);
#endif

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

            delete [] type_array;

            // some checks re bitmaps
            size_t bitmap_idx = 0;
            if(struct_dict_has_bitmap(dict_type)) {
                assert(structType->getStructElementType(bitmap_idx)->isArrayTy());
                bitmap_idx++;
            }
            if(struct_dict_has_presence_map(dict_type)) {
                assert(structType->getStructElementType(bitmap_idx)->isArrayTy());
            }

            return structType;
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

        // load entries to structure
        SerializableValue struct_dict_load_from_values(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const python::Type& dict_type, flattened_struct_dict_decoded_entry_list_t entries, llvm::Value* ptr) {
            using namespace llvm;

            auto& ctx = env.getContext();
            auto F = builder.GetInsertBlock()->getParent();

            // get the corresponding type
            auto stype = create_structured_dict_type(env, dict_type);
            assert(ptr);

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
                std::tie(bitmap_idx, present_idx, field_idx, size_idx) = struct_dict_get_indices(dict_type, access_path);

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
            // if has bitmap then it's the first field
            return struct_dict_has_bitmap(dict_type) ? 0 : -1;
        }

        int presence_map_field_idx(const python::Type& dict_type) {
            // if it has bitmap then it's the second field, else it's the first field if present.
            bool has_bitmap = struct_dict_has_bitmap(dict_type);
            bool has_presence = struct_dict_has_presence_map(dict_type);

            if(has_bitmap && has_presence)
                return 1;
            if(has_presence)
                return 0;
            return -1;
        }

        // --- load functions ---
        llvm::Value* struct_dict_load_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path) {



            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = struct_dict_get_indices(dict_type, path);

            // if path is not contained within indices, it's an always present (parent) object.
            if(-1 == bitmap_idx && -1 == present_idx && -1 == field_idx && -1 == size_idx)
                return env.i1Const(true);

            // load only if valid present_idx
            if(present_idx >= 0) {
                // env.printValue(builder, is_present, "storing away is_present at index " + std::to_string(present_idx));

                // make sure type has presence map index
                auto p_idx = presence_map_field_idx(dict_type);
                assert(p_idx >= 0);
                // i1 load logic
                auto bitmapPos = present_idx;

                if(ptr->getType()->isStructTy()) {
                    auto bitmap = CreateStructLoad(builder, ptr, p_idx);
                    return builder.CreateExtractValue(bitmap, std::vector<unsigned>(1, bitmapPos));
                } else {
                    assert(ptr->getType()->isPointerTy() && ptr->getType()->getPointerElementType()->isStructTy());
                     auto structBitmapIdx = CreateStructGEP(builder, ptr, (size_t)p_idx); // bitmap comes first!
                     auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                     return builder.CreateLoad(bitmapIdx);
                }
            } else {
                // always present
                return env.i1Const(true);
            }
        }

        llvm::Value* struct_dict_load_path_presence(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                                    llvm::Value* ptr, const python::Type& dict_type,
                                                    const access_path_t& full_path,
                                                    const access_path_t& ignore_prefix={}) {
            assert(!full_path.empty());

            // check that ignore prefix is actually a prefix
            if(!ignore_prefix.empty()) {
                assert(ignore_prefix.size() <= full_path.size());
                for(unsigned i = 0; i < ignore_prefix.size(); ++i) {
                    assert(full_path[i].first == ignore_prefix[i].first);
                    assert(full_path[i].second == ignore_prefix[i].second);
                }
            }



            auto t = struct_dict_get_indices(dict_type, full_path);
            auto present_idx = std::get<1>(t);

            // if not found, it's an always present element (i.e. all indices -1)
            if(-1 == std::get<0>(t) && -1 == std::get<1>(t) && -1 == std::get<2>(t) && -1 == std::get<3>(t))
                return env.i1Const(true);

            // load is present
            auto is_present = struct_dict_load_present(env, builder, ptr, dict_type, full_path);

            // not present? -> return element directly, else check parent up to ignore prefix.
            // if(!is_present) return false;
            // -> can also do an and reduction. I.e., element is only present if parent element is also present.
            // -> go parent chain up till ignore prefix
            for(unsigned i = 1; i < full_path.size() - ignore_prefix.size(); ++i) {
                access_path_t parent_path(full_path.begin(), full_path.end() - i);

#ifndef NDEBUG
                auto parent_path_str = access_path_to_str(parent_path);
#endif

                auto parent_present = struct_dict_load_present(env, builder, ptr, dict_type, parent_path);
                is_present = builder.CreateAnd(is_present, parent_present);
            }
            assert(is_present && is_present->getType() == env.i1Type());
            return is_present;
        }



        bool struct_dict_path_has_presence_entry(const python::Type& dict_type, const access_path_t& path) {
            //TODO: isn't there something missing?
            return false;
        }

        void struct_dict_store_value(LLVMEnvironment& env,
                                     llvm::IRBuilder<>& builder,
                                     const SerializableValue& value,
                                     llvm::Value* dest_ptr,
                                     const python::Type& dest_dict_type,
                                     const access_path_t& dest_path) {
            using namespace llvm;

            auto element_type = struct_dict_type_get_element_type(dest_dict_type, dest_path);
            // make sure path exists
            if(python::Type::UNKNOWN == element_type)
                throw std::runtime_error("path does not exist in dictionary, can't store value.");

            // need to store presence bit?
            // if so do!
            if(struct_dict_path_has_presence_entry(dest_dict_type, dest_path)) {
                // store present
                struct_dict_store_present(env, builder, dest_ptr, dest_dict_type, dest_path, env.i1Const(true));
            }
            auto& ctx = builder.getContext();

            // optional?
            BasicBlock* bNext = nullptr;
            if(element_type.isOptionType()) {
                // store only if not null
                assert(value.is_null);
                BasicBlock* bStore = BasicBlock::Create(ctx, "store_dict", builder.GetInsertBlock()->getParent());
                bNext = BasicBlock::Create(ctx, "next", builder.GetInsertBlock()->getParent());

                // store bitmap bit
                struct_dict_store_isnull(env, builder, dest_ptr, dest_dict_type, dest_path, value.is_null);

                builder.CreateCondBr(value.is_null, bNext, bStore);
                builder.SetInsertPoint(bStore);
            }

            // store value
            // --> primitive?
            if(value.val)
                struct_dict_store_value(env, builder, dest_ptr, dest_dict_type, dest_path, value.val);
            if(value.size) {
                assert(value.size->getType() == env.i64Type());
                struct_dict_store_size(env, builder, dest_ptr, dest_dict_type, dest_path, value.size);
            }

            // go to next block if option was used...
            if(bNext) {
                builder.CreateBr(bNext);
                builder.SetInsertPoint(bNext);
            }
        }

        SerializableValue struct_dict_load_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path) {
            auto& logger = Logger::instance().logger("codegen");

            // get element type
            auto element_type = struct_dict_type_get_element_type(dict_type, path);
            if(python::Type::UNKNOWN == element_type) {
                throw std::runtime_error("Could not retrieve element type for access path " + access_path_to_str(path));
            }

            // is it not a struct dict? -> trivial, simple lookup.
            bool is_struct_dict = element_type.isStructuredDictionaryType() || (element_type.isOptionType() && element_type.getReturnType().isStructuredDictionaryType());
            if(is_struct_dict) {

                // this is a bit more involved. First, need to init new var for the subdict
                auto element_type_wo_option = element_type.isOptionType() ? element_type.getReturnType() : element_type;
                auto llvm_element_type = env.getOrCreateStructuredDictType(element_type_wo_option);
                auto element_ptr = env.CreateFirstBlockAlloca(builder, llvm_element_type);
                struct_dict_mem_zero(env, builder, element_ptr, element_type_wo_option);

                llvm::Value* is_null = env.i1Const(false); // <-- option should decode this.

                if(element_type.isOptionType()) {
                    // is it not an option? that means resorting indices
                    // fetch indices
                    // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
                    int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
                    std::tie(bitmap_idx, present_idx, field_idx, size_idx) = struct_dict_get_indices(dict_type, path);

                    assert(bitmap_idx >= 0);

                    // load is_null from original ptr
                    // make sure type has presence map index
                    auto b_idx = bitmap_field_idx(dict_type);
                    assert(b_idx >= 0);
                    // i1 store logic
                    auto bitmapPos = bitmap_idx;
                    auto structBitmapIdx = CreateStructGEP(builder, ptr, (size_t)b_idx); // bitmap comes first!
                    auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                    is_null = builder.CreateLoad(bitmapIdx);

                    // TODO: now load elements...
                }

                // go over access paths and access elements.
                auto prefix_path = path;

                // logger.debug("prefix path: " + access_path_to_str(prefix_path));

                // paths for sub pointer
                flattened_struct_dict_entry_list_t element_entries;
                flatten_recursive_helper(element_entries, element_type_wo_option);
                for(auto element_entry : element_entries) {
                    access_path_t suffix_path = std::get<0>(element_entry);
                    access_path_t full_path = prefix_path;
                    for(auto atom : suffix_path)
                        full_path.push_back(atom);

                    // load presence of element (& top elements!)
                    auto is_present = struct_dict_load_path_presence(env, builder, ptr, dict_type, full_path);

                    // load original element
                    auto type = struct_dict_type_get_element_type(dict_type, full_path);
                    if(type == python::Type::UNKNOWN)
                        throw std::runtime_error("could not find element under path " + access_path_to_str(full_path));
                    // @TODO: deal with presence...

                    auto element_value = struct_dict_load_value(env, builder, ptr, dict_type, full_path);

                    // store in subdict
                    struct_dict_store_value(env, builder, element_value, element_ptr, element_type_wo_option, suffix_path);
                    struct_dict_store_present(env, builder, element_ptr, element_type_wo_option, suffix_path, is_present);
                }

                return SerializableValue(element_ptr, nullptr, is_null);
            } else {
                // some UDF examples that should work:
                // x = {}
                // x['test'] = 10 # <-- type of x is now Struct['test' -> i64]
                // x['blub'] = {'a' : 20, 'b':None} # <-- type of x is now Struct['test' -> i64, 'blub' -> Struct['a' -> i64, 'b' -> null]]

                // fetch indices
                // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
                int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
                std::tie(bitmap_idx, present_idx, field_idx, size_idx) = struct_dict_get_indices(dict_type, path);

                SerializableValue val;
                val.size = env.i64Const(sizeof(int64_t));
                val.is_null = env.i1Const(false);

                // load only if valid field_idx
                if(field_idx >= 0) {
                    // env.printValue(builder, value, "storing away value at index " + std::to_string(field_idx));

                    // // load
                    // auto llvm_idx = CreateStructGEP(builder, ptr, field_idx);
                    // val.val = builder.CreateLoad(llvm_idx);
                    val.val = CreateStructLoad(builder, ptr, field_idx);
                }

                // load only if valid size_idx
                if(size_idx >= 0) {
                    // env.printValue(builder, size, "storing away size at index " + std::to_string(size_idx));

                    // // load
                    // auto llvm_idx = CreateStructGEP(builder, ptr, size_idx);
                    // val.size = builder.CreateLoad(llvm_idx);
                    val.size = CreateStructLoad(builder, ptr, size_idx);
                }

                // load only if valid bitmap_idx
                if(bitmap_idx >= 0) {
                    // env.printValue(builder, is_null, "storing away is_null at index " + std::to_string(bitmap_idx));

                    // make sure type has presence map index
                    auto b_idx = bitmap_field_idx(dict_type);
                    assert(b_idx >= 0);
                    // i1 load logic
                    auto bitmapPos = bitmap_idx;
                    auto structBitmapIdx = CreateStructGEP(builder, ptr, (size_t)b_idx); // bitmap comes first!
                    auto bitmapIdx = builder.CreateConstInBoundsGEP2_64(structBitmapIdx, 0ull, bitmapPos);
                    val.is_null = builder.CreateLoad(bitmapIdx);
                }

                return val;
            }
        }

        // --- store functions ---
        void struct_dict_store_present(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_present) {
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = struct_dict_get_indices(dict_type, path);

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
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = struct_dict_get_indices(dict_type, path);

            // store only if valid field_idx
            if(field_idx >= 0) {
                // env.printValue(builder, value, "storing away value at index " + std::to_string(field_idx));

                // store
                auto llvm_idx = CreateStructGEP(builder, ptr, field_idx);

                // special case: sometimes storing element type dict or list will be based upon pointer -> load first!
                if(value->getType() == llvm_idx->getType()) {
                    value = builder.CreateLoad(value);
                }

                builder.CreateStore(value, llvm_idx);
            }
        }

        void struct_dict_store_isnull(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type, const access_path_t& path, llvm::Value* is_null) {
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = struct_dict_get_indices(dict_type, path);

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
            // fetch indices
            // 1. null bitmap index 2. maybe bitmap index 3. field index 4. size index
            int bitmap_idx = 0, present_idx =0, field_idx=0, size_idx=0;
            std::tie(bitmap_idx, present_idx, field_idx, size_idx) = struct_dict_get_indices(dict_type, path);

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

            // check bitmap size (i.e. multiples of 64bit)
            auto bitmap_size = struct_dict_bitmap_size_in_bytes(dict_type);

            llvm::Value* size = env.i64Const(bitmap_size);

            auto bytes8 = env.i64Const(sizeof(int64_t));

            // get indices to properly decode
            int pos = -1;
            for(auto entry : entries) {
                pos++;
                auto access_path = std::get<0>(entry);
                auto value_type = std::get<1>(entry);
                bool always_present = std::get<2>(entry);
                auto t_indices = struct_dict_get_indices(dict_type, access_path);

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
                    auto list_ptr = CreateStructLoad(builder, ptr, value_idx);

                    // is list_ptr a pointer?
                    if(!list_ptr->getType()->isPointerTy()) {
                        auto list = list_ptr;
                        list_ptr = env.CreateFirstBlockAlloca(builder, list_ptr->getType());
                        builder.CreateStore(list, list_ptr);
                    }

                    auto s = list_serialized_size(env, builder, list_ptr, value_type);

                    env.printValue(builder, s, "got list size of: ");

                    assert(s->getType() == env.i64Type());
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
                    size = builder.CreateAdd(size, bytes8, "dict_el_" + std::to_string(pos));
                    if(size_idx >= 0) { // <-- size_idx >= 0 indicates a variable length field!
                        // add size field + data
                        auto value_size = CreateStructLoad(builder, ptr, size_idx);
                        assert(value_size->getType() == env.i64Type());
                        size = builder.CreateAdd(size, value_size);
                    }
                }

                // // debug print
                auto path_desc = json_access_path_to_string(access_path, value_type, always_present);
                env.printValue(builder, size, "size after serializing " + path_desc + ": ");
            }

            return SerializableValue(size, bytes8, nullptr);
        }

        llvm::Value* serializeBitmap(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* bitmap, llvm::Value* dest_ptr) {
            using namespace std;

            assert(bitmap && dest_ptr);
            assert(bitmap->getType()->isArrayTy());
            auto element_type = bitmap->getType()->getArrayElementType();
            assert(element_type == env.i1Type());
            assert(dest_ptr->getType() == env.i8ptrType());

            auto num_bitmap_bits = bitmap->getType()->getArrayNumElements();
            auto num_elements = core::ceilToMultiple(num_bitmap_bits, 64ul) / 64ul;

            // use the approach from FlattenedTuple using or (direct load from array DOESNT work)
            vector<llvm::Value*> bitmap_array;
            for(unsigned i = 0; i < num_elements; ++i)
                bitmap_array.emplace_back(env.i64Const(0));

            // create ors
            for(unsigned i = 0; i < num_bitmap_bits; ++i) {
                // i1 array logic.
                llvm::Value* bitmapIdx = nullptr;
                if(bitmap->getType()->isArrayTy()) { // can not load directly from array, hence store into tmp variable - then load!
                    auto bitmap_tmp = env.CreateFirstBlockAlloca(builder, bitmap->getType());
                    builder.CreateStore(bitmap, bitmap_tmp);
                    bitmap = bitmap_tmp;
                }
                bitmapIdx = builder.CreateConstInBoundsGEP2_64(bitmap, 0ull, i);
                auto bit = builder.CreateLoad(bitmapIdx);
                auto bit_ext = builder.CreateShl(builder.CreateZExt(bit, env.i64Type()), env.i64Const(i % 64ul));
                bitmap_array[i / 64ul] = builder.CreateOr(bitmap_array[i / 64ul], bit_ext);
            }

            // write out elements
            for(auto bitmap_element : bitmap_array) {
                builder.CreateStore(bitmap_element, builder.CreatePointerCast(dest_ptr, env.i64ptrType()));
                dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t)));
            }

            return dest_ptr;
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
                auto t_indices = struct_dict_get_indices(dict_type, access_path);
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

        size_t struct_dict_get_field_count(const python::Type& dict_type) {
            assert(dict_type.isStructuredDictionaryType());

            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            // count how many fields there are => important to compute offsets!
            size_t num_fields = 0;
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto t_indices = struct_dict_get_indices(dict_type, access_path);
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

            return num_fields;
        }

        // deserializastion code...
        SerializableValue struct_dict_deserialize_from_memory(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* ptr, const python::Type& dict_type) {
            auto& logger = Logger::instance().logger("codegen");
            assert(dict_type.isStructuredDictionaryType());

            using namespace llvm;
            using namespace std;

            SerializableValue v;
            auto stype = env.getOrCreateStructuredDictType(dict_type);
            v.val = env.CreateFirstBlockAlloca(builder, stype);
            struct_dict_mem_zero(env, builder, v.val, dict_type);
            auto dict_ptr = v.val;
            auto original_mem_start_ptr = ptr; // save pointer for memory distance

            // get flattened structure!
            flattened_struct_dict_entry_list_t entries;
            flatten_recursive_helper(entries, dict_type);

            // optionally the bitmaps to store within the struct
            vector<llvm::Value*> bitmap;
            vector<llvm::Value*> presence_map;

            // step 1: decode bitmap if exists and load to array
            size_t bitmap_idx = 0;
            if(struct_dict_has_bitmap(dict_type)) {
                assert(stype->isStructTy() && stype->getStructElementType(bitmap_idx)->isArrayTy());
                size_t num_bitmap_bits = stype->getStructElementType(bitmap_idx)->getArrayNumElements();
                std::tie(ptr, bitmap) = deserializeBitmap(env, builder, ptr, num_bitmap_bits);
                bitmap_idx++;
            }
            // step 2: decode presence map if exists and load to array
            if(struct_dict_has_presence_map(dict_type)) {
                assert(stype->isStructTy() && stype->getStructElementType(bitmap_idx)->isArrayTy());
                size_t num_presence_bits = stype->getStructElementType(bitmap_idx)->getArrayNumElements();
                std::tie(ptr, presence_map) = deserializeBitmap(env, builder, ptr, num_presence_bits);
            }

            // step 3: go over entries and load if present.
            // count how many fields there are => important to compute offsets!
            size_t num_fields = struct_dict_get_field_count(dict_type);
            logger.debug("found " + pluralize(num_fields, "field") + " to deserialize.");

            size_t field_index = 0; // used in order to compute offsets!
            llvm::Value* varLengthOffset = env.i64Const(0); // current offset from varfieldsstart ptr
            llvm::Value* varFieldsStartPtr = builder.CreateGEP(ptr, env.i64Const(sizeof(int64_t) * num_fields)); // where in memory the variable field storage starts!

            // get indices to properly decode
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto value_type = std::get<1>(entry);
                auto t_indices = struct_dict_get_indices(dict_type, access_path);

                // special case list: --> needs extra care
                if(value_type.isOptionType())
                    value_type = value_type.getReturnType();
                if(python::Type::EMPTYLIST != value_type && value_type.isListType()) {

                    // list is always stored as var-length field, so extract info
                    // load info from ptr & move
                    auto info = builder.CreateLoad(builder.CreateBitOrPointerCast(ptr, env.i64ptrType()));

                    // unpack offset and size from info
                    llvm::Value* offset=nullptr; llvm::Value *size=nullptr;
                    std::tie(offset, size) = unpack_offset_and_size(builder, info);

                    // get the pointer to the data
                    auto data_ptr = builder.CreateGEP(ptr, offset);

                    // move decode ptr.
                    ptr = builder.CreateGEP(ptr, env.i64Const(sizeof(int64_t)));

                    // call list decode and store result in struct!
                    llvm::Value* end_ptr= nullptr;
                    SerializableValue list_val;
                    std::tie(end_ptr, list_val) = list_deserialize_from(env, builder, data_ptr, value_type);

                    // store value in struct (pointer should be sufficient)
                    struct_dict_store_value(env, builder, list_val, dict_ptr, dict_type, access_path);

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

                if(!is_varlength_field) {
                    llvm::Value * value = nullptr;

                    // simple: just load data and copy!
                    // make sure it's bool/i64/64 -> these are the only fixed size fields!
                    assert(value_type == python::Type::BOOLEAN || value_type == python::Type::I64 || value_type == python::Type::F64);

                    llvm::Type* llvm_value_type_ptr = value_type == python::Type::F64 ? env.doublePointerType() : env.i64ptrType();

                    // store with casting
                    auto casted_src_ptr = builder.CreateBitOrPointerCast(ptr, llvm_value_type_ptr);
                    value = builder.CreateLoad(casted_src_ptr);
                    // store into struct ptr
                    struct_dict_store_value(env, builder, dict_ptr, dict_type, access_path, value); // always store value
                    struct_dict_store_size(env, builder, dict_ptr, dict_type, access_path, env.i64Const(sizeof(int64_t)));
                    ptr = builder.CreateGEP(ptr, env.i64Const(sizeof(int64_t)));
                } else {
                    // more complex:
                    // for now, only string supported... => load and fix!
                    if(value_type != python::Type::STRING)
                        throw std::runtime_error("unsupported type " + value_type.desc() + " encountered! ");

                    // load info from ptr & move
                    auto info = builder.CreateLoad(builder.CreateBitOrPointerCast(ptr, env.i64ptrType()));


                    // unpack offset and size from info
                    llvm::Value* offset=nullptr; llvm::Value *size=nullptr;

                    std::tie(offset, size) = unpack_offset_and_size(builder, info);

                    // get the pointer to the data
                    auto data_ptr = builder.CreateGEP(ptr, offset);

                    // store (always safe to do)
                    struct_dict_store_value(env, builder, dict_ptr, dict_type, access_path, data_ptr); // always store value
                    struct_dict_store_size(env, builder, dict_ptr, dict_type, access_path, size);

                    // move decode ptr.
                    ptr = builder.CreateGEP(ptr, env.i64Const(sizeof(int64_t)));
                }

                // serialized field -> inc index!
                field_index++;
            }

            // move ptr to end!
            ptr = builder.CreateGEP(ptr, varLengthOffset);
            llvm::Value* deserialized_size = builder.CreatePtrDiff(ptr, original_mem_start_ptr);
#ifndef NDEBUG
            env.printValue(builder, deserialized_size, "deserialized struct_dict from bytes: ");
#endif
            return v;
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

            // check bitmap size (i.e. multiples of 64bit)
            auto bitmap_size = struct_dict_bitmap_size_in_bytes(dict_type);

            llvm::Value* size = env.i64Const(bitmap_size);

            auto bytes8 = env.i64Const(sizeof(int64_t));

            // start: serialize bitmaps
            // 1. null-bitmap
            size_t bitmap_offset = 0;
            if(struct_dict_has_bitmap(dict_type)) {
                //                llvm::Value* bitmap = nullptr;
                //                if(ptr->getType()->isPointerTy()) {
                //                    auto bitmap_idx = CreateStructGEP(builder, ptr, bitmap_offset);
                //                    bitmap = builder.CreateLoad(bitmap_idx);
                //                } else {
                //                    bitmap = builder.CreateExtractValue(ptr, std::vector<unsigned>(1, bitmap_offset));
                //                }
                auto bitmap = CreateStructLoad(builder, ptr, bitmap_offset);
                dest_ptr = serializeBitmap(env, builder, bitmap, dest_ptr);
                bitmap_offset++;
            }
            // 2. presence-bitmap
            if(struct_dict_has_presence_map(dict_type)) {
                auto presence_map = CreateStructLoad(builder, ptr, bitmap_offset);
                dest_ptr = serializeBitmap(env, builder, presence_map, dest_ptr);
                bitmap_offset++;
            }

            // count how many fields there are => important to compute offsets!
            size_t num_fields = struct_dict_get_field_count(dict_type);
            logger.debug("found " + pluralize(num_fields, "field") + " to serialize.");

            size_t field_index = 0; // used in order to compute offsets!
            llvm::Value* varLengthOffset = env.i64Const(0); // current offset from varfieldsstart ptr
            llvm::Value* varFieldsStartPtr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t) * num_fields)); // where in memory the variable field storage starts!

//            // print debug info
//            env.printValue(builder, builder.CreatePtrDiff(varFieldsStartPtr, original_dest_ptr), "var fields begin at byte: ");
//            env.printValue(builder, builder.CreatePtrDiff(dest_ptr, original_dest_ptr), "current dest_ptr position at byte: ");
//            env.printValue(builder, varLengthOffset, "var length so far in bytes: ");

//  #define TRACE_STRUCT_SERIALIZATION
            // get indices to properly decode
            for(auto entry : entries) {
                auto access_path = std::get<0>(entry);
                auto value_type = std::get<1>(entry);
                auto t_indices = struct_dict_get_indices(dict_type, access_path);

#ifdef TRACE_STRUCT_SERIALIZATION
                auto path_str = json_access_path_to_string(access_path, value_type, std::get<2>(entry));

                // check
                auto present_idx = std::get<1>(t_indices);
                if(present_idx >= 0) {
                    env.printValue(builder, struct_dict_load_present(env, builder, ptr, dict_type, access_path), path_str + ": present_idx=" + std::to_string(present_idx) + " is present: ");
                }

                // // print debug info
                // env.printValue(builder, builder.CreatePtrDiff(varFieldsStartPtr, original_dest_ptr), "var fields begin at byte: ");
                // env.printValue(builder, builder.CreatePtrDiff(dest_ptr, original_dest_ptr), "current dest_ptr position at byte: ");
                // env.printValue(builder, varLengthOffset, "var length so far in bytes: ");
#endif
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
                    size_t cur_to_var_start_offset = (num_fields - field_index) * sizeof(int64_t); // no +1 here b.c. we do not store the varsize length unlike in a tuple.
                    auto offset = builder.CreateAdd(env.i64Const(cur_to_var_start_offset), varLengthOffset);

                    auto varDest = builder.CreateGEP(varFieldsStartPtr, varLengthOffset);

                    // env.printValue(builder, builder.CreatePtrDiff(varDest, original_dest_ptr), "list var start from start ptr: ");
                    // env.printValue(builder, builder.CreatePtrDiff(varDest, dest_ptr), "offset (calc) vardest to dest ptr: ");

                    // call list function
                    list_serialize_to(env, builder, list_ptr, list_type, varDest);

                    // pack offset and size into 64bit!
                    auto info = pack_offset_and_size(builder, offset, list_size_in_bytes);

                    // store info away
                    auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
                    builder.CreateStore(info, casted_dest_ptr);

                    // env.printValue(builder, offset, "encoded list " + list_type.desc() + " with offset: ");
                    // env.printValue(builder, list_size_in_bytes, "encoding to field " + std::to_string(field_index) + " list of size: ");
                    //  env.debugPrint(builder, path_str + ": encoding to field = " + std::to_string(field_index));

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

                    // env.debugPrint(builder, path_str + ": encoding to field = " + std::to_string(field_index));

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
                    size_t cur_to_var_start_offset = (num_fields - field_index) * sizeof(int64_t);
                    auto offset = builder.CreateAdd(env.i64Const(cur_to_var_start_offset), varLengthOffset);

                    auto varDest = builder.CreateGEP(varFieldsStartPtr, varLengthOffset);
                    builder.CreateMemCpy(varDest, 0, value, 0, value_size); // for string, simple value copy!

                    // pack offset and size into 64bit!
                    auto info = pack_offset_and_size(builder, offset, value_size);

                    // store info away
                    auto casted_dest_ptr = builder.CreateBitOrPointerCast(dest_ptr, env.i64ptrType());
                    builder.CreateStore(info, casted_dest_ptr);
                    // env.debugPrint(builder, path_str + ": encoding to field = " + std::to_string(field_index));
                    dest_ptr = builder.CreateGEP(dest_ptr, env.i64Const(sizeof(int64_t)));

                    varLengthOffset = builder.CreateAdd(varLengthOffset, value_size);
                }

                // serialized field -> inc index!
                field_index++;
            }

#ifdef TRACE_STRUCT_SERIALIZATION
            // print debug info
            env.printValue(builder, builder.CreatePtrDiff(varFieldsStartPtr, original_dest_ptr), "var fields begin at byte: ");
            env.printValue(builder, builder.CreatePtrDiff(dest_ptr, original_dest_ptr), "current dest_ptr position at byte: ");
            env.printValue(builder, varLengthOffset, "var length so far in bytes: ");
#endif

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

            // // print out paths
            // for(const auto& entry : entries)
            //    std::cout<<json_access_path_to_string(std::get<0>(entry), std::get<1>(entry), std::get<2>(entry))<<std::endl;

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
                    auto jt = struct_indices.find(access_path);
                    if(jt != struct_indices.end()) {
                        // note: following will only work for single element OR a nested struct that is maybe
                        auto indices = struct_indices.at(access_path);
                        std::tie(bitmap_idx, present_idx, field_idx, size_idx) = indices;
                    }
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

        // this function can be also used to implement some basic dict stuff
        // like:
        // x = {}
        // x['a'] = 'test'
        // x['b'] = 20
        // ...
        SerializableValue struct_dict_upcast(LLVMEnvironment& env,
                                             llvm::IRBuilder<>& builder,
                                             const SerializableValue& src,
                                             const python::Type& src_type,
                                             const python::Type& dest_type) {
            // make sure scenario is supported
            assert((src_type == python::Type::EMPTYDICT || src_type.isStructuredDictionaryType()) && dest_type.isStructuredDictionaryType());

            // check that llvm src val is correct.
            assert(src.val);
            auto src_val_llvm_type = src.val->getType();

            assert(src_val_llvm_type == env.getEmptyDictType() || src_val_llvm_type == env.getOrCreateStructuredDictType(src_type)
            || src_val_llvm_type == env.getOrCreateStructuredDictType(src_type)->getPointerTo());

            auto& logger = Logger::instance().logger("codegen");

            using namespace llvm;
            using namespace std;

            // allocate dest ptr and zero
            auto llvm_type = env.getOrCreateStructuredDictType(dest_type);
            auto dest_ptr = env.CreateFirstBlockAlloca(builder, llvm_type);
            struct_dict_mem_zero(env, builder, dest_ptr, dest_type);

            // simple case: EMPTYDICT -> StructDict
            if(src_type == python::Type::EMPTYDICT) {
                // simply return
                return SerializableValue(dest_ptr, nullptr, nullptr);
            }

            // more complex case: Basically insert all the data from the other dict while upcasting values.
            // for this, access paths are required of both types.
            flattened_struct_dict_entry_list_t src_paths;
            flattened_struct_dict_entry_list_t dest_paths;
            flatten_recursive_helper(src_paths, src_type);
            flatten_recursive_helper(dest_paths, dest_type);

            // for faster lookup, build dict
            std::unordered_map<access_path_t, std::tuple<python::Type, bool>> m;
            for(const auto& src_entry : src_paths) {
                auto src_access_path = std::get<0>(src_entry);
                auto src_value_type = std::get<1>(src_entry);
                auto src_always_present = std::get<2>(src_entry);
                m[src_access_path] = std::make_tuple(src_value_type, src_always_present);
            }

            // go through all dest access paths and check whether they exist in src paths
            for(const auto& dst_entry : dest_paths) {
                auto dst_access_path = std::get<0>(dst_entry);
                auto dst_value_type = std::get<1>(dst_entry);
                auto dst_always_present = std::get<2>(dst_entry);
                if(!dst_always_present) {
                    // store false for now
                    struct_dict_store_present(env, builder, dest_ptr, dest_type, dst_access_path, env.i1Const(false));
                }

                // check whether path exists within src_paths
                auto it = m.find(dst_access_path);
                if(it != m.end()) {
                    auto src_value_type = std::get<0>(it->second);
                    auto src_always_present = std::get<1>(it->second);

                    BasicBlock* bNext = nullptr;
                    // load and store value iff present
                    if(!src_always_present) {
                        // need to create basic blocks: I.e., store only if present...
                        auto& ctx = env.getContext();
                        BasicBlock* bStore = BasicBlock::Create(ctx, "store_from_src", builder.GetInsertBlock()->getParent());
                        bNext = BasicBlock::Create(ctx, "next_element", builder.GetInsertBlock()->getParent());

                        auto is_present = struct_dict_load_present(env, builder, src.val, src_type, dst_access_path);
                        builder.CreateCondBr(is_present, bStore, bNext);
                        builder.SetInsertPoint(bStore);
                        // store (same code as below)
                    }

                    // Store & upcast
                    // always present, no check necessary.
                    auto src_element = struct_dict_load_value(env, builder, src.val, src_type, dst_access_path);
                    // type upcast necessary?
                    if(src_value_type != dst_value_type)
                        src_element = env.upcastValue(builder, src_element, src_value_type, dst_value_type);
                    struct_dict_store_value(env, builder, src_element, dest_ptr, dest_type, dst_access_path);
                    struct_dict_store_present(env, builder, dest_ptr, dest_type, dst_access_path, env.i1Const(true));

                    // connect blocks from presence test
                    if(!src_always_present) {
                        builder.CreateBr(bNext);
                        builder.SetInsertPoint(bNext);
                    }
                } else {
                    // should be maybe... -> else warn?
                    if(dst_always_present) {
                        auto path = access_path_to_str(dst_access_path);
                        logger.debug("Found access path " + path + " which is always present, but is not flagged as maybe and src has no value for it. Is this correct? -> only ok if keycheck was added for normal-case code.");
                    }
                }
            }

            return SerializableValue(dest_ptr, nullptr, nullptr);
        }
    }
}