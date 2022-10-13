//
// Created by leonhard on 9/29/22.
//

#include "HyperUtils.h"

#include <physical/experimental/JsonHelper.h>
#include <physical/experimental/JSONParseRowGenerator.h>

class JsonTuplexTest : public HyperPyTest {};


TEST_F(JsonTuplexTest, BasicLoad) {
    using namespace tuplex;
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0"); // start single-threaded
    Context ctx(opt);
    bool unwrap_first_level = true;

    // this seems to work.
    // should show:
    // 10, None, 3.414
    // 2, 42, 12.0      <-- note the order correction
    // None, None, 2    <-- note the order (fallback/general case)
    unwrap_first_level = true;
    ctx.json("../resources/ndjson/example2.json", unwrap_first_level).show();
    auto v = ctx.json("../resources/ndjson/example2.json", unwrap_first_level).collectAsVector();
    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "(10,None,3.41400)");
    EXPECT_EQ(v[1].toPythonString(), "(2,42,12.00000)");
    EXPECT_EQ(v[2].toPythonString(), "(None,None,2)");


    unwrap_first_level = false; // --> requires implementing/adding decoding of struct dict...
    ctx.json("../resources/ndjson/example2.json", unwrap_first_level).show();
    // // large json file (to make sure buffers etc. work
    // ctx.json("/data/2014-10-15.json").show();
}

// dev func here
namespace tuplex {
    Field json_string_to_field(const std::string& s, const python::Type& type) {
        // base cases:
        if(type == python::Type::NULLVALUE) {
            return Field::null();
        } else if(type == python::Type::BOOLEAN) {
            return Field(stringToBool(s));
        } else if(type == python::Type::I64) {
            return Field(parseI64String(s));
        } else if(type == python::Type::F64) {
            return Field(parseF64String(s));
        } else if(type.isOptionType()) {
            if(s == "null")
                return Field::null();
            else {
                return json_string_to_field(s, type.getReturnType());
            }
        } else {
            throw std::runtime_error("Unknown type " + type.desc() + " encountered.");
        }
    }

    // to pyobject, recursively
    PyObject* json_string_to_pyobject(const std::string& s, const python::Type& type) {
        using namespace std;

        if(type.isStructuredDictionaryType()) {

            // parse string via simdjson into dom!
            simdjson::dom::parser parser;
            auto obj = parser.parse(s);
            assert(obj.is_object());

            auto dict_obj = PyDict_New();

            // special case: go over entries and decode string!
            for(auto kv_pair : type.get_struct_pairs()) {
                // what is the type?

                // HACK: only support string key types for now!
                // issue will be two keys with same value and different types -> need to be encoded in dict.
                // could solve by using the key as-is -> i.e. python escaped strings (b.c. other values won't start with '', use b'' for pickled object)
                if(kv_pair.keyType != python::Type::STRING)
                    throw std::runtime_error("only string key type supported so far in decode");

                auto key = str_value_from_python_raw_value(kv_pair.key);
                // get string from there
                auto raw_str = minify(obj[key]);

                PyObject *el_obj = nullptr;

                // special cases: struct, list, (dict?)
                el_obj = json_string_to_pyobject(raw_str, kv_pair.valueType);

                PyDict_SetItemString(dict_obj, key.c_str(), el_obj);
            }
            return dict_obj;
        } else if(type.isListType()) {
            simdjson::dom::parser parser;
            auto arr = parser.parse(s);
            assert(arr.is_array());
            auto element_type = type.elementType();
            std::vector<PyObject*> elements;
            for(const auto& el : arr) {
                // what is the element type?
                auto el_str = simdjson::minify(el);
                elements.push_back(json_string_to_pyobject(el_str, element_type));
            }

            // combine
            auto list_obj = PyList_New(elements.size());
            for(unsigned i = 0; i < elements.size(); ++i)
                PyList_SET_ITEM(list_obj, i, elements[i]);
            return list_obj;
        }         // base cases:
        else if(type == python::Type::NULLVALUE) {
            Py_XINCREF(Py_None);
            return Py_None;
        } else if(type == python::Type::BOOLEAN) {
            auto b = stringToBool(s);
            return python::boolToPython(b);
        } else if(type == python::Type::I64) {
            auto val = parseI64String(s);
            return PyLong_FromLongLong(val);
        } else if(type == python::Type::F64) {
            auto val = parseF64String(s);
            return PyFloat_FromDouble(val);
        } else if(type == python::Type::STRING) {
            auto unescaped = unescape_json_string(s);
            return python::PyString_FromString(unescaped.c_str());
        } else if(type.isOptionType()) {
            if(s == "null") {
               Py_XINCREF(Py_None);
               return Py_None;
            } else {
                return json_string_to_pyobject(s, type.getReturnType());
            }
        }else {
            throw std::runtime_error("unsupported type to decode");
        }
        Py_XINCREF(Py_None); // <-- unknown, use None.
        return Py_None;
    }


    bool struct_dict_field_present(const std::unordered_map<codegen::access_path_t, std::tuple<int, int, int, int>>& indices,
                                   const codegen::access_path_t& path, const std::vector<uint64_t>& presence_map) {
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
            codegen::access_path_t parent_path(path.begin(), path.end() - 1);
            return struct_dict_field_present(indices, parent_path, presence_map);
        } else {
            assert(is_present);
            return is_present; // should be true here...
        }
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

    std::vector<std::string> access_path_to_json_keys(const codegen::access_path_t& path) {
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

    std::string decodeStructDictFromBinary(const python::Type& dict_type, const uint8_t* buf, size_t buf_size) {
        assert(dict_type.isStructuredDictionaryType());

        codegen::flattened_struct_dict_entry_list_t entries;
        codegen::flatten_recursive_helper(entries, dict_type, {});
        auto indices = codegen::struct_dict_load_indices(dict_type);

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

            // go over elements
            for(unsigned i = 0; i < presence_map.size(); ++i) {
                for(unsigned j = 0; j < 64; ++j) {
                    if(presence_map[i] & (0x1ull << j))
                        std::cout<<"presence bit "<<j + 64 * i<<" set."<<std::endl;
                }
            }

        }

        // start decode:
        auto ptr = buf;
        size_t remaining_bytes = buf_size;

        // check if dict type has bitmaps, if so decode!
        if(codegen::struct_dict_has_bitmap(dict_type)) {
            // decode
            auto size_to_decode = sizeof(uint64_t) * num_option_bitmap_elements;
            assert(remaining_bytes >= size_to_decode);
            memcpy(&bitmap[0], ptr, size_to_decode);
            ptr += size_to_decode;
            remaining_bytes -= size_to_decode;
        }
        if(codegen::struct_dict_has_presence_map(dict_type)) {
            auto size_to_decode = sizeof(uint64_t) * num_maybe_bitmap_elements;
            assert(remaining_bytes >= size_to_decode);
            memcpy(&presence_map[0], ptr, size_to_decode);
            ptr += size_to_decode;
            remaining_bytes -= size_to_decode;
        }

        // go through entries & decode!
        // get indices to properly decode
        // this func is basically modeled after struct_dict_serialize_to_menory
        unsigned field_index = 0;

        std::unordered_map<codegen::access_path_t, std::string> elements;

        auto tree = std::make_unique<StrJsonTree>();

        for(auto entry : entries) {
            auto access_path = std::get<0>(entry);
            auto value_type = std::get<1>(entry);
            auto always_present = std::get<2>(entry);
            auto t_indices = indices.at(access_path);

            auto path_str = codegen::json_access_path_to_string(access_path, value_type, always_present);

            // debug:
            if(path_str.find("'repo' (str) -> 'id' (str) -> i64") != std::string::npos) {
                std::cerr<<"debug found"<<std::endl;
            }


            int bitmap_idx = -1, present_idx = -1, value_idx = -1, size_idx = -1;
            std::tie(bitmap_idx, present_idx, value_idx, size_idx) = t_indices;

            assert(field_index < field_count);
            auto field_ptr = ptr + sizeof(uint64_t) * field_index;

            bool is_null = false;
            bool is_present = struct_dict_field_present(indices, access_path, presence_map);

            std::cout<<"decoding "<<path_str<<" from field= "<<field_index<<" present= "<<std::boolalpha<<is_present<<std::endl;

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

                // @TODO. for now, save empty list
                elements[access_path] = "[]";
                tree->add(access_path_to_json_keys(access_path), "[]");

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

        // sort elements after length of access path -> then do a work list algorithm to print out everything.
        std::vector<std::pair<codegen::access_path_t, std::string>> data(elements.begin(), elements.end());
        std::sort(data.begin(), data.end(), [](const std::pair<codegen::access_path_t, std::string>& lhs,
                                                       const std::pair<codegen::access_path_t, std::string>& rhs) {
            return lhs.first.size() < rhs.first.size();
        });

        for(auto p : data) {
            std::cout<<codegen::access_path_to_str(p.first)<<":  "<<p.second<<std::endl;
        }

        return tree->to_json();
//        // @TODO.
//        for(auto p : elements) {
//            nlohmann::json& obj = j;
//            auto access_path = p.first;
//            for(auto& atom : access_path)
//                obj = obj[std::get<0>(atom)];
//
//            // save
//            obj = p.second;
//
//            std::cout<<codegen::access_path_to_str(p.first)<<":  "<<p.second<<std::endl;
//        }
//
//        // debug
//        std::cout<<j.dump(4)<<std::endl;
//
//        return j.dump();
    }
}


TEST_F(JsonTuplexTest, JsonToRow) {
    using namespace tuplex;
    using namespace std;
    // dev for decoding struct dicts (unfortunately necessary...)
    // need memory -> Row

    // and Row -> python

    auto content = "{\"repo\":{\"id\":355634,\"url\":\"https://api.github.dev/repos/projectblacklight/blacklight\",\"name\":\"projectblacklight/blacklight\"},\"type\":\"PushEvent\",\"org\":{\"gravatar_id\":\"6cb76a4a521c36d96a0583e7c45eaf95\",\"id\":120516,\"url\":\"https://api.github.dev/orgs/projectblacklight\",\"avatar_url\":\"https://secure.gravatar.com/avatar/6cb76a4a521c36d96a0583e7c45eaf95?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-org-420.png\",\"login\":\"projectblacklight\"},\"public\":true,\"created_at\":\"2011-02-12T00:00:00Z\",\"payload\":{\"shas\":[[\"d3da39ab96a2caecae5d526596a04820c6f848a6\",\"b31a437f57bdf70558bed8ac28790a53e8174b87@stanford.edu\",\"We have to stay with cucumber < 0.10 and rspec < 2, otherwise our tests break. I updated the gem requirements to reflect this, so people won't accidentally upgrade to a gem version that's too new when they run rake gems:install\",\"Bess Sadler\"],[\"898150b7830102cdc171cbd4304b6a783101c3e3\",\"b31a437f57bdf70558bed8ac28790a53e8174b87@stanford.edu\",\"Removing unused cruise control tasks. Also, we shouldn't remove the coverage.data file, so we can look at the coverage data over time.\",\"Bess Sadler\"]],\"repo\":\"projectblacklight/blacklight\",\"actor\":\"bess\",\"ref\":\"refs/heads/master\",\"size\":2,\"head\":\"898150b7830102cdc171cbd4304b6a783101c3e3\",\"actor_gravatar\":\"887fa2fcd0cf1cbdc6dc43e5524f33f6\",\"push_id\":24643024},\"actor\":{\"gravatar_id\":\"887fa2fcd0cf1cbdc6dc43e5524f33f6\",\"id\":65608,\"url\":\"https://api.github.dev/users/bess\",\"avatar_url\":\"https://secure.gravatar.com/avatar/887fa2fcd0cf1cbdc6dc43e5524f33f6?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png\",\"login\":\"bess\"},\"id\":\"1127195475\"}";

    // parse as row, and then convert to python!
    auto rows = parseRowsFromJSON(content, nullptr, false);

    ASSERT_EQ(rows.size(), 1);
    auto row = rows.front();

    python::lockGIL();
    try {
        auto obj = json_string_to_pyobject(row.getString(0), row.getType(0));
        PyObject_Print(obj, stdout, 0); std::cout<<std::endl;
    } catch(...) {
        std::cerr<<"exception occurred"<<std::endl;
    }
    python::unlockGIL();

    // let's start with JSON string -> Row.
    //auto f = json_string_to_field("{")
}

namespace tuplex {
    namespace codegen {
        // helper function to generate a quick json-buffer parse and match against general-case check
        std::function<int64_t(const uint8_t*, size_t, uint8_t**, size_t*)> generateJsonTestParse(JITCompiler& jit, const python::Type& dict_type) {
            using namespace llvm;
            using namespace std;

            assert(dict_type.isStructuredDictionaryType());

            std::string func_name = "json_test_parse";

            LLVMEnvironment env;

            // create func
            auto& ctx = env.getContext();
            auto func = getOrInsertFunction(env.getModule().get(), func_name, ctypeToLLVM<int64_t>(ctx), ctypeToLLVM<uint8_t*>(ctx),
                    ctypeToLLVM<int64_t>(ctx),
                            ctypeToLLVM<uint8_t**>(ctx),
                                    ctypeToLLVM<int64_t*>(ctx));

            auto argMap = mapLLVMFunctionArgs(func, {"buf", "buf_size", "out", "out_size"});

            BasicBlock* bBody = BasicBlock::Create(ctx, "entry", func);
            IRBuilder<> builder(bBody);

            // create mismatch block
            BasicBlock* bbMismatch = BasicBlock::Create(ctx, "mismatch", func);

            // init parser
            auto Finitparser = getOrInsertFunction(env.getModule().get(), "JsonParser_Init", env.i8ptrType());

            auto parser = builder.CreateCall(Finitparser, {});

            auto F = getOrInsertFunction(env.getModule().get(), "JsonParser_open", env.i64Type(), env.i8ptrType(),
                                         env.i8ptrType(), env.i64Type());
            auto buf = argMap["buf"];
            auto buf_size = argMap["buf_size"];
            auto open_rc = builder.CreateCall(F, {parser, buf, buf_size});

            // get initial object
            // => this is from parser
            auto Fgetobj = getOrInsertFunction(env.getModule().get(), "JsonParser_getObject", env.i64Type(),
                                               env.i8ptrType(), env.i8ptrType()->getPointerTo(0));

            auto row_object_var = env.CreateFirstBlockVariable(builder, env.i8nullptr(), "row_object");
            auto obj_var = row_object_var;

            builder.CreateCall(Fgetobj, {parser, obj_var});

            // don't forget to free everything...
            // alloc variable
            auto struct_dict_type = create_structured_dict_type(env, dict_type);
            auto row_var = env.CreateFirstBlockAlloca(builder, struct_dict_type);
            struct_dict_mem_zero(env, builder, row_var, dict_type); // !!! important !!!

            // parse now
            JSONParseRowGenerator gen(env, dict_type, bbMismatch);
            gen.parseToVariable(builder, builder.CreateLoad(obj_var), row_var);

            // ok, now serialize
            auto s_length = struct_dict_serialized_memory_size(env, builder, row_var, dict_type).val;
            env.printValue(builder, s_length, "serialized size is: ");
            auto out_buf = env.cmalloc(builder, s_length);

            // serialize
            struct_dict_serialize_to_memory(env, builder, row_var, dict_type, out_buf);

            builder.CreateStore(s_length, argMap["out_size"]);
            builder.CreateStore(out_buf, argMap["out"]);

            // do not care about leaks in this function...
            builder.CreateRet(env.i64Const(0));

            builder.SetInsertPoint(bbMismatch);
            builder.CreateRet(env.i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));

            // --- gen done ---, compile...
            bool rc_compile = jit.compile(std::move(env.getModule()));

            if(!rc_compile)
                throw std::runtime_error("compile error for module");

            auto ptr = jit.getAddrOfSymbol(func_name);
            return reinterpret_cast<int64_t(*)(const uint8_t*, size_t, uint8_t**, size_t*)>(ptr);
        }
    }
}


TEST_F(JsonTuplexTest, BinToPython) {
    using namespace tuplex;
    using namespace std;

    auto test_type_str = "(Struct[(str,'actor'->Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)]),(str,'created_at'->str),(str,'id'->str),(str,'org'=>Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)]),(str,'payload'->Struct[(str,'action'=>str),(str,'actor'=>str),(str,'actor_gravatar'=>str),(str,'comment_id'=>i64),(str,'commit'=>str),(str,'desc'=>null),(str,'head'=>str),(str,'name'=>str),(str,'object'=>str),(str,'object_name'=>Option[str]),(str,'page_name'=>str),(str,'push_id'=>i64),(str,'ref'=>str),(str,'repo'=>str),(str,'sha'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'snippet'=>str),(str,'summary'=>null),(str,'title'=>str),(str,'url'=>str)]),(str,'public'->boolean),(str,'repo'->Struct[(str,'id'=>i64),(str,'name'->str),(str,'url'->str)]),(str,'type'->str)])";

    auto type = python::Type::decode(test_type_str);

    auto dict_type = type.parameters().front();

    runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

    JITCompiler jit;
    auto f = codegen::generateJsonTestParse(jit, dict_type);

    // call f
    size_t size = 0;
    uint8_t* buf = nullptr;

    // input buffer and size
    char input_buf[] = "{\"repo\":{\"id\":1357116,\"url\":\"https://api.github.dev/repos/ezmobius/super-nginx\",\"name\":\"ezmobius/super-nginx\"},\"type\":\"WatchEvent\",\"public\":true,\"created_at\":\"2011-02-12T00:00:06Z\",\"payload\":{\"repo\":\"ezmobius/super-nginx\",\"actor\":\"sosedoff\",\"actor_gravatar\":\"cd73497eb3c985f302723424c3fa5b50\",\"action\":\"started\"},\"actor\":{\"gravatar_id\":\"cd73497eb3c985f302723424c3fa5b50\",\"id\":71051,\"url\":\"https://api.github.dev/users/sosedoff\",\"avatar_url\":\"https://secure.gravatar.com/avatar/cd73497eb3c985f302723424c3fa5b50?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png\",\"login\":\"sosedoff\"},\"id\":\"1127195541\"}";
    size_t input_buf_size = strlen(input_buf) + 1;
    auto rc = f(reinterpret_cast<const uint8_t*>(input_buf), input_buf_size, &buf, &size);
    EXPECT_EQ(rc, 0);
    ASSERT_TRUE(size > 0);

    // now decode
    auto json_str = decodeStructDictFromBinary(dict_type, buf, size);
    std::cout<<json_str<<std::endl;


//    // load data from file
//    auto data = fileToString(string("../resources/struct_dict_data.bin"));
//    EXPECT_EQ(data.size(), 726); // size
//
//    // decode
//    auto ptr = reinterpret_cast<const uint8_t*>(data.c_str());
//
//    uint32_t offset = *((uint64_t*)ptr) & 0xFFFFFFFF;
//    uint32_t size = *((uint64_t*)ptr) >> 32u;
//
//    std::cout<<"offset: "<<offset<<" size: "<<size<<std::endl;
//
//    auto start_ptr = ptr + offset; // this is where the data starts
//
//    // for ref: this is what should be contained within the buffer:
//    // {"repo":{"id":1357116,"url":"https://api.github.dev/repos/ezmobius/super-nginx","name":"ezmobius/super-nginx"},"type":"WatchEvent","public":true,"created_at":"2011-02-12T00:00:06Z","payload":{"repo":"ezmobius/super-nginx","actor":"sosedoff","actor_gravatar":"cd73497eb3c985f302723424c3fa5b50","action":"started"},"actor":{"gravatar_id":"cd73497eb3c985f302723424c3fa5b50","id":71051,"url":"https://api.github.dev/users/sosedoff","avatar_url":"https://secure.gravatar.com/avatar/cd73497eb3c985f302723424c3fa5b50?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png","login":"sosedoff"},"id":"1127195541"}
//    // dump buffer
//    core::asciidump(std::cout, start_ptr, size, true);
//
//    // now decode
//    auto json_str = decodeStructDictFromBinary(type.parameters().front(), start_ptr, size);
//
//    std::cout<<json_str<<std::endl;
//
//    ASSERT_TRUE(!json_str.empty());
}

TEST_F(JsonTuplexTest, GithubLoad) {
    using namespace tuplex;
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0"); // start single-threaded
    opt.set("tuplex.resolveWithInterpreterOnly", "false");
    Context ctx(opt);
    bool unwrap_first_level = true;

    // check ref file lines
    string ref_path = "../resources/ndjson/github.json";
    auto lines = splitToLines(fileToString(ref_path));
    auto ref_row_count = lines.size();

//    {
//        // first test -> simple load in both unwrap and no unwrap mode.
//        unwrap_first_level = true;
//        auto& ds = ctx.json(ref_path, unwrap_first_level);
//        // no columns
//        EXPECT_TRUE(!ds.columns().empty());
//        auto v = ds.collectAsVector();
//
//        EXPECT_EQ(v.size(), ref_row_count);
//    }

    {
        // first test -> simple load in both unwrap and no unwrap mode.
        unwrap_first_level = false;
        auto& ds = ctx.json(ref_path, unwrap_first_level);
        // no columns
        EXPECT_TRUE(ds.columns().empty());
        auto v = ds.collectAsVector();

        EXPECT_EQ(v.size(), ref_row_count);
    }

//    auto& ds = ctx.json("../resources/ndjson/github_two_rows.json", unwrap_first_level);
    //// could extract columns via keys() or so? -> no support for this yet.
    // ds.map(UDF("lambda x: (x['type'], int(x['id']))")).show();
    //ds.show();


    //std::cout<<"json malloc report:\n"<<codegen::JsonMalloc_Report()<<std::endl;

    //ds.map(UDF("lambda x: x['type']")).unique().show();

    // Next steps: -> should be able to load any github file (start with the small samples)
    //             -> use maybe show(5) to speed things up (--> sample manipulation?)
    //             -> add Python API to tuplex for dealing with JSON?
    //             -> design example counting events across files maybe?
    //             -> what about pushdown then?

//    // simple func --> this works only with unwrapping!
// unwrap_first_level = true;
//    ctx.json("../resources/ndjson/github.json", unwrap_first_level)
//       .filter(UDF("lambda x: x['type'] == 'PushEvent'"))
//       .mapColumn("id", UDF("lambda x: int(x)"))
//       .selectColumns(std::vector<std::string>({"repo", "type", "id"}))
//       .show();

    // @TODO: tests
    // -> unwrap should be true and the basic show pipeline work as well.
    // -> a decode of stored struct dict/list into JSON would be cool.
    // -> a decode of struct dict as python would be cool.
    // -> assigning to dictionaries, i.e. the example below working would be an improvement.
    // finally, run the show (with limit!) example on ALL json data for the days to prove it works.
}

TEST_F(JsonTuplexTest, StructAndFT) {
    using namespace tuplex;
    using namespace std;
    using namespace tuplex::codegen;
    using namespace llvm;

    LLVMEnvironment env;

    auto& ctx = env.getContext();
    auto t = python::Type::decode("(Struct[(str,'column1'->Struct[(str,'a'->i64),(str,'b'->i64),(str,'c'->Option[i64])])])");
    FlattenedTuple ft(&env);
    ft.init(t);
    auto F = getOrInsertFunction(env.getModule().get(), "test_func", ctypeToLLVM<int64_t>(ctx), ft.getLLVMType());
    auto bEntry = BasicBlock::Create(ctx, "entry", F);

    llvm::IRBuilder<> builder(bEntry);
    builder.CreateRet(env.i64Const(0));
    auto ir = moduleToString(*env.getModule().get());

    std::cout<<"\n"<<ir<<std::endl;
    EXPECT_TRUE(ir.size() > 0);

}

TEST_F(JsonTuplexTest, TupleFlattening) {
    using namespace tuplex;
    using namespace std;
    std::string type_str = "(Struct[(str,'id'->i64),(str,'url'->str),(str,'name'->str)],str,Option[Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)]],boolean,str,Struct[(str,'shas'->List[List[str]]),(str,'repo'->str),(str,'actor'->str),(str,'ref'->str),(str,'size'->i64),(str,'head'->str),(str,'actor_gravatar'->str),(str,'push_id'->i64)],Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)],str)";

    auto type = python::Type::decode(type_str);

    // proper flattening needs to be achieved.
    auto tree = TupleTree<codegen::SerializableValue>(type);

    // make sure no element is of option type?
    for(unsigned i = 0; i < tree.numElements(); ++i)
        std::cout<<"field "<<i<<": "<<tree.fieldType(i).desc()<<std::endl;
    ASSERT_GT(tree.numElements(), 0);
}

// some UDF examples that should work:
// x = {}
// x['test'] = 10 # <-- type of x is now Struct['test' -> i64]
// x['blub'] = {'a' : 20, 'b':None} # <-- type of x is now Struct['test' -> i64, 'blub' -> Struct['a' -> i64, 'b' -> null]]