//
// Created by leonhard on 9/29/22.
//

#include "HyperUtils.h"

#include <physical/experimental/JsonHelper.h>

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

    {
        // first test -> simple load in both unwrap and no unwrap mode.
        unwrap_first_level = true;
        auto& ds = ctx.json(ref_path, unwrap_first_level);
        // no columns
        EXPECT_TRUE(!ds.columns().empty());
        auto v = ds.collectAsVector();

        EXPECT_EQ(v.size(), ref_row_count);
    }

//    {
//        // first test -> simple load in both unwrap and no unwrap mode.
//        unwrap_first_level = false;
//        auto& ds = ctx.json(ref_path, unwrap_first_level);
//        // no columns
//        EXPECT_TRUE(ds.columns().empty());
//        auto v = ds.collectAsVector();
//
//        EXPECT_EQ(v.size(), ref_row_count);
//    }

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