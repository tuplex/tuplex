//
// Created by Leonhard Spiegelberg on 9/20/22.
//

#include "HyperUtils.h"
#include "LLVM_wip.h"

// bindings
#include <StringUtils.h>
#include <JSONUtils.h>
#include <JsonStatistic.h>
#include <fstream>
#include <TypeHelper.h>
#include <Utils.h>
#include <compression.h>
#include <RuntimeInterface.h>

#include <AccessPathVisitor.h>

#include <llvm/IR/TypeFinder.h>

#include <experimental/ListHelper.h>
#include <experimental/StructDictHelper.h>
#include <physical/experimental/JsonHelper.h>

#include "JSONParseRowGenerator.h"
#include "JSONSourceTaskBuilder.h"

#include "TuplexMatchBuilder.h"

// NOTES:
// for concrete parser implementation with pushdown etc., use
// https://github.com/simdjson/simdjson/blob/master/doc/basics.md#json-pointer
// => this will allow to extract field...

namespace tuplex {
    namespace codegen {


        // refactor for more flexibility to cover more of the experimental scenarios
        // -> a decoder/parser  class for JSON
        // -> gets hthe type, the object to decode and some basic blocks to store free objects (start & end block!)
        // and a variable/decode if decode was successful?
        // => use this then to hook it up with serialization logic etc.







        void calculate_field_counts(const python::Type &type, size_t &field_count, size_t &option_count,
                                    size_t &maybe_count) {
            if (type.isStructuredDictionaryType()) {
                // recurse
                auto kv_pairs = type.get_struct_pairs();
                for (const auto &kv_pair: kv_pairs) {
                    maybe_count += !kv_pair.alwaysPresent;

                    // count optional key as well
                    if (kv_pair.keyType.isOptionType())
                        throw std::runtime_error("unsupported now");

                    calculate_field_counts(kv_pair.valueType, field_count, option_count, maybe_count);
                }
            } else {
                if (type.isOptionType()) {
                    option_count++;
                    calculate_field_counts(type.getReturnType(), field_count, option_count, maybe_count);
                } else {
                    // count as one field (true even for lists etc.) -> only unnest { { ...}, ... }
                    field_count++;
                }
            }
        }


        SerializableValue struct_dict_type_to_memory(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* value, const python::Type& dict_type);



        SerializableValue struct_dict_get_item(LLVMEnvironment &env, llvm::Value *obj, const python::Type &dict_type,
                                               const SerializableValue &key, const python::Type &key_type);

        void struct_dict_set_item(LLVMEnvironment &env, llvm::Value *obj, const python::Type &dict_type,
                                  const SerializableValue &key, const python::Type &key_type);


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

        llvm::Value *struct_dict_contains_key(LLVMEnvironment &env, llvm::Value *obj, const python::Type &dict_type,
                                              const SerializableValue &key, const python::Type &key_type) {
            assert(dict_type.isStructuredDictionaryType());

            auto &logger = Logger::instance().logger("codegen");

            // quick check
            // is key-type at all contained?
            auto kv_pairs = dict_type.get_struct_pairs();
            auto it = std::find_if(kv_pairs.begin(), kv_pairs.end(),
                                   [&key_type](const python::StructEntry &entry) { return entry.keyType == key_type; });
            if (it == kv_pairs.end())
                return env.i1Const(false);

            // is it a constant key? => can decide during compile time as well!
            if (key_type.isConstantValued()) {
                auto it = find_by_key(dict_type, key_type.constant(), key_type.underlying());
                return env.i1Const(it != dict_type.get_struct_pairs().end());
            }

            // is the value a llvm constant? => can optimize as well!
            if (key.val && llvm::isa<llvm::Constant>(key.val)) {
                // there are only a couple cases where this works...
                // -> string, bool, i64, f64...
                // if(key_type == python::Type::STRING && llvm::isa<llvm::Constant)
                // @TODO: skip for now
                logger.debug("optimization potential here... can decide this at compile time!");
            }

            // can't decide statically, use here LLVM IR code to decide whether the struct type contains the key or not!
            // => i.e. want to do semantic comparison.
            // only need to compare against all keys with key_type (or that are compatible to it (e.g. options))
            std::vector<python::StructEntry> pairs_to_compare_against;
            for (auto kv_pair: kv_pairs) {

            }

            return nullptr;
        }

    }
}

namespace tuplex {
    void create_dummy_function(codegen::LLVMEnvironment &env, llvm::Type *stype) {
        using namespace llvm;
        assert(stype);
        assert(stype->isStructTy());

        auto FT = FunctionType::get(env.i64Type(), {env.i64Type()}, false);
        auto F = Function::Create(FT, llvm::GlobalValue::ExternalLinkage, "dummy", *env.getModule().get());

        auto bb = BasicBlock::Create(env.getContext(), "entry", F);
        IRBuilder<> b(bb);
        b.CreateAlloca(stype);
        b.CreateRet(env.i64Const(0));

    }
}

// let's start with some simple tests for a basic dict struct type
namespace tuplex {
    Field get_representative_value(const python::Type &type) {
        using namespace tuplex;
        std::unordered_map<python::Type, Field> m{{python::Type::BOOLEAN,    Field(false)},
                                                  {python::Type::I64,        Field((int64_t) 42)},
                                                  {python::Type::F64,        Field(5.3)},
                                                  {python::Type::STRING,     Field("hello world!")},
                                                  {python::Type::NULLVALUE,  Field::null()},
                                                  {python::Type::EMPTYTUPLE, Field::empty_tuple()},
                                                  {python::Type::EMPTYLIST,  Field::empty_list()},
                                                  {python::Type::EMPTYDICT,  Field::empty_dict()}};

        if (type.isOptionType()) {
            // randomize:
            if (rand() % 1000 > 500)
                return Field::null();
            else
                return m.at(type.getReturnType());
        }

        return m.at(type);
    }
}

TEST_F(HyperTest, StructLLVMTypeContains) {
    using namespace tuplex;
    using namespace std;

    // create a struct type with everything in it.
    auto types = python::primitiveTypes(true);

    cout << "got " << pluralize(types.size(), "primitive type") << endl;

    // create a big struct type!
    std::vector<python::StructEntry> pairs;
    for (auto kt: types)
        for (auto vt: types) {
            auto key = escape_to_python_str(kt.desc());
            python::StructEntry entry;
            entry.key = key;
            entry.keyType = kt;
            entry.valueType = vt;
            pairs.push_back(entry);
        }

    // key type and value have to be unique!
    // -> i.e. remove any duplicates...

    auto stype = python::Type::makeStructuredDictType(pairs);

    cout << "created type: " << prettyPrintStructType(stype) << endl;
    cout << "type: " << stype.desc() << endl;
}


namespace tuplex {
    std::tuple<python::Type, python::Type, size_t, size_t> detectTypes(const std::string& sample) {
        auto buf = sample.data();
        auto buf_size = sample.size();


        // detect (general-case) type here:
//    ContextOptions co = ContextOptions::defaults();
//    auto sample_size = co.CSV_MAX_DETECTION_MEMORY();
//    auto nc_th = co.NORMALCASE_THRESHOLD();
        auto sample_size = 256 * 1024ul; // 256kb
        auto nc_th = 0.9;
        auto rows = parseRowsFromJSON(buf, std::min(buf_size, sample_size), nullptr, false);

        // general case version
        auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
        conf_general_case_type_policy.unifyMissingDictKeys = true;
        conf_general_case_type_policy.allowUnifyWithPyObject = true;

        double conf_nc_threshold = 0.;
        // type cover maximization
        std::vector<std::pair<python::Type, size_t>> type_counts;
        for (unsigned i = 0; i < rows.size(); ++i) {
            // row check:
            //std::cout<<"row: "<<rows[i].toPythonString()<<" type: "<<rows[i].getRowType().desc()<<std::endl;
            type_counts.emplace_back(std::make_pair(rows[i].getRowType(), 1));
        }

        auto general_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
        auto normal_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true,
                                                      TypeUnificationPolicy::defaultPolicy());

        auto normal_case_type = normal_case_max_type.first.parameters().front();
        auto general_case_type = general_case_max_type.first.parameters().front();
        std::cout << "normal  case:  " << normal_case_type.desc() << std::endl;
        std::cout << "general case:  " << general_case_type.desc() << std::endl;

        // check data layout
        codegen::LLVMEnvironment env;
        auto n_size = codegen::struct_dict_heap_size(env, normal_case_type);
        auto g_size = codegen::struct_dict_heap_size(env, general_case_type);

        return std::make_tuple(normal_case_type, general_case_type, n_size, g_size);
    }
}

TEST_F(HyperTest, PushEventPaperExample) {
    using namespace tuplex;
    using namespace std;


    auto content_before_filter_promote = "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 42}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 39}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 2}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 0}}\n"
                                         "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20, \"C\" : null, \"D\" : 32}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : 0}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                         "{\"type\" : \"ForkEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}";
    auto content_after_filter_promote = "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20, \"C\" : null}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 42}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 39}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 2}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 0}}\n"
                                        "{\"type\" : \"PushEvent\", \"A\" : {\"B\" : 20, \"C\" : null, \"D\" : 32}}";

    python::Type normal, general;
    auto t_before = detectTypes(content_before_filter_promote);
    normal = std::get<0>(t_before); general = std::get<1>(t_before);

    std::cout<<"before filter promo:: normal: "<<normal.desc()<<"  general: "<<general.desc()<<endl;
    std::cout<<"              size :: normal: "<<std::get<2>(t_before)<<"  general: "<<std::get<3>(t_before)<<endl;

    auto t_after = detectTypes(content_after_filter_promote);
    normal = std::get<0>(t_after); general = std::get<1>(t_after);

    std::cout<<"after filter promo:: normal: "<<normal.desc()<<"  general: "<<general.desc()<<endl;
    std::cout<<"              size :: normal: "<<std::get<2>(t_after)<<"  general: "<<std::get<3>(t_after)<<endl;
    EXPECT_TRUE(normal != python::Type::UNKNOWN);

    // --- manual --- post filter pushdown + constant folding
    // Struct[type -> _Constant["PushEvent"], A -> Struct[B -> i64]]


    throw std::runtime_error("");
}

// test to generate a struct type
TEST_F(HyperTest, StructLLVMType) {
    using namespace tuplex;
    using namespace std;

    string sample_path = "/Users/leonhards/Downloads/github_sample";
    string sample_file = sample_path + "/2011-11-26-13.json.gz";

    auto path = sample_file;

    Logger::init();


    path = "../resources/2011-11-26-13.json.gz";

    // // smaller sample
    // path = "../resources/2011-11-26-13.sample.json";

    //   // payload removed, b.c. it's so hard to debug... // there should be one org => one exception row.
    //  path = "../resources/2011-11-26-13.sample2.json"; // -> so this works.

      path = "../resources/2011-11-26-13.sample3.json"; // -> single row, the parse should trivially work.

    // tiny json example to simplify things
    // path = "../resources/ndjson/example1.json";


    auto raw_data = fileToString(path);

    const char *pointer = raw_data.data();
    std::size_t size = raw_data.size();

    // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
    std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;


    // parse code starts here...
    auto buf = decompressed_data.data();
    auto buf_size = decompressed_data.size();


    // detect (general-case) type here:
//    ContextOptions co = ContextOptions::defaults();
//    auto sample_size = co.CSV_MAX_DETECTION_MEMORY();
//    auto nc_th = co.NORMALCASE_THRESHOLD();
    auto sample_size = 256 * 1024ul; // 256kb
    auto nc_th = 0.9;
    auto rows = parseRowsFromJSON(buf, std::min(buf_size, sample_size), nullptr, false);

    // general case version
    auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
    conf_general_case_type_policy.unifyMissingDictKeys = true;
    conf_general_case_type_policy.allowUnifyWithPyObject = true;

    double conf_nc_threshold = 0.;
    // type cover maximization
    std::vector<std::pair<python::Type, size_t>> type_counts;
    for (unsigned i = 0; i < rows.size(); ++i) {
        // row check:
        //std::cout<<"row: "<<rows[i].toPythonString()<<" type: "<<rows[i].getRowType().desc()<<std::endl;
        type_counts.emplace_back(std::make_pair(rows[i].getRowType(), 1));
    }

    auto general_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
    auto normal_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true,
                                                  TypeUnificationPolicy::defaultPolicy());

    auto normal_case_type = normal_case_max_type.first.parameters().front();
    auto general_case_type = general_case_max_type.first.parameters().front();
    std::cout << "normal  case:  " << normal_case_type.desc() << std::endl;
    std::cout << "general case:  " << general_case_type.desc() << std::endl;

    auto row_type = normal_case_type;//general_case_type;
    row_type = general_case_type; // <-- this should match MOST of the rows...

    // codegen now here...
    codegen::LLVMEnvironment env;

    auto stype = codegen::create_structured_dict_type(env, row_type);
    // create new func with this
    create_dummy_function(env, stype);

//    std::string err;
//    EXPECT_TRUE(codegen::verifyModule(*env.getModule(), &err));
//    std::cerr<<err<<std::endl;

    // TypeFinderDebug.run(*env.getModule());

    std::cout << "running typefinder" << std::endl;
    //env.getModule()->dump();
    llvm::TypeFinder type_finder;
    type_finder.run(*env.getModule(), true);
    for (auto t: type_finder) {
        std::cout << t->getName().str() << std::endl;
    }

    std::cout << "type finder done, dumping module" << std::endl;
    // bitcode --> also fails.
    // codegen::moduleToBitCodeString(*env.getModule());

    auto ir_code = codegen::moduleToString(*env.getModule());

    std::cout << "generated code:\n" << core::withLineNumbers(ir_code) << std::endl;
}


// notes: type of line can be


namespace tuplex {
    std::tuple<size_t, size_t, size_t> runCodegen(const python::Type& row_type, const uint8_t* buf, size_t buf_size) {
        using namespace tuplex;
        using namespace std;

        // codegen here
        codegen::LLVMEnvironment env;
        auto parseFuncName = "parseJSONCodegen";

        // verify storage architecture/layout
        codegen::struct_dict_verify_storage(env, row_type, std::cout);

        codegen::JSONSourceTaskBuilder jtb(env, row_type, parseFuncName);
        jtb.build();
        auto ir_code = codegen::moduleToString(*env.getModule());
        std::cout << "generated code:\n" << core::withLineNumbers(ir_code) << std::endl;

        // load runtime lib
        runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

        // init JITCompiler
        JITCompiler jit;
        codegen::addJsonSymbolsToJIT(jit);
        jit.registerSymbol("rtmalloc", runtime::rtmalloc);


        // // optimize code (note: is in debug mode super slow)
        // LLVMOptimizer opt;
        // ir_code = opt.optimizeIR(ir_code);

        // compile func
        auto rc_compile = jit.compile(ir_code);
        //ASSERT_TRUE(rc_compile);

        // get func
        auto func = reinterpret_cast<int64_t(*)(const char *, size_t, size_t*, size_t*, size_t*)>(jit.getAddrOfSymbol(parseFuncName));

        // runtime init
        ContextOptions co = ContextOptions::defaults();
        runtime::init(co.RUNTIME_LIBRARY(false).toPath());

        // call code generated function!
        Timer timer;
        size_t total_rows = 0;
        size_t bad_rows = 0;
        size_t total_size = 0;
        auto rc = func(reinterpret_cast<const char*>(buf), buf_size, &total_rows, &bad_rows, &total_size);
        std::cout << "parsed rows in " << timer.time() << " seconds, (" << sizeToMemString(buf_size) << ")" << std::endl;
        std::cout << "done" << std::endl;

        return make_tuple(total_rows, bad_rows, total_size);
    }

    struct MatchResult {
        size_t totalRows;
        size_t normalRows;
        size_t generalRows;
        size_t fallbackRows;
        size_t normalMemorySize;
        size_t generalMemorySize;
        size_t fallbackMemorySize;

        nlohmann::json to_json() const {
            nlohmann::json j;
            j["total_rows"] = totalRows;
            j["normal_rows"] = normalRows;
            j["general_rows"] = generalRows;
            j["fallback_rows"] = fallbackRows;
            j["normal_mem"] = normalMemorySize;
            j["general_mem"] = generalMemorySize;
            j["fallback_mem"] = fallbackMemorySize;
            return j;
        }
    };

    MatchResult runMatchCodegen(const python::Type& normal_row_type,
                                                       const python::Type& general_row_type,
                                                       const uint8_t* buf, size_t buf_size) {
        using namespace tuplex;
        using namespace std;

        // codegen here
        codegen::LLVMEnvironment env;
        auto parseFuncName = "parseJSONMatchCodegen";

        codegen::TuplexMatchBuilder jtb(env, normal_row_type, general_row_type, parseFuncName);
        jtb.build();
        auto ir_code = codegen::moduleToString(*env.getModule());
        // std::cout << "generated code:\n" << core::withLineNumbers(ir_code) << std::endl;

        // load runtime lib
        runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

        // init JITCompiler
        JITCompiler jit;
        codegen::addJsonSymbolsToJIT(jit);
        jit.registerSymbol("rtmalloc", runtime::rtmalloc);


        // // optimize code (note: is in debug mode super slow)
        // LLVMOptimizer opt;
        // ir_code = opt.optimizeIR(ir_code);

        // compile func
        auto rc_compile = jit.compile(ir_code);
        //ASSERT_TRUE(rc_compile);

        // get func
        auto func = reinterpret_cast<int64_t(*)(const char *, size_t,
                                                size_t*,
                                                size_t*, size_t*, size_t*,
                                                size_t*, size_t*, size_t*)>(jit.getAddrOfSymbol(parseFuncName));

        // runtime init
        ContextOptions co = ContextOptions::defaults();
        runtime::init(co.RUNTIME_LIBRARY(false).toPath());


//        // chunk data
//        if(buf_size > 128 * 1024 * 1024) {
//            // chunk data...
//            std::vector<size_t> offsets = {0};
//            size_t chunk_size = 128 * 1024 * 1024; // 128MB chunks?
//            size_t cur = chunk_size;
//            while(cur <= buf_size) {
//                // find start
//                auto start = findNLJsonStart(reinterpret_cast<const char*>(buf), buf_size - cur);
//                std::cout<<"found NL start at "<<start<<cur + start<<std::endl;
//                offsets.push_back(cur + start);
//
//                cur += chunk_size;
//            }
//           std::cout<<"done"<<std::endl;
//        }

        // assert(buf[buf_size - 1] == '\0');

        // call code generated function!
        Timer timer;
        MatchResult m;
        auto rc = func(reinterpret_cast<const char*>(buf), buf_size,
                       &m.totalRows,
                       &m.normalRows, &m.generalRows, &m.fallbackRows,
                       &m.normalMemorySize, &m.generalMemorySize, &m.fallbackMemorySize);
        std::cout << "parsed rows in " << timer.time() << " seconds, (" << sizeToMemString(buf_size) << ")" << std::endl;
        std::cout << "done" << std::endl;

        return m;
    }
}

namespace tuplex {
    std::vector<std::pair<python::Type, size_t>> counts_from_rows(const std::vector<Row>& rows) {
        std::unordered_map<python::Type, size_t> m;
        for(const auto& row : rows) {
            auto key = row.getRowType();
            auto it = m.find(key);
            if(it == m.end())
                m[key] = 1;
            else
                m[key]++;
        }

        std::vector<std::pair<python::Type, size_t>> v(m.begin(), m.end()); // maybe sort asc?
        return v;
    }

    std::vector<std::pair<python::Type, size_t>> combine_type_counts(const std::vector<std::pair<python::Type, size_t>>& rhs, const std::vector<std::pair<python::Type, size_t>>& lhs) {
        std::unordered_map<python::Type, size_t> m;
        for(auto p : rhs) {
            auto it = m.find(p.first);
            if(it == m.end())
                m[p.first] = p.second;
            else
                m[p.first] += p.second;
        }

        for(auto p : lhs) {
            auto it = m.find(p.first);
            if(it == m.end())
                m[p.first] = p.second;
            else
                m[p.first] += p.second;
        }

        std::vector<std::pair<python::Type, size_t>> v(m.begin(), m.end());
        return v;
    }

    bool findTypeCoverSolution(unsigned cur_search_start,
                               size_t count_threshold,
                               const std::vector<std::pair<python::Type, size_t>>& t_counts,
                               const TypeUnificationPolicy& t_policy,
                               std::pair<python::Type, size_t>* best_solution) {
        auto cur_pair = t_counts[cur_search_start];
        for(unsigned i = cur_search_start + 1; i < t_counts.size(); ++i) {
            auto uni_type = unifyTypes(cur_pair.first, t_counts[i].first, t_policy);
            if(uni_type != python::Type::UNKNOWN) {
                cur_pair.first = uni_type;
                cur_pair.second += t_counts[i].second;
            }
            // valid solution?
            if(cur_pair.second >= count_threshold) {
                if(best_solution) {
                    *best_solution = cur_pair;
                }
                return true;
            }
        }

        if(best_solution) {
            *best_solution = cur_pair;
        }

        return false;
    }

    std::pair<python::Type, size_t> maximizeTypeCoverNew(const std::vector<std::pair<python::Type, size_t>>& counts,
                                                         double threshold,
                                                         bool use_nvo,
                                                         const TypeUnificationPolicy& t_policy) {
        using namespace std;

        if(counts.empty()) {
            return make_pair(python::Type::UNKNOWN, 0);
        }

        // @TODO: implement this here! incl. recursive null checking for structured dict types...

        // sort desc after count pairs. Note: goal is to have maximum type cover!
        auto t_counts = counts;
        std::sort(t_counts.begin(), t_counts.end(), [](const pair<python::Type, size_t>& lhs,
                                                       const pair<python::Type, size_t>& rhs) { return lhs.second > rhs.second; });

//        for(auto& e : t_counts)
//            // equal weight on all
//            e.second = 1;

        // cumulative counts (reverse)
        std::vector<size_t> cum_counts(t_counts.size(), 0);
        cum_counts[0] = t_counts[0].second;
        size_t total_count = t_counts[0].second;
        for(unsigned i = 1; i < t_counts.size(); ++i) {
            cum_counts[i] = cum_counts[i - 1] + t_counts[i].second;
            total_count += t_counts[i].second;
        }

        // solution via backtracking
        vector<bool> initial_solution(t_counts.size(), false);
        size_t count_threshold = floor(threshold * total_count); // anything there indicates a valid solution!

        unsigned cur_search_start = 0; // <-- this is still not a perfect solution

        // find solution from current search start (and check if better than current one)
        std::pair<python::Type, size_t> best_pair = make_pair(python::Type::UNKNOWN, 0);
        for(unsigned i = 0; i < std::min(5ul, t_counts.size()); ++i) {
            std::pair<python::Type, size_t> cur_pair;
            if(findTypeCoverSolution(i, count_threshold, t_counts, t_policy, &cur_pair)) {
                std::cout<<"solution found"<<std::endl;
                return cur_pair;
            }

            // update
            if(cur_pair.second >= best_pair.second)
                best_pair = cur_pair;
        }

        std::cout<<"no solution found, but best pair..."<<std::endl;
        return best_pair;
    }

    size_t number_of_fields(python::Type type) {
        if(type.isOptimizedType())
            type = deoptimizedType(type);

        if(type.isStructuredDictionaryType()) {
            // recurse
            size_t total = 0;
            for(const auto& kv_pair : type.get_struct_pairs())
                total += number_of_fields(kv_pair.valueType);
            return total;
        } else if(type.isTupleType()) {
            size_t total = 0;
            for(const auto& t : type.parameters())
                total += number_of_fields(t);
            return total;
        } else if(type.isOptionType()) {
            return number_of_fields(type.getReturnType());
        } else if(type.isListType() && type != python::Type::EMPTYLIST) {
            return number_of_fields(type.elementType());
        } else if(type.isDictionaryType()) {
            return number_of_fields(type.valueType());
        }
        // simple type, return 1
        return 1;
    }

    nlohmann::json json_paths_to_json_array(const python::Type& type) {
        assert(type.isStructuredDictionaryType());
        codegen::flattened_struct_dict_entry_list_t entries;
        codegen::flatten_recursive_helper(entries, type);

        auto j = nlohmann::json::array();
        for(auto entry : entries) {
            auto access_path = std::get<0>(entry);
            auto value_type = std::get<1>(entry);
            auto always_present = std::get<2>(entry);

            auto str = codegen::json_access_path_to_string(access_path, value_type, always_present);
            j.push_back(str);
        }

        return j;
    }
}

TEST_F(HyperTest, SimdJSONFailure) {
    using namespace tuplex;
    using namespace tuplex::codegen;
    using namespace std;
    auto path = "/home/leonhard/Downloads/testsingle.json";
    path = "../resources/ndjson/example1_with_empty_lines.json";
    auto test = tuplex::fileToString(URI(path));

//    simdjson::ondemand::parser parser;
//    auto json = simdjson::padded_string::load(path); // load JSON file 'twitter.json'.
//    simdjson::ondemand::document doc = parser.iterate(json);
//    auto t = doc.type().value();
//    std::cout<<"type is: "<<t<<std::endl;

//    auto json = simdjson::padded_string::load(path).take_value(); // load JSON file 'twitter.json'.
//    auto buf = json.data();
//    auto buf_size = json.size();



//    // dom parser failing?
//    simdjson::dom::parser parser;
//    simdjson::dom::document_stream stream;
//    auto SIMDJSON_BATCH_SIZE = simdjson::dom::DEFAULT_BATCH_SIZE;
//    stream = parser.parse_many(buf, buf_size, std::min(buf_size, SIMDJSON_BATCH_SIZE)).take_value();
//    auto it = stream.begin();
//    while(stream.end() != it) {
//        auto doc = *it;
//        auto t = doc.type().value();
//        std::cout<<"type is: "<<t<<std::endl;
//        ++it;
//    }
//
//    // on-demand parser failing?
//    simdjson::ondemand::parser parser;
//    simdjson::ondemand::document_stream stream;
//    auto SIMDJSON_BATCH_SIZE = simdjson::dom::DEFAULT_BATCH_SIZE;
//    auto batch_size = std::min(buf_size, SIMDJSON_BATCH_SIZE);
//    stream = parser.iterate_many(buf, buf_size, batch_size).take_value();
//    auto it = stream.begin();
//    while(stream.end() != it) {
//        auto doc = *it;
//        auto t = doc.type().value();
//        std::cout<<"type is: "<<t<<std::endl;
//        ++it;
//    }

    // now, regular routine...
    auto raw_data = fileToString(URI(path));

    const char *pointer = raw_data.data();
    std::size_t size = raw_data.size();

    // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
    std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;
    for(unsigned i = 0; i < simdjson::SIMDJSON_PADDING; ++i)
        decompressed_data.push_back('\0');

    // make sure capacity exceeds string!
    decompressed_data.reserve(decompressed_data.size() + simdjson::SIMDJSON_PADDING);

    // parse code starts here...
    auto buf = decompressed_data.data();
    auto buf_size = decompressed_data.size();


    // this here fails b.c. batch size < document size.
    unsigned row_number = 0;
    auto j = JsonParser_init();
    if (!j)
        throw std::runtime_error("failed to initialize parser");
    JsonParser_open(j, buf, buf_size);
    while (JsonParser_hasNextRow(j)) {
        if (JsonParser_getDocType(j) != JsonParser_objectDocType()) {
            // BADPARSE_STRINGINPUT
            auto line = JsonParser_getMallocedRow(j);
            free(line);
        }

        // line ok, now extract something from the object!
        // => basically need to traverse...
        auto doc = *j->it;

        row_number++;
        JsonParser_moveToNextRow(j);
    }
    JsonParser_close(j);
    JsonParser_free(j);

    std::cout << "Parsed " << pluralize(row_number, "row") << std::endl;
}

TEST_F(HyperTest, LargeFileParse) {
    // check parsing on a file > 4GB
    using namespace tuplex;
    using namespace tuplex::codegen;
    using namespace std;

    Logger::instance().init();
    auto& logger = Logger::instance().logger("experiment");

    string path = "/data/github_sample_daily/2012-10-15.json.gz";
    path = "/home/leonhard/Downloads/testsingle.json";

    auto sample_size = 2 * 1024 * 1024ul;// 8MB ////256 * 1024ul; // 256kb
//    auto sample_size = 256 * 1024ul; // 256kb
    bool perfect_sample = false;//true; // if true, sample the whole file (slow, but perfect representation)
    bool pushdown_pushevent = true;
    auto nc_th = 0.9;
    // general case version
    auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
    conf_general_case_type_policy.unifyMissingDictKeys = true;
    conf_general_case_type_policy.allowUnifyWithPyObject = false; // <-- do NOT allow this. => can't compile... --> this could be helped with DelayedParse[...]

    double conf_nc_threshold = 0.9;

    // now, regular routine...
    auto raw_data = fileToString(path);

    const char *pointer = raw_data.data();
    std::size_t size = raw_data.size();

    // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
    std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;
    for(unsigned i = 0; i < simdjson::SIMDJSON_PADDING; ++i)
        decompressed_data.push_back('\0');

    // make sure capacity exceeds string!
    decompressed_data.reserve(decompressed_data.size() + simdjson::SIMDJSON_PADDING);

    // parse code starts here...
    auto buf = decompressed_data.data();
    auto buf_size = decompressed_data.size();

    // sample full file
    if(perfect_sample)
        sample_size = buf_size;

    // detect types here:
    auto actual_sample_size = std::min(buf_size, sample_size);

    // create sample buffer & parse
    char *sample_buffer = new char[actual_sample_size + simdjson::SIMDJSON_PADDING];
    memcpy(sample_buffer, buf, actual_sample_size);
    memset(sample_buffer + actual_sample_size, 0, simdjson::SIMDJSON_PADDING);

    auto rows = parseRowsFromJSON(sample_buffer, actual_sample_size, nullptr, false);

    delete [] sample_buffer; sample_buffer = nullptr;

    std::vector<std::pair<python::Type, size_t>> type_counts = counts_from_rows(rows);
    auto general_case_max_type = maximizeTypeCoverNew(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
    auto normal_case_max_type = maximizeTypeCoverNew(type_counts, conf_nc_threshold, true, TypeUnificationPolicy::defaultPolicy());

    auto normal_case_type = normal_case_max_type.first.parameters().front();
    auto general_case_type = general_case_max_type.first.parameters().front();
    std::cout << "normal  case:  " << normal_case_type.desc() << std::endl;
    std::cout << "general case:  " << general_case_type.desc() << std::endl;

    bool is_ok = simdjson::validate_utf8((const char*)buf, buf_size);
    std::cout<<"is string UTF-8? "<<is_ok<<std::endl;

//    // C-version of parsing
//    uint64_t row_number = 0;
//
//    auto j = JsonParser_init();
//    if (!j)
//        throw std::runtime_error("failed to initialize parser");
//    JsonParser_open(j, buf, buf_size);
//    while (JsonParser_hasNextRow(j)) {
//
//        if (JsonParser_getDocType(j) != JsonParser_objectDocType()) {
//            // BADPARSE_STRINGINPUT
//            auto line = JsonParser_getMallocedRow(j);
//            free(line);
//        }
//
//        // line ok, now extract something from the object!
//        // => basically need to traverse...
//        auto doc = *j->it;
//
//        row_number++;
//        JsonParser_moveToNextRow(j);
//    }
//    JsonParser_close(j);
//    JsonParser_free(j);
//
//    std::cout << "Parsed " << pluralize(row_number, "row") << std::endl;


    // parse and check
    auto m = runMatchCodegen(normal_case_type, general_case_type, reinterpret_cast<const uint8_t*>(buf), buf_size);

    std::cout<<"parsed "<<pluralize(m.totalRows, "row")<<std::endl;
}

TEST_F(HyperTest, LoadAllFiles) {
    using namespace tuplex;
    using namespace std;

    Logger::instance().init();
    auto& logger = Logger::instance().logger("experiment");

    // settings are here
    string root_path = "/data/github_sample/*.json.gz";
    // daily sample
    root_path = "/data/github_sample_daily/*.json.gz";

    root_path = "/data/github_sample_daily/2011-10-15.json.gz";

#ifdef MACOS
    root_path = "/Users/leonhards/Downloads/github_sample/*.json.gz";
#endif

    auto sample_size = 2 * 1024 * 1024ul;// 8MB ////256 * 1024ul; // 256kb
//    auto sample_size = 256 * 1024ul; // 256kb
    bool perfect_sample = false;//true; // if true, sample the whole file (slow, but perfect representation)
    bool pushdown_pushevent = true;
    auto nc_th = 0.9;
    // general case version
    auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
    conf_general_case_type_policy.unifyMissingDictKeys = true;
    conf_general_case_type_policy.allowUnifyWithPyObject = false; // <-- do NOT allow this. => can't compile... --> this could be helped with DelayedParse[...]

    double conf_nc_threshold = 0.9;


    auto paths = glob(root_path);

//    // debug
//    paths = std::vector<std::string>(paths.begin(), paths.begin() + 1);

    logger.info("Found " + pluralize(paths.size(), "path") + " under " + root_path);

    std::vector<std::string> bad_paths;

    std::stringstream ss;
    std::vector<nlohmann::json> results;

    std::vector<std::pair<python::Type, size_t>> global_type_counts; // holds type counts across ALL files.

    // ----------------------------------------------------------------------------------------------------------------
    // hyperspecialized processing
    // now perform detection & parse for EACH file.
    size_t total_actual_sample_size = 0;
    std::reverse(paths.begin(), paths.end());
    for(const auto& path : paths) {
        logger.info("Processing " + path);

        try {
            // now, regular routine...
            auto raw_data = fileToString(path);

            const char *pointer = raw_data.data();
            std::size_t size = raw_data.size();

            // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
            std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;


            // parse code starts here...
            auto buf = decompressed_data.data();
            auto buf_size = decompressed_data.size();

            // sample full file
            if(perfect_sample)
                sample_size = buf_size;

            // detect types here:
            auto actual_sample_size = std::min(buf_size, sample_size);
            total_actual_sample_size += actual_sample_size;
            auto rows = parseRowsFromJSON(buf, actual_sample_size, nullptr, false);
            std::vector<std::pair<python::Type, size_t>> type_counts = counts_from_rows(rows);
            global_type_counts = combine_type_counts(global_type_counts, type_counts);
            auto general_case_max_type = maximizeTypeCoverNew(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
            auto normal_case_max_type = maximizeTypeCoverNew(type_counts, conf_nc_threshold, true, TypeUnificationPolicy::defaultPolicy());

            auto normal_case_type = normal_case_max_type.first.parameters().front();
            auto general_case_type = general_case_max_type.first.parameters().front();
            std::cout << "normal  case:  " << normal_case_type.desc() << std::endl;
            std::cout << "general case:  " << general_case_type.desc() << std::endl;

            // parse and check
            auto m = runMatchCodegen(normal_case_type, general_case_type, reinterpret_cast<const uint8_t*>(buf), buf_size);

            auto j = m.to_json();
            j["mode"] = "hyper";
            j["path"] = path;
            j["normal_case"] = normal_case_type.desc();
            j["general_case"] = general_case_type.desc();
            j["normal_case_field_count"] = number_of_fields(normal_case_type);
            j["general_case_field_count"] = number_of_fields(general_case_type);
            j["normal_json_paths"] = json_paths_to_json_array(normal_case_type);
            j["general_json_paths"] = json_paths_to_json_array(general_case_type);
            j["buf_size_compressed"] = raw_data.size();
            j["buf_size_uncompressed"] = decompressed_data.size();
            j["sample_size"] = actual_sample_size;
            j["perfect_sample"] = perfect_sample;
            j["sample_row_count"] = rows.size();

            ss<<j.dump()<<endl; // dump without type counts (b.c. they're large!

            // add type counts (good idea for later investigation on what could be done to improve sampling => maybe separate experiment?
            auto j_arr = nlohmann::json::array();
            for(auto p : type_counts) {
                auto j_obj = nlohmann::json::object();
                j_obj["type"] = p.first.desc();
                j_obj["count"] = p.second;
                j_arr.push_back(j_obj);
            }
            j["type_counts"] = j_arr;

            // output result
            results.push_back(j);

        } catch (std::exception& e) {
            logger.error("path " + path + " failed processing with: " + e.what());
            bad_paths.push_back(path);
            continue;
        }
    }

    if(!bad_paths.empty()) {
        for(const auto& path : bad_paths) {
            logger.error("failed to process: " + path);
        }
        return;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // global specialized processing
    // now perform global optimization and parse!
    {
        auto general_case_max_type = maximizeTypeCoverNew(global_type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
        auto normal_case_max_type = maximizeTypeCoverNew(global_type_counts, conf_nc_threshold, true,
                                                      TypeUnificationPolicy::defaultPolicy());

        auto global_normal_case_type = normal_case_max_type.first.parameters().front();
        auto global_general_case_type = general_case_max_type.first.parameters().front();
        std::cout << "global normal  case:  " << global_normal_case_type.desc() << std::endl;
        std::cout << "global general case:  " << global_general_case_type.desc() << std::endl;

        size_t global_actual_sample_size = total_actual_sample_size;
        size_t global_sample_count = 0;
        for(auto kv : global_type_counts)
            global_sample_count += kv.second;

        for(const auto& path : paths) {
            logger.info("Processing " + path);

            try {
                // now, regular routine...
                auto raw_data = fileToString(path);

                const char *pointer = raw_data.data();
                std::size_t size = raw_data.size();

                // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
                std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;

                // parse code starts here...
                auto buf = decompressed_data.data();
                auto buf_size = decompressed_data.size();

                // parse and check
                auto m = runMatchCodegen(global_normal_case_type, global_general_case_type, reinterpret_cast<const uint8_t*>(buf), buf_size);

                auto j = m.to_json();
                j["mode"] = "global";
                j["path"] = path;
                j["normal_case"] = global_normal_case_type.desc();
                j["general_case"] = global_general_case_type.desc();
                j["normal_case_field_count"] = number_of_fields(global_normal_case_type);
                j["general_case_field_count"] = number_of_fields(global_general_case_type);
                j["buf_size_compressed"] = raw_data.size();
                j["buf_size_uncompressed"] = decompressed_data.size();
                j["sample_size"] = global_actual_sample_size;
                j["perfect_sample"] = perfect_sample;
                j["sample_row_count"] = global_sample_count;

                ss<<j.dump()<<endl; // dump without type counts (b.c. they're large!

                // add global type counts (good idea for later investigation on what could be done to improve sampling => maybe separate experiment?
                auto j_arr = nlohmann::json::array();
                for(auto p : global_type_counts) {
                    auto j_obj = nlohmann::json::object();
                    j_obj["type"] = p.first.desc();
                    j_obj["count"] = p.second;
                    j_arr.push_back(j_obj);
                }
                j["type_counts"] = j_arr;

                // output result
                results.push_back(j);
            } catch (std::exception& e) {
                logger.error("path " + path + " failed processing with: " + e.what());
                bad_paths.push_back(path);
                break;
            }
        }
    }

    std::cout<<ss.str()<<std::endl;
    {
        std::stringstream out;
        for(auto j : results)
            out<<j.dump()<<endl;
        stringToFile("experiment_result.json", out.str());
    }

    // perform some quick analysis (summing up values) to print out.
    MatchResult m_hyper; memset(&m_hyper, 0, sizeof(m_hyper));
    MatchResult m_global; memset(&m_global, 0, sizeof(m_global));
    for(auto j : results) {
        if(j["mode"].get<std::string>() == "global") {
            m_global.normalRows += j["normal_rows"].get<size_t>();
            m_global.generalRows += j["general_rows"].get<size_t>();
            m_global.fallbackRows += j["fallback_rows"].get<size_t>();
        } else {
            m_hyper.normalRows += j["normal_rows"].get<size_t>();
            m_hyper.generalRows += j["general_rows"].get<size_t>();
            m_hyper.fallbackRows += j["fallback_rows"].get<size_t>();
        }
    }

    std::cout<<"overall result ("<<pluralize(results.size(), "file")<<"): "<<std::endl;
    std::cout<<"hyper:\n-------"<<std::endl;
    std::cout<<"  "<<m_hyper.to_json().dump()<<std::endl;
    std::cout<<"global:\n-------"<<std::endl;
    std::cout<<"  "<<m_global.to_json().dump()<<std::endl;
}

TEST_F(HyperTest, TypeMaximization) {
    // use from data  stringToFile("experiment_result.json", out.str());
    using namespace tuplex;
    using namespace std;

    Logger::instance().init();

    string path = "experiment_result.json";
    auto data = fileToString(path);
    auto lines = splitToLines(data);

    // first file:
    auto j = nlohmann::json::parse(lines.back());
    auto j_arr = j["type_counts"];
    std::vector<std::pair<python::Type, size_t>> type_counts;
    std::cout<<"loading for mode: "<<j["mode"].get<std::string>()<<std::endl;
    for(auto el : j_arr) {
        type_counts.push_back(make_pair(python::Type::decode(el["type"].get<std::string>()),el["count"].get<size_t>()));
    }

    // new algorithm to maximize type cover.
    auto& logger = Logger::instance().logger("typer");
    logger.debug("found " + pluralize(type_counts.size(), "type"));
    size_t total_count = 0; for(auto p : type_counts) total_count += p.second;

    auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
    conf_general_case_type_policy.unifyMissingDictKeys = true;
    conf_general_case_type_policy.allowUnifyWithPyObject = false; // <-- do NOT allow this. => can't compile...

    auto conf_nc_policy = TypeUnificationPolicy::defaultPolicy();

    auto best_pair = maximizeTypeCoverNew(type_counts, 0.9, true, conf_general_case_type_policy);
    logger.debug("best pair type: " + best_pair.first.desc());
    logger.debug("best pair covers " + std::to_string(best_pair.second) + "/" + std::to_string(total_count));
}

TEST_F(HyperTest, BasicStructLoad) {
    using namespace tuplex;
    using namespace std;

    string sample_path = "/Users/leonhards/Downloads/github_sample";
    string sample_file = sample_path + "/2011-11-26-13.json.gz";

    auto path = sample_file;

    //


    path = "../resources/2011-11-26-13.json.gz";

    // this 3 files here failed processing:
    // [2022-09-26 12:57:02.183] [experiment] [error] failed to process: /data/github_sample/2012-11-08-17.json.gz
    // [2022-09-26 12:57:02.183] [experiment] [error] failed to process: /data/github_sample/2013-07-27-3.json.gz
    // [2022-09-26 12:57:02.183] [experiment] [error] failed to process: /data/github_sample/2014-04-09-12.json.gz

    path = "/data/github_sample/2012-11-08-17.json.gz";

    // // smaller sample
    // path = "../resources/2011-11-26-13.sample.json";

    //   // payload removed, b.c. it's so hard to debug... // there should be one org => one exception row.
      //path = "../resources/2011-11-26-13.sample2.json"; // -> so this works.

     // path = "../resources/2011-11-26-13.sample3.json"; // -> single row, the parse should trivially work.


    // // tiny json example to simplify things
     // path = "../resources/ndjson/example1.json";


//     // mini example in order to analyze code
//     path = "test.json";
//     auto content = "{\"column1\": {\"a\": \"hello\", \"b\": 20, \"c\": 30}}\n"
//                    "{\"column1\": {\"a\": \"test\", \"b\": 20, \"c\": null}}\n"
//                    "{\"column1\": {\"a\": \"cat\",  \"c\": null}}";
//     stringToFile(path, content);

//     // mini example in order to analyze code
//     path = "test.json";
//
//     // this here is a simple example of a list decode
//     auto content = "{\"column1\": {\"a\": [1, 2, 3, 4]}}\n"
//                    "{\"column1\": {\"a\": [1, 4]}}\n"
//                    "{\"column1\": {\"a\": []}}";
//     stringToFile(path, content);


//     // mini example in order with optional struct type!
//     path = "test.json";
//
//     // this here is a simple example of a list decode
//     auto content = "{\"column1\": {\"a\": [1, 2, 3, 4]}}\n"
//                    "{\"column1\": null}\n"
//                    "{\"column1\": null}";
//     stringToFile(path, content);

     // steps: 1.) integer list decode
     //        2.) struct dict list decode (this is MORE involved)

//
//     // mini example in order with list of tuples
//     path = "test.json";
//
//     // this here is a simple example of a list decode
//     auto content = "{\"column1\": [[\"a\", 20], [\"b\", 10]]}\n"
//                    "{\"column1\": [[\"c\", 0], [\"d\", 0]]}\n";
//     stringToFile(path, content);


     // mini example in order with list of options
     path = "test.json";

     // this here is a simple example of a list decode
//     auto content = "{\"column1\": [[0, 1, null], [0, 1, null]]}\n"
//                    "{\"column1\": [[null], []]}\n";
    auto content = "{\"column1\": [[\"test\", null], [null, \"hello\"], []]}\n"
                   "{\"column1\": [[]]}\n";
     stringToFile(path, content);

    // now, regular routine...
    auto raw_data = fileToString(path);

    const char *pointer = raw_data.data();
    std::size_t size = raw_data.size();

    // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
    std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;


    // parse code starts here...
    auto buf = decompressed_data.data();
    auto buf_size = decompressed_data.size();


    // detect (general-case) type here:
//    ContextOptions co = ContextOptions::defaults();
//    auto sample_size = co.CSV_MAX_DETECTION_MEMORY();
//    auto nc_th = co.NORMALCASE_THRESHOLD();
    auto sample_size = 512 * 1024ul; // 512KB //256 * 1024ul; // 256kb
    auto nc_th = 0.9;
    auto rows = parseRowsFromJSON(buf, std::min(buf_size, sample_size), nullptr, false);

    // general case version
    auto conf_general_case_type_policy = TypeUnificationPolicy::defaultPolicy();
    conf_general_case_type_policy.unifyMissingDictKeys = true;
    conf_general_case_type_policy.allowUnifyWithPyObject = false; // <-- do NOT allow this. => can't compile...

    double conf_nc_threshold = 0.;
    // type cover maximization
    std::vector<std::pair<python::Type, size_t>> type_counts = counts_from_rows(rows);

    auto general_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true, conf_general_case_type_policy);
    auto normal_case_max_type = maximizeTypeCover(type_counts, conf_nc_threshold, true,
                                                  TypeUnificationPolicy::defaultPolicy());

    auto normal_case_type = normal_case_max_type.first.parameters().front();
    auto general_case_type = general_case_max_type.first.parameters().front();
    std::cout << "normal  case:  " << normal_case_type.desc() << std::endl;
    std::cout << "general case:  " << general_case_type.desc() << std::endl;

    // modify here which type to use for the parsing...
    auto row_type = normal_case_type;//general_case_type;
    //row_type = general_case_type; // <-- this should match MOST of the rows...
    // row_type = normal_case_type;


    // could do here a counter experiment: I.e., how many general case rows? how many normal case rows? how many fallback rows?
    // => then also measure how much memory is required!
    // => can perform example experiments for the 10 different files and plot it out.
    //std::cout<<"-----\nrunning using normal case:\n-----\n"<<std::endl;
    //auto normal_rc = runCodegen(normal_case_type, reinterpret_cast<const uint8_t*>(buf), buf_size);
    std::cout<<"-----\nrunning using general case:\n-----\n"<<std::endl;
    auto general_rc = runCodegen(general_case_type, reinterpret_cast<const uint8_t*>(buf), buf_size);

//    {
//        std::vector<std::tuple<size_t, size_t, size_t>> rows({normal_rc, general_rc});
//        std::vector<std::string> headers({"normal", "general"});
//        for(int i = 0; i < 2; ++i) {
//        std::cout<<headers[i]<<":  "<<"#rows: "<<std::get<0>(rows[i])<<"  #bad-rows: "<<std::get<1>(rows[i])<<"  size(bytes): "<<std::get<2>(rows[i])<<std::endl;
//        }
//    }
}

TEST_F(HyperTest, CParse) {
    using namespace tuplex;
    using namespace tuplex::codegen;
    using namespace std;

    string sample_path = "/Users/leonhards/Downloads/github_sample";
    string sample_file = sample_path + "/2011-11-26-13.json.gz";

    auto path = sample_file;

    path = "../resources/2011-11-26-13.json.gz";

    auto raw_data = fileToString(path);

    const char *pointer = raw_data.data();
    std::size_t size = raw_data.size();

    // gzip::is_compressed(pointer, size); // can use this to check for gzip file...
    std::string decompressed_data = strEndsWith(path, ".gz") ? gzip::decompress(pointer, size) : raw_data;


    // parse code starts here...
    auto buf = decompressed_data.data();
    auto buf_size = decompressed_data.size();



    // C-version of parsing
    uint64_t row_number = 0;

    auto j = JsonParser_init();
    if (!j)
        throw std::runtime_error("failed to initialize parser");
    JsonParser_open(j, buf, buf_size);
    while (JsonParser_hasNextRow(j)) {
        if (JsonParser_getDocType(j) != JsonParser_objectDocType()) {
            // BADPARSE_STRINGINPUT
            auto line = JsonParser_getMallocedRow(j);
            free(line);
        }

        // line ok, now extract something from the object!
        // => basically need to traverse...
        auto doc = *j->it;

//        auto obj = doc.get_object().take_value();

        // get type
        JsonItem *obj = nullptr;
        uint64_t rc = JsonParser_getObject(j, &obj);
        if (rc != 0)
            break; // --> don't forget to release stuff here!
        char *type_str = nullptr;
        rc = JsonItem_getString(obj, "type", &type_str);
        if (rc != 0)
            continue; // --> don't forget to release stuff here
        JsonItem *sub_obj = nullptr;
        rc = JsonItem_getObject(obj, "repo", &sub_obj);
        if (rc != 0)
            continue; // --> don't forget to release stuff here!

        // check wroong type
        int64_t val_i = 0;
        rc = JsonItem_getInt(obj, "repo", &val_i);
        EXPECT_EQ(rc, ecToI64(ExceptionCode::TYPEERROR));
        if (rc != 0) {
            row_number++;
            JsonParser_moveToNextRow(j);
            continue; // --> next
        }

        char *url_str = nullptr;
        rc = JsonItem_getString(sub_obj, "url", &url_str);

        // error handling: KeyError?
        rc = JsonItem_getString(sub_obj, "key that doesn't exist", &type_str);
        EXPECT_EQ(rc, ecToI64(ExceptionCode::KEYERROR));

        // release all allocated things
        JsonItem_Free(obj);
        JsonItem_Free(sub_obj);

        row_number++;
        JsonParser_moveToNextRow(j);
    }
    JsonParser_close(j);
    JsonParser_free(j);

    std::cout << "Parsed " << pluralize(row_number, "row") << std::endl;
}