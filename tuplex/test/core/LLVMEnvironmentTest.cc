//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include <codegen/LLVMEnvironment.h>
#include <jit/JITCompiler.h>
#include <string>
#include <vector>
#include <physical/codegen/AggregateFunctions.h>
#include <ContextOptions.h>
#include <jit/RuntimeInterface.h>

// bitmap test
// simple tests for compiled stuff

typedef int(*str_test_func_f)(const char* str);

str_test_func_f compileNullValueComparisonFunction(tuplex::JITCompiler& jit, const std::vector<std::string>& null_values) {

    using namespace llvm;
    using namespace std;
    using namespace tuplex::codegen;

    auto env = make_shared<LLVMEnvironment>();

// v
    FunctionType* FT = FunctionType::get(env->i32Type(), {env->i8ptrType()}, false);

    string name = "strcmp_test";
#if LLVM_VERSION_MAJOR < 9
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT));
#else
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT).getCallee());
#endif
    name = func->getName();

    auto args = mapLLVMFunctionArgs(func, vector<string>{"str"});

    BasicBlock* bbEntry = BasicBlock::Create(env->getContext(), "entry", func);

    IRBuilder<> builder(bbEntry);

    // execute compare code
    auto resVal = env->compareToNullValues(builder, args["str"], null_values);

    builder.CreateRet(builder.CreateZExt(resVal, env->i32Type()));

    // compile func
    auto irCode = env->getIR();

    cout<<"\n"<<irCode<<endl;

    jit.compile(irCode);

    return reinterpret_cast<str_test_func_f>(jit.getAddrOfSymbol(name));
}

TEST(LLVMENV, NullValueStringComparison) {
    using namespace tuplex::codegen;
    using namespace tuplex;
    using namespace std;

    // test function
    JITCompiler jit;

    // empty string
    auto test_func1 = compileNullValueComparisonFunction(jit, {""});
    EXPECT_EQ(test_func1(""), true);
    EXPECT_EQ(test_func1("a"), false);
    EXPECT_EQ(test_func1("abc"), false);

    // a typical NULL string
    auto test_func2 = compileNullValueComparisonFunction(jit, {"NULL"});
    EXPECT_EQ(test_func2(""), false);
    EXPECT_EQ(test_func2("NULL"), true);
    EXPECT_EQ(test_func2("NUL"), false);
    EXPECT_EQ(test_func2("NU"), false);
    EXPECT_EQ(test_func2("N"), false);
    EXPECT_EQ(test_func2("null"), false);
    EXPECT_EQ(test_func2("nul"), false);
    EXPECT_EQ(test_func2("nu"), false);
    EXPECT_EQ(test_func2("n"), false);

    // a couple NULL strings

    // pandas null values:
    vector<string> pandas_null_vals{"", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND",
                                    "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null", "N/a"};

    // compile function and test
    auto test_func3 = compileNullValueComparisonFunction(jit, pandas_null_vals);

    // now check that for each of the strings equality is true
    for(auto s : pandas_null_vals) {
        EXPECT_EQ(test_func3(s.c_str()), true);
    }

    // some other strings that are not contained in the list
    EXPECT_EQ(test_func3("1234"), false);
    EXPECT_EQ(test_func3("hello world"), false);
    EXPECT_EQ(test_func3("-3.14159"), false);
    EXPECT_EQ(test_func3("N/A"), true);
    EXPECT_EQ(test_func3("N/a"), true);
}

typedef int64_t(*bitmap_test_func_f)(bool, int64_t);

bitmap_test_func_f compileBitmapTestFunction(tuplex::JITCompiler& jit) {

    using namespace llvm;
    using namespace std;
    using namespace tuplex::codegen;

    auto env = make_shared<LLVMEnvironment>();

    FunctionType* FT = FunctionType::get(env->i64Type(), {env->i1Type(), env->i64Type()}, false);

    string name = "bitmap_test";
#if LLVM_VERSION_MAJOR < 9
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT));
#else
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT).getCallee());
#endif
    name = func->getName();

    auto args = mapLLVMFunctionArgs(func, vector<string>{"isnull", "pos"});

    BasicBlock* bbEntry = BasicBlock::Create(env->getContext(), "entry", func);

    IRBuilder<> builder(bbEntry);

    // isnull << pos is the result
    // does that work for pos > 32? doubt it...
    auto resVal = builder.CreateShl(
            builder.CreateZExt(args["isnull"], env->i64Type()),
            args["pos"]);

    builder.CreateRet(resVal);

    // compile func
    auto irCode = env->getIR();

    cout<<"\n"<<irCode<<endl;

    jit.compile(irCode);

    return reinterpret_cast<bitmap_test_func_f>(jit.getAddrOfSymbol(name));
}

TEST(LLVMENV, BitmapTest) {
    using namespace tuplex::codegen;
    using namespace tuplex;
    using namespace std;

    // test function
    JITCompiler jit;

    auto func = compileBitmapTestFunction(jit);

    for(int i = 0; i < 64; ++i)
        EXPECT_EQ(func(false, i), 0);

    for(int i = 0; i < 64; ++i) {
        // cout<<"i: "<<i<<" "<<func(true, i)<<endl;
        EXPECT_NE(func(true, i), 0);
    }
}

TEST(LLVMENV, strCastFunctions) {
    using namespace tuplex::codegen;
    using namespace tuplex;
    using namespace std;

    JITCompiler jit;

    // test f64ToStr/i64ToStr...

    // @TODO
}


llvm::Type* createStructType(llvm::LLVMContext& ctx, const python::Type &type, const std::string &twine) {
    using namespace llvm;

    python::Type T = python::Type::propagateToTupleType(type);
    assert(T.isTupleType());

    auto size_field_type = llvm::Type::getInt64Ty(ctx); // what type to use for size fields.

    bool packed = false;

    // empty tuple?
    // is special type
    if(type.parameters().size() == 0) {
        llvm::ArrayRef<llvm::Type*> members;
        llvm::Type *structType = llvm::StructType::create(ctx, members, "emptytuple", packed);

        // // add to mapping (make sure it doesn't exist yet!)
        // assert(_typeMapping.find(structType) == _typeMapping.end());
        // _typeMapping[structType] = type;

        return structType;
    }

    assert(type.parameters().size() > 0);
    // define type
    std::vector<llvm::Type*> memberTypes;

    auto params = type.parameters();
    // count optional elements
    int numNullables = 0;
    for(int i = 0; i < params.size(); ++i) {
        if(params[i].isOptionType()) {
            numNullables++;
            params[i] = params[i].withoutOptions();
        }

        assert(!params[i].isTupleType()); // no nesting at this level here supported!
    }

    int numBitmapElements = core::ceilToMultiple(numNullables, 64) / 64; // 0 if no optional elements
    assert(type.isOptional() ? numBitmapElements > 0 : numBitmapElements == 0);

    // first, create bitmap as array
    if(numBitmapElements > 0) {
        //memberTypes.emplace_back(ArrayType::get(Type::getInt64Ty(ctx), numBitmapElements));
        // i1 array!
        memberTypes.emplace_back(ArrayType::get(Type::getInt1Ty(ctx), numBitmapElements));
    }

    // size fields at end
    int numVarlenFields = 0;

    // define bitmap on the fly
    for(const auto& el: T.parameters()) {
        auto t = el.isOptionType() ? el.getReturnType() : el; // get rid of most outer options

        // @TODO: special case empty tuple! also doesn't need to be represented

        if(python::Type::BOOLEAN == t) {
            // i8
            //memberTypes.push_back(getBooleanType());
            memberTypes.push_back(llvm::Type::getInt64Ty(ctx));
        } else if(python::Type::I64 == t) {
            // i64
            //memberTypes.push_back(i64Type());
            memberTypes.push_back(llvm::Type::getInt64Ty(ctx));
        } else if(python::Type::F64 == t) {
            // double
            memberTypes.push_back(llvm::Type::getDoubleTy(ctx));
        } else if(python::Type::STRING == t) {
            memberTypes.push_back(llvm::Type::getInt8PtrTy(ctx, 0));
            numVarlenFields++;
        } else if(python::Type::GENERICDICT == t || t.isDictionaryType()) { // dictionary
            memberTypes.push_back(llvm::Type::getInt8PtrTy(ctx, 0));
            numVarlenFields++;
        } else if(python::Type::NULLVALUE == t || python::Type::EMPTYTUPLE == t || python::Type::EMPTYDICT == t) {
            // leave out. Not necessary to represent it!
        } else {
            // nested tuple?
            // ==> do lookup!
            // add i64 (for length)
            // and pointer type
            // previously defined? => get!
            if(t.isTupleType()) {
                // recurse!
                // add struct into it (can be accessed via recursion then!!!)
                memberTypes.push_back(createStructType(ctx, t, twine));
            } else {
                Logger::instance().logger("codegen").error("not supported type " + el.desc() + " encountered in LLVM struct type creation");
                return nullptr;
            }
        }
    }

    for(int i = 0; i < numVarlenFields; ++i)
        memberTypes.emplace_back(size_field_type); // 64 bit int as size

    llvm::ArrayRef<llvm::Type*> members(memberTypes);
    llvm::Type *structType = llvm::StructType::create(ctx, members, "struct." + twine, packed);

    // // add to mapping (make sure it doesn't exist yet!)
    // assert(_typeMapping.find(structType) == _typeMapping.end());
    // _typeMapping[structType] = type;

    return structType;
}

TEST(LLVMENV, TupleStructs) {
    // layout of a tuple (flattened), is in general
    // struct tuple {
    //     int64_t bitmap[n];
    //     double a;
    //     int64_t b;
    //     char* c;
    //     int64_t a_size;
    //     int64_t b_size;...
    // };

    // to pass & receive tuples from a function use pointers together with byval / sret attributes!

    using namespace llvm;
    using namespace std;
    using namespace tuplex;
    using namespace tuplex::codegen;
    auto env = make_shared<LLVMEnvironment>();
    auto& ctx = env->getContext();

    auto argTupleType = python::Type::makeTupleType({python::Type::makeOptionType(python::Type::STRING), python::Type::I64, python::Type::F64});
    auto retTupleType = python::Type::makeTupleType({python::Type::STRING, python::Type::F64});

    FunctionType* FT = FunctionType::get(Type::getInt64Ty(ctx), {createStructType(ctx, retTupleType, "tuple")->getPointerTo(),
                                                                 createStructType(ctx, argTupleType, "tuple")->getPointerTo()}, false);

    string name = "process_row";
#if LLVM_VERSION_MAJOR < 9
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT));
#else
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT).getCallee());
#endif
    // add attributes to the arguments (sret, byval)
    for (int i = 0; i < func->arg_size(); ++i) {
        auto& arg = *(func->arg_begin() + i);

        // set attribute
        if(0 == i) {
            arg.setName("outRow");
            // maybe align by 8?
        }

        if(1 == i) {
            arg.setName("inRow");
            //arg.addAttr(Attribute::ByVal);
            // maybe align by 8?
        }
    }

    // add norecurse to function & inline hint
    func->addFnAttr(Attribute::NoRecurse);
    func->addFnAttr(Attribute::InlineHint);
    func->addFnAttr(Attribute::NoUnwind); // explicitly disable unwind! (no external lib calls!)


    auto argMap = mapLLVMFunctionArgs(func, {"outRow", "inRow"});

    // codegen
    BasicBlock* bbEntry = BasicBlock::Create(ctx, "entry", func);
    IRBuilder<> builder(bbEntry);

    auto val = env->getTupleElement(builder, argTupleType, argMap["inRow"], 0);
    env->setTupleElement(builder, retTupleType, argMap["outRow"], 1, SerializableValue(env->f64Const(3.141), nullptr, nullptr));

    builder.CreateRet(env->i64Const(0));

    cout<<"\n"<<env->getIR()<<endl;


    // compile
    JITCompiler jit;
    jit.compile(env->getIR());

    auto addrFunctor = jit.getAddrOfSymbol(name);

    EXPECT_TRUE(addrFunctor);
}


TEST(LLVMENV, SingleElementStructTypes) {
    using namespace llvm;
    using namespace std;
    using namespace tuplex;
    using namespace tuplex::codegen;
    auto env = make_shared<LLVMEnvironment>();
    auto& ctx = env->getContext();

    // store/load for option[()], option[{}] and null?
    auto et_type = python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::EMPTYTUPLE));
    auto ed_type = python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::EMPTYDICT));

    auto et_llvm = env->getOrCreateTuplePtrType(et_type);
    auto ed_llvm = env->getOrCreateTuplePtrType(ed_type);


    FunctionType* FT = FunctionType::get(Type::getInt64Ty(ctx), {et_llvm,
                                                                 ed_llvm}, false);

    string name = "process_row";

#if LLVM_VERSION_MAJOR < 9
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT));
#else
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT).getCallee());
#endif

    auto argMap = mapLLVMFunctionArgs(func, {"outRow", "inRow"});

    // codegen
    BasicBlock* bbEntry = BasicBlock::Create(ctx, "entry", func);
    IRBuilder<> builder(bbEntry);

    auto et_res = env->getTupleElement(builder, et_type, argMap["outRow"], 0);
    auto ed_res = env->getTupleElement(builder, ed_type, argMap["inRow"], 0);

    ASSERT_TRUE(et_res.is_null); // is null option should be a LLVM Value for both...
    ASSERT_TRUE(ed_res.is_null);

    // setting values
    env->setTupleElement(builder, et_type, argMap["outRow"], 0, codegen::SerializableValue(nullptr, nullptr, env->i1Const(false)));
    env->setTupleElement(builder, ed_type, argMap["inRow"], 0, codegen::SerializableValue(nullptr, nullptr, env->i1Const(false)));


    builder.CreateRet(env->i64Const(0));

    // dummy to make sure test is counted, however here it's more about that calls work...
    ASSERT_FALSE(env->getIR().empty());
    cout<<env->getIR()<<endl;


    // fetch bitcode
    size_t bufSize = 0;
    auto buf = moduleToBitCode(*env->getModule().get(), &bufSize);
    cout<<"wrote module to bitcode: "<<bufSize<<" bytes"<<endl;
    cout<<"module size in IR: "<<env->getIR().size()<<endl;
    delete [] buf;
}

TEST(LLVMENV, StringConstantFromGlobal) {
    using namespace llvm;
    using namespace std;
    using namespace tuplex;
    using namespace tuplex::codegen;
    auto env = make_shared<LLVMEnvironment>();
    auto& ctx = env->getContext();

    FunctionType* FT = FunctionType::get(env->i64Type(), {}, false);
    string name = "test";
#if LLVM_VERSION_MAJOR < 9
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT));
#else
    Function* func = cast<Function>(env->getModule()->getOrInsertFunction(name, FT).getCallee());
#endif

    BasicBlock* bb = BasicBlock::Create(ctx, "body", func);
    IRBuilder<> builder(bb);

    auto strObj = env->strConst(builder, "teststring");
    builder.CreateRet(env->i64Const(0));

    EXPECT_EQ(codegen::globalVariableToString(strObj), "teststring");
}

extern "C" void throwingFunc() {
    throw std::runtime_error("some exception...");
}

TEST(LLVMENV, exceptionsegfault) {
    using namespace llvm;
    using namespace tuplex;
    using namespace std;

    auto jit = new JITCompiler();

    delete jit;

    try {
        throwingFunc();
    } catch(const std::exception& e) {
        string s = e.what();
        EXPECT_EQ(s, "some exception...");
    }
}

TEST(LLVMENV, initAggregateFixedSize) {
    using namespace llvm;
    using namespace std;
    using namespace tuplex;
    using namespace tuplex::codegen;
    auto env = make_shared<LLVMEnvironment>();
    auto jit = make_shared<JITCompiler>();

    Row row(10, 20);
    auto func = createAggregateInitFunction(env.get(), "initAggregate", row);

    jit->compile(env->getIR());

    auto fun = reinterpret_cast<int64_t(*)(void**,int64_t*)>(jit->getAddrOfSymbol(func->getName().str()));

    uint8_t* agg = nullptr;
    int64_t size = 0;

    ASSERT_EQ(fun(reinterpret_cast<void **>(&agg), &size), 0);

    // compare aggregate!
    EXPECT_EQ(size, 16);
    EXPECT_EQ(*(int64_t*)agg, 10);
    EXPECT_EQ(*((int64_t*)agg + 1), 20);
}

TEST(LLVMENV, initAggregateVariable) {
    using namespace llvm;
    using namespace std;
    using namespace tuplex;
    using namespace tuplex::codegen;
    auto env = make_shared<LLVMEnvironment>();
    auto jit = make_shared<JITCompiler>();

    Row row(42, 99, "Hello world!");
    auto func = createAggregateInitFunction(env.get(), "initAggregate", row);

    jit->compile(env->getIR());

    auto fun = reinterpret_cast<int64_t(*)(void**,int64_t*)>(jit->getAddrOfSymbol(func->getName().str()));

    uint8_t* agg = nullptr;
    int64_t size = 0;

    ASSERT_EQ(fun(reinterpret_cast<void **>(&agg), &size), 0);

    // deserialize Row!
    auto res = Row::fromMemory(Schema(Schema::MemoryLayout::ROW, row.getRowType()), agg, size);
    EXPECT_EQ(res.toPythonString(), "(42,99,'Hello world!')");
}

TEST(LLVMENV, combineAggregate) {
    using namespace llvm;
    using namespace std;
    using namespace tuplex;
    using namespace tuplex::codegen;
    auto env = make_shared<LLVMEnvironment>();
    auto jit = make_shared<JITCompiler>();

    UDF udf("lambda a, b: a + b");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::I64, python::Type::I64})));

    auto func = createAggregateCombineFunction(env.get(), "combineAggregate", udf, python::Type::I64);
    jit->compile(env->getIR());

    auto fun = reinterpret_cast<int64_t(*)(void**,int64_t*,void*,int64_t)>(jit->getAddrOfSymbol(func->getName().str()));

    // two sample aggregates
    int64_t* agg0 = new int64_t; *agg0 = 10; // needs to be allocated, b.c. could get replaced!
    int64_t agg1 = 20;

    int64_t s = 8;

    EXPECT_EQ(fun(reinterpret_cast<void**>(&agg0), &s, &agg1, s), 0);
    EXPECT_EQ(*agg0, 30);
    EXPECT_EQ(s, 8);
}

TEST(LLVMENV, combineAggregateVariable) {
    using namespace llvm;
    using namespace std;
    using namespace tuplex;
    using namespace tuplex::codegen;

    auto env = make_shared<LLVMEnvironment>();
    auto jit = make_shared<JITCompiler>();

    // init runtime memory
    runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

    Row rowA("Hello");
    Row rowB(" world!");
    auto funcInitA = createAggregateInitFunction(env.get(), "initAggregateHello", rowA);
    auto funcInitB = createAggregateInitFunction(env.get(), "initAggregateWorld", rowB);

    UDF udf("lambda a, b: a + b");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::STRING, python::Type::STRING})));

    auto func = createAggregateCombineFunction(env.get(), "combineAggregate", udf, python::Type::STRING);
    jit->compile(env->getIR());

    auto fun = reinterpret_cast<int64_t(*)(void**,int64_t*,void*,int64_t)>(jit->getAddrOfSymbol(func->getName().str()));
    auto funInitA = reinterpret_cast<int64_t(*)(void**,int64_t*)>(jit->getAddrOfSymbol(funcInitA->getName().str()));
    auto funInitB = reinterpret_cast<int64_t(*)(void**,int64_t*)>(jit->getAddrOfSymbol(funcInitB->getName().str()));

    // init first!
    char* aggA = nullptr; int64_t aggA_size = 0;
    char* aggB = nullptr; int64_t aggB_size = 0;

    funInitA(reinterpret_cast<void **>(&aggA), &aggA_size);
    funInitB(reinterpret_cast<void **>(&aggB), &aggB_size);

    Row resA = Row::fromMemory(rowA.getSchema(), aggA, aggA_size);
    Row resB = Row::fromMemory(rowB.getSchema(), aggB, aggB_size);

    EXPECT_EQ(resA.getString(0), "Hello");
    EXPECT_EQ(resB.getString(0), " world!");

    // now, combine everything!
    fun(reinterpret_cast<void **>(&aggA), &aggA_size, aggB, aggB_size);

    Row resC = Row::fromMemory(rowA.getSchema(), aggA, aggA_size);
    EXPECT_EQ(resC.getString(0), "Hello world!");
}