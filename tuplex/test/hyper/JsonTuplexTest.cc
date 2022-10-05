//
// Created by leonhard on 9/29/22.
//

#include "HyperUtils.h"

class JsonTuplexTest : public HyperPyTest {};


TEST_F(JsonTuplexTest, BasicLoad) {
    using namespace tuplex;
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0"); // start single-threaded
    Context ctx(opt);

    // this seems to work.
    // should show:
    // 10, None, 3.414
    // 2, 42, 12.0      <-- note the order correction
    // None, None, 2    <-- note the order (fallback/general case)
    bool unwrap_first_level = true;
    ctx.json("../resources/ndjson/example2.json", unwrap_first_level).show();
    auto v = ctx.json("../resources/ndjson/example2.json", unwrap_first_level).collectAsVector();
    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "(10,None,3.41400)");
    EXPECT_EQ(v[1].toPythonString(), "(2,42,12.00000)");
    EXPECT_EQ(v[2].toPythonString(), "(None,None,2)");


//    unwrap_first_level = false; // --> requires implementing/adding decoding of struct dict...
//    ctx.json("../resources/ndjson/example2.json", unwrap_first_level).show();
    // // large json file (to make sure buffers etc. work
    // ctx.json("/data/2014-10-15.json").show();
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