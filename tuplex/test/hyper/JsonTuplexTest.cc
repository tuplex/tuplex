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
    bool unwrap_first_level = true;

//    // this seems to work.
//    // should show:
//    // 10, None, 3.414
//    // 2, 42, 12.0      <-- note the order correction
//    // None, None, 2    <-- note the order (fallback/general case)
//    unwrap_first_level = true;
//    ctx.json("../resources/ndjson/example2.json", unwrap_first_level).show();
//    auto v = ctx.json("../resources/ndjson/example2.json", unwrap_first_level).collectAsVector();
//    ASSERT_EQ(v.size(), 3);
//    EXPECT_EQ(v[0].toPythonString(), "(10,None,3.41400)");
//    EXPECT_EQ(v[1].toPythonString(), "(2,42,12.00000)");
//    EXPECT_EQ(v[2].toPythonString(), "(None,None,2)");


    unwrap_first_level = false; // --> requires implementing/adding decoding of struct dict...
    ctx.json("../resources/ndjson/example2.json", unwrap_first_level).show();
    // // large json file (to make sure buffers etc. work
    // ctx.json("/data/2014-10-15.json").show();
}

TEST_F(JsonTuplexTest, GithubLoad) {
    using namespace tuplex;
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0"); // start single-threaded
    Context ctx(opt);
    bool unwrap_first_level = true;

    unwrap_first_level = true;
//    ctx.json("../resources/ndjson/github.json", unwrap_first_level).show();

    // simple func --> this works only with unwrapping!
    ctx.json("../resources/ndjson/github.json", unwrap_first_level)
       .filter(UDF("lambda x: x['type'] == 'PushEvent'"))
       .mapColumn("id", UDF("lambda x: int(x)"))
       .selectColumns(std::vector<std::string>({"repo", "type", "id"}))
       .show();

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