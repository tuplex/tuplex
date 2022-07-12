//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Context.h>
#include "TestUtils.h"

#include <physical/codegen/PipelineBuilder.h>

class VariableTest : public PyTest {};

TEST_F(VariableTest, TypeReassign) {
    // use a function where types get reassigned (no branches!)
    // -> version where actually param of function gets reassigned
    // -> version where just some there declared variable gets reassigned.
    using namespace tuplex;

    Context c(microTestOptions());

    auto codeI = "def f(x):\n"
                  "\tx = 20\n"
                  "\tx = 7.8\n"
                  "\tx = False\n"
                  "\tx = 'hello world'\n"
                  "\tx = {}\n"
                  "\tx = (1, 2, 3)\n"
                  "\treturn x";
    auto resI = c.parallelize({Row(10)}).map(UDF(codeI)).collectAsVector();
    ASSERT_EQ(resI.size(), 1);
    EXPECT_EQ(resI[0].toPythonString(), "(1,2,3)");

    auto codeII = "def f(x):\n"
                "\tx = 20\n"
                "\tx = 7.8\n"
                "\tx = False\n"
                "\tx = 'hello world'\n"
                "\tx = {}\n"
                "\tx = (1, 2, 3)\n"
                "\treturn x";
    auto resII = c.parallelize({Row(10)}).map(UDF(codeII)).collectAsVector();
    ASSERT_EQ(resII.size(), 1);
    EXPECT_EQ(resII[0].toPythonString(), "(1,2,3)");
}

TEST_F(VariableTest, RandomAssignExpressions) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto codeI = "def f(x):\n"
                 "\tz = 20\n"
                 "\tx -= 18\n"
                 "\tx = x * z\n"
                 "\ty = x - 1.0\n"
                 "\tx = 'hello world'\n"
                 "\tz = 2 * x\n"
                 "\tw = (x[0] + str(x)).lstrip()\n"
                 "\treturn x, y, z, w";
    auto resI = c.parallelize({Row(10), Row(20)}).map(UDF(codeI)).collectAsVector();
    ASSERT_EQ(resI.size(), 2);
    EXPECT_EQ(resI[0].toPythonString(), "('hello world',-161.00000,'hello worldhello world','hhello world')");
    EXPECT_EQ(resI[1].toPythonString(), "('hello world',39.00000,'hello worldhello world','hhello world')");
}

// now if statements, which do potentially lead to issues...
TEST_F(VariableTest, IfAndVariables) {
    using namespace tuplex;

    Context c(microTestOptions());

    // scenario I:
    // => no issues, new variable has same type in each branch. Thus, there's no conflict.
    auto codeI = "def f(x):\n"
                 "\tz = 20\n"
                 "\tif x > 20:\n"
                 "\t\tz = 24\n"
                 "\telse:\n"
                 "\t\tz = 27\n"
                 "\treturn x,z\n";
    auto resI = c.parallelize({Row(22), Row(18)}).map(UDF(codeI)).collectAsVector();
    ASSERT_EQ(resI.size(), 2);
    EXPECT_EQ(resI[0].toPythonString(), "(22,24)");
    EXPECT_EQ(resI[1].toPythonString(),"(18,27)");

    // Note: for the following two scenarios, by rewriting the AST, an optimization can be carried out!
    // i.e. by copying whatever follows the if statement within it,

    // scenario II:
    // => variable get castable type assigned in each branch, hence if undefinedBehavior is defined,
    // can optimize that.

    // Reassigning example but no further use:
    // ==> this should work too!
    // i.e., the return statement makes this a final block. not necessary to speculate on stuff then...
    // def f(x):
    //    if x > 20:
    //       x = 'hello world'
    //       return x[:5]
    //    return str(x)
    auto code_altI = "def f(x):\n"
                     "   if x > 20:\n"
                     "      x = 'hello world'\n"
                     "      return x[:5]\n"
                     "   return str(x)";
    auto& ds_altI = c.parallelize({Row(19), Row(21)}).map(UDF(code_altI));
    EXPECT_EQ(ds_altI.schema().getRowType().desc(), python::Type::propagateToTupleType(python::Type::STRING).desc());
    auto res_altI = ds_altI.collectAsVector();
    ASSERT_EQ(res_altI.size(), 2);
    EXPECT_EQ(res_altI[0].toPythonString(), "('19',)");
    EXPECT_EQ(res_altI[1].toPythonString(), "('hello',)");

    // check nested version as well:
    // def f(x, y):
    //    if x > 20:
    //       x = 'hello world'
    //       if y == 0:
    //           return x[:5]
    //       else:
    //           return 'blub'
    //    return str(x)

    auto code_altII = "def f(x, y):\n"
                      "   if x > 20:\n"
                      "      x = 'hello world'\n"
                      "      if y == 0:\n"
                      "          return x[:5]\n"
                      "      else:\n"
                      "          return 'blub'\n"
                      "   return str(x)";
    auto& ds_altII = c.parallelize({Row(19, 0), Row(21, 0), Row(22, 1)}).map(UDF(code_altII));
    EXPECT_EQ(ds_altII.schema().getRowType().desc(), python::Type::propagateToTupleType(python::Type::STRING).desc());
    auto res_altII = ds_altII.collectAsVector();
    ASSERT_EQ(res_altII.size(), 3);
    EXPECT_EQ(res_altII[0].toPythonString(), "('19',)");
    EXPECT_EQ(res_altII[1].toPythonString(), "('hello',)");
    EXPECT_EQ(res_altII[2].toPythonString(), "('blub',)");


    // def f(x, y):
    //    if x > 20:
    //       x = 'hello world'
    //       if y == 0:
    //           return x[:5]
    //       return 'blub'
    //    return str(x)

    auto code_altIII = "def f(x, y):\n"
                       "   if x > 20:\n"
                       "      x = 'hello world'\n"
                       "      if y == 0:\n"
                       "          return x[:5]\n"
                       "      return 'blub'\n"
                       "   return str(x)";
    auto& ds_altIII = c.parallelize({Row(19, 0), Row(21, 0), Row(22, 1)}).map(UDF(code_altIII));
    EXPECT_EQ(ds_altIII.schema().getRowType().desc(), python::Type::propagateToTupleType(python::Type::STRING).desc());
    auto res_altIII = ds_altIII.collectAsVector();
    ASSERT_EQ(res_altIII.size(), 3);
    EXPECT_EQ(res_altIII[0].toPythonString(), "('19',)");
    EXPECT_EQ(res_altIII[1].toPythonString(), "('hello',)");
    EXPECT_EQ(res_altIII[2].toPythonString(), "('blub',)");


    // Note: with multitypes etc., we could also carry out better dataflow analysis and rewrite things accordingly.
    // for this, tracing could be used!

    auto codeII = "def f(x):\n"
                  "\tz = 20\n"
                  "\tif x <= 20:\n"
                  "\t\tz = 3.14159\n"
                  "\telse:\n"
                  "\t\tz = True\n"
                  "\treturn x,z\n";

    // i.e. z has here a conflict for the two branches: It's once assigned as bool, once as f64.
    // => b.c. we allow undefined behavior this can be unified into float.
    // else, we would need to use speculation.
    auto opt_undef = microTestOptions();
    opt_undef.set("tuplex.autoUpcast", "true");
    auto opt_noundef = opt_undef;
    opt_noundef.set("tuplex.autoUpcast", "false");
    Context c_undef(opt_undef);
    Context c_noundef(opt_noundef);

    // optimized, i.e. z's return type gets unified as float
    auto resIIa = c_undef.parallelize({Row(22), Row(18)}).map(UDF(codeII)).collectAsVector();
    ASSERT_EQ(resIIa.size(), 2);
    EXPECT_EQ(resIIa[0].toPythonString(), "(22,1.00000)");
    EXPECT_EQ(resIIa[1].toPythonString(),"(18,3.14159)");

    // strictly follow python semantics, i.e. need to speculate on types
    // and hence interpreter needs to get invoked.
    auto resIIb = c_noundef.parallelize({Row(22), Row(18)}).map(UDF(codeII)).collectAsVector();
    ASSERT_EQ(resIIb.size(), 2);
    EXPECT_EQ(resIIb[0].toPythonString(), "(22,True)");
    EXPECT_EQ(resIIb[1].toPythonString(),"(18,3.14159)");

    // scenario III:
    // => variables declared within branches get assigned different types.
    // => need to speculate and deactivate a branch, then process via interpreter!
    // usually only one frequent type will occur! it's not in the nature of a developer to write esoteric code
    // with polymorphic return types
    auto codeIII = "def f(x):\n"
                  "\tz = 20\n"
                  "\tif x > 20:\n"
                  "\t\tz = 42\n"
                  "\telse:\n"
                  "\t\tz = 'smaller than 20'\n"
                  "\treturn x,z\n";
    auto resIII = c.parallelize({Row(22), Row(18)}).map(UDF(codeIII)).collectAsVector();
    ASSERT_EQ(resIII.size(), 2);
    EXPECT_EQ(resIII[0].toPythonString(), "(22,42)");
    EXPECT_EQ(resIII[1].toPythonString(),"(18,'smaller than 20')");
}

TEST_F(VariableTest, ExtractPriceRedef) {
    using namespace tuplex;
    auto extractPrice_c = "def extractPrice(x):\n"
                          "    price = x['price']\n"
                          "\n"
                          "    if x['offer'] == 'sold':\n"
                          "        price = 20\n"
                          "    else:\n"
                          "        # take price from price column\n"
                          "        price = 7\n"
                          "\n"
                          "    return price";

    Context c(microTestOptions());
    auto res = c.parallelize({Row(10.0, "Coca Cola", "sold"),
                              Row(5.0, "Sprite", "available")},
                             {"price", "name", "offer"})
             .map(UDF(extractPrice_c)).collectAsVector();

    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].getInt(0), 20);
    EXPECT_EQ(res[1].getInt(0), 7);
}

TEST_F(VariableTest, IfBranchTypeSpeculation) {
    using namespace tuplex;

    auto codeII = "def f(x):\n"
                  "\tz = 20\n"
                  "\tif x <= 20:\n"
                  "\t\tz = 3.14159\n"
                  "\telse:\n"
                  "\t\tz = True\n"
                  "\treturn x,z\n";

    // i.e. z has here a conflict for the two branches: It's once assigned as bool, once as f64.
    // => b.c. we allow undefined behavior this can be unified into float.
    // else, we would need to use speculation.
    auto opt_undef = microTestOptions();
    opt_undef.set("tuplex.autoUpcast", "true");
    auto opt_noundef = opt_undef;
    opt_noundef.set("tuplex.autoUpcast", "false");
    Context c_undef(opt_undef);
    Context c_noundef(opt_noundef);

    // works.
    // optimized, i.e. z's return type gets unified as float
    auto& dsIIa = c_undef.parallelize({Row(22), Row(18)}).map(UDF(codeII));
    auto rowtypeIIa = dsIIa.schema().getRowType();
    EXPECT_EQ(rowtypeIIa.desc(), python::Type::makeTupleType({python::Type::I64, python::Type::F64}).desc());
    // compute results
    auto resIIa = dsIIa.collectAsVector();
    ASSERT_EQ(resIIa.size(), 2);
    EXPECT_EQ(resIIa[0].toPythonString(), "(22,1.00000)");
    EXPECT_EQ(resIIa[1].toPythonString(), "(18,3.14159)");


    // strictly follow python semantics, i.e. need to speculate on types
    // and hence interpreter needs to get invoked.
    // for this, simply deduce from input sample!
    auto& dsIIb1 = c_noundef.parallelize({Row(22), Row(21), Row(18)}).map(UDF(codeII));
    auto rowtypeIIb1 = dsIIb1.schema().getRowType();
    EXPECT_EQ(rowtypeIIb1.desc(), python::Type::makeTupleType({python::Type::I64, python::Type::BOOLEAN}).desc());

    auto resIIb1 = dsIIb1.collectAsVector();
    ASSERT_EQ(resIIb1.size(), 3);
    EXPECT_EQ(resIIb1[0].toPythonString(), "(22,True)");
    EXPECT_EQ(resIIb1[1].toPythonString(), "(21,True)");
    EXPECT_EQ(resIIb1[2].toPythonString(), "(18,3.14159)");


    auto& dsIIb2 = c_noundef.parallelize({Row(22), Row(19), Row(18)}).map(UDF(codeII));
    auto rowtypeIIb2 = dsIIb2.schema().getRowType();
    EXPECT_EQ(rowtypeIIb2.desc(), python::Type::makeTupleType({python::Type::I64, python::Type::F64}).desc());

    auto resIIb2 = dsIIb2.collectAsVector();
    ASSERT_EQ(resIIb2.size(), 3);
    EXPECT_EQ(resIIb2[0].toPythonString(), "(22,True)");
    EXPECT_EQ(resIIb2[1].toPythonString(), "(19,3.14159)");
    EXPECT_EQ(resIIb2[2].toPythonString(), "(18,3.14159)");
}


TEST_F(VariableTest, IfBranchTypeSpeculationlarge) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(x):\n"
                  "\tz = 20\n"
                  "\tif x <= 20:\n"
                  "\t\tz = 10 / x\n"
                  "\telse:\n"
                  "\t\tz = x % 2 == 1\n"
                  "\treturn x,z\n";

    // i.e. z has here a conflict for the two branches: It's once assigned as bool, once as f64.
    // => b.c. we allow undefined behavior this can be unified into float.
    // else, we would need to use speculation.
    auto opt_undef = microTestOptions();
    opt_undef.set("tuplex.autoUpcast", "true");
    auto opt_noundef = opt_undef;
    opt_noundef.set("tuplex.autoUpcast", "false");
    Context c_undef(opt_undef);
    Context c_noundef(opt_noundef);

    // perform a larger test on speculating so multiple partitions etc. are used
    srand(40);
    int N = 10000;
    vector<Row> ref_undef;
    vector<Row> ref_noundef;
    vector<Row> in;
    for(int i = 0; i < N; ++i) {
        auto x = rand() % 40 + 1; // 1 - 40
        in.push_back(Row(x));
        if(x <= 20) {
            ref_undef.push_back(Row(x, 10.0 / x));
            ref_noundef.push_back(Row(x, 10.0 / x));
        } else {
            ref_undef.push_back(Row(x, 1.0 * (x % 2 == 1)));
            ref_noundef.push_back(Row(x, x % 2 == 1));
        }
    }
    ASSERT_EQ(in.size(), ref_undef.size());
    ASSERT_EQ(in.size(), ref_noundef.size());

    // check undef first
    auto res_undef = c_undef.parallelize(in).map(UDF(code)).collectAsVector();
    ASSERT_EQ(res_undef.size(), ref_undef.size());
    for(int i = 0; i < res_undef.size(); ++i)
        EXPECT_EQ(res_undef[i].toPythonString(), ref_undef[i].toPythonString());

    // now, check when autoupcast is disabled. I.e., two cases need to get processed separately.
    auto res_noundef = c_noundef.parallelize(in).map(UDF(code)).collectAsVector();
    ASSERT_EQ(res_noundef.size(), ref_undef.size());

    // // for debug purposes, print valuef
    // cout<<"\nN\tres\tref\n";
    // for(int i = 0; i < res_noundef.size(); ++i) {
    //     cout<<i<<":\t"<<res_noundef[i].toPythonString()<<"\t"<<ref_noundef[i].toPythonString()<<endl;
    // }

    for(int i = 0; i < res_noundef.size(); ++i)
        EXPECT_EQ(res_noundef[i].toPythonString(), ref_noundef[i].toPythonString());
}

TEST_F(VariableTest, SingleIf) {
    // code with single if statement, no else.

    using namespace tuplex;
    using namespace std;

    // i.e. speculate on i64/f64.
    auto code = "def f(x):\n"
                "\tz = 20\n"
                "\tif x <= 20:\n"
                "\t\tz = 10 / x\n"
                "\treturn x,z\n";

    // i.e. z has here a conflict for the two branches: It's once assigned as bool, once as f64.
    // => b.c. we allow undefined behavior this can be unified into float.
    // else, we would need to use speculation.
    auto opt_undef = microTestOptions();
    opt_undef.set("tuplex.autoUpcast", "true");
    auto opt_noundef = opt_undef;
    opt_noundef.set("tuplex.autoUpcast", "false");
    Context c_undef(opt_undef);
    Context c_noundef(opt_noundef);

    // perform a larger test on speculating so multiple partitions etc. are used
    srand(43);
    int N = 10;
    vector<Row> ref_undef;
    vector<Row> ref_noundef;
    vector<Row> in;
    for(int i = 0; i < N; ++i) {
        auto x = rand() % 40 + 1; // 1 - 40
        in.push_back(Row(x));
        if(x <= 20) {
            ref_undef.push_back(Row(x, 10.0 / x));
            ref_noundef.push_back(Row(x, 10.0 / x));
        } else {
            ref_undef.push_back(Row(x, 20.0));
            ref_noundef.push_back(Row(x, 20));
        }
    }
    ASSERT_EQ(in.size(), ref_undef.size());
    ASSERT_EQ(in.size(), ref_noundef.size());

    // check with autoupcast true first
    auto res_undef = c_undef.parallelize(in).map(UDF(code)).collectAsVector();
    ASSERT_EQ(res_undef.size(), ref_undef.size());
    for(int i = 0; i < res_undef.size(); ++i)
        EXPECT_EQ(res_undef[i].toPythonString(), ref_undef[i].toPythonString());

     // // for debug purposes, print valuef
     // cout<<"\nN\tres\tref\n";
     // for(int i = 0; i < res_undef.size(); ++i) {
     //     cout<<i<<":\t"<<res_undef[i].toPythonString()<<"\t"<<ref_undef[i].toPythonString()<<endl;
     // }

    // now, check when autoupcast is disabled. I.e., two cases need to get processed separately.
    auto res_noundef = c_noundef.parallelize(in).map(UDF(code)).collectAsVector();
    ASSERT_EQ(res_noundef.size(), ref_undef.size());

     // // for debug purposes, print valuef
     // cout<<"\nN\tres\tref\n";
     // for(int i = 0; i < res_noundef.size(); ++i) {
     //     cout<<i<<":\t"<<res_noundef[i].toPythonString()<<"\t"<<ref_noundef[i].toPythonString()<<endl;
     // }

    for(int i = 0; i < res_noundef.size(); ++i)
        EXPECT_EQ(res_noundef[i].toPythonString(), ref_noundef[i].toPythonString());
}

TEST_F(VariableTest, NonLeakingComprehensions) {
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    // python test example is:
    // def f(x):
    //    t = 'test'
    //    y = [t * t for t in range(x)]
    //    return t, y[-1]
    //
    //print(f(1))
    //print(f(2))
    //print(f(3))
    //print(f(4))
    //
    //('test', 0)
    //('test', 1)
    //('test', 4)
    //('test', 9)

    // check with this code that t defined in list comprehension does not leak outside.
    auto code = "def f(x):\n"
                "    t = 'test'\n"
                "    y = [t * t for t in range(x)]\n"
                "    return t, y[-1]\n";

    auto res = c.parallelize({Row(1), Row(2), Row(3), Row(4)}).map(UDF(code)).collectAsVector();
    ASSERT_EQ(res.size(), 4);
    EXPECT_EQ(res[0].toPythonString(), "('test',0)");
    EXPECT_EQ(res[1].toPythonString(), "('test',1)");
    EXPECT_EQ(res[2].toPythonString(), "('test',4)");
    EXPECT_EQ(res[3].toPythonString(), "('test',9)");
}

TEST_F(VariableTest, IfElseReturnAndVars) {
    using namespace tuplex;
    using namespace std;

    auto opt_auto = microTestOptions();
    opt_auto.set("tuplex.autoUpcast", "true");
    auto opt_noauto = opt_auto;
    opt_noauto.set("tuplex.autoUpcast", "false");
    Context c_auto(opt_auto);
    Context c_noauto(opt_noauto);

    // create a conflict in if or else branch!
    // => test then again with autoupcasting and no autoupcasting!

    vector<Row> in_rows{Row(18), Row(19), Row(20), Row(21)};

    // part A
    // -------------------------------------------------------
    // i.e. speculate on i64/f64.
    auto codeA = "def f(x):\n"
                "\tz = 20\n"
                "\tif x <= 20:\n"
                "\t\treturn x, 10/x\n"
                "\treturn x,z\n";
    vector<Row> refA_auto{Row(18, 10.0 / 18), Row(19, 10.0/19), Row(20, 10.0/20), Row(21, 20.0)};
    vector<Row> refA_noauto{Row(18, 10.0 / 18), Row(19, 10.0/19), Row(20, 10.0/20), Row(21, 20)};

    auto resA_auto = c_auto.parallelize(in_rows).map(UDF(codeA)).collectAsVector();
    auto resA_noauto = c_noauto.parallelize(in_rows).map(UDF(codeA)).collectAsVector();

    // check equality
    ASSERT_EQ(resA_auto.size(), in_rows.size());
    ASSERT_EQ(resA_noauto.size(), in_rows.size());

    for(int i = 0; i < in_rows.size(); ++i) {
        EXPECT_EQ(resA_auto[i].toPythonString(), refA_auto[i].toPythonString());
        EXPECT_EQ(resA_noauto[i].toPythonString(), refA_noauto[i].toPythonString());
    }

    // part B
    // -------------------------------------------------------
    auto codeB = "def f(x):\n"
                 "\tz = 20\n"
                 "\tif x <= 20:\n"
                 "\t\tw = 24\n"
                 "\t\tz = 10/x\n"
                 "\telse:\n"
                 "\t\treturn x, z\n"
                 "\treturn x,z\n";

    vector<Row> refB_auto{Row(18, 10.0 / 18), Row(19, 10.0/19), Row(20, 10.0/20), Row(21, 20.0)};
    vector<Row> refB_noauto{Row(18, 10.0 / 18), Row(19, 10.0/19), Row(20, 10.0/20), Row(21, 20)};

    auto resB_auto = c_auto.parallelize(in_rows).map(UDF(codeB)).collectAsVector();
    auto resB_noauto = c_noauto.parallelize(in_rows).map(UDF(codeB)).collectAsVector();

    // check equality
    ASSERT_EQ(resB_auto.size(), in_rows.size());
    ASSERT_EQ(resB_noauto.size(), in_rows.size());

    for(int i = 0; i < in_rows.size(); ++i) {
        EXPECT_EQ(resB_auto[i].toPythonString(), refB_auto[i].toPythonString());
        EXPECT_EQ(resB_noauto[i].toPythonString(), refB_noauto[i].toPythonString());
    }

    // part C
    // -------------------------------------------------------
    auto codeC = "def f(x):\n"
                 "\tz = 20\n"
                 "\tif x <= 20:\n"
                 "\t\treturn x, z\n"
                 "\telse:\n"
                 "\t\tz = 10/x\n"
                 "\treturn x,z\n";
    // x = 18, 19, 20, 21
    vector<Row> refC_auto{Row(18, 20.0), Row(19, 20.0), Row(20, 20.0), Row(21, 10.0 / 21.0)};
    vector<Row> refC_noauto{Row(18, 20), Row(19, 20), Row(20, 20), Row(21, 10.0 / 21.0)};

    auto resC_auto = c_auto.parallelize(in_rows).map(UDF(codeC)).collectAsVector();
    auto resC_noauto = c_noauto.parallelize(in_rows).map(UDF(codeC)).collectAsVector();

    // check equality
    ASSERT_EQ(resC_auto.size(), in_rows.size());
    ASSERT_EQ(resC_noauto.size(), in_rows.size());

    for(int i = 0; i < in_rows.size(); ++i) {
        EXPECT_EQ(resC_auto[i].toPythonString(), refC_auto[i].toPythonString());
        EXPECT_EQ(resC_noauto[i].toPythonString(), refC_noauto[i].toPythonString());
    }
}

#warning "TODO: speculation on return type for lambda function as well!"

TEST_F(VariableTest, AugAssign) {
    using namespace tuplex;

    Context c(microTestOptions());

    // taken from test_augassign.py of the cpython tests
    // class AugAssignTest(unittest.TestCase):
    //    def testBasic(self):
    //        x = 2
    //        x += 1
    //        x *= 2
    //        x **= 2
    //        x -= 8
    //        x //= 5
    //        x %= 3
    //        x &= 2
    //        x |= 5
    //        x ^= 1
    //        x /= 2
    //        self.assertEqual(x, 3.0)
    auto code = "def f(y):\n"
                "    x = 2\n"
                "    x += 1\n"
                "    x *= 2\n"
                "    x **= 2\n"
                "    x -= 8\n"
                "    x //= 5\n"
                "    x %= 3\n"
                "    x &= 2\n"
                "    x |= 5\n"
                "    x ^= 1\n"
                "    x /= 2\n"
                "    return x";
    auto res = c.parallelize({Row(0)}).map(UDF(code)).collectAsVector();
    ASSERT_EQ(res.size(), 1);
    EXPECT_DOUBLE_EQ(res[0].getDouble(0), 3.0);
}

TEST_F(VariableTest, SharedTypeOverride) {
    // when the variable as a result in both branches yields a common type, then simply overwrite the internal slot.
    // no need for speculation then.
    // def f(x):
    //    x = 20
    //    if x > 20:
    //       x = 'hello'
    //    else:
    //       x = 'test'
    //    return x
    using namespace tuplex;

    Context c(microTestOptions());

    auto code = "def f(x):\n"
                "   x = 20\n"
                "   if x > 20:\n"
                "      x = 'hello'\n"
                "   else:\n"
                "      x = 'test'\n"
                "   return x";

    auto res = c.parallelize({Row(10)}).map(UDF(code)).collectAsVector();
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0].getString(0), "test");

    // same code, this time with a lot of unnecessary overwrites...
    auto code_alt = "def f(x):\n"
                "   x = 20\n"
                "   if x > 20:\n"
                "      x = 3.14159\n"
                "      x = -1\n"
                "      x = {}\n"
                "      x = 'hello'\n"
                "   else:\n"
                "      x = 3.14159\n"
                "      x = -1\n"
                "      x = {}\n"
                "      x = 'test'\n"
                "   return x";

    auto res_alt = c.parallelize({Row(10)}).map(UDF(code_alt)).collectAsVector();
    ASSERT_EQ(res_alt.size(), 1);
    EXPECT_EQ(res_alt[0].getString(0), "test");
}


// this file contains various test programs to make sure python variable rules are follow.
// they're sometimes very weird.

// test program I:
// -------------
// x = 'hello'
// for x in range(2): # leaks through!
//     print(x)
// print(x)
// ------------
// output is:
// 0
// 1
// 1
// => x leaked via for loop.

// test program II:
// ----------------
// x = 'hello'
// [print(x) for x in range(2)]
// print(x)
// ----------------
// output is:
// 0
// 1
// hello


// test program III:
// -----------------
// def f(x):
//
//    if x > 10:
//        z = 3.141
//    else:
//        w = 4.5
//    return z
// -----------------
// output is:
// f(11) => 3.141
// f(9) => UnboundLocalError
// => note: if variable is not defined, nameerror. Else, UnboundLocalError.
// What about [...] expressions? do they leak?
// no: I.e.,
// def f(x):
//    z = [w for w in range(2)]
//    return w
// gives nameerror!

// test program IV: Leakage from for loop
// ----------------------
// def f(x):
//    for w in range(2):
//        pass
//    return w
// ----------------------
// output:
// f(0) => 1


// check special symbol _ ???
// => should support that as well!




// other cool augassign tests:
//  def testInDict(self):
//        x = {0: 2}
//        x[0] += 1
//        x[0] *= 2
//        x[0] **= 2
//        x[0] -= 8
//        x[0] //= 5
//        x[0] %= 3
//        x[0] &= 2
//        x[0] |= 5
//        x[0] ^= 1
//        x[0] /= 2
//        self.assertEqual(x[0], 3.0)
//
//    def testSequences(self):
//        x = [1,2]
//        x += [3,4]
//        x *= 2
//
//        self.assertEqual(x, [1, 2, 3, 4, 1, 2, 3, 4])
//
//        x = [1, 2, 3]
//        y = x
//        x[1:2] *= 2
//        y[1:2] += [1]
//
//        self.assertEqual(x, [1, 2, 1, 2, 3])
//        self.assertTrue(x is y)