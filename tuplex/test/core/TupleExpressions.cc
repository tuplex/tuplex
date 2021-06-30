//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "TestUtils.h"
#include <Context.h>

class TupleExprTest : public PyTest {};

TEST_F(TupleExprTest, SimpleExpr) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto code = "def f(a, b):\n"
                "\treturn b, a";

    auto res = c.parallelize({Row(10, 20), Row(30, 40)}).map(UDF(code)).collectAsVector();

    for(auto r : res)
        std::cout<<r.toPythonString()<<std::endl;

    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0], Row(20, 10));
    EXPECT_EQ(res[1], Row(40, 30));
}

TEST_F(TupleExprTest, StringAssign) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto code = "def f(a):\n"
                "\tx, y = a\n"
                "\treturn y, x";

    auto res = c.parallelize({Row("hi"), Row("ab")}).map(UDF(code)).collectAsVector();

    for(auto r : res)
        std::cout<<r.toPythonString()<<std::endl;

    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0], Row("i", "h"));
    EXPECT_EQ(res[1], Row("b", "a"));
}

// TODO: add here MORE tuple expression tests...
TEST_F(TupleExprTest, NestedTuple) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto code = "def f(a):\n"
                "\tx, y = a, 3\n"
                "\treturn x, y";

    auto res = c.parallelize({Row(1, 2)}).map(UDF(code)).collectAsVector();

    for(auto r : res)
        std::cout<<r.toPythonString()<<std::endl;

    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], Row(Tuple(1, 2), 3));
//    EXPECT_EQ(res[1], Row("abcde", 3));
}

TEST_F(TupleExprTest, SimpleSwap) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto code = "def f(a):\n"
                "\tx, y, z = a\n"
                "\tx, y, z = y, z, x\n"
                "\treturn x, y, z";

    auto res = c.parallelize({Row(1, 2, 3)}).map(UDF(code)).collectAsVector();

    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], Row(2, 3, 1));

    res = c.parallelize({Row("abc")}).map(UDF(code)).collectAsVector();
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], Row("b", "c", "a"));
}

TEST_F(TupleExprTest, InvalidStringUnpack) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto code = "def f(a):\n"
                "\tx, y, z = a\n"
                "\tx, y, z = y, z, x\n"
                "\treturn x, y, z";

    auto resolver = "lambda x: (\"0\", \"0\", \"0\")";

    auto res = c.parallelize({Row("ab"), Row("eee"), Row(""), Row("lmno")})
            .map(UDF(code))
            .resolve(ExceptionCode::VALUEERROR, UDF(resolver))
            .collectAsVector();


    ASSERT_EQ(res.size(), 4);
    EXPECT_EQ(res[0], Row("0", "0", "0"));
    EXPECT_EQ(res[1], Row("e", "e", "e"));
    EXPECT_EQ(res[2], Row("0", "0", "0"));
    EXPECT_EQ(res[3], Row("0", "0", "0"));
}

TEST_F(TupleExprTest, Swap) {
    using namespace tuplex;

    Context c(microTestOptions());

//    auto codeI = "def f(a, b):\n"
//                "\ta, b = b, a\n"
//                "\treturn a, b";
//    auto resI = c.parallelize({Row("a", 1), Row("b", 2)}).map(UDF(codeI)).collectAsVector();
//    ASSERT_EQ(resI.size(), 2);
//    EXPECT_EQ(resI[0].toPythonString(), "(1,'a')");
//    EXPECT_EQ(resI[1].toPythonString(), "(2,'b')");

//    auto codeII = "def f(a, b):\n"
//                 "\tb, a = a, b\n"
//                 "\treturn a, b";
//    auto resII = c.parallelize({Row("a", 1), Row("b", 2)}).map(UDF(codeII)).collectAsVector();
//    ASSERT_EQ(resII.size(), 2);
//    EXPECT_EQ(resII[0].toPythonString(), "(1,'a')");
//    EXPECT_EQ(resII[1].toPythonString(), "(2,'b')");

    // this doesn't work, but the code above does. Weird??
    // ==> prob no true store to variables!
    // ----> also need to change how code is generated for variables, i.e. they're name tags
    // not true vars like in C!

    auto codeIII = "def f(x):\n"
                   "\ta = x[0]\n"
                   "\tb = x[1]\n"
                   "\tb, a = a, b\n"
                   "\treturn a, b";
    auto resIII = c.parallelize({Row("a", 1), Row("b", 2)}).map(UDF(codeIII)).collectAsVector();
    ASSERT_EQ(resIII.size(), 2);
    EXPECT_EQ(resIII[0].toPythonString(), "(1,'a')");
    EXPECT_EQ(resIII[1].toPythonString(), "(2,'b')");
}

// TODO test compile time checks for invalid tuple unpacking assignments (just run from main and see)