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

class AssertAndRaiseTest : public PyTest {
};

using namespace tuplex;
using namespace std;

// Note: only this test here fails...
TEST_F(AssertAndRaiseTest, Assert) {
    Context c(microTestOptions());

    auto code = "def f(x):\n"
                "\tassert x % 2 == 1, 'only odd numbers'\n"
                "\treturn x * x\n";

    auto& ds = c.parallelize({1, 2, 3, 4, 5}).map(UDF(code));

    auto v0 = ds.collectAsVector();
    ASSERT_EQ(v0.size(), 3);
    EXPECT_EQ(v0[0].getInt(0), 1);
    EXPECT_EQ(v0[1].getInt(0), 9);
    EXPECT_EQ(v0[2].getInt(0), 25);

    // with resolver
    auto v1 = ds.resolve(ExceptionCode::ASSERTIONERROR, UDF("lambda x: (x-1) * (x-1)")).collectAsVector();
    ASSERT_EQ(v1.size(), 5);
    EXPECT_EQ(v1[0].getInt(0), 1);
    EXPECT_EQ(v1[1].getInt(0), 1);
    EXPECT_EQ(v1[2].getInt(0), 9);
    EXPECT_EQ(v1[3].getInt(0), 9);
    EXPECT_EQ(v1[4].getInt(0), 25);
}

TEST_F(AssertAndRaiseTest, Raise) {
    Context c(microTestOptions());

    auto code = "def f(x):\n"
                "\tif x % 2 == 1:\n"
                "\t\traise FileNotFoundError\n"
                "\treturn x * x\n";

    auto& ds = c.parallelize({1, 2, 3, 4, 5}).map(UDF(code));

    auto v0 = ds.collectAsVector();
    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].getInt(0), 4);
    EXPECT_EQ(v0[1].getInt(0), 16);

    // with resolver
    // ==> use a more general class to resolve things!

    // --> here a more general error is used to resolve! I.e. check hierarchy!
    auto v1 = ds.resolve(ExceptionCode::OSERROR, UDF("lambda x: 0")).collectAsVector();

    ASSERT_EQ(v1.size(), 5);
    EXPECT_EQ(v1[0].getInt(0), 0);
    EXPECT_EQ(v1[1].getInt(0), 4);
    EXPECT_EQ(v1[2].getInt(0), 0);
    EXPECT_EQ(v1[3].getInt(0), 16);
    EXPECT_EQ(v1[4].getInt(0), 0);
}

// ignore test for hierarchy?
TEST_F(AssertAndRaiseTest, Ignore) {
    Context c(microTestOptions());

    auto code = "def f(x):\n"
                "\tif x % 2 == 0:\n"
                "\t\traise FileNotFoundError\n"
                "\tif x % 3 == 0:\n"
                "\t\traise LookupError\n"
                "\tif x % 5 == 0:\n"
                "\t\traise IndexError\n"
                "\treturn x * x\n";

    auto& ds = c.parallelize({1, 2, 3, 4, 5}).map(UDF(code));

    EXPECT_EQ(ds.collectAsVector().size(), 1); // only one element doesn't cause an exception
    EXPECT_EQ(c.metrics().totalExceptionCount, 4);

    EXPECT_EQ(ds.ignore(ExceptionCode::EXCEPTION).collectAsVector().size(), 1);
}