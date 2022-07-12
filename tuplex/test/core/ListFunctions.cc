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
#include <Context.h>
#include "../../utils/include/Utils.h"
#include "TestUtils.h"
#include "jit/RuntimeInterface.h"

// need for these tests a running python interpreter, so spin it up
class ListFunctions : public PyTest {};

TEST_F(ListFunctions, ListSubscript) {
    using namespace tuplex;
    Context c(microTestOptions());

    // nonempty cases
    auto v0 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [1, 2, 3][x]")).collectAsVector();

    EXPECT_EQ(v0.size(), 3);
    ASSERT_EQ(v0[0].getInt(0), 1);
    ASSERT_EQ(v0[1].getInt(0), 2);
    ASSERT_EQ(v0[2].getInt(0), 3);

    auto v1 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [1.1, 2.2, 3.3][x]")).collectAsVector();

    EXPECT_EQ(v1.size(), 3);
    ASSERT_EQ(v1[0].getDouble(0), 1.1);
    ASSERT_EQ(v1[1].getDouble(0), 2.2);
    ASSERT_EQ(v1[2].getDouble(0), 3.3);

    auto v2 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [True, False, True][x]")).collectAsVector();

    EXPECT_EQ(v2.size(), 3);
    ASSERT_EQ(v2[0].getBoolean(0), true);
    ASSERT_EQ(v2[1].getBoolean(0), false);
    ASSERT_EQ(v2[2].getBoolean(0), true);

    auto v3 = c.parallelize({
            Row(0), Row(1), Row(2), Row(3)
    }).map(UDF("lambda x: ['abcd', 'b', '', 'efghi'][x]")).collectAsVector();

    EXPECT_EQ(v3.size(), 4);
    ASSERT_EQ(v3[0].toPythonString(), "('abcd',)");
    ASSERT_EQ(v3[1].toPythonString(), "('b',)");
    ASSERT_EQ(v3[2].toPythonString(), "('',)");
    ASSERT_EQ(v3[3].toPythonString(), "('efghi',)");

    // empty cases
    auto v4 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [None, None, None][x]")).collectAsVector();

    EXPECT_EQ(v4.size(), 3);
    ASSERT_EQ(v4[0].toPythonString(), "(None,)");
    ASSERT_EQ(v4[1].toPythonString(), "(None,)");
    ASSERT_EQ(v4[2].toPythonString(), "(None,)");

    auto v5 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [(), (), ()][x]")).collectAsVector();

    EXPECT_EQ(v5.size(), 3);
    ASSERT_EQ(v5[0].toPythonString(), "((),)");
    ASSERT_EQ(v5[1].toPythonString(), "((),)");
    ASSERT_EQ(v5[2].toPythonString(), "((),)");

    auto v6 = c.parallelize({
            Row(0), Row(1), Row(2)
    }).map(UDF("lambda x: [{}, {}, {}][x]")).collectAsVector();

    EXPECT_EQ(v6.size(), 3);
    ASSERT_EQ(v6[0].toPythonString(), "({},)");
    ASSERT_EQ(v6[1].toPythonString(), "({},)");
    ASSERT_EQ(v6[2].toPythonString(), "({},)");

    // index error test
    auto v7 = c.parallelize({
            Row(0), Row(3), Row(4)
    }).map(UDF("lambda x: [1.1, 2.2, 3.3][x]")).resolve(ExceptionCode::INDEXERROR,
                                                        UDF("lambda x: -1.0")).collectAsVector();

    EXPECT_EQ(v7.size(), 3);
    ASSERT_EQ(v7[0].getDouble(0), 1.1);
    ASSERT_EQ(v7[1].getDouble(0), -1.0);
    ASSERT_EQ(v7[2].getDouble(0), -1.0);
}

TEST_F(ListFunctions, ListReturn) {
    using namespace tuplex;
    Context c(microTestOptions());

    // Test list return in tuple
    auto ds = c.parallelize({Row("D1"), Row("D2")})
            .map(UDF("lambda x: ([x, 'abc', x + 'def'],)"));

    auto v1 = ds.collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "((['D1','abc','D1def'],),)");
    EXPECT_EQ(v1[1].toPythonString(), "((['D2','abc','D2def'],),)");

    auto v2 = ds.map(UDF("lambda y: y[0][2]")).collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "('D1def',)");
    EXPECT_EQ(v2[1].toPythonString(), "('D2def',)");

    // directly return list
    auto v3 = c.parallelize({Row("D1"), Row("D2")})
            .map(UDF("lambda x: [x, 'abc', x + 'def']"))
            .collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "(['D1','abc','D1def'],)");
    EXPECT_EQ(v3[1].toPythonString(), "(['D2','abc','D2def'],)");

    // return wrapped list literal
    auto v4 = c.parallelize({Row(1), Row(2), Row(3)}).map(UDF("lambda x: ([1, 2, 3],)")).collectAsVector();
    ASSERT_EQ(v4.size(), 3);
    EXPECT_EQ(v4[0].toPythonString(), "(([1,2,3],),)");
    EXPECT_EQ(v4[1].toPythonString(), "(([1,2,3],),)");
    EXPECT_EQ(v4[2].toPythonString(), "(([1,2,3],),)");

    // empty lists
    auto v5 = c.parallelize({Row(1), Row(2), Row(3)}).map(UDF("lambda x: []")).collectAsVector();
    ASSERT_EQ(v5.size(), 3);
    EXPECT_EQ(v5[0].toPythonString(), "([],)");
    EXPECT_EQ(v5[1].toPythonString(), "([],)");
    EXPECT_EQ(v5[2].toPythonString(), "([],)");
}

TEST_F(ListFunctions, ListReturnII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto code1 = "def a(x):\n"
                 "    if(x > 2):\n"
                 "        return [1, 2, 3]\n"
                 "    else:\n"
                 "        return None";

    auto v1 = c.parallelize({Row(0), Row(1), Row(4)}).map(UDF(code1)).collectAsVector();
    ASSERT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0].toPythonString(), "(None,)");
    EXPECT_EQ(v1[1].toPythonString(), "(None,)");
    EXPECT_EQ(v1[2].toPythonString(), "([1,2,3],)");
}

TEST_F(ListFunctions, RegressionTests) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row(Field::null(), Field::null(), Field::null()), Row(Field::null(), Field::null(), Field::null())}).map(UDF("lambda x, y, z: [x, y, z]")).collectAsVector();
    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].toPythonString(), "([None,None,None],)");
    EXPECT_EQ(v0[1].toPythonString(), "([None,None,None],)");

    auto v1 = c.parallelize({Row(Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple()), Row(Field::empty_tuple(), Field::empty_tuple(), Field::empty_tuple())}).map(UDF("lambda x, y, z: [x, y, z]")).collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "([(),(),()],)");
    EXPECT_EQ(v1[1].toPythonString(), "([(),(),()],)");
}

TEST_F(ListFunctions, ListComprehension) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({Row(0), Row(5), Row(6)}).map(UDF("lambda x: [t for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0].toPythonString(), "([],)");
    EXPECT_EQ(v1[1].toPythonString(), "([0,1,2,3,4],)");
    EXPECT_EQ(v1[2].toPythonString(), "([0,1,2,3,4,5],)");

    auto v2 = c.parallelize({Row(0), Row(5), Row(6)}).map(UDF("lambda x: [10*t for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0].toPythonString(), "([],)");
    EXPECT_EQ(v2[1].toPythonString(), "([0,10,20,30,40],)");
    EXPECT_EQ(v2[2].toPythonString(), "([0,10,20,30,40,50],)");

    auto v3 = c.parallelize({Row(0), Row(5), Row(6)}).map(UDF("lambda x: [t*'a' for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v3.size(), 3);
    EXPECT_EQ(v3[0].toPythonString(), "([],)");
    EXPECT_EQ(v3[1].toPythonString(), "(['','a','aa','aaa','aaaa'],)");
    EXPECT_EQ(v3[2].toPythonString(), "(['','a','aa','aaa','aaaa','aaaaa'],)");

    auto v4 = c.parallelize({Row(0), Row(1), Row(2)}).map(UDF("lambda x: [None for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v4.size(), 3);
    EXPECT_EQ(v4[0].toPythonString(), "([],)");
    EXPECT_EQ(v4[1].toPythonString(), "([None],)");
    EXPECT_EQ(v4[2].toPythonString(), "([None,None],)");

    auto v5 = c.parallelize({Row(0), Row(3), Row(4)}).map(UDF("lambda x: [() for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v5.size(), 3);
    EXPECT_EQ(v5[0].toPythonString(), "([],)");
    EXPECT_EQ(v5[1].toPythonString(), "([(),(),()],)");
    EXPECT_EQ(v5[2].toPythonString(), "([(),(),(),()],)");

    auto v6 = c.parallelize({Row(0), Row(5), Row(6)}).map(UDF("lambda x: [{} for t in range(x)]")).collectAsVector();
    ASSERT_EQ(v6.size(), 3);
    EXPECT_EQ(v6[0].toPythonString(), "([],)");
    EXPECT_EQ(v6[1].toPythonString(), "([{},{},{},{},{}],)");
    EXPECT_EQ(v6[2].toPythonString(), "([{},{},{},{},{},{}],)");
}

TEST_F(ListFunctions, ListComprehensionII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v1 = c.parallelize({Row(0, 5, 2), Row(5, 0, -2), Row(4, -4, -3)}).map(UDF("lambda x, y, z: [t for t in range(x, y, z)]")).collectAsVector();
    ASSERT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0].toPythonString(), "([0,2,4],)");
    EXPECT_EQ(v1[1].toPythonString(), "([5,3,1],)");
    EXPECT_EQ(v1[2].toPythonString(), "([4,1,-2],)");

    auto code2 = "def a(x):\n"
                 "    y = range(x)\n"
                 "    return [t for t in y]";

    auto v2 = c.parallelize({Row(0), Row(1), Row(4)}).map(UDF(code2)).collectAsVector();
    ASSERT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0].toPythonString(), "([],)");
    EXPECT_EQ(v2[1].toPythonString(), "([0],)");
    EXPECT_EQ(v2[2].toPythonString(), "([0,1,2,3],)");
}

TEST_F(ListFunctions, ListComprehensionIII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row("abcde"), Row("12345"), Row("")}).map(UDF("lambda x: [t for t in x]")).collectAsVector();
    ASSERT_EQ(v0.size(), 3);
    EXPECT_EQ(v0[0].toPythonString(), "(['a','b','c','d','e'],)");
    EXPECT_EQ(v0[1].toPythonString(), "(['1','2','3','4','5'],)");
    EXPECT_EQ(v0[2].toPythonString(), "([],)");

    auto v1 = c.parallelize({Row("abcde"), Row("12345")}).map(UDF("lambda x: [1 for t in x]")).collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "([1,1,1,1,1],)");
    EXPECT_EQ(v1[1].toPythonString(), "([1,1,1,1,1],)");

    auto v2 = c.parallelize({Row(List(1, 2, 3, 4, 5)), Row(List(1))}).map(UDF("lambda x: [t*t for t in x]")).collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "([1,4,9,16,25],)");
    EXPECT_EQ(v2[1].toPythonString(), "([1],)");

    auto v3 = c.parallelize({Row(List(Field::null(), Field::null(), Field::null())), Row(List(Field::null()))}).map(UDF("lambda x: [t for t in x]")).collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "([None,None,None],)");
    EXPECT_EQ(v3[1].toPythonString(), "([None],)");

    auto v4 = c.parallelize({Row(List("hello", "world", "!")), Row(List("goodbye"))}).map(UDF("lambda x: [t[1:] for t in x]")).collectAsVector();
    ASSERT_EQ(v4.size(), 2);
    EXPECT_EQ(v4[0].toPythonString(), "(['ello','orld',''],)");
    EXPECT_EQ(v4[1].toPythonString(), "(['oodbye'],)");

    auto v5 = c.parallelize({Row(Tuple("hello", "world", "!")), Row(Tuple("goodbye", "test", "!"))}).map(UDF("lambda x: [t[1:] for t in x]")).collectAsVector();
    ASSERT_EQ(v5.size(), 2);
    EXPECT_EQ(v5[0].toPythonString(), "(['ello','orld',''],)");
    EXPECT_EQ(v5[1].toPythonString(), "(['oodbye','est',''],)");
}

TEST_F(ListFunctions, ListIn) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row("abcde"), Row("12345"), Row("")}).filter(UDF("lambda x: x in ['abcde', '']")).collectAsVector();
    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].toPythonString(), "('abcde',)");
    EXPECT_EQ(v0[1].toPythonString(), "('',)");

    auto v1 = c.parallelize({Row(Field::null()), Row(Field::null())}).filter(UDF("lambda x: x in [None]")).collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "(None,)");
    EXPECT_EQ(v1[1].toPythonString(), "(None,)");

    auto v2 = c.parallelize({Row(Field::empty_dict()), Row(Field::empty_dict()), Row(Field::empty_dict())}).filter(UDF("lambda x: x in [{}]")).collectAsVector();
    ASSERT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0].toPythonString(), "({},)");
    EXPECT_EQ(v2[1].toPythonString(), "({},)");
    EXPECT_EQ(v2[2].toPythonString(), "({},)");
}