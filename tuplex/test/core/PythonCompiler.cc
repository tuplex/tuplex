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


// this an example file on how to run the python udf compiler
// documentation on the google testing framework is available under https://github.com/google/googletest/blob/master/googletest/docs/Primer.md

TEST(Compiler, AddOperation) {
    using namespace tuplex;

    Row res;

    // template:


    // type propagation working?
    // 3 x 3 tests

    // + with bool constantx
    res = execRow(Row({Field(true)}), UDF("lambda x: x + True"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::I64);
    EXPECT_EQ(res.getInt(0), 2);

    // + with i64 constant
    res = execRow(Row({Field(true)}), UDF("lambda x: 22 + x"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::I64);
    EXPECT_EQ(res.getInt(0), 23);

    // + with f64 constant
    res = execRow(Row({Field(true)}), UDF("lambda x: x + 10.9"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::F64);
    EXPECT_EQ(res.getDouble(0), 11.9);


    // + with bool constant
    res = execRow(Row({Field((int64_t)20)}), UDF("lambda x: x + True"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::I64);
    EXPECT_EQ(res.getInt(0), 21);

    // + with i64 constant
    res = execRow(Row({Field((int64_t)20)}), UDF("lambda x: 22 + x"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::I64);
    EXPECT_EQ(res.getInt(0), 42);

    // + with f64 constant
    res = execRow(Row({Field((int64_t)20)}), UDF("lambda x: x + 10.9"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::F64);
    EXPECT_EQ(res.getDouble(0), 30.9);


    // + with bool constant
    res = execRow(Row({Field(3.141)}), UDF("lambda x: x + True"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::F64);
    EXPECT_EQ(res.getDouble(0), 4.141);

    // + with i64 constant
    res = execRow(Row({Field(3.141)}), UDF("lambda x: 22 + x"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::F64);
    EXPECT_EQ(res.getDouble(0), 25.141);

    // + with f64 constant
    res = execRow(Row({Field(3.141)}), UDF("lambda x: x + 10.9"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::F64);
    EXPECT_EQ(res.getDouble(0), 14.041);
}

TEST(Compiler, AddOperationAutoUnpacking) {

    using namespace tuplex;

    Row res;

    // adding two arguments
    res = execRow(Row({Field((int64_t)20), Field(10.67)}), UDF("lambda x, y: x + y"));
    // check correct return type & result
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::F64);
    EXPECT_EQ(res.getDouble(0), 30.67);
}

TEST(Compiler, NullIfConstruction) {
    using namespace tuplex;

    // Note: ... if ... else ... also uses short circuit evaluation! I.e. can't be realized via switch statement.

    auto res = execRow(Row({Field("127")}), UDF("lambda x: int(x) if len(x) > 0 else None"));
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_TRUE(res.get(0).getType() == python::Type::makeOptionType(python::Type::I64));
    EXPECT_FALSE(res.get(0).isNull());
    EXPECT_EQ(res.getInt(0), 127);
}

TEST(Compiler, BitwiseOperators) {
    using namespace tuplex;

    auto res = execRow(Row(true, false), UDF("lambda a, b: a & b"));
    EXPECT_EQ(res.getNumColumns(), 1);
    EXPECT_EQ(res.getBoolean(0), false);
}


// @TODO
//TEST(Compiler, LenFunction) {
//    using namespace tuplex;
//
//    Row res;
//
//    // need better error handling for this...
//    execRow(Row(42), UDF("lambda x: bool()"));
//
//    // adding two arguments
//    res = execRow(Row(Tuple(1, "hello world")), UDF("lambda x: (len(x), len(x[1]))"));
//    // check correct return type & result
//    ASSERT_EQ(res.getNumColumns(), 2);
//    EXPECT_TRUE(res.get(0).getType() == python::Type::I64);
//    EXPECT_TRUE(res.get(1).getType() == python::Type::I64);
//
//    EXPECT_EQ(res.getInt(0), 2);
//    EXPECT_EQ(res.getInt(1), strlen("hello world"));
//
//    res = execRow(Row(""), UDF("lambda x: len(x)"));
//    ASSERT_EQ(res.getNumColumns(), 1);
//    EXPECT_TRUE(res.get(0).getType() == python::Type::I64);
//    EXPECT_EQ(res.getInt(0), 0);
//}

TEST(Compiler, AddOperationOption) {
    using namespace tuplex;

    Row res;

    // template:


    // type propagation working?
    // 3 x 3 tests

    // + with bool constant
    res = execRow(Row({Field(option<bool>(true))}), UDF("lambda x: x + True"));
}