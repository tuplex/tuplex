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
#include <Row.h>
#include <Field.h>
#include <UDF.h>
#include <Python.h>

using namespace tuplex;

TEST(Row, PythonStringify) {
    Row row(10.5, true);

    EXPECT_EQ(row.toPythonString(), std::string("(10.50000,True)"));
}

TEST(Row, ElementgettersTuple) {
    //Row row({Field(21.0), Field("hello"), Field(Tuple({Field(10.5)})), Field((int64_t)42)});
    Row row(21.0, "hello", Tuple(10.5), 42);

    EXPECT_EQ(row.getDouble(0), 21.0);
    EXPECT_EQ(row.getString(1), std::string("hello"));
    EXPECT_EQ(row.getTuple(2).desc(), std::string("(10.50000,)"));
    EXPECT_EQ(row.getInt(3), 42);
}

TEST(Row, ColumnStrings) {
    //Row row({Field(21.5), Field("hello"), Field(Tuple({Field(10.5)})), Field((int64_t)42)});
    Row row(21.5, "hello", Tuple(10.5), 42);

    std::vector<std::string> ref{"21.50000", "'hello'", "(10.50000,)", "42"};
    auto res = row.getAsStrings();

    EXPECT_EQ(res.size(), ref.size());

    for(int i = 0; i < ref.size(); i++)
        EXPECT_EQ(res[i], ref[i]);
}


// tests for variadic constructor
TEST(Row, VariadicInts) {
    Row row(10, 20, 30, 40);

    auto res = row.toPythonString();
    EXPECT_EQ(res, "(10,20,30,40)");
}

TEST(Row, VariadicFloats) {
    Row row(10.0f, 20.0, 3.1);
    EXPECT_EQ(row.getRowType(), python::Type::makeTupleType({python::Type::F64, python::Type::F64, python::Type::F64}));
    EXPECT_EQ(row.toPythonString(), "(10.00000,20.00000,3.10000)");
}

TEST(Row, VariadicBools) {
    Row row(true, false);
    EXPECT_EQ(row.toPythonString(), "(True,False)");
}

TEST(Row, VariadicTuples) {
    Row row(Tuple(10, 20, 30), 20);
    EXPECT_EQ(row.toPythonString(), "((10,20,30),20)");
}

TEST(Row, VariadicMixedI) {
    Row row(Tuple(), Tuple(2, true, 3.1), 8, "hello world");
    EXPECT_EQ(row.toPythonString(), "((),(2,True,3.10000),8,'hello world')");
}

TEST(Row, VariadicMixedII) {
    Row row(Tuple(), true, 3.1, 8, "hello world");
    EXPECT_EQ(row.toPythonString(), "((),True,3.10000,8,'hello world')");
}

TEST(Row, NestedTuples) {
    EXPECT_EQ(Row(Tuple()).toPythonString(), "((),)");
    EXPECT_EQ(Row(Tuple(42)).toPythonString(), "((42,),)");
    // note that here Field must be called to prevent copy constructor!
    EXPECT_EQ(Row(Tuple(Field(Tuple(42)))).toPythonString(), "(((42,),),)");
    EXPECT_EQ(Row(Tuple(Field(Tuple(Field(Tuple(3)))))).toPythonString(), "((((3,),),),)");
}

TEST(Row, Nullables) {

    // @TODO: test options for all sorts of types! I.e. at least all primitives should work...

    // step 1: fields
    // Tuple types (that is more difficult in pure C++)
    EXPECT_EQ(Field(option<Tuple>::none).getType(), python::Type::makeOptionType(python::Type::EMPTYTUPLE));

    // to express NULL value typed for a tuple type, need to use std::tuple template class
    // tuple with certain types
    EXPECT_EQ(Field(option<std::tuple<>>::none).getType(), python::Type::makeOptionType(python::Type::EMPTYTUPLE));

    EXPECT_EQ(Field(option<std::tuple<int64_t, double>>::none).getType(), python::Type::makeOptionType(python::Type::makeTupleType({python::Type::I64, python::Type::F64})));


    // step 2: Rows
    auto r1 = Row(option<int64_t>::none);
    EXPECT_EQ(r1.toPythonString(), "(None,)");

    EXPECT_EQ(Row(option<int64_t>(42)).toPythonString(), "(42,)");

    // not working yet
    // // special case: empty tuple!
    // auto r2 = Row(option<Tuple>::none);
    // EXPECT_EQ(r2.toPythonString(), "(None,)");
    Row r2;

    // step 3: serialization
    char buffer[4096];
    memset(buffer, 0xFF, 4096);

    Serializer s;
    s.append(option<int64_t>::none);
    s.serialize(buffer, 4096);

    core::hexdump(std::cout, buffer, 128);

    // deserialization
    auto rowType = python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::I64));
    r1 = Row::fromMemory(Schema(Schema::MemoryLayout::ROW,rowType), buffer, 4096);
    EXPECT_EQ(r1.toPythonString(), "(None,)");
    EXPECT_EQ(r1.getRowType(), rowType);

    s.reset();

    s.append(option<int64_t>(654321));
    s.serialize(buffer, 4096);

    core::hexdump(std::cout, buffer, 128);

    r2 = Row::fromMemory(Schema(Schema::MemoryLayout::ROW,rowType), buffer, 4096);
    EXPECT_EQ(r2.toPythonString(), "(654321,)");
    EXPECT_EQ(r2.getRowType(), rowType);
}

TEST(Row, NullValue) {
    Row r1(Field::null());
    EXPECT_EQ(r1.toPythonString(), "(None,)");

    Row r2(12, 13, 14.56, Field::null());
    EXPECT_EQ(r2.toPythonString(), "(12,13,14.56000,None)");

    Row r3(12, Tuple(Field::null()), Field::null(), Tuple(1, Field::null()), Tuple(Field::null(), 2));
    EXPECT_EQ(r3.toPythonString(), "(12,(None,),None,(1,None),(None,2))");
}

TEST(Row, StructTypeToPythonString) {
    auto stype_sub = python::Type::makeStructuredDictType({std::make_pair("a", python::Type::I64),
                                                           std::make_pair("b", python::Type::I64),
                                                           std::make_pair("c", python::Type::NULLVALUE)});
    auto stype = python::Type::makeStructuredDictType({std::make_pair("column1", stype_sub)});
    Row r({Field::from_str_data("{\"column1\": {\"a\": 10, \"b\": 20, \"c\": null}}", stype)});

    std::cout<<r.toPythonString()<<std::endl;
}