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
#include <Serializer.h>
#include <Row.h>

using namespace tuplex;

TEST(Serializer, InvarianceI) {
    Serializer s;
    uint8_t *buffer = (uint8_t*)malloc(2048);

    int len = s.append(32L).append(42L).append("Hello world!").append(3.14159).serialize(buffer, 2048);

    Schema schema = s.getSchema();
    Deserializer d(schema);
    d.deserialize(buffer, 2048);
    free(buffer);

    EXPECT_EQ(d.getInt(0), 32L);
    EXPECT_EQ(d.getInt(1), 42L);
    EXPECT_EQ(d.getString(2), "Hello world!");
    EXPECT_EQ(d.getDouble(3), 3.14159);
}

TEST(Serializer, EmptyString) {
    Serializer s;
    uint8_t *buffer = (uint8_t*)malloc(2048);

    int len = s.append("").serialize(buffer, 2048);

    Schema schema = s.getSchema();
    Deserializer d(schema);
    d.deserialize(buffer, 2048);
    free(buffer);

    EXPECT_EQ(d.getString(0), "");
}

TEST(Serializer, TupleSupport) {
    Serializer s;
    uint8_t *buffer = (uint8_t*)malloc(2048);

    auto len = s.append(32L)
            .append(42L)
//            .append(Tuple({Field("hello"), Field(20.0)}))
            .append(Tuple("hello", 20.0))
            .append("Hello world!")
            .serialize(buffer, 2048);

    Schema schema = s.getSchema();
    auto et = python::Type::makeTupleType({python::Type::I64, python::Type::I64,
                                           python::Type::makeTupleType(std::vector<python::Type>{python::Type::STRING, python::Type::F64}),
                                           python::Type::STRING});
    EXPECT_TRUE(schema.getRowType() == et);

    Deserializer d(schema);
    d.deserialize(buffer, 2048);
    free(buffer);

    auto tup = d.getTuple();
    EXPECT_EQ(tup.numElements(), 4);
    EXPECT_EQ(tup.getField(0).getInt(), 32L);
    EXPECT_EQ(tup.getField(1).getInt(), 42L);
    auto ntup = *((Tuple*)tup.getField(2).getPtr());
    EXPECT_EQ(ntup.numElements(), 2);
    // @Todo: string comparison
    EXPECT_EQ(ntup.getField(1).getDouble(), 20.0);
    // @Todo: string comparison #2
#warning "incomplete test here..."
}

TEST(Serializer, TupleWithOptions) {
    Row row({Field("test"), Field(option<int64_t>(42)), Field(option<std::string>::none)});

    uint8_t buffer[4096];
    memset(buffer, 0, 4096);

    row.serializeToMemory(buffer, 4096);


    std::cout<<"\n\n"<<Row::fromMemory(row.getSchema(), buffer, 4096).toPythonString()<<std::endl;

    // check bitmap (2 entries, should be 0x02)
    EXPECT_EQ(*((int64_t*)buffer), 0x02);

    // the integer value (0-1 for bitmap, 1-2 for test, 2-3 is where data is stored
    EXPECT_EQ(*( ((int64_t*)buffer) + 2), 42);

    core::hexdump(std::cout, buffer, 64);
}

TEST(Serializer, NullValues) {
    Row row(option<std::string>::none);
    uint8_t buffer[4096];
    memset(buffer, 0, 4096);

    row.serializeToMemory(buffer, 4096);

    EXPECT_EQ(Row::fromMemory(row.getSchema(), buffer, 4096).toPythonString(), "(None,)");
}

TEST(Serializer, OptionFields) {
    using namespace tuplex;
    uint8_t buf[1024];

    // fixed field as option which is none
    auto intRow = Row(option<int>::none);
    EXPECT_EQ(intRow.serializedLength(), intRow.serializeToMemory(buf, 1024));
    // varlen field as option which is None
    auto strRow = Row(option<std::string>::none);
    EXPECT_EQ(strRow.serializedLength(), strRow.serializeToMemory(buf, 1024));
}

TEST(Serializer,OptI64Format) {
    using namespace tuplex;

    // testing an assumption when converting unique rows...
    int64_t N = 100;

    char buf[4096];
    for(int64_t i = -N; i < N; ++i) {
        Field f(i);
        f.makeOptional();
        Row r(f);
        ASSERT_EQ(r.serializedLength(), 16);
        r.serializeToMemory(reinterpret_cast<uint8_t *>(buf), 4096);
        // decode
        EXPECT_EQ(*(int64_t*)buf, 0);
        EXPECT_EQ(*(((int64_t*)buf) + 1), i);
    }
}

TEST(Serializer, ListOfTuples) {
    Serializer s;
    auto *buffer = (uint8_t*)malloc(2048);

    // ("abcd", [(1234, "hello,", 5.6789, "TUPLEX"), (98, "world!!", 76543.21, "??&&")], True, 10000000)
    auto len = s.append("abcd")
            .append(List(Tuple(1234, "hello,", 5.6789, "TUPLEX"), Tuple(98, "world!!", 76543.21, "??&&")))
            .append(true)
            .append(10000000)
            .serialize(buffer, 2048);

    Schema schema = s.getSchema();
    auto et = python::Type::makeTupleType({python::Type::STRING,
                                           python::Type::makeListType(python::Type::makeTupleType({python::Type::I64, python::Type::STRING, python::Type::F64, python::Type::STRING})),
                                           python::Type::BOOLEAN,
                                           python::Type::I64});
    EXPECT_TRUE(schema.getRowType() == et);

    {
        // check length
        Deserializer d(schema);
        auto inferred_len = d.inferLength(buffer);
        EXPECT_EQ(len, inferred_len);
    }

    Deserializer d(schema);
    d.deserialize(buffer, 2048);
    free(buffer);

    auto row = d.getTuple();
    EXPECT_EQ(row.numElements(), 4);
    EXPECT_EQ(std::string((char *)row.getField(0).getPtr()), "abcd");
    auto lst = *(List *)row.getField(1).getPtr();
    EXPECT_EQ(lst.numElements(), 2);
    auto tup1 = *(Tuple *)(lst.getField(0).getPtr());
    auto tup2 = *(Tuple *)(lst.getField(1).getPtr());
    EXPECT_EQ(tup1.numElements(), 4);
    EXPECT_EQ(tup2.numElements(), 4);
    EXPECT_EQ(tup1.getField(0).getInt(), 1234);
    EXPECT_EQ(std::string((char *)tup1.getField(1).getPtr()), "hello,");
    EXPECT_FLOAT_EQ(tup1.getField(2).getDouble(), 5.6789);
    EXPECT_EQ(std::string((char *)tup1.getField(3).getPtr()), "TUPLEX");
    EXPECT_EQ(tup2.getField(0).getInt(), 98);
    EXPECT_EQ(std::string((char *)tup2.getField(1).getPtr()), "world!!");
    EXPECT_FLOAT_EQ(tup2.getField(2).getDouble(), 76543.21);
    EXPECT_EQ(std::string((char *)tup2.getField(3).getPtr()), "??&&");
    EXPECT_EQ(row.getField(2).getInt(), 1);
    EXPECT_EQ(row.getField(3).getInt(), 10000000);
}

TEST(Serializer, ListOfLists) {
    Serializer s;
    auto *buffer = (uint8_t*)malloc(2048);

    // ([[1, 2, 3, 4], [5, 6]], [["ab"], ["####", "QWERT"]])
    auto len = s.append(List(List(1, 2, 3, 4), List(5, 6)))
            .append(List(List("ab"), List("####", "QWERT")))
            .serialize(buffer, 2048);

    Schema schema = s.getSchema();
    auto et = python::Type::makeTupleType({python::Type::makeListType(python::Type::makeListType(python::Type::I64)),
                                           python::Type::makeListType(python::Type::makeListType(python::Type::STRING))});
    EXPECT_TRUE(schema.getRowType() == et);

    Deserializer d(schema);
    d.deserialize(buffer, 2048);
    free(buffer);

    auto row = d.getTuple();
    EXPECT_EQ(row.numElements(), 2);
    auto lst1 = *(List *)row.getField(0).getPtr();
    EXPECT_EQ(lst1.numElements(), 2);
    auto lst11 = *(List *)(lst1.getField(0).getPtr());
    EXPECT_EQ(lst11.desc(), "[1,2,3,4]");
    auto lst12 = *(List *)(lst1.getField(1).getPtr());
    EXPECT_EQ(lst12.desc(), "[5,6]");
    auto lst2 = *(List *)row.getField(1).getPtr();
    EXPECT_EQ(lst2.numElements(), 2);
    auto lst21 = *(List *)lst2.getField(0).getPtr();
    EXPECT_EQ(lst21.desc(), "['ab']");
    auto lst22 = *(List *)lst2.getField(1).getPtr();
    EXPECT_EQ(lst22.desc(), "['####','QWERT']");
}

TEST(Serializer, ListOfOptionalList) {
    // "[[5,6],None]"
    Serializer s;
    auto *buffer = (uint8_t*)malloc(2048);

    // test with [[5,6], None]
    auto len = s.append(List(List(5, 6), option<List>::none))
            .serialize(buffer, 2048);



    Schema schema = s.getSchema();
    auto et = python::Type::makeTupleType({python::Type::makeListType(python::Type::makeOptionType(python::Type::makeListType(python::Type::I64)))});
    EXPECT_TRUE(schema.getRowType() == et);

    Deserializer d(schema);
    d.deserialize(buffer, 2048);
    free(buffer);

    auto row = d.getTuple();
    EXPECT_EQ(row.numElements(), 1);
    auto lst1 = *(List *)row.getField(0).getPtr();
    EXPECT_EQ(lst1.numElements(), 2);
    EXPECT_EQ(lst1.desc(), "[[5,6],None]");
}

TEST(Serializer, OptionalTuple) {
    Serializer s;
    auto *buffer = (uint8_t*)malloc(2048);

    // (i64, Option[(i64, i64)], str)
    auto len = s.append(5000)
            .append(option<Tuple>(Tuple(1234, 9876)), python::Type::makeTupleType({python::Type::I64, python::Type::I64}))
            .append("$$$$tuple$$$$")
            .serialize(buffer, 2048);

    Schema schema = s.getSchema();
    auto et = python::Type::makeTupleType({python::Type::I64, python::Type::makeOptionType(python::Type::makeTupleType({python::Type::I64, python::Type::I64})), python::Type::STRING});

    EXPECT_TRUE(schema.getRowType() == et);
    Deserializer d(schema);
    d.deserialize(buffer, 2048);
    free(buffer);

    auto row = d.getTuple();
    EXPECT_EQ(row.numElements(), 3);
    EXPECT_EQ(row.getField(0).getInt(), 5000);
    auto tuple = *(Tuple *)row.getField(1).getPtr();
    EXPECT_EQ(tuple.numElements(), 2);
    EXPECT_EQ(tuple.desc(), "(1234,9876)");
    EXPECT_EQ(std::string((char *)(row.getField(2).getPtr())), "$$$$tuple$$$$");
}