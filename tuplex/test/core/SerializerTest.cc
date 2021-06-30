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