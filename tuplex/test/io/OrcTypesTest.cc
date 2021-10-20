
#include <gtest/gtest.h>

#ifdef BUILD_WITH_ORC

#include <orc/OrcTypes.h>

// Tests for ORCToTuplex
TEST(ORC, ORCToTuplexPrimitives) {
    using namespace tuplex::orc;
    auto booleanType = orcTypeToTuplex(*orc::createPrimitiveType(orc::BOOLEAN), false);
    EXPECT_EQ(python::Type::BOOLEAN, booleanType);

    auto byteType = orcTypeToTuplex(*orc::createPrimitiveType(orc::BYTE), false);
    EXPECT_EQ(python::Type::I64, byteType);

    auto shortType = orcTypeToTuplex(*orc::createPrimitiveType(orc::SHORT), false);
    EXPECT_EQ(python::Type::I64, shortType);

    auto intType = orcTypeToTuplex(*orc::createPrimitiveType(orc::INT), false);
    EXPECT_EQ(python::Type::I64, intType);

    auto longType = orcTypeToTuplex(*orc::createPrimitiveType(orc::LONG), false);
    EXPECT_EQ(python::Type::I64, longType);

    auto floatType = orcTypeToTuplex(*orc::createPrimitiveType(orc::FLOAT), false);
    EXPECT_EQ(python::Type::F64, floatType);

    auto doubleType = orcTypeToTuplex(*orc::createPrimitiveType(orc::DOUBLE), false);
    EXPECT_EQ(python::Type::F64, doubleType);

    auto stringType = orcTypeToTuplex(*orc::createPrimitiveType(orc::STRING), false);
    EXPECT_EQ(python::Type::STRING, stringType);

    auto varcharType = orcTypeToTuplex(*orc::createCharType(orc::VARCHAR, 1), false);
    EXPECT_EQ(python::Type::STRING, varcharType);

    auto charType = orcTypeToTuplex(*orc::createCharType(orc::CHAR, 1), false);
    EXPECT_EQ(python::Type::STRING, charType);

    auto binaryType = orcTypeToTuplex(*orc::createPrimitiveType(orc::BINARY), false);
    EXPECT_EQ(python::Type::STRING, binaryType);

    auto timestampType = orcTypeToTuplex(*orc::createPrimitiveType(orc::TIMESTAMP), false);
    EXPECT_EQ(python::Type::I64, timestampType);

    auto dateType = orcTypeToTuplex(*orc::createPrimitiveType(orc::DATE), false);
    EXPECT_EQ(python::Type::I64, dateType);
}

TEST(ORC, ORCToTuplexUndefined) {
    using namespace tuplex::orc;
    EXPECT_THROW(orcTypeToTuplex(*orc::createUnionType(), false), std::runtime_error);
}

TEST(ORC, ORCToTuplexLists) {
    using namespace tuplex::orc;
    auto intListType = orcTypeToTuplex(*orc::createListType(orc::createPrimitiveType(orc::INT)), false);
    EXPECT_EQ(python::Type::makeListType(python::Type::I64), intListType);

    auto nestedListType = orcTypeToTuplex(*orc::createListType(orc::createListType(orc::createPrimitiveType(orc::STRING))), false);
    EXPECT_EQ(python::Type::makeListType(python::Type::makeListType(python::Type::STRING)), nestedListType);
}

TEST(ORC, ORCToTuplexMap) {
    using namespace tuplex::orc;
    auto mapType = orcTypeToTuplex(*orc::createMapType(orc::createPrimitiveType(orc::STRING), orc::createPrimitiveType(orc::FLOAT)),
                                   false);
    EXPECT_EQ(python::Type::makeDictionaryType(python::Type::STRING, python::Type::F64), mapType);

    auto nestedMapType = orcTypeToTuplex(*orc::createMapType(orc::createMapType(orc::createPrimitiveType(orc::STRING), orc::createPrimitiveType(orc::FLOAT)),
                                                             orc::createPrimitiveType(orc::FLOAT)), false);
    EXPECT_EQ(python::Type::makeDictionaryType(mapType, python::Type::F64), nestedMapType);
}

TEST(ORC, ORCToTuplexStruct) {
    using namespace tuplex::orc;
    auto structType = orc::createStructType();
    structType->addStructField("", orc::createPrimitiveType(orc::INT));
    structType->addStructField("", orc::createPrimitiveType(orc::STRING));
    structType->addStructField("", orc::createPrimitiveType(orc::FLOAT));
    structType->addStructField("", orc::createListType(createPrimitiveType(orc::LONG)));
    structType->addStructField("", orc::createPrimitiveType(orc::BOOLEAN));

    auto targetType = python::Type::makeTupleType({
                                                          python::Type::I64,
                                                          python::Type::STRING,
                                                          python::Type::F64,
                                                          python::Type::makeListType(python::Type::I64),
                                                          python::Type::BOOLEAN
                                                  });

    EXPECT_EQ(targetType, orcTypeToTuplex(*structType, false));
}

// Tests for TuplexToORC
TEST(ORC, TuplexToORCTuple) {
    auto tupleType = tuplex::orc::tuplexRowTypeToOrcType(python::Type::makeTupleType({
                                                            python::Type::BOOLEAN,
                                                            python::Type::I64,
                                                            python::Type::F64,
                                                            python::Type::STRING}));
    EXPECT_EQ(0, tupleType->getColumnId());
    EXPECT_EQ(4, tupleType->getMaximumColumnId());
    EXPECT_EQ(4, tupleType->getSubtypeCount());
    EXPECT_EQ(orc::STRUCT, tupleType->getKind());
    EXPECT_EQ("struct<:boolean,:bigint,:double,:string>", tupleType->toString());

    auto child = tupleType->getSubtype(0);
    EXPECT_EQ(1, child->getColumnId());
    EXPECT_EQ(1, child->getMaximumColumnId());
    EXPECT_EQ(orc::BOOLEAN, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = tupleType->getSubtype(1);
    EXPECT_EQ(2, child->getColumnId());
    EXPECT_EQ(2, child->getMaximumColumnId());
    EXPECT_EQ(orc::LONG, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = tupleType->getSubtype(2);
    EXPECT_EQ(3, child->getColumnId());
    EXPECT_EQ(3, child->getMaximumColumnId());
    EXPECT_EQ(orc::DOUBLE, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = tupleType->getSubtype(3);
    EXPECT_EQ(4, child->getColumnId());
    EXPECT_EQ(4, child->getMaximumColumnId());
    EXPECT_EQ(orc::STRING, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());
}

TEST(ORC, TuplexToORCDictionary) {
auto dictionaryType = tuplex::orc::tuplexRowTypeToOrcType(python::Type::makeDictionaryType(
        python::Type::I64, python::Type::STRING));
    EXPECT_EQ(0, dictionaryType->getColumnId());
    EXPECT_EQ(2, dictionaryType->getMaximumColumnId());
    EXPECT_EQ(orc::MAP, dictionaryType->getKind());
    EXPECT_EQ(2, dictionaryType->getSubtypeCount());

    auto keyType = dictionaryType->getSubtype(0);
    EXPECT_EQ(1, keyType->getColumnId());
    EXPECT_EQ(1, keyType->getMaximumColumnId());
    EXPECT_EQ(orc::LONG, keyType->getKind());
    EXPECT_EQ(0, keyType->getSubtypeCount());

    auto valueType = dictionaryType->getSubtype(1);
    EXPECT_EQ(2, valueType->getColumnId());
    EXPECT_EQ(2, valueType->getMaximumColumnId());
    EXPECT_EQ(orc::STRING, valueType->getKind());
    EXPECT_EQ(0, valueType->getSubtypeCount());
}

TEST(ORC, TuplexToORCList) {
    auto intListType = tuplex::orc::tuplexRowTypeToOrcType(python::Type::makeListType(
            python::Type::I64));
    EXPECT_EQ(0, intListType->getColumnId());
    EXPECT_EQ(1, intListType->getMaximumColumnId());
    EXPECT_EQ(orc::LIST, intListType->getKind());
    EXPECT_EQ(1, intListType->getSubtypeCount());

    auto elementType = intListType->getSubtype(0);
    EXPECT_EQ(1, elementType->getColumnId());
    EXPECT_EQ(1, elementType->getMaximumColumnId());
    EXPECT_EQ(orc::LONG, elementType->getKind());
    EXPECT_EQ(0, elementType->getSubtypeCount());
}

TEST(ORC, TuplexToORCPrimitive) {
    using namespace tuplex::orc;
    auto booleanType = tuplexRowTypeToOrcType(python::Type::BOOLEAN);
    EXPECT_EQ(orc::BOOLEAN, booleanType->getKind());

    auto intType = tuplexRowTypeToOrcType(python::Type::I64);
    EXPECT_EQ(orc::LONG, intType->getKind());

    auto floatType = tuplexRowTypeToOrcType(python::Type::F64);
    EXPECT_EQ(orc::DOUBLE, floatType->getKind());

    auto stringType = tuplexRowTypeToOrcType(python::Type::STRING);
    EXPECT_EQ(orc::STRING, stringType->getKind());
}

#endif