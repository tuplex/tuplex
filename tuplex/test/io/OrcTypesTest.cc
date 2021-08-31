#include <gtest/gtest.h>
#include <orc/OrcTypes.h>

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