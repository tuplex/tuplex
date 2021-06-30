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
#include "TypeSystem.h"
#include <TupleTree.h>

TEST(TypeSys, tupleTypes) {
    using namespace python;

    EXPECT_TRUE(Type::EMPTYTUPLE.isTupleType());
    EXPECT_FALSE(Type::BOOLEAN.isTupleType());
    EXPECT_FALSE(Type::I64.isTupleType());
    EXPECT_FALSE(Type::F64.isTupleType());
    EXPECT_TRUE(Type::makeTupleType({Type::I64, Type::F64}).isTupleType());
}

TEST(TypeSys, fixedSizeTypes) {
    using namespace python;

    EXPECT_TRUE(Type::I64.isFixedSizeType());
    EXPECT_TRUE(Type::F64.isFixedSizeType());
    EXPECT_TRUE(Type::BOOLEAN.isFixedSizeType());


    EXPECT_FALSE(Type::STRING.isFixedSizeType());
    EXPECT_TRUE(Type::makeTupleType({Type::I64}).isFixedSizeType());
    EXPECT_TRUE(Type::makeTupleType({Type::I64, Type::F64}).isFixedSizeType());

    EXPECT_FALSE(Type::makeTupleType({Type::I64, Type::STRING}).isFixedSizeType());

    // functions, lists, dictionaries are never fixed size
    EXPECT_FALSE(TypeFactory::instance().createOrGetFunctionType(Type::F64, Type::EMPTYTUPLE).isFixedSizeType());
    EXPECT_FALSE(TypeFactory::instance().createOrGetDictionaryType(Type::STRING, Type::F64).isFixedSizeType());
    EXPECT_FALSE(TypeFactory::instance().createOrGetListType(Type::I64).isFixedSizeType());

    // emptydict, emptytuple & NULL are fixed size (0 bytes)
    EXPECT_TRUE(Type::NULLVALUE.isFixedSizeType());
    EXPECT_TRUE(Type::EMPTYDICT.isFixedSizeType());
    EXPECT_TRUE(Type::EMPTYTUPLE.isFixedSizeType());
}

TEST(TypeSys, StrDecoding) {
    using namespace python;

    EXPECT_TRUE(Type::EMPTYTUPLE == decodeType("()"));
    Type complex = Type::makeTupleType({Type::I64, Type::EMPTYTUPLE,
                                        Type::F64, Type::makeTupleType({Type::STRING,Type::BOOLEAN})});
    EXPECT_TRUE(complex == decodeType("(i64, (), f64, (str, bool))"));
    Type more_complex = Type::makeTupleType({Type::F64, Type::makeDictionaryType(Type::STRING, Type::makeTupleType(
            {Type::I64, Type::BOOLEAN, Type::makeDictionaryType(Type::F64, Type::STRING)})), Type::F64});
    EXPECT_TRUE(more_complex == decodeType("(f64, {str, (i64, bool, {f64, str})}, f64)"));
    Type even_more_complex = Type::makeTupleType(
            {Type::makeListType(Type::F64), Type::makeDictionaryType(Type::STRING, Type::makeTupleType(
                    {Type::makeListType(Type::I64), Type::BOOLEAN,
                     Type::makeListType(Type::makeDictionaryType(Type::F64, Type::makeOptionType(Type::STRING)))})),
             Type::F64,
             Type::makeListType(Type::makeOptionType(Type::STRING))});
    EXPECT_TRUE(even_more_complex == decodeType("([f64], {str, ([i64], bool, [{f64, Option[str]}])}, f64, [Option[str]])"));
}

TEST(TypeSys, TupleHaveSameType) {
    using namespace python;

    // (i64,i64,i64,i64)
    Type i64 = python::Type::I64;
    Type f64 = python::Type::F64;
    Type multi = Type::makeTupleType({i64, i64, i64, i64});

    EXPECT_TRUE(tupleElementsHaveSameType(python::Type::EMPTYTUPLE));

    EXPECT_TRUE(tupleElementsHaveSameType(multi));
    EXPECT_TRUE(tupleElementsHaveSameType(Type::makeTupleType({i64, i64, i64, i64})));

    EXPECT_FALSE(tupleElementsHaveSameType(Type::makeTupleType({f64, i64, i64, i64})));
    EXPECT_FALSE(tupleElementsHaveSameType(Type::makeTupleType({i64, f64, i64, i64})));
    EXPECT_FALSE(tupleElementsHaveSameType(Type::makeTupleType({i64, i64, f64, i64})));
    EXPECT_FALSE(tupleElementsHaveSameType(Type::makeTupleType({i64, i64, i64, f64})));
}

TEST(TypeSys, OptionalTypes) {
    using namespace python;

    EXPECT_EQ(Type::NULLVALUE, Type::makeOptionType(Type::NULLVALUE));
    auto t1 = Type::makeOptionType(Type::I64);
    EXPECT_EQ(t1, Type::makeOptionType(t1));

    // to string
    EXPECT_EQ(t1.desc(), "Option[i64]");

    // decode?
    EXPECT_EQ(decodeType("Option[str]"), Type::makeOptionType(Type::STRING));

    EXPECT_EQ(t1.getReturnType(), python::Type::I64);
}

TEST(TypeSys, ZeroSize) {
    using namespace std;
    EXPECT_TRUE(python::Type::NULLVALUE.isZeroSerializationSize());
    EXPECT_TRUE(python::Type::EMPTYTUPLE.isZeroSerializationSize());
    EXPECT_TRUE(python::Type::EMPTYLIST.isZeroSerializationSize());
    EXPECT_TRUE(python::Type::EMPTYDICT.isZeroSerializationSize());

    // any combination of these!
    vector<python::Type> t{python::Type::NULLVALUE, python::Type::EMPTYTUPLE, python::Type::EMPTYDICT, python::Type::EMPTYLIST};

    EXPECT_TRUE(python::Type::makeTupleType(t).isZeroSerializationSize());

    EXPECT_FALSE(python::Type::F64.isZeroSerializationSize());

    EXPECT_FALSE(python::Type::makeTupleType({python::Type::EMPTYTUPLE, python::Type::NULLVALUE, python::Type::STRING}).isZeroSerializationSize());

}

TEST(TypeSys, Subclass) {
    EXPECT_TRUE(python::Type::BOOLEAN.isSubclass(python::Type::I64));
    EXPECT_TRUE(python::Type::BOOLEAN.isSubclass(python::Type::F64));
    EXPECT_TRUE(python::Type::I64.isSubclass(python::Type::F64));
    EXPECT_FALSE(python::Type::F64.isSubclass(python::Type::I64));
}

TEST(TypeSys, Superclass) {
    auto derived = python::Type::BOOLEAN.derivedClasses();

    ASSERT_EQ(derived.size(), 2); // i64 and f64 are derived from bool!
    EXPECT_TRUE((derived[0] == python::Type::I64 && derived[1] == python::Type::F64) ||
    (derived[1] == python::Type::I64 && derived[0] == python::Type::F64) );
}

TEST(TypeSys, SpecializeGenerics) {
    // concrete versions
    // "([pyobject]) -> i64"
    // "(Option[[f64]]) -> i64"
    auto specialType = python::specializeGenerics(python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::makeListType(python::Type::F64))),
                                          python::Type::makeTupleType({python::Type::makeListType(python::Type::PYOBJECT)}));
    // result should be
    // "()" because Option is not a general primitive type!
    EXPECT_EQ(specialType, python::Type::propagateToTupleType(python::Type::makeListType(python::Type::F64)));
}

TEST(TypeSys, flattenWithPyObject) {

    auto row_type = python::Type::makeTupleType({python::Type::I64, python::Type::I64, python::Type::PYOBJECT});
    auto num_params = tuplex::flattenedType(row_type).parameters().size();
    EXPECT_EQ(num_params, 3);
}