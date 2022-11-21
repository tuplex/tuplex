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
#include <vector>
#include <TypeHelper.h>

boost::any get_representative_value(const python::Type& type) {
    using namespace tuplex;
    std::unordered_map<python::Type, Field> m{{python::Type::BOOLEAN, Field(false)},
                                                   {python::Type::I64, Field((int64_t)42)},
                                                   {python::Type::F64, Field(5.3)},
                                                   {python::Type::STRING, Field("hello world!")},
                                                   {python::Type::NULLVALUE, Field::null()},
                                                   {python::Type::EMPTYTUPLE, Field::empty_tuple()},
                                                   {python::Type::EMPTYLIST, Field::empty_list()},
                                                   {python::Type::EMPTYDICT, Field::empty_dict()}};
    return m.at(type);
}


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

    EXPECT_EQ(Type::EMPTYTUPLE, decodeType("()"));
    Type complex = Type::makeTupleType({Type::I64, Type::EMPTYTUPLE,
                                        Type::F64, Type::makeTupleType({Type::STRING,Type::BOOLEAN})});
    EXPECT_EQ(complex, decodeType("(i64, (), f64, (str, bool))"));
    Type more_complex = Type::makeTupleType({Type::F64, Type::makeDictionaryType(Type::STRING, Type::makeTupleType(
            {Type::I64, Type::BOOLEAN, Type::makeDictionaryType(Type::F64, Type::STRING)})), Type::F64});
    EXPECT_EQ(more_complex, decodeType("(f64, {str, (i64, bool, {f64, str})}, f64)"));
    Type even_more_complex = Type::makeTupleType(
            {Type::makeListType(Type::F64), Type::makeDictionaryType(Type::STRING, Type::makeTupleType(
                    {Type::makeListType(Type::I64), Type::BOOLEAN,
                     Type::makeListType(Type::makeDictionaryType(Type::F64, Type::makeOptionType(Type::STRING)))})),
             Type::F64,
             Type::makeListType(Type::makeOptionType(Type::STRING))});
    EXPECT_EQ(even_more_complex, decodeType("([f64], {str, ([i64], bool, [{f64, Option[str]}])}, f64, [Option[str]])"));
}

TEST(TypeSys, MoreDecoding) {
    using namespace python;

    auto t = decodeType("(i64,i64,i64,str,i64,i64,i64,i64,f64,i64,f64,f64,f64,Option[f64],Option[f64],Option[f64],Option[f64],Option[f64])");
    EXPECT_TRUE(t.isTupleType());
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

TEST(TypeSys, Pyobject) {
    using namespace python;

    EXPECT_EQ(decodeType("pyobject"), Type::PYOBJECT);

    // nested
    auto t = Type::makeTupleType({Type::I64, Type::PYOBJECT,
                                  Type::makeDictionaryType(Type::makeOptionType(Type::PYOBJECT), Type::PYOBJECT)});
    EXPECT_EQ(decodeType(t.desc()), t);
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

TEST(TypeSys, StructTypeStringKeyDecodingEncoding) {
    using namespace tuplex;
    using namespace std;

    // create string key
    auto t1 = python::Type::makeStructuredDictType(std::vector<pair<boost::any, python::Type>>{make_pair(10, python::Type::STRING),
                                                                                               make_pair("key", python::Type::STRING)});
    EXPECT_EQ(t1.desc(), "Struct[(i64,'10'->str),(str,'key'->str)]");

    auto t2 = python::Type::makeStructuredDictType(std::vector<pair<boost::any, python::Type>>{make_pair("42", python::Type::STRING),
                                                                                               make_pair("key", python::Type::NULLVALUE)});

    std::cout<<"t1: "<<t1.desc()<<std::endl;
    std::cout<<"t2: "<<t2.desc()<<std::endl;

    TypeUnificationPolicy t_policy; t_policy.unifyMissingDictKeys = true;
    auto combo_type = unifyTypes(t1, t2, t_policy);
    std::cout<<"unified: "<<combo_type.desc()<<std::endl;
    EXPECT_EQ(combo_type.desc(), "Struct[(str,'42'=>str),(str,'key'->Option[str]),(i64,'10'=>str)]");
}

#ifdef BUILD_WITH_CEREAL
TEST(TypeSys, Cerealization) {
    auto row_type = python::Type::makeTupleType({python::Type::I64, python::Type::I64, python::Type::PYOBJECT});
    std::ostringstream oss; {
        cereal::BinaryOutputArchive ar(oss);
        ar(row_type);
    }

    EXPECT_TRUE(oss.str().size() > 0);

    // @TODO: deserialization test.

    // ==> note: this requires some fancier lookup! b.c. the typemaps of the two systems may be out of sync...
    // @TODO: correct this...!
    // => better: semantic encoding of types and decoding from there...?
}
#endif

TEST(TypeSys, NewTypEncodeDecode) {
    using namespace tuplex;
    auto row_type = python::Type::makeTupleType({python::Type::I64, python::Type::I64, python::Type::PYOBJECT});

    // a couple other test types (mainly compound types)
    auto encoded_str = row_type.encode();
    auto t = python::Type::decode(encoded_str);
    EXPECT_EQ(t.desc(), row_type.desc());

    // check func has the right order
    auto func_type = python::Type::makeFunctionType(row_type, python::Type::makeOptionType(python::Type::makeTupleType({python::Type::F64, python::Type::STRING})));
    encoded_str = func_type.encode();
    t = python::Type::decode(encoded_str);
    EXPECT_EQ(t.desc(), func_type.desc());
}

TEST(TypeSys, ReturnTypeUpcasting) {
    using namespace tuplex;
    // [2022-04-12 10:45:23.706] [codegen] [debug] Deoptimized func ret type:   (i64,i64,i64,str,i64,i64,i64,f64,f64,Option[f64],Option[f64],Option[f64],Option[f64],Option[f64],Option[f64])
    //[2022-04-12 10:45:23.706] [codegen] [debug] Deoptimized target ret type: (i64,i64,i64,str,i64,i64,i64,f64,f64,f64,Option[f64],Option[f64],Option[f64],Option[f64],Option[f64])

    auto func_ret_type = python::Type::decode("Tuple[i64,i64,i64,str,i64,i64,i64,f64,f64,Option[f64],Option[f64],Option[f64],Option[f64],Option[f64],Option[f64]]");
    auto target_type = python::Type::decode("Tuple[i64,i64,i64,str,i64,i64,i64,f64,f64,f64,Option[f64],Option[f64],Option[f64],Option[f64],Option[f64]]");

    EXPECT_TRUE(python::canUpcastType(target_type, func_ret_type));

    std::cout<<"ret type: "<<func_ret_type.desc()<<std::endl;
    std::cout<<"target type: "<<target_type.desc()<<std::endl;
}

TEST(TypeSys, compatibleType) {
    using namespace tuplex;

    TypeUnificationPolicy policy; policy.allowAutoUpcastOfNumbers = true;


    // [Option[[i64]]] and [[Option[i64]]] ==> [Option[[Option[i64]]]]
    auto a1_type = python::Type::makeListType(python::Type::makeOptionType(python::Type::makeListType(python::Type::I64)));
    auto b1_type = python::Type::makeListType(python::Type::makeListType(python::Type::makeOptionType(python::Type::I64)));
    auto ab1_compatible_type = unifyTypes(a1_type, b1_type, policy);
    EXPECT_EQ(ab1_compatible_type, python::Type::makeListType(python::Type::makeOptionType(python::Type::makeListType(python::Type::makeOptionType(python::Type::I64)))));

    // Option[[Option[(Option[str], [Option[F64]])]]] and [(str, Option[[F64]])] ==> Option[[Option[(Option[str], Option[[Option[F64]]])]]]
    auto a2_type = python::Type::makeOptionType(python::Type::makeListType(python::Type::makeOptionType(python::Type::makeTupleType({python::Type::makeOptionType(python::Type::STRING), python::Type::makeListType(python::Type::makeOptionType(python::Type::F64))}))));
    auto b2_type = python::Type::makeListType(python::Type::makeTupleType({python::Type::STRING, python::Type::makeOptionType(python::Type::makeListType(python::Type::F64))}));
    auto ab2_compatible_type = unifyTypes(a2_type, b2_type, policy);
    EXPECT_EQ(ab2_compatible_type, python::Type::makeOptionType(python::Type::makeListType(python::Type::makeOptionType(python::Type::makeTupleType({python::Type::makeOptionType(python::Type::STRING), python::Type::makeOptionType(python::Type::makeListType(python::Type::makeOptionType(python::Type::F64)))})))));
}

TEST(TypeSys, structuredDictType) {
    using namespace tuplex;
    using namespace std;

    // Struct[] is empty dict!
    EXPECT_EQ(python::decodeType("Struct[]").desc(), python::Type::EMPTYDICT.desc());

    // all primitive types
    // -> create structured types

    // test 1: string keys (this is probably the most common scenario)
    vector<pair<boost::any, python::Type>> pairs;
    for(const auto& p : python::primitiveTypes(true)) {
        pairs.push_back(make_pair(p.desc(), p));
    }
    auto t = python::Type::makeStructuredDictType(pairs);
    auto encoded = t.desc();
    auto decoded_t = python::decodeType(encoded);
    EXPECT_EQ(decoded_t.desc(), t.desc());

    // test 2: non-string keys -> i.e., use Tuples, integers etc. as keys
    auto t_2 = python::Type::makeStructuredDictType({make_pair(10, python::Type::F64)}); // i64 -> f64 struct!
    auto encoded_2 = t_2.desc();
    auto decoded_2 = python::decodeType(encoded_2);
    EXPECT_EQ(decoded_2.desc(), t_2.desc());


    // test 3: nested struct type, different keys etc.
    auto subt_3_1 = python::Type::makeStructuredDictType({make_pair(10, python::Type::F64)}); // i64 -> f64 struct!
    auto encoded_subt_3_1 = subt_3_1.desc();
    auto decoded_subt_3_1 = python::decodeType(encoded_subt_3_1);
    EXPECT_EQ(decoded_subt_3_1.desc(), subt_3_1.desc());
    auto subt_3_2 = python::Type::makeStructuredDictType({make_pair(Field(Tuple(20, 30)), python::Type::I64),
                                                          make_pair(Field(Tuple(Field(Tuple(20, 32)), 20, 30)), Field(Tuple(20, 3.4)).getType()),
                                                          make_pair("test", python::Type::STRING),
                                                          make_pair("world", python::Type::I64),
                                                          make_pair(42, python::Type::makeTupleType({
                                                              python::Type::makeOptionType(python::Type::I64), python::Type::STRING
                                                          }))
    });
    auto encoded_subt_3_2 = subt_3_2.desc();
    auto decoded_subt_3_2 = python::decodeType(encoded_subt_3_2);
    EXPECT_EQ(decoded_subt_3_2.desc(), subt_3_2.desc());
    auto t_3 = python::Type::makeStructuredDictType({make_pair("nested_1", subt_3_1),
                                                     make_pair("nested_2", subt_3_2),
                                                     make_pair("str_list", python::Type::makeListType(python::Type::I64))});
    auto encoded_3 = t_3.desc();
    auto decoded_3 = python::decodeType(encoded_3);
    EXPECT_EQ(decoded_3.desc(), t_3.desc());

}

TEST(TypeSys, TypeObject) {
    auto types = python::primitiveTypes();

    for(const auto& t : types) {
        auto to = python::Type::makeTypeObjectType(t);
        ASSERT_TRUE(to.isTypeObjectType());
        EXPECT_EQ(to.desc(), "Type[" + t.desc() + "]");
        EXPECT_EQ(to.underlying(), t);
    }
}

TEST(TypeSys, ConstantEncodeDecode) {
    auto c1 = python::Type::makeConstantValuedType(python::Type::STRING, "hello 'world'!");
    auto str_c1 = c1.encode();
    auto c1_res = python::Type::decode(str_c1);
    EXPECT_EQ(c1, c1_res);
}