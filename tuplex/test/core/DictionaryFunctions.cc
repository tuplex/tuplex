//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by rahuly first first on 9/15/19                                                                          //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include <Context.h>
#include "../../utils/include/Utils.h"
#include "TestUtils.h"
#include "RuntimeInterface.h"

// need for these tests a running python interpreter, so spin it up
class DictionaryFunctions : public PyTest {};

TEST_F(DictionaryFunctions, DictionarySubscript) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({
            Row("ab"), Row("cd"), Row("ef")
    }).map(UDF("lambda x: {'ab': 1, 'cd': 2, 'ef': 3}[x]")).collectAsVector();

    EXPECT_EQ(v0.size(), 3);
    ASSERT_EQ(v0[0].getInt(0), 1);
    ASSERT_EQ(v0[1].getInt(0), 2);
    ASSERT_EQ(v0[2].getInt(0), 3);

    auto v1 = c.parallelize({
            Row("ab"), Row("cd"), Row("ef")
    }).map(UDF("lambda x: {'ab': 0.23, 'cd': 1.345, 'ef': 3.45}[x]")).collectAsVector();

    EXPECT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0], 0.23);
    EXPECT_EQ(v1[1], 1.345);
    EXPECT_EQ(v1[2], 3.45);

    auto v2 = c.parallelize({
            Row(3.12), Row(-0.45), Row(10.0)
    }).map(UDF("lambda x: {-0.45: 'abc', 3.12: 'def', 10.0: 'ghj'}[x]")).collectAsVector();

    EXPECT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0], std::string("def"));
    EXPECT_EQ(v2[1], std::string("abc"));
    EXPECT_EQ(v2[2], std::string("ghj"));

    auto v3 = c.parallelize({
            Row(3.12), Row(-0.45), Row(10.0)
    }).map(UDF("lambda x: {-0.45: False, 3.12: True, 10.0: False}[x]")).collectAsVector();

    EXPECT_EQ(v3.size(), 3);
    EXPECT_EQ(v3[0], true);
    EXPECT_EQ(v3[1], false);
    EXPECT_EQ(v3[2], false);

    auto v4 = c.parallelize({
            Row(-10), Row(0), Row(1)
    }).map(UDF("lambda x: {1: 2.444, 0: 1.02, -10: 3.22}[x]")).collectAsVector();

    EXPECT_EQ(v4.size(), 3);
    EXPECT_EQ(v4[0], 3.22);
    EXPECT_EQ(v4[1], 1.02);
    EXPECT_EQ(v4[2], 2.444);

    auto v5 = c.parallelize({
            Row(-10), Row(0), Row(1)
    }).map(UDF("lambda x: {1: True, 0: False, -10: True}[x]")).collectAsVector();

    EXPECT_EQ(v5.size(), 3);
    EXPECT_EQ(v5[0], true);
    EXPECT_EQ(v5[1], false);
    EXPECT_EQ(v5[2], true);
}

TEST_F(DictionaryFunctions, DictionaryPop) {
    using namespace tuplex;
    auto co = microTestOptions();
    co.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    Context c(co);

    auto v1 = c.parallelize({
            Row("ab"), Row("cd"), Row("ef"), Row("gh")
    }).map(UDF("lambda x: {'ab': 0.23, 'cd': 1.345, 'gh': 3.45}.pop(x, -1.23)")).collectAsVector();

    ASSERT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0], 0.23);
    EXPECT_EQ(v1[1], 1.345);
    EXPECT_EQ(v1[2], -1.23);
    EXPECT_EQ(v1[3], 3.45);

    auto v2 = c.parallelize({
            Row(-11), Row(-2), Row(0), Row(13)
    }).map(UDF("lambda x: {-2: True, 0: False}.pop(x, True)")).collectAsVector();

    EXPECT_EQ(v2.size(), 4);
    EXPECT_EQ(v2[0], true);
    EXPECT_EQ(v2[1], true);
    EXPECT_EQ(v2[2], false);
    EXPECT_EQ(v2[3], true);

    // when only a single var is given, then return item if there else produce keyerror
    auto v3 = c.parallelize({
                                    Row(-11), Row(-2), Row(0), Row(13)
                            }).map(UDF("lambda x: {-2: True, 0: False}.pop(x)"))
                                    .resolve(ExceptionCode::KEYERROR, UDF("lambda x: False"))
                                    .collectAsVector();
}

TEST_F(DictionaryFunctions, DictionaryPopItem) {
    using namespace tuplex;
    auto co = microTestOptions();
    co.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    Context c(co);

    auto v1 = c.parallelize({
            Row("1"), Row("2"), Row("3"), Row("4")
    }).map(UDF("lambda x: {'ab': 0.23, 'cd': 1.345, 'gh': 3.45}.popitem()")).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0].toPythonString(), "('ab',0.23000)");
    EXPECT_EQ(v1[1].toPythonString(), "('ab',0.23000)");
    EXPECT_EQ(v1[2].toPythonString(), "('ab',0.23000)");
    EXPECT_EQ(v1[3].toPythonString(), "('ab',0.23000)");

    auto v2 = c.parallelize({
            Row("1"), Row("2")
    }).map(UDF("lambda x: {123: 'abcc', 234: 'gh'}.popitem()")).collectAsVector();

    EXPECT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "(123,'abcc')");
    EXPECT_EQ(v2[1].toPythonString(), "(123,'abcc')");

    auto v3 = c.parallelize({
            Row("1"), Row("2")
    }).map(UDF("lambda x: {0.12: True, 234.34: False}.popitem()")).collectAsVector();

    EXPECT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "(0.12000,True)");
    EXPECT_EQ(v3[1].toPythonString(), "(0.12000,True)");

    auto v4 = c.parallelize({
            Row("1"), Row("2")
    }).map(UDF("lambda x: {True: 123, False: 678}.popitem()")).collectAsVector();

    EXPECT_EQ(v4.size(), 2);
    EXPECT_EQ(v4[0].toPythonString(), "(True,123)");
    EXPECT_EQ(v4[1].toPythonString(), "(True,123)");


    // popitem on empty dict should produce key error!
    auto code = "def f(x):\n"
                "   d = {'test' : 'hello'}\n"
                "   d.popitem()\n"
                "   return d.popitem()\n";
    auto v5 = c.parallelize({Row("1")}).map(UDF(code)).resolve(ExceptionCode::KEYERROR, UDF("lambda x: ('a','b')")).collectAsVector();
    ASSERT_EQ(v5.size(), 1);
    EXPECT_EQ(v5[0].toPythonString(), "('a','b')");
}

TEST_F(DictionaryFunctions, TestFunctionsMaps) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto code1 = "def a(x):\n"
                "    y = {'a': '30', 'b': 'hello'}\n"
                "    return {'c': 10, 'y': y['b'], 'test': int(y['a'])}";
    auto code2 = "def b(x):\n"
                 "   return x['c']";
    auto code3 = "def c(x):\n"
                 "   return x['y']";
    auto code4 = "def d(x):\n"
                 "   return x['test']";
    auto ds = c.parallelize({Row(1)}).map(UDF(code1));
    auto v1 = ds.collectAsVector();
    auto v2 = ds.map(UDF(code2)).collectAsVector();
    auto v3 = ds.map(UDF(code3)).collectAsVector();
    auto v4 = ds.map(UDF(code4)).collectAsVector();

    // TODO: Should the compiler throw an error when rewriteDictAccessInAST() doesn't find the column name? (right now it just prints an error with IFailable)
    ASSERT_EQ(v1.size(), 1);
    EXPECT_EQ(v1[0].toPythonString(), "(10,'hello',30)");
    ASSERT_EQ(v2.size(), 1);
    EXPECT_EQ(v2[0].toPythonString(), "(10,)");
    ASSERT_EQ(v3.size(), 1);
    EXPECT_EQ(v3[0].toPythonString(), "('hello',)");
    ASSERT_EQ(v4.size(), 1);
    EXPECT_EQ(v4[0].toPythonString(), "(30,)");
}

TEST_F(DictionaryFunctions, TestLambdaMapI) {
    using namespace tuplex;
    Context c(microTestOptions());

    // Test output column rewrite
    auto v0 = c.parallelize({Row(1,2), Row(3,4)})
            .map(UDF("lambda x: {'a': x[0], 'b':x[1]}"))
            .collectAsVector();

    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].toPythonString(), "(1,2)");
    EXPECT_EQ(v0[1].toPythonString(), "(3,4)");

    // Test column rewrites with various types, collapse to single value
    auto v1 = c.parallelize({Row("123", "456"), Row("012", "102")}, {"col1","col2"})
            .map(UDF("lambda x: {'col3': int(x['col1']), 'col4': int(x['col2'])}"))
            .map(UDF("lambda x: x['col3']+x['col4']"))
            .collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "(579,)");
    EXPECT_EQ(v1[1].toPythonString(), "(114,)");

    auto v2 = c.parallelize({Row("a"), Row("b")}, {"col1"})
            .map(UDF("lambda x: {'col2': x['col1']+'c'}"))
            .map(UDF("lambda x: x['col2']+'d'"))
            .collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "('acd',)");
    EXPECT_EQ(v2[1].toPythonString(), "('bcd',)");

    auto v3 = c.parallelize({Row(1.2, 3.4), Row(5.6, 7.8)}, {"col1","col2"})
            .map(UDF("lambda x: {'col3': x['col1']+0.1, 'col4': x['col2']+0.2}"))
            .map(UDF("lambda x: x['col3']*x['col4']"))
            .collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_DOUBLE_EQ(v3[0].getDouble(0), 4.68);
    EXPECT_DOUBLE_EQ(v3[1].getDouble(0), 45.6);

    auto v4 = c.parallelize({Row(1, 2), Row(3, 4)}, {"col1","col2"})
            .map(UDF("lambda x: {'col3': x['col1']-1, 'col4': x['col2']+1}"))
            .map(UDF("lambda x: x['col3']-x['col4']"))
            .collectAsVector();
    ASSERT_EQ(v4.size(), 2);
    EXPECT_EQ(v4[0].toPythonString(), "(-3,)");
    EXPECT_EQ(v4[1].toPythonString(), "(-3,)");
}

TEST_F(DictionaryFunctions, TestLambdaMapII) {
    using namespace tuplex;
    Context c(microTestOptions());

    // mixed types, collapse to one value
    auto code1 = "def f(x):\n"
                 "    if x['col3']:\n"
                 "        return x['col4']\n"
                 "    else:\n"
                 "        return -1";
    auto v5 = c.parallelize({Row(true, 4), Row(false, 12)}, {"col1","col2"})
            .map(UDF("lambda x: {'col3': x['col1'], 'col4': x['col2']+2}"))
            .map(UDF(code1))
            .collectAsVector();
    ASSERT_EQ(v5.size(), 2);
    EXPECT_EQ(v5[0].toPythonString(), "(6,)");
    EXPECT_EQ(v5[1].toPythonString(), "(-1,)");

    auto v6 = c.parallelize({Row(1.2, "abc"), Row(3.4, "def")})
            .map(UDF("lambda x, y: {'col1': x+2.4, 'col2': y[:2]}"))
            .map(UDF("lambda x: str(x['col1']) + ' ' + x['col2']"))
            .collectAsVector();
    ASSERT_EQ(v6.size(), 2);
    EXPECT_EQ(v6[0].toPythonString(), "('3.6 ab',)");
    EXPECT_EQ(v6[1].toPythonString(), "('5.8 de',)");

    // return multiple columns
    auto v7 = c.parallelize({Row(12, -1.23), Row(-23, 3.54)})
            .map(UDF("lambda x, y: {'col1': x+y, 'col2': x-y}"))
            .map(UDF("lambda x: {'col3': x['col1']+1.2, 'col4': int(x['col2'])}"))
            .collectAsVector();
    ASSERT_EQ(v7.size(), 2);
    EXPECT_EQ(v7[0].toPythonString(), "(11.97000,13)");
    EXPECT_EQ(v7[1].toPythonString(), "(-18.26000,-26)");

    auto v8 = c.parallelize({Row("hello", true), Row("bye", false)})
            .map(UDF("lambda x, y: {'col1': x + str(y), 'col2': (bool(x) and y)}"))
            .map(UDF("lambda x: {'col3': x['col1'][::-1], 'col4': x['col2']}"))
            .collectAsVector();
    ASSERT_EQ(v8.size(), 2);
    EXPECT_EQ(v8[0].toPythonString(), "('eurTolleh',True)");
    EXPECT_EQ(v8[1].toPythonString(), "('eslaFeyb',False)");
}

TEST_F(DictionaryFunctions, TestMapColumn) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row(1,2), Row(3,4)})
            .map(UDF("lambda x, y: {'col1': x+y, 'col2': x-y}"))
            .mapColumn("col1", UDF("lambda x: str(x)"))
            .map(UDF("lambda x: {'col3': x['col1']+'1', 'col4': x['col2'] - 1}"))
            .collectAsVector();
    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].toPythonString(), "('31',-2)");
    EXPECT_EQ(v0[1].toPythonString(), "('71',-2)");

    auto v1 = c.parallelize({Row("1","2"), Row("3","4")})
            .map(UDF("lambda x, y: {'col1': x+y, 'col2': int(x)+int(y)}"))
            .mapColumn("col2", UDF("lambda x: str(x)"))
            .map(UDF("lambda x: {'col3': x['col1'], 'col4': x['col1'] + x['col2']}"))
            .collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "('12','123')");
    EXPECT_EQ(v1[1].toPythonString(), "('34','347')");

    auto v2 = c.parallelize({Row(true,false), Row(false,false)})
            .map(UDF("lambda x, y: {'col1': x and y, 'col2': x or y}"))
            .mapColumn("col1", UDF("lambda x: True"))
            .collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "(True,True)");
    EXPECT_EQ(v2[1].toPythonString(), "(True,False)");

    auto v3 = c.parallelize({Row(1.345,2.31), Row(-10.234,0.0)})
            .map(UDF("lambda x, y: {'col1': x + y, 'col2': y - x}"))
            .mapColumn("col1", UDF("lambda x: 2*x"))
            .map(UDF("lambda x: x['col1'] + x['col2']"))
            .collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "(8.27500,)");
    EXPECT_EQ(v3[1].toPythonString(), "(-10.23400,)");
}

TEST_F(DictionaryFunctions, TestWithColumn) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row(1,2), Row(3,4)})
            .map(UDF("lambda x, y: {'col1': x+y, 'col2': x-y}"))
            .withColumn("col2", UDF("lambda x: x['col1'] + x['col2'] + 1")) // overwrite existing column
            .withColumn("col3", UDF("lambda x: x['col1'] - 1")) // add new column
            .map(UDF("lambda x: {'col4': x['col1'] + x['col3'], 'col5': x['col2']}")) // map back to two columns
            .collectAsVector();
    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].toPythonString(), "(5,3)");
    EXPECT_EQ(v0[1].toPythonString(), "(13,7)");

    auto v1 = c.parallelize({Row("1","2"), Row("3","4")})
            .map(UDF("lambda x, y: {'col1': x+y, 'col2': 2*x}"))
            .withColumn("col2", UDF("lambda x: x['col1'] + x['col2'] + '1'")) // overwrite existing column
            .withColumn("col3", UDF("lambda x: 2*x['col1']")) // add new column
            .map(UDF("lambda x: {'col4': x['col1'] + x['col3'], 'col5': x['col2']}")) // map back to two columns
            .collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "('121212','12111')");
    EXPECT_EQ(v1[1].toPythonString(), "('343434','34331')");

    auto v2 = c.parallelize({Row(true,false), Row(true,true)})
            .map(UDF("lambda x, y: {'col1': x, 'col2': y}"))
            .withColumn("col1", UDF("lambda x: x['col1'] and x['col2']")) // overwrite existing column
            .withColumn("col3", UDF("lambda x: True")) // add new column
            .collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "(False,False,True)");
    EXPECT_EQ(v2[1].toPythonString(), "(True,True,True)");

    auto v3 = c.parallelize({Row(-11.1,21.3), Row(3.4,-4.5)})
            .map(UDF("lambda x, y: {'col1': x+y, 'col2': x-y}"))
            .withColumn("col2", UDF("lambda x: x['col1'] + x['col2'] + 1")) // overwrite existing column
            .withColumn("col3", UDF("lambda x: x['col1'] - 1")) // add new column
            .map(UDF("lambda x: {'col4': x['col1'] + x['col3'], 'col5': x['col2']}")) // map back to two columns
            .collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "(19.40000,-21.20000)");
    EXPECT_EQ(v3[1].toPythonString(), "(-3.20000,7.80000)");
}

TEST_F(DictionaryFunctions, TestDictionaryReturns) {
    using namespace tuplex;
    Context c(microTestOptions());

    // Test dictionary return in tuple
    auto ds = c.parallelize({Row("D1"), Row("D2")})
            .map(UDF("lambda x: ({x: True},)"));

    auto v1 = ds.collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "(({'D1':True},),)");
    EXPECT_EQ(v1[1].toPythonString(), "(({'D2':True},),)");

    auto v2 = ds.map(UDF("lambda y: y[0]['D1']")).collectAsVector();
    ASSERT_EQ(v2.size(), 1);
    EXPECT_EQ(v2[0].toPythonString(), "(True,)");

    // the above test has the same behavior as the one below
    auto v3 = c.parallelize({Row("D1"), Row("D2")})
            .map(UDF("lambda x: (x,)"))
            .map(UDF("lambda s: s[0][1]"))
            .collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "('1',)");
    EXPECT_EQ(v3[1].toPythonString(), "('2',)");

    auto v4 = c.parallelize({Row(1), Row(2), Row(3)}).map(UDF("lambda x: ({'a': 10, 'b': 20},)")).collectAsVector();
    ASSERT_EQ(v4.size(), 3);
    EXPECT_EQ(v4[0].toPythonString(), "(({'a':10,'b':20},),)");
    EXPECT_EQ(v4[1].toPythonString(), "(({'a':10,'b':20},),)");
    EXPECT_EQ(v4[2].toPythonString(), "(({'a':10,'b':20},),)");
}

TEST_F(DictionaryFunctions, TestDictionaryLen) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row(1), Row(2)}).map(UDF("lambda x: len({'a': 1, 'b': 2, 'c': 3})")).collectAsVector();
    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].toPythonString(), "(3,)");
    EXPECT_EQ(v0[1].toPythonString(), "(3,)");

    auto v1 = c.parallelize({Row(1, "a", 2, "b")}).map(UDF("lambda x: len({x[0]: x[1], x[2]: x[3]})")).collectAsVector();
    ASSERT_EQ(v1.size(), 1);
    EXPECT_EQ(v1[0].toPythonString(), "(2,)");
}

TEST_F(DictionaryFunctions, TestBooleanBug) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto v0 = c.parallelize({Row(true, false, 1, 2), Row(false, true, 1, 2)}).map(UDF("lambda x: ({x[0]: x[2], x[1]: x[3]},)")).collectAsVector();
    ASSERT_EQ(v0.size(), 2);
    EXPECT_EQ(v0[0].toPythonString(), "(({True:1,False:2},),)");
    EXPECT_EQ(v0[1].toPythonString(), "(({False:1,True:2},),)");
}

TEST_F(DictionaryFunctions, SingleElementUnwrap) {
    // make sure that when a single item as returned in string/dict syntax, the unwrapping works accordingly!
    using namespace tuplex;
    Context c(microTestOptions());

    auto v = c.parallelize({Row("Snowwhite"), Row("Huntsman")}, {"name"})
              .map(UDF("lambda t: {'res' : t}")) // note that b.c. it's a single column, it's single element access!
              .collectAsVector();

    for(auto r : v)
        std::cout<<r.toPythonString()<<std::endl;
    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], Row("Snowwhite"));
    EXPECT_EQ(v[1], Row("Huntsman"));
}

TEST_F(DictionaryFunctions, EmptyDict) {
    // make sure that when a single item as returned in string/dict syntax, the unwrapping works accordingly!
    using namespace tuplex;
    using namespace std;


    // type decoding for special dict string
    EXPECT_EQ(python::Type::EMPTYDICT, python::decodeType("{}"));
    EXPECT_EQ(python::Type::makeTupleType({python::Type::EMPTYDICT, python::Type::I64}), python::decodeType("({}, i64)"));


    // serialization:
    python::lockGIL();
    PyObject* emptyDict = PyDict_New();
    Row r = python::pythonToRow(emptyDict);
    PyObject* deserialized = python::rowToPython(r, true);
    ASSERT_TRUE(PyDict_Check(deserialized));
    EXPECT_EQ(PyDict_Size(deserialized), 0);
    EXPECT_EQ(PyDict_Size(emptyDict), 0);
    python::unlockGIL();

    // Tuplex based serialization/deserialization
    Serializer s;
    char buf[1024];
    memset(buf, 0 , 1024);
    s.append("{}", python::Type::EMPTYDICT);
    s.serialize(buf, 1024);

    // deserialize
    Row r2 = Row::fromMemory(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::EMPTYDICT)), buf, 1024);

    EXPECT_EQ(r2.toPythonString(), "({},)");

    Context c(microTestOptions());

    // special case, single empty dict?
    auto v =  c.parallelize({Row("Snowwhite"), Row("Huntsman")}, {"name"})
               .map(UDF("lambda t: {}"))
               .collectAsVector();

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), "({},)");
    EXPECT_EQ(v[1].toPythonString(), "({},)");

    // check empty dict len({}) == 0! ==> test for this as well!
    auto v2 = c.parallelize({Row(Field::empty_dict())})
               .map(UDF("lambda x: len(x)")).collectAsVector();

    ASSERT_EQ(v2.size(), 1);
    EXPECT_EQ(v2[0], Row(0));

    // check also key error always produced on empty dict access! ==> with resolve...

    // .popitem() generates KeyError
    // .pop(val) KeyError
    // ==> left for later testing because it's a bit more complicated...
#warning "implement fast, special functions for empty dict..."
}