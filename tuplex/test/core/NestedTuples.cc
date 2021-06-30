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
#include <Context.h>

TEST(NestedTuples, TupleTree) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    auto t = Tuple(1, Tuple("hello"));
    auto tt = tupleToTree(t);

    EXPECT_TRUE(tt.get(vector<int>{0}).getType() == python::Type::I64);
    EXPECT_TRUE(tt.get(vector<int>{1, 0}).getType() == python::Type::STRING);

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, SimpleNesting) {
    // basic test to check whether mapping double value to tuple works...

    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    auto f64t = python::Type::makeTupleType({python::Type::F64});

    Context c(testOptions());
    auto v = c.parallelize({Row(10), Row(21), Row(-78)})
            .map(UDF("lambda x: (x,)"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "((10,),)");
    EXPECT_EQ(v[1].toPythonString(), "((21,),)");
    EXPECT_EQ(v[2].toPythonString(), "((-78,),)");

    python::lockGIL();
    python::closeInterpreter();
}

// identity applied to basic column
TEST(NestedTuples, TupleIdentityI) {
    // basic test to check whether mapping double value to tuple works...

    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    auto v = c.parallelize({Row(10), Row(21), Row(-78)})
            .map(UDF("lambda x: x"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "(10,)");
    EXPECT_EQ(v[1].toPythonString(), "(21,)");
    EXPECT_EQ(v[2].toPythonString(), "(-78,)");

    python::lockGIL();
    python::closeInterpreter();
}

// identity func applied to tuple
TEST(NestedTuples, TupleIdentityII) {
    // basic test to check whether mapping double value to tuple works...

    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    auto v = c.parallelize({Row(10), Row(21), Row(-78)})
            .map(UDF("lambda x: (x, x+1)"))
            .map(UDF("lambda x: x")) // identity should just map the tuple
            .collectAsVector();

    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "(10,11)");
    EXPECT_EQ(v[1].toPythonString(), "(21,22)");
    EXPECT_EQ(v[2].toPythonString(), "(-78,-77)");

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, TupleDeepNestedI) {
    // basic test to check whether mapping double value to tuple works...

    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    auto v = c.parallelize({Row(10), Row(21), Row(-78)})
            .map(UDF("lambda x: (x, 'hello world', (x * x, (((x,),),), x + 1, (10, 3.10000)))"))
            .map(UDF("lambda x: x")) // identity should just map the tuple
            .collectAsVector();

    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "(10,'hello world',(100,(((10,),),),11,(10,3.10000)))");
    EXPECT_EQ(v[1].toPythonString(), "(21,'hello world',(441,(((21,),),),22,(10,3.10000)))");
    EXPECT_EQ(v[2].toPythonString(), "(-78,'hello world',(6084,(((-78,),),),-77,(10,3.10000)))");

    python::lockGIL();
    python::closeInterpreter();
}


TEST(NestedTuples, TupleBasicNestedI) {
    // basic test to check whether mapping double value to tuple works...

    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    auto v = c.parallelize({Row(10), Row(21), Row(-78)})
            .map(UDF("lambda x: (1+x,)"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "((11,),)");
    EXPECT_EQ(v[1].toPythonString(), "((22,),)");
    EXPECT_EQ(v[2].toPythonString(), "((-77,),)");

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, TupleBasicNestedII) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    Row row(10, Tuple(20), 30);
    auto v = c.parallelize({row})
            .map(UDF("lambda x: (x, x)"))
            .collectAsVector();
    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0].toPythonString(), "((10,(20,),30),(10,(20,),30))");

    python::lockGIL();
    python::closeInterpreter();
}


TEST(NestedTuples, TupleDeepNestedII) {
    // basic test to check whether mapping double value to tuple works...

    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    auto v = c.parallelize({Row(10), Row(21), Row(0)})
            .map(UDF("lambda x: 1+x"))
            .map(UDF("lambda x: (x, x)"))
            .map(UDF("lambda x: (x, x)"))
            .map(UDF("lambda x: (x, x)"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "(((11,11),(11,11)),((11,11),(11,11)))");
    EXPECT_EQ(v[1].toPythonString(), "(((22,22),(22,22)),((22,22),(22,22)))");
    EXPECT_EQ(v[2].toPythonString(), "(((1,1),(1,1)),((1,1),(1,1)))");

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, EmptyTupleI) {
    // basic test to check whether mapping double value to tuple works...

    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    auto v = c.parallelize({Row(10), Row(21), Row(0)})
            .map(UDF("lambda x: ()"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "((),)");
    EXPECT_EQ(v[1].toPythonString(), "((),)");
    EXPECT_EQ(v[2].toPythonString(), "((),)");

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, NestedEmptyTupleII) {
    // basic test to check whether mapping double value to tuple works...

    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    auto v = c.parallelize({Row(10), Row(21), Row(0)})
            .map(UDF("lambda x: ((), 10, ())"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "((),10,())");
    EXPECT_EQ(v[1].toPythonString(), "((),10,())");
    EXPECT_EQ(v[2].toPythonString(), "((),10,())");

    python::lockGIL();
    python::closeInterpreter();
}


TEST(NestedTuples, TupleWithString) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    Row row(20, "hello");

    auto v = c.parallelize({row})
            .map(UDF("lambda x: (x, 10)"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0].toPythonString(), "((20,'hello'),10)");

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, MultiRows) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    Row row1(10, "hello");
    Row row2(20, "world");

    auto v = c.parallelize({row1, row2})
            .map(UDF("lambda x: (x, 'test')"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), "((10,'hello'),'test')");
    EXPECT_EQ(v[1].toPythonString(), "((20,'world'),'test')");

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, MultiParametersTuples) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    Row row1(Tuple(10, 5.1), Tuple(10, 5.0));
    Row row2(Tuple(10, 56.1), Tuple(20, 7.0));

    auto v = c.parallelize({row1, row2})
            .map(UDF("lambda a, b: a"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), "(10,5.10000)");
    EXPECT_EQ(v[1].toPythonString(), "(10,56.10000)");

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, MultiParameters) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    Row row1(Tuple(10, 5.2));
    Row row2(Tuple(20, 7.2));

    auto v = c.parallelize({row1, row2})
            .map(UDF("lambda a, b: a + b"))
            .collectAsVector();

    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), "(15.20000,)");
    EXPECT_EQ(v[1].toPythonString(), "(27.20000,)");

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, MultiInclEmptyString) {
    //c.parallelize([('hello', '', (), ('world',  ()))]).collect()

    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(testOptions());
    Row row1("hello", "", Tuple(), Tuple("world", Tuple()));
    auto v = c.parallelize({row1})
            .collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0].toPythonString(), "('hello','',(),('world',()))");

    python::lockGIL();
    python::closeInterpreter();
}

TEST(NestedTuples, Slicing) {
    //c.parallelize([('hello', '', (), ('world',  ()))]).collect()

    using namespace tuplex;
    python::initInterpreter();
    python::unlockGIL();

    Context c(microTestOptions());

    Row row1_1(Tuple(1, 2, 3));
    Row row1_2(Tuple(4, 5, 6));
    Row row1_3(Tuple(-10, 20, -30));
    auto v1 = c.parallelize({row1_1, row1_2, row1_3})
            .map(UDF("lambda x: x[1:3]"))
            .collectAsVector();

    ASSERT_EQ(v1.size(), 3);
    EXPECT_EQ(v1[0].toPythonString(), "(2,3)");
    EXPECT_EQ(v1[1].toPythonString(), "(5,6)");
    EXPECT_EQ(v1[2].toPythonString(), "(20,-30)");

    Row row2_1(Tuple(1, 2, 3, 5, 7));
    Row row2_2(Tuple(4, 5, 6, 10, 13));
    Row row2_3(Tuple(-10, 20, -30, 25, -50));
    auto v2 = c.parallelize({row2_1, row2_2, row2_3})
            .map(UDF("lambda x: x[4:-10:-2]"))
            .collectAsVector();

    ASSERT_EQ(v2.size(), 3);
    EXPECT_EQ(v2[0].toPythonString(), "(7,3,1)");
    EXPECT_EQ(v2[1].toPythonString(), "(13,6,4)");
    EXPECT_EQ(v2[2].toPythonString(), "(-50,-30,-10)");

    Row row4_1(Tuple(-10, 20, -30, 25, -50));
    auto v4 = c.parallelize({row4_1})
            .map(UDF("lambda x: x[1:3:0]"))
            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: ()"))
            .collectAsVector();
    ASSERT_EQ(v4.size(), 1);
    EXPECT_EQ(v4[0].toPythonString(), "((),)");

    Row row3_1(Tuple(1, 2, 3, 5, 7));
    Row row3_2(Tuple(4, 5, 6, 10, 13));
    Row row3_3(Tuple(-10, 20, -30, 25, -50));
    auto v3 = c.parallelize({row3_1, row3_2, row3_3})
            .map(UDF("lambda x: x[4:-2]"))
            .collectAsVector();
    ASSERT_EQ(v3.size(), 3);
    EXPECT_EQ(v3[0].toPythonString(), "((),)");
    EXPECT_EQ(v3[1].toPythonString(), "((),)");
    EXPECT_EQ(v3[2].toPythonString(), "((),)");

    python::lockGIL();
    python::closeInterpreter();
}

// ((), ("foobar",), 1443, "no", (100, 0))
TEST(NestedTuples, SliceComplex) {
    // from python tests
    using namespace tuplex;
    using namespace std;

    python::initInterpreter();
    python::unlockGIL();

    Context c(microTestOptions());

    auto row = Row(Field::empty_tuple(), Tuple("foobar"), 1443, "no", Tuple(100, 0));

    // quick serialize/deserialize test
    unsigned char buffer[4096];
    row.serializeToMemory(buffer, 4096);
    EXPECT_EQ(Row::fromMemory(row.getSchema(), buffer, 4096).toPythonString(), row.toPythonString());

    Deserializer ds(row.getSchema());
    EXPECT_EQ(row.serializedLength(), ds.inferLength(buffer));

    auto v = c.parallelize({row})
            .map(UDF("lambda x: x[-10:]")).collectAsVector();

    cout<<"schema is: "<<v[0].getSchema().getRowType().desc()<<endl;


    EXPECT_EQ(v[0].toPythonString(), row.toPythonString());

    python::lockGIL();
    python::closeInterpreter();
}