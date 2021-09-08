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
#include <Context.h>
#include "TestUtils.h"
#include <random>


class DataSetTest : public PyTest {};

TEST_F(DataSetTest, MixedTransform) {
    using namespace tuplex;

    ContextOptions co = testOptions();
    co.set("tuplex.partitionSize", "100B");
    co.set("tuplex.executorMemory", "1MB");
    co.set("tuplex.useLLVMOptimizer", "false");

    Context c(co);
    Row row1(Tuple(10.0, "This"));
    Row row2(Tuple(20.0, "is"));
    Row row3(Tuple(30.0, "a"));
    Row row4(Tuple(40.0, "test"));
    Row row5(Tuple(50.0, "!"));

    auto v = c.parallelize({row1, row2, row3, row4, row5})
            .filter(UDF("lambda x: x[0] > 25.0"))
            .map(UDF("lambda x: x[1]")).collectAsVector();

    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].getString(0), "a");
    EXPECT_EQ(v[1].getString(0), "test");
    EXPECT_EQ(v[2].getString(0), "!");
}

TEST_F(DataSetTest, LenOptionCall) {
    using namespace tuplex;
    Context c(microTestOptions());

    std::string test_str = "Hello world";
    auto f_str = Field(test_str);
    Row row1({f_str});
    auto v1 = c.parallelize(std::vector<Row>{row1, row1}).map(UDF("lambda x: len(x)")).collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].getInt(0), test_str.length());

    auto f_dict = Field::from_str_data(std::string("{\"a\":30,\"b\":10}"), python::Type::makeDictionaryType(python::Type::STRING, python::Type::I64));
    Row row2({f_dict});
    auto v2 = c.parallelize(std::vector<Row>{row2, row2}).map(UDF("lambda x: len(x)")).collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].getInt(0), 2);

    auto f_list = Field(List::from_vector({Field(10.0), Field(20.0)}));
    Row row3({f_list});
    auto v3 = c.parallelize(std::vector<Row>{row3, row3}).map(UDF("lambda x: len(x)")).collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].getInt(0), 2);

    auto f_tuple = Field(Tuple(1, 2, 3));
    Row row4({f_tuple});
    auto v4 = c.parallelize(std::vector<Row>{row4, row4}).map(UDF("lambda x: len(x)")).collectAsVector();
    ASSERT_EQ(v4.size(), 2);
    EXPECT_EQ(v4[0].getInt(0), 3);

    // option versions of str, dict, list, tuple
    f_str.makeOptional();
    Row row1b({f_str});
    auto v1b = c.parallelize(std::vector<Row>{row1b, row1b}).map(UDF("lambda x: len(x)")).collectAsVector();
    ASSERT_EQ(v1b.size(), 2);
    EXPECT_EQ(v1b[0].getInt(0), test_str.length());

    f_dict.makeOptional();
    Row row2b({f_dict});
    auto v2b = c.parallelize(std::vector<Row>{row2b, row2b}).map(UDF("lambda x: len(x)")).collectAsVector();
    ASSERT_EQ(v2b.size(), 2);
    EXPECT_EQ(v2b[0].getInt(0), 2);

    f_list.makeOptional();
    Row row3b({f_list});
    auto v3b = c.parallelize(std::vector<Row>{row3b, row3b}).map(UDF("lambda x: len(x)")).collectAsVector();
    ASSERT_EQ(v3b.size(), 2);
    EXPECT_EQ(v3b[0].getInt(0), 2);

    // can't test because Row.cc implementation for serializing b.c. serialization of Opt[tuple] is not working yet.
    // f_tuple.makeOptional();
    // Row row4b({f_tuple});
    // auto v4b = c.parallelize(std::vector<Row>{row4b, row4b}).map(UDF("lambda x: len(x)")).collectAsVector();
    // ASSERT_EQ(v4b.size(), 2);
    // EXPECT_EQ(v4b[0].getInt(0), 3);
}


TEST_F(DataSetTest, parallelizeAndCollect) {
    using namespace tuplex;

    Context c(testOptions());
    DataSet& ds = c.parallelize({1, 2, 3, 4});

    auto v = ds.collectAsVector();
    ASSERT_EQ(v.size(), 4);

    // order dependent check!
    for(int i = 0; i < 4; i++)
        EXPECT_EQ(v[i].getInt(0), i + 1);
}

TEST_F(DataSetTest, parallelizeAndTake) {
    using namespace tuplex;

    Context c(testOptions());
    DataSet& ds = c.parallelize({1, 2, 3, 4});

    auto v = ds.takeAsVector(2);
    ASSERT_EQ(v.size(), 2);

    // order dependent check!
    for(int i = 0; i < 2; i++)
    EXPECT_EQ(v[i].getInt(0), i + 1);
}

TEST_F(DataSetTest, parallelizeAndTakeLarge) {
    using namespace tuplex;
    using namespace std;


    Context c(testOptions());
    vector<Row> ref;
    vector<Row> data;
    size_t N = 1000;
    size_t limit = 10;

    ASSERT_LE(limit, N); // must be the case here for the testing logic below...

    for(int i = 0; i < N; ++i) {
        data.push_back(Row(i));
        if(i < limit)
            ref.push_back(Row(i * i));
    }

    auto v = c.parallelize(data).map(UDF("lambda x: x * x"))
            .takeAsVector(limit);

    ASSERT_EQ(v.size(), limit);
    for(int i = 0; i < limit; ++i) {
        EXPECT_EQ(v[i], ref[i]);
    }

}


//// large dataset test (multiple partitions, this is > 32MB of data)
//// uncomment this, if tests should run faster...
//TEST(DataSet, parallelizeAndCollectLarge) {
//    using namespace tuplex;
//
//    std::vector<int64_t> vLarge;
//
//    int num = 1048576 * 5 / 8;
//
//    // memory needed:
//    std::cout<<"memory needed for this test: "<<sizeToMemString(num * sizeof(int64_t))<<std::endl;
//
//    for(int64_t i = 0; i < num; i++) {
//        vLarge.push_back(i);
//    }
//
//    Context c;
//    DataSet& ds = c.parallelize(vLarge.begin(), vLarge.end());
//    auto v = ds.collect();
//
//    EXPECT_EQ(v.size(), num);
//    for(int i = 0; i < num; i++) {
//    EXPECT_EQ(v[i].getInt(0), i);
//    }
//}


TEST_F(DataSetTest, filterAllWithEmptyPartitions) {
    using namespace tuplex;

    std::vector<int64_t> vLarge = {100, 200, 300, 400, 500,
                                   600, 500, 400, 300, 200,
                                   600, 500, 400, 300, 200,
                                   600, 500, 400, 300, 200,
                                   600, 500, 400, 300, 200,
                                   600, 500, 400, 300, 200,
                                   600, 500, 400, 300, 200,
                                   600, 500, 400, 300, 200,
                                   600, 500, 400, 300, 200};


    // idea of this test is to filter away a complete partition!
    ContextOptions co = testOptions();
    co.set("tuplex.partitionSize", "100B");
    co.set("tuplex.executorMemory", "1MB");
    co.set("tuplex.useLLVMOptimizer", "false");
    Context c(co);

    DataSet& ds = c.parallelize(vLarge.begin(), vLarge.end())
            .filter(UDF("lambda x: x > 1000"));
    auto v = ds.collectAsVector();

    for(auto el : v)
        std::cout<<el.toPythonString()<<std::endl;

    EXPECT_EQ(v.size(), 0);
}


TEST_F(DataSetTest, mapWithExceptions) {
    using namespace tuplex;

    // Python version of this is
    //c.parallelize([(0, 0), (-1, 0), (1, 0), (42, 1)]).map(lambda x: x[0] / x[1]).collect()
    //assert res == [42]

    Row row1(0, 0); // (0, 0)
    Row row2(-1,0); // (-1, 0)
    Row row3(1, 0); // (1, 0)
    Row row4(42,1); // (0, 0)

    // This produces errors
//    Row row1(Tuple(0, 0)); // (0, 0)
//    Row row2(Tuple(-1,0)); // (-1, 0)
//    Row row3(Tuple(1, 0)); // (1, 0)
//    Row row4(Tuple(42,1)); // (0, 0)

    Context c(testOptions());

    auto v = c.parallelize({row1, row2, row3, row4})
              .map(UDF("lambda x: x[0] / x[1]"))
              .collectAsVector();

    EXPECT_EQ(v.size(), 1);
    auto res = v.front();
    EXPECT_EQ(res.getDouble(0), 42.0); // div return type in Python3 is float
}


TEST_F(DataSetTest, StringComparison) {
    using namespace tuplex;

    Row row1({Field("Hello")});
    Row row2({Field("hello")});

    Context c(testOptions());

    auto v = c.parallelize({row1, row2}).map(UDF("lambda x: x == 'hello'")).collectAsVector();

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].getBoolean(0), false);
    EXPECT_EQ(v[1].getBoolean(0), true);
}

TEST_F(DataSetTest, SimpleIfElse) {
    using namespace tuplex;


    auto code = "def test(x):\n"
                "    if x % 2 == 0:\n"
                "        return 'even'\n"
                "    else:\n"
                "        return 'odd'\n"
                "        ";

    Context c(microTestOptions());

    auto v = c.parallelize({Row(1), Row(2), Row(3), Row(4), Row(5)}).map(UDF(code)).collectAsVector();

    ASSERT_EQ(v.size(), 5);
    EXPECT_EQ(v[0], Row("odd"));
    EXPECT_EQ(v[1], Row("even"));
    EXPECT_EQ(v[2], Row("odd"));
    EXPECT_EQ(v[3], Row("even"));
    EXPECT_EQ(v[4], Row("odd"));
}

TEST_F(DataSetTest, Haversine) {
    using namespace tuplex;

    // Haversine formula test code to be used within the compiler...
    auto code = "def haversine(lat1, lon1, lat2, lon2):\n"
                "    r = 6378137.0\n"
                "    lathalfdiff = 0.5 * (lat2 - lat1)\n"
                "    lonhalfdiff = 0.5 * (lon2 - lon1)\n"
                "    sinlat = sin(lathalfdiff)\n"
                "    sinlon = sin(lonhalfdiff)\n"
                "    h = sinlat**2 + cos(lat1) * cos(lat2) * sinlon**2\n"
                "    return 2.0 * r * asin(sqrt(h))";

    Context c(microTestOptions());
    ClosureEnvironment ce;
    ce.fromModuleImport("math", "sin")
            .fromModuleImport("math", "cos")
            .fromModuleImport("math", "asin")
            .fromModuleImport("math", "sqrt");
    auto v = c.parallelize({Row(1.0, 2.0, 3.0, 4.0), Row(-2.0, -1.0, 0.0, 1.0)}).map(UDF(code, "", ce)).collectAsVector();
}

TEST_F(DataSetTest, FilterAll) {
    using namespace tuplex;

    // c = Context()
    // ds = c.parallelize([1, 2, 3, 4, 5])
    // res = ds.filter(lambda x: x > 10).collect()
    // assert res == []

    Context c(microTestOptions());
    auto res = c.parallelize({Row(1), Row(2), Row(3), Row(4), Row(5)}).filter(UDF("lambda x: x > 10")).collectAsVector();
    ASSERT_EQ(res.size(), 0);
}

TEST_F(DataSetTest, BitNeg) {
    using namespace tuplex;

    Context c(microTestOptions());
    auto res = c.parallelize({Row(0), Row(1), Row(2)})
                .map(UDF("lambda x: ~x")).collectAsVector();
    ASSERT_EQ(res.size(), 3);
    EXPECT_EQ(res[0].toPythonString(), Row(-1).toPythonString());
    EXPECT_EQ(res[1].toPythonString(), Row(-2).toPythonString());
    EXPECT_EQ(res[2].toPythonString(), Row(-3).toPythonString());
}

TEST_F(DataSetTest, NullCompare) {
    using namespace tuplex;

    Context c(microTestOptions());
    auto res = c.parallelize({Row(option<int64_t>(0)), Row(option<int64_t>::none)})
            .map(UDF("lambda x: x == None")).collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), Row(false).toPythonString());
    EXPECT_EQ(res[1].toPythonString(), Row(true).toPythonString());
}

TEST_F(DataSetTest, SingleColWithCol) {
    using namespace tuplex;
    using namespace std;
    Context c(microTestOptions());
    auto res = c.parallelize({Row(option<int64_t>(0)), Row(option<int64_t>::none)}, vector<string>{"A"})
            .withColumn("B", UDF("lambda x: str(x) + '_str'")).collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), Row(0, "0_str").toPythonString());
    EXPECT_EQ(res[1].toPythonString(), Row(option<int64_t>::none, "None_str").toPythonString());

}

TEST_F(DataSetTest, StrConvEmptyTuple) {
    using namespace tuplex;
    using namespace std;
    Context c(microTestOptions());

    c.parallelize({Row(Tuple())}).map(UDF("lambda x: str(x)")).show();
    c.parallelize({Row(Field::empty_dict())}).map(UDF("lambda x: str(x)")).show();

    c.parallelize({Row(Field::null(), Field::empty_tuple(), Field::empty_dict())})
     .map(UDF("lambda a, b, c: (str(a), str(b), str(c))")).collect();

}

TEST_F(DataSetTest, TruthTestForFilter) {
    using namespace tuplex;
    using namespace std;
    Context c(microTestOptions());

    // test for https://github.com/LeonhardFS/Tuplex/issues/102
    // i.e., when using filter should be able to also use functions resulting in a truth-test
    auto v = c.parallelize({Row(option<double>(20.0)),
                            Row(option<double>::none),
                            Row(option<double>(40.0))})
              .filter(UDF("lambda x: x")).collectAsVector();
    ASSERT_EQ(v.size(), 2);
    EXPECT_DOUBLE_EQ(v[0].getDouble(0), 20.0);
    EXPECT_DOUBLE_EQ(v[1].getDouble(0), 40.0);

    // check with another type which would eval always to false (empty dataset)
    auto v2 = c.parallelize({Row(Field::empty_list())}).filter(UDF("lambda x: x")).collectAsVector();
    ASSERT_EQ(v2.size(), 0);
    auto v3 = c.parallelize({Row(20)}).filter(UDF("lambda x: ()")).collectAsVector();
    ASSERT_EQ(v3.size(), 0);
    auto v4 = c.parallelize({Row(42)}).filter(UDF("lambda x: {}")).collectAsVector();
    ASSERT_EQ(v4.size(), 0);

    // check for individual column as filter out indicator
    auto v5 = c.parallelize({Row(10, 30), Row(10, 0)})
               .filter(UDF("lambda a, b: b"))
               .collectAsVector();
    ASSERT_EQ(v5.size(), 1);
    EXPECT_EQ(v5[0].toPythonString(), "(10,30)");

    // check negation
    auto v6 = c.parallelize({Row(option<double>(20.0)),
                            Row(option<double>::none),
                            Row(option<double>(40.0))})
            .filter(UDF("lambda x: not x")).collectAsVector();
    ASSERT_EQ(v6.size(), 1);
    EXPECT_EQ(v6[0].toPythonString(), "(None,)");
}

TEST_F(DataSetTest, withColumnUnpackParams) {
    // c.parallelize([(1, 2), (3, 2)]).withColumn('newcol',
    //lambda a, b: (a + b)/10).collect()
    // reported in https://github.com/LeonhardFS/Tuplex/issues/55 to not work
    using namespace tuplex;
    using namespace std;
    Context c(microTestOptions());

    auto v = c.parallelize({Row(1, 2), Row(3, 2)})
              .withColumn("newcol", UDF("lambda a, b: (a+b)/10"))
              .collectAsVector();
    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), "(1,2,0.30000)");
    EXPECT_EQ(v[1].toPythonString(), "(3,2,0.50000)");
}


TEST_F(DataSetTest, IfElseExpressionDifferentTypes) {
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    auto v = c.parallelize({Row(1, 0.5), Row(2, 1.7)}).map(UDF("lambda a, b: a if a > b else b")).collectAsVector();

    ASSERT_EQ(v.size(), 2);
    if(c.getOptions().AUTO_UPCAST_NUMBERS()) {
        EXPECT_DOUBLE_EQ(v[0].getDouble(0), 1.0);
        EXPECT_DOUBLE_EQ(v[1].getDouble(0), 2.0);
    } else {
        EXPECT_EQ(v[0].getInt(0), 1);
        EXPECT_EQ(v[1].getInt(0), 2);
    }
}

TEST_F(DataSetTest, BuiltinPowerBool) {
    // test boolean operators
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    auto v = c.parallelize({Row(false, false), Row(true, false),
                               Row(false, true), Row(true, true)}).map(UDF("lambda a, b: a ** b")).collectAsVector();
    ASSERT_EQ(v.size(), 4);
    EXPECT_EQ(v[0].getInt(0), 1);
    EXPECT_EQ(v[1].getInt(0), 1);
    EXPECT_EQ(v[2].getInt(0), 0);
    EXPECT_EQ(v[3].getInt(0), 1);
}

TEST_F(DataSetTest, BuiltinPowerInt) {
    //    // bool to various integer values
    //    // because speculation happens on this, test Row by Row here
    //    // 42 is the dummy from the resolve function!
    //    vector<Row> test{Row(true, -200), Row(false, -200), Row(true, -1), Row(false, -1), Row(true, 0), Row(false, 0), Row(true, 1), Row(false, 1), Row(true, 125), Row(false, 125)};
    //    vector<Row> ref{Row(1.0), Row(42), Row(1.0), Row(42)};

    // test boolean operators
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    // simple integer case, everything well defined...
    // --> majority positive
    auto v1 = c.parallelize({Row(0, 1), Row(1, 2),
                            Row(2, 3), Row(3, 4)}).map(UDF("lambda a, b: a ** b")).collectAsVector();
    ASSERT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0].getInt(0), 0);
    EXPECT_EQ(v1[1].getInt(0), 1);
    EXPECT_EQ(v1[2].getInt(0), 2 * 2 * 2);
    EXPECT_EQ(v1[3].getInt(0), 3 * 3 * 3 * 3);

    // --> majority negative
    auto v2 = c.parallelize({Row(1, -1), Row(1, -2),
                             Row(2, -3), Row(3, -4)}).map(UDF("lambda a, b: a ** b")).collectAsVector();
    ASSERT_EQ(v2.size(), 4);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), 1.0 / (2 * 2 * 2));
    EXPECT_DOUBLE_EQ(v2[3].getDouble(0), 1.0 / (3 * 3 * 3 * 3));

    // zero division check/error
    auto v3 = c.parallelize({Row(0, -10), Row(1, 0), Row(0, 0),
                             Row(2, 3), Row(3, 4)}).map(UDF("lambda a, b: a ** b")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b: 42")).collectAsVector();
    ASSERT_EQ(v3.size(), 5);
    EXPECT_EQ(v3[0].getInt(0), 42);
    EXPECT_EQ(v3[1].getInt(0), 1);
    EXPECT_EQ(v3[2].getInt(0), 1);
    EXPECT_EQ(v3[3].getInt(0), 2 * 2 * 2);
    EXPECT_EQ(v3[4].getInt(0), 3 * 3 * 3 * 3);
}

TEST_F(DataSetTest, PowerOperatorConstExponent) {
    using namespace std;
    using namespace tuplex;

    // test for a couple of results with fixed exponent
    int N = 10;
    for(int i = -10; i < 10; ++i) {
        cout<<"Testing exponent="<<i<<endl;

        // Test case 1: integers
        // ref values (randomized)
        std::default_random_engine generator;
        vector<Row> input_ints;
        std::string input_ints_py_list= "[";
        for(int j = 0; j < N; ++j) {
            // always insert a 0 as special case
            if(j == 0) {
                input_ints.push_back(Row(0));
                input_ints_py_list += "0,";
            }
            std::uniform_int_distribution<int64_t> distribution(-9,9);
            int64_t val = distribution(generator);
            input_ints.push_back(Row(val));
            input_ints_py_list += std::to_string(val) + ",";
        }
        input_ints_py_list += "]";
        // print out as array
        cout<<input_ints_py_list<<endl;
        cout<<"test"<<endl;

        // compute python result (incl. error handling via 42!)
        auto py_code_template = "import functools\n"
                       "\n"
                       "def f(x, p):\n"
                       "    try:\n"
                       "        return x ** p\n"
                       "    except ZeroDivisionError:\n"
                       "        return 42\n"
                       "\n"
                       "L = list(map(functools.partial(f, p=";
        stringstream py_code;
        py_code<<py_code_template<<i<<"), "<<input_ints_py_list<<"))"<<endl;
        python::lockGIL();
        auto code = py_code.str();
        auto res_obj = python::runAndGet(code, "L");
        PyObject_Print(res_obj, stdout, 0);
        cout<<endl;
        python::unlockGIL();
        cout<<"hello"<<endl;
        // Test case 2: floats
    }
}

TEST_F(DataSetTest, BuiltinPowerWithComplexResult) {
    //         for exp in np.arange(-5.0, 6.0, 0.5):
    //            exp = float(exp)  # numpy datatype not support yet, hence explicit cast to python float type.
    //            ref = list(map(functools.partial(f, p=exp), data))
    //
    //            # compute via tuplex
    //            ds = c.parallelize(data).map(lambda x: x ** exp)
    //            res = ds.resolve(ZeroDivisionError, lambda x: 42).collect()
    using namespace std;
    using namespace tuplex;

    Context c(microTestOptions());

    auto& ds = c.parallelize({Row(0), Row(-9), Row(-8), Row(-6), Row(3), Row(5), Row(8)});

    // float exponent
    auto res = ds.map(UDF("lambda x: x ** -5.5"))
                 .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 42"))
                 .collectAsVector();

    ASSERT_EQ(res.size(), 7);
    // to python string!
}

TEST_F(DataSetTest, BuiltinPowerFloatDataNullexponent) {
    //         for exp in np.arange(-5.0, 6.0, 0.5):
    //            exp = float(exp)  # numpy datatype not support yet, hence explicit cast to python float type.
    //            ref = list(map(functools.partial(f, p=exp), data))
    //
    //            # compute via tuplex
    //            ds = c.parallelize(data).map(lambda x: x ** exp)
    //            res = ds.resolve(ZeroDivisionError, lambda x: 42).collect()
    using namespace std;
    using namespace tuplex;

    Context c(microTestOptions());

    auto& ds = c.parallelize({Row(0.0), Row(-9.0), Row(-8.0), Row(-6.0), Row(3.0), Row(5.0), Row(8.0)});

    // float data, but integer exponent.
    auto res = ds.map(UDF("lambda x: x ** 0"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 42"))
            .collectAsVector();

    ASSERT_EQ(res.size(), 7);
    // to python string!
}

TEST_F(DataSetTest, LenEmptyListDictTuple) {
    using namespace std;
    using namespace tuplex;

    Context c(microTestOptions());

    auto res = c.parallelize({Row(Field::empty_list()), Row(Field::empty_list())}).map(UDF("lambda x: len(x)")).collectAsVector();
    EXPECT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].getInt(0), 0);
    EXPECT_EQ(res[1].getInt(0), 0);

    res = c.parallelize({Row(Field::empty_tuple()), Row(Field::empty_tuple())}).map(UDF("lambda x: len(x)")).collectAsVector();
    EXPECT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].getInt(0), 0);
    EXPECT_EQ(res[1].getInt(0), 0);

    res = c.parallelize({Row(Field::empty_dict()), Row(Field::empty_dict())}).map(UDF("lambda x: len(x)")).collectAsVector();
    EXPECT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].getInt(0), 0);
    EXPECT_EQ(res[1].getInt(0), 0);

    res = c.parallelize({Row(0), Row(1)}).map(UDF("lambda x: len([])")).collectAsVector();
    EXPECT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].getInt(0), 0);
    EXPECT_EQ(res[1].getInt(0), 0);

    res = c.parallelize({Row(0), Row(1)}).map(UDF("lambda x: len({})")).collectAsVector();
    EXPECT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].getInt(0), 0);
    EXPECT_EQ(res[1].getInt(0), 0);

    res = c.parallelize({Row(0), Row(1)}).map(UDF("lambda x: len(())")).collectAsVector();
    EXPECT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].getInt(0), 0);
    EXPECT_EQ(res[1].getInt(0), 0);
}