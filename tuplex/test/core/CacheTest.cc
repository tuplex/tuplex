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

class CacheTest : public PyTest {
};

using namespace tuplex;
using namespace std;

TEST_F(CacheTest, MergeInOrderWithFilter) {
    using namespace std;
    using namespace tuplex;

    auto fileURI = URI(testName + ".csv");

    auto opt = microTestOptions();
    // enable NullValue Optimization
    opt.set("tuplex.useLLVMOptimizer", "true");
    opt.set("tuplex.optimizer.generateParser", "true");
    opt.set("tuplex.executorCount", "0");
    opt.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    opt.set("tuplex.normalcaseThreshold", "0.6");
    opt.set("tuplex.resolveWithInterpreterOnly", "false");

    Context c(opt);

    stringstream ss;
    for (int i = 0; i < 500; ++i) {
        if (i % 4 == 0) {
            ss << ",-1\n";
        } else {
            ss << to_string(i) << "," << to_string(i) << "\n";
        }
    }
    stringToFile(fileURI, ss.str());

    auto ds_cached = c.csv(fileURI.toPath()).cache();

    auto res = ds_cached.filter(UDF("lambda x, y: y % 3 != 0")).map(UDF("lambda x, y: y")).collectAsVector();

    std::vector<Row> expectedOutput;
    for (int i = 0; i < 500; ++i) {
        if (i % 4 == 0) {
            expectedOutput.push_back(Row(-1));
        } else if (i % 3 != 0) {
            expectedOutput.push_back(Row(i));
        }
    }

    ASSERT_EQ(expectedOutput.size(), res.size());
    for (int i = 0; i < expectedOutput.size(); ++i) {
        EXPECT_EQ(expectedOutput[i].toPythonString(), res[i].toPythonString());
    }
}

TEST_F(CacheTest, MergeInOrder) {
    using namespace std;
    using namespace tuplex;

    auto fileURI = URI(testName + ".csv");

    auto opt = microTestOptions();
    // enable NullValue Optimization
    opt.set("tuplex.useLLVMOptimizer", "true");
    opt.set("tuplex.optimizer.generateParser", "true");
    opt.set("tuplex.executorCount", "0");
    opt.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    opt.set("tuplex.normalcaseThreshold", "0.6");
    opt.set("tuplex.resolveWithInterpreterOnly", "false");

    Context c(opt);

    auto size = 403;
    auto mod1 = 5;
    auto mod2 = 6;

    stringstream ss;
    for (int i = 0; i < size; ++i) {
        if (i % mod1 == 0 || i % mod2 == 0) {
            ss << ",,,,-1\n";
        } else {
            ss << to_string(i) << "," << to_string(i) << "," << to_string(i) << "," << to_string(i) << "," << to_string(i) << "\n";
        }
    }

    stringToFile(fileURI, ss.str());

    auto ds_cached = c.csv(fileURI.toPath()).cache();

    auto res = ds_cached.map(UDF("lambda x: x[4]")).collectAsVector();
    printRows(res);

    ASSERT_EQ(res.size(), size);
    for (int i = 0; i < res.size(); ++i) {
        if (i % mod1 == 0 || i % mod2 == 0) {
            EXPECT_EQ(res[i].toPythonString(), Row(-1).toPythonString());
        } else {
            EXPECT_EQ(res[i].toPythonString(), Row(i).toPythonString());
        }
    }
}

// Note: only this test here fails...
TEST_F(CacheTest, SimpleCSVLoad) {
    using namespace std;
    auto fileURI = URI(testName + ".csv");
    auto opt = microTestOptions();

    // first, deactivate logical optimizations and make caching work as is...
    opt.set("tuplex.csv.selectionPushdown", "false");
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt.set("tuplex.optimizer.filterPushdown", "false");
    opt.set("tuplex.optimizer.sharedObjectPropagation", "false");
    opt.set("tuplex.optimizer.mergeExceptionsInOrder", "false");

    Context c(opt);
    // write a simple csv file & cache before running pipeline...
    stringToFile(fileURI, "A,B\n1,2\n3,4\n");

    Timer timer;

    auto &dsCached = c.csv(fileURI.toPath()).cache(); //! this causes the issue...

    cout<<"cached data in: "<<timer.time()<<"s"<<endl;

    // run query in addition
    auto res = dsCached.map(UDF("lambda a, b: {'A_squared' : a * a}")).collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), "(1,)");
    EXPECT_EQ(res[1].toPythonString(), "(9,)");
}

// next test: logical optimization, i.e. pushdowns!
// things to test for:
// => can't project stuff down .cache()
// => can't push filter below .cache()
TEST_F(CacheTest, LogicalOptCSVLoad) {
    using namespace std;
    auto fileURI = URI(testName + ".csv");
    auto opt = microTestOptions();

    // first, deactivate logical optimizations and make caching work as is...
    opt.set("tuplex.csv.selectionPushdown", "true");
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt.set("tuplex.optimizer.filterPushdown", "true");
    opt.set("tuplex.optimizer.sharedObjectPropagation", "false");
    opt.set("tuplex.optimizer.mergeExceptionsInOrder", "false");

    Context c(opt);
    // write a simple csv file & cache before running pipeline...
    stringToFile(fileURI, "A,B,C,D\n1,2,0,-1\n3,4,0,-1\n-2,-2,0,-1\n");

    Timer timer;
    auto &dsCached = c.csv(fileURI.toPath()).cache();

    cout<<"cached data in: "<<timer.time()<<"s"<<endl;

    // run query in addition
    auto res = dsCached.selectColumns(vector<string>{"A", "B"}).filter(UDF("lambda x: x[1] > 0")).map(UDF("lambda a, b: {'A_squared' : a * a}"))
            .collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), "(1,)");
    EXPECT_EQ(res[1].toPythonString(), "(9,)");

    auto &dsCachedII = c.csv(fileURI.toPath()).selectColumns(vector<string>{"A", "B", "C"}).cache();

    // make sure the tree was not weirdly altered
    auto resII = dsCached.collectAsVector();
    ASSERT_EQ(resII.size(), 3);
    EXPECT_EQ(resII[0].toPythonString(), "(1,2,0,-1)");
}

// Now, time to make null-value optimization work
// => this is a bit tricky b.c. we need to use common case/general case types correctly
// and pass exceptions to resolve functor in Localbackend...
TEST_F(CacheTest, NullValueOptIf) {
    using namespace tuplex;
    using namespace std;

    auto opt_nopt = microTestOptions();
    // enable NullValue Optimization
    opt_nopt.set("tuplex.useLLVMOptimizer", "true");
    opt_nopt.set("tuplex.optimizer.generateParser", "true");
    opt_nopt.set("tuplex.executorCount", "0");
    opt_nopt.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opt_nopt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    opt_nopt.set("tuplex.normalcaseThreshold", "0.6"); // set higher, so optimization is more aggressive.

    // whether to use compiled functor or not
    opt_nopt.set("tuplex.resolveWithInterpreterOnly", "false");

    Context c(opt_nopt);

    // test code with simple if branch
    auto code_I = "def test(a, b):\n"
                  "\tif a:\n"
                  "\t\treturn 10\n"
                  "\telse:\n"
                  "\t\treturn b\n";

    // null-value opt does not work with parallelize, b.c. there the type is known
    // so it will be rejected! However, more interesting case when reading from files!
    std::stringstream ss;
    ss<<"A, B\n";
    for(int i = 0; i < 10; ++i)
        ss<<"10,20\n";
    ss<<",20\n"; // single non-conforming row, which gets cached separately!
    stringToFile(URI(testName + ".csv"), ss.str());

    // => cache materializes both normal and exceptional case in memory
    // => this can be then used to speed up processing!
    auto& ds_cached = c.csv(testName + ".csv").cache();

    cout<<"cache done"<<endl;
    auto vIA = ds_cached.map(UDF(code_I)).collectAsVector();

    for(const auto& r : vIA)
        std::cout<<r.toPythonString()<<std::endl;
    ASSERT_EQ(vIA.size(), 11);
}

TEST_F(CacheTest, NullValueOptIfAlt) {
    // same test as above, but with interpreter to resolve exceptions...
    using namespace tuplex;
    using namespace std;

    auto opt_nopt = microTestOptions();
    // enable NullValue Optimization
    opt_nopt.set("tuplex.useLLVMOptimizer", "true");
    opt_nopt.set("tuplex.optimizer.generateParser", "true");
    opt_nopt.set("tuplex.executorCount", "0");
    opt_nopt.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opt_nopt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    opt_nopt.set("tuplex.normalcaseThreshold", "0.6"); // set higher, so optimization is more aggressive.

    // whether to use compiled functor or not
    opt_nopt.set("tuplex.resolveWithInterpreterOnly", "true");

    Context c(opt_nopt);

    // test code with simple if branch
    auto code_I = "def test(a, b):\n"
                  "\tif a:\n"
                  "\t\treturn 10\n"
                  "\telse:\n"
                  "\t\treturn b\n";

    // null-value opt does not work with parallelize, b.c. there the type is known
    // so it will be rejected! However, more interesting case when reading from files!
    std::stringstream ss;
    ss<<"A, B\n";
    for(int i = 0; i < 10; ++i)
        ss<<"10,20\n";
    ss<<",20\n"; // single non-conforming row, which gets cached separately!
    stringToFile(URI(testName + ".csv"), ss.str());

    // => cache materializes both normal and exceptional case in memory
    // => this can be then used to speed up processing!
    auto& ds_cached = c.csv(testName + ".csv").cache();

    cout<<"cache done"<<endl;
    auto vIA = ds_cached.map(UDF(code_I)).collectAsVector();

    for(const auto& r : vIA)
        std::cout<<r.toPythonString()<<std::endl;
    ASSERT_EQ(vIA.size(), 11);
}

//TEST_F(CacheTest, SpecializedVsNonSpecialized) {
//    ASSERT_TRUE(false); // test this here...
//}
//
//TEST_F(CacheTest, CheckWithPythonObjects) {
//    ASSERT_TRUE(false); //check this here...
//}


//// @TODO: fix this, i.e. case when null value vs. str join in null-value opt
////        does not lead to any result
////        ==> should work somehow...
//TEST_F(CacheTest, NullValueOptJoinWithHashResolve) {
//
//    // NOTE: this test here fails, because resolve tasks with hashtable endpoint are not supported yet.
//    // => setting threshold .6 produces null key column
//
//    // check whether null-value optimization works with a join where one parent is cached!
//    using namespace tuplex;
//    using namespace std;
//
//    auto opt_nopt = microTestOptions();
//    // enable NullValue Optimization
//    opt_nopt.set("tuplex.useLLVMOptimizer", "true");
//    opt_nopt.set("tuplex.optimizer.generateParser", "true");
//    opt_nopt.set("tuplex.executorCount", "0");
//    opt_nopt.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
//    opt_nopt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
//    opt_nopt.set("tuplex.normalcaseThreshold", "0.6"); // set lower, so optimization is more aggressive which will result in more exceptions typically.
//    Context c(opt_nopt);
//
//    // use flights example
//    Timer timer;
//    string path = "../resources/pipelines/flights/GlobalAirportDatabase.txt"; // 760KB or so
//    auto& dsAirports = c.csv(path,
//                             vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
//                                            "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
//                                            "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
//                             option<bool>::none, option<char>(':'), '"', vector<string>{"", "N/A"}).cache();
//    cout<<"caching airports in memory took: "<<timer.time()<<"s"<<endl;
//
//    // mini merge (build forced to right!)
//    auto& ds = c.parallelize({Row("ATL", "FRA", 20), Row("FRA", "BOS", 10), Row("JFK", "ATL", 5), Row("BOS", "JFK", 12)},
//                             vector<string>{"Origin", "Dest", "Delay"}); // 4 rows
//
//    ds.leftJoin(dsAirports, string("Origin"), string("IATACode")).show();
//}

TEST_F(CacheTest, NullValueOptJoinNoCachedErrors) {

    // NOTE: here the cached file produces no errors for the normal-case, hence we can generate
    //       efficient code for the join

    // check whether null-value optimization works with a join where one parent is cached!
    using namespace tuplex;
    using namespace std;

    auto opt_nopt = microTestOptions();
    // enable NullValue Optimization
    opt_nopt.set("tuplex.useLLVMOptimizer", "true");
    opt_nopt.set("tuplex.optimizer.generateParser", "true");
    opt_nopt.set("tuplex.executorCount", "0");
    opt_nopt.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opt_nopt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    opt_nopt.set("tuplex.normalcaseThreshold", "0.9");
    Context c(opt_nopt);

    // use flights example
    Timer timer;
    string path = "../resources/pipelines/flights/GlobalAirportDatabase.txt"; // 760KB or so
    auto& dsAirports = c.csv(path,
                             vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                            "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                            "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                             option<bool>::none, option<char>(':'), '"', vector<string>{"", "N/A"}).cache();
    cout<<"caching airports in memory took: "<<timer.time()<<"s"<<endl;

    // mini merge (build forced to right!)
    auto& ds = c.parallelize({Row("ATL", "FRA", 20), Row("FRA", "BOS", 10), Row("JFK", "ATL", 5), Row("BOS", "JFK", 12)},
                             vector<string>{"Origin", "Dest", "Delay"}); // 4 rows

    ds.leftJoin(dsAirports, string("Origin"), string("IATACode")).show();
}

//// @TODO: test optimizations with cached flights query (zillow trivially should work when above stuff works)
//TEST_F(CacheTest, Parallelize) {
//
//}


// multiple caches after each other...
// i.e. cache().cache().cache() => should still work!


// i.e. for the testfile, use
// 10
// 10
// NULL
// badrow

// => 10, 10 is i64 common case
// => NULL is Option[i64] general case
// => badrow is python case (don't know what it's supposed to mean)


//@TODO: future API: provide custom data