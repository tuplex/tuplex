//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Context.h>
#include "TestUtils.h"
#include <random>

using namespace tuplex;


class Resolve : public PyTest {};

TEST_F(Resolve, BasicMap) {

    // basic zero exception
    Context c(microTestOptions());

//    // this here works
//    auto resWOExc = c.parallelize({Row(10, 0), Row(20, 2), Row(30, 2), Row(40,0)})
//            .map(UDF("lambda a, b : a / b"))
//            .collectAsVector();
//
//    ASSERT_EQ(resWOExc.size(), 2);
//
//    // Note: / in Python3 leads to type f64!
//    EXPECT_EQ(resWOExc[0], Row(10.0));
//    EXPECT_EQ(resWOExc[1], Row(15.0));

    // make sure this works in WebUI as well...
    // now with exception resolution
    auto resWExc = c.parallelize({Row(10, 0), Row(20, 2), Row(30, 2), Row(40,0)})
            .map(UDF("lambda a, b : a / b"))
            //.resolve(ExceptionCode::INDEXERROR, UDF("lambda x: 42.0"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 0.0")) // note the 0.0 so types work!
            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 0.0"))
            .collectAsVector();

    ASSERT_EQ(resWExc.size(), 4);
    EXPECT_EQ(resWExc[0].toPythonString(), Row(0.0).toPythonString());
    EXPECT_EQ(resWExc[1].toPythonString(), Row(10.0).toPythonString());
    EXPECT_EQ(resWExc[2].toPythonString(), Row(15.0).toPythonString());
    EXPECT_EQ(resWExc[3].toPythonString(), Row(0.0).toPythonString());
}

TEST_F(Resolve, BasicPipeline) {

    // operators coming after resolve
    // basic zero exception
    Context c(testOptions());

    // enable number auto upcast
    // .resolve(ExceptionCode::DIVISIONBYZERO, UDF("lambda x: 0")) i.e. if float is expected, this can be safely
    // upcast to int

    // now with exception resolution
    auto resWExc = c.parallelize({Row(10, 0), Row(20, 2), Row(30, 2), Row(40,0)})
            .map(UDF("lambda a, b : a / b"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 0.0"))
            .map(UDF("lambda x: (x, x)")) // [(0,0), (10,10),(15,15),(0,0)]
            .filter(UDF("lambda z: z[0] > 11.0")) // [(15, 15)]
            .map(UDF("lambda y: y[0] * y[0]")) // [225]
            .collectAsVector();

    ASSERT_EQ(resWExc.size(), 1);
    EXPECT_EQ(resWExc[0], Row(225.0));

}

TEST_F(Resolve, RandomizedZeroDivResolve) {
    using namespace std;

    auto conf = microTestOptions();
    conf.set("tuplex.useLLVMOptimizer", "false");
    conf.set("tuplex.allowUndefinedBehavior", "false"); // else zero-div is not an exception.
    Context c(conf);

    //    auto N = 500; // <-- this fails. Probably a problem with out-of-memory writing/reading?
    auto N = 5;

    std::vector<Row> rows;
    std::vector<double> ref;

    auto seed = time(nullptr);
    std::srand(seed);
    //std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(42); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<int64_t> dis(0, 6);

    int num_exceptions = 0;
    for(unsigned i = 0; i < N; ++i) {

        auto a = dis(gen);
        auto b = dis(gen);

        if(b == 0) {
            ref.push_back(0.0);
            num_exceptions++;
        }
        else
            ref.push_back(a / (double)b); // python semantic here is cast to double

        rows.emplace_back(a, b);
    }

    cout<<"num rows: "<<rows.size()<<" (ref: "<<ref.size()<<")"<<endl;
    cout<<"num exceptions: "<<num_exceptions<<endl;

    auto resWExc = c.parallelize(rows)
            .map(UDF("lambda a, b : (a, b, a / b)"))
            .resolve(ExceptionCode::ZERODIVISIONERROR,
                     UDF("lambda x: (x[0], x[1], 0.0)")).collectAsVector();

    ASSERT_EQ(resWExc.size(), ref.size());
    for(int i = 0; i < resWExc.size(); ++i) {

        std::cout<<"collected output is: "<<resWExc[i].toPythonString()<<std::endl;

        EXPECT_EQ(resWExc[i].getDouble(2), ref[i]);
    }
}

TEST_F(Resolve, MultipleResolvers) {

    Context c(microTestOptions());

    auto& ds = c.parallelize({Row(0, "Hello"), Row(1, "hi"), Row(0, "hola"), Row(2, "Morgen")})
            .map(UDF("lambda a, b: (10 / a, b)")).map(UDF("lambda a, b: (a, b[5])"));

    auto resWOExc = ds.collectAsVector();
    EXPECT_EQ(resWOExc.size(), 1);
    EXPECT_EQ(resWOExc[0].toPythonString(), "(5.00000,'n')");


    // i.e.
    // this here is the code in Python API
    // c.parallelize([(0, 'Hello'), (1, 'hi'), (0, 'hola'), (2, 'Morgen')]).map(lambda a: (10/a[0], a[1])).resolve(Zero
    //DivisionError, lambda x: (0.0, x[1])).map(lambda x: (x[0], x[1][5])).resolve(IndexError, lambda x: (x[0], '.')).coll
    //ect()

    std::vector<Row> rows{Row(0, "Hello"), Row(1, "hi"), Row(0, "hola"), Row(2, "Morgen")};

    // now with resolvers
    auto res = c.parallelize(rows)
            .map(UDF("lambda a, b: (10 / a, b)"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: (0.00000, x[1])"))
            .map(UDF("lambda a, b: (a, b[5])"))
            .resolve(ExceptionCode::INDEXERROR, UDF("lambda x: (x[0], '.')"))
            .collectAsVector();

    EXPECT_EQ(res.size(), 4);

    // compare to correct result
    std::vector<Row> ref{Row(0.0, "."), Row(10.0, "."), Row(0.0, "."), Row(5.0, "n")};

    for(int i = 0; i < 4; ++i)
        EXPECT_EQ(res[i].toPythonString(), ref[i].toPythonString());
}
TEST_F(Resolve, LargeTracingTest) {
    // create here a good amount of rows and test ALL operators which may throw exceptions...

    // --> mix with resolve or not
    // map, filter, mapColumn, withColumn, ...


    std::vector<Row> rows;

    // generate columns with (i64, i64, string)

    for(int i = 0; i < 5; ++i) {
        rows.push_back(Row(0, 1, "Hello"));
        rows.push_back(Row(1, 0, "hi"));
        rows.push_back(Row(0, 4, "hola"));
        rows.push_back(Row(8, 8, "Morgen"));
        rows.push_back(Row(8, 8, "Hello world said Tux!"));
    }

    Context c(microTestOptions());

    // map!
    // auto res = c.parallelize(rows).map(UDF("lambda a, b, c: (a / b, c[a])")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b, c: (0.0, c[a])")).collect();

    // mapColumn!
    //c.parallelize(rows, {"a", "b", "c"}).mapColumn("c", UDF("lambda x: x[8] + '_hello'")).collect();

    // withColumn!
    //c.parallelize(rows, {"a", "b", "c"}).withColumn("d", UDF("lambda x: x['c'][8] + '_hello'")).collect();

    // filter
    // that works too ...
    auto res = c.parallelize(rows).filter(UDF("lambda a, b, c: a / b > 1.5")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b, c: True")).collect();


    // @TODO: resolver which throws too...


    // @TODO
    // this is an interesting special case because of the short circuiting...
    //auto res = c.parallelize(rows).filter(UDF("lambda a, b, c: c[a] == 'a' and a / b > 1.5")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b, c: False")).collect();

    // to be implemented:
    // auto res = c.parallelize(rows).filter(UDF("lambda a, b, c: not c[a] == 'a'")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b, c: False")).collect();
    // auto res = c.parallelize(rows).filter(UDF("lambda a, b, c: c[a] != 'a'")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b, c: False")).collect();
}

TEST_F(Resolve, FilterMapMix) {
    using namespace std;

    Context c(microTestOptions());

    vector<Row> rows;
    vector<Row> ref;

    auto N = 100;

    vector<string> templates{"Hello", "", "world", "Tux", "sealion eats penguins"};

    // rand input, fails
    auto seed = time(nullptr);
    srand(seed);
    for(int i = 0; i < N; ++i) {
        auto r1 = rand() % 10;
        auto r2 = rand() % 10;
        auto r3 = templates[rand() % templates.size()];

        rows.push_back(Row(r1, r2, r3));

        // computing the pipeline incl. resolvers
        Row t1 = Row(r1, r2 + 2, r3);
        Row t2;
        Row t3;
        // indexerror exception?
        if(r3.length() < 6) {
            // index error exception
            t2 = Row(t1.getInt(0), t1.getInt(1), "err", "");
        } else {
            // all ok
            string s = "a";
            s[0] = t1.getString(2)[5];
            t2 = Row(t1.getInt(0), t1.getInt(1), t1.getString(2), s + "_test");
        }

        if(t2.getInt(0) >= 5)
            continue;

        // t3
        if( t2.getInt(1) == 0) {
            // zero div exception
            t3 = Row(t2.getString(2), 0.0);
        } else {
            t3 = Row(t2.getString(2), t2.getInt(0) / (double)t2.getInt(1));
        }

        ref.push_back(t3);
    }

    // pipeline with filters and other fun things
    auto res = c.parallelize(rows).map(UDF("lambda a, b, c: (a, b + 2, c)"))
    .map(UDF("lambda x: (x[0], x[1], x[2], x[2][5] + '_test')"))
    .resolve(ExceptionCode::INDEXERROR, UDF("lambda x: (x[0], x[1], 'err', '')"))
    .filter(UDF("lambda t: t[0] < 5"))
    .map(UDF("lambda a, b, c, d: (c, a / b)"))
    .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b, c, d: (c, 0.0)"))
    .collectAsVector();

     ASSERT_EQ(ref.size(), res.size());

    // compare content
    for(int i = 0; i < ref.size(); ++i) {
        // cout<<"ref: "<<ref[i].toPythonString()<<" res: "<<res[i].toPythonString()<<endl;
        EXPECT_EQ(ref[i].toPythonString(), res[i].toPythonString());
    }
}

TEST_F(Resolve, ResolveAccessesOtherCol) {
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.csv.selectionPushdown", "true");
    opt.set("tuplex.resolveWithInterpreterOnly", "false");
    Context c(opt);

    //    Row(0, 1, 2, 4), Row(1, 1, 2, 4)

    stringToFile(URI(testName + ".csv"), "0,1,2,4\n1,1,2,4\n");

    auto res = c.csv(testName + ".csv").map(UDF("lambda x: 10 / x[0]"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x[2] * 1.0"))
            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 0.0"))
            .collectAsVector();

    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), "(2.00000,)");
    EXPECT_EQ(res[1].toPythonString(), "(10.00000,)");
}

TEST_F(Resolve, ResolveAccessesOtherColInterpreterOnlyAndPushdown) {
    using namespace std;

    // similar to the other test. Yet, the difference is that here
    // the resolver only is used. This requires remapping
    // of input columns.

    auto opt = microTestOptions();
    opt.set("tuplex.csv.selectionPushdown", "true");
    opt.set("tuplex.resolveWithInterpreterOnly", "true");
    Context c(opt);

    //    Row(0, 1, 2, 4), Row(1, 1, 2, 4)
    stringToFile(URI(testName + ".csv"), "0,1,2,4\n1,1,2,4\n");

    auto res = c.csv(testName + ".csv").map(UDF("lambda x: 10 / x[0]"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x[2] * 1.0"))
            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 0.0"))
            .collectAsVector();

    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), "(2.00000,)");
    EXPECT_EQ(res[1].toPythonString(), "(10.00000,)");
}

TEST_F(Resolve, FilterResolve) {
    // filter is an odd function for exception resolution, this test here ensures it works logically
    using namespace std;

    Context c(microTestOptions());

    vector<Row> rows;
    vector<Row> ref;

    auto N = 100;

    // rand input, fails
    srand(42);
    for(int i = 0; i < N; ++i) {
        // schema

        auto r1 = rand() % 10;
        auto r2 = rand() % 10;

//        r1 = 9; r2 = 0;

        rows.push_back(Row(r1, r2));
        // cout<<"rows.push_back(Row("<<r1<<", "<<r2<<"));"<<endl;
    }


    // Here are a couple tricky cases for the data below (all contained in N=100, srand(42)).
    /*// another sequence that fails when filter is involved. So many weird cases here...
    rows.push_back(Row(4, 3));
    rows.push_back(Row(9, 3));
    rows.push_back(Row(6, 1));
    rows.push_back(Row(8, 0));
    rows.push_back(Row(7, 2));
    rows.push_back(Row(0, 1));
    rows.push_back(Row(7, 7));
    rows.push_back(Row(0, 5));
    rows.push_back(Row(6, 1));
    rows.push_back(Row(3, 5));
    rows.push_back(Row(9, 0));
    rows.push_back(Row(3, 0));
    rows.push_back(Row(6, 5));
    rows.push_back(Row(3, 8));*/


    // minimal difficult example:
    // rows.push_back(Row(8,0));
    // rows.push_back(Row(7,2));
    // rows.push_back(Row(0,1));
    // rows.push_back(Row(3,5));
    // rows.push_back(Row(9,0));

    // does not work:
    // rows.push_back(Row(8,0));
    // rows.push_back(Row(7,2));
    // rows.push_back(Row(0,1));
    // rows.push_back(Row(3,5));
    // rows.push_back(Row(9,0));


    // use these rows to create hard test
    //  rows.push_back(Row(6,1));
    //  rows.push_back(Row(1,0));
    //  rows.push_back(Row(0,5));
    //  rows.push_back(Row(8,3));
    //  rows.push_back(Row(8,0));
    //  rows.push_back(Row(7,2));
    //  rows.push_back(Row(0,1));
    //  rows.push_back(Row(3,5));
    //  rows.push_back(Row(9,0));
    //  rows.push_back(Row(3,0));
    //  rows.push_back(Row(1,0));
    //  rows.push_back(Row(2,6));

    // create ref:
    for(auto row : rows) {

        auto r1 = row.getInt(0);
        auto r2 = row.getInt(1);

        // ref case check:
        // exception resolve?
        if(r2 == 0) {
            // exception produced:
            if(r1 %2 == 1)
                ref.push_back(Row(r1, r2));
        } else {
            // keep if filter works
            if((double)r1 / (double)r2 > .5)
                ref.push_back(Row(r1, r2));
        }
    }

    auto code = "def resolve(a, b):\n"
                "    if a % 2 == 1:\n"
                "         return True\n"
                "    else:\n"
                "         return False\n";

    auto res = c.parallelize(rows).filter(UDF("lambda a, b: a / b > 0.5")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF(code)).collectAsVector();

    ASSERT_EQ(res.size(), ref.size());

#ifndef NDEBUG
    for(int i = 0; i < std::max(ref.size(), res.size()); ++i) {
        if(i < std::min(ref.size(), res.size())) {
            if(ref[i].toPythonString() != res[i].toPythonString())
                cout<<"ERROR::: ";
            cout<<"ref: "<<ref[i].toPythonString()<<"  res: "<<res[i].toPythonString()<<endl;
        }
        else if (i < ref.size()) {
            cout<<"ref: "<<ref[i].toPythonString()<<"  res: --"<<endl;
        } else if (i < res.size()) {
            cout<<"ref: -- "<<"  res: "<<res[i].toPythonString()<<endl;
        }
    }
#endif

    for(int j = 0; j < ref.size(); ++j)
        EXPECT_EQ(res[j], ref[j]);
}


// @TODO: Better testing infrastructure for exception handling
// However, may be difficult when architecture shall not be ruined...
// maybe test via history server connection?

TEST_F(Resolve, ResolverThrowingExceptions) {
    // reset log
    logStream.str("");
    Context c(microTestOptions());

    // check that throwing exceptions is correctly accounted for
    auto rs = c.parallelize({Row(1), Row(2), Row(0)})
            .map(UDF("lambda x: (10 / x, x * x)"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: (2 / x, x)")).collect();

    std::string expected = "During execution 1 exception occurred:\n"
                           "+- parallelize\n"
                           "|- map\n"
                           "|- resolve\n"
                           "   +-------------------+---+\n"
                           "   | ZeroDivisionError | 1 |\n"
                           "   +-------------------+---+\n"
                           "+- collect";

    // search within logstream for expected
    auto log = logStream.str();
    EXPECT_NE(log.find(expected), std::string::npos);
}

// exclude this test for faster dev for now...
//// this will probably be bad...
//TEST_F(Resolve, JoinWithExceptions) {
//    using namespace std;
//
//    // reset log
//    logStream.str("");
//
//    // this is to check the formatting of errors
//    Context c(microTestOptions());
//
//    auto& ds = c.parallelize({Row(10, "abc"), Row(20, "def"), Row(30, "xyz")}, vector<string>{"A", "B"});
//
//    c.parallelize({Row("abc"), Row("xy")}, vector<string>{"A"}).withColumn("X", UDF("lambda x: 'ab' + x[2]"))
//    .join(ds, string("A"), string("B")).show();
//
//    std::string expected = "+- parallelize\n"
//                           "|- withColumn\n"
//                           "   +------------+---+\n"
//                           "   | IndexError | 1 |\n"
//                           "   +------------+---+\n"
//                           "|  +- parallelize\n"
//                           "| /\n"
//                           "|/ join\n"
//                           "+- collect";
//
//    // search within logstream for expected
//    auto log = logStream.str();
//
//    cout<<"LOG IS:\n=======\n"<<log<<endl;
//    EXPECT_NE(log.find(expected), std::string::npos);
//
//}

TEST_F(Resolve, ResolverThrowingException) {
    using namespace std;

    Context c(microTestOptions());

    // map operator produces exception
    // however, first resolver applied to it also throws an exception!
    // -> the resolver after that catches the exception
    stringstream ss;
    c.parallelize({Row(10, "abc"), Row(0, "abc")}).map(UDF("lambda a, b: (10 / a, b + 'test')"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b: (0.0, b[5] + 'test')"))
            .collectAsVector(ss);

    // compare output, should hold that resolver throws exception!!


}

TEST_F(Resolve, ResolverResolvingResolver) {

    Context c(microTestOptions());

    // map operator produces exception
    // however, first resolver applied to it also throws an exception!
    // -> the resolver after that catches the exception
    auto v = c.parallelize({Row(10, "abc"), Row(0, "abc")}).map(UDF("lambda a, b: (10 / a, b + 'test')"))
    .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b: (0.0, b[5] + 'test')"))
    .resolve(ExceptionCode::INDEXERROR, UDF("lambda a, b: (0.0, 'resolved')"))
    .collectAsVector();

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], Row(1.0, "abctest"));
    EXPECT_EQ(v[1], Row(0.0, "resolved"));

}

TEST_F(Resolve, SimpleResolver) {
    logStream.str("");
    Context c(microTestOptions());

    // c.parallelize([1, 2, 3, 4, 0]).map(lambda x: (10 /x, x * x)).resolve(ZeroDivisionError, lambda x: (0.0, x)).collect()
    auto rs = c.parallelize({Row(1), Row(2), Row(0)})
               .map(UDF("lambda x: (10 / x, x * x)"))
               .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: (0.0, x)"))
               .collect();


    // there should be no errors logged in log.
    auto log = logStream.str();
    EXPECT_EQ(log.find("During execution"), std::string::npos);

}

TEST_F(Resolve, ShiftResolver) {

    // basic zero exception
    Context c(microTestOptions());

    auto resWOExc = c.parallelize({Row(-2, 3), Row(0, 1), Row(-5, -1), Row(5, 3), Row(10,0), Row(2, -4)})
            .map(UDF("lambda a, b : a << b"))
            .collectAsVector();

    ASSERT_EQ(resWOExc.size(), 4);
    EXPECT_EQ(resWOExc[0], Row(-16));
    EXPECT_EQ(resWOExc[1], Row(0));
    EXPECT_EQ(resWOExc[2], Row(40));
    EXPECT_EQ(resWOExc[3], Row(10));


    // now with exception resolution
    auto resWExc = c.parallelize({Row(Tuple(-2, 3)), Row(Tuple(0, 1)), Row(Tuple(-5, -1)), Row(Tuple(5, 3)), Row(Tuple(10,0)), Row(Tuple(2, -4))})
            .map(UDF("lambda a, b : a << b"))
            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 0"))
            .collectAsVector();

    ASSERT_EQ(resWExc.size(), 6);
    EXPECT_EQ(resWExc[0], Row(-16));
    EXPECT_EQ(resWExc[1], Row(0));
    EXPECT_EQ(resWExc[2], Row(0));
    EXPECT_EQ(resWExc[3], Row(40));
    EXPECT_EQ(resWExc[4], Row(10));
    EXPECT_EQ(resWExc[5], Row(0));

    // exception resolution with right shift
    auto resRSWExc = c.parallelize({Row(Tuple(-2, 3)), Row(Tuple(0, 1)), Row(Tuple(-5, -1)), Row(Tuple(5, 3)), Row(Tuple(10,0)), Row(Tuple(2, -4))})
            .map(UDF("lambda a, b : a >> b"))
            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 1"))
            .collectAsVector();

    ASSERT_EQ(resRSWExc.size(), 6);
    EXPECT_EQ(resRSWExc[0], Row(-1));
    EXPECT_EQ(resRSWExc[1], Row(0));
    EXPECT_EQ(resRSWExc[2], Row(1));
    EXPECT_EQ(resRSWExc[3], Row(0));
    EXPECT_EQ(resRSWExc[4], Row(10));
    EXPECT_EQ(resRSWExc[5], Row(1));

}

TEST_F(Resolve, SimpleFilterTest) {

    // c.parallelize([(1, 0), (3, 1), (4, 5)]) \
    // .filter(lambda x: x[0] / x[1] > 0.5) \
    // .resolve(ZeroDivisionError, lambda x: True) \
    // .collect()


    // basic zero exception
    Context c(microTestOptions());

    auto res = c.parallelize({Row(1, 0), Row(3, 1), Row(4, 5)})
            .filter(UDF("lambda x: x[0] / x[1] > 0.5"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: True"))
            .collectAsVector();

    printRows(res);

    ASSERT_EQ(res.size(), 3);

    EXPECT_EQ(res[0], Row(1, 0));
    EXPECT_EQ(res[1], Row(3, 1));
    EXPECT_EQ(res[2], Row(4, 5));
}

TEST_F(Resolve, IgnoreException) {

    Context c(microTestOptions());

    auto res = c.parallelize({Row(1, 0), Row(3, 1), Row(4, 5)})
            .filter(UDF("lambda x: x[0] / x[1] > 0.5"))
            .ignore(ExceptionCode::ZERODIVISIONERROR)
            .collectAsVector();

    printRows(res);

    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0], Row(3, 1));
    EXPECT_EQ(res[1], Row(4, 5));

}

TEST_F(Resolve, AllOperators) {
    // test resolvers on ALL operators

    Context c(microTestOptions()); // disables div by zero

    // also add a bunch of resolvers that just expand the code...
    // with column
    auto resWColumn = c.parallelize({Row(42, 7), Row(3, 0), Row(4, 5)}, {"colA", "colB"})
            .withColumn("colC", UDF("lambda x: x['colA'] / x['colB']"))
//            .resolve(ExceptionCode::VALUEERROR, UDF("lambda a, b: 7.0"))
//            .resolve(ExceptionCode::INDEXERROR, UDF("lambda a, b: 6.0"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b: 0.0"))
//            .resolve(ExceptionCode::ASSERTIONERROR, UDF("lambda a, b: 8.0"))
            .collectAsVector();
    ASSERT_EQ(resWColumn.size(), 3);
    EXPECT_EQ(resWColumn[0], Row(42, 7, 42.0 / 7.0));
    EXPECT_EQ(resWColumn[1], Row(3, 0, 0.0));
    EXPECT_EQ(resWColumn[2], Row(4, 5, 4.0 / 5.0));

    // map column
    auto resMColumn = c.parallelize({Row(42, 7), Row(3, 0), Row(4, 5)}, {"colA", "colB"})
            .mapColumn("colB", UDF("lambda x: 10 / x"))
            .resolve(ExceptionCode::VALUEERROR, UDF("lambda a: 7.0"))
            .resolve(ExceptionCode::INDEXERROR, UDF("lambda a: 6.0"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a: 0.0"))
            .resolve(ExceptionCode::ASSERTIONERROR, UDF("lambda a: 8.0"))
            .collectAsVector();
    ASSERT_EQ(resMColumn.size(), 3);
    EXPECT_EQ(resMColumn[0], Row(42, 10.0 / 7.0));
    EXPECT_EQ(resMColumn[1], Row(3, 0.0));
    EXPECT_EQ(resMColumn[2], Row(4, 10.0 /  5.0));


    // filter + map done before

    // @TODO: add a couple more tests here...
}

TEST(ResolvePython, SimplePythonString) {
    using namespace std;
    string python_code ="def extractSqft(x):\n"
                      "    val = x['facts and features']\n"
                      "    max_idx = val.find(' sqft')\n"
                      "    if max_idx < 0:\n"
                      "        max_idx = len(val)\n"
                      "    s = val[:max_idx]\n"
                      "\n"
                      "    split_idx = s.rfind('ba ,')\n"
                      "    if split_idx < 0:\n"
                      "        split_idx = 0\n"
                      "    else:\n"
                      "        split_idx += 5\n"
                      "    r = s[split_idx:]\n"
                      "    r = r.replace(',', '')\n"
                      "    return int(r)\n"
                      "\n"
                      "lamFun0 = lambda x: 0\n"
                      "lamFun1 = lambda t: (t['state'], t['city'], t['address'], t['sqft'], t['price'], )";

    python::initInterpreter();

    // in GIL here...
    auto mod = python::getMainModule();
    auto d = PyModule_GetDict(mod);
    PyRun_String(python_code.c_str(), Py_file_input, d, d);
    if(PyErr_Occurred()) {
        PyErr_Print();
        PyErr_Clear();
    }

    python::closeInterpreter();
}

//TEST_F(WrapperTest, BadPythonObjects) {
//    // add here test what to do with general, bad python objects...
//}

TEST_F(Resolve, DirtyZillowData) {
    using namespace tuplex;
    using namespace std;

    // def extractSqft(x):
    //    val = x['facts and features']
    //    max_idx = val.find(' sqft')
    //    if max_idx < 0:
    //        max_idx = len(val)
    //    s = val[:max_idx]
    //
    //    split_idx = s.rfind('ba ,')
    //    if split_idx < 0:
    //        split_idx = 0
    //    else:
    //        split_idx += 5
    //    r = s[split_idx:]
    //    r = r.replace(',', '')
    //    return int(r)
    //
    //def extractType(x):
    //    t = x['title'].lower()
    //    type = 'unknown'
    //    if 'condo' in t or 'apartment' in t:
    //        type = 'condo'
    //    if 'house' in t:
    //        type = 'house'
    //    return type
    //
    //
    //c.csv('data/zillow_dirty.csv') \
    // .withColumn('sqft', extractSqft) \
    // .mapColumn('city', lambda x: x[0].upper() + x[1:].lower()) \
    // .withColumn('type', extractType) \
    // .selectColumns(['state', 'city', 'address', 'type', 'sqft', 'price']).show(5)

    auto extractSqft_c = "def extractSqft(x):\n"
                         "    val = x['facts and features']\n"
                         "    max_idx = val.find(' sqft')\n"
                         "    if max_idx < 0:\n"
                         "        max_idx = len(val)\n"
                         "    s = val[:max_idx]\n"
                         "\n"
                         "    split_idx = s.rfind('ba ,')\n"
                         "    if split_idx < 0:\n"
                         "        split_idx = 0\n"
                         "    else:\n"
                         "        split_idx += 5\n"
                         "    r = s[split_idx:]\n"
                         "    r = r.replace(',', '')\n"
                         "    return int(r)\n";
    auto extractType_c = "def extractType(x):\n"
                         "    t = x['title'].lower()\n"
                         "    type = 'unknown'\n"
                         "    if 'condo' in t or 'apartment' in t:\n"
                         "        type = 'condo'\n"
                         "    if 'house' in t:\n"
                         "        type = 'house'\n"
                         "    return type\n";

    auto opt = testOptions();
    opt.set("tuplex.executorMemory", "48MB");
    opt.set("tuplex.driverMemory", "48MB");

    Context c(opt);

    // This doesn't work yet. Presumably because of mapColumn...
    auto res = c.csv("../resources/zillow_dirty.csv").cache()
            .withColumn("sqft", UDF(extractSqft_c))
            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 0"))
            //.mapColumn("city", UDF("lambda x: x[0].upper() + x[1:].lower()"))
            //.withColumn("type", UDF(extractType_c))
            .selectColumns({"sqft"})
            //.selectColumns({"state", "city", "address", "type", "sqft", "price"})
            .collectAsVector();

// fix this...
//    auto resWExc = c.csv("../resources/zillow_dirty.csv")
//            .withColumn("sqft", UDF(extractSqft_c))
//            .selectColumns({"state", "city", "address", "sqft", "price"})
//            .collectAsVector();
//
//    EXPECT_EQ(resWExc.size(), 28458);
//    std::cout<<"there are rows: "<<resWExc.size()<<std::endl;
//
//    auto res = c.csv("../resources/zillow_dirty.csv")
//            .withColumn("sqft", UDF(extractSqft_c))
//            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 0"))
//            .selectColumns({"state", "city", "address", "sqft", "price"})
//            .collectAsVector();
//
//    EXPECT_EQ(res.size(), 28458 + 6145);

//    std::cout<<"after resolution there are "<<res.size()<<std::endl; // <-- something still wrong here...


    // this has to work:
    // def extractSqft(x):
    //    val = x['facts and features']
    //    max_idx = val.find(' sqft')
    //    if max_idx < 0:
    //        max_idx = len(val)
    //    s = val[:max_idx]
    //
    //    split_idx = s.rfind('ba ,')
    //    if split_idx < 0:
    //        split_idx = 0
    //    else:
    //        split_idx += 5
    //    r = s[split_idx:]
    //    r = r.replace(',', '')
    //    return int(r)
    //
    //def resolveSqft(x):
    //    return 0
    //
    //c.csv('data/zillow_dirty.csv') \
    // .withColumn('sqft', extractSqft) \
    // .resolve(ValueError, resolveSqft) \
    // .selectColumns(['state', 'city', 'address', 'sqft', 'price']) \
    // .show(5)

    // full data: "../resources/zillow_dirty.csv"

    // sample: c.csv("../resources/zillow_dirty_sample.csv")

    // yay works now!
   // for(int i = 0; i < 10; ++i) {
        auto rs = c.csv("../resources/zillow_dirty_sample_mini.csv")
                .withColumn("sqft", UDF(extractSqft_c))
                .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 0"))
                .selectColumns({"state", "city", "address", "sqft", "price"})
                .collect();

        std::cout<<rs->rowCount()<<std::endl;
//    }

//    auto rs2 = c.csv("../resources/zillow_dirty.csv")
//            .withColumn("sqft", UDF(extractSqft_c))
//            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 0"))
//            .selectColumns({"state", "city", "address", "sqft", "price"})
//            .collect();

//    c.csv("../resources/zillow_dirty.csv")
//            .withColumn("sqft", UDF(extractSqft_c))
//            .mapColumn("city", UDF("lambda x: x[0].upper() + x[1:].lower()"))
//            .withColumn("type", UDF(extractType_c))
//            .selectColumns({"state", "city", "address", "type", "sqft", "price"})
//            .collectAsVector();
}

TEST_F(Resolve, DemoII) {
//    c.parallelize(['123', '12345', '1234567', '987']).map(lambda s: s[4]).collect()

    Context c(microTestOptions());

    auto r = c.parallelize({Row("123"), Row("12345")})
            .map(UDF("lambda s: s[4]"))
            .collectAsVector(std::cout);

}

TEST_F(Resolve, PartitionTypes) {
    using namespace std;


    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0");
    Context c(opt);

    vector<Row> ref;
    vector<Row> data;

    int N = 100;
    int M = 10;

    for(int j = 0; j < M; ++j) {
        int ri = rand() % 3;
        std::cout<<"random: "<<ri<<std::endl;


        if(0 == ri) {
            // add some partitions with only exceptions
            for(int i = 0; i < N; ++i) {
                data.push_back(Row("12"));
                ref.push_back(Row(42));
            }
        } else if(1 == ri) {
            // mixed use
            for(int i = 0; i < N; ++i) {
                if(rand() % 20 > 10) {
                    data.push_back(Row("12"));
                    ref.push_back(Row(42));
                } else {
                    data.push_back(Row("1237"));
                    ref.push_back(Row(7));
                }
            }
        } else {
            assert(ri == 2);

            // no exceptions
            for(int i = 0; i < N; ++i) {
                data.push_back(Row("1237"));
                ref.push_back(Row(7));
            }
        }
    }


    ASSERT_EQ(ref.size(), data.size());

    auto r = c.parallelize(data).map(UDF("lambda x: int(x[3])"))
            .resolve(ExceptionCode::INDEXERROR, UDF("lambda x: 42")).collectAsVector(std::cout);

    ASSERT_EQ(ref.size(), r.size());

    // run validation step
    for(int i = 0; i < ref.size(); ++i)
        EXPECT_EQ(ref[i], r[i]);
}


// resolve tests for dataframe
TEST_F(Resolve, MapColumnResolve) {

    using namespace tuplex;

    Context c(microTestOptions());
    auto& ds = c.parallelize({Row(1, 0), Row(2, 2), Row(3, 0), Row(4, 4)}, {"colA", "colB"})
            .mapColumn("colA", UDF("lambda x: x * 20"))
            .mapColumn("colB", UDF("lambda x: 10 / x"));

    auto vWoExc = ds.collectAsVector();
    ASSERT_EQ(vWoExc.size(), 2);
    EXPECT_EQ(vWoExc[0], Row(40, 5.0));
    EXPECT_EQ(vWoExc[1], Row(80, 2.5));

    // apply resolve function
    auto v = ds.resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 0.0")).collectAsVector();

    printRows(v);

    ASSERT_EQ(v.size(), 4);
    EXPECT_EQ(v[0], Row(20, 0.0));
    EXPECT_EQ(v[1], Row(40, 5.0));
    EXPECT_EQ(v[2], Row(60, 0.0));
    EXPECT_EQ(v[3], Row(80, 2.5));
}

TEST_F(Resolve, WithColumnResolve) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto& ds = c.parallelize({Row(1, 0), Row(2, 1), Row(3, 0), Row(4, 1)}, {"colA", "colB"})
            .withColumn("colC", UDF("lambda x: 42"))
            .withColumn("newcol", UDF("lambda a, b, c: a / b"));

    auto vWoExc = ds.collectAsVector();
    ASSERT_EQ(vWoExc.size(), 2);
    EXPECT_EQ(vWoExc[0], Row(2, 1, 42, 2.0));
    EXPECT_EQ(vWoExc[1], Row(4, 1, 42, 4.0));

    // apply resolve function
    auto v = ds.resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda a, b, c: a + 42.0")).collectAsVector();

    printRows(v);

    ASSERT_EQ(v.size(), 4);
    EXPECT_EQ(v[0], Row(1, 0, 42, 43.0));
    EXPECT_EQ(v[1], Row(2, 1, 42, 2.0));
    EXPECT_EQ(v[2], Row(3, 0, 42, 45.0));
    EXPECT_EQ(v[3], Row(4, 1, 42, 4.0));
}

TEST_F(Resolve, DemoStringMap) {
    // c.parallelize(['123', '12345', '1234567', '987']) \
    // .map(lambda s: s[4]) \
    // .resolve(IndexError, lambda x: '0') \
    // .collect()

    Context c(microTestOptions());

    auto res = c.parallelize({Row("123"), Row("12345"), Row("1234567"), Row("987")})
    .map(UDF("lambda s: s[4]"))
    .resolve(ExceptionCode::INDEXERROR, UDF("lambda x: '0'"))
    .collectAsVector();

    ASSERT_EQ(res.size(), 4);
    EXPECT_EQ(res[0], Row("0"));
    EXPECT_EQ(res[1], Row("5"));
    EXPECT_EQ(res[2], Row("5"));
    EXPECT_EQ(res[3], Row("0"));
}


// TODO: Need a test here for bad/wrongly formatted input rows and how they should be fixed!
TEST_F(Resolve, BadCSVInputRows) {

    // Bad CSV input rows resolution not yet supported...
    // to be implemented
#warning "fix this at a later stage, no time because of paper stress"

//    using namespace std;
//    using namespace tuplex;
//
//    // write test file
//    ofstream ofs("test.csv");
//
//    ofs<<"firstname,surname,income\n";
//    ofs<<"# this is a bad input row\n";
//    ofs<<"Manuel,Neuer,48000\n";
//    ofs<<"Miroslav,Klose,52000\n";
//    ofs<<"# this is another bad input row\n";
//    ofs.close();
//
//    Context c(microTestOptions());
//
//    std::stringstream ss;
//    auto res = c.csv("test.csv")
//            .filter(UDF("lambda x: x['income'] > 50000"))
//            .map(UDF("lambda x: {'firstname' : x['firstname']}"))
//            .collectAsVector(ss);
//
//    for(auto r : res)
//        std::cout<<r.toPythonString()<<std::endl; // why is this so off??
//    ASSERT_EQ(res.size(), 1);
//    EXPECT_EQ(res[0], Row("Miroslav"));
//    std::cout<<ss.str()<<std::endl;
//
//    // with ignore
//    ss.str("");
//    auto res2 = c.csv("test.csv")
//            .ignore(ExceptionCode::BADPARSE_STRING_INPUT)
//            .filter(UDF("lambda x: x['income'] < 50000"))
//            .map(UDF("lambda x: {'firstname' : x['firstname']}"))
//            .collectAsVector(ss);
//
//    ASSERT_EQ(res.size(), 1);
//    EXPECT_EQ(res[0], Row("Manuel"));
//    std::cout<<ss.str()<<std::endl;
//
//
//    // with resolve
//    auto res3 =  c.csv("test.csv")
//            .resolve(ExceptionCode::BADPARSE_STRING_INPUT, UDF("lambda x: ('Unknown', '', 60000)"))
//            .filter(UDF("lambda x: x['income'] > 50000"))
//            .map(UDF("lambda x: {'firstname' : x['firstname']}"))
//            .collectAsVector(std::cout);
//
//    EXPECT_TRUE(false); // todo... add checks for things above...
//
//    remove("test.csv");
}

TEST_F(Resolve, NoProjectionPushdown) {
    using namespace tuplex;

    // need here resolve test for the case when projection pushdown is disabled!
    // ==> i.e. what if the parser outputs weird stuff?
    // ==> need to resolve that!!!
    // ==> easiest via python processing pipeline, i.e. add auto resolver...
    // ==> that's why the python code will be always required.
    // ==> i.e. in the case of projection pushdown enabled, only need to add resolver to CSV rows (string -> (...) whatever is expected.
    // i.e. in the case of pushdown, the result of this needs to extract the correct rows!
    // ==> how about auto adding a resolver which places None wherever parse failed?
    // yeah, that would work...

    auto options = microTestOptions();
    options.set("tuplex.csv.selectionPushdown", "false"); // setting to false should produce errors...
    Context c(options);

//    auto extractSqft_c = "def extractSqft(x):\n"
//                         "    val = x['facts and features']\n"
//                         "    max_idx = val.find(' sqft')\n"
//                         "    if max_idx < 0:\n"
//                         "        max_idx = len(val)\n"
//                         "    s = val[:max_idx]\n"
//                         "\n"
//                         "    split_idx = s.rfind('ba ,')\n"
//                         "    if split_idx < 0:\n"
//                         "        split_idx = 0\n"
//                         "    else:\n"
//                         "        split_idx += 5\n"
//                         "    r = s[split_idx:]\n"
//                         "    r = r.replace(',', '')\n"
//                         "    return int(r)\n";
    auto extractSqft_c = "def extractSqft(x):\n"
                         "    val = x['facts and features']\n"
                         "    max_idx = val.find(' sqft')\n"
                         "    if max_idx < 0:\n"
                         "        max_idx = len(val)\n"
                         "    s = val[:max_idx]\n"
                         "\n"
                         "    split_idx = s.rfind('ba ,')\n"
                         "    if split_idx < 0:\n"
                         "        split_idx = 0\n"
                         "    else:\n"
                         "        split_idx += 5\n"
                         "    r = s[split_idx:]\n"
                         "    r = r.replace(',', '')\n"
                         "    return int(r)\n";

   auto rs = c.csv("../resources/zillow_dirty_sample_mini.csv")
            .withColumn("sqft", UDF(extractSqft_c))
            .resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 0"))
            .selectColumns({"state", "city", "address", "sqft", "price"})
            .collectAsVector();

    for(const auto& r : rs)
        std::cout<<r.toPythonString()<<std::endl;

    // check result (needs to go through python pipeline to work b/c of mismatch...)
    ASSERT_EQ(rs.size(), 4);
    EXPECT_EQ(rs[0].toPythonString(), Row("MA", "WOBURN", "7 Parker St", 1560, "$489,000").toPythonString());
    EXPECT_EQ(rs[1].toPythonString(), Row("MA", "Woburn", "Lake Ave", 0, "$1,700/mo").toPythonString()); // resolved row
    EXPECT_EQ(rs[2].toPythonString(), Row(Field::null(), Field::null(), Field::null(), 0, Field::null()).toPythonString()); // resolved ==> should be computed via pure python pipeline
    EXPECT_EQ(rs[3].toPythonString(), Row("MA", "Woburn", "23 Place Ln", 0, "$2,100/mo").toPythonString()); // resolved
}


TEST_F(Resolve, UpcastResolverType) {
    // these tests check that resolver types can be
    using namespace tuplex;
    using namespace std;

    auto options = microTestOptions();
    options.set("tuplex.autoUpcast", "true"); // else following tests won't work.
    ASSERT_TRUE(options.AUTO_UPCAST_NUMBERS());
    // @TODO: check that the fallback solution also works.

    Context c(options);

    auto code = "def f(x):\n"
                "    return float(x[3])\n"; // 4th digit to be cast to float!

    vector<Row> v;
    // upcast boolean to f64
    v.clear();
    v = c.parallelize({Row("1234"), Row("123")}).map(UDF(code)).resolve(ExceptionCode::INDEXERROR, UDF("lambda x: True")).collectAsVector();
    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], Row(4.0));
    EXPECT_EQ(v[1], Row(1.0));

    // upcast int to f64
    v.clear();
    v = c.parallelize({Row("1234"), Row("123")}).map(UDF(code)).resolve(ExceptionCode::INDEXERROR, UDF("lambda x: -3")).collectAsVector();
    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], Row(4.0));
    EXPECT_EQ(v[1], Row(-3.0));

    v.clear();
    // upcast int to option[f64]
    auto codeII = "def f(x):\n"
                   "    if len(x) < 10: \n"
                   "        return float(x[3])\n"
                   "    else:\n"
                   "        return None\n"; // 4th digit to be cast to float!
    v = c.parallelize({Row("1234"), Row("123")}).map(UDF(codeII)) //
            .resolve(ExceptionCode::INDEXERROR, UDF("lambda x: 7"))
            .collectAsVector();

    for(auto r : v)
        std::cout<<r.toPythonString()<<std::endl;
    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), Row(4.0).toPythonString());
    EXPECT_EQ(v[1].toPythonString(), Row(7.0).toPythonString());

    // upcasting within if statement!
    v.clear();
    auto codeIII = "def f(x):\n"
                  "    y = float(x[3]) if len(x) < 10 else 42 \n"
                  "    return y\n"; // 4th digit to be cast to float!

    v = c.parallelize({Row("1234"), Row("123")}).map(UDF(codeIII))
            .resolve(ExceptionCode::INDEXERROR, UDF("lambda x: 7"))
            .collectAsVector();

    for(auto r : v)
        std::cout<<r.toPythonString()<<std::endl;
    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), Row(4.0).toPythonString());
    EXPECT_EQ(v[1].toPythonString(), Row(7.0).toPythonString());
}

// @TODO: nested