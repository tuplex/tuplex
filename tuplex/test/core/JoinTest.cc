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


// following tests should work when integer keys are supported (postponed)


class JoinTest : public PyTest {};


TEST_F(JoinTest, InnerJoinNullBucket) {
    // this test makes sure join works when match on NULL occurs (i.e. the null bucket is used!)
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!

    // disable null-value opt
    opt.set("optimizer.retypeUsingOptimizedInputSchema", "false");

    Context c(opt);

    // test setup to correctly check inner join
    auto &dsA = c.parallelize({Row(option<std::string>("abc"), 42),
                               Row(option<std::string>::none, 84),
                               Row(option<std::string>("xyz"), 100)},
                              vector<string>{"a", "b"});
    auto &dsB = c.parallelize({Row(Field::null(), -1),
                               Row(Field::null(), -2)}, vector<string>{"x", "y"});
    auto &dsC = c.parallelize({Row(option<std::string>::none, -1),
                               Row(option<std::string>::none, -2)},
                              vector<string>{"x", "y"});

    auto &dsD = c.parallelize({Row(Field::null(), 84, "hello")},
                              vector<string>{"a", "b", "c"});

    // join with type Option[String] vs. null
    auto res1 = dsA.join(dsB, std::string("a"), std::string("x")).collectAsVector();
    ASSERT_EQ(res1.size(), 2);
    EXPECT_EQ(res1[0].toPythonString(), "(84,None,-1)");
    EXPECT_EQ(res1[1].toPythonString(), "(84,None,-2)");

    // join with type Option[String] vs. Option[String]
    auto res2 = dsA.join(dsC, std::string("a"), std::string("x")).collectAsVector();
    ASSERT_EQ(res2.size(), 2);
    EXPECT_EQ(res2[0].toPythonString(), "(84,None,-1)");
    EXPECT_EQ(res2[1].toPythonString(), "(84,None,-2)");

    // join with type null vs. Option[String]
    // this is also does left build...
    // output should be
    // (None, 84, "hello") matches (None, -1), (None,-2)
    // => (84,"hello",None,-1)
    // => (84,"hello",None,-2)
    auto res3 = dsD.join(dsC, std::string("a"), std::string("x")).collectAsVector();
    ASSERT_EQ(res3.size(), 2);
    EXPECT_EQ(res3[0].toPythonString(), "(84,'hello',None,-1)");
    EXPECT_EQ(res3[1].toPythonString(), "(84,'hello',None,-2)");

    // join with type null vs. null
    auto res4 = dsD.join(dsB, std::string("a"), std::string("x")).collectAsVector();
    ASSERT_EQ(res4.size(), 2);
    EXPECT_EQ(res4[0].toPythonString(), "(84,'hello',None,-1)");
    EXPECT_EQ(res4[1].toPythonString(), "(84,'hello',None,-2)");
}

TEST_F(JoinTest, InnerJoinInt64Option) {
    // this test makes sure join works when match on NULL occurs (i.e. the null bucket is used) for Option[i64]
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!

    // disable null-value opt
    opt.set("optimizer.retypeUsingOptimizedInputSchema", "false");

    Context c(opt);

    // test setup to correctly check inner join
    auto &dsA = c.parallelize({Row(option<int64_t>(1), "abc"),
                               Row(option<int64_t>::none, "def"),
                               Row(option<int64_t>(2), "ghi")},
                              vector<string>{"a", "b"});
    auto &dsB = c.parallelize({Row(Field::null(), -1),
                               Row(Field::null(), -2)}, vector<string>{"x", "y"});
    auto &dsC = c.parallelize({Row(option<int64_t>::none, -1),
                               Row(option<int64_t>::none, -2)},
                              vector<string>{"x", "y"});

    auto &dsD = c.parallelize({Row(Field::null(), 84, "hello")},
                              vector<string>{"a", "b", "c"});

    // join with type Option[i64] vs. null
    auto res1 = dsA.join(dsB, std::string("a"), std::string("x")).collectAsVector();
    ASSERT_EQ(res1.size(), 2);
    EXPECT_EQ(res1[0].toPythonString(), "('def',None,-1)");
    EXPECT_EQ(res1[1].toPythonString(), "('def',None,-2)");

    // join with type Option[String] vs. Option[String]
    auto res2 = dsA.join(dsC, std::string("a"), std::string("x")).collectAsVector();
    ASSERT_EQ(res2.size(), 2);
    EXPECT_EQ(res2[0].toPythonString(), "('def',None,-1)");
    EXPECT_EQ(res2[1].toPythonString(), "('def',None,-2)");

    // join with type null vs. Option[String]
    // this is also does left build...
    // output should be
    // (None, 84, "hello") matches (None, -1), (None,-2)
    // => (84,"hello",None,-1)
    // => (84,"hello",None,-2)
    auto res3 = dsD.join(dsC, std::string("a"), std::string("x")).collectAsVector();
    ASSERT_EQ(res3.size(), 2);
    EXPECT_EQ(res3[0].toPythonString(), "(84,'hello',None,-1)");
    EXPECT_EQ(res3[1].toPythonString(), "(84,'hello',None,-2)");

    // join with type null vs. null
    auto res4 = dsD.join(dsB, std::string("a"), std::string("x")).collectAsVector();
    ASSERT_EQ(res4.size(), 2);
    EXPECT_EQ(res4[0].toPythonString(), "(84,'hello',None,-1)");
    EXPECT_EQ(res4[1].toPythonString(), "(84,'hello',None,-2)");
}

// inner join using strings
// => TODO: need to also make this work with integers... => can be done simpler...
TEST_F(JoinTest, SimpleColumnBasedInnerJoinStr) {
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!
    Context c(opt);

    // test setup to correctly check inner join
    auto& dsA = c.parallelize({Row(1, "one", 3),
                               Row(1, "one", 4),
                               Row(2, "two", 1),
                               Row(4, "four", 2)},
                                       vector<string>{"a", "b", "c"});
    auto& dsB = c.parallelize({Row(1, "one"),
                               Row(10, "one"),
                               Row(2, "two"),
                               Row(3, "three")}, vector<string>{"x", "y"});


    // inner join
    // join b == y, inner
    // Row(1, "one", 3) => matches with Row(1, "one"), Row(10, "one")
    // Row(1, "one", 4) => matches with Row(1, "one"), Row(10, "one")
    // Row(2, "two", 1) => matches with Row(2, "two")
    // Row(4, "four", 2) => no match
    auto res1 = dsA.join(dsB, std::string("b"), std::string("y")).collectAsVector();
    ASSERT_EQ(res1.size(), 5);
    EXPECT_EQ(res1[0].toPythonString(), "(1,3,'one',1)");
    EXPECT_EQ(res1[1].toPythonString(), "(1,3,'one',10)");
    EXPECT_EQ(res1[2].toPythonString(), "(1,4,'one',1)");
    EXPECT_EQ(res1[3].toPythonString(), "(1,4,'one',10)");
    EXPECT_EQ(res1[4].toPythonString(), "(2,1,'two',2)");

    // inner join with filter before
    // join b == y, inner
    // Row(1, "one", 3) => matches with Row(1, "one"), Row(10, "one")
    // Row(1, "one", 4) => matches with Row(1, "one"), Row(10, "one")
    // Row(2, "two", 1) => matches with Row(2, "two")
    // Row(4, "four", 2) => no match
    auto res2 = dsA.filter(UDF("lambda a, b, c: a % 2 == 0"))
                   .join(dsB, std::string("b"), std::string("y"))
                   .collectAsVector();
    ASSERT_EQ(res2.size(), 1);
    EXPECT_EQ(res2[0].toPythonString(), "(2,1,'two',2)");

    // inner join with filter after
    // join b == y, inner
    // Row(1, "one", 3) => matches with Row(1, "one"), Row(10, "one")
    // Row(1, "one", 4) => matches with Row(1, "one"), Row(10, "one")
    // Row(2, "two", 1) => matches with Row(2, "two")
    // Row(4, "four", 2) => no match
    auto res3 = dsA.join(dsB, std::string("b"), std::string("y"))
                   .filter(UDF("lambda a, b, c, d: d > 5"))
                   .collectAsVector();
    ASSERT_EQ(res3.size(), 2);
    EXPECT_EQ(res3[0].toPythonString(), "(1,3,'one',10)");
    EXPECT_EQ(res3[1].toPythonString(), "(1,4,'one',10)");





    // time for left join?


//    ASSERT_EQ(res.size(), 3);
//
//    EXPECT_EQ(res[0], Row(1, 3, 2, "two"));
//    EXPECT_EQ(res[1], Row(1, 4, 2, "two"));
//    EXPECT_EQ(res[2], Row(2, 1, 1, "one"));
//
//    // collect the other datasets (to make sure copy & co works)
//    auto res1 = ds1.collectAsVector();
//    ASSERT_EQ(res1.size(), 3);
//    EXPECT_EQ(res1[0], Row(1, 2, 3));
//    EXPECT_EQ(res1[1], Row(1, 2, 4));
//    EXPECT_EQ(res1[2], Row(2, 1, 1));
//
//    // fun map over the second
//    auto res2 = ds2.map(UDF("lambda a, b: len(b)")).collectAsVector();
//    ASSERT_EQ(res2.size(), 3);
//    EXPECT_EQ(res2[0], Row(3));
//    EXPECT_EQ(res2[1], Row(3));
//    EXPECT_EQ(res2[2], Row((int64_t)strlen("three")));
}

// two inner joins
TEST_F(JoinTest, TwoInnerJoins) {
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!
    Context c(opt);
}

TEST_F(JoinTest, InnerJoinSingleCol) {
    // match two single col dataframes => special case, b.c. nothing gets saved in bucket...
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!
    Context c(opt);

    auto& dsA = c.parallelize({Row("a"), Row("b"), Row("c"), Row("d"), Row("e")}, vector<string>{"colA"});
    auto& dsB = c.parallelize({Row("b"), Row("d")}, vector<string>{"colA"});

    auto res = dsA.join(dsB, string("colA"), string("colA")).collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), "('b',)");
    EXPECT_EQ(res[1].toPythonString(), "('d',)");
}


TEST_F(JoinTest, InnerJoinTwoTimes) {
    // join two times...
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.useLLVMOptimizer", "false");
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!
    Context c(opt);

    // simple flight inspired example
    auto& ds = c.parallelize({Row("ATL", "FRA", 20), Row("FRA", "BOS", 10), Row("JFK", "ATL", 5), Row("BOS", "JFK", 0)},
            vector<string>{"Origin", "Dest", "Delay"}); // 4 rows
    auto& dsAirportsSmall = c.parallelize({Row("ATL", "Atlanta"), Row("FRA", "Frankfurt"), Row("JFK", "New York")}, vector<string>{"Code", "Name"}); // 3 rows

    auto& dsAirports = c.parallelize({Row("ATL", "Atlanta"), Row("FRA", "Frankfurt"),
                                         Row("JFK", "New York"), Row("LAX", "Los Angeles"),
                                         Row("TXL", "Berlin")}, vector<string>{"Code", "Name"}); // 5 rows

    auto res1 = ds.join(dsAirports, string("Origin"), string("Code"), string(""), string(""), string("Origin"))
      .join(dsAirports, string("Dest"), string("Code"), string(""), string(""), string("Dest"))
      .selectColumns(std::vector<std::string>{"Origin", "OriginName", "Dest", "DestName", "Delay"}).collectAsVector();
    ASSERT_EQ(res1.size(), 2);
    EXPECT_EQ(res1[0].toPythonString(), "('ATL','Atlanta','FRA','Frankfurt',20)");
    EXPECT_EQ(res1[1].toPythonString(), "('JFK','New York','ATL','Atlanta',5)");

    auto res2 = ds.join(dsAirportsSmall, string("Origin"), string("Code"), string(""), string(""), string("Origin"))
            .join(dsAirportsSmall, string("Dest"), string("Code"), string(""), string(""), string("Dest"))
            .selectColumns(std::vector<std::string>{"Origin", "OriginName", "Dest", "DestName", "Delay"}).collectAsVector();
    ASSERT_EQ(res2.size(), 2);
    EXPECT_EQ(res2[0].toPythonString(), "('ATL','Atlanta','FRA','Frankfurt',20)");
    EXPECT_EQ(res2[1].toPythonString(), "('JFK','New York','ATL','Atlanta',5)");
}

// write extensive tests for joins here
TEST_F(JoinTest, SimpleLeftJoin) {
    // join two times...
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.useLLVMOptimizer", "false");
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!
    Context c(opt);

    auto& dsA = c.parallelize({Row("abc", 20), Row("def", 30)}, vector<string>{"A", "B"});
    auto& dsB = c.parallelize({Row(30, "abc"), Row(40, "xyz")}, vector<string>{"C", "D"});

    auto res = dsA.leftJoin(dsB, string("A"), string("D"))
                  .selectColumns(vector<string>{"A", "B", "C"}).collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), "('abc',20,30)");
    EXPECT_EQ(res[1].toPythonString(), "('def',30,None)");
}


// left join test where hash table should be built on right side
TEST_F(JoinTest, LeftMiniBuildRight) {
    // join two times...
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.useLLVMOptimizer", "false");
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!
    Context c(opt);

    // build side is right, that's why this here works...
    auto& ds = c.parallelize({Row("ATL", "FRA", 20), Row("FRA", "BOS", 10)},
                             vector<string>{"Origin", "Dest", "Delay"}); // 2 rows
    auto& dsAirports = c.parallelize({Row("ATL", "Atlanta")}, vector<string>{"Code", "Name"}); // 1 row

    auto res = ds.leftJoin(dsAirports, string("Origin"), string("Code"), string(""), string(""), string("Origin"))
    .selectColumns(vector<string>{"Origin", "OriginName", "Dest", "Delay"}).collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), "('ATL','Atlanta','FRA',20)");
    EXPECT_EQ(res[1].toPythonString(), "('FRA',None,'BOS',10)");
}

// left join test where hash table should be built on left side
TEST_F(JoinTest, LeftMiniBuildLeft) {
    // realized via a hack. Why? b.c. right join logic would be required...
    // => not yet implemented...
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.useLLVMOptimizer", "false");
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!
    Context c(opt);

    // build side is right, that's why this here works...
    auto& ds = c.parallelize({Row("ATL", "FRA", 20), Row("JFK", "BOS", 0)},
                             vector<string>{"Origin", "Dest", "Delay"}); // 1 row
    auto& dsAirports = c.parallelize({Row("ATL", "Atlanta"), Row("FRA", "Frankfurt"), Row("BOS", "Boston")}, vector<string>{"Code", "Name"}); // 3 rows

    auto res = ds.leftJoin(dsAirports, string("Origin"), string("Code"), string(""), string(""), string("Origin"))
            .selectColumns(vector<string>{"Origin", "OriginName", "Dest", "Delay"}).collectAsVector();
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0].toPythonString(), "('ATL','Atlanta','FRA',20)");
    EXPECT_EQ(res[1].toPythonString(), "('JFK',None,'BOS',0)");
}

// nested left join
TEST_F(JoinTest, LeftJoinTwoTimes) {
    // join two times...
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.useLLVMOptimizer", "false");
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!
    Context c(opt);

    // simple flight inspired example
    auto& ds = c.parallelize({Row("ATL", "FRA", 20), Row("FRA", "BOS", 10), Row("JFK", "ATL", 5), Row("BOS", "JFK", 12)},
                             vector<string>{"Origin", "Dest", "Delay"}); // 4 rows

    auto& dsAirports = c.parallelize({Row("ATL", "Atlanta"), Row("FRA", "Frankfurt"),
                                      Row("JFK", "New York"), Row("LAX", "Los Angeles"),
                                      Row("TXL", "Berlin")}, vector<string>{"Code", "Name"}); // 5 rows

    auto res = ds.leftJoin(dsAirports, string("Origin"), string("Code"), string(""), string(""), string("Origin"))
            .leftJoin(dsAirports, string("Dest"), string("Code"), string(""), string(""), string("Dest"))
            .selectColumns(std::vector<std::string>{"Origin", "OriginName", "Dest", "DestName", "Delay"}).collectAsVector();


    vector<Row> ref{Row("ATL", "Atlanta","FRA","Frankfurt",20),
                    Row("FRA", "Frankfurt", "BOS",Field::null(),10),
                    Row("JFK","New York","ATL","Atlanta",5),
                    Row("BOS",Field::null(),"JFK","New York",12)};
    ASSERT_EQ(res.size(), ref.size());
    for(int i = 0; i < std::min(res.size(), ref.size()); ++i)
        EXPECT_EQ(res[i].toPythonString(), ref[i].toPythonString());
}

// when hash table can't be build with one task only => i.e. requires single-threaded merging
TEST_F(JoinTest, LargeHashTable) {
    // join two times...
    using namespace tuplex;
    using namespace std;
    auto opt = microTestOptions();
    opt.set("tuplex.useLLVMOptimizer", "false");
    opt.set("tuplex.optimizer.filterPushdown", "false"); // no filter pushdown so codegen works properly!
    opt.set("tuplex.partitionSize", "128KB"); // force so hashmap is in multiple partitions
    opt.set("tuplex.inputSplitSize", "128KB"); // force so file reading is split into multiple tasks...
    Context c(opt);

    // use flights example
    string path = "../resources/pipelines/flights/GlobalAirportDatabase.txt"; // 760KB or so
    auto& dsAirports = c.csv(path,
          vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                         "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                         "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
          option<bool>::none, option<char>(':'), '"', vector<string>{"", "N/A"});

    // mini merge (build forced to right!)
    auto& ds = c.parallelize({Row("ATL", "FRA", 20), Row("FRA", "BOS", 10), Row("JFK", "ATL", 5), Row("BOS", "JFK", 12)},
                             vector<string>{"Origin", "Dest", "Delay"}); // 4 rows

     ds.leftJoin(dsAirports, string("Origin"), string("IATACode")).show();

}

// null bucket Left Join

// i.e. inner
// with/without filter / map before/after
// inner with 2x joining


TEST_F(JoinTest, SimpleIntJoins) {
    using namespace tuplex;
    using namespace std;
    Context c(microTestOptions());

    auto& ds1 = c.parallelize({Row(1, 2, 3), Row(1, 2, 4), Row(2, 1, 1)}, vector<string>{"a", "b", "c"});
    auto& ds2 = c.parallelize({Row(1, "one"), Row(2, "two"), Row(3, "three")}, vector<string>{"x", "y"});

    // inner join
    auto& ds = ds1.join(ds2, std::string("b"), std::string("x")).filter(UDF("lambda x: x[0] < 10"));

    auto res = ds.collectAsVector();
    ASSERT_EQ(res.size(), 3);

    EXPECT_EQ(res[0], Row(1, 3, 2, "two"));
    EXPECT_EQ(res[1], Row(1, 4, 2, "two"));
    EXPECT_EQ(res[2], Row(2, 1, 1, "one"));

    // collect the other datasets (to make sure copy & co works)
    auto res1 = ds1.collectAsVector();
    ASSERT_EQ(res1.size(), 3);
    EXPECT_EQ(res1[0], Row(1, 2, 3));
    EXPECT_EQ(res1[1], Row(1, 2, 4));
    EXPECT_EQ(res1[2], Row(2, 1, 1));

    // fun map over the second
    auto res2 = ds2.map(UDF("lambda a, b: len(b)")).collectAsVector();
    ASSERT_EQ(res2.size(), 3);
    EXPECT_EQ(res2[0], Row(3));
    EXPECT_EQ(res2[1], Row(3));
    EXPECT_EQ(res2[2], Row((int64_t)strlen("three")));

    auto& ds3 = c.parallelize({Row(1, 2, 4), Row(1, 2, 5), Row(2, 1, 8), Row(1, 10, 2)}, vector<string>{"a", "b", "c"});
    // left join
    auto res3 = ds3.leftJoin(ds2, std::string("b"), std::string("x")).collectAsVector();
    ASSERT_EQ(res3.size(), 4);
    EXPECT_EQ(res3[0].toPythonString(), "(1,4,2,'two')");
    EXPECT_EQ(res3[1].toPythonString(), "(1,5,2,'two')");
    EXPECT_EQ(res3[2].toPythonString(), "(2,8,1,'one')");
    EXPECT_EQ(res3[3].toPythonString(), "(1,2,10,None)");

    // inner join
    auto res4 = ds3.join(ds2, std::string("b"), std::string("x")).collectAsVector();
    ASSERT_EQ(res4.size(), 3);
    EXPECT_EQ(res4[0], Row(1, 4, 2, "two"));
    EXPECT_EQ(res4[1], Row(1, 5, 2, "two"));
    EXPECT_EQ(res4[2], Row(2, 8, 1, "one"));
}
//
//TEST_F(JoinTest, FilterPushdown_OnKeyColumn) {
//    using namespace std;
//    using namespace tuplex;
//
//    // @TODO: activate filter pushdown as option...
//    // @TODO: ignore/resolve operators following a filter should be pushed with it down!
//    auto co = microTestOptions();
//    co.set("tuplex.optimizer.selectionPushdown", "true");
//    Context c(co);
//
//    auto& ds1 = c.parallelize({Row(1, 2, 3), Row(1, 2, 4), Row(2, 1, 1)}, vector<string>{"a", "b", "c"});
//    auto& ds2 = c.parallelize({Row(1, "one"), Row(2, "two"), Row(3, "three")}, vector<string>{"x", "y"});
//
//    // join and filter then on
//    auto& ds = ds1.join(ds2, std::string("b"), std::string("x"))
//            .mapColumn("c", UDF("lambda x: x * x"))
//            .withColumn("d", UDF("lambda t: t['a'] > 0"))
//            .filter(UDF("lambda t: t['b'] < 10"))
//            .filter(UDF("lambda t: 0 < t['y'] < 6"))
//            .selectColumns(vector<string>{"a", "b", "y"});
//
//    ds.collect();
//}