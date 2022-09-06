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

class AggregateTest : public PyTest {};


// not supported yet, needs to work together with NVO.
//TEST_F(AggregateTest, ColumnTest) {
//    using namespace tuplex;
//    using namespace std;
//    auto opt = microTestOptions();
//    Context context(opt);
//    context.metrics().getLLVMCompilationTime();
//    auto& ds = context.parallelize(
//            {Row("a", 1, false), Row("b", 2, true),
//             Row("c", 3, false),
//             Row("c", 3, false),
//             Row("b", 2, true),
//             Row("a", 1, false)}, {"X", "Y", "Z"}).unique();
//
//    context.metrics().getLLVMCompilationTime();
//    auto cols = ds.columns();
//    ASSERT_EQ(cols.size(), 3);
//    EXPECT_EQ(cols[0], "X");
//    EXPECT_EQ(cols[1], "Y");
//    EXPECT_EQ(cols[2], "Z");
//
//    std::stringstream ss;
//    ds.show(-1, ss);
//    auto lines = splitToLines(ss.str());
//    std::sort(lines.begin(), lines.end());
//
//    auto ref_show = "+-----+---+-------+\n"
//                    "| X   | Y | Z     |\n"
//                    "+-----+---+-------+\n"
//                    "| 'b' | 2 | True  |\n"
//                    "+-----+---+-------+\n"
//                    "| 'a' | 1 | False |\n"
//                    "+-----+---+-------+\n"
//                    "| 'c' | 3 | False |\n"
//                    "+-----+---+-------+";
//    auto ref = splitToLines(ref_show);
//    std::sort(ref.begin(), ref.end());
//    ASSERT_EQ(ref.size(), lines.size());
//    for(int i = 0; i < ref.size(); ++i)
//        EXPECT_EQ(ref[i], lines[i]);
//}

//// not supported yet, needs to work together with NVO.
//TEST_F(AggregateTest, MixedRowUniqueTest) {
//    using namespace tuplex;
//    auto opt = microTestOptions();
//    Context context(opt);
//
//    auto v1 = context.parallelize(
//            {Row("a", 1, false), Row("b", 2, true), Row("c", 3, false), Row("c", 3, false), Row("b", 2, true), Row("a", 1, false)}).unique().collectAsVector();
//
//    ASSERT_EQ(v1.size(), 3);
//
//    std::vector<Row> expected = {Row("a", 1, false), Row("b", 2, true), Row("c", 3, false)};
//    for(const auto& res_row : v1) {
//        bool found = false;
//        for(const auto &exp_row : expected) {
//            if(res_row == exp_row) {
//                found = true;
//                break;
//            }
//        }
//        ASSERT_TRUE(found);
//    }
//}

TEST_F(AggregateTest, StrUniqueTest) {
    using namespace tuplex;
    auto opt = testOptions();
    Context context(opt);

    auto v1 = context.parallelize(
            {Row("a"), Row("b"), Row("c"), Row("c"), Row("b"), Row("a")}).unique().collectAsVector();

    ASSERT_EQ(v1.size(), 3);

    std::map<std::string, size_t> m1;
    for (const auto &r: v1) {
        m1[r.getString(0)] += 1;
    }
    ASSERT_EQ(m1.size(), 3);
    EXPECT_EQ(m1["a"], 1);
    EXPECT_EQ(m1["b"], 1);
    EXPECT_EQ(m1["c"], 1);

    auto v2 = context.parallelize(
            {Row("hello"), Row("world"), Row("! :)"), Row("world"), Row("hello"), Row("!"), Row("! :)"), Row("!")}).unique().collectAsVector();

    ASSERT_EQ(v2.size(), 4);

    std::map<std::string, size_t> m2;
    for (const auto &r: v2) {
        m2[r.getString(0)] += 1;
    }
    ASSERT_EQ(m2.size(), 4);
    EXPECT_EQ(m2["hello"], 1);
    EXPECT_EQ(m2["world"], 1);
    EXPECT_EQ(m2["! :)"], 1);
    EXPECT_EQ(m2["!"], 1);
}

TEST_F(AggregateTest, StrUniqueTestLarge) {
    using namespace tuplex;
    auto opt = testOptions();
    Context context(opt);

    // first 10k lines from https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt replicated 3 times
    std::unordered_map<size_t, python::Type> m; m[0] = python::Type::STRING;
    auto v1 = context.csv("../resources/unique_test.txt", std::vector<std::string>(),
                          option<bool>::none, option<char>::none,'"',std::vector<std::string>{""}, m)
                                  .unique().collectAsVector();
    ASSERT_EQ(v1.size(), 10000);

    std::map<std::string, size_t> m1;
    for (const auto &r: v1) {
        m1[r.getString(0)] += 1;
    }
    ASSERT_EQ(m1.size(), 10000);

    // first 10k lines, no replication
    std::ifstream f("../resources/unique_test_res.txt");
    std::string word;
    while(std::getline(f, word)) {
        word.pop_back(); // remove new line
        EXPECT_EQ(m1[word], 1);
    }
    f.close();
}

// not supported yet, needs to work together with NVO.
//TEST_F(AggregateTest, NoneTypeUnique) {
//    using namespace tuplex;
//    auto opt = microTestOptions();
//    Context context(opt);
//
//    auto v1 = context.parallelize(
//            {Row(Field::null(), Field::null()), Row(Field::null(), Field::null())}).unique().collectAsVector();
//
//    ASSERT_EQ(v1.size(), 1);
//    EXPECT_EQ(v1[0], Row(Field::null(), Field::null()));
//
//    auto v2 = context.parallelize(
//            {
//                    Row(Field::null(), "abc", Field::null()),
//                    Row(Field::null(), "abc", Field::null()),
//                    Row(Field::null(), "wxyz", Field::null()),
//                    Row(Field::null(), "wxyz", Field::null()),
//            }).unique().collectAsVector();
//
//    ASSERT_EQ(v2.size(), 2);
//
//    std::map<std::string, size_t> m2;
//    for (const auto &r: v2) {
//        m2[r.toPythonString()] += 1;
//    }
//    ASSERT_EQ(m2.size(), 2);
//    EXPECT_EQ(m2["(None,'abc',None)"], 1);
//    EXPECT_EQ(m2["(None,'wxyz',None)"], 1);
//
//    auto v3 = context.csv("../resources/optional_str_test.csv").unique().collectAsVector();
//    ASSERT_EQ(v3.size(), 4);
//
//    std::map<std::string, size_t> m3;
//    for (const auto &r: v3) {
//        m3[r.toPythonString()] += 1;
//    }
//    ASSERT_EQ(m3.size(), 4);
//    EXPECT_EQ(m3["(None,None)"], 1);
//    EXPECT_EQ(m3["(None,'abc')"], 1);
//    EXPECT_EQ(m3["('abc',None)"], 1);
//    EXPECT_EQ(m3["('abc','abc')"], 1);
//}

TEST_F(AggregateTest, Int64Unique) {
    using namespace tuplex;
    auto opt = microTestOptions();
    Context context(opt);

    auto v1 = context.parallelize(
            {Row(1), Row(2), Row(3), Row(3), Row(2), Row(1)}).unique().collectAsVector();

    ASSERT_EQ(v1.size(), 3);

    std::map<int64_t, size_t> m1;
    for (const auto &r: v1) {
        m1[r.getInt(0)] += 1;
    }
    ASSERT_EQ(m1.size(), 3);
    EXPECT_EQ(m1[1], 1);
    EXPECT_EQ(m1[2], 1);
    EXPECT_EQ(m1[3], 1);

    auto v2 = context.parallelize(
            {Row(123), Row(456), Row(789), Row(456), Row(123), Row(1000), Row(789), Row(1000)}).unique().collectAsVector();

    ASSERT_EQ(v2.size(), 4);

    std::map<int64_t, size_t> m2;
    for (const auto &r: v2) {
        m2[r.getInt(0)] += 1;
    }
    ASSERT_EQ(m2.size(), 4);
    EXPECT_EQ(m2[123], 1);
    EXPECT_EQ(m2[456], 1);
    EXPECT_EQ(m2[789], 1);
    EXPECT_EQ(m2[1000], 1);
}

TEST_F(AggregateTest, WarnWrongOrder) {
    using namespace tuplex;
    auto opt = microTestOptions();
    Context c(opt);

    auto& ds = c.parallelize({Row(1, "abc", 0), Row(2, "xyz", 0), Row(3, "xyz", 1)});

    auto combine = UDF("def f(a, b):\n"
                   "\treturn a + b");
    auto agg = UDF("lambda a, x: a + x[0]");

    // now an incorrect order is used, thus an exception should be thrown!
    auto v = ds.aggregate(agg, combine, Row(0)).collectAsVector();
    ASSERT_EQ(v.size(), 1);
    EXPECT_EQ(v.front().getInt(0), 6);

    // correct order
    v = ds.aggregate(combine, agg, Row(0)).collectAsVector();
    ASSERT_EQ(v.size(), 1);
    EXPECT_EQ(v.front().getInt(0), 6);
}

TEST_F(AggregateTest, AggregateByKeyBasic) {
    using namespace tuplex;
    auto opt = testOptions();
    Context c(opt);

    auto combine1 = UDF("lambda a, b: a + b");
    auto agg1 = UDF("lambda a, x: a + x[0] * x[2]");
    auto& ds1 = c.parallelize({Row(1, "abc", 0), Row(2, "xyz", 1), Row(4, "xyz", 2), Row(3, "abc", -1)}, {"col0", "col1", "col2"}).aggregateByKey(combine1, agg1, Row(0), {"col1"});

    auto v1 = ds1.collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "('abc',-3)");
    EXPECT_EQ(v1[1].toPythonString(), "('xyz',10)");

    auto columns1 = ds1.columns();
    ASSERT_EQ(columns1.size(), 2);
    EXPECT_EQ(columns1[0], "col1");
    EXPECT_EQ(columns1[1], "");

    auto combine2 = UDF("lambda a, b: (a[0] + b[0], a[1] + b[1])");
    auto agg2 = UDF("lambda a, x: (a[0] + x[0], a[1] + x[2])");
    auto& ds2 = c.parallelize({Row(1, "abc", 0), Row(2, "xyz", 1), Row(4, "xyz", 2), Row(3, "abc", -1)}, {"col0", "col1", "col2"}).aggregateByKey(combine2, agg2, Row(0, 0), {"col1"});

    auto v2 = ds2.collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "('abc',4,-1)");
    EXPECT_EQ(v2[1].toPythonString(), "('xyz',6,3)");

    auto columns2 = ds2.columns();
    ASSERT_EQ(columns2.size(), 3);
    EXPECT_EQ(columns2[0], "col1");
    EXPECT_EQ(columns2[1], "");
    EXPECT_EQ(columns2[2], "");
}

TEST_F(AggregateTest, AggregateByKeyMultipleColumns) {
    using namespace tuplex;
    auto opt = testOptions();
    Context c(opt);

    auto combine1 = UDF("lambda a, b: a + b");
    auto agg1 = UDF("lambda a, x: a + x[0] * x[2]");
    auto &ds1 = c.parallelize(
            {Row(1, "abc", 0, "def"), Row(2, "xyz", 1, "def"), Row(4, "xyz", 2, "def"), Row(3, "abc", -1, "def")},
            {"col0", "col1", "col2", "col3"}).aggregateByKey(combine1, agg1, Row(0), {"col1", "col3"});

    auto v1 = ds1.collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "('abc','def',-3)");
    EXPECT_EQ(v1[1].toPythonString(), "('xyz','def',10)");

    auto columns1 = ds1.columns();
    ASSERT_EQ(columns1.size(), 3);
    EXPECT_EQ(columns1[0], "col1");
    EXPECT_EQ(columns1[1], "col3");
    EXPECT_EQ(columns1[2], "");
}

TEST_F(AggregateTest, LargeAggregateByKey) {
    // Same as AggregateByKeyBasic test, but input is scaled up by 2500x
    using namespace tuplex;
    auto opt = microTestOptions();
    opt.set("tuplex.inputSplitSize", "10KB"); // make sure there's lots of combiners
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.executorMemory", "64MB");
    opt.set("tuplex.executorCount", "4");
    Context c(opt);

    auto combine1 = UDF("lambda a, b: a + b");
    auto agg1 = UDF("lambda a, x: a + x[0] * x[2]");
    auto& ds1 = c.csv("../resources/aggbykey_large_test.csv").aggregateByKey(combine1, agg1, Row(0), {"col1"});

    auto v1 = ds1.collectAsVector();
    ASSERT_EQ(v1.size(), 2);
    EXPECT_EQ(v1[0].toPythonString(), "('abc',-7500)");
    EXPECT_EQ(v1[1].toPythonString(), "('xyz',25000)");

    auto columns1 = ds1.columns();
    ASSERT_EQ(columns1.size(), 2);
    EXPECT_EQ(columns1[0], "col1");
    EXPECT_EQ(columns1[1], "");

    auto combine2 = UDF("lambda a, b: (a[0] + b[0], a[1] + b[1])");
    auto agg2 = UDF("lambda a, x: (a[0] + x[0], a[1] + x[2])");
    auto& ds2 = c.csv("../resources/aggbykey_large_test.csv").aggregateByKey(combine2, agg2, Row(0, 0), {"col1"});

    auto v2 = ds2.collectAsVector();
    ASSERT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].toPythonString(), "('abc',10000,-2500)");
    EXPECT_EQ(v2[1].toPythonString(), "('xyz',15000,7500)");

    auto columns2 = ds2.columns();
    ASSERT_EQ(columns2.size(), 3);
    EXPECT_EQ(columns2[0], "col1");
    EXPECT_EQ(columns2[1], "");
    EXPECT_EQ(columns2[2], "");

    // have the combiner drop some data
    // --> this will work when semantics guarantee that combiner is executed at least ONCE per group.

    auto combine3 = UDF("lambda a, b: (1112, a[1] + b[1])");
    auto agg3 = UDF("lambda a, x: (a[0] + x[0], a[1] + x[2])");
    auto& ds3 = c.csv("../resources/aggbykey_large_test.csv").aggregateByKey(combine3, agg3, Row(0, 0), {"col1"});

    auto v3 = ds3.collectAsVector();
    ASSERT_EQ(v3.size(), 2);
    EXPECT_EQ(v3[0].toPythonString(), "('abc',1112,-2500)");
    EXPECT_EQ(v3[1].toPythonString(), "('xyz',1112,7500)");

    auto columns3 = ds3.columns();
    ASSERT_EQ(columns3.size(), 3);
    EXPECT_EQ(columns3[0], "col1");
    EXPECT_EQ(columns3[1], "");
    EXPECT_EQ(columns3[2], "");
}


TEST_F(AggregateTest, UniqueMixedTypesWithInterpreterFallback) {
    using namespace tuplex;
    using namespace std;

    // for reference deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorMemory", "64MB");
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.nullValueOptimization", "true");
    opt_ref.set("tuplex.csv.selectionPushdown", "false");
    opt_ref.set("tuplex.optimizer.generateParser", "false");

    Context c_ref(opt_ref);

    // create a mixed types file (majority should be string or int64 because these are the supported ones...)
    std::string content = "A\nB\n3\n4.5\nC\nNULL\n";
    stringToFile(content, testName + ".csv");

    // test pipeline could be:
    // c_ref.csv('test.csv').unique().map(lambda x: str(x)).collect()
    // => passing of pyobjects not yet supported here...

    // @TODO: other types??
}

//TEST_F(AggregateTest, ComplaintTypeAgg) {
//    using namespace tuplex;
//    using namespace std;
//
//    auto opt = ContextOptions::defaults();
//    // c = tuplex.Context({'tuplex.redirectToPythonLogging':True, 'tuplex.executorMemory':'2G', 'tuplex.driverMemory':'2G'})
//    opt.set("tuplex.redirectToPythonLogging", "True");
//    opt.set("tuplex.executorMemory", "2G");
//    opt.set("tuplex.driverMemory", "2G");
//    opt.set("tuplex.executorCount", "0");
//
//    // ds = c.csv('311_subset.csv')
//    // def combine_udf(a, b):
//    //  return a + b
//    //
//    // def aggregate_udf(agg, row):
//    //  return agg + 1
//    // ds.aggregateByKey(combine_udf, aggregate_udf, 0, ["Complaint Type"]).show()
//
//    Context c(opt);
//    auto path = ".../311_subset.csv";
//    auto& ds = c.csv(path);
//    auto combine_code = "def combine_udf(a, b):\n"
//                        "  return a + b\n";
//
//    auto agg_code =     "def aggregate_udf(agg, row):\n"
//                        "  return agg + 1";
//    ds.aggregateByKey(UDF(combine_code), UDF(agg_code), Row(0), std::vector<std::string>{"Complaint Type"}).show();
//}