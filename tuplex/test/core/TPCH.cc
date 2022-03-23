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

#include <physical/codegen/PipelineBuilder.h>

class TpchTest : public PyTest {};


static std::vector<std::string> lineitem_columns{"l_orderkey", // 0
                                                 "l_partkey", // 1
                                                 "l_suppkey", // 2
                                          "l_linenumber", // 3
                                          "l_quantity", // 4
                                          "l_extendedprice", // 5
                                          "l_discount", // 6
                                          "l_tax", // 7
                                          "l_returnflag", // 8
                                          "l_linestatus", // 9
                                          "l_shipdate", // 10
                                          "l_commitdate", // 11
                                          "l_receiptdate", // 12
                                          "l_shipinstruct", // 13
                                          "l_shipmode", // 14
                                          "l_comment"}; // 15

static std::vector<std::string> part_columns{"p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size",
                                             "p_container",
                                             "p_retailprice", "p_comment"};

TEST_F(TpchTest, Q6) {
    // Original query is:
    // SELECT
    //    sum(l_extendedprice * l_discount) as revenue
    //FROM
    //    lineitem
    //WHERE.
    //    l_shipdate >= date '1994-01-01'
    //    AND l_shipdate < date '1994-01-01' + interval '1' year
    //    AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
    //    AND l_quantity < 24;

    using namespace tuplex;

    auto ref_conf = testOptions();
    ref_conf.set("tuplex.executorCount", "0");

    // auto path = "../resources/tpch/lineitem.minisample.tbl"; // small test file, in case bugs are encountered...
    auto path = "../resources/tpch/lineitem.tbl"; // SF 0.01

    // count & verify for file the numbers
    // there are 60174 lines in the file.
    auto lines = core::splitLines(fileToString((const URI)path), "\n");

    // test with combinations
    for(unsigned i = 0; i < (0x1ul << 4ul); ++i) {
        ContextOptions conf(ref_conf);
        conf.set("tuplex.optimizer.filterPushdown", (i & (0x1ul << 0ul)) ? "true" : "false");
        conf.set("tuplex.csv.selectionPushdown", (i & (0x1ul << 1ul)) ? "true" : "false");
        conf.set("tuplex.useLLVMOptimizer", (i & (0x1ul << 2ul)) ? "true" : "false");
        conf.set("tuplex.optimizer.generateParser", (i & (0x1ul << 3ul)) ? "true" : "false");

        Context c(conf);

        std::vector<Row> res;
        res = c.csv(path, lineitem_columns,false,'|')
                .aggregate(UDF("lambda a, b: a + b"), UDF("lambda a, x: a + 1"), Row(0))
                .collectAsVector();
        ASSERT_EQ(res.size(), 1);
        EXPECT_EQ(res[0].getInt(0), lines.size());

        // alternative instead of using integer indices is to use syntactic sugar, i.e.
        // .aggregate(UDF("lambda a, b: a + b"), UDF("lambda a, x: a + x['l_extendedprice'] * x['l_discount']"), Row(0.0))
        // => done in other test

        // Hyper reference value: 1193053.2252999984
        auto& ds = c.csv(path, lineitem_columns, false,'|')
                .mapColumn("l_shipdate", UDF("lambda x: int(x.replace('-', ''))"))
                .filter(UDF("lambda x: 19940101 <= x['l_shipdate'] < 19940101 + 10000"))
                .filter(UDF("lambda x: 0.06 - 0.01 <= x['l_discount'] <= 0.06 + 0.01"))
                .filter(UDF("lambda x: x['l_quantity'] < 24"))
                .aggregate(UDF("lambda a, b: a + b"), UDF("lambda a, x: a + x[5] * x[6]"), Row(0.0));

        auto v = ds.collectAsVector();
        auto agg_ref = 1193053.2252999984;
        ASSERT_EQ(v.size(), 1);
        double tol = 0.0001;
        EXPECT_TRUE(std::abs(v[0].getDouble(0) - agg_ref) < tol);
    }
}

TEST_F(TpchTest, Q6_AggDictRewrite) {
    // Original query is:
    // SELECT
    //    sum(l_extendedprice * l_discount) as revenue
    //FROM
    //    lineitem
    //WHERE.
    //    l_shipdate >= date '1994-01-01'
    //    AND l_shipdate < date '1994-01-01' + interval '1' year
    //    AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
    //    AND l_quantity < 24;

    using namespace tuplex;

    auto ref_conf = testOptions();
    ref_conf.set("tuplex.executorCount", "0");

    // auto path = "../resources/tpch/lineitem.minisample.tbl"; // small test file, in case bugs are encountered...
    auto path = "../resources/tpch/lineitem.tbl"; // SF 0.01

    // count & verify for file the numbers
    // there are 60174 lines in the file.
    auto lines = core::splitLines(fileToString((const URI)path), "\n");

    ContextOptions conf(ref_conf);
    conf.set("tuplex.optimizer.filterPushdown", "true");
    conf.set("tuplex.csv.selectionPushdown", "true");
    conf.set("tuplex.useLLVMOptimizer", "true");
    conf.set("tuplex.optimizer.generateParser", "true");

    Context c(conf);

    std::vector<Row> res;
    res = c.csv(path, lineitem_columns,false,'|')
            .aggregate(UDF("lambda a, b: a + b"), UDF("lambda a, x: a + 1"), Row(0))
            .collectAsVector();
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0].getInt(0), lines.size());

    // following syntactic sugar doesn't work, b.c. UDF needs to know what to rewrite. @TODO: change that
    // this is in general supported.
    // .aggregate(UDF("lambda a, b: a + b"), UDF("lambda a, x: a + x['l_extendedprice'] * x['l_discount']"), Row(0.0))

    // @TODO: check functions are submitted in correct order!!!

    // Hyper reference value: 1193053.2252999984
    auto& ds = c.csv(path, lineitem_columns, false,'|')
            .mapColumn("l_shipdate", UDF("lambda x: int(x.replace('-', ''))"))
            .filter(UDF("lambda x: 19940101 <= x['l_shipdate'] < 19940101 + 10000"))
            .filter(UDF("lambda x: 0.06 - 0.01 <= x['l_discount'] <= 0.06 + 0.01"))
            .filter(UDF("lambda x: x['l_quantity'] < 24"))
            .aggregate(UDF("lambda a, b: a + b"), UDF("lambda a, x: a + x['l_extendedprice'] * x['l_discount']"), Row(0.0));

    auto v = ds.collectAsVector();
    auto agg_ref = 1193053.2252999984;
    ASSERT_EQ(v.size(), 1);
    double tol = 0.0001;
    EXPECT_TRUE(std::abs(v[0].getDouble(0) - agg_ref) < tol);
}

TEST_F(TpchTest, SimpleSumAggregate) {
    using namespace tuplex;

    auto conf = testOptions();
    // deactivate optimizers for now
    conf.set("tuplex.optimizer.filterPushdown", "false");
    conf.set("tuplex.csv.selectionPushdown", "false");
    conf.set("tuplex.useLLVMOptimizer", "false");
    conf.set("tuplex.executorCount", "0");
    conf.set("tuplex.optimizer.generateParser", "true");
    Context c(conf);

    c.parallelize({Row(1), Row(2), Row(3), Row(4), Row(5)})
     .aggregate(UDF("lambda a, b: a + b"), UDF("lambda a, x: a + x"), Row(0)).show();
}

TEST_F(TpchTest, SimpleFileCountWGeneratedParser) {
    using namespace tuplex;

    auto conf = testOptions();
    // deactivate optimizers for now
    conf.set("tuplex.optimizer.filterPushdown", "false");
    conf.set("tuplex.csv.selectionPushdown", "false");
    conf.set("tuplex.useLLVMOptimizer", "false");
    conf.set("tuplex.executorCount", "0");
    conf.set("tuplex.optimizer.generateParser", "true");
    Context c(conf);
    auto path = URI(testName + ".txt");
    stringToFile(path, "a\nb");

    // should be two rows!
    // error for generated parser...
    auto& ds = c.csv(path.toPath(), std::vector<std::string>{}, false)
     .aggregate(UDF("lambda a, b: a + b"), UDF("lambda a, x: a + 1"), Row(0));

    //ds.show();

    auto res = ds.collectAsVector();
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0].getInt(0), 2); // 2 rows
}

TEST_F(TpchTest, Pip) {
    using namespace tuplex;
    using namespace std;
    auto env = make_shared<codegen::LLVMEnvironment>();

    auto row_type = Row(10, 20, option<std::string>("test"), option<std::string>("test")).getRowType();

    UDF aggUDF("lambda a, x: a + x[0]");
    aggUDF.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::I64, row_type})));


    // compile func
    auto cf = aggUDF.compile(*env);

    ASSERT_TRUE(cf.good());
//    auto pip = new codegen::PipelineBuilder(env, python::Type::propagateToTupleType(python::Type::F64));
//    pip->buildWithTuplexWriter("writeRow", 100);
//    pip->addAggregate(101, aggUDF, Row(0), true, true);
//    pip->build();
//
//    cout<<core::withLineNumbers(env->getIR())<<endl;



}

TEST_F(TpchTest, Q19) {
    using namespace tuplex;
    // See original query: https://examples.citusdata.com/tpch_queries.html
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded

    for(int i = 0; i < 2; i ++) {
        ContextOptions conf(opt_ref);
        conf.set("tuplex.optimizer.filterPushdown", ((i == 0) ? "false" : "true"));
        Context c_ref(conf);

        auto condition = "def f(x):\n"
                         "    return (\n"
                         "        (\n"
                         "            x['p_brand'] == 'Brand#12'\n"
                         "            and x['p_container'] in ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']\n"
                         "            and 1 <= x['l_quantity'] <= 1 + 10\n"
                         "            and 1 <= x['p_size'] <= 5\n"
                         "            and x['l_shipmode'] in ['AIR', 'AIR REG']\n"
                         "            and x['l_shipinstruct'] == 'DELIVER IN PERSON'\n"
                         "        )\n"
                         "        or (\n"
                         "            x['p_brand'] == 'Brand#23'\n"
                         "            and x['p_container'] in ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']\n"
                         "            and 10 <= x['l_quantity'] <= 10 + 10\n"
                         "            and 1 <= x['p_size'] <= 10\n"
                         "            and x['l_shipmode'] in ['AIR', 'AIR REG']\n"
                         "            and x['l_shipinstruct'] == 'DELIVER IN PERSON'\n"
                         "        )\n"
                         "        or (\n"
                         "            x['p_brand'] == 'Brand#34'\n"
                         "            and x['p_container'] in ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']\n"
                         "            and 20 <= x['l_quantity'] <= 20 + 10\n"
                         "            and 1 <= x['p_size'] <= 15\n"
                         "            and x['l_shipmode'] in ['AIR', 'AIR REG']\n"
                         "            and x['l_shipinstruct'] == 'DELIVER IN PERSON'\n"
                         "        )\n"
                         "    )";
        auto lineitem_path = "../resources/tpch/lineitem.tbl";
        auto part_path = "../resources/tpch/part.tbl";
        auto lineitem = c_ref.csv(lineitem_path, lineitem_columns, false, '|');
        auto resds = c_ref.csv(part_path, part_columns, false, '|').join(
                lineitem, std::string("p_partkey"),
                std::string("l_partkey")).filter(UDF(condition)).aggregate(UDF("lambda a, b: a + b"),
                                                                           UDF("lambda a, x: a + x['l_extendedprice'] * (1 - x['l_discount'])"),
                                                                           0.0);

        auto res = resds.collectAsVector();
        ASSERT_EQ(res.size(), 1);
        EXPECT_EQ(res[0].getDouble(0), 22923.02800);
    }
}