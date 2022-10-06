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
#include <PythonHelpers.h>
#include "gtest/gtest.h"

class MetricsTest : public PyTest {};

TEST_F(MetricsTest, BasicTest) {
    using namespace tuplex;
    using namespace std;

    auto co = microTestOptions();
    co.set("tuplex.useLLVMOptimizer", "true");
    EXPECT_TRUE(co.USE_LLVM_OPTIMIZER());
    Context c(co);
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
    auto metrics = c.getMetrics();
    auto t1 = metrics->getLLVMCompilationTime();
    auto t2 = metrics->getLLVMOptimizationTime();
    auto t3 = metrics->getLogicalOptimizationTime();
    auto t4 = metrics->getTotalCompilationTime();
    EXPECT_GT(t1, 0.0);
    EXPECT_GT(t2, 0.0);
    EXPECT_GT(t3, 0.0);
    EXPECT_GT(t4, 0.0);
}

TEST_F(MetricsTest, JsonStats) {
    // get stats as json and parse to check whether it's valid json
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    c.csv("../resources/flight_mini.csv")
     .withColumn("YEAR", UDF("lambda x: int(x['FL_DATE'][:4])"))
     .collect();

    // fetch metrics
    auto json_str = c.metrics().to_json();

    EXPECT_NO_THROW(nlohmann::json::parse(json_str));
}