//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/1/2021                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <Context.h>
#include "TestUtils.h"

class IncrementalTest : public PyTest {};

TEST_F(IncrementalTest, Debug) {
    using namespace tuplex;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    Context c(opts);

    auto &ds = c.parallelize({Row(1), Row(2), Row(0), Row(4), Row(5)});

    auto res1 = ds.map(UDF("lambda x: 1 / x")).collectAsVector();
    printRows(res1);

    auto res2 = ds.map(UDF("lambda x: 1 / x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1.0")).collectAsVector();
    printRows(res2);
}

TEST_F(IncrementalTest, Performance) {
    using namespace tuplex;

    auto opts = testOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    Context c(opts);

    auto numRows = 100000
    std::vector<Row> rows;
    rows.reserve(numRows);

    for (int i = 0; i < numRows; ++i) {
        if (i % 100 == 0) {
            rows.push_back(Row(0));
        } else {
            rows.push_back(Row(i));
        }
    }

    auto &ds_cached = c.parallelize(rows).cache();

    auto res1 = ds_cached.map(UDF("lambda x: 1 / x")).collectAsVector();
    ASSERT_EQ(res1.size(), numRows - numRows / 100);

    auto res2 = ds_cached.map(UDF("lambda x: 1 / x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1.0")).collectAsVector();
    ASSERT_EQ(res2.size(), numRows);
}
