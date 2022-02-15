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

TEST_F(IncrementalTest, FileOutput) {
    using namespace tuplex;
    using namespace std;

    auto opts = testOptions();
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    Context c(opts);

    auto numRows = 100;
    auto amountExps = 0.25;
    std::vector<Row> inputRows;
    inputRows.reserve(numRows);
    std::unordered_multiset<std::string> expectedOutput1;
    expectedOutput1.reserve((int) (numRows * amountExps));
    std::unordered_multiset<std::string> expectedOutput2;
    expectedOutput2.reserve(numRows);

    for (int i = 0; i < numRows; ++i) {
        if (i % (int) (1 / amountExps) == 0) {
            inputRows.push_back(Row(0));
            expectedOutput2.insert(Row(-1).toPythonString());
        } else {
            inputRows.push_back(Row(i));
            expectedOutput1.insert(Row(i).toPythonString());
            expectedOutput2.insert(Row(i).toPythonString());
        }
    }

    auto fileURI = URI(scratchDir + "/" + testName + ".csv");
    auto outputFileURI = URI(scratchDir + "/" + testName + ".*.csv");

    auto &ds_cached = c.parallelize(inputRows).cache();

    ds_cached.map(UDF("lambda x: 1 // x if x == 0 else x")).tocsv(fileURI.toPath());
    auto output1 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output1.size(), expectedOutput1.size());
    for (const auto &row : output1) {
        ASSERT_TRUE(expectedOutput1.find(row.toPythonString()) != expectedOutput1.end());
    }

    ds_cached.map(UDF("lambda x: 1 // x if x == 0 else x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1")).tocsv(fileURI.toPath());
    auto output2 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output2.size(), expectedOutput2.size());
    for (const auto &row : output2) {
        ASSERT_TRUE(expectedOutput2.find(row.toPythonString()) != expectedOutput2.end());
    }
}