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

TEST_F(IncrementalTest, CSV) {
    using namespace std;
    using namespace tuplex;

    auto opts = microTestOptions();
    Context c(opts);

    auto fileURI = URI(testName + ".csv");
    stringstream ss;
    ss << "1, 1.0\n",
    ss << "2, 2.0\n",
    ss << "3, 3.0\n";
    stringToFile(fileURI, ss.str());

    c.csv(fileURI.toPath(),
                     vector<string>(),
                     option<bool>::none,
                     option<char>::none,
                     '"',
                     std::vector<std::string>{""},
                     unordered_map<size_t, python::Type>({{1, python::Type::I64}})).show();
}

TEST_F(IncrementalTest, Debug) {
    using namespace tuplex;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    opts.set("tuplex.resolveWithInterpreterOnly", "false");
    Context c(opts);

    auto &ds = c.parallelize({Row(1), Row(2), Row(0), Row(4), Row(5)});

    auto res1 = ds.map(UDF("lambda x: 1 / x")).collectAsVector();
    printRows(res1);
//
//    auto res2 = ds.map(UDF("lambda x: 1 / x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1.0")).collectAsVector();
//    printRows(res2);
}

TEST_F(IncrementalTest, Performance) {
    using namespace tuplex;

    auto opts = testOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.resolveWithInterpreterOnly", "false");
    Context c(opts);

    auto numRows = 100;
    std::vector<Row> rows;
    rows.reserve(numRows);

    for (int i = 0; i < numRows; ++i) {
        if (i % 10 == 0) {
            rows.push_back(Row(0));
        } else {
            rows.push_back(Row(i));
        }
    }

    auto &ds_cached = c.parallelize(rows).cache();
    c.clearCache();

    Timer timer;
    auto res1 = ds_cached.map(UDF("lambda x: 1 / x")).collectAsVector();
    std::cout << "First iteration took: " << std::to_string(timer.time());
    ASSERT_EQ(res1.size(), numRows - numRows / 10);

    timer.reset();
    auto res2 = ds_cached.map(UDF("lambda x: 1 / x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1.0")).collectAsVector();
    std::cout << "Second iteration took: " << std::to_string(timer.time());
    ASSERT_EQ(res2.size(), numRows);
}
