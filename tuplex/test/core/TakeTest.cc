//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <random>

#include <Context.h>
#include "TestUtils.h"

using namespace tuplex;
using namespace std;

class TakeTest : public PyTest {};

/**
 * Randomly generate a vector of rows for testing
 * @param N the size of vector
 * @return a vector of size N, containing the random data
 */
vector<Row> generateTestData(size_t N, uint64_t seed) {
    mt19937 gen(seed); //Standard mersenne_twister_engine seeded with rd()
    uniform_int_distribution<> distrib(1, 100000000);

    vector<Row> data;
    data.reserve(N);

    for (int i = 0; i < N; i++) {
        data.emplace_back(distrib(gen), distrib(gen), distrib(gen));
    }

    return data;
}

vector<Row> generateReferenceData(const vector<Row>& input, size_t topLimit, size_t bottomLimit) {
    vector<Row> output;
    for(size_t i = 0; i < topLimit && i < input.size(); i++) {
        output.push_back(input[i]);
    }
    size_t start_bottom = input.size() >= bottomLimit ? input.size() - bottomLimit : 0;
    start_bottom = max(topLimit, start_bottom);

    for(size_t i = start_bottom; i < input.size(); i++) {
        output.push_back(input[i]);
    }

    return output;
}

TEST_F(TakeTest, takeTopTest) {
    auto opt = testOptions();
    Context context(opt);

    auto rs = context.parallelize(
        {Row(1), Row(2), Row(3), Row(4), Row(5), Row(6)}).take(1, 0);

    ASSERT_EQ(rs->rowCount(), 1);
    auto v = rs->getRows(1);

    EXPECT_EQ(v[0].getInt(0), 1);

    auto rs2 = context.parallelize(
        {Row(1), Row(2), Row(3), Row(4), Row(5), Row(6)}).take(3, 0);

    ASSERT_EQ(rs2->rowCount(), 3);
    auto v2 = rs2->getRows(3);

    EXPECT_EQ(v2[0].getInt(0), 1);
    EXPECT_EQ(v2[1].getInt(0), 2);
    EXPECT_EQ(v2[2].getInt(0), 3);

    auto rs3 = context.parallelize(
        {Row("hello"), Row("world"), Row("! :)"), Row("world"), Row("hello"), Row("!"), Row("! :)"), Row("!")}).take(5, 0);

    ASSERT_EQ(rs3->rowCount(), 5);
    auto v3 = rs3->getRows(5);

    EXPECT_EQ(v3[0].getString(0), "hello");
    EXPECT_EQ(v3[1].getString(0), "world");
    EXPECT_EQ(v3[2].getString(0), "! :)");
    EXPECT_EQ(v3[3].getString(0), "world");
    EXPECT_EQ(v3[4].getString(0), "hello");

}

TEST_F(TakeTest, takeBottomTest) {
    auto opt = testOptions();
    Context context(opt);

    auto rs = context.parallelize(
        {Row(1), Row(2), Row(3), Row(4), Row(5), Row(6)}).take(0, 1);

    ASSERT_EQ(rs->rowCount(), 1);
    auto v = rs->getRows(1);

    EXPECT_EQ(v[0].getInt(0), 6);

    auto rs2 = context.parallelize(
        {Row(1), Row(2), Row(3), Row(4), Row(5), Row(6)}).take(0, 3);

    ASSERT_EQ(rs2->rowCount(), 3);
    auto v2 = rs2->getRows(3);

    EXPECT_EQ(v2[0].getInt(0), 4);
    EXPECT_EQ(v2[1].getInt(0), 5);
    EXPECT_EQ(v2[2].getInt(0), 6);

    auto rs3 = context.parallelize(
        {Row("hello"), Row("world"), Row("! :)"), Row("world"), Row("hello"), Row("!"), Row("! :)"), Row("!")}).take(0, 5);

    ASSERT_EQ(rs3->rowCount(), 5);
    auto v3 = rs3->getRows(5);

    EXPECT_EQ(v3[0].getString(0), "world");
    EXPECT_EQ(v3[1].getString(0), "hello");
    EXPECT_EQ(v3[2].getString(0), "!");
    EXPECT_EQ(v3[3].getString(0), "! :)");
    EXPECT_EQ(v3[4].getString(0), "!");

}

TEST_F(TakeTest, takeBothTest) {
    auto opt = testOptions();
    Context context(opt);

    auto rs = context.parallelize(
        {Row(1), Row(2), Row(3), Row(4), Row(5), Row(6)}).take(1, 1);

    ASSERT_EQ(rs->rowCount(), 2);
    auto v = rs->getRows(2);

    EXPECT_EQ(v[0].getInt(0), 1);
    EXPECT_EQ(v[1].getInt(0), 6);

    auto rs2 = context.parallelize(
        {Row(1), Row(2), Row(3), Row(4), Row(5), Row(6)}).take(2, 1);

    ASSERT_EQ(rs2->rowCount(), 3);
    auto v2 = rs2->getRows(3);

    EXPECT_EQ(v2[0].getInt(0), 1);
    EXPECT_EQ(v2[1].getInt(0), 2);
    EXPECT_EQ(v2[2].getInt(0), 6);

    auto rs3 = context.parallelize(
        {Row("hello"), Row("world"), Row("! :)"), Row("world"), Row("hello"), Row("!"), Row("! :)"), Row("!")}).take(2, 3);

    ASSERT_EQ(rs3->rowCount(), 5);
    auto v3 = rs3->getRows(5);

    EXPECT_EQ(v3[0].getString(0), "hello");
    EXPECT_EQ(v3[1].getString(0), "world");
    EXPECT_EQ(v3[2].getString(0), "!");
    EXPECT_EQ(v3[3].getString(0), "! :)");
    EXPECT_EQ(v3[4].getString(0), "!");
}

TEST_F(TakeTest, takeBigTest) {
    mt19937 data_seed_gen(4242);

    const std::vector<size_t> test_size{1, 10, 100, 1001, 10001};
    const std::vector<size_t> limit_values{0, 1, 5, 11, 600, 10000};
    const std::vector<string> partition_sizes{"256B", "512KB", "1MB"};

    for(auto& part_size : partition_sizes) {
        auto opt = testOptions();
        opt.set("tuplex.partitionSize", part_size);
        Context context(opt);

        for(auto data_size : test_size) {
            for (auto top_limit: limit_values) {
                for (auto bottom_limit: limit_values) {
                    std::cout << "testing with partition size:" << part_size << " data size:"
                              << data_size << " top:" << top_limit << " bottom:" << bottom_limit << std::endl;

                    auto data = generateTestData(data_size, data_seed_gen());
                    auto ref_data = generateReferenceData(data, top_limit, bottom_limit);

                    auto res = context.parallelize(data).take(top_limit, bottom_limit);
                    ASSERT_EQ(ref_data.size(), res->rowCount());
                    for (Row &r: ref_data) {
                        Row res_row = res->getNextRow();
                        if (!(res_row == r)) {
                            ASSERT_EQ(res_row, r);
                        }
                    }
                }
            }
        }
    }
}

// TODO(march): with map, filter function
//TEST_F(TakeTest, takeMapFilterTest) {
//    srand(4242);
//}

// TODO(march): with file input
//    context.csv("../resources/");

// TODO(march): collect operator