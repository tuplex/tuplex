//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include <Context.h>
#include "../../utils/include/Utils.h"
#include "TestUtils.h"

class DiskSwapping : public PyTest {};

TEST_F(DiskSwapping, MicroSwap) {
    using namespace tuplex;

    ContextOptions co = testOptions();
    co.set("tuplex.partitionSize", "64B");
    co.set("tuplex.executorMemory", "256B");
    co.set("tuplex.useLLVMOptimizer", "false");

    Context c(co);


    // one integer needs 8 bytes, given here are 200KB partitions
    // i.e. in order to go over the limit (1MB) following number of integers are needed
    int64_t numIntegers = 40;

    std::vector<int64_t> data;
    data.reserve(numIntegers);
    for(int64_t i = 0; i < numIntegers; i++)
        data.push_back(i);//(i * i) % 42);

    // @TODO: there is a bad problem with the error datasets. Hence, solve that first so no assertion error is produced
    // then,
    // parallelize (swaps occurs)
    // like when the & is left away, then this code fails
    auto& ds = c.parallelize(data.begin(), data.end());
    auto res = ds.collectAsVector();

    // result type?
    EXPECT_EQ(res.size(), numIntegers);

    // validate result
    for(int64_t i = 0; i < numIntegers; i++)
        EXPECT_EQ(i, res[i].getInt(0));
}

TEST_F(DiskSwapping, MiniSwap) {
    using namespace tuplex;

    ContextOptions co = testOptions();
    co.set("tuplex.partitionSize", "200KB");
    //co.set("tuplex.executorMemory", "1MB");
    co.set("tuplex.executorMemory", "400KB");
    co.set("tuplex.useLLVMOptimizer", "false");

    Context c(co);


    // one integer needs 8 bytes, given here are 200KB partitions
    // i.e. in order to go over the limit (1MB) following number of integers are needed
    int64_t numIntegers = static_cast<int64_t>(1.1 * (400 * 1024 / 8)); //1.1 * (1024 * 1024 / 8); // give 10% more

    std::vector<int64_t> data;
    data.reserve(numIntegers);
    for(int64_t i = 0; i < numIntegers; i++)
        data.push_back(i);//(i * i) % 42);

    // @TODO: there is a bad problem with the error datasets. Hence, solve that first so no assertion error is produced
    // then,
    // parallelize (swaps occurs)
    // like when the & is left away, then this code fails
    auto& ds = c.parallelize(data.begin(), data.end());
    auto res = ds.collectAsVector();

    // result type?
    EXPECT_EQ(res.size(), numIntegers);

    // validate result
    for(int64_t i = 0; i < numIntegers; i++)
        EXPECT_EQ(i, res[i].getInt(0));
}

TEST_F(DiskSwapping, SwapWithLambda) {
    using namespace tuplex;

    Context c(testOptions());
//    Context c(microTestOptions());

    // one integer needs 8 bytes, given here are 200KB partitions
    // i.e. in order to go over the limit (1MB) following number of integers are needed
    int64_t numIntegers = 1.1 * (1024 * 1024 / 8); // give 10% more

    std::vector<int64_t> data;
    data.reserve(numIntegers);
    for(int64_t i = 0; i < numIntegers; i++)
        data.push_back((i * i) % 42);

    std::cout<<"size is:"<<data.size()<<std::endl;


    // @TODO: there is a bad problem with the error datasets. Hence, solve that first so no assertion error is produced
    // then,
    // parallelize (swaps occurs)
    // like when the & is left away, then this code fails
    auto& ds = c.parallelize(data.begin(), data.end())
    .map(UDF("lambda x: (x, x * x)"));// this will double the necessary memory
    auto res = ds.collectAsVector();

    // result type?
    EXPECT_EQ(res.size(), numIntegers);

    // validate result
    for(int64_t i = 0; i < numIntegers; i++) {
        auto val = i * i % 42;
        EXPECT_EQ(val, res[i].getInt(0));
        EXPECT_EQ(val * val, res[i].getInt(1));
    }
}