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
#include "TestUtils.h"

class ContextBasicsTest : public TuplexTest {};

TEST_F(ContextBasicsTest, parallelizationI) {
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(microTestOptions());
    c.parallelize({1, 2, 3, 4});
    c.parallelize({true, false});
    c.parallelize({1.0, 2.0});
    c.parallelize({5.0f});

    std::vector<bool> v{true, false};
    c.parallelize(v.begin(), v.end());

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(ContextBasicsTest, multiPartitionResultset) {
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    ContextOptions co = testOptions();
    co.set("tuplex.partitionSize", "100B"); // super small partition size
    co.set("tuplex.executorMemory", "1MB");

    // following code would lead to an IndexError
    //c.parallelize([("hello", "test")] * 100000).map(lambda x: x[4]).collect()

    Context c(co);
    Row row1(Tuple(0), Tuple("hello"));
    Row row2(Tuple(1), Tuple("this"));
    Row row3(Tuple(2), Tuple("is"));
    Row row4(Tuple(3), Tuple("a"));
    Row row5(Tuple(4), Tuple("test"));
    auto v = c.parallelize({row1, row2, row3, row4, row5})
            .map(UDF("lambda x: x[1][0]")) // new code: string index operator! first to raise an exception!
            .collectAsVector();

    EXPECT_EQ(v.size(), 5);
    std::vector<std::string> ref{"hello", "this", "is", "a", "test"};
    for(int i = 0; i < 5; i++) {
        EXPECT_EQ(v[i].getString(0), ref[i]);
    }

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(ContextBasicsTest, EmptyResultSetI) {
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    ResultSet *rs = new ResultSet(Schema(Schema::MemoryLayout::ROW, python::Type::I64),
                                  std::vector<Partition*>());

    EXPECT_TRUE(rs);
    EXPECT_FALSE(rs->hasNextRow());
    delete rs;

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(ContextBasicsTest, EmptyResultSetII) {
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    auto exec = testExecutor();

    auto schema = Schema(Schema::MemoryLayout::ROW, python::Type::I64);
    ResultSet *rs = new ResultSet(schema,
                                  std::vector<Partition*>{
            exec->allocWritablePartition(1000, schema, 100),
            exec->allocWritablePartition(1000, schema, 101)
                                  });

    EXPECT_TRUE(rs);
    EXPECT_FALSE(rs->hasNextRow());
    delete rs;

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(ContextBasicsTest, basic) {
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    // solve this via Adapter pattern -.-
    // --> i.e. DataSet should have a virtual member
    // impl that can be ErrorDataSet or DataSet...
    // this avoids these stupid assertion errors...

    Context c(microTestOptions());

    // NOTE: never use datasets without references!
    // this will strip error data set...
    // auto ds = c.makeError("this is an error dataset"); // <-- Don't write this.

    auto& ds = c.makeError("this is an error dataset");

    EXPECT_TRUE(ds.isError());

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(ContextBasicsTest, JSON) {
    using namespace tuplex;

    auto str = ContextOptions::defaults().asJSON();
    EXPECT_GT(str.length(), 2);
}