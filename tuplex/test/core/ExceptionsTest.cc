//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 10/25/2021                                                               //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Context.h>
#include "TestUtils.h"
#include <PythonContext.h>

class ExceptionsTest : public PyTest {
protected:

    void SetUp() override {
        python::initInterpreter();

        // hold GIL
        assert(python::holdsGIL());
    }

    void TearDown() override {

        // important to get GIL for this
        python::closeInterpreter();
    }
};

TEST_F(ExceptionsTest, Join) {
    using namespace tuplex;
    auto opts = microTestOptions();
    Context c(opts);

    auto &ds1 = c.parallelize({Row(0, "Ben"), Row(1, "Hannah"), Row(2, "Sam")}, {"id", "name"});
    auto &ds2 = c.parallelize({Row(0, 21), Row(1, 25), Row(2, 27)}, {"id", "age"})
            .join(ds1, std::string("id"), std::string("id"));
    auto res = ds2.collectAsVector();

    // Split into 2 stages
    // S1 = input
    // S2 = input -> join -> output




}


TEST_F(ExceptionsTest, Type) {
    using namespace tuplex;
    auto opts = microTestOptions();
    Context c(opts);

    stringToFile(URI("test.csv"), "1,2\n3,4\n5,6\n7,\n8,\n9,10");
    auto &ds = c.csv("test.csv",
                     std::vector<std::string>(),
                     option<bool>::none,
                     option<char>::none,
                     '"',
                     std::vector<std::string>{""},
                     std::unordered_map<size_t, python::Type>({{0, python::Type::I64}, {1, python::Type::I64}}));

    auto output = ds.collectAsVector();
    auto ecounts = ds.getContext()->metrics().getOperatorExceptionCounts(ds.getOperator()->getID());

    EXPECT_EQ(output.size(), 4);
    EXPECT_EQ(output.at(0).toPythonString(), Row(1, 2).toPythonString());
    EXPECT_EQ(output.at(1).toPythonString(), Row(3, 4).toPythonString());
    EXPECT_EQ(output.at(2).toPythonString(), Row(5, 6).toPythonString());
    EXPECT_EQ(output.at(3).toPythonString(), Row(9, 10).toPythonString());

    EXPECT_EQ(ecounts.size(), 1);
    EXPECT_EQ(ecounts["Exception"], 2);
}

TEST_F(ExceptionsTest, Map) {
    using namespace tuplex;
    auto opts = microTestOptions();
    Context c(opts);

    auto &ds = c.parallelize({Row(1), Row(0), Row(0), Row(2)})
            .map(UDF("lambda x: 1 // x"));

    auto output = ds.collectAsVector();
    auto ecounts = ds.getContext()->metrics().getOperatorExceptionCounts(ds.getOperator()->getID());

    EXPECT_EQ(output.size(), 2);
    EXPECT_EQ(output.at(0).toPythonString(), Row(1).toPythonString());
    EXPECT_EQ(output.at(1).toPythonString(), Row(0).toPythonString());

    EXPECT_EQ(ecounts.size(), 1);
    EXPECT_EQ(ecounts["ZeroDivisionError"], 2);
}

TEST_F(ExceptionsTest, Filter) {
    using namespace tuplex;
    auto opts = microTestOptions();
    Context c(opts);

    auto &ds = c.parallelize({Row(1), Row(0), Row(0), Row(2)})
            .filter(UDF("lambda x: (1 / x) < 5"));

    auto output = ds.collectAsVector();
    auto ecounts = ds.getContext()->metrics().getOperatorExceptionCounts(ds.getOperator()->getID());

    EXPECT_EQ(output.size(), 2);
    EXPECT_EQ(output.at(0).toPythonString(), Row(1).toPythonString());
    EXPECT_EQ(output.at(1).toPythonString(), Row(2).toPythonString());

    EXPECT_EQ(ecounts.size(), 1);
    EXPECT_EQ(ecounts["ZeroDivisionError"], 2);
}

TEST_F(ExceptionsTest, Aggregate) {
    using namespace tuplex;
    auto opts = microTestOptions();
    Context c(opts);

    auto &ds = c.parallelize({Row(1), Row(0), Row(0), Row(2)})
            .aggregate(UDF("lambda x, y: x + y"), UDF("lambda a, x: a + (1 / x)"), Row(0.0));

    auto output = ds.collectAsVector();
    auto ecounts = ds.getContext()->metrics().getOperatorExceptionCounts(ds.getOperator()->getID());

    EXPECT_EQ(output.size(), 1);
    EXPECT_EQ(output.at(0).toPythonString(), Row(1.5).toPythonString());

    EXPECT_EQ(ecounts.size(), 1);
    EXPECT_EQ(ecounts["ZeroDivisionError"], 2);
}
