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
#include <logical/FilterOperator.h>
#include <FilterBreakdownVisitor.h>
#include <parser/Parser.h>
#include <TypeAnnotatorVisitor.h>

// need for these tests a running python interpreter, so spin it up
class LogicalOptimizerTest : public PyTest {};

// this test case doesn't work yet :/
// with new code for WITHCOLUMN in logical optimizer...
TEST_F(LogicalOptimizerTest, SimpleFilterPushdown) {
    using namespace tuplex;

    auto conf = microTestOptions();
    conf.set("tuplex.optimizer.filterPushdown", "true");
    Context c(conf);

    c.parallelize({Row(1), Row(2), Row(3), Row(4)})
    .withColumn("squared", UDF("lambda x: x * x"))
    .filter(UDF("lambda a, b: a % 2 == 0"))
    .ignore(ExceptionCode::TYPEERROR)
    .collectAsVector();

    // pushed result should be parallelize.filter.ignore.withcolumn.collectAsVector
}

TEST_F(LogicalOptimizerTest, FilterBreakdownVisitor) {
    using namespace tuplex;

    auto udf1 = UDF("lambda a: (a < 6 and a > 2) or (a < 5 and a > 1)");
    udf1.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::I64})));
    auto node1 = udf1.getAnnotatedAST().getFunctionAST();
    FilterBreakdownVisitor fbv1;
    node1->accept(fbv1);
    auto ranges1 = fbv1.getRanges();
    ASSERT_EQ(ranges1.size(), 1);
    ASSERT_EQ(ranges1[0].createLambdaString("a"), "(2 <= a <= 5)");

    auto udf2 = UDF("lambda a: (1 < a < 5 or 7 < a <= 9 or 14 < a < 19) and (3 < a < 9 or 17 < a < 29)");
    udf2.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::I64})));
    auto node2 = udf2.getAnnotatedAST().getFunctionAST();
    FilterBreakdownVisitor fbv2;
    node2->accept(fbv2);
    auto ranges2 = fbv2.getRanges();
    ASSERT_EQ(ranges2.size(), 1);
    ASSERT_EQ(ranges2[0].createLambdaString("a"), "(a == 4) or (a == 8) or (a == 18)");

    auto udf3 = UDF("lambda a: a in ['abc', 'def', 'ghi'] and a in ['def', 'xyz', 'abc']");
    udf3.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::STRING})));
    auto node3 = udf3.getAnnotatedAST().getFunctionAST();
    FilterBreakdownVisitor fbv3;
    node3->accept(fbv3);
    auto ranges3 = fbv3.getRanges();
    ASSERT_EQ(ranges3.size(), 1);
    ASSERT_EQ(ranges3[0].createLambdaString("a"), "(a == 'abc') or (a == 'def')");

    auto udf4 = UDF("lambda a: a and -4 < a < 4");
    udf4.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::I64})));
    auto node4 = udf4.getAnnotatedAST().getFunctionAST();
    FilterBreakdownVisitor fbv4;
    node4->accept(fbv4);
    auto ranges4 = fbv4.getRanges();
    ASSERT_EQ(ranges4.size(), 1);
    ASSERT_EQ(ranges4[0].createLambdaString("a"), "(-3 <= a <= -1) or (1 <= a <= 3)");
}