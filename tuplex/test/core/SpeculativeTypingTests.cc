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

class SpeculativeTyping : public PyTest {};

TEST_F(SpeculativeTyping, PowerFunction) {
    using namespace tuplex;

    auto opt_undef = microTestOptions();
    opt_undef.set("tuplex.autoUpcast", "true");
    auto opt_noundef = opt_undef;
    opt_noundef.set("tuplex.autoUpcast", "false");
    Context c_undef(opt_undef);
    Context c_noundef(opt_noundef);

    // part A: check for float, typing should be float!
    UDF udf("lambda a, b: a ** b");
    auto resA1_type = c_undef.parallelize({Row(10.0, 0.0), Row(10.0, -1.0),
                                      Row(10.0, 1.0)}).map(udf).schema().getRowType();
    EXPECT_EQ(resA1_type.desc(), "(f64)");
    auto resB1_type = c_noundef.parallelize({Row(10.0, 0.0), Row(10.0, -1.0),
                                           Row(10.0, 1.0)}).map(udf).schema().getRowType();
    EXPECT_EQ(resB1_type.desc(), "(f64)");

    // part B: when using integer/bool for both a/b, output is always float when autoupcast is true,
    //         else the speculative result.
    auto resA2_type = c_undef.parallelize({Row(10, 0), Row(10, -1),
                                           Row(10, 1)}).map(udf).schema().getRowType();
    EXPECT_EQ(resA2_type.desc(), "(f64)");
    auto resB2a_type = c_noundef.parallelize({Row(10, 0), Row(10, -1),
                                             Row(10, 1)}).map(udf).schema().getRowType();
    EXPECT_EQ(resB2a_type.desc(), "(i64)");
    auto resB2b_type = c_noundef.parallelize({Row(10, -2), Row(10, -1),
                                              Row(10, 1)}).map(udf).schema().getRowType();
    EXPECT_EQ(resB2b_type.desc(), "(f64)");
}