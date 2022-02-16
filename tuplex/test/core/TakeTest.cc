//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Context.h>
#include "TestUtils.h"

class TakeTest : public PyTest {};

TEST_F(TakeTest, takeTopTest) {
    using namespace tuplex;
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
    using namespace tuplex;
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
    using namespace tuplex;
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