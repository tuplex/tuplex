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
#include <StatUtils.h>
#include <cmath>

TEST(StatUtils, RollingStatsI) {
    using namespace tuplex;

    RollingStats<int> rs;
    std::vector<int> v{1, 2, 3, 4, 5, 6};

    for(auto el : v)
        rs.update(el);

    EXPECT_EQ(rs.max(), 6);
    EXPECT_EQ(rs.min(), 1);
    EXPECT_EQ(rs.mean(), 3.5);
    EXPECT_EQ(rs.std(), sqrt(3.5));
}

TEST(StatUtils, RollingStatsII) {
    using namespace tuplex;

    RollingStats<int> rs;
    std::vector<int> v{1};

    for(auto el : v)
        rs.update(el);

    EXPECT_EQ(rs.max(), 1);
    EXPECT_EQ(rs.min(), 1);
    EXPECT_EQ(rs.mean(), 1);
    EXPECT_EQ(rs.std(), 0);
}

TEST(StatUtils, RollingStatsIII) {
    using namespace tuplex;

    RollingStats<int> rs;
    std::vector<int> v{0, 0};

    for(auto el : v)
        rs.update(el);

    EXPECT_EQ(rs.max(), 0);
    EXPECT_EQ(rs.min(), 0);
    EXPECT_EQ(rs.mean(), 0);
    EXPECT_EQ(rs.std(), 0);
}