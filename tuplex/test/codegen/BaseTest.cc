//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <Base.h>

TEST(Base, floori) {
    EXPECT_EQ(core::floori(5, 2), 2);
    EXPECT_EQ(core::floori(47, -3), -16);
    EXPECT_EQ(core::floori(-8, 2), -4);
    EXPECT_EQ(core::floori(-8, -3), 2);
}

TEST(Base, swap) {
    int a = 500;
    int b = 42;

    core::swap(a, b);

    EXPECT_EQ(a, 42);
    EXPECT_EQ(b, 500);
}