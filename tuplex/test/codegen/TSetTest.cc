//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <TSet.h>
#include <gtest/gtest.h>


TEST(TSet, Basics) {
    TSet<int> s1;

    EXPECT_FALSE(s1.contains(10));
    EXPECT_EQ(s1.size(), 0);

    TSet<int> s2({1, 3, 7});
    EXPECT_TRUE(s2.contains(1));
    EXPECT_TRUE(s2.contains(3));
    EXPECT_TRUE(s2.contains(7));
    s2.add(42).add(57);
    EXPECT_TRUE(s2.contains(42));
    EXPECT_TRUE(s2.contains(57));
    s2.remove(57);
    EXPECT_FALSE(s2.contains(57));
    s2.remove(3);
    EXPECT_FALSE(s2.contains(3));
    EXPECT_EQ(s2.size(), 3);
}

TEST(TSet, AddAndRemove) {
    TSet<int> s;

    for(int i = 0; i < 1000; i++)
        s.add(i);

    for(int j = 500; j < 1000; j++)
    s.remove(j);

    for(int i = 0; i < 500; i++)
        EXPECT_TRUE(s.contains(i));

    EXPECT_EQ(s.size(), 500);
}