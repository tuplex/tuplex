//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <URI.h>
#include "gtest/gtest.h"
#include "../../utils/include/Utils.h"

using namespace tuplex;


TEST(MemString, Basic) {
    EXPECT_EQ(memStringToSize("987"), 987);
    EXPECT_EQ(memStringToSize("987MB"), 987 * 1024 * 1024);
    EXPECT_EQ(memStringToSize("98mb15b"), 98 * 1024 * 1024 + 15);
    EXPECT_EQ(memStringToSize("7.5GB"), 7.5 * 1024 * 1024 * 1024);
}

TEST(MemString, Malformed) {
    EXPECT_EQ(memStringToSize("7643a"), 0);
}

TEST(VecUtils, Reverse) {
    std::vector<int> vRes1({1, 2, 3, 4, 5});
    std::vector<int> vRes2({1, 2});

    std::vector<int> v1({5, 4, 3, 2, 1});
    std::vector<int> v2({2, 1});
    reverseVector(v1);
    reverseVector(v2);

    EXPECT_EQ(vRes1, v1);
    EXPECT_EQ(vRes2, v2);
}

TEST(VecUtils, SortAsc) {
    using namespace std;
    using namespace tuplex;
    EXPECT_TRUE(isSortedAsc(vector<int>{}));
    EXPECT_TRUE(isSortedAsc(vector<int>{1, 2, 3}));
    EXPECT_TRUE(isSortedAsc(vector<int>{1}));
    EXPECT_TRUE(isSortedAsc(vector<int>{-10, 20, 90}));
    EXPECT_FALSE(isSortedAsc(vector<int>{-90, 89, 0}));
    EXPECT_FALSE(isSortedAsc(vector<int>{5, 4, 3}));
    EXPECT_FALSE(isSortedAsc(vector<int>{1, 2, 4, 5, 4, 3, 2, 1}));
}

TEST(URI, INVALID) {
    using namespace tuplex;

    URI uriHDFS = URI("hdfs://jfhjg/kfjgkg");
    URI uriLOCAL = URI("file://jfhjg/kfjgkg");
    URI uriS3 = URI("s3://jfhjg/kfjgkg");

    EXPECT_FALSE(URI::INVALID == uriHDFS);
    EXPECT_FALSE(URI::INVALID == uriLOCAL);
    EXPECT_FALSE(URI::INVALID == uriS3);
}

TEST(URI, equal) {
    using namespace tuplex;

    URI uriA = URI("file://test/test/test.txt");
    URI uriB = URI("file://test/test/test.txt");
    URI uriC = URI("file://test/test/test123.txt");
    URI uriD = URI("hdfs://test/test/test.txt");

    EXPECT_TRUE(uriA == uriB);
    EXPECT_FALSE(uriA == uriC);
    EXPECT_FALSE(uriA == uriD);
}