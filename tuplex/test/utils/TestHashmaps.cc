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
#include <int_hashmap.h>

TEST(HashmapUtils, IntHashmap) {
    const uint64_t test_size = 100000;
    map_t m = int64_hashmap_new();
    for(uint64_t i = 0; i < test_size; i++) {
        ASSERT_EQ(int64_hashmap_put(m, i, (int64_any_t) (i + 1)), MAP_OK);
    }
    for(uint64_t i = 0; i < test_size; i++) {
        int64_any_t t;
        ASSERT_EQ(int64_hashmap_get(m, i, &t), MAP_OK);
        ASSERT_EQ((uint64_t)(t), i+1);
    }
    for(uint64_t i = test_size; i < 2 * test_size; i++) {
        int64_any_t t;
        ASSERT_EQ(int64_hashmap_get(m, i, &t), MAP_MISSING);
        ASSERT_EQ(t, nullptr);
    }
    int64_hashmap_free(m);
}