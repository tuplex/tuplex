//
// Created by Leonhard Spiegelberg on 2/4/22.
//

#include "TestUtils.h"
#include <physical/Hashmap.h>

class HashmapTest : public PyTest {};

TEST_F(HashmapTest, BasicStringMap) {
    using namespace tuplex;

    // init hashmap
    auto hm = hashmap_new();
}