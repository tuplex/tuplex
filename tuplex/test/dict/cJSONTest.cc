//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 8/9/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <cJSONDictProxyImpl.h>
#include "gtest/gtest.h"

TEST(cJSONTest, PutItemTest) {
    using namespace tuplex;
    using namespace std;

    // testing non-codegenerated put item
    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    EXPECT_EQ(false, dict_proxy.keyExists(Field((int64_t)10)));

    // put test values into test dict
    dict_proxy.putItem(Field((int64_t)10), Field("a"));
    dict_proxy.putItem(Field((int64_t)20), Field("b"));

    EXPECT_EQ(true, dict_proxy.keyExists(Field((int64_t)10)));
    EXPECT_EQ(true, dict_proxy.keyExists(Field((int64_t)20)));
    EXPECT_EQ(false, dict_proxy.keyExists(Field((int64_t)30)));

    dict_proxy.putItem(Field((int64_t)30), Field("c"));

    EXPECT_EQ(true, dict_proxy.keyExists(Field((int64_t)20)));
    EXPECT_EQ(true, dict_proxy.keyExists(Field((int64_t)30)));
}

TEST(cJSONTest, GetItemTest) {
    using namespace tuplex;
    using namespace std;

    // testing non-codegenerated put item
    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    // put test values into test dict
    dict_proxy.putItem(Field((int64_t)10), Field("a"));
    dict_proxy.putItem(Field((int64_t)20), Field("b"));

    EXPECT_EQ(Field("a"), dict_proxy.getItem(Field((int64_t)10)));
    EXPECT_EQ(Field("b"), dict_proxy.getItem(Field((int64_t)20)));
    EXPECT_THROW(dict_proxy.getItem(Field((int64_t)30)), std::runtime_error);

    dict_proxy.putItem(Field((int64_t)30), Field("c"));

    EXPECT_EQ(Field("c"), dict_proxy.getItem(Field((int64_t)30)));
}

TEST(cJSONTest, DeleteItemTest) {
    using namespace tuplex;
    using namespace std;

    // testing non-codegenerated put item
    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    // put test values into test dict
    dict_proxy.putItem(Field((int64_t)10), Field("a"));
    dict_proxy.putItem(Field((int64_t)20), Field("b"));

    EXPECT_EQ(Field("a"), dict_proxy.getItem(Field((int64_t)10)));
    EXPECT_EQ(Field("b"), dict_proxy.getItem(Field((int64_t)20)));

    dict_proxy.deleteItem(Field((int64_t)10));

    EXPECT_EQ(false, dict_proxy.keyExists(Field((int64_t)10)));
    EXPECT_EQ(true, dict_proxy.keyExists(Field((int64_t)20)));

    dict_proxy.deleteItem(Field((int64_t)20));
    dict_proxy.putItem(Field((int64_t)10), Field((int64_t)100));

    Field res = dict_proxy.getItem(Field((int64_t)10));

    // NOTE: expected result will be a double, bc I think cJSON stores all numbers as doubles
    EXPECT_EQ(Field((double)100), dict_proxy.getItem(Field((int64_t)10)));
    EXPECT_EQ(false, dict_proxy.keyExists(Field((int64_t)20)));
}

TEST(cJSONTest, ReplaceItemTest) {
    using namespace tuplex;
    using namespace std;

    // testing non-codegenerated put item
    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    // put test values into test dict
    dict_proxy.putItem(Field((int64_t)10), Field("a"));
    dict_proxy.putItem(Field((int64_t)20), Field("b"));

    EXPECT_EQ(Field("a"), dict_proxy.getItem(Field((int64_t)10)));
    EXPECT_EQ(Field("b"), dict_proxy.getItem(Field((int64_t)20)));

    dict_proxy.replaceItem(Field((int64_t)10), Field("c"));

    EXPECT_EQ(Field("c"), dict_proxy.getItem(Field((int64_t)10)));
    EXPECT_EQ(Field("b"), dict_proxy.getItem(Field((int64_t)20)));

    dict_proxy.putItem(Field((int64_t)30), Field("c"));

    EXPECT_EQ(Field("c"), dict_proxy.getItem(Field((int64_t)10)));
    EXPECT_EQ(Field("c"), dict_proxy.getItem(Field((int64_t)30)));
    
    dict_proxy.replaceItem(Field((int64_t)30), Field((int64_t)50));

    // NOTE: expected result will be a double, bc I think cJSON stores all numbers as doubles
    EXPECT_EQ(Field((double)50), dict_proxy.getItem(Field((int64_t)30)));
}

// tests to write:

// 1. heterogenous dict -> basically use modified JSON as in-memory storage format.
// 2. homogenous keytype dict -> can encode dict directly & serialize it more efficiently. Represent in-memory as hash table specialized depending on type.
// 3. homogenous valuetype -> ignore case, specialize to 1.
// 4. compile-time known keys/restricted keyset, keys do not change. -> struct type with fixed offsets!