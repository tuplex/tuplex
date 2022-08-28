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

    // testing non-codegenerated get item
    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

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

    // testing non-codegenerated delete item
    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

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
    EXPECT_EQ(Field((int64_t)100), dict_proxy.getItem(Field((int64_t)10)));
    EXPECT_EQ(false, dict_proxy.keyExists(Field((int64_t)20)));
}

TEST(cJSONTest, ReplaceItemTest) {
    using namespace tuplex;
    using namespace std;

    // testing non-codegenerated replace item
    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

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

    // NOTE: expected result will be a double, bc cJSON stores all numbers as doubles
    EXPECT_EQ(Field((int64_t)50), dict_proxy.getItem(Field((int64_t)30)));
}

// str -> _
TEST(cJSONTest, StrKeysTest) {
    using namespace tuplex;
    using namespace std;

    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    dict_proxy.putItem(Field("a"), Field((int64_t)1));
    dict_proxy.putItem(Field("b"), Field((int64_t)2));

    EXPECT_EQ(Field((int64_t)1), dict_proxy.getItem(Field("a")));
    EXPECT_EQ(Field((int64_t)2), dict_proxy.getItem(Field("b")));

    dict_proxy.putItem(Field("a"), Field("hello"));
    dict_proxy.replaceItem(Field("b"), Field(true));

    EXPECT_EQ(Field("hello"), dict_proxy.getItem(Field("a")));
    EXPECT_EQ(Field(true), dict_proxy.getItem(Field("b")));

    dict_proxy.deleteItem(Field("b"));

    EXPECT_EQ(true, dict_proxy.keyExists(Field("a")));
    EXPECT_EQ(false, dict_proxy.keyExists(Field("b")));
}

// _ -> null
TEST(cJSONTest, NullValsTest) {
    using namespace tuplex;
    using namespace std;

    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    dict_proxy.putItem(Field((int64_t)10), Field::null());
    dict_proxy.putItem(Field("a"), Field((int64_t)10));

    EXPECT_EQ(Field::null(), dict_proxy.getItem(Field((int64_t)10)));
    EXPECT_EQ(Field((int64_t)10), dict_proxy.getItem(Field("a")));

    dict_proxy.replaceItem(Field("a"), Field::null());

    EXPECT_EQ(Field::null(), dict_proxy.getItem(Field("a")));
}

// null -> _
TEST(cJSONTest, NullKeysTest) {
    using namespace tuplex;
    using namespace std;

    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    dict_proxy.putItem(Field::null(), Field((int64_t)10));

    EXPECT_EQ(Field((int64_t)10), dict_proxy.getItem(Field::null()));

    dict_proxy.putItem(Field::null(), Field("a"));

    EXPECT_EQ(Field("a"), dict_proxy.getItem(Field::null()));

    dict_proxy.replaceItem(Field::null(), Field(true));

    EXPECT_EQ(Field(true), dict_proxy.getItem(Field::null()));
}

// mix -> mix
TEST(cJSONTest, FloatTest) {
    using namespace tuplex;
    using namespace std;

    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    dict_proxy.putItem(Field((double)3.14), Field("pi"));
    dict_proxy.putItem(Field((double)1), Field(true));

    EXPECT_EQ(Field("pi"), dict_proxy.getItem(Field((double)3.14)));
    EXPECT_EQ(Field(true), dict_proxy.getItem(Field((double)1)));

    dict_proxy.putItem(Field("pi"), Field((double)3.14));
    dict_proxy.putItem(Field(true), Field((double)1));

    EXPECT_EQ(Field((double)3.14), dict_proxy.getItem(Field("pi")));
    EXPECT_EQ(Field((double)1), dict_proxy.getItem(Field(true)));

    dict_proxy.replaceItem(Field((double)3.14), Field((int64_t)3));
    dict_proxy.replaceItem(Field("pi"), Field((int64_t)3));

    EXPECT_EQ(Field((int64_t)3), dict_proxy.getItem(Field((double)3.14)));
    EXPECT_EQ(Field((int64_t)3), dict_proxy.getItem(Field("pi")));
}

// _ -> list
TEST(cJSONTest, ListValsTest) {
    using namespace tuplex;
    using namespace std;

    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    // init list 1
    vector<Field> vec_1{ Field((int64_t)10), Field((int64_t)20), Field((int64_t)30) };
    List list_1 = List::from_vector(vec_1);

    // init list 2
    vector<Field> vec_2{ Field("a"), Field("b"), Field("c") };
    List list_2 = List::from_vector(vec_2);

    // init list 3
    vector<Field> vec_3{ Field((double)15), Field((double)3.14), Field((double)2.7) };
    List list_3 = List::from_vector(vec_3);

    dict_proxy.putItem(Field((int64_t)10), Field(list_1));

    EXPECT_EQ(Field(list_1), dict_proxy.getItem(Field((int64_t)10)));

    dict_proxy.putItem(Field("a"), Field(list_2));
    dict_proxy.replaceItem(Field((int64_t)10), Field(list_3));

    EXPECT_EQ(Field(list_2), dict_proxy.getItem(Field("a")));
    EXPECT_EQ(Field(list_3), dict_proxy.getItem(Field((int64_t)10)));

    dict_proxy.deleteItem(Field("a"));

    EXPECT_EQ(false, dict_proxy.keyExists(Field("a")));
    EXPECT_EQ(true, dict_proxy.keyExists(Field((int64_t)10)));
}

// _ -> tuple
TEST(cJSONTest, TupleValsTest) {
    using namespace tuplex;
    using namespace std;

    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    // init tuple 1
    vector<Field> vec_1{ Field((int64_t)10), Field("a"), Field((double)30) };
    Tuple tup_1 = Tuple::from_vector(vec_1);

    // init tuple 2
    vector<Field> vec_2{ Field("a"), Field(true), Field((int64_t)30) };
    Tuple tup_2 = Tuple::from_vector(vec_2);

    // init tuple 3
    vector<Field> vec_3{ Field((double)3.14), Field((int64_t)2), Field(false) };
    Tuple tup_3 = Tuple::from_vector(vec_3);

    dict_proxy.putItem(Field((int64_t)10), Field(tup_1));

    EXPECT_EQ(Field(tup_1), dict_proxy.getItem(Field((int64_t)10)));

    dict_proxy.putItem(Field("a"), Field(tup_2));
    dict_proxy.replaceItem(Field((int64_t)10), Field(tup_3));

    EXPECT_EQ(Field(tup_2), dict_proxy.getItem(Field("a")));
    EXPECT_EQ(Field(tup_3), dict_proxy.getItem(Field((int64_t)10)));

    dict_proxy.deleteItem(Field("a"));

    EXPECT_EQ(false, dict_proxy.keyExists(Field("a")));
    EXPECT_EQ(true, dict_proxy.keyExists(Field((int64_t)10)));
}

TEST(cJSONTest, FloatIntDifferentiation) {
    using namespace tuplex;
    using namespace std;

    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    dict_proxy.putItem(Field((double)10), Field("a"));

    EXPECT_EQ(true, dict_proxy.keyExists(Field((double)10)));
    EXPECT_EQ(false, dict_proxy.keyExists(Field((int64_t)10)));

    dict_proxy.putItem(Field((int64_t)10), Field("b"));

    EXPECT_EQ(true, dict_proxy.keyExists(Field((double)10)));
    EXPECT_EQ(true, dict_proxy.keyExists(Field((int64_t)10)));
    EXPECT_EQ(Field("a"), dict_proxy.getItem(Field((double)10)));
    EXPECT_EQ(Field("b"), dict_proxy.getItem(Field((int64_t)10)));

    dict_proxy.replaceItem(Field((double)10), Field((int64_t)64));

    EXPECT_EQ(Field((int64_t)64), dict_proxy.getItem(Field((double)10)));
    EXPECT_EQ(Field("b"), dict_proxy.getItem(Field((int64_t)10)));

    dict_proxy.deleteItem(Field((double)10));
    
    EXPECT_EQ(false, dict_proxy.keyExists(Field((double)10)));
    EXPECT_EQ(true, dict_proxy.keyExists(Field((int64_t)10)));
}

TEST(cJSONTest, ListTupleDifferentiation) {
    using namespace tuplex;
    using namespace std;

    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    vector<Field> vec{ Field("a"), Field("a"), Field("a") };
    Tuple tup = Tuple::from_vector(vec);
    List lis = List::from_vector(vec);

    dict_proxy.putItem(Field("tup"), Field(tup));
    dict_proxy.putItem(Field("lis"), Field(lis));

    EXPECT_EQ(Field(tup), dict_proxy.getItem(Field("tup")));
    EXPECT_EQ(Field(lis), dict_proxy.getItem(Field("lis")));
    EXPECT_EQ(true, dict_proxy.getItem(Field("tup")).getType().isTupleType());
    EXPECT_EQ(true, dict_proxy.getItem(Field("lis")).getType().isListType());

    vector<Field> vec_2{ Field((double)3.14), Field((double)2.7), Field((double)1.414) };
    Tuple tup_2 = Tuple::from_vector(vec_2);
    List lis_2 = List::from_vector(vec_2);

    dict_proxy.replaceItem(Field("lis"), Field(tup_2));
    dict_proxy.replaceItem(Field("tup"), Field(lis_2));

    EXPECT_EQ(Field(tup_2), dict_proxy.getItem(Field("lis")));
    EXPECT_EQ(Field(lis_2), dict_proxy.getItem(Field("tup")));
    EXPECT_EQ(true, dict_proxy.getItem(Field("tup")).getType().isListType());
    EXPECT_EQ(true, dict_proxy.getItem(Field("lis")).getType().isTupleType());
}

TEST(cJSONTest, KeysValuesView) {
    using namespace tuplex;
    using namespace std;

    // initialise test dict
    codegen::cJSONDictProxyImpl dict_proxy;

    vector<Field> vec_1{ Field("a"), Field("b"), Field("c") };
    List lis = List::from_vector(vec_1);
    vector<Field> vec_2{ Field("a"), Field((int64_t)10), Field(true) };
    Tuple tup = Tuple::from_vector(vec_2);

    std::cout << tup.desc() << "\n";

    dict_proxy.putItem(Field((int64_t)15), Field("test"));
    dict_proxy.putItem(Field((double)3.14), Field((int64_t)15));
    dict_proxy.putItem(Field(false), Field((double)3.14));
    dict_proxy.putItem(Field(tup), Field(false));
    // dict_proxy.putItem(Field(lis), Field(tup));
    dict_proxy.putItem(Field("list"), Field(lis));

    vector<Field> keys{ Field((int64_t)15),
                        Field((double)3.14),
                        Field(false),
                        Field(tup),
                        // Field(lis),
                        Field("list")
                        };
    
    vector<Field> vals{ Field("test"),
                        Field((int64_t)15),
                        Field((double)3.14),
                        Field(false),
                        // Field(tup),
                        Field(lis)
                        };
    
    EXPECT_EQ(keys, dict_proxy.getKeysView());
    EXPECT_EQ(vals, dict_proxy.getValuesView());

    dict_proxy.replaceItem(Field((int64_t)15), Field((int64_t)150));
    dict_proxy.deleteItem(Field(false));
    dict_proxy.deleteItem(Field("list"));
    dict_proxy.putItem(Field("new"), Field(true));

    vector<Field> keys_2{ Field((int64_t)15),
                          Field((double)3.14),
                        //   Field(lis),
                          Field("list"),
                          Field("new") 
                          };
    
    vector<Field> vals_2{ Field((int64_t)150),
                          Field((int64_t)15),
                        //   Field(tup),
                          Field(lis),
                          Field(true) 
                          };

    EXPECT_EQ(keys_2, dict_proxy.getKeysView());
    EXPECT_EQ(vals_2, dict_proxy.getValuesView());
}