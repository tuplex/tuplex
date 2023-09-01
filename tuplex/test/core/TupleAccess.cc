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
#include <Context.h>
#include "../../utils/include/Utils.h"
#include "TestUtils.h"
#include "jit/RuntimeInterface.h"

// need for these tests a running python interpreter, so spin it up
class TupleAccess : public PyTest {};


TEST_F(TupleAccess, IntegerIndex) {
    using namespace tuplex;
    Context c(microTestOptions());

    // skip this test for now, until better error mgmt is implemented!
    GTEST_SKIP_("skip, till better error mgmt (static analysis) is added.");

    // this should not work b.c. single element and NOT a tuple...
    // ==> i.e. produce meaningful message!
    auto v1 = c.parallelize({Row(10), Row(20), Row(30)}).map(UDF("lambda x: x[0]")).collectAsVector();

    // @TODO: fix in docker container the exception code thingy. ==> I.e. develop on ubuntu on desktop at home!


    // should find this string in err messages
    auto needle = std::string("subscript operation [] is performed on unsupported type");

    // search within logstream
    auto log = logStream.str();

    EXPECT_NE(log.find(needle), std::string::npos); // make sure this err string is there.
}

TEST_F(TupleAccess, SingleColumnKeyIndex) {

    // special case: what if it is a single column and then access via t['column'] be allowed?
    // ==> named access should be probably fine. However t[idx] with ints etc. leads to confusion..
    // make sure inferred type is not dict!
    // "lambda t: {'res' : t['name']}" ==> should get resolved if rowtype is not dict to lambda t: {'res' : t}
    using namespace tuplex;

    Context c(microTestOptions());

    auto& ds =  c.parallelize({Row("hello"), Row("world")}, {"name"})
            .map(UDF("lambda t: {'res' : t['name']}"));
    auto v =  ds
              .collectAsVector();

    // column test
    ASSERT_EQ(ds.columns().size(), 1);
    EXPECT_EQ(ds.columns().front(), "res");

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], Row("hello"));
    EXPECT_EQ(v[1], Row("world"));
}

TEST_F(TupleAccess, MultiColumnKeyIndex) {
    // multicol access?

    using namespace tuplex;

    Context c(microTestOptions());

    auto& ds = c.parallelize({Row("hello", "world"), Row("tux", "sealion")}, {"a", "b"});

    auto v1 = ds.map(UDF("lambda x: (x['b'], x['a'])")).collectAsVector();
    auto v2 = ds.filter(UDF("lambda x: x['b'] == 'sealion'")).collectAsVector();
    auto code = "def f(x):\n"
                "\treturn x['a'] + '_str'\n";
    auto v3 = ds.map(UDF(code)).collectAsVector();

    ASSERT_EQ(v1.size(), 2);
    ASSERT_EQ(v2.size(), 1);
    ASSERT_EQ(v3.size(), 2);

    EXPECT_EQ(v1[0], Row("world", "hello"));
    EXPECT_EQ(v1[1], Row("sealion", "tux"));

    EXPECT_EQ(v2[0], Row("tux", "sealion"));

    EXPECT_EQ(v3[0], Row("hello_str"));
    EXPECT_EQ(v3[1], Row("tux_str"));
}