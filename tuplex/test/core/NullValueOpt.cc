//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "TestUtils.h"
#include <Context.h>
#include <PythonHelpers.h>

// need for these tests a running python interpreter, so spin it up
class NVOTest : public PyTest {};

// these tests here are meant to check that features work in Tuplex when involving null-value optimization
// i.e. partial specialization
TEST_F(NVOTest, JoinWithNVO) {
    using namespace tuplex;

    // create two test files (one smaller)
    // then force NVO on them


    // @TODO

    // also test for left-join!
}

// test involving aggregates: => unique, general, bykey.