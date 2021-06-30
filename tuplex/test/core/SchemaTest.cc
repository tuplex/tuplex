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
#include "Schema.h"

TEST(Schema, FixedSize) {
    using namespace tuplex;

    EXPECT_TRUE(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::I64})).hasFixedSize());
}