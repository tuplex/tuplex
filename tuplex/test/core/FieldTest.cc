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
#include <Field.h>
#include <Tuple.h>
#include <List.h>
#include <string>

using namespace tuplex;

TEST(Field, Basics) {
    EXPECT_EQ(Field((int64_t)10).desc(), std::string("10"));
    EXPECT_EQ(Field(10.5).desc(), std::string("10.50000"));
    EXPECT_EQ(Field(true).desc(), std::string("True"));
    EXPECT_EQ(Field(false).desc(), std::string("False"));
    EXPECT_EQ(Field("Hello world!").desc(), std::string("'Hello world!'"));

    // copy & assign operators
    auto f = Field(10.125);
    auto f2(f);
    auto f3 = Field(true);
    f3 = f;
    EXPECT_EQ(f.desc(), std::string("10.12500"));
    EXPECT_EQ(f2.desc(), std::string("10.12500"));
    EXPECT_EQ(f3.desc(), std::string("10.12500"));
}


TEST(Field, Tuple) {
    auto empty = Tuple();
    EXPECT_EQ(empty.desc(), std::string("()"));
    EXPECT_EQ(Tuple(10.5, true).desc(), std::string("(10.50000,True)"));
    EXPECT_EQ(Tuple(10.5, Tuple(false), true).desc(), std::string("(10.50000,(False,),True)"));
}

TEST(Field, List) {
    auto empty = List();
    EXPECT_EQ(empty.desc(), std::string("[]"));
    EXPECT_EQ(List(10.5, 11.5).desc(), std::string("[10.50000,11.50000]"));
    EXPECT_EQ(List("hello", "world").desc(), std::string("['hello','world']"));
    EXPECT_EQ(List(List(), List()).desc(), std::string("[[],[]]"));
}

TEST(Field, Constants) {
    using namespace tuplex;
    auto f1 = constantTypeToField(python::Type::makeConstantValuedType(python::Type::STRING, "hello world"));
    EXPECT_EQ(f1.toPythonString(), "'hello world'");
    auto f2 = constantTypeToField(python::Type::makeConstantValuedType(python::Type::I64, "-1234"));
    EXPECT_EQ(f2.getInt(), -1234);
    auto f3 = constantTypeToField(python::Type::makeConstantValuedType(python::Type::F64, "-3.001"));
    EXPECT_EQ(f3.getDouble(), -3.001);
}