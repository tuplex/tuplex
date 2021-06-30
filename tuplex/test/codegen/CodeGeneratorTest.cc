//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <string>
#include <AnnotatedAST.h>
#include "gtest/gtest.h"


// Code Generator is more advanced now and restricts to lambda functions.
// following tests need to be undocumented as soon as parser succeeds
//// following are tests to just see whether parsing succeeded
//TEST(AnnotatedAST, atom) {
//    AnnotatedAST cg;
//    EXPECT_TRUE(cg.parseString("hello")); // IDENTIFIER
//    EXPECT_TRUE(cg.parseString("10")); // NUMBER
//    EXPECT_TRUE(cg.parseString("'hello world!'")); // STRING
//    EXPECT_TRUE(cg.parseString("'hello world!' 'how are you?'")); // STRING+
//    EXPECT_TRUE(cg.parseString("...")); // ELLIPSIS
//    EXPECT_TRUE(cg.parseString("None")); // None
//    EXPECT_TRUE(cg.parseString("True")); // True
//    EXPECT_TRUE(cg.parseString("False")); // False
//}
//
//
//TEST(AnnotatedAST, term) {
//    AnnotatedAST cg;
//    EXPECT_TRUE(cg.parseString("10 * 20"));
//    EXPECT_TRUE(cg.parseString("10 / 20"));
//    // no primitive type implements this operation...
//    // it is merely for overloading...
//    //EXPECT_TRUE(cg.parseString("10 @ 20"));
//
//    EXPECT_TRUE(cg.parseString("10 % 20"));
//
//    EXPECT_TRUE(cg.parseString("2 * 3 * 4"));
//    EXPECT_TRUE(cg.parseString("2 + 10 * 20"));
//    EXPECT_TRUE(cg.parseString("10 * 20 + 20"));
//    EXPECT_TRUE(cg.parseString("10 * 20 - 20"));
//
////    EXPECT_TRUE(cg.parseString("(12) / sqrt(10)"));
////    EXPECT_TRUE(cg.parseString("4 * pi + (1 + 3 * x) / sqrt(7 // 8)"));
////
////    // fun fact about python3: ;; is illegal! also ; is not a valid statement. In C++ it is...
//    //EXPECT_TRUE(cg.parseString("4 + 3; 7 + 9; 13 * y46758;1, 2, 3;1,2,3,"));
//}