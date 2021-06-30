//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <Pipe.h>

TEST(Pipe, CallPython) {
    Pipe pipe("python3 -c print('hello')");
    pipe.pipe();
    EXPECT_EQ(pipe.stdout(), "hello\n");

}

TEST(Pipe, CallPythonWithFile) {
    Pipe pipe("python3");
    pipe.pipe("print('hello')\nprint('world!')");
    EXPECT_EQ(pipe.stdout(), "hello\nworld!\n");
}