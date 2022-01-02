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

class TextParse : public PyTest {};

using namespace tuplex;
using namespace std;

TEST_F(TextParse, Basic) {
    // simple test where default file type is str
    auto opt = microTestOptions();
    opt.set("tuplex.optimizer.generateParser", "false");
    Context c(opt);

    auto content = "hello\n"
                   "how\n"
                   "\n"
                   "is everything?"; // note: last line not delimited
    stringToFile(testName + ".txt", content);

    auto res = c.text(testName + ".txt").collectAsVector();
    ASSERT_EQ(res.size(), 4);
    EXPECT_EQ(res[0].toPythonString(), "('hello',)");
    EXPECT_EQ(res[1].toPythonString(), "('how',)");
    EXPECT_EQ(res[2].toPythonString(), "('',)");
    EXPECT_EQ(res[3].toPythonString(), "('is everything?',)");
}

TEST_F(TextParse, NULLValues) {
    auto opt = microTestOptions();
    opt.set("tuplex.optimizer.generateParser", "false");
    Context c(opt);

    auto content = "hello\n"
                   "world\n"
                   "NULL\n"
                   "N/A\n"
                   "test\n"
                   "\n"
                   "how is everything?\n";
    stringToFile(testName + ".txt", content);

    auto res = c.text(testName + ".txt", vector<string>{"", "NULL", "N/A"}).collectAsVector();
    ASSERT_EQ(res.size(), 7);
    EXPECT_EQ(res[0].toPythonString(), "('hello',)");
    EXPECT_EQ(res[1].toPythonString(), "('world',)");
    EXPECT_EQ(res[2].toPythonString(), "(None,)");
    EXPECT_EQ(res[3].toPythonString(), "(None,)");
    EXPECT_EQ(res[4].toPythonString(), "('test',)");
    EXPECT_EQ(res[5].toPythonString(), "(None,)");
    EXPECT_EQ(res[6].toPythonString(), "('how is everything?',)");
}