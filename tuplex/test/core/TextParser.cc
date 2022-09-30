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
    opt.set("tuplex.optimizer.nullValueOptimization", "false");
    auto ctxs = std::vector<std::unique_ptr<Context>>();
    ctxs.push_back(std::make_unique<Context>(opt));

    // test with null-value opt on/off
    opt.set("tuplex.optimizer.nullValueOptimization", "true");
    opt.set("tuplex.normalcaseThreshold", "0.5");
    ctxs.push_back(std::make_unique<Context>(opt));

    for(auto& c : ctxs) {

        std::cout<<"Testing with context using NVO: "<<std::boolalpha<<c->getOptions().OPT_NULLVALUE_OPTIMIZATION()<<std::endl;
        auto content = "hello\n"
                       "world\n"
                       "NULL\n"
                       "N/A\n"
                       "test\n"
                       "\n"
                       "how is everything?\n";
        stringToFile(testName + ".txt", content);

        auto res = c->text(testName + ".txt", vector<string>{"", "NULL", "N/A"}).collectAsVector();
        ASSERT_EQ(res.size(), 7);
        EXPECT_EQ(res[0].toPythonString(), "('hello',)");
        EXPECT_EQ(res[1].toPythonString(), "('world',)");
        EXPECT_EQ(res[2].toPythonString(), "(None,)");
        EXPECT_EQ(res[3].toPythonString(), "(None,)");
        EXPECT_EQ(res[4].toPythonString(), "('test',)");
        EXPECT_EQ(res[5].toPythonString(), "(None,)");
        EXPECT_EQ(res[6].toPythonString(), "('how is everything?',)");
    }
}