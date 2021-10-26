
#include "gtest/gtest.h"
#include <Context.h>
#include "TestUtils.h"
#include <random>

#include "gtest/gtest.h"
#include <AnnotatedAST.h>
#include <graphviz/GraphVizGraph.h>
#include "../../codegen/include/parser/Parser.h"

class IsKeywordTest : public PyTest {};

/*
    Tests for codegen support for `is` keyword (only for True, False and None types).
*/

TEST_F(IsKeywordTest, OptionIsBool) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(option<bool>(true)));
    Row row2(Field(option<bool>::none));
    Row row3(Field(option<bool>(false)));

    auto code = "lambda x: x is True";
    auto m = c.parallelize({row1, row2, row3})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 3);
    assert(m[0].toPythonString() == "(True,)");
    for(int i = 1; i < m.size(); i++) {
        assert(m[i].toPythonString() == "(False,)");
    }
}

TEST_F(IsKeywordTest, OptionIsNotBool) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(option<bool>(true)));
    Row row2(Field(option<bool>(false)));
    Row row3(Field(option<bool>::none));

    auto code = "lambda x: x is not True";
    auto m = c.parallelize({row1, row2, row3})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 3);
    assert(m[0].toPythonString() == "(False,)");
    for(int i = 1; i < m.size(); i++) {
        assert(m[i].toPythonString() == "(True,)");
    }
}


TEST_F(IsKeywordTest, BoolIsBool) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(true));
    Row row2(Field(false));
    Row row3(Field(false));
    Row row4(Field(false));

    auto code = "lambda x: x is True";
    auto m = c.parallelize({row1, row2, row3, row4})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 4);
    assert(m[0].toPythonString() == "(True,)");
    assert(m[1].toPythonString() == "(False,)");
    assert(m[2].toPythonString() == "(False,)");
    assert(m[3].toPythonString() == "(False,)");
}

TEST_F(IsKeywordTest, BoolIsNotBool) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(true));
    Row row2(Field(false));
    Row row3(Field(false));
    Row row4(Field(true));

    auto code = "lambda x: x is not True";
    auto m = c.parallelize({row1, row2, row3, row4})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 4);
    assert(m[0].toPythonString() == "(False,)");
    assert(m[1].toPythonString() == "(True,)");
    assert(m[2].toPythonString() == "(True,)");
    assert(m[3].toPythonString() == "(False,)");
}


TEST_F(IsKeywordTest, NoneIsNone) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(option<int64_t>(10)));
    Row row2(Field(option<int64_t>::none));

    auto code = "lambda x: x is None";
    auto m = c.parallelize({row1, row2})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 2);
    assert(m[0].toPythonString() == "(False,)");
    assert(m[1].toPythonString() == "(True,)");
}

TEST_F(IsKeywordTest, NoneIsNotNone) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(option<int32_t>(-1)));
    Row row2(Field(option<int32_t>(45)));
    Row row3(Field(option<int32_t>::none));
    Row row4(Field::null());

    auto code = "lambda x: x is not None";
    auto m = c.parallelize({row1, row2, row3})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 3);

    assert(m[0].toPythonString() == "(True,)");
    assert(m[1].toPythonString() == "(True,)");
    assert(m[2].toPythonString() == "(False,)");
}

TEST_F(IsKeywordTest, StringIsNotNone) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(option<std::string>("hello")));
    Row row2(Field(option<std::string>::none));

    auto code = "lambda x: x is not None";
    auto m = c.parallelize({row1, row2})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 2);

    assert(m[0].toPythonString() == "(True,)");
    assert(m[1].toPythonString() == "(False,)");
}

TEST_F(IsKeywordTest, StringIsBool) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(option<std::string>("hello")));
    Row row2(Field(option<std::string>::none));

    auto code = "lambda x: x is True";
    auto m = c.parallelize({row1, row2})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 2);

    assert(m[0].toPythonString() == "(False,)");
    assert(m[1].toPythonString() == "(False,)");
}

TEST_F(IsKeywordTest, FailingTest) {
    using namespace tuplex;

    Context c(microTestOptions());

    // reset log
    logStream.str("");

    Row row1(45);
    Row row2(false);
    Row row3("hi");

    auto code = "lambda x: x is \"hi\"";

    auto node = UDF(code);

    auto m = c.parallelize({row1, row2, row3})
            .map(node).collectAsVector();

    auto log = logStream.str();
    EXPECT_NE(log.find("use of is comparison only supported with types"), std::string::npos);
}
