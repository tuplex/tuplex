
#include "gtest/gtest.h"
#include <Context.h>
#include "TestUtils.h"
#include <random>

#include "gtest/gtest.h"
#include <AnnotatedAST.h>
#include <graphviz/GraphVizGraph.h>
#include "../../codegen/include/parser/Parser.h"

class IsKeywordTest : public PyTest {};

// bool saveToPDFx(tuplex::ASTNode* root, const std::string& path) {
//     using namespace tuplex;
//     assert(root);
//     GraphVizGraph graph;
//     graph.createFromAST(root);
//     return graph.saveAsPDF(path);
// }

// // works
// TEST_F(IsKeywordTest, xIsNone) {
//     using namespace tuplex;
//     auto code = "lambda x: x is None";
//     auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
//     ASSERT_TRUE(node);
//     printParseTree(code, std::cout);
//     saveToPDFx(node.get(), "xIsNone.pdf");
// }

TEST_F(IsKeywordTest, OptionBoolIsBool) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(option<bool>(true)));
    Row row2(Field(option<bool>(true)));
    Row row3(Field(option<bool>(true)));
    Row row4(Field(option<bool>(true)));

    auto code = "lambda x: x is True";
    auto m = c.parallelize({row1, row2, row3, row4})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 4);
    for(int i = 0; i < m.size(); i++) {
        assert(m[i].toPythonString() == "(True,)");
    }
}

TEST_F(IsKeywordTest, OptionBoolIsNotBool) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(option<bool>(true)));
    Row row2(Field(option<bool>(true)));
    Row row3(Field(option<bool>(true)));
    Row row4(Field(option<bool>(true)));

    auto code = "lambda x: x is not True";
    auto m = c.parallelize({row1, row2, row3, row4})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 4);
    for(int i = 0; i < m.size(); i++) {
        assert(m[i].toPythonString() == "(False,)");
    }
}


TEST_F(IsKeywordTest, BoolIsBool) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field(true));
    Row row2(Field(false));
    Row row3(Field(false));
    Row row4(Field(true));

    auto code = "lambda x: x is True";
    auto m = c.parallelize({row1, row2, row3, row4})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 4);
    assert(m[0].toPythonString() == "(True,)");
    assert(m[1].toPythonString() == "(False,)");
    assert(m[2].toPythonString() == "(False,)");
    assert(m[3].toPythonString() == "(True,)");
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
    Row row1(Field::null());
    Row row2(Field::null());
    Row row3(Field::null());
    Row row4(Field::null());

    auto code = "lambda x: x is None";
    auto m = c.parallelize({row1, row2, row3, row4})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 4);
    for(int i = 0; i < m.size(); i++) {
        assert(m[i].toPythonString() == "(True,)");
    }
}

TEST_F(IsKeywordTest, NoneIsNotNone) {
    using namespace tuplex;

    Context c(microTestOptions());
    Row row1(Field::null());
    Row row2(Field::null());
    Row row3(Field::null());
    Row row4(Field::null());

    auto code = "lambda x: x is not None";
    auto m = c.parallelize({row1, row2, row3, row4})
            .map(UDF(code)).collectAsVector();

    EXPECT_EQ(m.size(), 4);
    for(int i = 0; i < m.size(); i++) {
        assert(m[i].toPythonString() == "(False,)");
    }
}

// this test is invalid because codegen will only work for one of the types.
// TEST_F(IsKeywordTest, MixedIsNotNone) {
//     using namespace tuplex;

//     Context c(microTestOptions());
//     Row row1(Field::null());
//     Row row2(Field(true));
//     Row row3(Field::null());
//     Row row4(Field(false));

//     auto code = "lambda x: x is not None";
//     auto m = c.parallelize({row1, row2, row3, row4})
//             .map(UDF(code)).collectAsVector();

//     EXPECT_EQ(m.size(), 4);
//     assert(m[0].toPythonString() == "(False,)");
//     assert(m[1].toPythonString() == "(True,)");
//     assert(m[2].toPythonString() == "(False,)");
//     assert(m[3].toPythonString() == "(True,)");
// }


/*
    Even though a std::runtime_error is thrown in BlockGeneratorVisitor.cc, it
    doesn't get caught in catch(...) for some reason.
*/


// TEST_F(IsKeywordTest, AnyIsNone) {
//     using namespace tuplex;

//     // Logger::instance().defaultLogger().warn("asdgasdjlkgalksdgjalksdjgalksdjgas");

//     Context c(microTestOptions());
//     Row row1(Field(option<int64_t>(5)));
//     Row row2(Field("hi"));
//     Row row3(Field(0.55));
//     Row row4(Field("0.55"));

//     auto code = "lambda x: x is None";

//     try {
//         auto udf = UDF(code);
//     } catch(const std::exception& e) {
//         std::string s = e.what();
//         Logger::instance().defaultLogger().error("exception string is this: " + s);
//     } catch(...) {
//         Logger::instance().defaultLogger().error("hmm, different exception occured");
//     }

//     auto m = c.parallelize({row1, row2, row3, row4})
//         .map(UDF(code)).collectAsVector();

// }

// TEST_F(IsKeywordTest, AnyIsNotNone) {
//     using namespace tuplex;

//     Context c(microTestOptions());
//     Row row1(Field(option<int64_t>(5)));
//     Row row2(Field("hi"));
//     Row row3(Field(0.55));
//     Row row4(Field("0.55"));

//     auto code = "lambda x: x is not None";
//     auto m = c.parallelize({row1, row2, row3, row4})
//         .map(UDF(code)).collectAsVector();

//     EXPECT_EQ(m.size(), 0);
// }
