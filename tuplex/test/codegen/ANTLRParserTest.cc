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

#include <parser/Parser.h>

using namespace tuplex;

TEST(ANTLRParserTest, ParseLiteral) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("42"));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseSimpleExpressionI) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("42 + x"));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseSimpleExpressionII) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("42 + x * 4 | 3 ^ 1"));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTestI) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: x * 2 * 3 - 20 - 40 << 2 | 3 ^ 1 & 0"));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTest2) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: x.lower().upper().lower()"));
    EXPECT_TRUE(ast.get());
}
TEST(ANTLRParserTest, ParseTestIII) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: x.replace('hello', '').lower().upper().replace('jdhgjh', 'kfgjk')"));
    EXPECT_TRUE(ast.get());
}
TEST(ANTLRParserTest, ParseTestIV) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: '{:05}'.format(int(x['postal_code']))"));
    EXPECT_TRUE(ast.get());
}
TEST(ANTLRParserTest, ParseTestV) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: x['city'][0].upper() + x['city'][1:].lower()"));
    EXPECT_TRUE(ast.get());
}
TEST(ANTLRParserTest, ParseTestVI) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: x[1:20:2]"));
    EXPECT_TRUE(ast.get());
}
TEST(ANTLRParserTest, ParseTestVII) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: x[:10:2] + x[1:10:] + x[1::2]"));
    EXPECT_TRUE(ast.get());
}
TEST(ANTLRParserTest, ParseTestVIII) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: x[1:10:]"));
    EXPECT_TRUE(ast.get());
}
TEST(ANTLRParserTest, ParseTestIX) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: x[0]"));
    EXPECT_TRUE(ast.get());
}
TEST(ANTLRParserTest, ParseTestX) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: x[1:] + x[:20] + x[::-1]"));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTestXI) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: (x, 2, 3, (), (3, 5))"));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTestXII) {
    auto code = "def extractBa(x):\n"
            "    val = x['facts and features']\n"
            "    max_idx = val.find(' ba')\n"
            "    if max_idx < 0:\n"
            "        max_idx = len(val)\n"
            "    s = val[:max_idx]\n"
            "\n"
            "    # find comma before\n"
            "    split_idx = s.rfind(',')\n"
            "    if split_idx < 0:\n"
            "        split_idx = 0\n"
            "    else:\n"
            "        split_idx += 2\n"
            "    r = s[split_idx:]\n"
            "    return int(r)";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTestXIII) {
    auto code = "def extractOffer(x):\n"
            "    offer = x['title']\n"
            "    assert 'Sale' in offer or 'Rent' in offer or 'SOLD' in offer\n"
            "\n"
            "    if 'Sale' in offer:\n"
            "        offer = 'sale'\n"
            "    elif 'Rent' in offer:\n"
            "        offer = 'rent'\n"
            "    elif 'SOLD' in offer:\n"
            "        offer = 'sold'\n"
            "    elif 'foreclose' in offer.lower():\n"
            "        offer = 'foreclosed'\n"
            "\n"
            "    return offer";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTestXIV) {
    auto code = "def f(x):\n\ty = z = x + 2\n\treturn y";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTestXV) {
    auto code = "def f(x):\n\ty = 20\n\ty += x\n\treturn y";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTestXVI) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: {'a':1, 'b':2, 'c':3, 'd':(1, 2, 3), 'e':True}"));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTestXVII) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: [1, 2, 'a', 'b', True]"));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseTestXVIII) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("lambda x: [y for y in range(x)]"));
    EXPECT_TRUE(ast.get());
}

TEST(ANTLRParserTest, ParseAttributeChain) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("os.path.join"));
    EXPECT_TRUE(ast.get());
    auto attr_I = dynamic_cast<NAttribute*>(ast.get()); ASSERT_TRUE(attr_I);
    EXPECT_EQ(attr_I->type(), ASTNodeType::Attribute);
    ASSERT_TRUE(attr_I->_value);
    EXPECT_EQ(attr_I->_value->type(), ASTNodeType::Attribute);
    ASSERT_TRUE(attr_I->_attribute);
}

TEST(ANTLRPParserTest, ParseTestForStmt) {
    auto code = "for i in [1, 2, 3, 4]:\n"
                "    num = num + i\n";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());
    auto for_node = dynamic_cast<NFor*>(ast.get());
    ASSERT_TRUE(for_node);
    EXPECT_EQ(for_node->type(), ASTNodeType::For);
    EXPECT_EQ(for_node->suite_body->type(), ASTNodeType::Suite);
    EXPECT_EQ(for_node->suite_else, nullptr);
}

TEST(ANTLRPParserTest, ParseTestForElse) {
    auto code = "for i in range(10):\n"
                "    num = num + i\n"
                "else:\n"
                "    num = num * 2";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());
    auto for_node = dynamic_cast<NFor*>(ast.get());
    ASSERT_TRUE(for_node);
    EXPECT_EQ(for_node->type(), ASTNodeType::For);
    EXPECT_EQ(for_node->suite_body->type(), ASTNodeType::Suite);
    EXPECT_EQ(for_node->suite_else->type(), ASTNodeType::Suite);
}

TEST(ANTLRPParserTest, ParseTestContinue) {
    auto code = "for i in range(10):\n"
                "    if i == 5:\n"
                "        continue\n"
                "    num = num + i\n";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());

    auto for_node = dynamic_cast<NFor*>(ast.get());
    ASSERT_TRUE(for_node);
    EXPECT_EQ(for_node->type(), ASTNodeType::For);
    auto suite_node = dynamic_cast<NSuite*>(for_node->suite_body.get());
    ASSERT_TRUE(suite_node);
    auto if_node = dynamic_cast<NIfElse*>(suite_node->_statements[0].get());
    ASSERT_TRUE(if_node);
    auto then_node = dynamic_cast<NSuite*>(if_node->_then.get());
    ASSERT_TRUE(then_node);
    EXPECT_EQ(then_node->_statements[0]->type(), ASTNodeType::Continue);
}

TEST(ANTLRPParserTest, ParseTestBreak) {
    auto code = "for i in range(10):\n"
                "    if i == 8:\n"
                "        break";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());

    auto for_node = dynamic_cast<NFor*>(ast.get());
    ASSERT_TRUE(for_node);
    EXPECT_EQ(for_node->type(), ASTNodeType::For);
    auto suite_node = dynamic_cast<NSuite*>(for_node->suite_body.get());
    ASSERT_TRUE(suite_node);
    auto if_node = dynamic_cast<NIfElse*>(suite_node->_statements[0].get());
    ASSERT_TRUE(if_node);
    auto then_node = dynamic_cast<NSuite*>(if_node->_then.get());
    ASSERT_TRUE(then_node);
    EXPECT_EQ(then_node->_statements[0]->type(), ASTNodeType::Break);
}

TEST(ANTLRPParserTest, ParseTestWhile1) {
    auto code = "while i < 10:\n"
                "    if val[i] == 'a':\n"
                "        break\n"
                "    i = i + 1\n"
                "else:\n"
                "    found = 0";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());

    auto while_node = dynamic_cast<NWhile*>(ast.get());
    ASSERT_TRUE(while_node);
    EXPECT_EQ(while_node->type(), ASTNodeType::While);
    auto suite_node = dynamic_cast<NSuite*>(while_node->suite_body.get());
    ASSERT_TRUE(suite_node);
    EXPECT_EQ(suite_node->type(), ASTNodeType::Suite);
    auto if_node = dynamic_cast<NIfElse*>(suite_node->_statements[0].get());
    ASSERT_TRUE(if_node);
    auto then_node = dynamic_cast<NSuite*>(if_node->_then.get());
    ASSERT_TRUE(then_node);
    EXPECT_EQ(then_node->_statements[0]->type(), ASTNodeType::Break);
    ASSERT_TRUE(while_node->suite_else);
    EXPECT_EQ(while_node->suite_else->type(), ASTNodeType::Suite);
}

TEST(ANTLRPParserTest, ParseTestWhile2) {
    auto code = "while x < 20:\n"
                "    x = x + 1\n"
                "    if x < 10:\n"
                "        continue\n"
                "    y = y * x\n";
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    EXPECT_TRUE(ast.get());

    auto while_node = dynamic_cast<NWhile*>(ast.get());
    ASSERT_TRUE(while_node);
    EXPECT_EQ(while_node->type(), ASTNodeType::While);
    auto suite_node = dynamic_cast<NSuite*>(while_node->suite_body.get());
    ASSERT_TRUE(suite_node);
    auto if_node = dynamic_cast<NIfElse*>(suite_node->_statements[1].get());
    ASSERT_TRUE(if_node);
    auto then_node = dynamic_cast<NSuite*>(if_node->_then.get());
    ASSERT_TRUE(then_node);
    EXPECT_EQ(then_node->_statements[0]->type(), ASTNodeType::Continue);
    EXPECT_EQ(while_node->suite_else, nullptr);
}