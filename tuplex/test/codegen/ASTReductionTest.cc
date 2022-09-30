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
#include <visitors/ReducableExpressionsVisitor.h>
#include <visitors/ReduceExpressionsVisitor.h>
#include "Helper.h"
#include <parser/Parser.h>

using namespace tuplex;

TEST(ReductionOpportunity, BasicI) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("1 + 2"));
    EXPECT_TRUE(containsReducableExpressions(ast.get()));
}

TEST(ReductionOpportunity, BasicII) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("1 - 2 * 8"));
    EXPECT_TRUE(containsReducableExpressions(ast.get()));
}

TEST(ReductionOpportunity, BasicIII) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("1 + (2 * x)"));
    EXPECT_FALSE(containsReducableExpressions(ast.get()));
}

TEST(ReductionOpportunity, BasicIV) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("'test' + 3"));
    EXPECT_TRUE(containsReducableExpressions(ast.get()));
}


TEST(ReductionOpportunity, BasicV) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("x + 7"));
    EXPECT_FALSE(containsReducableExpressions(ast.get()));
}

TEST(ReductionOpportunity, BasicVI) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("(1 + 2) * x"));
    EXPECT_TRUE(containsReducableExpressions(ast.get()));
}

TEST(ReductionOpportunity, BasicVII) {
    // more sophisticated algorithm might reorder this!!!
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("1 + 2 * x + 7"));
    EXPECT_FALSE(containsReducableExpressions(ast.get()));
}

TEST(ReductionOpportunity, BasicVIII) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("13 and 8 and 7 or 8"));
    EXPECT_TRUE(containsReducableExpressions(ast.get()));
}


//TEST(ReduceExpr, UnopI) {
//    auto ast = genAST("+10");
//    ReduceExpressionsVisitor v;
//    ast.get()->accept(v);
//
//    EXPECT_TRUE(v.getNumPerformedReductions() >= 1);
//}
//
//TEST(ReduceExpr, UnopII) {
//    auto ast = genAST("~10");
//    ReduceExpressionsVisitor v;
//    ast.get()->accept(v);
//
//    EXPECT_TRUE(v.getNumPerformedReductions() >= 1);
//}
//
//TEST(ReduceExpr, UnopIII) {
//    auto ast = genAST("-10");
//    ReduceExpressionsVisitor v;
//    ast.get()->accept(v);
//
//    EXPECT_TRUE(v.getNumPerformedReductions() >= 1);
//}
//
//TEST(ReduceExpr, UnopIV) {
//auto ast = genAST("10 * 'hello world'");
//ReduceExpressionsVisitor v;
//ast.get()->accept(v);
//
//EXPECT_TRUE(v.getNumPerformedReductions() >= 1);
//}

TEST(ReduceExpr, UnopV) {
    auto ast = std::unique_ptr<ASTNode>(tuplex::parseToAST("-10 * 39"));
    tuplex::ClosureEnvironment ce;
    ReduceExpressionsVisitor v(ce);
    ast.get()->accept(v);

    EXPECT_TRUE(v.getNumPerformedReductions() >= 1);
}

//TEST(ReduceExpr, UnopVI) {
//auto ast = genAST("'hello' * 10");
//ReduceExpressionsVisitor v;
//ast.get()->accept(v);
//
//EXPECT_TRUE(v.getNumPerformedReductions() >= 1);
//}