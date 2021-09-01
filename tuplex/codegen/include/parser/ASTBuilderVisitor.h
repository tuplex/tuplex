//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ASTBUILDERVISITOR_H
#define TUPLEX_ASTBUILDERVISITOR_H

#include <vector>
#include <string>

#include <antlr4-runtime/antlr4-runtime.h>

// generated sources
#include <Python3Parser.h>
#include <Python3BaseVisitor.h>
#include <ASTNodes.h>

namespace tuplex {

    class ASTBuilderVisitor : public  antlr4::Python3BaseVisitor {
    private:
        std::vector<ASTNode*> nodes;

        void clearStack() {
            for(auto node : nodes)
                delete node;
            nodes.clear();
        }

        void error(const std::string& message) {
            // clear stack
            clearStack();

            // throw error (to jump out of visitor) - this is a bad hack
            throw message;
        }


        void pushNode(ASTNode* node) {
            nodes.emplace_back(node);
        }

        ASTNode* popNode() {
            ASTNode* node = nodes.back();
            nodes.pop_back();
            return node;
        }

        // helper function to construct binary operation
        void constructBinaryOperation(const std::vector<antlr4::tree::ParseTree*>& children);
    public:

        ASTBuilderVisitor() = default;

        ~ASTBuilderVisitor() { clearStack(); }

        ASTNode* getAST() {
            return popNode();
        }


        // below functions should be captured...


        virtual antlrcpp::Any visitAtom(antlr4::Python3Parser::AtomContext *ctx) override;
        // unary operations +, -, ~
        virtual antlrcpp::Any visitFactor(antlr4::Python3Parser::FactorContext *ctx) override;

        // binary operations *, @,
        virtual antlrcpp::Any visitTerm(antlr4::Python3Parser::TermContext *ctx) override;
        virtual antlrcpp::Any visitArith_expr(antlr4::Python3Parser::Arith_exprContext *ctx) override;
        virtual antlrcpp::Any visitShift_expr(antlr4::Python3Parser::Shift_exprContext *ctx) override;
        virtual antlrcpp::Any visitExpr(antlr4::Python3Parser::ExprContext *ctx) override;
        virtual antlrcpp::Any visitXor_expr(antlr4::Python3Parser::Xor_exprContext *ctx) override;
        virtual antlrcpp::Any visitAnd_expr(antlr4::Python3Parser::And_exprContext *ctx) override;
        virtual antlrcpp::Any visitTrailer(antlr4::Python3Parser::TrailerContext *ctx) override;
        virtual antlrcpp::Any visitAtom_expr(antlr4::Python3Parser::Atom_exprContext *ctx) override;
        virtual antlrcpp::Any visitArgument(antlr4::Python3Parser::ArgumentContext *ctx) override;
        virtual antlrcpp::Any visitSubscriptlist(antlr4::Python3Parser::SubscriptlistContext *ctx) override;
        virtual antlrcpp::Any visitSliceop(antlr4::Python3Parser::SliceopContext *ctx) override;
        virtual antlrcpp::Any visitSubscript(antlr4::Python3Parser::SubscriptContext *ctx) override;
        virtual antlrcpp::Any visitStar_expr(antlr4::Python3Parser::Star_exprContext *ctx) override;
        virtual antlrcpp::Any visitComparison(antlr4::Python3Parser::ComparisonContext *ctx) override;
        virtual antlrcpp::Any visitOr_test(antlr4::Python3Parser::Or_testContext *ctx) override;
        virtual antlrcpp::Any visitAnd_test(antlr4::Python3Parser::And_testContext *ctx) override;
        virtual antlrcpp::Any visitNot_test(antlr4::Python3Parser::Not_testContext *ctx) override;
        virtual antlrcpp::Any visitTest(antlr4::Python3Parser::TestContext *ctx) override;
        virtual antlrcpp::Any visitLambdef(antlr4::Python3Parser::LambdefContext *ctx) override;
        virtual antlrcpp::Any visitLambdef_nocond(antlr4::Python3Parser::Lambdef_nocondContext *ctx) override;
        virtual antlrcpp::Any visitTypedargslist(antlr4::Python3Parser::TypedargslistContext *ctx) override;
        virtual antlrcpp::Any visitTfpdef(antlr4::Python3Parser::TfpdefContext *ctx) override;

        // this here is the most complex one because of all the weird options in python...
        virtual antlrcpp::Any visitVarargslist(antlr4::Python3Parser::VarargslistContext *ctx) override;
        virtual antlrcpp::Any visitFuncdef(antlr4::Python3Parser::FuncdefContext *ctx) override;
        virtual antlrcpp::Any visitParameters(antlr4::Python3Parser::ParametersContext *ctx) override;
        virtual antlrcpp::Any visitSuite(antlr4::Python3Parser::SuiteContext *ctx) override;

        // simply emit identifier
        virtual antlrcpp::Any visitVfpdef(antlr4::Python3Parser::VfpdefContext *ctx) override;

        // tuple construction, ...
        virtual antlrcpp::Any visitTestlist_comp(antlr4::Python3Parser::Testlist_compContext *ctx) override;
        virtual antlrcpp::Any visitTestlist_star_expr(antlr4::Python3Parser::Testlist_star_exprContext *ctx) override;
        virtual antlrcpp::Any visitReturn_stmt(antlr4::Python3Parser::Return_stmtContext *ctx) override;

        // dictionary construction
        virtual antlrcpp::Any visitDictorsetmaker(antlr4::Python3Parser::DictorsetmakerContext *ctx) override;

        // assign stmt
        virtual antlrcpp::Any visitExpr_stmt(antlr4::Python3Parser::Expr_stmtContext *ctx) override;

        // if elif else stmt
        virtual antlrcpp::Any visitIf_stmt(antlr4::Python3Parser::If_stmtContext *ctx) override;

        virtual antlrcpp::Any visitPower(antlr4::Python3Parser::PowerContext *ctx) override;

        // deal with the case when expression (also called testlist) in for loop contains multiple elements
        virtual antlrcpp::Any visitTestlist(antlr4::Python3Parser::TestlistContext *ctx) override;

        // list comprehensions
        virtual antlrcpp::Any visitExprlist(antlr4::Python3Parser::ExprlistContext *context) override;
//        virtual antlrcpp::Any visitComp_iter(antlr4::Python3Parser::Comp_iterContext *context) override;
        virtual antlrcpp::Any visitComp_for(antlr4::Python3Parser::Comp_forContext *context) override;
//        virtual antlrcpp::Any visitComp_if(Python3Parser::Comp_ifContext *context) override;


        virtual antlrcpp::Any visitAssert_stmt(antlr4::Python3Parser::Assert_stmtContext *ctx) override;

        virtual antlrcpp::Any visitRaise_stmt(antlr4::Python3Parser::Raise_stmtContext *ctx) override;

        // @TODO: Loop support, override antlr visitor to generate AST Nodes for
        //        For, while, break, continue...
        virtual antlrcpp::Any visitFor_stmt(antlr4::Python3Parser::For_stmtContext *ctx) override;

        virtual antlrcpp::Any visitWhile_stmt(antlr4::Python3Parser::While_stmtContext *ctx) override;

        virtual antlrcpp::Any visitBreak_stmt(antlr4::Python3Parser::Break_stmtContext *ctx) override;

        virtual antlrcpp::Any visitContinue_stmt(antlr4::Python3Parser::Continue_stmtContext *ctx) override;
    };
}

#endif //TUPLEX_ASTBUILDERVISITOR_H