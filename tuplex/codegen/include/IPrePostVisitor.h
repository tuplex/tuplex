//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IPREPOSTVISITOR_H
#define TUPLEX_IPREPOSTVISITOR_H

#include <ApatheticVisitor.h>

namespace tuplex {
    /*!
 * abstract class to define simple visitors executing
 * the same postorder/preorder function on each node
 */
    class IPrePostVisitor : public ApatheticVisitor {
    protected:

        virtual void postOrder(ASTNode *node) = 0;
        virtual void preOrder(ASTNode *node) = 0;

    public:

        // leaf nodes
        virtual void visit(NNone* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NNumber* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NIdentifier* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NBoolean* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NEllipsis* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NString* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }

        // non-leaf nodes, recursive calls are carried out for these
        virtual void visit(NParameter* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NParameterList* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NFunction* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }

        virtual void visit(NBinaryOp* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NUnaryOp* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NSuite* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NModule* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NLambda* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NAwait* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NStarExpression* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NCompare* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NIfElse* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NTuple * n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NDictionary * n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NList * n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }

        virtual void visit(NSubscription* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NReturn* n) override {preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NAssign* n) override {preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NCall* n) override {preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NAttribute* n) override {preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NSlice* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NSliceItem* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }

        virtual void visit(NRange* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NComprehension* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NListComprehension* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }

        virtual void visit(NAssert* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NRaise* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }

        virtual void visit(NWhile* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NFor* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NBreak* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
        virtual void visit(NContinue* n) override { preOrder(n); ApatheticVisitor::visit(n); postOrder(n); }
    };
}

#endif //TUPLEX_IPREPOSTVISITOR_H