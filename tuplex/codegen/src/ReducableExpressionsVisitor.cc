//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ReducableExpressionsVisitor.h>

namespace tuplex {
    bool containsReducableExpressions(ASTNode *node) {
        if(!node)
            return false;

        ReducableExpressionsVisitor v;
        node->accept(v);

        return v.isReducable();
    }

// this here is a pretty simple visitor
// if for unary or binary operations two literals are encountered, then a reduction can be done!
    void ReducableExpressionsVisitor::visit(NBinaryOp *op) {
        // check if both operands are literals
        if(python::isLiteralType(op->_left->getInferredType()) &&
           python::isLiteralType(op->_right->getInferredType()))
            _result = true;
        else {
            // continue visit
            ApatheticVisitor::visit(op);
        }
    }

    void ReducableExpressionsVisitor::visit(NUnaryOp *op) {
        if(python::isLiteralType(op->_operand->getInferredType()))
            _result = true;
        else {
            ApatheticVisitor::visit(op);
        }
    }

    void ReducableExpressionsVisitor::visit(NCompare *cmp) {
        // check if all operators here are literals
        // two cases: only left hand => reduction easily possible!!!
        // left hand + ops => reduction if all are literals
        if(cmp->_comps.size() > 0) {
            bool areAllLiterals = python::isLiteralType(cmp->_left->getInferredType());
            int pos = 0;
            while(areAllLiterals && pos < cmp->_comps.size() > 0) {
                areAllLiterals = python::isLiteralType(cmp->_comps[pos++]->getInferredType());
            }

            if(areAllLiterals)
                _result = true;
            else
                ApatheticVisitor::visit(cmp);
        } else {
            if(python::isLiteralType(cmp->_left->getInferredType()))
                _result = true;
            else
                ApatheticVisitor::visit(cmp);
        }
    }
}