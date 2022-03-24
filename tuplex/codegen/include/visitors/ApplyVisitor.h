//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_APPLYVISITOR_H
#define TUPLEX_APPLYVISITOR_H

#include "IPrePostVisitor.h"

namespace tuplex {

    /*!
     * helper class to execute lambda function for all nodes of a specific type fulfilling some predicate
     */
    class ApplyVisitor : public IPrePostVisitor {
    protected:
        std::function<bool(const ASTNode*)> _predicate;
        std::function<void(ASTNode&)> _func;

        void postOrder(ASTNode *node) override {
            if(_predicate(node))
                _func(*node);
        }

        void preOrder(ASTNode *node) override {}
    public:
        ApplyVisitor() = delete;

        /*!
         * create a new ApplyVisitor
         * @param predicate visit only nodes which return true
         * @param func call this function on every node which satisfies the predicate
         * @param followAll whether to visit all nodes, or only follow the normal-case path
         */
        ApplyVisitor(std::function<bool(const ASTNode*)> predicate,
                std::function<void(ASTNode&)> func, bool followAll=true) : _predicate(predicate), _func(func), _followAll(followAll)   {}

        // speculation so far only on ifelse branches
        void visit(NIfElse* ifelse) override {
            if(!_followAll) {
                // speculation on?
                bool speculate = ifelse->annotation().numTimesVisited > 0;
                auto visit_t = whichBranchToVisit(ifelse);
                auto visit_ifelse = std::get<0>(visit_t);
                auto visit_if = std::get<1>(visit_t);
                auto visit_else = std::get<2>(visit_t);

                // only one should be true, logical xor
                if(speculate && (!visit_if != !visit_else)) {
                    if(visit_if) {
                        ifelse->_expression->accept(*this);
                        ifelse->_then->accept(*this);
                    }
                    if(visit_else && ifelse->_else) {
                        ifelse->_expression->accept(*this);
                        ifelse->_else->accept(*this);
                    }
                    return;
                }
            }

            IPrePostVisitor::visit(ifelse);
        }

    private:
        bool _followAll;
    };
}

#endif //TUPLEX_FORCETYPEVISITOR_H