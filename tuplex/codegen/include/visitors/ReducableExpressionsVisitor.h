//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_REDUCABLEEXPRESSIONSVISITOR_H
#define TUPLEX_REDUCABLEEXPRESSIONSVISITOR_H

#include <IVisitor.h>
#include <visitors/ApatheticVisitor.h>
#include <stack>

namespace tuplex {
    class ReducableExpressionsVisitor : public ApatheticVisitor {
    private:
        // contains return values
        bool _result;
    public:
        ReducableExpressionsVisitor():_result(false)    {
        }

        void visit(NBinaryOp*);
        void visit(NUnaryOp*);
        void visit(NCompare*);

        bool isReducable() { return _result; }
    };

    /*!
     * returns true if somewhere in the ast tree, there are some expressions that can be reduced
     * @param node
     * @return
     */
    extern bool containsReducableExpressions(ASTNode *node);
}


#endif //TUPLEX_REDUCABLEEXPRESSIONSVISITOR_H