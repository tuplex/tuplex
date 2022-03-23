//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_APATHETICVISITOR_H
#define TUPLEX_APATHETICVISITOR_H


#include "IVisitor.h"

/*!
 * @Todo: find a better name for this class & refactor
 * this class is a helper to help you write visitors faster. It basically is a visitor that does nothing.
 * If you only need partially some functions
 * implemented, use this as it saves a lot of code to write. However, be aware that inheriting from it might lead to undesired behaviour!
 * it basically calls all the functions
 *
 *
 * Usage: I.e. create a class which inherits ApatheticVisitor. Then
 * class SomeVisitor : public IVisitor {
 *      public:
 *      void visit(NNumber *number) {
 *          cout<<"found number on preorder traversal"<<endl;
 *          ApatheticVisitor::visit(number);
 *          cout<<"found  number on postorder traversal"<<endl;
 *      }
 * }
 */
namespace tuplex {
    class ApatheticVisitor : public IVisitor {
    private:
        ASTNode* _lastParent;
    protected:
        ASTNode* parent() { return _lastParent; }

        void setLastParent(ASTNode* parent) { _lastParent = parent; }
    public:
        ApatheticVisitor() : _lastParent(nullptr) {}

        // leaf nodes
        virtual void visit(NNone*)  {}
        virtual void visit(NNumber*) {}
        virtual void visit(NIdentifier*) {}
        virtual void visit(NBoolean*) {}
        virtual void visit(NEllipsis*) {}
        virtual void visit(NString*) {}

        // non-leaf nodes, recursive calls are carried out for these
        virtual void visit(NParameter*);
        virtual void visit(NParameterList*);
        virtual void visit(NFunction*);

        virtual void visit(NBinaryOp*);
        virtual void visit(NUnaryOp*);
        virtual void visit(NSuite*);
        virtual void visit(NModule*);
        virtual void visit(NLambda*);
        virtual void visit(NAwait*);
        virtual void visit(NStarExpression*);
        virtual void visit(NCompare*);
        virtual void visit(NIfElse*);
        virtual void visit(NTuple*);
        virtual void visit(NDictionary*);
        virtual void visit(NList*);

        virtual void visit(NSubscription*);
        virtual void visit(NReturn*);
        virtual void visit(NAssign*);
        virtual void visit(NCall*);
        virtual void visit(NAttribute*);
        virtual void visit(NSlice*);
        virtual void visit(NSliceItem*);

        virtual void visit(NRange*);
        virtual void visit(NComprehension*);
        virtual void visit(NListComprehension*);

        virtual void visit(NAssert*);
        virtual void visit(NRaise*);

        virtual void visit(NWhile*);
        virtual void visit(NFor*);
        virtual void visit(NBreak*);
        virtual void visit(NContinue*);
    };
}

#endif //TUPLEX_APATHETICVISITOR_H