//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IVISITOR_H
#define TUPLEX_IVISITOR_H

#include "ASTNodes.h"

namespace tuplex {
    class IVisitor;
    class NNone;
    class NParameterList;
    class NParameter;
    class NFunction;
    class NNumber;
    class NIdentifier;
    class NBoolean;
    class NEllipsis;
    class NString;
    class NBinaryOp;
    class NUnaryOp;
    class NSuite;
    class NModule;
    class NLambda;
    class NAwait;
    class NStarExpression;
    class NCompare;
    class NIfElse;
    class NTuple;
    class NDictionary;
    class NList;
    class NSubscription;
    class NReturn;
    class NAssign;
    class NCall;
    class NAttribute;
    class NSlice;
    class NSliceItem;
    class NRange;
    class NComprehension;
    class NListComprehension;
    class NAssert;
    class NRaise;
    class NWhile;
    class NFor;
    class NBreak;
    class NContinue;

    class IVisitor {
    public:
        virtual void visit(NNone*) = 0;
        virtual void visit(NParameter*) = 0;
        virtual void visit(NParameterList*) = 0;
        virtual void visit(NFunction*) = 0;
        virtual void visit(NNumber*) = 0;
        virtual void visit(NIdentifier*) = 0;
        virtual void visit(NBoolean*) = 0;
        virtual void visit(NEllipsis*) = 0;
        virtual void visit(NString*) = 0;
        virtual void visit(NBinaryOp*)  = 0;
        virtual void visit(NUnaryOp*) = 0;
        virtual void visit(NSuite*) = 0;
        virtual void visit(NModule*) = 0;
        virtual void visit(NLambda*) = 0;
        virtual void visit(NAwait*) = 0;
        virtual void visit(NStarExpression*) = 0;
        virtual void visit(NCompare*) = 0;
        virtual void visit(NIfElse*) = 0;
        virtual void visit(NTuple*) = 0;
        virtual void visit(NDictionary*) = 0;
        virtual void visit(NList*) = 0;
        virtual void visit(NSubscription*) = 0;
        virtual void visit(NReturn*) = 0;
        virtual void visit(NAssign*) = 0;
        virtual void visit(NCall*) = 0;
        virtual void visit(NAttribute*) = 0;
        virtual void visit(NSlice*) = 0;
        virtual void visit(NSliceItem*) = 0;
        virtual void visit(NRange*) = 0;
        virtual void visit(NComprehension*) = 0;
        virtual void visit(NListComprehension*) = 0;
        virtual void visit(NAssert*) = 0;
        virtual void visit(NRaise*) = 0;
        virtual void visit(NWhile*) = 0;
        virtual void visit(NFor*) = 0;
        virtual void visit(NBreak*) = 0;
        virtual void visit(NContinue*) = 0;
    };

    /*!
     * helper function to decide which branch to follow
     * returns 3 bools for ifelse statment, then branch, else branch indicating whetehr statement should be visited, and if so which branch
     * @param ifelse
     * @return visit_ifelse, visit_if, visit_else
     */
    extern std::tuple<bool, bool, bool> whichBranchToVisit(NIfElse* ifelse);
}
#endif //TUPLEX_IVISITOR_H