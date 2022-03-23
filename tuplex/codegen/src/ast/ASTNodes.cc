//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "ast/ASTNodes.h"

namespace tuplex {
    void NNumber::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NCollection::accept(class IVisitor &visitor) {
        EXCEPTION("collection should not be part of the AST! It is a helper class for parsing...");
    }

    void NIdentifier::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NParameter::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NParameterList::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NNone::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NAwait::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NSuite::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NIfElse::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NLambda::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }


    void NModule::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NString::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NBoolean::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NCompare::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NUnaryOp::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NBinaryOp::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NEllipsis::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NFunction::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NStarExpression::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NTuple::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NDictionary::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NList::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NSubscription::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NReturn::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NAssign::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NCall::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NAttribute::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NSlice::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NSliceItem::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NRange::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NComprehension::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NListComprehension::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NAssert::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NRaise::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NWhile::accept(class IVisitor& visitor) {
        visitor.visit(this);
    }

    void NFor::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NBreak::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }

    void NContinue::accept(class IVisitor &visitor) {
        visitor.visit(this);
    }
}