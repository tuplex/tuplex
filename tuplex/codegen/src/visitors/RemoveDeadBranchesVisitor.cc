//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <visitors/RemoveDeadBranchesVisitor.h>
#include <optional.h>

namespace tuplex {
    option<bool> compileTimeTruthEval(ASTNode* node) {

        switch(node->type()) {
            case ASTNodeType::Boolean: {
                // literal, simple eval
                return option<bool>(((NBoolean*)node)->_value);
            }
            case ASTNodeType::Number: {
                NNumber* num = (NNumber*)node;
                if(num->getInferredType() == python::Type::I64)
                    return option<bool>(num->getI64() > 0);
                assert(num->getInferredType() == python::Type::F64);
                return option<bool>(num->getF64() > 0.0);
            }
            case ASTNodeType::String: {
                NString* str = (NString*)node;

                // stupid process string needs to be called @TODO: better interface...
                auto val = str->value();
                return option<bool>(val.substr(1, val.length() - 2).size() != 0);
            }
            case ASTNodeType::None: {
                return option<bool>(false);
            }

            // simple compare (constant folding??)
            case ASTNodeType::Compare: {
                NCompare* cmp = (NCompare*)node;
                auto cmpA = compileTimeTruthEval(cmp->_left.get());

                if(!cmpA.has_value())
                    return option<bool>::none;

                for(int i = 0; i < cmp->_ops.size(); ++i) {
                    auto cmpB = compileTimeTruthEval(cmp->_comps[i].get());

                    if(!cmpB.has_value())
                        return option<bool>::none;
                    // both have valid values
                    if(!cmpA.value() && !cmpB.value())
                        return option<bool>(false);

                    // check whether their value us true
                    if(cmp->_ops[i] == TokenType::EQEQUAL) {
                        // both the same? true
                        cmpA = cmpA.value() && (cmpA.value() == cmpB.value());
                    } else if(cmp->_ops[i] == TokenType::NOTEQUAL) {
                        // different? true
                        cmpA = cmpA.value() && (cmpA.value() != cmpB.value());
                    } else
                        return option<bool>::none; // unknown, type error likely
                }
                return cmpA; // all true
            }
            default: {
                break;
            }
        }

        // check inferred type
        if(node->getInferredType() == python::Type::NULLVALUE ||
        node->getInferredType() == python::Type::EMPTYTUPLE ||
        node->getInferredType() == python::Type::EMPTYDICT)
            return option<bool>(false);

        return option<bool>::none;
    }

    ASTNode* RemoveDeadBranchesVisitor::replace(ASTNode *parent, ASTNode* node) {

        // if else is interesting
        if(!node)
            return nullptr;

        switch(node->type()) {
            case ASTNodeType::IfElse: {

                // can expression be determined at compile time?
                auto ifelse = (NIfElse*)node;

                auto compileTimeVal = tuplex::compileTimeTruthEval(ifelse->_expression.get());
                if(compileTimeVal.has_value()) {
                    // remove dead branch!

                    // there are two options here:
                    // a) either just add annotation on which branch to follow or
                    // b) remove nodes

                    // true?
                    if(compileTimeVal.value()) {
                        if(!_useAnnotations)
                            // always true, so replace with then branch
                            return ifelse->_then.release(); // HAVE to release from the parent!
                        else {
                            // do not replace, but always annotate ifelse->_then
                            ifelse->_then->annotation().numTimesVisited = 1;
                            if(ifelse->_else)
                                ifelse->_else->annotation().numTimesVisited = 0;
                            return node;
                        }
                    }
                    // false?
                    else {
                        if(!_useAnnotations) {
                            // always false, either remove completely or return else branch
                            if(ifelse->_else) {
                                return ifelse->_else.release(); // HAVE to release from the parent!
                            } else {
                                // nullptr is removal?
                                return nullptr;
                            }
                        } else {
                            // always false, i.e. annotate if to never visit then
                            // or only visit else branch
                            if(ifelse->_else) {
                                // do not visit then
                                ifelse->_then->annotation().numTimesVisited = 0;
                                ifelse->_else->annotation().numTimesVisited = 1;
                            } else {
                                // do not visit if at all
                                ifelse->annotation().numTimesVisited = 0;
                                ifelse->_then->annotation().numTimesVisited = 0;
                            }
                        }
                    }
                }

                return node;
            }
            default:
                return node; // no replacement
        }
    }
}