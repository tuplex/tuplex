//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <UnrollLoopsVisitor.h>
#include <ASTHelpers.h>
#include <cassert>

namespace tuplex {
    ASTNode* UnrollLoopsVisitor::replace(ASTNode *parent, ASTNode *next) {
        assert(parent);
        if(!next) {
            return nullptr;
        }

        switch(next->type()) {
            // for each assign statement 'id = val', add id to nameTable with val as mapped value
            // for each 'id2 = id1', add id2 to nameTable with id1's nameTable value as mapped value
            case ASTNodeType::Assign: {
                auto assign = static_cast<NAssign*>(next);
                if(assign->_target->type() == ASTNodeType::Identifier) {
                    auto id = static_cast<NIdentifier*>(assign->_target);
                    if(assign->_value->type() == ASTNodeType::Identifier) {
                        auto valId = static_cast<NIdentifier*>(assign->_value);
                        if(nameTable.find(valId->_name) == nameTable.end()) {
                            return next;
                        }
                        nameTable[id->_name] = nameTable[valId->_name];
                    } else {
                        nameTable[id->_name] = assign->_value;
                    }
                }
                return next;
            }

            case ASTNodeType::For: {
                auto forStmt = static_cast<NFor*>(next);
                NTuple* expr;
                if(forStmt->expression->type() == ASTNodeType::Tuple) {
                    expr = static_cast<NTuple*>(forStmt->expression);
                } else if(forStmt->expression->type() == ASTNodeType::Identifier) {
                    auto id = static_cast<NIdentifier*>(forStmt->expression);
                    if(nameTable.find(id->_name) != nameTable.end() && nameTable[id->_name]->type() == ASTNodeType::Tuple) {
                        expr = static_cast<NTuple*>(nameTable.at(id->_name));
                    } else {
                        // the expression identifier is not in nameTable (either it hasn't been declared or it can be the parameter variable) or it is not associated with an NTuple
                        // no need to unroll
                        return next;
                    }
                } else {
                    return next;
                }
                // replace the NFor node with an NSuite node with statements {assign, suite_body, assign, suite_body, ..., (suite_else)}
                auto loopSuite = new NSuite();
                loopSuite->_isUnrolledLoopSuite = true;
                auto targetASTType = forStmt->target->type();
                if(targetASTType == ASTNodeType::Identifier) {
                    // target is single identifier
                    for (const auto & element : expr->_elements) {
                        loopSuite->addStatement(new NAssign(forStmt->target, element));
                        auto forSuite = forStmt->suite_body->clone();
                        loopSuite->addStatement(forSuite);
                    }
                } else if(targetASTType == ASTNodeType::Tuple || targetASTType == ASTNodeType::List) {
                    // target has multiple identifiers. Expression should be a tuple of tuples/lists where each tuple/list has the same number of elements as the target
                    // 'for a, b in ((v1, v2), (v3, v4)): suite_body, suite_else'
                    // will be replaced with '{{a = v1, b = v2}, suite_body, {a = v3, b = v4}, suite_body, suite_else}' where {} denotes a new suite
                    auto idTuple = getForLoopMultiTarget(forStmt->target); // a vector of identifiers each corresponds to a value in idValueTuple
                    for (const auto & element : expr->_elements) {
                        if(element->type() == ASTNodeType::Tuple) {
                            auto idValueTuple = static_cast<NTuple*>(element)->_elements; // a vector of all values to be assigned to identifiers
                            assert(idTuple.size() == idValueTuple.size());
                            // add all assign statements needed for a single iteration into this assignSuite
                            auto assignSuite = new NSuite();
                            for (int i = 0; i < idTuple.size(); ++i) {
                                assignSuite->addStatement(new NAssign(idTuple[i], idValueTuple[i]));
                            }
                            loopSuite->addStatement(assignSuite);
                        } else if(element->type() == ASTNodeType::List) {
                            auto idValueList = static_cast<NList*>(element)->_elements; // a vector of all values to be assigned to identifiers
                            assert(idTuple.size() == idValueList.size());
                            auto assignSuite = new NSuite();
                            for (int i = 0; i < idTuple.size(); ++i) {
                                assignSuite->addStatement(new NAssign(idTuple[i], idValueList[i]));
                            }
                            loopSuite->addStatement(assignSuite);
                        } else {
                            fatal_error("Unsupported for loop expression");
                        }
                        auto forSuite = forStmt->suite_body->clone();
                        loopSuite->addStatement(forSuite);
                    }
                }

                if(forStmt->suite_else) {
                    loopSuite->addStatement(forStmt->suite_else);
                }
                return loopSuite;
            }

            default:
                return next;
        }
    }
}