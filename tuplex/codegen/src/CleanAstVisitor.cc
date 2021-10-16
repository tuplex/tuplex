//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "CleanAstVisitor.h"
#include <cassert>

namespace tuplex {
    std::unique_ptr<ASTNode> CleanAstVisitor::replace(ASTNode *parent, std::unique_ptr<ASTNode> next) {
        // parent must always be set
        assert(parent);

        // next may be an empty field
        if(!next)
            return nullptr;

        // check what type next is and optimize away if possible
        switch(next->type()) {
            case ASTNodeType::Compare: {

                auto cmp = static_cast<NCompare *>(next.get());

                // compare node can be eliminated when only left hand side is set
                // is an inefficiency of the python parser...
                if (cmp->_left && cmp->_ops.empty() && cmp->_comps.empty()) {
                    // remove the "next" node
                    return std::unique_ptr<ASTNode>(cmp->_left->clone());
                }

                // else just return the node itself
                return next;
            }

            case ASTNodeType::Suite: {
                // NOTE: when using try/except this does not work anymore!!!
                // in suite remove statements after return if there are any
                int returnIndex = -1;
                auto suite = static_cast<NSuite*>(next.get());
                int pos = 0;
                for(const auto &stmt : suite->_statements) {
                    if(stmt->type() == ASTNodeType::Return )
                        returnIndex = pos;
                    pos++;
                }

                // return found?
                if(returnIndex != -1) {
                    // statements after return?
                    if(returnIndex != suite->_statements.size() - 1) {
                        suite->_statements.resize(returnIndex+1);
                        return next;
                    }
                }

                return next;
            }

            default:
                return next;
        }

        return next;
    }
}