//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CLEANASTVISITOR_H
#define TUPLEX_CLEANASTVISITOR_H


#include "IReplaceVisitor.h"

namespace tuplex {
    // @Todo: add here logs on what this visitor does...

// clean visitor optimizes currently the following:
// (1) collapses suites (except for upward merging!)
// (2) collapsed cmp nodes if they only consist of lhs
    class CleanAstVisitor : public IReplaceVisitor {
    private:

        // visits next node
        // tries to remove the next node if possible
        ASTNode* replace(ASTNode* parent, ASTNode* next);

    public:
        CleanAstVisitor() {}


        // there is no point in visiting these Literals or endnodes since they won't be pruned in any way
        // hence, just define them as empty functions.
        void visit(NNone*) {}
        void visit(NNumber*){}
        void visit(NIdentifier*){}
        void visit(NBoolean*){}
        void visit(NEllipsis*){}
        void visit(NString*){}

    };
}

#endif //TUPLEX_CLEANASTVISITOR_H