//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_UNROLLLOOPSVISITOR_H
#define TUPLEX_UNROLLLOOPSVISITOR_H

#include "visitors/IReplaceVisitor.h"
#include <IFailable.h>

namespace tuplex {
    // handle the mismatch between slot type and identifier type issue in code generation
    // when expression in for node is a tuple and tuple elements have different types
    // solve this by unrolling "for target in tuple: suite-body" to a new suite with repeated "assign" and "suite-body" statements
    class UnrollLoopsVisitor : public IReplaceVisitor, public IFailable {
    private:
        std::unordered_map<std::string, ASTNode*> nameTable;

        ASTNode* replace(ASTNode* parent, ASTNode* next);

    public:
        UnrollLoopsVisitor() {}

        void visit(NNone*) {}
        void visit(NNumber*){}
        void visit(NIdentifier*){}
        void visit(NBoolean*){}
        void visit(NEllipsis*){}
        void visit(NString*){}
    };
}
#endif //TUPLEX_UNROLLLOOPSVISITOR_H