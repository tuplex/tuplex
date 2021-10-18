//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_REMOVEDEADBRANCHESVISITOR_H
#define TUPLEX_REMOVEDEADBRANCHESVISITOR_H

#include "IReplaceVisitor.h"

namespace tuplex {
    // this class removes dead branches as would typically
// occur when null-value optimization is enabled...
    class RemoveDeadBranchesVisitor : public IReplaceVisitor {
    protected:
        ASTNode* replace(ASTNode* parent, ASTNode* node) override;

        bool _useAnnotations; ///! whether to actually remove nodes or simply insert/update annotations.
    public:
        RemoveDeadBranchesVisitor(bool useAnnotations=true) : _useAnnotations(useAnnotations) {}
    };
}

#endif //TUPLEX_REMOVEDEADBRANCHESVISITOR_H