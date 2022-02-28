//
// Created by Leonhard Spiegelberg on 2/27/22.
//

#ifndef TUPLEX_REPLACECONSTANTTYPESVISITOR_H
#define TUPLEX_REPLACECONSTANTTYPESVISITOR_H

#include <IReplaceVisitor.h>
#include <ClosureEnvironment.h>

namespace tuplex {
    // if it encounters a constant valued type, it replaces it with a corresponding literal node
    class ReplaceConstantTypesVisitor : public IReplaceVisitor {
    protected:
        ASTNode *replace(ASTNode *parent, ASTNode *node) override;
    };
}

#endif //TUPLEX_REPLACECONSTANTTYPESVISITOR_H
