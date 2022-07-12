//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by rahuly first first on 10/13/19                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_COLUMNRETURNREWRITEVISITOR_H
#define TUPLEX_COLUMNRETURNREWRITEVISITOR_H

#include <ast/ASTNodes.h>
#include <visitors/IReplaceVisitor.h>
#include <codegen/IFailable.h>
#include <string>
#include <vector>

namespace tuplex {
    class ColumnReturnRewriteVisitor : public IReplaceVisitor, public IFailable {
    private:
        ASTNode *rewriteLiteralKeyDict(NDictionary* dict);
        bool isLiteralKeyDict(ASTNode* node);
        python::Type _newOutputType;
    protected:
        ASTNode* replace(ASTNode* parent, ASTNode* node);
    public:
        std::vector<std::string> columnNames;
        ColumnReturnRewriteVisitor() = default;
        bool foundColumnNames() { return columnNames.size() > 0; }
        python::Type getNewOutputType() { return _newOutputType; }
    };
}

#endif //TUPLEX_COLUMNRETURNREWRITEVISITOR_H