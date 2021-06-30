//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_COLUMNREWRITEVISITOR_H
#define TUPLEX_COLUMNREWRITEVISITOR_H

#include <ASTNodes.h>
#include <IReplaceVisitor.h>
#include <IFailable.h>
#include <string>
#include <vector>

namespace tuplex {
    class ColumnRewriteVisitor : public IReplaceVisitor, public IFailable {
    private:
        std::vector<std::string> _columnNames;
        std::string _parameter; //! the parameter for which to rewrite subscripts...

        bool _rewrite;
        bool _dictAccessFound;
    protected:
        ASTNode* replace(ASTNode* parent, ASTNode* node) override;
    public:
        ColumnRewriteVisitor() = delete;
        ColumnRewriteVisitor(const std::vector<std::string>& columnNames,
                             const std::string parameter,
                             bool rewrite = true) : _columnNames(columnNames), _parameter(parameter), _rewrite(true), _dictAccessFound(false)   {}

        bool dictAccessFound() const { return _dictAccessFound; }
    };
}
#endif //TUPLEX_COLUMNREWRITEVISITOR_H