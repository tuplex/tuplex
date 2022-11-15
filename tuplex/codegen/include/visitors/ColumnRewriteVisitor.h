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

#include <ast/ASTNodes.h>
#include <visitors/IReplaceVisitor.h>
#include <codegen/IFailable.h>
#include <string>
#include <vector>

namespace tuplex {

    enum class ColumnRewriteMode {
        NAME_TO_INDEX,
        INDEX_TO_NAME
    };

    class ColumnRewriteVisitor : public IReplaceVisitor, public IFailable {
    private:
        std::vector<std::string> _columnNames;
        std::string _parameter; //! the parameter for which to rewrite subscripts...

        std::unordered_map<size_t, size_t> _rewriteMap; // original idx -> new idx

        bool _rewrite;
        bool _dictAccessFound;
        ColumnRewriteMode _mode;

        // rewrite helper
        ASTNode* rewriteNameToIndex(NSubscription* sub);
        ASTNode* rewriteIndexToName(NSubscription* sub);
    protected:
        ASTNode* replace(ASTNode* parent, ASTNode* node) override;

        inline void warning(const std::string& msg) {
            auto& logger = Logger::instance().logger("codegen");
            logger.warn(msg);
        }

        // translate indices
        inline size_t originalIdxToProjectedIdx(size_t original_idx) const {
            if(_rewriteMap.empty()) // <-- no rewrite
                return original_idx;
            // translate via map
            return _rewriteMap.at(original_idx);
        }

        inline size_t projectedIdxToOriginalIdx(size_t projected_idx) const {
            if(_rewriteMap.empty()) // <-- no rewrite
                return projected_idx;

            for(auto kv : _rewriteMap) {
                if(kv.second == projected_idx)
                    return kv.first;
            }
            throw std::runtime_error("index not found!");
        }

    public:
        ColumnRewriteVisitor() = delete;
        ColumnRewriteVisitor(const std::vector<std::string>& columnNames,
                             const std::string parameter,
                             const std::unordered_map<size_t, size_t>& rewriteMap,
                             const ColumnRewriteMode& mode=ColumnRewriteMode::NAME_TO_INDEX,
                             bool rewrite = true) : _columnNames(columnNames),
                             _parameter(parameter),
                             _rewriteMap(rewriteMap),
                             _rewrite(true),
                             _dictAccessFound(false) ,
                             _mode(mode) {}

        bool dictAccessFound() const { return _dictAccessFound; }
    };
}
#endif //TUPLEX_COLUMNREWRITEVISITOR_H