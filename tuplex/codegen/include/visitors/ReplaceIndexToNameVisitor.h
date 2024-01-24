//
// Created by leonhards on 1/23/24.
//

#ifndef TUPLEX_REPLACEINDEXTONAMEVISITOR_H
#define TUPLEX_REPLACEINDEXTONAMEVISITOR_H

#include "IReplaceVisitor.h"

namespace tuplex {
    class ReplaceIndexToNameVisitor : public IReplaceVisitor {
    public:
        ReplaceIndexToNameVisitor(const std::vector<std::string>& names) : _names(names) {}
    private:
        std::vector<std::string> _names;
    protected:
        ASTNode* replace(ASTNode* parent, ASTNode* node) override;
    };

    ASTNode *ReplaceIndexToNameVisitor::replace(tuplex::ASTNode *parent, tuplex::ASTNode *node) {
        if(node->type() == ASTNodeType::Subscription) {
            auto sub = static_cast<NSubscription*>(node);
            if(sub->_expression->type() == ASTNodeType::Number) {
                auto num = static_cast<NNumber*>(sub->_expression.get());
                auto i = num->getI64();

                if(i < 0 || i >= _names.size()) {
                    throw std::runtime_error("can not rewrite index " + std::to_string(i) + " to name");
                }

                return new NSubscription(sub->_value.get(), new NString(escape_to_python_str(_names[i])));
            }
        }

        return node;
    }
}

#endif //TUPLEX_REPLACEINDEXTONAMEVISITOR_H
