//
// Created by Leonhard Spiegelberg on 2/15/23.
//

#ifndef TUPLEX_IREPLACEPARAMETERVISITOR_H
#define TUPLEX_IREPLACEPARAMETERVISITOR_H

#include "IReplaceVisitor.h"

namespace tuplex {

    // @TODO: check when name occurs multiple times / is assigned different stuff...
    // -> if statements or loops are to watch out for!
    class IReplaceParameterVisitor : public IReplaceVisitor {
    private:
        std::string _parameter;
    protected:
        virtual ASTNode* replaceAccess(NSubscription* sub) = 0;

        inline ASTNode* replace(ASTNode* parent, ASTNode* node) override {
            if(!node)
                return nullptr;

            switch(node->type()) {
                case ASTNodeType::Subscription: {

                    auto sub = (NSubscription *) node;

                    if(sub->_value->type() == ASTNodeType::Identifier) {
                        auto id = (NIdentifier *) sub->_value.get();
                        if(id->_name == _parameter) {
                            return replaceAccess(sub);
                        }
                    }

                    return node;
                    break;
                }
                default:
                    return node;
            }
        }

    public:
        IReplaceParameterVisitor() = delete;
        IReplaceParameterVisitor(const std::string& parameter) : _parameter(parameter) {}

        inline std::string parameter() const { return _parameter; }
    };
}

#endif //TUPLEX_IREPLACEPARAMETERVISITOR_H
