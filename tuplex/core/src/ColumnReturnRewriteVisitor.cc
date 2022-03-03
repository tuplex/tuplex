//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by rahuly first first on 10/13/19                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ColumnReturnRewriteVisitor.h>
namespace tuplex {

    bool ColumnReturnRewriteVisitor::isLiteralKeyDict(ASTNode* node) {
        assert(node);
        if(node->type() != ASTNodeType::Dictionary) return false;
        for(auto &p: ((NDictionary*)node)->_pairs) { // check that all keys are literal strings
            if(p.first->type() != ASTNodeType::String) {
                //                error("Return with columns that are not literal string keys: not yet supported."
                //                      "To return a dictionary, wrap it in a tuple: ({key:val,...},)");
                return false; // <- eventually allow this
            }
        }
        return true;
    }

    // NOTE: this will NEVER return [dict] directly, so we don't need to worry about dedup when using!
    ASTNode *ColumnReturnRewriteVisitor::rewriteLiteralKeyDict(NDictionary* dict) {
        std::vector<std::string> newColumnNames;
        std::vector<std::unique_ptr<ASTNode>> retNodes;
        std::vector<python::Type> retTypes;

        // special case: If dict is empty dict, result is empty dict.
        if(dict->_pairs.empty())
            return dict;

        // gather column names and rows
        for(auto &p: dict->_pairs) {
            std::string new_name = "";
            const auto &node = p.first;
            if(node->type() == ASTNodeType::String) {
                new_name = ((NString*)p.first.get())->value();
            } else if(node->type() == ASTNodeType::Identifier) {
                auto& ann = node->annotation();
                if(!ann.symbol)
                    throw std::runtime_error("global symbol annotation for identifier missing!");
            } else throw std::runtime_error("unknown ASTNode for rewriting encountered in " + std::string(__FILE_NAME__));

            newColumnNames.push_back(new_name);
            retTypes.push_back(p.second->getInferredType());
            retNodes.push_back(std::move(p.second)); // take ownership away to give to tuple
        }

        // for functions, check if this return statement is inconsistent
        if(!columnNames.empty()) {
            bool sameColumns = true;
            if(columnNames.size() != newColumnNames.size()) sameColumns = false;
            for(int i=0; i<(int)columnNames.size(); i++) {
                if(columnNames[i] != newColumnNames[i]) sameColumns = false;
            }
            if(!sameColumns) error("Return statements have different columns, not supported yet!");
        }

        // set the column names
        columnNames = newColumnNames;

        // special case: If the dict consists of a single pair, i.e. {'key' : val}, then
        // unpack this, i.e. don't return a tuple (val,) but just val.
        // else, return a tuple
        if(retNodes.size() == 1) {
            assert(retTypes.size() == 1);
            _newOutputType = retTypes[0];
            return retNodes[0].release();
        } else {
            // else,
            // construct and return the new tuple object
            auto ret = new NTuple();
            ret->_elements = std::move(retNodes);
            _newOutputType = python::Type::makeTupleType(retTypes);
            return ret;
        }
    }

    ASTNode* ColumnReturnRewriteVisitor::replace(ASTNode* parent, ASTNode* node) {
        if(!node)
            return nullptr;

        switch(node->type()) {
            case ASTNodeType::Lambda: {
                auto lambda = ((NLambda*)node);
                auto retVal = lambda->_expression.get();
                if(isLiteralKeyDict(retVal)) {
                    auto newRetVal = rewriteLiteralKeyDict(((NDictionary*)retVal));
                    lambda->_expression = std::unique_ptr<ASTNode>(newRetVal);
                }

                return node;
            }
            case ASTNodeType::Dictionary: {
                if(parent->type() == ASTNodeType::Lambda && node == ((NLambda*)parent)->_expression.get()) {
                    if(isLiteralKeyDict(node)) {
                        auto newRetVal = rewriteLiteralKeyDict(((NDictionary*)node));
                        return newRetVal;
                    }
                }
                return node;
            }
            // TODO: This will not work with nested functions, because it will try to rewrite all of them
#warning "This rewrite will not work for nested functions."
            case ASTNodeType::Suite: {
                if(parent->type() == ASTNodeType::Function) {
                    auto suite = ((NSuite*)node);
                    for (auto &_statement : suite->_statements) { // iterate over statements
                        if (_statement->type() == ASTNodeType::Return) { // ...and find the return statements
                            const auto &retVal = ((NReturn *) _statement.get())->_expression;
                            if (isLiteralKeyDict(retVal.get())) { // check if we need to rewrite the return value
                                auto newRetVal = rewriteLiteralKeyDict((NDictionary *) retVal.get());
                                ((NReturn*)_statement.get())->_expression = std::unique_ptr<ASTNode>(newRetVal);
                            }
                        }
                    }
                }

                return node;
            }

            case ASTNodeType::Return: {
                auto &retVal = ((NReturn *) node)->_expression;
                if (isLiteralKeyDict(retVal.get())) { // check if we need to rewrite the return value
                    auto newRetVal = rewriteLiteralKeyDict((NDictionary *) retVal.get());
                    ((NReturn*)node)->_expression = std::unique_ptr<ASTNode>(newRetVal);
                }

                return node;
            }
            default: return node;
        }
    }
}