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
        for(auto p: ((NDictionary*)node)->_pairs) { // check that all keys are literal strings
            if(p.first->type() != ASTNodeType::String) {
                //                error("Return with columns that are not literal string keys: not yet supported."
                //                      "To return a dictionary, wrap it in a tuple: ({key:val,...},)");
                return false; // <- eventually allow this
            }
        }
        return true;
    }

    ASTNode *ColumnReturnRewriteVisitor::rewriteLiteralKeyDict(NDictionary* dict) {
        std::vector<std::string> newColumnNames;
        std::vector<ASTNode*> retNodes;
        std::vector<python::Type> retTypes;

        // special case: If dict is empty dict, result is empty dict.
        if(dict->_pairs.empty())
            return dict;

        // gather column names and rows
        for(auto p: dict->_pairs) {
            std::string new_name = "";
            auto node = p.first;
            if(node->type() == ASTNodeType::String) {
                new_name = ((NString*)p.first)->value();
            } else if(node->type() == ASTNodeType::Identifier) {
                auto& ann = node->annotation();
                if(!ann.symbol)
                    throw std::runtime_error("global symbol annotation for identifier missing!");
            } else throw std::runtime_error("unknown ASTNode for rewriting encountered in " + std::string(__FILE_NAME__));

            newColumnNames.push_back(new_name);
            retNodes.push_back(p.second);
            retTypes.push_back(p.second->getInferredType());
        }

        // for functions, check if this return statement is inconsistent
        if(columnNames.size() > 0) {
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
            return retNodes[0];
        } else {
            // else,
            // construct and return the new tuple object
            auto ret = new NTuple();
            ret->_elements = retNodes;
            _newOutputType = python::Type::makeTupleType(retTypes);
            return ret;
        }
    }

    ASTNode *ColumnReturnRewriteVisitor::replace(ASTNode* parent, ASTNode* node) {
        if(!node)
            return nullptr;

        switch(node->type()) {
            case ASTNodeType::Lambda: {
                auto lambda = ((NLambda*)node);
                auto retVal = lambda->_expression;
                if(isLiteralKeyDict(retVal)) {
                    auto newRetVal = rewriteLiteralKeyDict(((NDictionary*)retVal));
                    lambda->_expression = newRetVal;
                }

                return node;
            }
            case ASTNodeType::Dictionary: {
                if(parent->type() == ASTNodeType::Lambda && node == ((NLambda*)parent)->_expression) {
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
                            auto retVal = ((NReturn *) _statement)->_expression;
                            if (isLiteralKeyDict(retVal)) { // check if we need to rewrite the return value
                                auto newRetVal = rewriteLiteralKeyDict((NDictionary *) retVal);
                                ((NReturn*)_statement)->_expression = newRetVal;
                            }
                        }
                    }
                }

                return node;
            }

            case ASTNodeType::Return: {
                auto retVal = ((NReturn *) node)->_expression;
                if (isLiteralKeyDict(retVal)) { // check if we need to rewrite the return value
                    auto newRetVal = rewriteLiteralKeyDict((NDictionary *) retVal);
                    ((NReturn*)node)->_expression = newRetVal;
                }

                return node;
            }
            default: return node;
        }
    }
}