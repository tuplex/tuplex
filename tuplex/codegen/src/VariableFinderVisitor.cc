//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <BlockGeneratorVisitor.h>
#include <IPrePostVisitor.h>
#include <ASTHelpers.h>

namespace tuplex {
    class VariableFinderVisitor : public IPrePostVisitor {
    public:
        std::set<std::tuple<std::string, python::Type>> foundVariables; // old, before changing list comprehension.
        std::set<std::tuple<std::string, python::Type>> declaredVariables; // declared vars (incl. for loops!)
    protected:
        void postOrder(ASTNode* node) override {
            if(node->type() == ASTNodeType::Assign) {
                // add to set
                NAssign* assign = (NAssign*)node;
                switch (assign->_target->type()) {
                    case ASTNodeType::Identifier: {
                        NIdentifier* id = (NIdentifier*)assign->_target;

                        declaredVariables.insert(std::make_tuple(id->_name, id->getInferredType()));
                        break;
                    }
                    case ASTNodeType::Tuple: {
                        auto tuple = (NTuple *) assign->_target;
                        for(const auto& elt : tuple->_elements) {
                            NIdentifier* id = (NIdentifier*) elt;
                            declaredVariables.insert(std::make_tuple(id->_name, id->getInferredType()));
                        }
                        break;
                    }
                    default: {
                        break;
                    }
                }
            }

            // for loop:
            if(node->type() == ASTNodeType::For) {
                NFor* f = dynamic_cast<NFor*>(node);
                auto targetASTType = f->target->type();
                // check what target it is:
                if(targetASTType == ASTNodeType::Identifier) {
                    // single identifier
                    NIdentifier* id = (NIdentifier*) f->target;
                    declaredVariables.insert(std::make_tuple(id->_name, id->getInferredType()));
                } else {
                    // multiple identifiers, i.e. something like a, b, c in ...
                    assert(targetASTType == ASTNodeType::Tuple || targetASTType == ASTNodeType::List);
                    auto idTuple = getForLoopMultiTarget(f->target);
                    for(const auto& el : idTuple) {
                        assert(el->type() == ASTNodeType::Identifier);
                        NIdentifier* id = (NIdentifier*) el;
                        declaredVariables.insert(std::make_tuple(id->_name, id->getInferredType()));
                    }
                }
            }

            // comprehensions. (only list so far)
            if(node->type() == ASTNodeType::Comprehension) {
                NIdentifier* id = dynamic_cast<NComprehension*>(node)->target;
                foundVariables.insert(std::make_tuple(id->_name, id->getInferredType()));
            }
        }

        void preOrder(ASTNode* node) override {};
    };


    namespace codegen {
        std::map<std::string, std::vector<python::Type>> getDeclaredVariables(ASTNode* root) {
            if(!root)
                return std::map<std::string, std::vector<python::Type>>{};

            VariableFinderVisitor vfv;
            root->accept(vfv);
            std::map<std::string, std::vector<python::Type>> info;

            for(auto v : vfv.declaredVariables) {
                auto name = std::get<0>(v);
                auto type = std::get<1>(v);

                // already in map?
                auto it = info.find(name);
                if(it == info.end()) {
                    info[name] = std::vector<python::Type>{type};
                } else {
                    // make sure to not have duplicates!
                    auto jt = std::find(info[name].begin(), info[name].end(), type);
                    if(jt == info[name].end())
                        info[name].push_back(type);
                }
            }
            return info;
        }

        // comprehension defines its own scope. I.e., they do not leak.
        std::vector<std::tuple<std::string, python::Type>> getComprehensionVariables(NLambda* root) {
            VariableFinderVisitor vfv;
            root->accept(vfv);
            return std::vector<std::tuple<std::string, python::Type>>(vfv.foundVariables.begin(), vfv.foundVariables.end());
        }
    }
}