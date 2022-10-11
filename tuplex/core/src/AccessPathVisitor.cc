//
// Created by Leonhard Spiegelberg on 9/6/22.
//

#include <AccessPathVisitor.h>

namespace tuplex {
    std::vector<size_t> AccessPathVisitor::getAccessedIndices() const {

        std::vector<size_t> idxs;

        // first check what type it is
        if(_tupleArgument) {
            assert(_argNames.size() == 1);
            std::string argName = _argNames.front();
            if(_argFullyUsed.at(argName)) {
                for(unsigned i = 0; i < _numColumns; ++i)
                    idxs.push_back(i);
            } else {
                return _argSubscriptIndices.at(argName);
            }
        } else {
            // simple, see which params are fully used.
            for(unsigned i = 0; i < _argNames.size(); ++i) {
                if(_argFullyUsed.at(_argNames[i]))
                    idxs.push_back(i);
            }
        }

        return idxs;
    }

    void AccessPathVisitor::preOrder(ASTNode *node) {
        switch(node->type()) {
            case ASTNodeType::Lambda: {
                assert(!_singleLambda);
                _singleLambda = true;

                NLambda* lambda = (NLambda*)node;
                auto itype = lambda->_arguments->getInferredType();
                assert(itype.isTupleType());

                // how many elements?
                _tupleArgument = itype.parameters().size() == 1 &&
                                 itype.parameters().front().isTupleType() &&
                                 itype.parameters().front() != python::Type::EMPTYTUPLE;
                _numColumns = _tupleArgument ? itype.parameters().front().parameters().size() : itype.parameters().size();

                // fetch identifiers for args
                for(const auto& argNode : lambda->_arguments->_args) {
                    assert(argNode->type() == ASTNodeType::Parameter);
                    NIdentifier* id = ((NParameter*)argNode.get())->_identifier.get();
                    _argNames.push_back(id->_name);
                    _argFullyUsed[id->_name] = false;
                    _argSubscriptIndices[id->_name] = std::vector<size_t>();
                }

                break;
            }

            case ASTNodeType::Function: {
                assert(!_singleLambda);
                _singleLambda = true;

                NFunction* func = (NFunction*)node;
                auto itype = func->_parameters->getInferredType();
                assert(itype.isTupleType());

                // how many elements?
                _tupleArgument = itype.parameters().size() == 1 &&
                                 itype.parameters().front().isTupleType() &&
                                 itype.parameters().front() != python::Type::EMPTYTUPLE;
                _numColumns = _tupleArgument ? itype.parameters().front().parameters().size() : itype.parameters().size();

                // fetch identifiers for args
                for(const auto& argNode : func->_parameters->_args) {
                    assert(argNode->type() == ASTNodeType::Parameter);
                    NIdentifier* id = ((NParameter*)argNode.get())->_identifier.get();
                    _argNames.push_back(id->_name);
                    _argFullyUsed[id->_name] = false;
                    _argSubscriptIndices[id->_name] = std::vector<size_t>();
                }

                break;
            }

            case ASTNodeType::Identifier: {
                NIdentifier* id = (NIdentifier*)node;
                if(parent()->type() != ASTNodeType::Parameter &&
                   parent()->type() != ASTNodeType::Subscription) {
                    // the actual identifier is fully used, mark as such!
                    _argFullyUsed[id->_name] = true;
                }

                break;
            }

                // check whether function with single parameter which is a tuple is accessed.
            case ASTNodeType::Subscription: {
                NSubscription* sub = (NSubscription*)node;

                assert(sub->_value->getInferredType() != python::Type::UNKNOWN); // type annotation/tracing must have run for this...

                auto val_type = sub->_value->getInferredType();
                // access/rewrite makes only sense for dict/tuple types!
                // just simple stuff yet.
                if (sub->_value->type() == ASTNodeType::Identifier &&
                    (val_type.isTupleType() || val_type.isDictionaryType())) {
                    NIdentifier* id = (NIdentifier*)sub->_value.get();

                    // first check whether this identifier is in args,
                    // if not ignore.
                    if(std::find(_argNames.begin(), _argNames.end(), id->_name) != _argNames.end()) {
                        // no nested paths yet, i.e. x[0][2]
                        if(sub->_expression->type() == ASTNodeType::Number) {
                            NNumber* num = (NNumber*)sub->_expression.get();

                            // should be I64 or bool
                            assert(num->getInferredType() == python::Type::BOOLEAN ||
                                   num->getInferredType() == python::Type::I64);


                            // can save this one!
                            auto idx = num->getI64();

                            // negative indices should be converted to positive ones
                            if(idx < 0)
                                idx += _numColumns;
                            assert(idx >= 0 && idx < _numColumns);

                            _argSubscriptIndices[id->_name].push_back(idx);
                        } else {
                            // dynamic access into identifier, so need to push completely back.
                            _argFullyUsed[id->_name] = true;
                        }
                    }

                }


                // path:
                auto path = longestAccessPath(sub);
                if(!path.empty()) {
                    _accessedPaths.push_back(path);
#ifndef NDEBUG
                    std::cout<<"found path: "<<path.desc()<<std::endl;
#endif
                }


                break;
            }
            default:
                // other nodes not supported.
                break;
        }
    }

    SelectionPath AccessPathVisitor::longestAccessPath(tuplex::NSubscription *sub) {
       if(!sub)
           return {};

       SelectionPath p;
       while(sub && sub->type() == ASTNodeType::Subscription) {

           // has this node been already visited?
           // yes, abort.
           if(node_visited(sub))
               return {};
           // mark visited
           mark_visited(sub);

           auto expr = sub->_expression.get();
           auto value = sub->_value.get();

           // update name
           if(value->type() == ASTNodeType::Identifier) {
               p.name = ((NIdentifier*)value)->_name;
               mark_visited(value); // mark the identifier as visited
           }

           // constant valued?
           if(isLiteral(expr)) {
              if(expr->type() == ASTNodeType::String) {
                  auto index = static_cast<NString*>(expr)->value();
                  p.atoms.push_back(SelectionPathAtom(index));
              } else if(expr->type() == ASTNodeType::Number
                        && deoptimizedType(expr->getInferredType()) == python::Type::I64) {
                  auto num = ((NNumber*)expr);
                  auto index = num->getI64();
                  p.atoms.push_back(SelectionPathAtom(index));
              } else {
                  std::reverse(p.atoms.begin(), p.atoms.end()); // b.c. of descent, need to reverse order
                  return p; // abort, can't decide for longest.
              }
           } else if(expr->getInferredType().isConstantValued()) {
             // string?
             auto t = expr->getInferredType().underlying();
             if(t == python::Type::STRING) {
                 p.atoms.push_back(expr->getInferredType().constant());
             } else if(t == python::Type::I64) {
                 p.atoms.push_back(std::stoi(expr->getInferredType().constant()));
             } else {
                 std::reverse(p.atoms.begin(), p.atoms.end()); // b.c. of descent, need to reverse order
                 return p; // abort, can't decide for longest.
             }
           }

           sub = dynamic_cast<NSubscription*>(value);
       }

        std::reverse(p.atoms.begin(), p.atoms.end()); // b.c. of descent, need to reverse order
       return p;
    }

    void AccessPathVisitor::postOrder(tuplex::ASTNode *node) {
        if(!node)
            return;
        switch(node->type()) {
            case ASTNodeType::Identifier: {
                NIdentifier *id = (NIdentifier*)node;
                // for identifiers that are not path of subscript AND within function body,
                // add access path if not visited
                if(parent() && parent()->type() == ASTNodeType::Parameter)
                    return;
                if(!node_visited(id)) {
                    mark_visited(id);
                    SelectionPath path;
                    path.name = id->_name; // a path without atoms.
                    _accessedPaths.push_back(path);
#ifndef NDEBUG
                    std::cout<<"Found access path: "<<path.desc()<<std::endl;
#endif
                }
                break;
            }
            default:
                break;
        }
    }
}