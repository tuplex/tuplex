//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ASTHelpers.h>
#include <ApatheticVisitor.h>
#include <IPrePostVisitor.h>
#include "ApplyVisitor.h"

namespace tuplex {
    bool isLiteral(ASTNode* root) {
        if(root->type() == ASTNodeType::Boolean)
            return true;
        if(root->type() == ASTNodeType::String)
            return true;
        return root->type() == ASTNodeType::Number;
    }

// helper visitor class for isStaticValue
    class StaticValueVisitor : public IPrePostVisitor {
    private:
        bool _nonliteralFound;

    protected:
        void postOrder(ASTNode *node) override;

        void preOrder(ASTNode *node) override {}
    public:
        StaticValueVisitor() : _nonliteralFound(false) {}

        bool yieldsLiteralExpression() { return !_nonliteralFound;}

    };

    void StaticValueVisitor::postOrder(ASTNode *node) {
        switch(node->type()) {
            // following are non-literals, if encountered expression is not reducable
            case ASTNodeType::UNKNOWN:
            case ASTNodeType::Lambda:
            case ASTNodeType::Identifier: // no const identifiers yet allowed! @Todo: later check with symboltable
            case ASTNodeType::Function:
            case ASTNodeType::Module:
            case ASTNodeType::Parameter:
                _nonliteralFound = true;
                break;
            default:
                break;
        }
    }

    bool isStaticValue(ASTNode *root, bool recursive) {
        if(!recursive)
            return isLiteral(root);

        // recursively visit
        StaticValueVisitor svv;
        root->accept(svv);
        return svv.yieldsLiteralExpression();
    }

// helper visitor class for isStaticValue
    class FunctionVariablesVisitor : public IPrePostVisitor {
    protected:

        void postOrder(ASTNode *node) override {
            if(node->type() == ASTNodeType::Assign) {
                // left side?
                auto assign = dynamic_cast<NAssign*>(node); assert(assign);
                std::set<std::string> assign_vars;
                // use apply visitor over left side!
                tuplex::ApplyVisitor av([](const ASTNode* n) { return n->type() == ASTNodeType::Identifier; },
                                        [&assign_vars](ASTNode& n) {
                                            assign_vars.insert(((NIdentifier&)n)._name);
                                        });
                assign->_target->accept(av);
                for(const auto& v: assign_vars)
                    vars.insert(v);
            }
        }

        void preOrder(ASTNode *node) override {}
    public:
        std::set<std::string> vars;
    };

    std::set<std::string> getFunctionVariables(ASTNode* root) {
        if(!root)
            return std::set<std::string>{};
        if(root->type() == ASTNodeType::Lambda) {
            // Note: We do not support := from Python 3.8, so return empty set
#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION > 7)
#warning "Python 3.8 adds :=, so variables can be defined with lambda expressions!"
#endif
            return std::set<std::string>{};
        }
        if(root->type() == ASTNodeType::Function) {
            FunctionVariablesVisitor fvv;
            root->accept(fvv);
            return fvv.vars;
        }

        // error
        throw std::runtime_error("getFunctionVariables only works on function nodes!");
    }

    std::vector<std::string> getFunctionParameters(ASTNode* root) {
        std::vector<std::string> v;
        if(!root)
            return v;

        if(root->type() == ASTNodeType::Lambda) {
            auto lam = (NLambda*)root;
            for(auto arg : lam->_arguments->_args) {
                assert(arg->type() == ASTNodeType::Parameter);
                auto param = (NParameter*)arg;
                v.emplace_back(param->_identifier->_name);
            }
            return v;
        }
        if(root->type() == ASTNodeType::Function) {
            auto func = (NFunction*)root;
            for(auto arg : func->_parameters->_args) {
                assert(arg->type() == ASTNodeType::Parameter);
                auto param = (NParameter*)arg;
                v.emplace_back(param->_identifier->_name);
            }
            return v;
        }

        // error
        throw std::runtime_error("getFunctionParameters only works on function nodes!");
    }

    ASTNode* fieldToAST(const tuplex::Field& f) {
        // option type?
        if(f.getType().isOptionType()) {
            if(f.isNull())
                return fieldToAST(tuplex::Field::null());
            else
                return fieldToAST(f.withoutOption());
        }

        if(f.getType() == python::Type::BOOLEAN)
            return new NBoolean(f.getInt());
        if(f.getType() == python::Type::I64)
            return new NNumber(f.getInt());
        if(f.getType() == python::Type::F64)
            return new NNumber(f.getDouble());
        if(f.getType() == python::Type::STRING) {
            // need to escape properly
            auto raw_str = std::string((const char*)f.getPtr());
            if(raw_str.find('"') == std::string::npos)
                return new NString("\"" + raw_str + "\"");
            else {
                // escape via \"
                std::string escaped_str = "\"";
                for(auto c : raw_str) {
                    if(c == '"')
                        escaped_str += "\\";
                    escaped_str += tuplex::char2str(c);
                }
                escaped_str += "\"";
                return new NString(escaped_str);
            }
        }

        if(f.getType() == python::Type::EMPTYTUPLE)
            return new NTuple();
        if(f.getType() == python::Type::EMPTYDICT)
            return new NDictionary();
        if(f.getType() == python::Type::EMPTYLIST)
            return new NList();

        // not supported...
        return nullptr;
    }


// Note: need to check this better for try/except in the future
    bool isExitPath(ASTNode* node) {
        if(!node)
            return false;

        // check type, recurse here directly, not necessary to use a visitor
        switch(node->type()) {
            case ASTNodeType::Return:
                return true;
            case ASTNodeType::IfElse: {
                // if it's an if (elif) else, then true if each is fine, else false
                NIfElse* ifelse = (NIfElse*)node;
                if(!ifelse->_else)
                    return false;

                // if and else are valid.
                // => check if each is exit path
                return isExitPath(ifelse->_then) && isExitPath(ifelse->_else);

                break;
            }
            case ASTNodeType::Suite: {
                // note: when try/except is used, this algorithm will fail because then a new exit path comes...
                // however, for now a simple escape analysis will do.


                // I.e., if there is a return statement directly within this suite, it's on an escape path!
                NSuite* suite = (NSuite*)node;
                for(auto stmt : suite->_statements) {
                    // check recursively, if one is found then all good!
                    if(isExitPath(stmt))
                        return true;
                }

                return false;
                break;
            }

            // is this the correct way to deal with loops?
            case ASTNodeType::For: {
                auto* forStmt = (NFor*)node;
                if(!forStmt->suite_else) {
                    return false;
                }
                return isExitPath(forStmt->suite_body) && isExitPath(forStmt->suite_else);
                break;
            }
            case ASTNodeType::While: {
                auto* whileStmt = (NWhile*)node;
                if(!whileStmt->suite_else) {
                    return false;
                }
                return isExitPath(whileStmt->suite_body) && isExitPath(whileStmt->suite_else);
                break;
            }

            case ASTNodeType::Function:
            case ASTNodeType::Lambda: {
                throw std::runtime_error("do not call this on function/lambda nodes. Use instead a suite or expression.");
            }
            default:
                return false;
        }
        return false;
    }

    bool isLogicalOp(ASTNode *node) {
        switch (node->type()) {
            case ASTNodeType::BinaryOp: {
                auto binop = (NBinaryOp *) node;
                return binop->_op == TokenType::AND || binop->_op == TokenType::OR;
            }
            case ASTNodeType::UnaryOp: {
                auto unop = (NUnaryOp *) node;
                return unop->_op == TokenType::NOT;
            }
            case ASTNodeType::Compare:
                return true; // ok.
            default:
                return false;
        }
    }

    std::vector<ASTNode *> getForLoopMultiTarget(ASTNode *target) {
        std::vector<ASTNode *> idTuple;
        if(target->type() == ASTNodeType::Tuple) {
            idTuple = static_cast<NTuple*>(target)->_elements;
        }
        if(target->type() == ASTNodeType::List) {
            idTuple = static_cast<NList*>(target)->_elements;
        }
        return idTuple;
    }
}