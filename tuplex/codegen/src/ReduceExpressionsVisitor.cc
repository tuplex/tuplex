//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ReduceExpressionsVisitor.h>
#include <cassert>
#include <Logger.h>
#include <cmath>
#include "ASTHelpers.h"

namespace tuplex {
    // some helper functions
    int64_t booleanToI64(NBoolean* boolean) {
        assert(boolean);

        if(boolean->_value)
            return 1;
        else
            return 0;
    }

    double toDouble(ASTNode* node) {
        assert(node->type() == ASTNodeType::Number || node->type() == ASTNodeType::Boolean);

        if(node->type() == ASTNodeType::Number)
            return static_cast<NNumber*>(node)->getF64();
        if(node->type() == ASTNodeType::Boolean)
            return booleanToI64(static_cast<NBoolean*>(node));

        Logger::instance().defaultLogger().error("unknown ast type encountered, "
                                                 "could not cast to double. Emitting 0.0");
        return 0.0;
    }

    int64_t toInt(ASTNode* node) {
        assert(node->type() == ASTNodeType::Number || node->type() == ASTNodeType::Boolean);

        if(node->type() == ASTNodeType::Number)
            return static_cast<NNumber*>(node)->getI64();
        if(node->type() == ASTNodeType::Boolean)
            return booleanToI64(static_cast<NBoolean*>(node));

        Logger::instance().defaultLogger().error("unknown ast type encountered, "
                                                 "could not cast to double. Emitting 0");
        return 0;
    }

    bool toBool(ASTNode* node) {
        assert(node->type() == ASTNodeType::Number || node->type() == ASTNodeType::Boolean || node->type() == ASTNodeType::String);
        if(node->type() == ASTNodeType::Number)
            return dynamic_cast<NNumber*>(node)->getI64() != 0;
        if(node->type() == ASTNodeType::Boolean)
            return dynamic_cast<NBoolean*>(node)->_value;
        if(node->type() == ASTNodeType::String)
            return !dynamic_cast<NString*>(node)->value().empty();
        Logger::instance().defaultLogger().error("invalid AST type, could not cast to boolean. Emitting false");
        return false;
    }

    std::string replicateString(const std::string& s, const int64_t times) {

        // for negative times, "" should be returned
        // not needed, but make it here explicit, so it is clear what is going on...
        if(times <= 0)
            return "";

        std::string res = "";
        for(int i = 0; i < times; i++)
            res += s;
        return res;
    }

    python::Type binop_super_type(const python::Type a, const python::Type& b) {

        assert(a != python::Type::UNKNOWN);
        assert(b != python::Type::UNKNOWN);

        if(a == b) {
            // special case boolean: => cast to int!
            if(a == python::Type::BOOLEAN)
                return python::Type::I64;
            return a;
        }

        // larger type wins
        // maybe later integrate this into type system???
        // flipping types handled via recursion...
        if(a == python::Type::I64 && b == python::Type::F64)
            return python::Type::F64;

        if(a == python::Type::BOOLEAN && b == python::Type::F64)
            return python::Type::F64;

        if(a == python::Type::BOOLEAN && b == python::Type::I64)
            return python::Type::I64;

        // flip to save some work
        return binop_super_type(b, a);
    }

    void ReduceExpressionsVisitor::error(const std::string &message) {
        Logger::instance().logger("compiler").error(message);
        _numErrors++;
    }


    ASTNode* ReduceExpressionsVisitor::cmp_replace(NCompare *op) {
        // check that all operands are literals, if not no reduction is possible
        if(!python::isLiteralType(op->_left->getInferredType()))
            return op;

        for(auto operand: op->_comps)
            if(!python::isLiteralType(operand->getInferredType()))
                return op;

        // all operands are literals, can reduce!

        // special case: single member
        if(op->_ops.size() == 0)
            return op;

        // there are at least two! => result will be a always a boolean!
        bool res = false;

        assert(!op->_comps.empty());
        assert(!op->_ops.empty());

        ASTNode* left = op->_left;
        ASTNode* right = op->_left;
        TokenType cmp = op->_ops.front();
        for(int i = 0; i < op->_ops.size(); ++i) {
            left = right;
            right = op->_comps[i];
            cmp = op->_ops[i];

            // check whether comparison should be done for floating point vals or integer vals
            bool fcmp = left->getInferredType() == python::Type::F64
                        || right->getInferredType() == python::Type::F64;

            // convert values
            auto dLeft = toDouble(left);
            auto dRight = toDouble(right);
            auto iLeft = toInt(left);
            auto iRight = toInt(right);

            // reduce
            bool term = false;
            switch(cmp) {
                case TokenType::LESS:{
                    term = fcmp ? dLeft < dRight : iLeft < iRight;
                    break;
                }
                case TokenType::GREATER: {
                    term = fcmp ? dLeft > dRight : iLeft > iRight;
                    break;
                }
                case TokenType::LESSEQUAL: {
                    term = fcmp ? dLeft <= dRight : iLeft <= iRight;
                    break;
                }
                case TokenType::GREATEREQUAL: {
                    term = fcmp ? dLeft >= dRight : iLeft >= iRight;
                    break;
                }
                case TokenType::NOTEQUAL: {
                    term = fcmp ? dLeft != dRight : iLeft != iRight;
                    break;
                }
                case TokenType::EQEQUAL: {
                    term = fcmp ? dLeft == dRight : iLeft == iRight;
                    break;
                }
                default: {
                    error("encounterd unknown operator '" + opToString(cmp) + "' in compare expression, can't reduce");
                    return op;
                    break;
                }
            }

            // multiple expressions are anded. However, be sure to treat the first one correctly!
            if(0 == i)
                res = term;
            else
                res &= term;
        }

        delete op;
        op = nullptr;
        _numReductions++;
        return new NBoolean(res);
    }


// supported operators:
// a * b
// a + b
// a - b
// a / b
// not yet implemented:
// a // b
// a % b
// a << b
// a >> b
// ...
// => a lot of work needs to be done here...
    ASTNode* ReduceExpressionsVisitor::binop_replace(NBinaryOp *op) {

        // check that both are literals, if not replace nothing
        if(!python::isLiteralType(op->_left->getInferredType()) ||
           !python::isLiteralType(op->_right->getInferredType()))
            return op;

        // so far only a subset of binary operations is supported...
        switch(op->_op) {
            case TokenType::STAR: {
                // special case: string * int
                // since bool :< int, implicit cast here possible...
                if(op->_left->getInferredType() == python::Type::STRING
                   && (op->_right->getInferredType() == python::Type::I64
                       || op->_right->getInferredType() == python::Type::BOOLEAN)) {
                    int64_t times = op->_right->getInferredType() == python::Type::BOOLEAN
                                    ? booleanToI64(static_cast<NBoolean*>(op->_right))
                                    : static_cast<NNumber*>(op->_right)->getI64();

                    NString *s = new NString(escape_to_python_str(replicateString(static_cast<NString*>(op->_left)->value(), times)));
                    delete op;
                    _numReductions++;
                    return s;
                }

                // special case: int * string
                // since bool :< int, implicit cast here possible...
                if(op->_right->getInferredType() == python::Type::STRING
                   && (op->_left->getInferredType() == python::Type::I64
                       || op->_left->getInferredType() == python::Type::BOOLEAN)) {
                    int64_t times = op->_left->getInferredType() == python::Type::BOOLEAN
                                    ? booleanToI64(static_cast<NBoolean*>(op->_left))
                                    : static_cast<NNumber*>(op->_left)->getI64();

                    NString *s = new NString(escape_to_python_str(replicateString(static_cast<NString*>(op->_right)->value(), times)));
                    delete op;
                    _numReductions++;
                    return s;
                }

                // result will be supertype of the literals
                python::Type resType = binop_super_type(op->_left->getInferredType(),
                                                        op->_right->getInferredType());

                // check what return type is
                if(python::Type::F64 == resType) {
                    double x = toDouble(op->_left) * toDouble(op->_right);
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else if(python::Type::I64 == resType) {
                    int64_t x = toInt(op->_left) * toInt(op->_right);
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else {
                    error("unknown result type '" + resType.desc() + "' inferred, can't reduce");
                    return op;
                }
            }

            case TokenType::PLUS: {
                // special case string + string
                // aka string concatenation...
                if(op->_left->getInferredType() == python::Type::STRING &&
                   op->_right->getInferredType() == python::Type::STRING) {
                    auto lstr = static_cast<NString*>(op->_left)->value();
                    auto rstr = static_cast<NString*>(op->_right)->value();
                    std::string string_res = lstr + rstr;
                    NString *s = new NString(escape_to_python_str(string_res));
                    assert(s->value() == string_res);
                    delete op;
                    _numReductions++;
                    return s;
                }


                // result will be supertype of the literals
                python::Type resType = binop_super_type(op->_left->getInferredType(),
                                                        op->_right->getInferredType());

                // check what return type is
                if(python::Type::F64 == resType) {
                    double x = toDouble(op->_left) + toDouble(op->_right);
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else if(python::Type::I64 == resType) {
                    int64_t x = toInt(op->_left) + toInt(op->_right);
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else {
                    error("unknown result type '" + resType.desc() + "' inferred, can't reduce");
                    return op;
                }
            }

            case TokenType::MINUS: {
                // result will be supertype of the literals
                python::Type resType = binop_super_type(op->_left->getInferredType(),
                                                        op->_right->getInferredType());

                // check what return type is
                if(python::Type::F64 == resType) {
                    double x = toDouble(op->_left) - toDouble(op->_right);
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else if(python::Type::I64 == resType) {
                    int64_t x = toInt(op->_left) - toInt(op->_right);
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else {
                    error("unknown result type '" + resType.desc() + "' inferred, can't reduce");
                    return op;
                }
            }

                // note: according to python3, result of a division is always a float!
            case TokenType::SLASH: {
                // result will be always f64
                python::Type superType = binop_super_type(op->_left->getInferredType(),
                                                          op->_right->getInferredType());

                // check what return type is
                if(python::Type::F64 == superType) {
                    double a = toDouble(op->_left);
                    double b = toDouble(op->_right);
                    // python issues a zero division error on this...
                    if(b == 0.0) {
                        error("zero division found");
                        return nullptr;
                    }

                    double x = a / b;
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    num->setInferredType(python::Type::F64);
                    delete op;
                    _numReductions++;
                    return num;
                } else if(python::Type::I64 == superType) {
                    int64_t a = toInt(op->_left);
                    int64_t b = toInt(op->_right);
                    // python issues a zero division error on this...
                    if(b == 0) {
                        error("zero division found");
                        return nullptr;
                    }

                    double x = (double)a / (double)b;
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    num->setInferredType(python::Type::F64);
                    delete op;
                    _numReductions++;
                    return num;
                } else {
                    error("unknown result type '" + superType.desc() + "' inferred, can't reduce");
                    return op;
                }
            }

            case TokenType::PERCENT: {

                // special case string not yet supported
                if(op->_left->getInferredType() == python::Type::STRING &&
                   op->_right->getInferredType() == python::Type::STRING) {
                    error("string formatting not yet supported!");
                    return nullptr;
                }

                // result will be supertype of the literals
                python::Type resType = binop_super_type(op->_left->getInferredType(),
                                                        op->_right->getInferredType());

                // check what return type is
                if(python::Type::F64 == resType) {
                    double a = toDouble(op->_left);
                    double b = toDouble(op->_right);
                    // python issues a zero division error on this...
                    if(b == 0.0) {
                        error("zero division found");
                        return nullptr;
                    }

                    double x = fmod(a, b);
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else if(python::Type::I64 == resType) {

                    int64_t a = toInt(op->_left);
                    int64_t b = toInt(op->_right);
                    // python issues a zero division error on this...
                    if(b == 0) {
                        error("zero division found");
                        return nullptr;
                    }

                    int64_t x = a % b;
                    std::stringstream ss;
                    ss<<x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else {
                    error("unknown result type '" + resType.desc() + "' inferred, can't reduce");
                    return op;
                }
            }

                // floor division, result is int or float
                //
            case TokenType::DOUBLESLASH: {

                // special case string not yet supported
                if (op->_left->getInferredType() == python::Type::STRING &&
                    op->_right->getInferredType() == python::Type::STRING) {
                    error("string formatting not yet supported!");
                    return nullptr;
                }

                // result will be supertype of the literals
                python::Type resType = binop_super_type(op->_left->getInferredType(),
                                                        op->_right->getInferredType());

                // check what return type is
                if (python::Type::F64 == resType) {

                    double a = toDouble(op->_left);
                    double b = toDouble(op->_right);
                    // python issues a zero division error on this...
                    if (b == 0.0) {
                        error("zero division found");
                        return nullptr;
                    }

                    double x = floor(a / b);

                    std::stringstream ss;
                    ss << x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else if (python::Type::I64 == resType) {

                    int64_t a = toInt(op->_left);
                    int64_t b = toInt(op->_right);

                    // python issues a zero division error on this...
                    if (b == 0) {
                        error("zero division found");
                        return nullptr;
                    }

                    int64_t x = core::floori(a, b);
                    std::stringstream ss;
                    ss << x;
                    NNumber *num = new NNumber(ss.str());
                    delete op;
                    _numReductions++;
                    return num;
                } else {
                    error("unknown result type '" + resType.desc() + "' inferred, can't reduce");
                    return op;
                }
            }
            case TokenType::AND: {
                // replace with boolean
                auto op1 = toBool(op->_left);
                auto op2 = toBool(op->_right);

                delete op;
                op = nullptr;
                _numReductions++;
                return new NBoolean(op1 && op2);

            }

            case TokenType::OR: {
                auto op1 = toBool(op->_left);
                auto op2 = toBool(op->_right);

                delete op;
                op = nullptr;
                _numReductions++;
                return new NBoolean(op1 || op2);
            }

            default: {
                error("Unsupported operation '" + opToString(op->_op) + "' detected, can't perform reduction");
                return op;
            }
        }
    }

    ASTNode* ReduceExpressionsVisitor::replace(ASTNode *parent, ASTNode *node) {
        // parent must always be set
        assert(parent);

        // next may be an empty field
        if(!node)
            return nullptr;

        // check what type next is and optimize away if possible
        switch(node->type()) {
            // when function is encountered, collect var names!
            case ASTNodeType::Function: {
                NFunction* func = dynamic_cast<NFunction*>(node);
                _currentFunctionLocals = getFunctionVariables(func);
                _currentFunctionParams = getFunctionParameters(func);
                break;
            }

                // identifier? check if we can replace a simple global with a node!
            case ASTNodeType::Identifier: {
                // only if not in local function vars or params!
                auto name = ((NIdentifier*)node)->_name;
                if(_currentFunctionLocals.find(name) == _currentFunctionLocals.end() &&
                   std::find(_currentFunctionParams.begin(),
                             _currentFunctionParams.end(), name) == _currentFunctionParams.end()) {
                    // is it in globals?
                    auto constants = _closure.constants();
                    auto it = std::find_if(constants.begin(), constants.end(), [&](const tuplex::ClosureEnvironment::Constant& c) {
                        return c.identifier == name;
                    });
                    if(it != constants.end()) {
                        // replace!
                        auto type = it->type;
                        auto value = it->value;

                        auto new_node = fieldToAST(value);
                        if(!new_node) {
                            Logger::instance().defaultLogger().debug("no support for converting field type " + value.getType().desc() + " to ast");
                        } else {
                            return new_node;
                        }
                    }
                }
                break;
            }

            case ASTNodeType::Compare: {

                NCompare *cmp = static_cast<NCompare *>(node);
                if (cmp->_left && cmp->_ops.size() == 0 && cmp->_comps.size() == 0) {
                    // remove the "next" node
                    ASTNode *res = cmp->_left->clone();
                    delete cmp;
                    _numReductions++;
                    return res;
                } else {
                    // check if all of the expressions are literals, if so reduction is possible
                    bool areAllLiterals = python::isLiteralType(cmp->_left->getInferredType());
                    int pos = 0;
                    while(areAllLiterals && pos < cmp->_comps.size() > 0) {
                        areAllLiterals = python::isLiteralType(cmp->_comps[pos++]->getInferredType());
                    }

                    if(areAllLiterals)
                        return cmp_replace(cmp);
                    else
                        // nothing can be optimized
                        return cmp;
                }

                break;
            }

            case ASTNodeType::BinaryOp: {
                NBinaryOp *op = static_cast<NBinaryOp*>(node);
                // make sure both operand are already reduced,
                // so they are literals by first visiting their subbranches!
                return binop_replace(op);
            }

            case ASTNodeType::UnaryOp: {
                NUnaryOp *op = static_cast<NUnaryOp*>(node);

                if(python::isLiteralType(op->_operand->getInferredType())) {
                    // reduction possible here

                    // only available for bool, i64, f64
                    python::Type optype = op->_operand->getInferredType();
                    if(!(optype == python::Type::I64 ||
                         optype == python::Type::BOOLEAN ||
                         optype == python::Type::F64)) {
                        std::stringstream ss;
                        ss<<"Can't reduce expression '"<<opToString(op->_op)<<optype.desc()<<"'";
                        error(ss.str());
                        return node;
                    }

                    switch(op->_op) {
                        case TokenType::MINUS: {
                            std::string val;
                            // if the operand is boolean, we convert it to int (true for 1 and false for 0)
                            // otherwise it should be a number
                            if (optype == python::Type::BOOLEAN) {
                                NBoolean *boolean = static_cast<NBoolean*>(op->_operand);
                                int64_t x = booleanToI64(boolean);
                                std::stringstream ss;
                                ss<<(-x);
                                val = ss.str();
                            } else {
                                val = static_cast<NNumber *>(op->_operand)->_value;
                                if (val[0] == '-')
                                    val = val.substr(1);
                                else
                                    val = "-" + val;
                            }
                            NNumber *num = new NNumber(val);
                            delete node;
                            _numReductions++;
                            return num;
                        }
                        case TokenType::PLUS: {
                            ASTNode *num = op->_operand->clone();
                            delete node;
                            _numReductions++;
                            return num;
                        }
                        case TokenType::TILDE: {
                            // this is two's complement
                            // i.e. ~x = -x-1 (for both bool and int, bool is implicitly cast to int)
                            // operation only allowed on bool and int!
                            // on float this gives a type error ...
                            if(optype == python::Type::F64) {
                                std::stringstream ss;
                                ss<<"Unary bitwise complement can't be applied to '"<<optype.desc()<<"' type";
                                error(ss.str());
                                return node;
                            }

                            int64_t x = 0;

                            // implicitly cast bool if necessary to int
                            if(op->_operand->type() == ASTNodeType::Boolean) {
                                NBoolean *boolean = static_cast<NBoolean*>(op->_operand);
                                x = booleanToI64(boolean);
                            } else {
                                // operand's type must be int
                                assert(op->_operand->getInferredType() ==  python::Type::I64);
                                x = static_cast<NNumber*>(op->_operand)->getI64();
                            }

                            std::stringstream ss;
                            ss<<(-x - 1);

                            NNumber *num = new NNumber(ss.str());
                            delete node;
                            _numReductions++;
                            return num;
                        }
                        default: {
                            std::stringstream ss;
                            ss<<"Can't reduce expression '"<<opToString(op->_op)<<optype.desc()<<"', unknown operation "<<opToString(op->_op)<<" encountered";
                            error(ss.str());
                            return node;
                        }
                    }
                } else {
                    return node;
                }
            }
            default:
                return node;
        }

        //!!! important if any of the logic above doesn't return
        return node;
    }
}