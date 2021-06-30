//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ASTNODES_H
#define TUPLEX_ASTNODES_H

#include <string>
#include <vector>
#include "IVisitor.h"
#include "TokenType.h"
#include "Token.h"
#include "TypeSystem.h"
#include "Base.h"
#include <vector>
#include <TSet.h>
#include "ASTAnnotation.h"

namespace tuplex {
    // @Todo: To avoid writing all these clone functions,
// use Polymorphic copy construction as described here
// https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern


// compiling with this option will add position of tokens to ASTNodes
//#define ASTNODES_WITH_LOCATION_

    enum class ASTNodeType {
        UNKNOWN,
        None,
        ParameterList,
        Parameter,
        Function,
        Number,
        Identifier,
        Boolean,
        Ellipsis,
        String,
        BinaryOp,
        UnaryOp,
        Suite,
        Module,
        Lambda,
        Await,
        StarExpression,
        Compare,
        IfElse,
        Tuple,
        Dictionary,
        List,
        Subscription,
        Return,
        Assign,
        Call,
        Attribute,
        Slice,
        SliceItem,
        ListComprehension,
        Comprehension,
        Range,
        Assert,
        Raise,
        For,
        While,
        Break,
        Continue
    };

//@TODO: Make sure that cloning also copies over protected attributes!

// base class used to store any node in the AST tree
    class ASTNode {
    protected:
#ifdef ASTNODES_WITH_LOCATION_
        // position of token
    int _line;
    int _start;
    int _end;
#endif

        // annotations for an AST Node:
        python::Type _inferredType; // --> annotated by lexer or type inference pass... @TODO
        int          _scopeID; // to which scope does this node belong to? --> annotated by SymbolTableVisitor
        ASTAnnotation *_annotation; // optional annotation stored for this node


        virtual void copyAstMembers(const ASTNode& other) {
            _inferredType = other._inferredType;
            _scopeID = other._scopeID;
            if(other._annotation)
                _annotation = new ASTAnnotation(*other._annotation);
        }
    public:
        ASTNode() : _inferredType(python::Type::UNKNOWN), _scopeID(-1), _annotation(nullptr)    {}

        virtual ~ASTNode() {
            if(_annotation)
                delete _annotation;
        }

        // returns a (deep) copy of the AST Node
        // also need to transfer protected members
        virtual ASTNode* clone() = 0;

        // later do codegen
        // virtual llvm::Value* codegen() = 0;

        // used for visitor pattern. => recursive traversal must be implemented by visitor!
        virtual void accept(class IVisitor& visitor) = 0;

        virtual ASTNodeType type() const = 0;

        void setInferredType(const python::Type& type) { _inferredType = type; }
        virtual python::Type getInferredType() { return _inferredType; }

        virtual ASTAnnotation& annotation() {
            if(!_annotation)
                _annotation = new ASTAnnotation();
            return *_annotation;
        }

        inline void removeAnnotation() {
            if(_annotation)
                delete _annotation;
            _annotation = nullptr;
        }

        inline bool hasAnnotation() const { return _annotation; }
    };


/**********************************************/
// I. atoms
/**********************************************/


    class NNumber : public ASTNode {
    public:
        std::string _value;

        NNumber() {}

        NNumber(const std::string& value): _value(value) {}
        NNumber(int64_t value): _value(std::to_string(value)) {
            setInferredType(python::Type::I64);
        }
        NNumber(double value): _value(std::to_string(value)) {
            setInferredType(python::Type::F64);
        }

        ~NNumber() {}

        // copy constructor
        NNumber(const NNumber& other) {
            _value = other._value;
            copyAstMembers(other);
        }

        NNumber& operator = (const NNumber& other) {
            if(&other != this) {
                _value = other._value;
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NNumber(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Number; };

        python::Type getInferredType() {
            // lazy update
            if(python::Type::UNKNOWN == _inferredType) {
                // check whether it is i64 or f64
                // it is f64 if a dot or e/E occurs...
                if(_value.find(".") != std::string::npos ||
                   _value.find("e") != std::string::npos ||
                   _value.find("E") != std::string::npos)
                    _inferredType = python::Type::F64;
                else
                    _inferredType = python::Type::I64;
            }
            return _inferredType;
        }

        int64_t getI64() {
            return std::stoll(_value);
        }

        double getF64() {
            return std::stod(_value);
        }
    };

    class NIdentifier : public ASTNode {
    public:
        std::string _name;

        NIdentifier() {}

        NIdentifier(const std::string& name):_name(name) {}

        ~NIdentifier() {}

        // copy constructor
        NIdentifier(const NIdentifier& other) {
            _name = other._name;
            copyAstMembers(other);
        }

        NIdentifier& operator = (const NIdentifier& other) {
            if(&other != this) {
                _name = other._name;
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NIdentifier(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Identifier; };

    };

    class NBoolean : public ASTNode {
    public:
        bool _value;

        NBoolean() {
            _inferredType = python::Type::BOOLEAN;
        }

        NBoolean(const std::string& val) {
            if(val.compare("True") == 0)
                _value = true;
            else
                _value = false;
            _inferredType = python::Type::BOOLEAN;
        }

        NBoolean(const bool value) : _value(value) { _inferredType = python::Type::BOOLEAN; }

        ~NBoolean() {}

        // copy constructor
        NBoolean(const NBoolean& other) {
            _value = other._value;
            copyAstMembers(other);
        }

        NBoolean& operator = (const NBoolean& other) {
            if(&other != this) {
                _value = other._value;
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NBoolean(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Boolean; };
    };

// nothing associated to this class
// can use this as template code
    class NEllipsis : public ASTNode {
    public:
        NEllipsis() {}

        // copy constructor
        NEllipsis(const NEllipsis& other) {
            copyAstMembers(other);
        }

        NEllipsis& operator = (const NEllipsis& other) {
            if(&other != this) {
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NEllipsis(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Ellipsis; };

    };

    class NNone : public ASTNode {
    public:
        NNone() {}

        // copy constructor
        NNone(const NNone& other) {
            copyAstMembers(other);
        }

        NNone& operator = (const NNone& other) {
            if(&other != this) {
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NNone(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::None; };

        // always give NULLVALUE back!
        python::Type getInferredType() { return python::Type::NULLVALUE; }

    };

    class NString : public ASTNode {
    private:
        std::string _raw_value;
    public:


        NString(const std::string& raw_value) : _raw_value(raw_value) {
            _inferredType = python::Type::STRING;
        }

        // copy constructor
        NString(const NString& other) {
            _raw_value = other._raw_value;
            copyAstMembers(other);
        }

        NString& operator = (const NString& other) {
            if(&other != this) {
                _raw_value = other._raw_value;
                copyAstMembers(other);
            }
            return *this;
        }

        std::string value() const { return str_value_from_python_raw_value(_raw_value);}

        virtual ASTNode* clone() {return new NString(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::String; };

        // one string statement might be comprised of a string expression of the form STRING+
        // we simply amend all the values => implement a smarter normalization aware strategy for this later
        NString* amend(const std::string& val) {
            _raw_value += val;
            return this;
        }
    };

/**********************************************/
// II. Expressions
/**********************************************/


    class NBinaryOp : public ASTNode {
        inline void release() {
            if(_left)
                delete _left;
            _left = nullptr;
            if(_right)
                delete _right;
            _right = nullptr;
        }
    public:

        TokenType _op;
        ASTNode *_left;
        ASTNode *_right;

        NBinaryOp():_left(nullptr), _right(nullptr),_op(TokenType::UNKNOWN)    {}

        NBinaryOp(ASTNode* left, const TokenType&& op, ASTNode* right) {
            _op = op;
            _left = left->clone();
            _right = right->clone();
        }

        NBinaryOp(const NBinaryOp& other) {
            _op = other._op;
            _left = other._left->clone();
            _right = other._right->clone();
            copyAstMembers(other);
        }

        ~NBinaryOp() {
            release();
        }

        NBinaryOp& operator = (const NBinaryOp& other) {
            if(&other != this) {
                release();

                _op = other._op;
                _left = other._left->clone();
                _right = other._right->clone();
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() {return new NBinaryOp(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::BinaryOp; };

    };

    class NUnaryOp : public ASTNode {
    public:
        TokenType _op;
        ASTNode *_operand;

        NUnaryOp():_operand(nullptr), _op(TokenType::UNKNOWN)    {}

        NUnaryOp(const TokenType&& op, ASTNode *operand) {
            _op = op;
            _operand = operand->clone();
        }

        NUnaryOp(const NUnaryOp& other) {
            _op = other._op;
            _operand = other._operand->clone();
            copyAstMembers(other);
        }

        ~NUnaryOp() {
            if(_operand)
                delete _operand;
            _operand = nullptr;
        }

        NUnaryOp& operator = (const NUnaryOp& other) {
            if(&other != this) {
                if(_operand)
                    delete _operand;
                _operand = nullptr;

                _op = other._op;
                _operand = other._operand->clone();
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() {return new NUnaryOp(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::UnaryOp; };

    };



    class NSubscription : public ASTNode {
    public:

        // a nullptr means error
        ASTNode *_value; // the value to be indexed
        ASTNode *_expression; // indexing expression


        NSubscription():_value(nullptr), _expression(nullptr)    {}

        NSubscription(ASTNode *value, ASTNode *expression) {
            _value = value->clone();
            _expression = expression->clone();
        }

        NSubscription(const NSubscription& other) {
            _value = other._value->clone();
            _expression = other._expression->clone();
            copyAstMembers(other);
        }

        ~NSubscription() {
            if(_expression)
                delete _expression;
            _expression = nullptr;

            if(_value)
                delete _value;
            _value = nullptr;
        }

        NSubscription& operator = (const NSubscription& other) {
            if(&other != this) {
                if(_expression)
                    delete _expression;
                _expression = nullptr;

                if(_value)
                    delete _value;
                _value = nullptr;
                _value = other._value->clone();
                _expression = other._expression->clone();
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() {return new NSubscription(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Subscription; };

    };

/**********************************************/
// X. TopLevel Nodes
/**********************************************/

// a suite is a collection of statements
    class NSuite : public ASTNode {
    public:
        std::vector<ASTNode*> _statements;
        bool _isUnrolledLoopSuite;

        NSuite() {
            _isUnrolledLoopSuite = false;
        }

        ~NSuite() {
            if(!_statements.empty())
                std::for_each(_statements.begin(), _statements.end(), [](ASTNode *node){delete node; node=nullptr;});
        }

        // copy constructor
        NSuite(const NSuite& other) {
            // copy statements of other
            if(!other._statements.empty()) {
                _statements.clear();
                _statements.reserve(other._statements.size());
                std::for_each(other._statements.begin(), other._statements.end(), [this](ASTNode *node) {
                    _statements.push_back(node->clone());
                });
            }
            _isUnrolledLoopSuite = other._isUnrolledLoopSuite;

            copyAstMembers(other);
        }

        NSuite& operator = (const NSuite& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._statements.empty()) {
                    // delete statements
                    if(!_statements.empty())
                        std::for_each(_statements.begin(), _statements.end(), [](ASTNode *node){delete node; node=nullptr;});

                    _statements.clear();
                    _statements.reserve(other._statements.size());
                    std::for_each(other._statements.begin(), other._statements.end(), [this](ASTNode *node) {
                        _statements.push_back(node->clone());
                    });
                }
                _isUnrolledLoopSuite = other._isUnrolledLoopSuite;

                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NSuite(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Suite; };

        void addStatement(ASTNode *node) {
            assert(node);
            _statements.push_back(node->clone());
        }

        void addSuite(const NSuite* other) {
            assert(other);

            if(!other->_statements.empty())
                std::for_each(other->_statements.begin(), other->_statements.end(), [this](ASTNode *node){ _statements.push_back(node->clone()); });

        }
    };

    class NModule : public ASTNode {
    public:
        ASTNode *_suite;

        NModule():_suite(nullptr) {}

        ~NModule() { if(_suite)delete _suite; _suite = nullptr;}

        NModule(NSuite *suite) {
            _suite = suite->clone();
        }

        NModule(ASTNode *node) {
            _suite = node->clone();
        }

        // copy constructor
        NModule(const NModule& other) {
            // copy statements of other
            _suite = other._suite->clone();
            copyAstMembers(other);
        }

        NModule& operator = (const NModule& other) {
            if(&other != this) {
                if(_suite)delete _suite; _suite = nullptr;
                _suite = other._suite->clone();
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NModule(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Module; };
    };


    class NParameter : public ASTNode {
        void release() {
            if(_identifier)
                delete _identifier;
            if(_annotation)
                delete _annotation;
            if(_default)
                delete _default;
            _default = nullptr;
            _identifier = nullptr;
            _annotation = nullptr;
        }
    public:

        NIdentifier *_identifier;

        // Python3 has an annotation feature. I.e. when defining a function, optionally an expression can be annotated
        // i.e. def fun(x : int):
        //          pass
        // later this feature should be removed perhaps from the ast tree...
        // in python annotations can be accesed via fun.__annotations__ => could be used for simplified type inference!
        ASTNode *_annotation;

        // optional default values
        ASTNode *_default;

        NParameter() : _identifier(nullptr), _annotation(nullptr), _default(nullptr)    {}

        NParameter(NIdentifier *identifier) {
            _identifier = static_cast<NIdentifier*>(identifier->clone());
            _annotation = nullptr;
            _default = nullptr;
        }


        // copy constructor
        NParameter(const NParameter& other) {
            _annotation = nullptr;
            _default = nullptr;

            // copy statements of other
            _identifier = static_cast<NIdentifier*>(other._identifier->clone());
            if(other._annotation)
                _annotation = other._annotation->clone();
            if(other._default)
                _default = other._default->clone();
            copyAstMembers(other);
        }

        NParameter& operator = (const NParameter& other) {
            if(&other != this) {
                release();
                _identifier = static_cast<NIdentifier*>(other._identifier->clone());
                if(other._annotation)
                    _annotation = other._annotation->clone();
                if(other._default)
                    _default = other._default->clone();
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NParameter(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Parameter; };


        NParameter*  setAnnotation(ASTNode *node) {
            if(_annotation)
                delete _annotation;
            _annotation = node->clone();
            return this;
        }

        NParameter*  setDefault(ASTNode *node) {
            if(!node)
                return this;

            if(_default)
                delete _default;
            _default = node->clone();
            return this;
        }

    };

// helper class for constructing higher level AST Nodes
// this one here is for collecting ASTs in a list
    class NCollection : public ASTNode {
    protected:
        std::vector<ASTNode*> _elements;

    public:

        NCollection() {}

        ~NCollection() {
            if(!_elements.empty())
                std::for_each(_elements.begin(), _elements.end(), [](ASTNode *node){delete node; node=nullptr;});
        }

        // copy constructor
        NCollection(const NCollection& other) {
            // copy statements of other
            if(!other._elements.empty()) {
                _elements.clear();
                _elements.reserve(other._elements.size());
                std::for_each(other._elements.begin(), other._elements.end(), [this](ASTNode *node) {
                    _elements.push_back(node->clone());
                });
            }
            copyAstMembers(other);
        }

        NCollection& operator = (const NCollection& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._elements.empty()) {
                    // delete statements
                    if(!_elements.empty())
                        std::for_each(_elements.begin(), _elements.end(), [](ASTNode *node){delete node; node=nullptr;});

                    _elements.clear();
                    _elements.reserve(other._elements.size());
                    std::for_each(other._elements.begin(), other._elements.end(), [this](ASTNode *node) {
                        _elements.push_back(node->clone());
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NCollection(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::UNKNOWN; };

        void add(ASTNode *node) {
            _elements.push_back(node->clone());
        }

        void addMultiple(const NCollection* other) {
            assert(other);

            if(!other->_elements.empty())
                std::for_each(other->_elements.begin(), other->_elements.end(), [this](ASTNode *node){ _elements.push_back(node->clone()); });

        }

        std::vector<ASTNode*> elements() const { return _elements; }
    };

    class NParameterList : public ASTNode {
    public:

        // the nodes are usually NParameters, but they can be also NUnaryOp(STAR) or NUnaryOp(DOUBLESTAR)
        std::vector<ASTNode*> _args;

        NParameterList() {}

        ~NParameterList() {
            if(!_args.empty())
                std::for_each(_args.begin(), _args.end(), [](ASTNode *node){delete node; node=nullptr;});
        }

        // copy constructor
        NParameterList(const NParameterList& other) {
            // copy statements of other
            if(!other._args.empty()) {
                _args.clear();
                _args.reserve(other._args.size());
                std::for_each(other._args.begin(), other._args.end(), [this](ASTNode *node) {
                    _args.push_back(node->clone());
                });
            }
            copyAstMembers(other);
        }

        NParameterList& operator = (const NParameterList& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._args.empty()) {
                    // delete statements
                    if(!_args.empty())
                        std::for_each(_args.begin(), _args.end(), [](ASTNode *node){delete node; node=nullptr;});

                    _args.clear();
                    _args.reserve(other._args.size());
                    std::for_each(other._args.begin(), other._args.end(), [this](ASTNode *node) {
                        _args.push_back(node->clone());
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }


        NParameterList* addParam(ASTNode *node) {
            assert(node);

            _args.push_back(node->clone());
            return this;
        }

        NParameterList* addParameterList(NParameterList* other) {
            assert(other);

            if(!other->_args.empty())
                std::for_each(other->_args.begin(), other->_args.end(), [this](ASTNode *node){ _args.push_back(node->clone()); });
            return this;
        }

        virtual ASTNode* clone() {return new NParameterList(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::ParameterList; };

        virtual python::Type getInferredType() {
            std::vector<python::Type> types;
            for(auto arg : _args)
                types.push_back(arg->getInferredType());

            // return tuple from arguments
            return python::Type::makeTupleType(types);
        }
    };

    class NLambda : public ASTNode {
    private:
        //! annotation for later code generation,
        //! states whether the first param should be treated as tuple or not
        bool _treatFirstArgAsTuple;

        void release() {
            if(_arguments)
                delete _arguments;
            if(_expression)
                delete _expression;
            _arguments = nullptr;
            _expression = nullptr;
            _treatFirstArgAsTuple = false;
        }
    public:
        NParameterList *_arguments;
        ASTNode *_expression;

        NLambda() : _arguments(nullptr), _expression(nullptr), _treatFirstArgAsTuple(false)   {}

        NLambda(NParameterList *params, ASTNode *expression) {
            assert(expression);

            _arguments = nullptr;
            _expression = nullptr;

            if(params)
                _arguments = static_cast<NParameterList*>(params->clone());
            if(expression)
                _expression = expression->clone();
            _treatFirstArgAsTuple = false;
        }

        ~NLambda() { release(); }

        // copy constructor
        NLambda(const NLambda& other) {
            // copy statements of other
            if(other._arguments)
                _arguments = static_cast<NParameterList*>(other._arguments->clone());
            else
                _arguments = nullptr;
            _expression = other._expression->clone();
            _treatFirstArgAsTuple = other._treatFirstArgAsTuple;
            copyAstMembers(other);
        }

        NLambda& operator = (const NLambda& other) {
            if(&other != this) {
                release();
                _arguments = static_cast<NParameterList*>(other._arguments->clone());
                _expression = other._expression->clone();
                _treatFirstArgAsTuple = other._treatFirstArgAsTuple;
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NLambda(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Lambda; };

        void setFirstArgTreatment(bool treatAsTuple) { _treatFirstArgAsTuple = treatAsTuple; }
        bool isFirstArgTuple() { return _treatFirstArgAsTuple; }

        inline std::vector<std::string> parameterNames() const {
            std::vector<std::string> names;
            if(_arguments) {
                for(auto arg : _arguments->_args) {
                    assert(arg->type() == ASTNodeType::Parameter);
                    names.emplace_back(((NParameter*)arg)->_identifier->_name);
                }
            }
            return names;
        }
    };


    class NAwait : public ASTNode {
    public:
        ASTNode *_target;

        NAwait() : _target(nullptr)     {}

        NAwait(ASTNode *target) {
            _target = target->clone();
        }

        ~NAwait() {
            if(_target)
                delete _target;
            _target = nullptr;
        }

        NAwait(const NAwait& other) {
            _target = nullptr;

            if(other._target) {
                _target = other._target->clone();
            }
            copyAstMembers(other);
        }

        NAwait& operator = (const NAwait& other) {
            if(&other != this) {
                if(_target)
                    delete _target;
                _target = nullptr;

                if(other._target) {
                    _target = other._target->clone();
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NAwait(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Await; };
    };

    class NStarExpression : public ASTNode {
    public:
        ASTNode *_target;

        NStarExpression() : _target(nullptr)     {}

        NStarExpression(ASTNode *target) {
            _target = target->clone();
        }

        ~NStarExpression() {
            if(_target)
                delete _target;
            _target = nullptr;
        }

        NStarExpression(const NStarExpression& other) {
            _target = nullptr;

            if(other._target) {
                _target = other._target->clone();
            }
            copyAstMembers(other);
        }

        NStarExpression& operator = (const NStarExpression& other) {
            if(&other != this) {
                if(_target)
                    delete _target;
                _target = nullptr;

                if(other._target) {
                    _target = other._target->clone();
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NStarExpression(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::StarExpression; };

    };

    class NCompare : public ASTNode {

        void cloneFrom(const NCompare& other) {
            _left = nullptr;

            if(other._left)
                _left = other._left->clone();

            // copy the arrays
            _ops.clear();
            _comps.clear();

            _ops.reserve(other._ops.size());
            _comps.reserve(other._comps.size());\

            std::for_each(other._ops.begin(), other._ops.end(), [this](TokenType tt) { _ops.push_back(tt); });
            std::for_each(other._comps.begin(), other._comps.end(), [this](ASTNode *node) { _comps.push_back(node->clone()); });
            copyAstMembers(other);
        }

        void release() {
            std::for_each(_comps.begin(), _comps.end(), [this](ASTNode *node) { if(node)delete  node; node = nullptr; });
            _comps.clear();
            _ops.clear();
        }
    public:

        ASTNode *_left;
        std::vector<TokenType> _ops; // operands
        std::vector<ASTNode*> _comps; // comparators

        NCompare() : _left(nullptr) {}

        NCompare(const NCompare& other) {
            cloneFrom(other);
        }

        NCompare& operator = (const NCompare& other) {
            if(&other != this) {
                release();
                cloneFrom(other);
            }
            return *this;
        }

        NCompare& addCmp(const TokenType&& tt, ASTNode *node) {
            _ops.push_back(tt);
            _comps.push_back(node->clone());
            return *this;
        }

        NCompare& setLeft(ASTNode *node) {

            if(_left)
                delete _left;

            _left = node->clone();
            return *this;
        }

        virtual ASTNode* clone() {return new NCompare(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Compare; };

    };


    class NIfElse : public ASTNode {
    private:
        void release() {
            if(_expression)
                delete _expression;
            if(_then)
                delete _then;
            if(_else)
                delete _else;
            _expression = nullptr;
            _then = nullptr;
            _else = nullptr;
        }

        void cloneFrom(const NIfElse& other) {
            if(other._expression)
                _expression = other._expression->clone();
            if(other._then)
                _then = other._then->clone();
            if(other._else)
                _else = other._else->clone();
            _isExpression = other._isExpression;

            copyAstMembers(other);
        }
        bool _isExpression;
    public:
        // abstract syntax tree

        ASTNode* _expression;
        ASTNode* _then;
        ASTNode* _else;

        NIfElse():_expression(nullptr), _then(nullptr), _else(nullptr), _isExpression(false)    {}
        ~NIfElse() { release(); }

        NIfElse(ASTNode* expression, ASTNode* thenChild, ASTNode* elseChild, bool isExpression) : _isExpression(isExpression) {
            _expression = nullptr;
            _then = nullptr;
            _else = nullptr;

            if(expression)
                _expression = expression->clone();
            if(thenChild)
                _then = thenChild->clone();
            if(elseChild)
                _else = elseChild->clone();
        }

        NIfElse(const NIfElse& other) {
            _expression = nullptr;
            _then = nullptr;
            _else = nullptr;

            cloneFrom(other);
        }

        NIfElse& operator = (const NIfElse& other) {
            if(&other != this) {
                release();
                cloneFrom(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NIfElse(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::IfElse; };

        void setElse(ASTNode* elseChild) {
            if(_else)
                delete _else;
            _else = nullptr;
            if(elseChild)
                _else = elseChild->clone();
        }

        /*!
         * is it a if-else stmt with two blocks or an inline expression?
         * @return true if inline expression
         */
        bool isExpression() {
            return _isExpression;
        }
    };

    class NFunction : public ASTNode {

        bool _treatFirstArgAsTuple;
        void release() {
            if(_name)
                delete _name;
            if(_parameters)
                delete _parameters;
            if(_annotation)
                delete _annotation;
            if(_suite)
                delete _suite;

            _treatFirstArgAsTuple = false;
        }

        void cloneFrom(const NFunction& other) {
            _parameters = nullptr;
            _annotation = nullptr;

            // function must have name & suite
            assert(other._name);
            _name = static_cast<NIdentifier*>(other._name->clone());

            assert(other._suite);
            _suite = static_cast<NSuite*>(other._suite->clone());

            if(other._parameters)
                _parameters = static_cast<NParameterList*>(other._parameters->clone());
            if(other._annotation)
                _annotation = other._annotation->clone();

            _treatFirstArgAsTuple = other._treatFirstArgAsTuple;

            copyAstMembers(other);
        }

    public:
        NIdentifier *_name;
        NParameterList *_parameters;
        ASTNode *_annotation;
        ASTNode  *_suite; // can be also a single statement OR expression!

        NFunction() : _name(nullptr), _parameters(nullptr), _annotation(nullptr), _suite(nullptr), _treatFirstArgAsTuple(false)   {}

        NFunction(NIdentifier *name, ASTNode *suite) {
            assert(name->type() == ASTNodeType::Identifier);
            _name = dynamic_cast<NIdentifier*>(name->clone());
            _suite = suite->clone();
            _parameters = nullptr;
            _annotation = nullptr;
            _treatFirstArgAsTuple = false;
        }

        ~NFunction() {
            release();
        }

        NFunction(const NFunction& other) {
            cloneFrom(other);
        }

        NFunction& operator = (const NFunction& other) {
            if(&other != this) {
                release();

                cloneFrom(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NFunction(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Function; };

        NFunction* setParams(NParameterList *params) {
            if(_parameters)
                delete _parameters;
            assert(params->type() == ASTNodeType::ParameterList);
            _parameters = dynamic_cast<NParameterList*>(params->clone());

            return this;
        }

        NFunction* setAnnotation(ASTNode *annotation) {
            if(_annotation)
                delete _annotation;
            _annotation = annotation->clone();

            return this;
        }

        void setFirstArgTreatment(bool treatAsTuple) { _treatFirstArgAsTuple = treatAsTuple; }
        bool isFirstArgTuple() { return _treatFirstArgAsTuple; }

        inline std::vector<std::string> parameterNames() const {
            std::vector<std::string> names;
            if(_parameters) {
                for(auto arg : _parameters->_args) {
                    assert(arg->type() == ASTNodeType::Parameter);
                    names.emplace_back(((NParameter*)arg)->_identifier->_name);
                }
            }
            return names;
        }
    };

    // holds a tuple
    class NTuple : public ASTNode {
    public:
        std::vector<ASTNode*> _elements;

        NTuple() {}

        ~NTuple() {
            if(!_elements.empty())
                std::for_each(_elements.begin(), _elements.end(), [](ASTNode *node){delete node; node=nullptr;});
        }

        // copy constructor
        NTuple(const NTuple& other) {
            // copy statements of other
            if(!other._elements.empty()) {
                _elements.clear();
                _elements.reserve(other._elements.size());
                std::for_each(other._elements.begin(), other._elements.end(), [this](ASTNode *node) {
                    _elements.push_back(node->clone());
                });
            }
            copyAstMembers(other);
        }

        /*!
         * creates tuple node from collection (may be empty). deletes collection
         */
        static NTuple* fromCollection(NCollection* collection) {
            assert(collection);
            NTuple* tuple = new NTuple();
            assert(tuple);

            for(auto el : collection->elements()) {
                tuple->_elements.push_back(el->clone());
            }

            delete collection;
            collection = nullptr;
            return tuple;
        }

        NTuple& operator = (const NTuple& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._elements.empty()) {
                    // delete statements
                    if(!_elements.empty())
                        std::for_each(_elements.begin(), _elements.end(), [](ASTNode *node){delete node; node=nullptr;});

                    _elements.clear();
                    _elements.reserve(other._elements.size());
                    std::for_each(other._elements.begin(), other._elements.end(), [this](ASTNode *node) {
                        _elements.push_back(node->clone());
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        // TODO: do Dictionary, List need similar methods?
        python::Type getInferredType() {
            // lazy update
            if(python::Type::UNKNOWN == _inferredType) {
                std::vector<python::Type> types;
                for(const auto& elt : _elements) {
                    types.push_back(elt->getInferredType());
                }
                _inferredType = python::Type::makeTupleType(types);
            }
            return _inferredType;
        }

        virtual ASTNode* clone() {return new NTuple(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Tuple; };
    };

// represents a dictionary expression ({ ... })
    class NDictionary : public ASTNode {
    public:
        std::vector<std::pair<ASTNode*, ASTNode*> > _pairs;

        NDictionary() {}

        ~NDictionary() {
            if (!_pairs.empty())
                std::for_each(_pairs.begin(), _pairs.end(), [](std::pair<ASTNode *, ASTNode *> nodes) {
                    delete nodes.first;
                    nodes.first = nullptr;
                    delete nodes.second;
                    nodes.second = nullptr;
                });
        }

        // copy constructor
        NDictionary(const NDictionary& other) {
            // copy statements of other
            if(!other._pairs.empty()) {
                _pairs.clear();
                _pairs.reserve(other._pairs.size());
                std::for_each(other._pairs.begin(), other._pairs.end(), [this](std::pair<ASTNode *, ASTNode *> nodes) {
                    _pairs.push_back(std::make_pair(nodes.first->clone(), nodes.second->clone()));
                });
            }
            copyAstMembers(other);
        }

        NDictionary& operator = (const NDictionary& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._pairs.empty()) {
                    if (!_pairs.empty())
                        std::for_each(_pairs.begin(), _pairs.end(), [](std::pair<ASTNode *, ASTNode *> nodes) {
                            delete nodes.first;
                            nodes.first = nullptr;
                            delete nodes.second;
                            nodes.second = nullptr;
                        });

                    _pairs.clear();
                    _pairs.reserve(other._pairs.size());
                    std::for_each(other._pairs.begin(), other._pairs.end(), [this](std::pair<ASTNode *, ASTNode *> nodes) {
                        _pairs.push_back(std::make_pair(nodes.first->clone(), nodes.second->clone()));
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NDictionary(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Dictionary; };
    };

// represents a list expression ([ ... ])
    class NList : public ASTNode {
    public:
        std::vector<ASTNode* > _elements;

        NList() {}

        ~NList() {
            if(!_elements.empty())
                std::for_each(_elements.begin(), _elements.end(), [](ASTNode *node){delete node; node=nullptr;});
        }

        // copy constructor
        NList(const NList& other) {
            // copy statements of other
            if(!other._elements.empty()) {
                _elements.clear();
                _elements.reserve(other._elements.size());
                std::for_each(other._elements.begin(), other._elements.end(), [this](ASTNode *node) {
                    _elements.push_back(node->clone());
                });
            }
            copyAstMembers(other);
        }

        NList& operator = (const NList& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._elements.empty()) {
                    // delete statements
                    if(!_elements.empty())
                        std::for_each(_elements.begin(), _elements.end(), [](ASTNode *node){delete node; node=nullptr;});

                    _elements.clear();
                    _elements.reserve(other._elements.size());
                    std::for_each(other._elements.begin(), other._elements.end(), [this](ASTNode *node) {
                        _elements.push_back(node->clone());
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() {return new NList(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::List; };
    };

    class NReturn : public ASTNode {
    public:

        ASTNode *_expression;
        NReturn(ASTNode *expression=nullptr)  { if(expression)_expression = expression->clone(); }
        NReturn(const NReturn& other) {
            _expression = other._expression ? other._expression->clone() : nullptr;
            copyAstMembers(other);
        }

        NReturn& operator = (const NReturn& other) {
            if(_expression)
                delete _expression;
            _expression = nullptr;
            _expression = other._expression ? other._expression->clone() : nullptr;
            copyAstMembers(other);
            return *this;
        }

        python::Type getInferredType() {
            // lazy update
            if(python::Type::UNKNOWN == _inferredType) {
                _inferredType = _expression->getInferredType();
            }
            return _inferredType;
        }

        virtual ASTNode* clone() { return new NReturn(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Return; };
    };

// cf. https://docs.python.org/3/reference/simple_stmts.html#grammar-token-assert-stmt
    class NAssert : public ASTNode {
    public:

        ASTNode *_expression;
        ASTNode *_errorExpression;
        NAssert(ASTNode *expression=nullptr, ASTNode* errorExpression=nullptr)  {
            _expression = nullptr;
            _errorExpression = nullptr;
            if(expression)_expression = expression->clone();
            if(errorExpression)_errorExpression = errorExpression->clone();
        }

        NAssert(const NAssert& other) {
            _expression = other._expression ? other._expression->clone() : nullptr;
            _errorExpression = other._errorExpression ? other._errorExpression->clone() : nullptr;
            copyAstMembers(other);
        }

        NAssert& operator = (const NAssert& other) {
            if(_expression)
                delete _expression;
            if(_errorExpression)
                delete _errorExpression;
            _expression = nullptr;
            _errorExpression = nullptr;
            _expression = other._expression ? other._expression->clone() : nullptr;
            _errorExpression = other._errorExpression ? other._errorExpression->clone() : nullptr;
            copyAstMembers(other);
            return *this;
        }

        python::Type getInferredType() {
            return python::Type::UNKNOWN; // assert has no type!
        }

        virtual ASTNode* clone() { return new NAssert(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Assert; };
    };

    class NRaise : public ASTNode {
    public:

        ASTNode *_expression;
        ASTNode *_fromExpression;
        NRaise(ASTNode *expression=nullptr, ASTNode *fromExpression=nullptr)  {
            _expression = nullptr;
            _fromExpression = nullptr;
            if(expression)_expression = expression->clone();
            if(fromExpression)_fromExpression = fromExpression->clone();
        }

        NRaise(const NRaise& other) {
            _expression = other._expression ? other._expression->clone() : nullptr;
            _fromExpression = other._fromExpression ? other._fromExpression->clone() : nullptr;
            copyAstMembers(other);
        }

        NRaise& operator = (const NRaise& other) {
            if(_expression)
                delete _expression;
            _expression = nullptr;
            if(_fromExpression)
                delete _fromExpression;
            _fromExpression = nullptr;
            _expression = other._expression ? other._expression->clone() : nullptr;
            _fromExpression = other._fromExpression ? other._fromExpression->clone() : nullptr;
            copyAstMembers(other);
            return *this;
        }

        python::Type getInferredType() {
            // no type for this statement!
            return python::Type::UNKNOWN;
        }

        virtual ASTNode* clone() { return new NRaise(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Raise; };
    };

// simple assign AST node
    class NAssign : public ASTNode {
    public:
        ASTNode* _target; // single target. need to extend to multiple target_lists!

        ASTNode* _value; // what value to assign to

        NAssign() : _target(nullptr), _value(nullptr)   {}

        NAssign(ASTNode* target, ASTNode* value) {
            _target = target->clone();
            _value = value->clone();
        }

        ~NAssign() {
            if(_target)
                delete _target;
            if(_value)
                delete _value;
        }

        NAssign(const NAssign& other) {
            _target = other._target->clone();
            _value = other._value->clone();
        }

        NAssign& operator = (const NAssign& other) {
            if(_target)
                delete _target;
            if(_value)
                delete _value;

            assert(other._target);
            assert(other._value);

            copyAstMembers(other);
            return *this;
        }

        virtual ASTNode* clone() { return new NAssign(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Assign; }
    };


    class NCall : public ASTNode {
    public:
        ASTNode *_func;
        std::vector<ASTNode*> _positionalArguments;

        NCall(ASTNode* func) { assert(func); _func = func->clone(); }

        ~NCall() {
            if(!_positionalArguments.empty())
                std::for_each(_positionalArguments.begin(),
                              _positionalArguments.end(),
                              [](ASTNode *node){delete node; node=nullptr;});
            delete _func;
        }

        // copy constructor
        NCall(const NCall& other) {
            // copy statements of other
            if(!other._positionalArguments.empty()) {
                _positionalArguments.clear();
                _positionalArguments.reserve(other._positionalArguments.size());
                std::for_each(other._positionalArguments.begin(), other._positionalArguments.end(), [this](ASTNode *node) {
                    _positionalArguments.push_back(node->clone());
                });
            }
            copyAstMembers(other);

            _func = other._func->clone();
        }

        /*!
         * creates tuple node from collection (may be empty). deletes collection
         */
        static NCall* fromCollection(ASTNode* func, NCollection* collection) {
            assert(collection);
            assert(func);
            NCall* call = new NCall(func);
            assert(call);

            for(auto el : collection->elements()) {
                call->_positionalArguments.push_back(el->clone());
            }

            delete collection;
            collection = nullptr;
            return call;
        }

        NCall& operator = (const NCall& other) {
            if(&other != this) {

                assert(_func);
                delete _func;
                _func = other._func->clone();

                // copy statements of other
                if(!other._positionalArguments.empty()) {
                    // delete statements
                    if(!_positionalArguments.empty())
                        std::for_each(_positionalArguments.begin(),
                                      _positionalArguments.end(),
                                      [](ASTNode *node){delete node; node=nullptr;});

                    _positionalArguments.clear();
                    _positionalArguments.reserve(other._positionalArguments.size());
                    std::for_each(other._positionalArguments.begin(), other._positionalArguments.end(), [this](ASTNode *node) {
                        _positionalArguments.push_back(node->clone());
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }


        /*!
         * returns tuple type of args. If one arg is unknown, then returns UNKNOWN as type
         * @return
         */
        python::Type getParametersInferredType() const {
            std::vector<python::Type> v;
            for(auto arg : _positionalArguments) {
                v.emplace_back(arg->getInferredType());
                if(arg->getInferredType() == python::Type::UNKNOWN)
                    return python::Type::UNKNOWN;
            }
            return python::Type::makeTupleType(v);
        }

        virtual ASTNode* clone() { return new NCall(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Call; }
    };

/*!
 * class representing an expression object.method
 * method is the attribute (str)
 * object an identifier
 */
    class NAttribute : public ASTNode {
    private:
        void release() {
            if(_value)delete _value;
            if(_attribute)delete _attribute;
            _value = nullptr;
            _attribute = nullptr;
        }

        python::Type _objectType; // the annotated constraint on the object to call attribute successfully. Introduced by TypeAnnotator Visitor
    public:
        // object.method operation basically.
        // fun fact: could be also represented as binary operator
        ASTNode *_value; // i.e. object (can be anything)
        NIdentifier *_attribute; // i.e. method

        NAttribute(ASTNode* value, NIdentifier* id) {
            _value = value->clone();
            _attribute = (NIdentifier*)id->clone();

            _objectType = _value->getInferredType();
        }

        NAttribute(const NAttribute& other) {
            _value = other._value->clone();
            _attribute = (NIdentifier*)other._attribute->clone();
            _objectType = other._objectType;
            copyAstMembers(other);
        }

        ~NAttribute() {
            release();
        }

        NAttribute& operator = (const NAttribute& other) {
            if(&other != this) {
                release();

                _value = other._value->clone();
                _attribute = (NIdentifier*)other._attribute->clone();
                _objectType = other._objectType;
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() { return new NAttribute(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Attribute; }

        void setObjectType(const python::Type& object_type) { _objectType = object_type; }
        python::Type objectType() const { return _objectType; }
    };

    class NSlice : public ASTNode {
    public:

        // a nullptr means error
        ASTNode *_value; // the value to be indexed
        std::vector<ASTNode*> _slices; // list of slices


        NSlice():_value(nullptr) {}

        NSlice(ASTNode* value, std::vector<ASTNode*> slices) {
            _value = value->clone();
            _slices.reserve(slices.size());
            std::for_each(slices.begin(), slices.end(), [this](ASTNode *node) {
                _slices.push_back(node->clone());
            });
        }

        NSlice(const NSlice& other) {
            if(!other._slices.empty()) {
                _slices.clear();
                _slices.reserve(other._slices.size());
                std::for_each(other._slices.begin(), other._slices.end(), [this](ASTNode *node) {
                    _slices.push_back(node->clone());
                });
            }
            _value = other._value ? other._value->clone() : nullptr;
            copyAstMembers(other);
        }

        ~NSlice() {
            if(!_slices.empty())
                std::for_each(_slices.begin(), _slices.end(), [](ASTNode *node){delete node; node=nullptr;});

            if(_value)
                delete _value;
            _value = nullptr;
        }

        NSlice& operator = (const NSlice& other) {
            if(&other != this) {
                if(!other._slices.empty()) {
                    // delete statements
                    if(!_slices.empty())
                        std::for_each(_slices.begin(), _slices.end(), [](ASTNode *node){delete node; node=nullptr;});

                    _slices.clear();
                    _slices.reserve(other._slices.size());
                    std::for_each(other._slices.begin(), other._slices.end(), [this](ASTNode *node) {
                        _slices.push_back(node->clone());
                    });
                }

                if(_value)
                    delete _value;
                _value = nullptr;
                _value = other._value ? other._value->clone() : nullptr;

                copyAstMembers(other);
            }
            return *this;
        }

        static NSlice* fromCollection(ASTNode *value, NCollection *slices) {
            assert(slices);
            NSlice* slice = new NSlice();
            assert(slice);

            for(auto el : slices->elements()) {
                slice->_slices.push_back(el->clone());
            }

            slice->_value = value->clone();

            delete slices;
            slices = nullptr;
            return slice;
        }

        ASTNode* clone() {return new NSlice(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Slice; };

    };


    class NSliceItem : public ASTNode {
    public:

        ASTNode *_start; // start expression
        ASTNode *_end; // end expression
        ASTNode *_stride; // stride expression


        NSliceItem(): _start(nullptr), _end(nullptr), _stride(nullptr) {}

        NSliceItem(ASTNode *start, ASTNode *end, ASTNode *stride) {
            _start = start ? start->clone() : nullptr;
            _end = end ? end->clone() : nullptr;
            _stride = stride ? stride->clone() : nullptr;
        }

        NSliceItem(const NSliceItem& other) {
            _start = other._start ? other._start->clone() : nullptr;
            _end = other._end ? other._end->clone() : nullptr;
            _stride = other._stride ? other._stride->clone() : nullptr;
            copyAstMembers(other);
        }

        ~NSliceItem() {
            if(_start)
                delete _start;
            _start = nullptr;

            if(_end)
                delete _end;
            _end = nullptr;

            if(_stride)
                delete _stride;
            _stride = nullptr;
        }

        NSliceItem& operator = (const NSliceItem& other) {
            if(&other != this) {
                if(_start)
                    delete _start;
                _start = nullptr;

                if(_end)
                    delete _end;
                _end = nullptr;

                if(_stride)
                    delete _stride;
                _stride = nullptr;

                _start = other._start ? other._start->clone() : nullptr;
                _end = other._end ? other._end->clone() : nullptr;
                _stride = other._stride ? other._stride->clone() : nullptr;
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() {return new NSliceItem(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::SliceItem; };

    };

// @TODO: refactor this into generator object.
// cf. here https://github.com/LeonhardFS/Tuplex/issues/211
    class NRange : public ASTNode {
    public:
        std::vector<ASTNode*> _positionalArguments;

        NRange()    {}

        ~NRange() {
            if(!_positionalArguments.empty())
                std::for_each(_positionalArguments.begin(),
                              _positionalArguments.end(),
                              [](ASTNode *node){delete node; node=nullptr;});
        }

        // copy constructor
        NRange(const NRange& other) {
            // copy statements of other
            if(!other._positionalArguments.empty()) {
                _positionalArguments.clear();
                _positionalArguments.reserve(other._positionalArguments.size());
                std::for_each(other._positionalArguments.begin(), other._positionalArguments.end(), [this](ASTNode *node) {
                    _positionalArguments.push_back(node->clone());
                });
            }
            copyAstMembers(other);
        }

        /*!
         * creates tuple node from collection (may be empty). deletes collection
         */
        static NRange* fromCollection(NIdentifier* func, NCollection* collection) {
            assert(collection);
            assert(func);
            auto range = new NRange();
            assert(range);

            for(auto el : collection->elements()) {
                range->_positionalArguments.push_back(el->clone());
            }

            delete collection;
            collection = nullptr;
            return range;
        }

        NRange& operator = (const NRange& other) {
            if(&other != this) {

                // copy statements of other
                if(!other._positionalArguments.empty()) {
                    // delete statements
                    if(!_positionalArguments.empty())
                        std::for_each(_positionalArguments.begin(),
                                      _positionalArguments.end(),
                                      [](ASTNode *node){delete node; node=nullptr;});

                    _positionalArguments.clear();
                    _positionalArguments.reserve(other._positionalArguments.size());
                    std::for_each(other._positionalArguments.begin(), other._positionalArguments.end(), [this](ASTNode *node) {
                        _positionalArguments.push_back(node->clone());
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }


        /*!
         * returns tuple type of args. If one arg is unknown, then returns UNKNOWN as type
         * @return
         */
        python::Type getParametersInferredType() const {
            std::vector<python::Type> v;
            for(auto arg : _positionalArguments) {
                v.emplace_back(arg->getInferredType());
                if(arg->getInferredType() == python::Type::UNKNOWN)
                    return python::Type::UNKNOWN;
            }
            return python::Type::makeTupleType(v);
        }

        virtual ASTNode* clone() { return new NRange(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Range; }
    };

    class NComprehension : public ASTNode {
    public:
        NIdentifier *target;
        ASTNode *iter;
        std::vector<ASTNode*> if_conditions;

        NComprehension(): target(nullptr), iter(nullptr) {}

        NComprehension(NIdentifier *_target, ASTNode *_iter) {
            target = _target ? dynamic_cast<NIdentifier *>(_target->clone()) : nullptr;
            iter = _iter ? _iter->clone() : nullptr;
        }

        // copy constructor
        NComprehension(const NComprehension& other) {
            target = other.target ? dynamic_cast<NIdentifier*>(other.target->clone()) : nullptr;
            iter = other.iter ? other.iter->clone() : nullptr;
            if(!other.if_conditions.empty()) {
                if_conditions.clear();
                if_conditions.reserve(other.if_conditions.size());
                std::for_each(other.if_conditions.begin(), other.if_conditions.end(), [this](ASTNode *node) {
                    if_conditions.push_back(node->clone());
                });
            }
            copyAstMembers(other);
        }

        ~NComprehension() {
            delete target; target = nullptr;
            delete iter; iter = nullptr;
            if(!if_conditions.empty())
                std::for_each(if_conditions.begin(), if_conditions.end(), [](ASTNode *node){delete node; node=nullptr;});
        }

        NComprehension& operator = (const NComprehension& other) {
            if(&other != this) {
                delete target; target = nullptr;
                target = other.target ? dynamic_cast<NIdentifier*>(other.target->clone()) : nullptr;

                delete iter; iter = nullptr;
                iter = other.iter ? other.iter->clone() : nullptr;

                if(!other.if_conditions.empty()) {
                    // delete statements
                    if(!if_conditions.empty())
                        std::for_each(if_conditions.begin(), if_conditions.end(), [](ASTNode *node){delete node; node=nullptr;});

                    if_conditions.clear();
                    if_conditions.reserve(other.if_conditions.size());
                    std::for_each(other.if_conditions.begin(), other.if_conditions.end(), [this](ASTNode *node) {
                        if_conditions.push_back(node->clone());
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() {return new NComprehension(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Comprehension; };
    };

    class NListComprehension : public ASTNode {
    public:
        ASTNode* expression;
        std::vector<NComprehension*> generators;

        NListComprehension(): expression(nullptr) {}

        NListComprehension(ASTNode *_expression) {
            expression = _expression ? _expression->clone() : nullptr;
        }

        // copy constructor
        NListComprehension(const NListComprehension& other) {
            expression = other.expression ? other.expression->clone() : nullptr;
            if(!other.generators.empty()) {
                generators.clear();
                generators.reserve(other.generators.size());
                std::for_each(other.generators.begin(), other.generators.end(), [this](ASTNode *node) {
                    generators.push_back(dynamic_cast<NComprehension*>(node->clone()));
                });
            }
            copyAstMembers(other);
        }

        ~NListComprehension() {
            delete expression; expression = nullptr;
            if(!generators.empty())
                std::for_each(generators.begin(), generators.end(), [](ASTNode *node){delete node; node=nullptr;});
        }

        NListComprehension& operator = (const NListComprehension& other) {
            if(&other != this) {
                delete expression; expression = nullptr;
                expression = other.expression ? other.expression->clone() : nullptr;

                if(!other.generators.empty()) {
                    // delete statements
                    if(!generators.empty())
                        std::for_each(generators.begin(), generators.end(), [](ASTNode *node){delete node; node=nullptr;});

                    generators.clear();
                    generators.reserve(other.generators.size());
                    std::for_each(other.generators.begin(), other.generators.end(), [this](ASTNode *node) {
                        generators.push_back(dynamic_cast<NComprehension*>(node->clone()));
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() {return new NListComprehension(*this);}
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::ListComprehension; };
    };

// https://docs.python.org/3/reference/compound_stmts.html#the-while-statement
// while_stmt ::=  "while" assignment_expression ":" suite
//                ["else" ":" suite]
    class NWhile : public ASTNode {
    public:
        ASTNode* expression;
        ASTNode* suite_body;
        ASTNode* suite_else;

        NWhile() : expression(nullptr), suite_body(nullptr), suite_else(nullptr) {}
        NWhile(const NWhile& other) {
            expression = other.expression ? other.expression->clone() : nullptr;
            suite_body = other.suite_body ? other.suite_body->clone() : nullptr;
            suite_else = other.suite_else ? other.suite_else->clone() : nullptr;

            copyAstMembers(other);
        }

        NWhile(ASTNode *_expression, ASTNode *_body, ASTNode *_else) {
            assert(_expression);
            assert(_body);
            expression = _expression->clone();
            suite_body = _body->clone();
            suite_else = nullptr;
            if(_else)
                suite_else = _else->clone();
        }

        ~NWhile() {
            if(expression)
                delete expression;
            if(suite_body)
                delete suite_body;
            if(suite_else)
                delete suite_else;
            expression = nullptr;
            suite_body = nullptr;
            suite_else = nullptr;
        }

        NWhile& operator = (const NWhile& other) {
            if(&other != this) {
                if(expression)
                    delete expression;
                if(suite_body)
                    delete suite_body;
                if(suite_else)
                    delete suite_else;

                expression = other.expression ? other.expression->clone() : nullptr;
                suite_body = other.suite_body ? other.suite_body->clone() : nullptr;
                suite_else = other.suite_else ? other.suite_else->clone() : nullptr;

                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() { return new NWhile(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::While; }
    };

    class NFor : public ASTNode {
    public:

        // for target in expression:
        //   suite_body
        // else:
        //   suite_else

        ASTNode* target;
        ASTNode* expression;
        ASTNode* suite_body;
        ASTNode* suite_else;

        NFor() : target(nullptr), expression(nullptr), suite_body(nullptr), suite_else(nullptr) {}
        NFor(const NFor& other) {
            target = other.target ? other.target->clone() : nullptr;
            expression = other.expression ? other.expression->clone() : nullptr;
            suite_body = other.suite_body ? other.suite_body->clone() : nullptr;
            suite_else = other.suite_else ? other.suite_else->clone() : nullptr;

            copyAstMembers(other);
        }

        NFor(ASTNode *_target, ASTNode *_expression, ASTNode *_body, ASTNode *_else) {
            assert(_target);
            assert(_expression);
            assert(_body);
            target = _target->clone();
            expression = _expression->clone();
            suite_body = _body->clone();
            suite_else = nullptr;
            if(_else)
                suite_else = _else->clone();
        }

        ~NFor() {
            if(target)
                delete target;
            if(expression)
                delete expression;
            if(suite_body)
                delete suite_body;
            if(suite_else)
                delete suite_else;
            target = nullptr;
            expression = nullptr;
            suite_body = nullptr;
            suite_else = nullptr;
        }

        NFor& operator = (const NFor& other) {
            if(&other != this) {
                if(target)
                    delete target;
                if(expression)
                    delete expression;
                if(suite_body)
                    delete suite_body;
                if(suite_else)
                    delete suite_else;

                target = other.target ? other.target->clone() : nullptr;
                expression = other.expression ? other.expression->clone() : nullptr;
                suite_body = other.suite_body ? other.suite_body->clone() : nullptr;
                suite_else = other.suite_else ? other.suite_else->clone() : nullptr;

                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() { return new NFor(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::For; }
    };

// https://docs.python.org/3/reference/simple_stmts.html#break
    class NBreak : public ASTNode {
    public:
        NBreak() = default;
        NBreak(const NBreak& other) = default;
        ASTNode* clone() { return new NBreak(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Break; }
    };

// https://docs.python.org/3/reference/simple_stmts.html#the-continue-statement
    class NContinue : public ASTNode {
    public:
        NContinue() = default;
        NContinue(const NContinue& other) = default;
        ASTNode* clone() { return new NContinue(*this); }
        virtual void accept(class IVisitor& visitor);
        virtual ASTNodeType type() const { return ASTNodeType::Continue; }
    };
}

#endif //TUPLEX_ASTNODES_H