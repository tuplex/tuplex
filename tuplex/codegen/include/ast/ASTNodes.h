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
#include "visitors/IVisitor.h"
#include "parser/TokenType.h"
#include "parser/Token.h"
#include "TypeSystem.h"
#include "Base.h"
#include <vector>
#include <TSet.h>
#include "ASTAnnotation.h"

#ifdef BUIld_WITH_CEREAL
// @TODO: cleaner? precompiled headers?
#include "cereal/access.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/common.hpp"
#include "cereal/archives/binary.hpp"
#endif

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
        std::unique_ptr<ASTAnnotation> _annotation; // optional annotation stored for this node


        virtual void copyAstMembers(const ASTNode& other) {
            _inferredType = other._inferredType;
            _scopeID = other._scopeID;
            if(other._annotation)
                _annotation = std::make_unique<ASTAnnotation>(*other._annotation);
        }
    public:
        ASTNode() : _inferredType(python::Type::UNKNOWN), _scopeID(-1), _annotation(nullptr)    {}
        ASTNode(const ASTNode& other) { copyAstMembers(other); }

        virtual ~ASTNode() {}

        // returns a (deep) copy of the AST Node
        // also need to transfer protected members
        virtual ASTNode* clone() const = 0;

        // later do codegen
        // virtual llvm::Value* codegen() = 0;

        // used for visitor pattern. => recursive traversal must be implemented by visitor!
        virtual void accept(class IVisitor& visitor) = 0;

        virtual ASTNodeType type() const = 0;

        void setInferredType(const python::Type& type) { _inferredType = type; }
        virtual python::Type getInferredType() { return _inferredType; }

        virtual ASTAnnotation& annotation() {
            if(!_annotation)
                _annotation = std::make_unique<ASTAnnotation>();
            return *_annotation;
        }

        inline void removeAnnotation() {
            _annotation = nullptr;
        }

        inline bool hasAnnotation() const {
            if(_annotation) return true;
            return false;
        }

#ifdef BUILD_WITH_CEREAL
        template <class Archive>
        void serialize(Archive &ar) { ar(_inferredType, _scopeID, _annotation); }
#endif
    };

    inline bool isLiteralASTNode(ASTNode* node) {
        switch(node->type()) {
            case ASTNodeType::Boolean:
            case ASTNodeType::Number:
            case ASTNodeType::String:
            case ASTNodeType::None:
                return true;
            default:
                return false;
        }
    }


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

        virtual ASTNode* clone() const {return new NNumber(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Number;
        virtual ASTNodeType type() const { return type_; };

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

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _value); }
#endif
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

        virtual ASTNode* clone() const {return new NIdentifier(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Identifier;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _name); }
#endif
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

        virtual ASTNode* clone() const {return new NBoolean(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Boolean;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _value); }
#endif
    };

// nothing associated to this class
// can use this as template code
    class NEllipsis : public ASTNode {
        bool __MAKE_CEREAL_WORK;
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

        virtual ASTNode* clone() const {return new NEllipsis(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Ellipsis;
        virtual ASTNodeType type() const { return type_; };
#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), __MAKE_CEREAL_WORK); }
#endif
    };

    class NNone : public ASTNode {
#ifdef BUILD_WITH_CEREAL
        bool __MAKE_CEREAL_WORK;
#endif
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

        virtual ASTNode* clone() const {return new NNone(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::None;
        virtual ASTNodeType type() const { return type_; };

        // always give NULLVALUE back!
        python::Type getInferredType() { return python::Type::NULLVALUE; }
#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), __MAKE_CEREAL_WORK); }
#endif
    };

    class NString : public ASTNode {
    private:
        std::string _raw_value;
    public:
        NString() {} // ONLY FOR USE BY CEREAL

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

        std::string raw_value() const { return _raw_value; }
        std::string value() const { return str_value_from_python_raw_value(_raw_value);}

        virtual ASTNode* clone() const {return new NString(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::String;
        virtual ASTNodeType type() const { return type_; };

        // one string statement might be comprised of a string expression of the form STRING+
        // we simply amend all the values => implement a smarter normalization aware strategy for this later
        NString* amend(const std::string& val) {
            _raw_value += val;
            return this;
        }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _raw_value); }
#endif

        // TODO: can't figure out how to incorporate base class into this
//        template<class Archive>
//        static void load_and_construct(Archive &ar, cereal::construct<NString> &construct) {
//            std::string
//        }
    };

/**********************************************/
// II. Expressions
/**********************************************/


    class NBinaryOp : public ASTNode {
        inline void release() {
            _left = nullptr;
            _right = nullptr;
        }
    public:

        TokenType _op;
        std::unique_ptr<ASTNode> _left;
        std::unique_ptr<ASTNode> _right;

        NBinaryOp():_left(nullptr), _right(nullptr),_op(TokenType::UNKNOWN)    {}

        NBinaryOp(ASTNode* left, const TokenType&& op, ASTNode* right) {
            _op = op;
            _left = std::unique_ptr<ASTNode>(left->clone());
            _right = std::unique_ptr<ASTNode>(right->clone());
        }

        NBinaryOp(const NBinaryOp& other) {
            _op = other._op;
            _left = std::unique_ptr<ASTNode>(other._left->clone());
            _right = std::unique_ptr<ASTNode>(other._right->clone());
            copyAstMembers(other);
        }

        ~NBinaryOp() {
            release();
        }

        NBinaryOp& operator = (const NBinaryOp& other) {
            if(&other != this) {
                release();

                _op = other._op;
                _left = std::unique_ptr<ASTNode>(other._left->clone());
                _right = std::unique_ptr<ASTNode>(other._right->clone());
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() const {return new NBinaryOp(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::BinaryOp;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _op, _left, _right); }
#endif
    };

    class NUnaryOp : public ASTNode {
    public:
        TokenType _op;
        std::unique_ptr<ASTNode> _operand;

        NUnaryOp():_operand(nullptr), _op(TokenType::UNKNOWN)    {}

        NUnaryOp(const TokenType&& op, ASTNode *operand) {
            _op = op;
            _operand = std::unique_ptr<ASTNode>(operand->clone());
        }

        NUnaryOp(const NUnaryOp& other) {
            _op = other._op;
            _operand = std::unique_ptr<ASTNode>(other._operand->clone());
            copyAstMembers(other);
        }

        ~NUnaryOp() {
            _operand = nullptr;
        }

        NUnaryOp& operator = (const NUnaryOp& other) {
            if(&other != this) {
                _operand = nullptr;

                _op = other._op;
                _operand = std::unique_ptr<ASTNode>(other._operand->clone());
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() const { return new NUnaryOp(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::UnaryOp;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _op, _operand); }
#endif
    };



    class NSubscription : public ASTNode {
    public:

        // a nullptr means error
        std::unique_ptr<ASTNode> _value; // the value to be indexed
        std::unique_ptr<ASTNode> _expression; // indexing expression


        NSubscription():_value(nullptr), _expression(nullptr)    {}

        NSubscription(ASTNode *value, ASTNode *expression) {
            _value = std::unique_ptr<ASTNode>(value->clone());
            _expression = std::unique_ptr<ASTNode>(expression->clone());
        }

        NSubscription(const NSubscription& other) {
            _value = std::unique_ptr<ASTNode>(other._value->clone());
            _expression = std::unique_ptr<ASTNode>(other._expression->clone());
            copyAstMembers(other);
        }

        ~NSubscription() {
            _expression = nullptr;
            _value = nullptr;
        }

        NSubscription& operator = (const NSubscription& other) {
            if(&other != this) {
                _expression = nullptr;
                _value = nullptr;

                _value = std::unique_ptr<ASTNode>(other._value->clone());
                _expression = std::unique_ptr<ASTNode>(other._expression->clone());
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() const {return new NSubscription(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Subscription;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _value, _expression); }
#endif
    };

/**********************************************/
// X. TopLevel Nodes
/**********************************************/

// a suite is a collection of statements
    class NSuite : public ASTNode {
    public:
        std::vector<std::unique_ptr<ASTNode> > _statements;
        bool _isUnrolledLoopSuite;

        NSuite() {
            _isUnrolledLoopSuite = false;
        }

        ~NSuite() {
            _statements.clear(); // automatically cleans up smart pointers
        }

        // copy constructor
        NSuite(const NSuite& other) {
            // copy statements of other
            if(!other._statements.empty()) {
                _statements.clear();
                _statements.reserve(other._statements.size());
                std::for_each(other._statements.begin(), other._statements.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _statements.push_back(std::unique_ptr<ASTNode>(node->clone()));
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
                    _statements.clear();
                    _statements.reserve(other._statements.size());
                    std::for_each(other._statements.begin(), other._statements.end(), [this](const std::unique_ptr<ASTNode> &node) {
                        _statements.push_back(std::unique_ptr<ASTNode>(node->clone()));
                    });
                }
                _isUnrolledLoopSuite = other._isUnrolledLoopSuite;

                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NSuite(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Suite;
        virtual ASTNodeType type() const { return type_; };

        void addStatement(ASTNode *node) {
            assert(node);
            _statements.push_back(std::unique_ptr<ASTNode>(node->clone()));
        }

        void addSuite(const NSuite* other) {
            assert(other);

            if(!other->_statements.empty())
                std::for_each(other->_statements.begin(), other->_statements.end(),
                              [this](const std::unique_ptr<ASTNode> &node) {
                                  _statements.push_back(std::unique_ptr<ASTNode>(node->clone()));
                              });

        }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _statements, _isUnrolledLoopSuite); }
#endif
    };

    class NModule : public ASTNode {
    public:
        std::unique_ptr<ASTNode> _suite;

        NModule():_suite(nullptr) {}

        ~NModule() { _suite = nullptr;}

        NModule(NSuite *suite) {
            _suite = std::unique_ptr<ASTNode>(suite->clone());
        }

        NModule(ASTNode *node) {
            // TODO: should this do a cast check to make sure it's a suite?
            _suite = std::unique_ptr<ASTNode>(node->clone());
        }

        // copy constructor
        NModule(const NModule& other) {
            // copy statements of other
            _suite = std::unique_ptr<ASTNode>(other._suite->clone());
            copyAstMembers(other);
        }

        NModule& operator = (const NModule& other) {
            if(&other != this) {
                _suite = nullptr;
                _suite = std::unique_ptr<ASTNode>(other._suite->clone());
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NModule(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Module;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _suite); }
#endif
    };


    class NParameter : public ASTNode {
        void release() {
            _default = nullptr;
            _identifier = nullptr;
            _annotation = nullptr;
        }
    public:

        std::unique_ptr<NIdentifier> _identifier;

        // Python3 has an annotation feature. I.e. when defining a function, optionally an expression can be annotated
        // i.e. def fun(x : int):
        //          pass
        // later this feature should be removed perhaps from the ast tree...
        // in python annotations can be accesed via fun.__annotations__ => could be used for simplified type inference!
        std::unique_ptr<ASTNode> _annotation;

        // optional default values
        std::unique_ptr<ASTNode> _default;

        NParameter() : _identifier(nullptr), _annotation(nullptr), _default(nullptr)    {}

        NParameter(NIdentifier *identifier) {
            _identifier = std::unique_ptr<NIdentifier>(static_cast<NIdentifier*>(identifier->clone()));
            _annotation = nullptr;
            _default = nullptr;
        }


        // copy constructor
        NParameter(const NParameter& other) {
            _annotation = nullptr;
            _default = nullptr;

            // copy statements of other
            _identifier = std::unique_ptr<NIdentifier>(static_cast<NIdentifier*>(other._identifier->clone()));
            if(other._annotation)
                _annotation = std::unique_ptr<ASTNode>(other._annotation->clone());
            if(other._default)
                _default = std::unique_ptr<ASTNode>(other._default->clone());
            copyAstMembers(other);
        }

        NParameter& operator = (const NParameter& other) {
            if(&other != this) {
                release();
                _identifier = std::unique_ptr<NIdentifier>(static_cast<NIdentifier*>(other._identifier->clone()));
                if(other._annotation)
                    _annotation = std::unique_ptr<ASTNode>(other._annotation->clone());
                if(other._default)
                    _default = std::unique_ptr<ASTNode>(other._default->clone());
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NParameter(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Parameter;
        virtual ASTNodeType type() const { return type_; };


        NParameter* setAnnotation(ASTNode *node) {
            _annotation = nullptr;
            _annotation = std::unique_ptr<ASTNode>(node->clone());
            return this;
        }

        NParameter*  setDefault(ASTNode *node) {
            if(!node)
                return this;

            _default = nullptr;
            _default = std::unique_ptr<ASTNode>(node->clone());
            return this;
        }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _identifier, _annotation, _default); }
#endif
    };

// helper class for constructing higher level AST Nodes
// this one here is for collecting ASTs in a list
    class NCollection : public ASTNode {
    protected:
        std::vector<std::unique_ptr<ASTNode>> _elements;

    public:

        NCollection() {}

        ~NCollection() {
            _elements.clear();
        }

        // copy constructor
        NCollection(const NCollection& other) {
            // copy statements of other
            if(!other._elements.empty()) {
                _elements.clear();
                _elements.reserve(other._elements.size());
                std::for_each(other._elements.begin(), other._elements.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _elements.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
            }
            copyAstMembers(other);
        }

        NCollection& operator = (const NCollection& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._elements.empty()) {
                    _elements.clear();
                    _elements.reserve(other._elements.size());
                    std::for_each(other._elements.begin(), other._elements.end(), [this](const std::unique_ptr<ASTNode> &node) {
                        _elements.push_back(std::unique_ptr<ASTNode>(node->clone()));
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NCollection(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::UNKNOWN;
        virtual ASTNodeType type() const { return type_; };

        void add(ASTNode *node) {
            _elements.push_back(std::unique_ptr<ASTNode>(node->clone()));
        }

        void addMultiple(const NCollection* other) {
            assert(other);

            if(!other->_elements.empty())
                std::for_each(other->_elements.begin(), other->_elements.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _elements.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
        }

        std::vector<ASTNode*> elements() const {
            std::vector<ASTNode*> ret;
            ret.resize(_elements.size());
            std::transform(_elements.begin(), _elements.end(), ret.begin(),
                           [](const std::unique_ptr<ASTNode> &e) { return e.get(); });
            return ret;
        }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _elements); }
#endif
    };

    class NParameterList : public ASTNode {
    public:

        // the nodes are usually NParameters, but they can be also NUnaryOp(STAR) or NUnaryOp(DOUBLESTAR)
        std::vector<std::unique_ptr<ASTNode>> _args;

        NParameterList() {}

        ~NParameterList() {
            _args.clear();
        }

        // copy constructor
        NParameterList(const NParameterList& other) {
            // copy statements of other
            if(!other._args.empty()) {
                _args.clear();
                _args.reserve(other._args.size());
                std::for_each(other._args.begin(), other._args.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _args.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
            }
            copyAstMembers(other);
        }

        NParameterList& operator = (const NParameterList& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._args.empty()) {
                    _args.clear();
                    _args.reserve(other._args.size());
                    std::for_each(other._args.begin(), other._args.end(), [this](const std::unique_ptr<ASTNode> &node) {
                        _args.push_back(std::unique_ptr<ASTNode>(node->clone()));
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }


        NParameterList* addParam(ASTNode *node) {
            assert(node);

            _args.push_back(std::unique_ptr<ASTNode>(node->clone()));
            return this;
        }

        NParameterList* addParameterList(NParameterList* other) {
            assert(other);

            if(!other->_args.empty())
                std::for_each(other->_args.begin(), other->_args.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _args.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
            return this;
        }

        virtual ASTNode* clone() const {return new NParameterList(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::ParameterList;
        virtual ASTNodeType type() const { return type_; };

        virtual python::Type getInferredType() const {
            std::vector<python::Type> types;
            std::for_each(_args.begin(), _args.end(), [this, &types](const std::unique_ptr<ASTNode> &arg) {
                types.push_back(arg->getInferredType());
            });

            // return tuple from arguments
            return python::Type::makeTupleType(types);
        }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _args); }
#endif
    };

    class NLambda : public ASTNode {
    private:
        //! annotation for later code generation,
        //! states whether the first param should be treated as tuple or not
        bool _treatFirstArgAsTuple;

        void release() {
            _arguments = nullptr;
            _expression = nullptr;
            _treatFirstArgAsTuple = false;
        }
    public:
        std::unique_ptr<NParameterList> _arguments;
        std::unique_ptr<ASTNode> _expression;

        NLambda() : _arguments(nullptr), _expression(nullptr), _treatFirstArgAsTuple(false)   {}

        NLambda(NParameterList *params, ASTNode *expression) {
            assert(expression);

            _arguments = nullptr;
            _expression = nullptr;

            if(params)
                _arguments = std::unique_ptr<NParameterList>(static_cast<NParameterList*>(params->clone()));
            if(expression)
                _expression = std::unique_ptr<ASTNode>(expression->clone());
            _treatFirstArgAsTuple = false;
        }

        ~NLambda() { release(); }

        // copy constructor
        NLambda(const NLambda& other) {
            // copy statements of other
            if(other._arguments)
                _arguments = std::unique_ptr<NParameterList>(static_cast<NParameterList*>(other._arguments->clone()));
            else
                _arguments = nullptr;
            _expression = std::unique_ptr<ASTNode>(other._expression->clone());
            _treatFirstArgAsTuple = other._treatFirstArgAsTuple;
            copyAstMembers(other);
        }

        NLambda& operator = (const NLambda& other) {
            if(&other != this) {
                release();
                _arguments = std::unique_ptr<NParameterList>(static_cast<NParameterList*>(other._arguments->clone()));
                _expression = std::unique_ptr<ASTNode>(other._expression->clone());
                _treatFirstArgAsTuple = other._treatFirstArgAsTuple;
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NLambda(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Lambda;
        virtual ASTNodeType type() const { return type_; };

        void setFirstArgTreatment(bool treatAsTuple) { _treatFirstArgAsTuple = treatAsTuple; }
        bool isFirstArgTuple() { return _treatFirstArgAsTuple; }

        inline std::vector<std::string> parameterNames() const {
            std::vector<std::string> names;
            if(_arguments) {
                std::for_each(_arguments->_args.begin(), _arguments->_args.end(), [this, &names](const std::unique_ptr<ASTNode> &arg) {
                    assert(arg->type() == ASTNodeType::Parameter);
                    names.emplace_back(((NParameter*)arg.get())->_identifier->_name);
                });
            }
            return names;
        }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _arguments, _expression, _treatFirstArgAsTuple); }
#endif
    };


    class NAwait : public ASTNode {
    public:
        std::unique_ptr<ASTNode> _target;

        NAwait() : _target(nullptr)     {}

        NAwait(ASTNode *target) {
            _target = std::unique_ptr<ASTNode>(target->clone());
        }

        ~NAwait() {
            _target = nullptr;
        }

        NAwait(const NAwait& other) {
            _target = nullptr;

            if(other._target) {
                _target = std::unique_ptr<ASTNode>(other._target->clone());
            }
            copyAstMembers(other);
        }

        NAwait& operator = (const NAwait& other) {
            if(&other != this) {
                _target = nullptr;

                if(other._target) {
                    _target = std::unique_ptr<ASTNode>(other._target->clone());
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NAwait(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Await;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _target); }
#endif
    };

    class NStarExpression : public ASTNode {
    public:
        std::unique_ptr<ASTNode> _target;

        NStarExpression() : _target(nullptr)     {}

        NStarExpression(ASTNode *target) {
            _target = std::unique_ptr<ASTNode>(target->clone());
        }

        ~NStarExpression() {
            _target = nullptr;
        }

        NStarExpression(const NStarExpression& other) {
            _target = nullptr;

            if(other._target) {
                _target = std::unique_ptr<ASTNode>(other._target->clone());
            }
            copyAstMembers(other);
        }

        NStarExpression& operator = (const NStarExpression& other) {
            if(&other != this) {
                _target = nullptr;

                if(other._target) {
                    _target = std::unique_ptr<ASTNode>(other._target->clone());
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NStarExpression(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::StarExpression;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _target); }
#endif
    };

    class NCompare : public ASTNode {

        void cloneFrom(const NCompare& other) {
            _left = nullptr;

            if(other._left)
                _left = std::unique_ptr<ASTNode>(other._left->clone());

            // copy the arrays
            _ops.clear();
            _comps.clear();

            _ops.reserve(other._ops.size());
            _comps.reserve(other._comps.size());

            std::for_each(other._ops.begin(), other._ops.end(), [this](TokenType tt) { _ops.push_back(tt); });
            std::for_each(other._comps.begin(), other._comps.end(), [this](const std::unique_ptr<ASTNode> &node) {
                _comps.push_back(std::unique_ptr<ASTNode>(node->clone()));
            });
            copyAstMembers(other);
        }

        void release() {
            _left = nullptr;
            _comps.clear();
            _ops.clear();
        }
    public:

        std::unique_ptr<ASTNode> _left;
        std::vector<TokenType> _ops; // operands (TokenType::IS, TokenType::NOTEQUAL, etc.)
        std::vector<std::unique_ptr<ASTNode>> _comps; // comparators (54, "hello", False, etc.)

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
            _comps.push_back(std::unique_ptr<ASTNode>(node->clone()));
            return *this;
        }

        NCompare& setLeft(ASTNode *node) {
            _left = nullptr;
            _left = std::unique_ptr<ASTNode>(node->clone());
            return *this;
        }

        virtual ASTNode* clone() const {return new NCompare(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Compare;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _left, _ops, _comps); }
#endif
    };


    class NIfElse : public ASTNode {
    private:
        void release() {
            _expression = nullptr;
            _then = nullptr;
            _else = nullptr;
        }

        void cloneFrom(const NIfElse& other) {
            if(other._expression)
                _expression = std::unique_ptr<ASTNode>(other._expression->clone());
            if(other._then)
                _then = std::unique_ptr<ASTNode>(other._then->clone());
            if(other._else)
                _else = std::unique_ptr<ASTNode>(other._else->clone());
            _isExpression = other._isExpression;

            copyAstMembers(other);
        }
        bool _isExpression;
    public:
        // abstract syntax tree

        std::unique_ptr<ASTNode> _expression;
        std::unique_ptr<ASTNode> _then;
        std::unique_ptr<ASTNode> _else;

        NIfElse():_expression(nullptr), _then(nullptr), _else(nullptr), _isExpression(false)    {}
        ~NIfElse() { release(); }

        NIfElse(ASTNode* expression, ASTNode* thenChild, ASTNode* elseChild, bool isExpression) : _isExpression(isExpression) {
            release();

            if(expression)
                _expression = std::unique_ptr<ASTNode>(expression->clone());
            if(thenChild)
                _then = std::unique_ptr<ASTNode>(thenChild->clone());
            if(elseChild)
                _else = std::unique_ptr<ASTNode>(elseChild->clone());
        }

        NIfElse(const NIfElse& other) {
            release();
            cloneFrom(other);
        }

        NIfElse& operator = (const NIfElse& other) {
            if(&other != this) {
                release();
                cloneFrom(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NIfElse(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::IfElse;
        virtual ASTNodeType type() const { return type_; };

        void setElse(ASTNode* elseChild) {
            _else = nullptr;
            if(elseChild)
                _else = std::unique_ptr<ASTNode>(elseChild->clone());
        }

        /*!
         * is it a if-else stmt with two blocks or an inline expression?
         * @return true if inline expression
         */
        bool isExpression() {
            return _isExpression;
        }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _expression, _then, _else, _isExpression); }
#endif
    };

    class NFunction : public ASTNode {

        bool _treatFirstArgAsTuple;
        void release() {
            _name=nullptr;
            _parameters=nullptr;
            _annotation=nullptr;
            _suite=nullptr;

            _treatFirstArgAsTuple = false;
        }

        void cloneFrom(const NFunction& other) {
            _parameters = nullptr;
            _annotation = nullptr;

            // function must have name & suite
            assert(other._name);
            _name = std::unique_ptr<NIdentifier>(static_cast<NIdentifier*>(other._name->clone()));

            assert(other._suite);
            _suite = std::unique_ptr<ASTNode>(static_cast<NSuite*>(other._suite->clone()));

            if(other._parameters)
                _parameters = std::unique_ptr<NParameterList>(static_cast<NParameterList*>(other._parameters->clone()));
            if(other._annotation)
                _annotation = std::unique_ptr<ASTNode>(other._annotation->clone());

            _treatFirstArgAsTuple = other._treatFirstArgAsTuple;

            copyAstMembers(other);
        }

    public:
        std::unique_ptr<NIdentifier> _name;
        std::unique_ptr<NParameterList> _parameters;
        std::unique_ptr<ASTNode> _annotation;
        std::unique_ptr<ASTNode> _suite; // can be also a single statement OR expression!

        NFunction() : _name(nullptr), _parameters(nullptr), _annotation(nullptr), _suite(nullptr), _treatFirstArgAsTuple(false)   {}

        NFunction(NIdentifier *name, ASTNode *suite) {
            assert(name->type() == ASTNodeType::Identifier);
            _name = std::unique_ptr<NIdentifier>(dynamic_cast<NIdentifier*>(name->clone()));
            _suite = std::unique_ptr<ASTNode>(suite->clone());
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

        virtual ASTNode* clone() const {return new NFunction(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Function;
        virtual ASTNodeType type() const { return type_; };

        NFunction* setParams(NParameterList *params) {
            _parameters=nullptr;
            assert(params->type() == ASTNodeType::ParameterList);
            _parameters = std::unique_ptr<NParameterList>(dynamic_cast<NParameterList*>(params->clone()));

            return this;
        }

        NFunction* setAnnotation(ASTNode *annotation) {
            _annotation = nullptr;
            _annotation = std::unique_ptr<ASTNode>(annotation->clone());

            return this;
        }

        void setFirstArgTreatment(bool treatAsTuple) { _treatFirstArgAsTuple = treatAsTuple; }
        bool isFirstArgTuple() const { return _treatFirstArgAsTuple; }

        inline std::vector<std::string> parameterNames() const {
            std::vector<std::string> names;
            if(_parameters) {
                std::for_each(_parameters->_args.begin(), _parameters->_args.end(), [this, &names](const std::unique_ptr<ASTNode> &arg) {
                    assert(arg->type() == ASTNodeType::Parameter);
                    names.emplace_back(((NParameter*)arg.get())->_identifier->_name);
                });
            }
            return names;
        }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _name, _parameters, _annotation, _suite, _treatFirstArgAsTuple); }
#endif
    };

    // holds a tuple
    class NTuple : public ASTNode {
    public:
        std::vector<std::unique_ptr<ASTNode>> _elements;

        NTuple() {}

        ~NTuple() {
            _elements.clear();
        }

        // copy constructor
        NTuple(const NTuple& other) {
            // copy statements of other
            if(!other._elements.empty()) {
                _elements.clear();
                _elements.reserve(other._elements.size());
                std::for_each(other._elements.begin(), other._elements.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _elements.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
            }
            copyAstMembers(other);
        }

        /*!
         * creates tuple node from collection (may be empty). deletes collection
         */
//        static NTuple* fromCollection(NCollection* collection) {
//            assert(collection);
//            NTuple* tuple = new NTuple();
//            assert(tuple);
//
//            for(auto el : collection->elements()) {
//                tuple->_elements.push_back(std::unique_ptr<ASTNode>(el->clone()));
//            }
//
//            delete collection;
//            collection = nullptr;
//            return tuple;
//        }

        NTuple& operator = (const NTuple& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._elements.empty()) {
                    _elements.clear();
                    _elements.reserve(other._elements.size());
                    std::for_each(other._elements.begin(), other._elements.end(), [this](const std::unique_ptr<ASTNode> &node) {
                        _elements.push_back(std::unique_ptr<ASTNode>(node->clone()));
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

        virtual ASTNode* clone() const {return new NTuple(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Tuple;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _elements); }
#endif
    };

// represents a dictionary expression ({ ... })
    class NDictionary : public ASTNode {
    public:
        std::vector<std::pair<std::unique_ptr<ASTNode>, std::unique_ptr<ASTNode> > > _pairs;

        NDictionary() {}

        ~NDictionary() {
            _pairs.clear();
        }

        // copy constructor
        NDictionary(const NDictionary& other) {
            // copy statements of other
            if(!other._pairs.empty()) {
                _pairs.clear();
                _pairs.reserve(other._pairs.size());
                std::for_each(other._pairs.begin(), other._pairs.end(), [this](const std::pair<std::unique_ptr<ASTNode>, std::unique_ptr<ASTNode> > &nodes) {
                    _pairs.push_back(std::make_pair(std::unique_ptr<ASTNode>(nodes.first->clone()), std::unique_ptr<ASTNode>(nodes.second->clone())));
                });
            }
            copyAstMembers(other);
        }

        NDictionary& operator = (const NDictionary& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._pairs.empty()) {
                    _pairs.clear();
                    _pairs.reserve(other._pairs.size());
                    std::for_each(other._pairs.begin(), other._pairs.end(), [this](const std::pair<std::unique_ptr<ASTNode>, std::unique_ptr<ASTNode> > &nodes) {
                        _pairs.push_back(std::make_pair(std::unique_ptr<ASTNode>(nodes.first->clone()), std::unique_ptr<ASTNode>(nodes.second->clone())));
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NDictionary(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Dictionary;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _pairs); }
#endif
    };

// represents a list expression ([ ... ])
    class NList : public ASTNode {
    public:
        std::vector<std::unique_ptr<ASTNode> > _elements;

        NList() {}

        ~NList() {
            _elements.clear();
        }

        // copy constructor
        NList(const NList& other) {
            // copy statements of other
            if(!other._elements.empty()) {
                _elements.clear();
                _elements.reserve(other._elements.size());
                std::for_each(other._elements.begin(), other._elements.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _elements.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
            }
            copyAstMembers(other);
        }

        NList& operator = (const NList& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._elements.empty()) {
                    _elements.clear();
                    _elements.reserve(other._elements.size());
                    std::for_each(other._elements.begin(), other._elements.end(), [this](const std::unique_ptr<ASTNode> &node) {
                        _elements.push_back(std::unique_ptr<ASTNode>(node->clone()));
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const {return new NList(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::List;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _elements); }
#endif
    };

    class NReturn : public ASTNode {
    public:

        std::unique_ptr<ASTNode> _expression;
        NReturn(ASTNode *expression=nullptr)  { if(expression) _expression = std::unique_ptr<ASTNode>(expression->clone()); }
        NReturn(const NReturn& other) {
            _expression = other._expression ? std::unique_ptr<ASTNode>(other._expression->clone()) : nullptr;
            copyAstMembers(other);
        }

        NReturn& operator = (const NReturn& other) {
            _expression = nullptr;
            _expression = other._expression ? std::unique_ptr<ASTNode>(other._expression->clone()) : nullptr;
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

        virtual ASTNode* clone() const { return new NReturn(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Return;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _expression); }
#endif
    };

// cf. https://docs.python.org/3/reference/simple_stmts.html#grammar-token-assert-stmt
    class NAssert : public ASTNode {
    public:

        std::unique_ptr<ASTNode> _expression;
        std::unique_ptr<ASTNode> _errorExpression;
        NAssert(ASTNode *expression=nullptr, ASTNode* errorExpression=nullptr)  {
            _expression = nullptr;
            _errorExpression = nullptr;
            if(expression)_expression = std::unique_ptr<ASTNode>(expression->clone());
            if(errorExpression)_errorExpression = std::unique_ptr<ASTNode>(errorExpression->clone());
        }

        NAssert(const NAssert& other) {
            _expression = other._expression ? std::unique_ptr<ASTNode>(other._expression->clone()) : nullptr;
            _errorExpression = other._errorExpression ? std::unique_ptr<ASTNode>(other._errorExpression->clone()) : nullptr;
            copyAstMembers(other);
        }

        NAssert& operator = (const NAssert& other) {
            _expression = nullptr;
            _errorExpression = nullptr;
            _expression = other._expression ? std::unique_ptr<ASTNode>(other._expression->clone()) : nullptr;
            _errorExpression = other._errorExpression ? std::unique_ptr<ASTNode>(other._errorExpression->clone()) : nullptr;
            copyAstMembers(other);
            return *this;
        }

        python::Type getInferredType() {
            return python::Type::UNKNOWN; // assert has no type!
        }

        virtual ASTNode* clone() const { return new NAssert(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Assert;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _expression, _errorExpression); }
#endif
    };

    class NRaise : public ASTNode {
    public:

        std::unique_ptr<ASTNode> _expression;
        std::unique_ptr<ASTNode> _fromExpression;
        NRaise(ASTNode *expression=nullptr, ASTNode *fromExpression=nullptr)  {
            _expression = nullptr;
            _fromExpression = nullptr;
            if(expression)_expression = std::unique_ptr<ASTNode>(expression->clone());
            if(fromExpression)_fromExpression = std::unique_ptr<ASTNode>(fromExpression->clone());
        }

        NRaise(const NRaise& other) {
            _expression = other._expression ? std::unique_ptr<ASTNode>(other._expression->clone()) : nullptr;
            _fromExpression = other._fromExpression ? std::unique_ptr<ASTNode>(other._fromExpression->clone()) : nullptr;
            copyAstMembers(other);
        }

        NRaise& operator = (const NRaise& other) {
            _expression = nullptr;
            _fromExpression = nullptr;
            _expression = other._expression ? std::unique_ptr<ASTNode>(other._expression->clone()) : nullptr;
            _fromExpression = other._fromExpression ? std::unique_ptr<ASTNode>(other._fromExpression->clone()) : nullptr;
            copyAstMembers(other);
            return *this;
        }

        python::Type getInferredType() {
            // no type for this statement!
            return python::Type::UNKNOWN;
        }

        virtual ASTNode* clone() const { return new NRaise(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Raise;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _expression, _fromExpression); }
#endif
    };

// simple assign AST node
    class NAssign : public ASTNode {
    public:
        std::unique_ptr<ASTNode> _target; // single target. need to extend to multiple target_lists!
        std::unique_ptr<ASTNode> _value; // what value to assign to

        NAssign() : _target(nullptr), _value(nullptr)   {}

        NAssign(ASTNode* target, ASTNode* value) {
            _target = std::unique_ptr<ASTNode>(target->clone());
            _value = std::unique_ptr<ASTNode>(value->clone());
        }

        ~NAssign() {
            _target = nullptr;
            _value = nullptr;
        }

        NAssign(const NAssign& other) {
            _target = std::unique_ptr<ASTNode>(other._target->clone());
            _value = std::unique_ptr<ASTNode>(other._value->clone());
        }

        NAssign& operator = (const NAssign& other) {
            _target = nullptr;
            _value = nullptr;

            _target = std::unique_ptr<ASTNode>(other._target->clone());
            _value = std::unique_ptr<ASTNode>(other._value->clone());

            assert(other._target);
            assert(other._value);

            copyAstMembers(other);
            return *this;
        }

        virtual ASTNode* clone() const { return new NAssign(*this); }
        virtual void accept(class IVisitor& visitor);

        static const ASTNodeType type_ = ASTNodeType::Assign;
        virtual ASTNodeType type() const { return type_; }
#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _target, _value); }
#endif
    };


    class NCall : public ASTNode {
    public:
        std::unique_ptr<ASTNode> _func;
        std::vector<std::unique_ptr<ASTNode>> _positionalArguments;

        NCall() {} // ONLY FOR USE BY CEREAL!
        NCall(ASTNode* func) { assert(func); _func = std::unique_ptr<ASTNode>(func->clone()); }

        ~NCall() {
            _positionalArguments.clear();
            _func = nullptr;
        }

        // copy constructor
        NCall(const NCall& other) {
            // copy statements of other
            if(!other._positionalArguments.empty()) {
                _positionalArguments.clear();
                _positionalArguments.reserve(other._positionalArguments.size());
                std::for_each(other._positionalArguments.begin(), other._positionalArguments.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _positionalArguments.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
            }
            copyAstMembers(other);

            _func = std::unique_ptr<ASTNode>(other._func->clone());
        }

        /*!
         * creates tuple node from collection (may be empty). deletes collection
         */
//        static NCall* fromCollection(ASTNode* func, NCollection* collection) {
//            assert(collection);
//            assert(func);
//            NCall* call = new NCall(func);
//            assert(call);
//
//            for(auto el : collection->elements()) {
//                call->_positionalArguments.push_back(el->clone());
//            }
//
//            delete collection;
//            collection = nullptr;
//            return call;
//        }

        NCall& operator = (const NCall& other) {
            if(&other != this) {

                assert(_func);
                _func = nullptr;
                _func = std::unique_ptr<ASTNode>(other._func->clone());

                // copy statements of other
                if(!other._positionalArguments.empty()) {
                    _positionalArguments.clear();
                    _positionalArguments.reserve(other._positionalArguments.size());
                    std::for_each(other._positionalArguments.begin(), other._positionalArguments.end(), [this](const std::unique_ptr<ASTNode> &node) {
                        _positionalArguments.push_back(std::unique_ptr<ASTNode>(node->clone()));
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
            for(const auto &arg : _positionalArguments) {
                v.emplace_back(arg->getInferredType());
                if(arg->getInferredType() == python::Type::UNKNOWN)
                    return python::Type::UNKNOWN;
            }
            return python::Type::makeTupleType(v);
        }

        virtual ASTNode* clone() const { return new NCall(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Call;
        virtual ASTNodeType type() const { return type_; }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _func, _positionalArguments); }
#endif
    };

/*!
 * class representing an expression object.method
 * method is the attribute (str)
 * object an identifier
 */
    class NAttribute : public ASTNode {
    private:
        void release() {
            _value = nullptr;
            _attribute = nullptr;
        }

        python::Type _objectType; // the annotated constraint on the object to call attribute successfully. Introduced by TypeAnnotator Visitor
    public:
        // object.method operation basically.
        // fun fact: could be also represented as binary operator
        std::unique_ptr<ASTNode> _value; // i.e. object (can be anything)
        std::unique_ptr<NIdentifier> _attribute; // i.e. method

        NAttribute() {} // ONLY FOR USE BY CEREAL

        NAttribute(ASTNode* value, NIdentifier* id) {
            _value = std::unique_ptr<ASTNode>(value->clone());
            _attribute = std::unique_ptr<NIdentifier>((NIdentifier*)id->clone());

            _objectType = _value->getInferredType();
        }

        NAttribute(const NAttribute& other) {
            _value = std::unique_ptr<ASTNode>(other._value->clone());
            _attribute = std::unique_ptr<NIdentifier>((NIdentifier*)other._attribute->clone());
            _objectType = other._objectType;
            copyAstMembers(other);
        }

        ~NAttribute() {
            release();
        }

        NAttribute& operator = (const NAttribute& other) {
            if(&other != this) {
                release();

                _value = std::unique_ptr<ASTNode>(other._value->clone());
                _attribute = std::unique_ptr<NIdentifier>((NIdentifier*)other._attribute->clone());
                _objectType = other._objectType;
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const { return new NAttribute(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Attribute;
        virtual ASTNodeType type() const { return type_; }

        void setObjectType(const python::Type& object_type) { _objectType = object_type; }
        python::Type objectType() const { return _objectType; }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _objectType, _value, _attribute); }
#endif
    };

    class NSlice : public ASTNode {
    public:

        // a nullptr means error
        std::unique_ptr<ASTNode> _value; // the value to be indexed
        std::vector<std::unique_ptr<ASTNode>> _slices; // list of slices


        NSlice():_value(nullptr) {}

        NSlice(ASTNode* value, std::vector<ASTNode*> slices) {
            _value = std::unique_ptr<ASTNode>(value->clone());
            _slices.reserve(slices.size());
            std::for_each(slices.begin(), slices.end(), [this](const ASTNode *node) {
                _slices.push_back(std::unique_ptr<ASTNode>(node->clone()));
            });
        }

        NSlice(const NSlice& other) {
            if(!other._slices.empty()) {
                _slices.clear();
                _slices.reserve(other._slices.size());
                std::for_each(other._slices.begin(), other._slices.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _slices.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
            }
            _value = other._value ? std::unique_ptr<ASTNode>(other._value->clone()) : nullptr;
            copyAstMembers(other);
        }

        ~NSlice() {
            _slices.clear();
            _value = nullptr;
        }

        NSlice& operator = (const NSlice& other) {
            if(&other != this) {
                if(!other._slices.empty()) {
                    _slices.clear();
                    _slices.reserve(other._slices.size());
                    std::for_each(other._slices.begin(), other._slices.end(), [this](const std::unique_ptr<ASTNode> &node) {
                        _slices.push_back(std::unique_ptr<ASTNode>(node->clone()));
                    });
                }

                _value = nullptr;
                _value = other._value ? std::unique_ptr<ASTNode>(other._value->clone()) : nullptr;

                copyAstMembers(other);
            }
            return *this;
        }
//
//        static NSlice* fromCollection(ASTNode *value, NCollection *slices) {
//            assert(slices);
//            NSlice* slice = new NSlice();
//            assert(slice);
//
//            for(auto el : slices->elements()) {
//                slice->_slices.push_back(el->clone());
//            }
//
//            slice->_value = value->clone();
//
//            delete slices;
//            slices = nullptr;
//            return slice;
//        }

        ASTNode* clone() const {return new NSlice(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Slice;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _value, _slices); }
#endif
    };


    class NSliceItem : public ASTNode {
    public:

        std::unique_ptr<ASTNode> _start; // start expression
        std::unique_ptr<ASTNode> _end; // end expression
        std::unique_ptr<ASTNode> _stride; // stride expression


        NSliceItem(): _start(nullptr), _end(nullptr), _stride(nullptr) {}

        NSliceItem(ASTNode *start, ASTNode *end, ASTNode *stride) {
            _start = start ? std::unique_ptr<ASTNode>(start->clone()) : nullptr;
            _end = end ? std::unique_ptr<ASTNode>(end->clone()) : nullptr;
            _stride = stride ? std::unique_ptr<ASTNode>(stride->clone()) : nullptr;
        }

        NSliceItem(const NSliceItem& other) {
            _start = other._start ? std::unique_ptr<ASTNode>(other._start->clone()) : nullptr;
            _end = other._end ? std::unique_ptr<ASTNode>(other._end->clone()) : nullptr;
            _stride = other._stride ? std::unique_ptr<ASTNode>(other._stride->clone()) : nullptr;
            copyAstMembers(other);
        }

        ~NSliceItem() {
            _start = nullptr;
            _end = nullptr;
            _stride = nullptr;
        }

        NSliceItem& operator = (const NSliceItem& other) {
            if(&other != this) {
                _start = nullptr;
                _end = nullptr;
                _stride = nullptr;

                _start = other._start ? std::unique_ptr<ASTNode>(other._start->clone()) : nullptr;
                _end = other._end ? std::unique_ptr<ASTNode>(other._end->clone()) : nullptr;
                _stride = other._stride ? std::unique_ptr<ASTNode>(other._stride->clone()) : nullptr;
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() const {return new NSliceItem(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::SliceItem;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _start, _end, _stride); }
#endif
    };

// @TODO: refactor this into generator object.
// cf. here https://github.com/LeonhardFS/Tuplex/issues/211
    class NRange : public ASTNode {
    public:
        std::vector<std::unique_ptr<ASTNode>> _positionalArguments;

        NRange()    {}

        ~NRange() {
            _positionalArguments.clear();
        }

        // copy constructor
        NRange(const NRange& other) {
            // copy statements of other
            if(!other._positionalArguments.empty()) {
                _positionalArguments.clear();
                _positionalArguments.reserve(other._positionalArguments.size());
                std::for_each(other._positionalArguments.begin(), other._positionalArguments.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    _positionalArguments.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
            }
            copyAstMembers(other);
        }

        /*!
         * creates tuple node from collection (may be empty). deletes collection
         */
//        static NRange* fromCollection(NIdentifier* func, NCollection* collection) {
//            assert(collection);
//            assert(func);
//            auto range = new NRange();
//            assert(range);
//
//            for(auto el : collection->elements()) {
//                range->_positionalArguments.push_back(el->clone());
//            }
//
//            delete collection;
//            collection = nullptr;
//            return range;
//        }

        NRange& operator = (const NRange& other) {
            if(&other != this) {
                // copy statements of other
                if(!other._positionalArguments.empty()) {
                    _positionalArguments.clear();
                    _positionalArguments.reserve(other._positionalArguments.size());
                    std::for_each(other._positionalArguments.begin(), other._positionalArguments.end(), [this](const std::unique_ptr<ASTNode> &node) {
                        _positionalArguments.push_back(std::unique_ptr<ASTNode>(node->clone()));
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        virtual ASTNode* clone() const { return new NRange(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Range;
        virtual ASTNodeType type() const { return type_; }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), _positionalArguments); }
#endif
    };

    class NComprehension : public ASTNode {
    public:
        std::unique_ptr<NIdentifier> target;
        std::unique_ptr<ASTNode> iter;
        std::vector<std::unique_ptr<ASTNode>> if_conditions;

        NComprehension(): target(nullptr), iter(nullptr) {}

        NComprehension(NIdentifier *_target, ASTNode *_iter) {
            target = _target ? std::unique_ptr<NIdentifier>(dynamic_cast<NIdentifier *>(_target->clone())) : nullptr;
            iter = _iter ? std::unique_ptr<ASTNode>(_iter->clone()) : nullptr;
        }

        // copy constructor
        NComprehension(const NComprehension& other) {
            target = other.target ? std::unique_ptr<NIdentifier>(dynamic_cast<NIdentifier*>(other.target->clone())) : nullptr;
            iter = other.iter ? std::unique_ptr<ASTNode>(other.iter->clone()) : nullptr;
            if(!other.if_conditions.empty()) {
                if_conditions.clear();
                if_conditions.reserve(other.if_conditions.size());
                std::for_each(other.if_conditions.begin(), other.if_conditions.end(), [this](const std::unique_ptr<ASTNode> &node) {
                    if_conditions.push_back(std::unique_ptr<ASTNode>(node->clone()));
                });
            }
            copyAstMembers(other);
        }

        ~NComprehension() {
            target = nullptr;
            iter = nullptr;
            if_conditions.clear();
        }

        NComprehension& operator = (const NComprehension& other) {
            if(&other != this) {
                target = nullptr;
                target = other.target ? std::unique_ptr<NIdentifier>(dynamic_cast<NIdentifier*>(other.target->clone())) : nullptr;

                iter = nullptr;
                iter = other.iter ? std::unique_ptr<ASTNode>(other.iter->clone()) : nullptr;

                if(!other.if_conditions.empty()) {
                    // delete statements
                    if_conditions.clear();
                    if_conditions.reserve(other.if_conditions.size());
                    std::for_each(other.if_conditions.begin(), other.if_conditions.end(), [this](const std::unique_ptr<ASTNode> &node) {
                        if_conditions.push_back(std::unique_ptr<ASTNode>(node->clone()));
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() const {return new NComprehension(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Comprehension;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), target, iter, if_conditions); }
#endif
    };

    class NListComprehension : public ASTNode {
    public:
        std::unique_ptr<ASTNode> expression;
        std::vector<std::unique_ptr<NComprehension>> generators;

        NListComprehension(): expression(nullptr) {}

        NListComprehension(ASTNode *_expression) {
            expression = _expression ? std::unique_ptr<ASTNode>(_expression->clone()) : nullptr;
        }

        // copy constructor
        NListComprehension(const NListComprehension& other) {
            expression = other.expression ? std::unique_ptr<ASTNode>(other.expression->clone()) : nullptr;
            if(!other.generators.empty()) {
                generators.clear();
                generators.reserve(other.generators.size());
                std::for_each(other.generators.begin(), other.generators.end(), [this](const std::unique_ptr<NComprehension> &node) {
                    generators.push_back(std::unique_ptr<NComprehension>(dynamic_cast<NComprehension*>(node->clone())));
                });
            }
            copyAstMembers(other);
        }

        ~NListComprehension() {
            expression = nullptr;
            generators.clear();
        }

        NListComprehension& operator = (const NListComprehension& other) {
            if(&other != this) {
                expression = nullptr;
                expression = other.expression ? std::unique_ptr<ASTNode>(other.expression->clone()) : nullptr;

                if(!other.generators.empty()) {
                    generators.clear();
                    generators.reserve(other.generators.size());
                    std::for_each(other.generators.begin(), other.generators.end(), [this](const std::unique_ptr<NComprehension> &node) {
                        generators.push_back(std::unique_ptr<NComprehension>(dynamic_cast<NComprehension*>(node->clone())));
                    });
                }
                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() const {return new NListComprehension(*this);}
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::ListComprehension;
        virtual ASTNodeType type() const { return type_; };

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), expression, generators); }
#endif
    };

// https://docs.python.org/3/reference/compound_stmts.html#the-while-statement
// while_stmt ::=  "while" assignment_expression ":" suite
//                ["else" ":" suite]
    class NWhile : public ASTNode {
    public:
        std::unique_ptr<ASTNode> expression;
        std::unique_ptr<ASTNode> suite_body;
        std::unique_ptr<ASTNode> suite_else;

        NWhile() : expression(nullptr), suite_body(nullptr), suite_else(nullptr) {}
        NWhile(const NWhile& other) {
            expression = other.expression ? std::unique_ptr<ASTNode>(other.expression->clone()) : nullptr;
            suite_body = other.suite_body ? std::unique_ptr<ASTNode>(other.suite_body->clone()) : nullptr;
            suite_else = other.suite_else ? std::unique_ptr<ASTNode>(other.suite_else->clone()) : nullptr;

            copyAstMembers(other);
        }

        NWhile(ASTNode *_expression, ASTNode *_body, ASTNode *_else) {
            assert(_expression);
            assert(_body);
            expression = std::unique_ptr<ASTNode>(_expression->clone());
            suite_body = std::unique_ptr<ASTNode>(_body->clone());
            suite_else = nullptr;
            if(_else)
                suite_else = std::unique_ptr<ASTNode>(_else->clone());
        }

        ~NWhile() {
            expression = nullptr;
            suite_body = nullptr;
            suite_else = nullptr;
        }

        NWhile& operator = (const NWhile& other) {
            if(&other != this) {
                expression = nullptr;
                suite_body = nullptr;
                suite_else = nullptr;

                expression = other.expression ? std::unique_ptr<ASTNode>(other.expression->clone()) : nullptr;
                suite_body = other.suite_body ? std::unique_ptr<ASTNode>(other.suite_body->clone()) : nullptr;
                suite_else = other.suite_else ? std::unique_ptr<ASTNode>(other.suite_else->clone()) : nullptr;

                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() const { return new NWhile(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::While;
        virtual ASTNodeType type() const { return type_; }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), expression, suite_body, suite_else); }
#endif
    };

    class NFor : public ASTNode {
    public:

        // for target in expression:
        //   suite_body
        // else:
        //   suite_else

        std::unique_ptr<ASTNode> target;
        std::unique_ptr<ASTNode> expression;
        std::unique_ptr<ASTNode> suite_body;
        std::unique_ptr<ASTNode> suite_else;

        NFor() : target(nullptr), expression(nullptr), suite_body(nullptr), suite_else(nullptr) {}
        NFor(const NFor& other) {
            target = other.target ? std::unique_ptr<ASTNode>(other.target->clone()) : nullptr;
            expression = other.expression ? std::unique_ptr<ASTNode>(other.expression->clone()) : nullptr;
            suite_body = other.suite_body ? std::unique_ptr<ASTNode>(other.suite_body->clone()) : nullptr;
            suite_else = other.suite_else ? std::unique_ptr<ASTNode>(other.suite_else->clone()) : nullptr;

            copyAstMembers(other);
        }

        NFor(ASTNode *_target, ASTNode *_expression, ASTNode *_body, ASTNode *_else) {
            assert(_target);
            assert(_expression);
            assert(_body);
            target = std::unique_ptr<ASTNode>(_target->clone());
            expression = std::unique_ptr<ASTNode>(_expression->clone());
            suite_body = std::unique_ptr<ASTNode>(_body->clone());
            suite_else = nullptr;
            if(_else)
                suite_else = std::unique_ptr<ASTNode>(_else->clone());
        }

        ~NFor() {
            target = nullptr;
            expression = nullptr;
            suite_body = nullptr;
            suite_else = nullptr;
        }

        NFor& operator = (const NFor& other) {
            if(&other != this) {
                target = nullptr;
                expression = nullptr;
                suite_body = nullptr;
                suite_else = nullptr;

                target = other.target ? std::unique_ptr<ASTNode>(other.target->clone()) : nullptr;
                expression = other.expression ? std::unique_ptr<ASTNode>(other.expression->clone()) : nullptr;
                suite_body = other.suite_body ? std::unique_ptr<ASTNode>(other.suite_body->clone()) : nullptr;
                suite_else = other.suite_else ? std::unique_ptr<ASTNode>(other.suite_else->clone()) : nullptr;

                copyAstMembers(other);
            }
            return *this;
        }

        ASTNode* clone() const { return new NFor(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::For;
        virtual ASTNodeType type() const { return type_; }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), target, expression, suite_body, suite_else); }
#endif
    };

// https://docs.python.org/3/reference/simple_stmts.html#break
    class NBreak : public ASTNode {
#ifdef BUILD_WITH_CEREAL
        bool __MAKE_CEREAL_WORK;
#endif
    public:
        NBreak() = default;
        NBreak(const NBreak& other) = default;
        ASTNode* clone() const { return new NBreak(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Break;
        virtual ASTNodeType type() const { return type_; }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), __MAKE_CEREAL_WORK); }
#endif
    };

// https://docs.python.org/3/reference/simple_stmts.html#the-continue-statement
    class NContinue : public ASTNode {
#ifdef BUILD_WITH_CEREAL
        bool __MAKE_CEREAL_WORK;
#endif
    public:
        NContinue() = default;
        NContinue(const NContinue& other) = default;
        ASTNode* clone() const { return new NContinue(*this); }
        virtual void accept(class IVisitor& visitor);
        static const ASTNodeType type_ = ASTNodeType::Continue;
        virtual ASTNodeType type() const { return type_; }

#ifdef BUILD_WITH_CEREAL
        template<class Archive>
        void serialize(Archive &ar) { ar(::cereal::base_class<ASTNode>(this), __MAKE_CEREAL_WORK); }
#endif
    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::NNumber);
CEREAL_REGISTER_TYPE(tuplex::NIdentifier);
CEREAL_REGISTER_TYPE(tuplex::NBoolean);
CEREAL_REGISTER_TYPE(tuplex::NEllipsis);
CEREAL_REGISTER_TYPE(tuplex::NNone);
CEREAL_REGISTER_TYPE(tuplex::NString);
CEREAL_REGISTER_TYPE(tuplex::NBinaryOp);
CEREAL_REGISTER_TYPE(tuplex::NUnaryOp);
CEREAL_REGISTER_TYPE(tuplex::NSubscription);
CEREAL_REGISTER_TYPE(tuplex::NSuite);
CEREAL_REGISTER_TYPE(tuplex::NModule);
CEREAL_REGISTER_TYPE(tuplex::NParameter);
CEREAL_REGISTER_TYPE(tuplex::NCollection);
CEREAL_REGISTER_TYPE(tuplex::NParameterList);
CEREAL_REGISTER_TYPE(tuplex::NLambda);
CEREAL_REGISTER_TYPE(tuplex::NAwait);
CEREAL_REGISTER_TYPE(tuplex::NStarExpression);
CEREAL_REGISTER_TYPE(tuplex::NCompare);
CEREAL_REGISTER_TYPE(tuplex::NIfElse);
CEREAL_REGISTER_TYPE(tuplex::NFunction);
CEREAL_REGISTER_TYPE(tuplex::NTuple);
CEREAL_REGISTER_TYPE(tuplex::NDictionary);
CEREAL_REGISTER_TYPE(tuplex::NList);
CEREAL_REGISTER_TYPE(tuplex::NReturn);
CEREAL_REGISTER_TYPE(tuplex::NAssert);
CEREAL_REGISTER_TYPE(tuplex::NRaise);
CEREAL_REGISTER_TYPE(tuplex::NAssign);
CEREAL_REGISTER_TYPE(tuplex::NCall);
CEREAL_REGISTER_TYPE(tuplex::NAttribute);
CEREAL_REGISTER_TYPE(tuplex::NSlice);
CEREAL_REGISTER_TYPE(tuplex::NSliceItem);
CEREAL_REGISTER_TYPE(tuplex::NRange);
CEREAL_REGISTER_TYPE(tuplex::NComprehension);
CEREAL_REGISTER_TYPE(tuplex::NListComprehension);
CEREAL_REGISTER_TYPE(tuplex::NWhile);
CEREAL_REGISTER_TYPE(tuplex::NFor);
CEREAL_REGISTER_TYPE(tuplex::NBreak);
CEREAL_REGISTER_TYPE(tuplex::NContinue);
#endif

#endif //TUPLEX_ASTNODES_H
