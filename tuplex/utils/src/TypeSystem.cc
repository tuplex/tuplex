//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <TypeSystem.h>
#include <Logger.h>
#include <stack>
#include <sstream>

#include <TSet.h>
#include <Utils.h>

// types should be like form mypy https://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html


// Note: Option type is actually nullable type. There is a difference because of the nesting.
// ==> i.e. cf. wiki page on this. @TODO clarify later.

namespace python {
    const Type Type::UNKNOWN = TypeFactory::instance().createOrGetPrimitiveType("unknown");
    const Type Type::BOOLEAN = TypeFactory::instance().createOrGetPrimitiveType("boolean");
    const Type Type::I64 = TypeFactory::instance().createOrGetPrimitiveType("i64", {python::Type::BOOLEAN});
    const Type Type::F64 = TypeFactory::instance().createOrGetPrimitiveType("f64", {python::Type::I64});
    const Type Type::STRING = TypeFactory::instance().createOrGetPrimitiveType("str");
    const Type Type::ANY = TypeFactory::instance().createOrGetPrimitiveType("any");
    const Type Type::INF = TypeFactory::instance().createOrGetPrimitiveType("inf");
    const Type Type::EMPTYTUPLE = python::TypeFactory::instance().createOrGetTupleType(std::vector<python::Type>());
    const Type Type::EMPTYDICT = python::TypeFactory::instance().createOrGetPrimitiveType("{}"); // empty dict
    const Type Type::EMPTYLIST = python::TypeFactory::instance().createOrGetPrimitiveType("[]"); // empty list: primitive because it can have any type element
    const Type Type::NULLVALUE = python::TypeFactory::instance().createOrGetPrimitiveType("null");
    const Type Type::PYOBJECT = python::TypeFactory::instance().createOrGetPrimitiveType("pyobject");
    const Type Type::GENERICTUPLE = python::TypeFactory::instance().createOrGetPrimitiveType("tuple");
    const Type Type::GENERICDICT = python::TypeFactory::instance().createOrGetDictionaryType(python::Type::PYOBJECT, python::Type::PYOBJECT);
    const Type Type::GENERICLIST = python::TypeFactory::instance().createOrGetListType(python::Type::PYOBJECT);
    const Type Type::VOID = python::TypeFactory::instance().createOrGetPrimitiveType("void");
    const Type Type::MATCHOBJECT = python::TypeFactory::instance().createOrGetPrimitiveType("matchobject");
    const Type Type::RANGE = python::TypeFactory::instance().createOrGetPrimitiveType("range");
    const Type Type::MODULE = python::TypeFactory::instance().createOrGetPrimitiveType("module");
    const Type Type::ITERATOR = python::TypeFactory::instance().createOrGetPrimitiveType("iterator");
    const Type Type::EMPTYITERATOR = python::TypeFactory::instance().createOrGetPrimitiveType("emptyiterator");

    // builtin exception types
    // --> class system

    std::string Type::desc() const {
        return TypeFactory::instance().getDesc(_hash);
    }

    Type TypeFactory::getByName(const std::string& name) {
        auto it = std::find_if(_typeMap.begin(),
                               _typeMap.end(),
                               [name](const std::pair<const int, TypeEntry>& p) {
                                   return p.second._desc.compare(name) == 0;
                               });
        if(it != _typeMap.end()) {
            auto hash = it->first;
            Type t = Type();
            t._hash = hash;
            return t;
        } else {
          return python::Type::UNKNOWN;
        }
    }

    Type TypeFactory::registerOrGetType(const std::string &name,
                                        const AbstractType at,
                                        const std::vector<Type>& params,
                                        const python::Type& retval,
                                        const std::vector<Type>& baseClasses,
                                        bool isVarLen) {
        auto it = std::find_if(_typeMap.begin(),
                               _typeMap.end(),
                               [name](const std::pair<const int, TypeEntry>& p) {
                                   return p.second._desc.compare(name) == 0;
                               });
        int hash = -1;

        if(it != _typeMap.end()) {
            hash = it->first;
        } else {
            // add new type to hashmap
            hash = _hash_generator++;
            _typeMap[hash] = TypeEntry(name, at, params, retval, baseClasses, isVarLen);
        }

        Type t = Type();
        t._hash = hash;
        return t;
    }

    Type TypeFactory::createOrGetPrimitiveType(const std::string &name, const std::vector<Type>& baseClasses) {
        return registerOrGetType(name, AbstractType::PRIMITIVE,
                                 std::vector<Type>{}, python::Type::VOID, baseClasses);
    }

    Type TypeFactory::createOrGetOptionType(const Type &type) {
        // special cases

        // 1.) NULLVALUE is NULLVALUE
        if(type == python::Type::NULLVALUE)
            return type;

        // 2.) already option? Optional[Optional[...]] = Optional[...]
        if(TypeFactory::instance()._typeMap.at(type._hash)._type == AbstractType::OPTION)
            return type;

        // create new option type!
        std::string name = "Option[" + TypeFactory::instance().getDesc(type._hash) + "]";
        return registerOrGetType(name, AbstractType::OPTION, {}, type);
    }

    Type TypeFactory::createOrGetFunctionType(const Type &param, const Type &ret) {
        std::string name = "";
        name += TypeFactory::instance().getDesc(param._hash);
        name += " -> ";
        name += TypeFactory::instance().getDesc(ret._hash);

        std::vector<Type> params, retvals;

        // special case: param is GENERICTUPLE!
        if(param == python::Type::GENERICTUPLE)
            return registerOrGetType(name, AbstractType::FUNCTION, params, ret, std::vector<Type>(), true);

        // convert to tuple representation
        if(isTupleType(param))
            params = param.parameters();
        else
            params.push_back(param);

        return registerOrGetType(name, AbstractType::FUNCTION, params, ret);
    }

    Type TypeFactory::createOrGetDictionaryType(const Type &key, const Type &val) {
        std::string name = "";
        name += "{";
        name += TypeFactory::instance().getDesc(key._hash);
        name += ",";
        name += TypeFactory::instance().getDesc(val._hash);
        name += "}";

        return registerOrGetType(name, AbstractType::DICTIONARY, {key, val});
    }

    Type TypeFactory::createOrGetListType(const Type &val) {
        std::string name;
        name += "[";
        name += TypeFactory::instance().getDesc(val._hash);
        name += "]";

        return registerOrGetType(name, AbstractType::LIST, {val});
    }

    // C++11 doesn't allow simple conversion to initializer_list, that's why this additional function is needed
    Type TypeFactory::createOrGetTupleType(const std::vector<Type>& args) {
        std::string name = "";
        name += "(";
        for(auto arg : args) {
            name += TypeFactory::instance().getDesc(arg._hash) + ",";
        }

        // end string
        if(name[name.length() - 1] == ',')
            name[name.length() - 1] = ')';
        else
            name += ")";

        return registerOrGetType(name, AbstractType::TUPLE, args);
    }

    Type TypeFactory::createOrGetTupleType(const std::initializer_list<Type> args) {
        std::string name = "";
        name += "(";
        for(auto arg : args) {
            name += TypeFactory::instance().getDesc(arg._hash) + ",";
        }

        // end string
        if(name[name.length() - 1] == ',')
            name[name.length() - 1] = ')';
        else
            name += ")";

        return registerOrGetType(name, AbstractType::TUPLE, args);
    }

    Type TypeFactory::createOrGetTupleType(const TTuple<Type>& args) {
        std::string name = "";
        name += "(";
        for(int i = 0; i < args.length(); ++i) {
            name += TypeFactory::instance().getDesc(args[i]._hash) + ",";
        }

        // end string
        if(name[name.length() - 1] == ',')
            name[name.length() - 1] = ')';
        else
            name += ")";

        std::vector<Type> _args;
        for(int i = 0; i < args.length(); ++i)
            _args.push_back(args[i]);

        return registerOrGetType(name, AbstractType::TUPLE, _args);
    }

    Type TypeFactory::createOrGetIteratorType(const Type& yieldType) {
        std::string name;
        name += "Iterator[";
        name += TypeFactory::instance().getDesc(yieldType._hash);
        name += "]";

        return registerOrGetType(name, AbstractType::ITERATOR, {yieldType});
    }

    std::string TypeFactory::getDesc(const int _hash) const {
        assert(_hash >= 0);
        assert(_typeMap.find(_hash) != _typeMap.end());

        return _typeMap.at(_hash)._desc;
    }
    TypeFactory::~TypeFactory() {

    }


    bool isLiteralType(const Type& type) {
        if(type == Type::I64)
            return true;
        if(type == Type::F64)
            return true;
        if(type == Type::BOOLEAN)
            return true;
        if(type == Type::STRING)
            return true;
        return false;
    }

    bool Type::isTupleType() const {
        return TypeFactory::instance().isTupleType(*this);
    }

    bool Type::isOptionType() const {
        return TypeFactory::instance().isOptionType(*this);
    }

    bool Type::isExceptionType() const {
        auto classes = baseClasses();
        if(classes.empty())
            return false;
        for(auto cls : classes)
            if(cls.desc() == "BaseException")
                return true;
        return false;
    }

    bool Type::isFunctionType() const {
        return TypeFactory::instance().isFunctionType(*this);
    }

    bool Type::isDictionaryType() const {
        return TypeFactory::instance().isDictionaryType(*this);
    }

    bool Type::isListType() const {
        return TypeFactory::instance().isListType(*this);
    }

    bool Type::isIteratorType() const {
        return TypeFactory::instance().isIteratorType(*this);
    }

    Type Type::getReturnType() const {
        // first make sure this a function type!
        if( ! (TypeFactory::instance().isFunctionType(*this) ||
               TypeFactory::instance().isOptionType(*this))) {
            Logger::instance().logger("inference").debug("interpreting non-function type as function type or non-option type as option type.");
            return Type::UNKNOWN;
        }

        return TypeFactory::instance().returnType(*this);
    }

    bool TypeFactory::isFunctionType(const Type &t) const {
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return it->second._type == AbstractType::FUNCTION;
    }

    bool TypeFactory::isOptionType(const python::Type &t) const {
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return it->second._type == AbstractType::OPTION;
    }

    bool TypeFactory::isDictionaryType(const Type &t) const {
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        auto type = it->second._type;
        return type == AbstractType::DICTIONARY || t == Type::EMPTYDICT || t == Type::GENERICDICT;
    }

    bool TypeFactory::isListType(const Type &t) const {
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        auto type = it->second._type;
        return type == AbstractType::LIST || t == Type::EMPTYLIST;
    }

    bool TypeFactory::isTupleType(const Type &t) const {
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return it->second._type == AbstractType::TUPLE;
    }

    bool TypeFactory::isIteratorType(const Type &t) const {
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return it->second._type == AbstractType::ITERATOR || t == Type::EMPTYITERATOR;
    }

    Type TypeFactory::returnType(const python::Type &t) const {
        auto it = _typeMap.find(t._hash);
        assert(it != _typeMap.end());
        return it->second._ret;
    }


    std::vector<Type> TypeFactory::parameters(const Type& t) const {
        auto it = _typeMap.find(t._hash);
        assert(it != _typeMap.end());
        // exclude dictionary here, but internal reuse.
        assert(it->second._type == AbstractType::TUPLE || it->second._type == AbstractType::FUNCTION);
        return it->second._params;
    }

    std::vector<Type> Type::parameters() const {
        return TypeFactory::instance().parameters(*this);
    }

    Type Type::keyType() const {
        assert(isDictionaryType() && _hash != EMPTYDICT._hash && _hash != GENERICDICT._hash);
        auto& factory = TypeFactory::instance();
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        assert(it->second._params.size() == 2);
        return it->second._params[0];
    }

    bool Type::hasVariablePositionalArgs() const {
        assert(isFunctionType());
        auto& factory = TypeFactory::instance();
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        return it->second._isVarLen;
    }

    Type Type::valueType() const {
        assert(isDictionaryType() && _hash != EMPTYDICT._hash && _hash != GENERICDICT._hash);
        auto& factory = TypeFactory::instance();
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        assert(it->second._params.size() == 2);
        return it->second._params[1];
    }

    Type Type::elementType() const {
        if(isListType()) {
            assert(isListType() && _hash != EMPTYLIST._hash);
            auto& factory = TypeFactory::instance();
            auto it = factory._typeMap.find(_hash);
            assert(it != factory._typeMap.end());
            assert(it->second._params.size() == 1);
            return it->second._params[0];
        } else {
            // option?
            assert(isOptionType());
            return getReturnType();
        }
    }

    Type Type::yieldType() const {
        assert(isIteratorType() && _hash != EMPTYITERATOR._hash);
        auto& factory = TypeFactory::instance();
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        assert(it->second._params.size() == 1);
        return it->second._params[0];
    }

    bool Type::isPrimitiveType() const {
        // is this type a primitive type?
        // => only bool, i64, f64 are fixed primitive types (user types not supported)
        return *this == python::Type::BOOLEAN || *this == python::Type::I64 || *this == python::Type::F64
                || *this == python::Type::STRING || *this == python::Type::NULLVALUE;
    }

    bool Type::isIterableType() const {
        return (*this).isIteratorType() || (*this).isListType() || (*this).isTupleType() || *this == python::Type::STRING || *this == python::Type::RANGE || (*this).isDictionaryType();
    }

    bool Type::isFixedSizeType() const {

        // string is a varlen type but a primitive type.
        if(isPrimitiveType() && *this != python::Type::STRING)
            return true;

        // a tuple can be further a fixed type
        if(isTupleType()) {

            // trivial case: empty tuple
            if(parameters().size() == 0)
                return true;

            // iterate over elements
            for(auto el : parameters())
                if(!el.isFixedSizeType())
                    return false;
            return true;
        }

        // empty dict or list
        if(_hash == EMPTYDICT._hash || _hash == EMPTYLIST._hash)
            return true;

        if(isListType() && elementType().isSingleValued())
            return true;

        // option?
        // ==> base type decides!
        if(isOptionType())
            return withoutOptions().isFixedSizeType();

        // functions, dictionaries, and lists are never a fixed type
        return false;
    }

    bool Type::isOptional() const {
        // contains any options?
        if(isOptionType())
            return true;

        if(isPrimitiveType())
            return false;

        // for composite types, search their params / return values
        return desc().find("Option") != std::string::npos; // @TODO: this is a quick and dirty hack, improve.
    }

    bool Type::isSingleValued() const {
        return *this == Type::NULLVALUE || *this == Type::EMPTYTUPLE || *this == Type::EMPTYDICT || *this == Type::EMPTYLIST;
    }

    bool Type::isIllDefined() const {
        // if tuple or function, recursively go over params/return vals
        if(isTupleType()) {
            for(const auto& el: parameters()) {
                if(el.isIllDefined())
                    return true;
            }
            return false;
        } else if(isFunctionType()) {
            for(const auto& el: parameters()) {
                if(el.isIllDefined())
                    return true;
            }
            if(getReturnType().isIllDefined())
                return true;
            return false;
        } else if(isDictionaryType()) {
            // empty or generic are well defined
            if(_hash == Type::EMPTYDICT._hash || _hash == Type::GENERICDICT._hash)
                return false;

            if(keyType().isIllDefined())
                return true;
            if(valueType().isIllDefined())
                return true;
            return false;
        } else if(isListType()) {
            if(_hash == Type::EMPTYLIST._hash)
                return false;

            if(elementType().isIllDefined())
                return true;
            return false;
        } else {
            // must be primitive, directly check
            return    *this == Type::UNKNOWN
                   || *this == Type::ANY
                   || *this == Type::INF;
        }
    }

    Type Type::makeTupleType(std::initializer_list<Type> L) {
        return TypeFactory::instance().createOrGetTupleType(L);
    }

    Type Type::makeTupleType(std::vector<Type> v) {
        return TypeFactory::instance().createOrGetTupleType(v);
    }

    Type Type::makeFunctionType(const python::Type &argsType, const python::Type &retType) {
        return python::TypeFactory::instance().createOrGetFunctionType(argsType, retType);
    }

    Type Type::makeDictionaryType(const python::Type &keyType, const python::Type &valType) {
        return python::TypeFactory::instance().createOrGetDictionaryType(keyType, valType);
    }

    Type Type::makeListType(const python::Type &elementType){
#warning "Nested lists are not yet supported!"
        return python::TypeFactory::instance().createOrGetListType(elementType);
    }

    Type Type::makeOptionType(const python::Type &type) {
        return python::TypeFactory::instance().createOrGetOptionType(type);
    }

    Type Type::makeIteratorType(const python::Type &yieldType) {
        return python::TypeFactory::instance().createOrGetIteratorType(yieldType);
    }

    std::string TypeFactory::TypeEntry::desc() {
        std::string res = "";


        res += _desc + ", ";
        switch(_type) {
            case AbstractType::FUNCTION: {
                res += "function";
                break;
            }
            case AbstractType::DICTIONARY: {
                res += "dictionary";
                break;
            }
            case AbstractType::LIST: {
                res += "list";
                break;
            }
            case AbstractType::TUPLE: {
                res += "tuple";
                break;
            }
            case AbstractType::PRIMITIVE: {
                res += "primitive";
                break;
            }
            default: {
                res += "unknown";
            }
        }


        return res;
    }


    std::string TypeFactory::printAllTypes() {

        std::string res = "";
        for(auto it : _typeMap) {
            res += "hash:" + std::to_string(it.first) + "   " + it.second.desc() + "\n";
        }

        return res;
    }

    Type Type::propagateToTupleType(const python::Type &type) {
        if(type.isTupleType())
            return type;
        else
            return makeTupleType(std::vector<python::Type>{type});
    }

    Type decodeType(const std::string& s) {

        if(s.length() == 0)
            return Type::UNKNOWN;

        // go through string
        int numOpenParentheses = 0;
        int numOpenBrackets = 0;
        int numClosedParentheses = 0;
        int numClosedBrackets = 0;
        int numOpenSqBrackets = 0;
        int numClosedSqBrackets = 0;
        int pos = 0;
        std::stack<bool> sqBracketIsListStack;
        std::stack<std::vector<python::Type> > expressionStack;

        while(pos < s.length()) {

            // parentheses
            if(s[pos] == '(') {
                numOpenParentheses++;
                expressionStack.push(std::vector<python::Type>());
                pos++;
            } else if(s[pos] == ')') {
                numClosedParentheses++;
                if(numOpenParentheses < numClosedParentheses) {
                    Logger::instance().defaultLogger().error("parentheses mismatch in encoded typestr '" + s + "'");
                    return Type::UNKNOWN;
                }

                // create tuple from vector & push back to stack (i.e. appending to last vector)
                assert(expressionStack.size() > 0);
                Type t = TypeFactory::instance().createOrGetTupleType(expressionStack.top());
                expressionStack.pop();
                // empty or not?
                if(expressionStack.empty())
                    expressionStack.push(std::vector<python::Type>({t}));
                else
                    expressionStack.top().push_back(t);
                pos++;
            } else if(s[pos] == '{') {
                numOpenBrackets++;
                expressionStack.push(std::vector<python::Type>());
                pos++;
            } else if(s[pos] == '}') {
                numClosedBrackets++;
                if(numOpenBrackets < numClosedBrackets) {
                    Logger::instance().defaultLogger().error("brackets mismatch in encoded typestr '" + s + "'");
                    return Type::UNKNOWN;
                }

                // create dictionary from vector and push back to stack (append to last vector) : treat it as a 2-element tuple
                assert(expressionStack.size() > 0);
                auto topVec = expressionStack.top();
                Type t = expressionStack.top().size() == 2 ?
                        TypeFactory::instance().createOrGetDictionaryType(topVec[0], topVec[1]) :
                        Type::EMPTYDICT;
                expressionStack.pop();

                if(expressionStack.empty())
                    expressionStack.push({t});
                else
                    expressionStack.top().push_back(t);
                pos++;
            } else if(s[pos] == '[') {
                numOpenSqBrackets++;
                expressionStack.push(std::vector<python::Type>());
                sqBracketIsListStack.push(true);
                pos++;
            } else if(s[pos] == ']') {
              numClosedSqBrackets++;
              if(numOpenSqBrackets < numClosedSqBrackets) {
                  Logger::instance().defaultLogger().error("square brackets [...] mismatch in encoded typestr '" + s + "'");
                  return Type::UNKNOWN;
              }

                // create option type from vector and push back to stack (append to last vector) : treat it as a 2-element tuple
//                if(expressionStack.size() != 1) {
//                    Logger::instance().defaultLogger().error("Option requires one type!");
//                    return Type::UNKNOWN;
//                }
                auto topVec = expressionStack.top();
                auto isList = sqBracketIsListStack.top();
                Type t;
                if(isList) {
                    t = TypeFactory::instance().createOrGetListType(topVec[0]);
                } else {
                    t = TypeFactory::instance().createOrGetOptionType(topVec[0]);
                }
                sqBracketIsListStack.pop();
                expressionStack.pop();

                if(expressionStack.empty())
                    expressionStack.push({t});
                else
                    expressionStack.top().push_back(t);
                pos++;

            } else if(s.substr(pos, 3).compare("i64") == 0) {
                Type t = Type::I64;
                if(expressionStack.empty())
                    expressionStack.push(std::vector<python::Type>({t}));
                else
                    expressionStack.top().push_back(t);
                pos += 3;
            } else if(s.substr(pos, 3).compare("f64") == 0) {
                Type t = Type::F64;
                if(expressionStack.empty())
                    expressionStack.push(std::vector<python::Type>({t}));
                else
                    expressionStack.top().push_back(t);
                pos += 3;
            } else if(s.substr(pos, 3).compare("str") == 0) {
                Type t = Type::STRING;
                if(expressionStack.empty())
                    expressionStack.push(std::vector<python::Type>({t}));
                else
                    expressionStack.top().push_back(t);
                pos += 3;
            } else if(s.substr(pos, 4).compare("bool") == 0) {
                Type t = Type::BOOLEAN;
                if(expressionStack.empty())
                    expressionStack.push(std::vector<python::Type>({t}));
                else
                    expressionStack.top().push_back(t);
                pos += 4;
            } else if(s.substr(pos, 4).compare("None") == 0) {
                Type t = Type::NULLVALUE;
                if(expressionStack.empty())
                    expressionStack.push(std::vector<python::Type>({t}));
                else
                    expressionStack.top().push_back(t);
                pos += 4;
            } else if (s.substr(pos, 7).compare("Option[") == 0) {
                expressionStack.push(std::vector<python::Type>());
                sqBracketIsListStack.push(false);
                numOpenSqBrackets++;
                pos += 7;
            } else if(s[pos] == ',' || s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n') {
                // skip ,
                pos++;
            }
            else {
                std::stringstream ss;
                ss<<"unknown token '"<<s[pos]<<"' in encoded type str '"<<s<<"' encountered.";
                Logger::instance().defaultLogger().error(ss.str());
                return Type::UNKNOWN;
            }
        }

        assert(expressionStack.size() > 0);
        assert(expressionStack.top().size() > 0);
        return expressionStack.top().front();
    }

    bool tupleElementsHaveSameType(const python::Type& tupleType) {
        assert(tupleType.isTupleType());

        if (tupleType == python::Type::EMPTYTUPLE)
            return true;

        assert(tupleType.parameters().size() > 0);

        auto first = tupleType.parameters().front();

        // GCC bug for this code. Clang has no problems with it
//        for (auto it = tupleType.parameters().cbegin() + 1; it != tupleType.parameters().cend(); ++it)
//            if (first != *it)
//                return false;

        // GCC can compile below code.
        for(auto el : tupleType.parameters()) {
            if(el != first)
                return false;
        }

        return true;
    }

    bool tupleElementsHaveTypes(const python::Type& tupleType, const std::vector<python::Type>& elementTypes) {
        assert(tupleType.isTupleType());

        // check that each element is of elementType
        // true if empty tuple!
        if(tupleType == python::Type::EMPTYTUPLE)
            return true;

        for(auto el : tupleType.parameters()) {
            // check that el is in elementTypes
            auto it = std::find(elementTypes.begin(), elementTypes.end(), el);
            if(it == elementTypes.end())
                return false;
        }
        return true;
    }

    bool tupleElementsHaveSimpleTypes(const python::Type& tupleType) {
        return tupleElementsHaveTypes(tupleType, {python::Type::BOOLEAN, python::Type::I64, python::Type::STRING, python::Type::F64});
    }


    bool Type::isNumericType() const {
        if(_hash == python::Type::BOOLEAN._hash)
            return true;
        if(_hash == python::Type::I64._hash)
            return true;
        if(_hash == python::Type::F64._hash)
            return true;
        return false;
    }


    Type Type::superType(const Type &A, const Type &B) {

        // null and x => option[x]
        if (A == python::Type::NULLVALUE)
            return python::Type::makeOptionType(B);
        if (B == python::Type::NULLVALUE)
            return python::Type::makeOptionType(A);

        // dealing with options
        if(A.isOptionType()) {
            if(B.isOptionType()) {
                auto res = superType(A.withoutOptions(), B.withoutOptions());
                if(res == python::Type::UNKNOWN)
                    return python::Type::UNKNOWN;
                return python::Type::makeOptionType(res);
            } else {
                auto res = superType(A.withoutOptions(), B.withoutOptions());
                if(res == python::Type::UNKNOWN)
                    return python::Type::UNKNOWN;
                return python::Type::makeOptionType(res);
            }
        }
        if(B.isOptionType())
            return superType(B, A);

        if (A == B)
            return A;
        if (TSet<Type>({Type::BOOLEAN, Type::I64, Type::F64}).contains(A) &&
            TSet<Type>({Type::BOOLEAN, Type::I64, Type::F64}).contains(B)) {
            if (A == Type::BOOLEAN && B == Type::I64)
                return Type::I64;
            if (A == Type::BOOLEAN && B == Type::F64)
                return Type::F64;
            if (A == Type::I64 && B == Type::F64)
                return Type::F64;
            return superType(B, A);
        }
        return Type::UNKNOWN;
    }


    Type Type::withoutOptions() const {
        using namespace std;

        // check what type it is
        // simple?
        if (isPrimitiveType() || *this == python::Type::EMPTYDICT || *this == python::Type::EMPTYTUPLE || *this == python::Type::EMPTYLIST ||
            *this == python::Type::GENERICDICT || *this == python::Type::GENERICTUPLE ||
            *this == python::Type::NULLVALUE || *this == python::Type::MATCHOBJECT || *this == python::Type::RANGE || *this == python::Type::PYOBJECT)
            return *this;

        if(isTupleType()) {
            // go through elements & construct optionless tuples
            vector<Type> params;
            for(auto t : parameters())
                params.push_back(t.withoutOptions());
            return Type::makeTupleType(params);
        }

        // dict?
        if(isDictionaryType()) {
            return Type::makeDictionaryType(keyType().withoutOptions(), valueType().withoutOptions());
        }

        // list?
        if(isListType()) {
            return Type::makeListType(elementType().withoutOptions());
        }

        // option? ==> ret type!
        if(isOptionType())
            return getReturnType().withoutOptions();

        // func not supported...
        return python::Type::UNKNOWN;
    }


    bool canUpcastType(const python::Type& from, const python::Type& to) {
        // fast check: None can be upcast to any option type!
        if(from == python::Type::NULLVALUE && to.isOptionType())
            return true;

        // fast check: same type
        if(from == to)
            return true;

        // option type?
        if(to.isOptionType()) {
            // from also option type?
            if(from.isOptionType())
                return canUpcastType(from.withoutOptions(), to.withoutOptions());
            else
                return canUpcastType(from, to.withoutOptions());
        }

        // can't upcast option[X] to X
        if(from.isOptionType() && !to.isOptionType())
            return false;

        assert(!from.isOptionType() && !to.isOptionType());

        // compound types: I.e. List or tuple
        if(from.isTupleType() && to.isTupleType()) {
            if(from.parameters().size() != to.parameters().size())
                return false;
            for(unsigned i = 0; i < from.parameters().size(); ++i)
                if(!canUpcastType(from.parameters()[i], to.parameters()[i]))
                    return false;
            return true;
        }

        if(from.isListType() && to.isListType())
            return canUpcastType(from.elementType(), to.elementType());


        // primitive types
        if(from == python::Type::BOOLEAN && (to == python::Type::I64 || to == python::Type::F64))
            return true;
        if(from == python::Type::I64 && to == python::Type::F64)
            return true;

        // empty consts for dict/list work too. NOT FOR EMPTY TUPLE!
        if(from == python::Type::EMPTYDICT && to.isDictionaryType())
            return true;
        if(from == python::Type::EMPTYLIST && to.isListType())
            return true;

        return false;
    }

    /*!
     * check whether types can be upcast
     * @param minor minor row type
     * @param major major row type
     * @return
     */
    bool canUpcastToRowType(const python::Type& minor, const python::Type& major) {
        if(!minor.isTupleType() || !major.isTupleType())
            throw std::runtime_error("upcast check requies both types to be tuple types!");

        auto num_cols = minor.parameters().size();

        for(unsigned i = 0; i < num_cols; ++i) {
            if(!canUpcastType(minor.parameters()[i], major.parameters()[i]))
                return false;
        }
        return true;
    }

    bool python::Type::isZeroSerializationSize() const {
        if(*this == python::Type::NULLVALUE)
            return true;
        if(*this == python::Type::EMPTYTUPLE)
            return true;
        if(*this == python::Type::EMPTYLIST)
            return true;
        if(*this == python::Type::EMPTYDICT)
            return true;
        if(isTupleType()) {
            for(const auto& p : parameters()) {
                if(!p.isZeroSerializationSize())
                    return false;
            }
            return true;
        }
        return false;
    }

    std::vector<Type> python::Type::baseClasses() const {
        // now search baseclasses recursively
        auto& factory = TypeFactory::instance();

        std::set<Type> classes;
        std::deque<Type> q;

        // do not include this itself!
        auto directBaseClasses = factory._typeMap.at(_hash)._baseClasses;
        for(const auto& c : directBaseClasses)
            q.push_back(c);

        // BFS
        while(!q.empty()) {
            auto t = q.front();
            q.pop_front();

            classes.insert(t);
            auto it = factory._typeMap.find(t._hash);
            assert(it != factory._typeMap.end());
            auto more = it->second._baseClasses;
            if(!more.empty()) {
                for(const auto& c : more)
                    q.push_back(c);
            }
        }

        return std::vector<Type>(classes.cbegin(), classes.cend());
    }

    bool python::Type::isSubclass(const Type &derived) const {
        // trivial check?
        if(this->_hash == derived._hash)
            return true;

        auto classes = derived.baseClasses();
        return std::find(classes.cbegin(), classes.cend(), *this) != classes.end();
    }

    std::vector<Type> python::Type::derivedClasses() const {
        // need to go through full hashmap to check for everything + recursive?

        // note this function may be super slow...
        std::set<Type> classes;
        auto& factory = TypeFactory::instance();

        for(const auto& keyval : factory._typeMap) {
            Type t;
            t._hash = keyval.first;
            if(keyval.first != _hash && isSubclass(t))
                classes.insert(t);
        }

        return std::vector<Type>(classes.cbegin(), classes.cend());
    }

    Type Type::byName(const std::string &name) {
        auto& factory = TypeFactory::instance();

        // slow linear search, is this good?
        for(const auto& keyval : factory._typeMap) {
            if(keyval.second._desc == name) {
                Type t;
                t._hash = keyval.first;
                return t;
            }
        }
        return python::Type::UNKNOWN;
    }
}