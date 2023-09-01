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
#include <TypeHelper.h>
#include <Logger.h>
#include <stack>
#include <sstream>

#include <TSet.h>
#include <Utils.h>


#include <Field.h>

#include <JSONUtils.h>

// types should be like form mypy https://mypy.readthedocs.io/en/latest/cheat_sheet_py3.html


// Note: Option type is actually nullable type. There is a difference because of the nesting.
// ==> i.e. cf. wiki page on this. @TODO clarify later.

namespace python {
    const Type Type::UNKNOWN = TypeFactory::instance().createOrGetPrimitiveType("unknown");
    // const Type Type::BOOLEAN = TypeFactory::instance().createOrGetPrimitiveType("boolean");
    const Type Type::BOOLEAN = TypeFactory::instance().createOrGetPrimitiveType("bool");
    const Type Type::I64 = TypeFactory::instance().createOrGetPrimitiveType("i64", {python::Type::BOOLEAN});
    const Type Type::F64 = TypeFactory::instance().createOrGetPrimitiveType("f64", {python::Type::I64});
    const Type Type::STRING = TypeFactory::instance().createOrGetPrimitiveType("str");
    const Type Type::ANY = TypeFactory::instance().createOrGetPrimitiveType("any");
    const Type Type::INF = TypeFactory::instance().createOrGetPrimitiveType("inf");
    const Type Type::EMPTYTUPLE = python::TypeFactory::instance().createOrGetTupleType(std::vector<python::Type>());
    const Type Type::EMPTYDICT = python::TypeFactory::instance().createOrGetPrimitiveType("{}"); // empty dict
    const Type Type::EMPTYLIST = python::TypeFactory::instance().createOrGetPrimitiveType("[]"); // empty list: primitive because it can have any type element
    const Type Type::EMPTYSET = python::TypeFactory::instance().createOrGetPrimitiveType("empty_set"); // empty list: primitive because it can have any type element
    const Type Type::NULLVALUE = python::TypeFactory::instance().createOrGetPrimitiveType("null");
    const Type Type::PYOBJECT = python::TypeFactory::instance().createOrGetPrimitiveType("pyobject");
    const Type Type::GENERICTUPLE = python::TypeFactory::instance().createOrGetPrimitiveType("tuple");
    const Type Type::GENERICDICT = python::TypeFactory::instance().createOrGetDictionaryType(python::Type::PYOBJECT, python::Type::PYOBJECT);
    const Type Type::GENERICLIST = python::TypeFactory::instance().createOrGetListType(python::Type::PYOBJECT);
    //const Type Type::GENERICSET = python::TypeFactory::instance().createOrGetSetType(python::Type::PYOBJECT); // @TODO: implement.
    const Type Type::VOID = python::TypeFactory::instance().createOrGetPrimitiveType("void");
    const Type Type::MATCHOBJECT = python::TypeFactory::instance().createOrGetPrimitiveType("matchobject");
    const Type Type::RANGE = python::TypeFactory::instance().createOrGetPrimitiveType("range");
    const Type Type::MODULE = python::TypeFactory::instance().createOrGetPrimitiveType("module");
    const Type Type::ITERATOR = python::TypeFactory::instance().createOrGetPrimitiveType("iterator");
    const Type Type::EMPTYITERATOR = python::TypeFactory::instance().createOrGetPrimitiveType("emptyiterator");
    const Type Type::TYPEOBJECT = python::TypeFactory::instance().registerOrGetType("type", python::TypeFactory::AbstractType::TYPE, std::vector<Type>{}, python::Type::VOID);

    // special case, empty row
    const Type Type::EMPTYROW = python::TypeFactory::instance().createOrGetRowType({});
    // builtin exception types
    // --> class system

    std::string Type::desc() const {
        return TypeFactory::instance().getDesc(_hash);
    }

    Type TypeFactory::getByName(const std::string& name) {
        std::lock_guard<std::mutex> lock(_typeMapMutex);

        auto it = _typeMapByName.find(name);
        if(it != _typeMapByName.end()) {
            auto hash = _typeVec[it->second]._hash;
            Type t = Type();
            t._hash = hash;
            return t;
        } else {
            return python::Type::UNKNOWN;
        }

        // auto it = std::find_if(_typeMap.begin(),
        //                        _typeMap.end(),
        //                        [name](const std::pair<const int, TypeEntry>& p) {
        //                            return p.second._desc.compare(name) == 0;
        //                        });
        // if(it != _typeMap.end()) {
        //     auto hash = it->first;
        //     Type t = Type();
        //     t._hash = hash;
        //     return t;
        // } else {
        //   return python::Type::UNKNOWN;
        // }
    }

    Type TypeFactory::registerOrGetType(const std::string &name,
                                        const AbstractType at,
                                        const std::vector<Type>& params,
                                        const python::Type& retval,
                                        const std::vector<Type>& baseClasses,
                                        bool isVarLen,
                                        const std::vector<StructEntry>& kv_pairs,
                                        int64_t lower_bound,
                                        int64_t upper_bound,
                                        const std::string& constant) {
        const std::lock_guard<std::mutex> lock(_typeMapMutex);

        // check map by name
        auto it = _typeMapByName.find(name);
//        auto it = std::find_if(_typeMap.begin(),
//                               _typeMap.end(),
//                               [name](const std::pair<const int, TypeEntry>& p) {
//                                   return p.second._desc.compare(name) == 0;
//                               });
        int hash = -1;

        if(it != _typeMapByName.end()) {
            // hash = it->first;
            hash = _typeVec[it->second]._hash;
        } else {
            // add new type to hashmap
            hash = _hash_generator++;
//            _typeMap[hash] = TypeEntry(name, at, params, retval, baseClasses,
//                                       isVarLen, kv_pairs, lower_bound, upper_bound, constant);
            TypeEntry entry(name, at, params, retval, baseClasses,
                      isVarLen, kv_pairs, lower_bound, upper_bound, constant);
            entry._hash = hash;
            _typeMap[hash] = _typeVec.size();
            _typeMapByName[name] = _typeVec.size();
            _typeVec.push_back(entry);
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

        {
            std::lock_guard<std::mutex> lock(_typeMapMutex);
            // 2.) already option? Optional[Optional[...]] = Optional[...]
            auto& factory = TypeFactory::instance();
            if(factory._typeVec[factory._typeMap.at(type._hash)]._type == AbstractType::OPTION)
                return type;
        }


        // create new option type!
        std::string name = "Option[" + TypeFactory::instance().getDesc(type._hash) + "]";
        return registerOrGetType(name, AbstractType::OPTION, {}, type);
    }

    Type TypeFactory::createOrGetTypeObjectType(const Type &type) {
        // special case: type object of type object?
        // e.g., type(type('hello'))
        // --> <class 'type'>
        if(type.isTypeObjectType())
            return Type::TYPEOBJECT; // type object itself is a type object...

        // create new option type!
        std::string name = "Type[" + TypeFactory::instance().getDesc(type._hash) + "]";
        return registerOrGetType(name, AbstractType::TYPE, {type}, type);
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

    Type TypeFactory::createOrGetRowType(const std::vector<python::Type>& column_types, const std::vector<std::string>& column_names) {
        if(column_names.empty()) {
           // no column names, simple tuple-like row type
           std::stringstream ss;
           ss<<"Row[";
           for(unsigned i = 0; i < column_types.size(); ++i) {
               ss<<column_types[i].desc();
               if(i != column_types.size() - 1)
                   ss<<",";
           }
           ss<<"]";
           auto name = ss.str();

           return registerOrGetType(name, AbstractType::ROW, column_types);
        } else {
            assert(column_types.size() == column_names.size());

            // create strurt pairs
            std::vector<StructEntry> kv_pairs;
            kv_pairs.reserve(column_types.size());

            std::stringstream ss;
            ss<<"Row[";
            for(unsigned i = 0; i < column_types.size(); ++i) {

                StructEntry entry;
                entry.key = column_names[i];
                entry.valueType = column_types[i];
                entry.keyType = python::Type::STRING;

                kv_pairs.push_back(entry);

                if(!column_names[i].empty())
                    ss<<escape_to_python_str(column_names[i])<<"->";
                ss<<column_types[i].desc();
                if(i != column_types.size() - 1)
                    ss<<",";
            }
            ss<<"]";
            auto name = ss.str();

            return registerOrGetType(name, AbstractType::ROW, column_types,
                                     python::Type::VOID, {}, false, kv_pairs);
        }
    }

    Type TypeFactory::createOrGetDictionaryType(const Type &key, const Type &val) {
        std::string name = "";
        name += "Dict[";
        name += TypeFactory::instance().getDesc(key._hash);
        name += ",";
        name += TypeFactory::instance().getDesc(val._hash);
        name += "]";

        return registerOrGetType(name, AbstractType::DICTIONARY, {key, val});
    }

    Type TypeFactory::createOrGetDictKeysViewType(const Type& key) {
        std::string name;
        name += "DictKeysView[";
        name += TypeFactory::instance().getDesc(key._hash);
        name += "]";

        return registerOrGetType(name, AbstractType::DICT_KEYS, {key});
    }

    Type TypeFactory::createOrGetDictValuesViewType(const Type& val) {
        std::string name;
        name += "DictValuesView[";
        name += TypeFactory::instance().getDesc(val._hash);
        name += "]";

        return registerOrGetType(name, AbstractType::DICT_VALUES, {val});
    }

    Type TypeFactory::createOrGetListType(const Type &val) {
        std::string name;
        name += "List[";
        name += TypeFactory::instance().getDesc(val._hash);
        name += "]";

        return registerOrGetType(name, AbstractType::LIST, {val});
    }

    // C++11 doesn't allow simple conversion to initializer_list, that's why this additional function is needed
    Type TypeFactory::createOrGetTupleType(const std::vector<Type>& args) {

        // new
        std::stringstream ss;
        ss<<'(';

        // do it with one lock, not locking multiple times! (getDesc each acquires a lock)
        {
            std::lock_guard<std::mutex> lock(_typeMapMutex);
            for(const auto& arg : args)
                ss<<_typeVec[_typeMap.at(arg._hash)]._desc<<',';
        }

        auto name = ss.str();
        // end string
        if(name[name.length() - 1] == ',')
            name[name.length() - 1] = ')';
        else
            name += ")";
        return registerOrGetType(name, AbstractType::TUPLE, args);

        // old
        //std::string name = "";
        //name += "(";
        //for(auto arg : args) {
        //    name += TypeFactory::instance().getDesc(arg._hash) + ",";
        //}
        //
        //// end string
        //if(name[name.length() - 1] == ',')
        //    name[name.length() - 1] = ')';
        //else
        //    name += ")";
        // return registerOrGetType(name, AbstractType::TUPLE, args);
    }

    Type TypeFactory::createOrGetStructuredDictType(const std::vector<std::pair<boost::any, python::Type>> &pairs) {
        std::vector<StructEntry> kv_pairs;
        // for each pair, construct tuple (val_type, value) -> type
        for(auto pair : pairs) {
            std::string pair_str = "(";

            // convert value to field
            tuplex::Field f = tuplex::any_to_field(pair.first, tuplex::Field::null());

            // create the actual kv_pair
            StructEntry kv_pair;
            kv_pair.keyType = f.getType();
            kv_pair.key = f.toPythonString();
            kv_pair.valueType = pair.second;
            kv_pair.alwaysPresent = true;
            kv_pairs.push_back(kv_pair);
        }
        return createOrGetStructuredDictType(kv_pairs);
    }

    static std::vector<StructEntry> remove_bad_pairs(const std::vector<StructEntry>& kv_pairs) {
        // keytype and value have to be unique
        // -> for now ok.
        std::unordered_map<Type, std::vector<StructEntry>> m;
        for(auto kv_pair : kv_pairs) {
            auto it = m.find(kv_pair.keyType);
            if(it == m.end()) {
                m[kv_pair.keyType] = {kv_pair};
            } else {
                // is there a semantically equivalent in there?
                auto jt = std::find_if(it->second.begin(), it->second.end(), [&](const StructEntry& entry) {
                    return tuplex::semantic_python_value_eq(entry.keyType, entry.key, kv_pair.key);
                });
                if(jt == it->second.end()) {
                    // append
                    it->second.push_back(kv_pair);
                } else {
                    // debug
#ifndef NDEBUG
            std::cout<<"found conflicting pair"<<std::endl;
#endif
                }
            }

        }

        // construct pairs from hashmap
        std::vector<StructEntry> pairs;
        for(const auto& kv : m) {
            for(auto p : kv.second)
            pairs.push_back(p);
        }
        return pairs;
    }


    Type TypeFactory::createOrGetStructuredDictType(const std::vector<StructEntry> &kv_pairs) {

        // Struct[] is empty dict
        if(kv_pairs.empty())
            return python::Type::EMPTYDICT;

        std::string name = "Struct[";

        // for each pair, construct tuple (val_type, value) -> type
        for(const auto& kv_pair : kv_pairs) {
            std::string pair_str = "(";

            // @TODO: we basically need a mechanism to serialize/deserialize field values to string and back.
            // add mapping
            // escape non-string values as string
            auto py_string = kv_pair.keyType == python::Type::STRING ? kv_pair.key : escape_to_python_str(kv_pair.key);
            auto map_str = kv_pair.alwaysPresent ? "->" : "=>";
            pair_str += kv_pair.keyType.desc() + "," + py_string + map_str + kv_pair.valueType.desc();

            pair_str += ")";
            name += pair_str + ",";
        }
        if(name.back() == ',')
            name.back() = ']';
        else
            name += "]";

        // store as new type in type factory (@TODO)
        auto t = registerOrGetType(name, AbstractType::STRUCTURED_DICTIONARY, {}, {}, {}, false, kv_pairs);
        return t;
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

    Type TypeFactory::createOrGetDelayedParsingType(const Type& underlying) {

        // check that it is a primitive type
        assert(underlying.isPrimitiveType());

        std::string name;
        name += "_Delayed[";
        name += TypeFactory::instance().getDesc(underlying._hash);
        name += "]";

        return registerOrGetType(name, AbstractType::OPTIMIZED_DELAYEDPARSING, {underlying});
    }

    Type TypeFactory::createOrGetRangeCompressedIntegerType(int64_t lower_bound, int64_t upper_bound) {

        // optimization: if lower_bound == upper_bound, use a constant!
        if(lower_bound == upper_bound)
            return createOrGetConstantValuedType(python::Type::I64, std::to_string(lower_bound));

        std::string name;
        name += "_Compressed[";
        name += "low=" + std::to_string(lower_bound);
        name += ",high=" + std::to_string(upper_bound);
        name += "]";

        return registerOrGetType(name, AbstractType::OPTIMIZED_RANGECOMPRESSION, {python::Type::I64},
                                 python::Type::VOID,
                                 {},
                                 false,
                                 {},
                                 lower_bound,
                                 upper_bound);
    }

    Type TypeFactory::createOrGetConstantValuedType(const Type& underlying, const std::string& constant) {

        // note that constant is the ACTUAL value stored.
//        // check that constant is valid python string in the string case
//#ifndef NDEBUG
//        if(python::Type::STRING == underlying) {
//            if(constant.empty() || (constant[0] != '\'' && constant[0] != '"'))
//                throw std::runtime_error("can only create string constant with properly escaped python string.");
//        }
//#endif

        std::string name;
        name += "_Constant[";
        name += TypeFactory::instance().getDesc(underlying._hash);
        name += ",value=" + constant;
        name += "]";

        return registerOrGetType(name, AbstractType::OPTIMIZED_CONSTANT, {underlying},
                                 python::Type::VOID,
                                 {},
                                 false,
                                 {},
                                 std::numeric_limits<int64_t>::min(),
                                 std::numeric_limits<int64_t>::max(),
                                 constant);
    }

    std::string TypeFactory::getDesc(const int _hash) const {
        if(_hash <= 0)
            return "unknown";

        std::lock_guard<std::mutex> lock(_typeMapMutex);
        assert(_hash >= 0);
        assert(_typeMap.find(_hash) != _typeMap.end());

        return _typeVec[_typeMap.at(_hash)]._desc;
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

    bool Type::isConstantValued() const {
        return TypeFactory::instance().isConstantValued(*this);
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

    bool Type::isStructuredDictionaryType() const {
        return TypeFactory::instance().isStructuredDictionaryType(*this);
    }

    bool Type::isRowType() const {
        return TypeFactory::instance().isRowType(*this);
    }

    bool Type::isListType() const {
        return TypeFactory::instance().isListType(*this);
    }

    bool Type::isIteratorType() const {
        return TypeFactory::instance().isIteratorType(*this);
    }

    bool Type::isDictKeysType() const {
        return TypeFactory::instance().isDictKeysType(*this);
    }

    bool Type::isDictValuesType() const {
        return TypeFactory::instance().isDictValuesType(*this);
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
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return _typeVec[it->second]._type == AbstractType::FUNCTION;
    }

    bool TypeFactory::isOptionType(const python::Type &t) const {
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return _typeVec[it->second]._type == AbstractType::OPTION;
    }

    bool TypeFactory::isDictionaryType(const Type &t) const {
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        auto type = _typeVec[it->second]._type;
        return type == AbstractType::DICTIONARY || t == Type::EMPTYDICT || t == Type::GENERICDICT || type == AbstractType::STRUCTURED_DICTIONARY;
    }

    bool TypeFactory::isStructuredDictionaryType(const Type& t) const {
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        auto type = _typeVec[it->second]._type;
        return type == AbstractType::STRUCTURED_DICTIONARY;
    }

    bool TypeFactory::isRowType(const python::Type &t) const {
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        auto type = _typeVec[it->second]._type;
        return type == AbstractType::ROW;
    }

    bool TypeFactory::isDictKeysType(const Type& t) const {
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return _typeVec[it->second]._type == AbstractType::DICT_KEYS;
    }

    bool TypeFactory::isDictValuesType(const Type& t) const {
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return _typeVec[it->second]._type == AbstractType::DICT_VALUES;
    }

    bool TypeFactory::isListType(const Type &t) const {
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        auto type = _typeVec[it->second]._type;
        return type == AbstractType::LIST || t == Type::EMPTYLIST;
    }

    bool TypeFactory::isTupleType(const Type &t) const {
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return _typeVec[it->second]._type == AbstractType::TUPLE;
    }

    bool TypeFactory::isIteratorType(const Type &t) const {
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return _typeVec[it->second]._type == AbstractType::ITERATOR || t == Type::EMPTYITERATOR;
    }

    Type TypeFactory::returnType(const python::Type &t) const {
        const std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        assert(it != _typeMap.end());
        return _typeVec[it->second]._ret;
    }


    std::vector<Type> TypeFactory::parameters(const Type& t) const {
        const std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        assert(it != _typeMap.end());
        // exclude dictionary here, but internal reuse.
        assert(_typeVec[it->second]._type == AbstractType::TUPLE || _typeVec[it->second]._type == AbstractType::FUNCTION);
        return _typeVec[it->second]._params;
    }

    std::vector<Type> Type::parameters() const {
        return TypeFactory::instance().parameters(*this);
    }

    std::vector<StructEntry> Type::get_struct_pairs() const {
        assert(isStructuredDictionaryType());
        auto& factory = TypeFactory::instance();

        const std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        return factory._typeVec[it->second]._struct_pairs;
    }


    std::vector<std::string> Type::get_column_names() const {
        assert(isRowType());
        auto& factory = TypeFactory::instance();

        const std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        auto kv_pairs = factory._typeVec[it->second]._struct_pairs;
        std::vector<std::string> names;
        for(const auto& entry : kv_pairs)
            names.push_back(entry.key);
        return names;
    }

    python::Type Type::get_column_type(const std::string &key) const {
        assert(isRowType());
        auto& factory = TypeFactory::instance();
        auto index_error_type = factory.getByName("IndexError");


        const std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        auto kv_pairs = factory._typeVec[it->second]._struct_pairs;

        // search within names
        if(kv_pairs.empty())
            return index_error_type;

        // case-sensitive match
        for(const auto& entry : kv_pairs)
            if(entry.key == key)
                return entry.valueType;

        return index_error_type;
    }

    python::Type Type::get_column_type(int64_t index) const {
        assert(isRowType());
        auto& factory = TypeFactory::instance();
        auto index_error_type = factory.getByName("IndexError");


        const std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        auto params = factory._typeVec[it->second]._params;

        // correct negative indices once
        if(index < 0)
            index += params.size();

        // invalid index?
        if(index < 0 || index >= params.size())
            return index_error_type;

        return params[index];
    }

    bool Type::has_column_names() const {
        assert(isRowType());
        auto& factory = TypeFactory::instance();
        const std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        auto kv_pairs = factory._typeVec[it->second]._struct_pairs;

        // check if names exist or not
        return !kv_pairs.empty();
    }

    Type Type::keyType() const {

        // special cases: empty dict, generic dict and structured dict
        if(_hash == EMPTYDICT._hash || _hash == GENERICDICT._hash)
            return PYOBJECT;


        // is it a structured dict? -> same key type?
        if(isStructuredDictionaryType()) {
            // check pairs and whether they all have the same type, if not return pyobject
            auto& factory = TypeFactory::instance();
            factory._typeMapMutex.lock();
            auto it = factory._typeMap.find(_hash);
            assert(it != factory._typeMap.end());
            auto pairs = factory._typeVec[it->second]._struct_pairs;
            factory._typeMapMutex.unlock();
            assert(!pairs.empty()); // --> should be empty dict
            auto key_type = pairs.front().keyType;
            for(auto entry : pairs) {
                // unify types...
                key_type = tuplex::unifyTypes(key_type, entry.keyType);
                if(key_type == UNKNOWN)
                    return PYOBJECT;
            }
            return key_type;
        }

        // regular dict
        assert(isDictionaryType() && !isStructuredDictionaryType() && _hash != EMPTYDICT._hash && _hash != GENERICDICT._hash);
        auto& factory = TypeFactory::instance();

        std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        assert(factory._typeVec[it->second]._params.size() == 2);
        return factory._typeVec[it->second]._params[0];
    }

    bool Type::hasVariablePositionalArgs() const {
        assert(isFunctionType());
        auto& factory = TypeFactory::instance();
        std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        return factory._typeVec[it->second]._isVarLen;
    }

    Type Type::valueType() const {
        // special cases: empty dict, generic dict and structured dict
        if(_hash == EMPTYDICT._hash || _hash == GENERICDICT._hash)
            return PYOBJECT;

        // is it a structured dict? -> same key type?
        if(isStructuredDictionaryType()) {
            // check pairs and whether they all have the same type, if not return pyobject
            auto& factory = TypeFactory::instance();
            factory._typeMapMutex.lock();
            auto it = factory._typeMap.find(_hash);
            assert(it != factory._typeMap.end());
            auto pairs = factory._typeVec[it->second]._struct_pairs;
            factory._typeMapMutex.unlock();
            assert(!pairs.empty()); // --> should be empty dict
            auto value_type = pairs.front().keyType;
            for(auto entry : pairs) {
                // unify types...
                value_type = tuplex::unifyTypes(value_type, entry.valueType);
                if(value_type == UNKNOWN)
                    return PYOBJECT;
            }
            return value_type;
        }

        assert(isDictionaryType() && _hash != EMPTYDICT._hash && _hash != GENERICDICT._hash);
        auto& factory = TypeFactory::instance();
        std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        assert(factory._typeVec[it->second]._params.size() == 2);
        return factory._typeVec[it->second]._params[1];
    }

    Type Type::elementType() const {
        if(isListType() || isDictKeysType() || isDictValuesType()) {
            assert((isListType() && _hash != EMPTYLIST._hash) || isDictKeysType() || isDictValuesType());
            auto& factory = TypeFactory::instance();
            std::lock_guard<std::mutex> lock(factory._typeMapMutex);
            auto it = factory._typeMap.find(_hash);
            assert(it != factory._typeMap.end());
            assert(factory._typeVec[it->second]._params.size() == 1);
            return factory._typeVec[it->second]._params[0];
        } else {
            // option?
            assert(isOptionType());
            return getReturnType();
        }
    }

    Type Type::underlying() const {
        // should be optimizing type or typeobject...
        assert(isOptimizedType() || isTypeObjectType());

        auto& factory = TypeFactory::instance();
        std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        assert(factory._typeVec[it->second]._params.size() == 1);
        return factory._typeVec[it->second]._params[0];
    }

    std::string Type::constant() const {
        assert(isConstantValued());
        auto& factory = TypeFactory::instance();
        std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        assert(factory._typeVec[it->second]._params.size() == 1);
        auto value = factory._typeVec[it->second]._constant_value;

        // this here would be checks if not the actual value was stored.
        // // string or option[string]? then unescape!
        // auto underlying_type = factory._typeVec[it->second]._params[0];
        // auto underlying_hash = underlying_type._hash;
        // auto jt = factory._typeMap.find(underlying_hash);
        // assert(jt != factory._typeMap.end());
        // auto underlying_name = factory._typeVec[jt->second]._desc;
        // if(python::Type::STRING == underlying_type || "Option[str]" == underlying_name) {
        //     if(value == "null" || value == "None")
        //         return value;
        //     return str_value_from_python_raw_value(value);
        // }
        return value;
    }

    Type Type::yieldType() const {
        assert(isIteratorType() && _hash != EMPTYITERATOR._hash);
        auto& factory = TypeFactory::instance();
        auto it = factory._typeMap.find(_hash);
        assert(it != factory._typeMap.end());
        assert(factory._typeVec[it->second]._params.size() == 1);
        return factory._typeVec[it->second]._params[0];
    }

    bool Type::isPrimitiveType() const {
        // is this type a primitive type?
        // => only bool, i64, f64 are fixed primitive types (user types not supported)
        return *this == python::Type::BOOLEAN || *this == python::Type::I64 || *this == python::Type::F64
                || *this == python::Type::STRING || *this == python::Type::NULLVALUE;
    }

    bool Type::isIterableType() const {
        return (*this).isIteratorType() || (*this).isListType() || (*this).isTupleType() || *this == python::Type::STRING || *this == python::Type::RANGE || (*this).isDictionaryType() || (*this).isDictKeysType() || (*this).isDictValuesType();
    }

    bool Type::isFixedSizeType() const {

        // constant valued types are fixed-size, i.e. null => they don't require memory per-instance
        if(isConstantValued())
            return true;

        // delayed parsing types are also fixed-size (they're 8 bytes each!)
        // @TODO.

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
            return withoutOption().isFixedSizeType();

        // dict_keys and dict_values are both immutable
        if(isDictKeysType() || isDictValuesType())
            return true;

        // functions, dictionaries, and lists are never a fixed type
        return false;
    }

    static bool recursive_contains_option(const python::Type& t) {
        if(t.isOptionType())
            return true;
        if(t.isPrimitiveType() || t.isConstantValued())
            return false;

        // composite types:
        if(t.isStructuredDictionaryType())
            return false; // has its own map...

        // similar tp struct dict,
        if(t.isListType())
            return false; //recursive_contains_option(t.elementType());
        if(t.isDictionaryType() && !t.isStructuredDictionaryType())
            return false; //    return recursive_contains_option(t.keyType()) && recursive_contains_option(t.valueType());
        if(t.isTupleType()) {
            for(const auto& p : t.parameters()) {
                if(recursive_contains_option(p))
                    return true;
            }
            return false;
        }

        // other types... prob. false
        return false;
    }

    bool Type::isOptional() const {
        // contains any options?
        if(isOptionType())
            return true;

        if(isPrimitiveType())
            return false;

        // for composite types, search their params / return values
        // return desc().find("Option") != std::string::npos; // @TODO: this is a quick and dirty hack, improve. --> not accurate for constants/struct types etc.
        return recursive_contains_option(*this);
    }

    bool Type::isOptimizedType() const {
        // currently know of the following...
//         OPTIMIZED_CONSTANT, // constant value
//            OPTIMIZED_DELAYEDPARSING, // dummy types to allow for certain optimizations
//            OPTIMIZED_RANGECOMPRESSION // range compression
        auto& factory = TypeFactory::instance();
        std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        const auto& entry = factory._typeVec[factory._typeMap.at(_hash)];
        switch(entry._type) {
            case TypeFactory::AbstractType::OPTIMIZED_CONSTANT:
            case TypeFactory::AbstractType::OPTIMIZED_DELAYEDPARSING:
            case TypeFactory::AbstractType::OPTIMIZED_RANGECOMPRESSION: {
                return true;
            }
            default:
                return false;
        }
    }

    bool Type::isTypeObjectType() const {
        auto& factory = TypeFactory::instance();
        std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        const auto& entry = factory._typeVec[factory._typeMap.at(_hash)];
        return entry._type == TypeFactory::AbstractType::TYPE;
    }

    bool Type::isSingleValued() const {
        // constants are single valued as well
        if(isConstantValued())
            return true;

        // exceptions too
        if(isExceptionType())
            return true;

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

            // special case, structured dict:
            if(isStructuredDictionaryType()) {
                for(auto pair : get_struct_pairs())
                    if(pair.keyType.isIllDefined() || pair.valueType.isIllDefined())
                        return true;
                return false;
            }

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
        } else if (isDictKeysType() || isDictValuesType()) {
            if (elementType().isIllDefined())
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

    Type Type::makeTypeObjectType(const python::Type &type) {
        return TypeFactory::instance().createOrGetTypeObjectType(type);
    }

    Type Type::makeFunctionType(const python::Type &argsType, const python::Type &retType) {
        return python::TypeFactory::instance().createOrGetFunctionType(argsType, retType);
    }

    Type Type::makeDictionaryType(const python::Type &keyType, const python::Type &valType) {
        return python::TypeFactory::instance().createOrGetDictionaryType(keyType, valType);
    }

    Type Type::makeDictKeysViewType(const python::Type& keyType) {
        return python::TypeFactory::instance().createOrGetDictKeysViewType(keyType);
    }

    Type Type::makeDictValuesViewType(const python::Type& valType) {
        return python::TypeFactory::instance().createOrGetDictValuesViewType(valType);
    }

    Type Type::makeListType(const python::Type &elementType){
#warning "Nested lists are not yet supported!"
        return python::TypeFactory::instance().createOrGetListType(elementType);
    }

    Type Type::makeStructuredDictType(const std::vector<std::pair<boost::any, python::Type>> &kv_pairs) {
        return python::TypeFactory::instance().createOrGetStructuredDictType(kv_pairs);
    }

    Type Type::makeStructuredDictType(const std::vector<StructEntry> &kv_pairs) {
        return python::TypeFactory::instance().createOrGetStructuredDictType(kv_pairs);
    }

    Type Type::makeOptionType(const python::Type &type) {
        return python::TypeFactory::instance().createOrGetOptionType(type);
    }

    Type Type::makeIteratorType(const python::Type &yieldType) {
        return python::TypeFactory::instance().createOrGetIteratorType(yieldType);
    }

    Type Type::makeRowType(const std::vector<python::Type>& column_types, const std::vector<std::string>& column_names) {
        return python::TypeFactory::instance().createOrGetRowType(column_types, column_names);
    }

    Type makeDelayedParsingType(const python::Type& underlying) {
        return python::TypeFactory::instance().createOrGetDelayedParsingType(underlying);
    }

    Type makeRangeCompressedIntegerType(int64_t lower_bound, int64_t upper_bound) {
        return python::TypeFactory::instance().createOrGetRangeCompressedIntegerType(lower_bound, upper_bound);
    }

    Type makeConstantValuedType(const python::Type& underlying, const std::string& value) {
        return python::TypeFactory::instance().createOrGetConstantValuedType(underlying, value);
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
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        std::string res = "";
        for(auto it : _typeMap) {
            res += "hash:" + std::to_string(it.first) + "   " + _typeVec[it.second].desc() + "\n";
        }

        return res;
    }

    bool TypeFactory::isConstantValued(const Type &t) const {
        std::lock_guard<std::mutex> lock(_typeMapMutex);
        auto it = _typeMap.find(t._hash);
        if(it == _typeMap.end())
            return false;

        return _typeVec[it->second]._type == AbstractType::OPTIMIZED_CONSTANT;
    }

     bool Type::isEmptyType() const {
        // hashs of constant types
        static std::set<int> empty_hashes{Type::EMPTYTUPLE._hash,
                                          Type::EMPTYLIST._hash,
                                          Type::EMPTYDICT._hash,
                                          Type::EMPTYITERATOR._hash};

        return empty_hashes.find(_hash) != empty_hashes.end();
    }

    Type Type::propagateToTupleType(const python::Type &type) {
        if(type.isTupleType())
            return type;
        else
            return makeTupleType(std::vector<python::Type>{type});
    }

    std::unordered_map<std::string, Type> TypeFactory::get_primitive_keywords() const {
        std::unordered_map<std::string, Type> keywords;
        _typeMapMutex.lock();
        for(const auto& keyval : _typeMap) {
            Type t;
            t._hash = keyval.first;
            if(_typeVec[keyval.second]._type == AbstractType::PRIMITIVE)
                keywords[_typeVec[keyval.second]._desc] = t;
        }
        _typeMapMutex.unlock();
        // add both None and null as NULLVALUE
        keywords["None"] = Type::NULLVALUE;
        keywords["null"] = Type::NULLVALUE;

        return keywords;
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
        // dealing with optimized types --> always deoptimize, For ranges though special case can be performed...
        if(A.isOptimizedType() && B.isOptimizedType())
            return tuplex::unifyOptimizedTypes(A, B);
        if(A.isOptimizedType())
            return superType(tuplex::deoptimizedType(A), B);
        if(B.isOptimizedType())
            return superType(A, tuplex::deoptimizedType(B));

        // ------ regular super types -------

        // null and x => option[x]
        if (A == python::Type::NULLVALUE)
            return python::Type::makeOptionType(B);
        if (B == python::Type::NULLVALUE)
            return python::Type::makeOptionType(A);

        // dealing with options
        if(A.isOptionType()) {
            if(B.isOptionType()) {
                auto res = superType(A.withoutOption(), B.withoutOption());
                if(res == python::Type::UNKNOWN)
                    return python::Type::UNKNOWN;
                return python::Type::makeOptionType(res);
            } else {
                auto res = superType(A.withoutOption(), B.withoutOption());
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

    Type Type::withoutOption() const {
        // option type?
        // else return type as is
        return isOptionType() ? getReturnType() : *this;
    }

    Type Type::withoutOptionsRecursive() const {
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
            for(const auto& t : parameters())
                params.push_back(t.withoutOptionsRecursive());
            return Type::makeTupleType(params);
        }

        // dict?
        if(isDictionaryType()) {
            if(isStructuredDictionaryType()) {
                // go through pairs!
                auto pairs = get_struct_pairs();
                for(auto& entry : pairs) {
                    entry.keyType = entry.keyType.withoutOptionsRecursive();
                    entry.valueType = entry.valueType.withoutOptionsRecursive();
                }
                return Type::makeStructuredDictType(pairs);
            } else {
                // homogenous key/value type dict
                return Type::makeDictionaryType(keyType().withoutOptionsRecursive(), valueType().withoutOptionsRecursive());
            }
        }

        // list?
        if(isListType()) {
            return Type::makeListType(elementType().withoutOptionsRecursive());
        }

        // option? ==> ret type!
        if(isOptionType())
            return getReturnType().withoutOptionsRecursive();

//        // constant valueed? that's a tricky one, b.c. constant plays a role.
//        if(isConstantValued())
//            return
        // func not supported...
        // return type as is
        return *this;
    }


    bool canUpcastType(const python::Type& from, const python::Type& to) {
        // fast check: None can be upcast to any option type!
        if(from == python::Type::NULLVALUE && to.isOptionType())
            return true;

        // fast check: same type
        if(from == to)
            return true;

        // NOTE: optimizing types should come first...
        // optimizing types -> i.e. deoptimized/optimized version should be interchangeabke
        // @TODO: hack. should have one set of things for all the opts
        if(from.isConstantValued())
            return canUpcastType(from.underlying(), to);
        if(to.isConstantValued())
            return canUpcastType(from, to.underlying());


        // option type?
        if(to.isOptionType()) {
            // from also option type?
            if(from.isOptionType())
                return canUpcastType(from.withoutOption(), to.withoutOption());
            else
                return canUpcastType(from, to.withoutOption());
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

        if(from.isListType() && to.isListType()) {
            // empty list can be upcasted to anything, but only empty list can be casted to emptylist
            if(python::Type::EMPTYLIST == from)
                return true;
            if(python::Type::EMPTYLIST == to) {
                assert(python::Type::EMPTYLIST != from);
                return false;
            }
            return canUpcastType(from.elementType(), to.elementType());
        }



        // primitive types
        if(from == python::Type::BOOLEAN && (to == python::Type::I64 || to == python::Type::F64))
            return true;
        if(from == python::Type::I64 && to == python::Type::F64)
            return true;

        // empty consts for dict/list work too. NOT FOR EMPTY TUPLE!
        if(from == python::Type::EMPTYDICT && to.isDictionaryType()) {
            // special case struct dict: upcast only possible if all pairs are maybe
            if(to.isStructuredDictionaryType()) {
                return to.all_struct_pairs_optional();
            }

            return true;
        }

        if(from == python::Type::EMPTYLIST && to.isListType())
            return true;

        // cast between struct dict and dict
        if(from.isStructuredDictionaryType() && to.isDictionaryType()) {
            // special case from & to are both structured -> are they compatible?
            if(from.isStructuredDictionaryType() && to.isStructuredDictionaryType()) {
                auto from_pairs = from.get_struct_pairs();
                auto to_pairs = to.get_struct_pairs();

                // to pairs must have at least as many pairs as from!
                if(from_pairs.size() > to_pairs.size())
                    return false;

                std::unordered_map<std::string, StructEntry> from_key_type_map;
                std::unordered_map<std::string, StructEntry> to_key_type_map;
                for(const auto& p : from_pairs)
                    from_key_type_map[p.key] = p;
                for(const auto& p :to_pairs)
                    to_key_type_map[p.key] = p;

                // now go through from entries and check whether maybe upcast is possible
                for(const auto& kv_from : from_pairs) {
                    // need to put it into from
                    if(to_key_type_map.find(kv_from.key) == to_key_type_map.end())
                        return false;

                    // check "maybe" compatibility. I.e., can upcast a present to maybe but not the other way round
                    auto kv_to = to_key_type_map.at(kv_from.key);
                    if(!kv_from.alwaysPresent && kv_to.alwaysPresent)
                        return false;

                    // can we upcast both key/value?
                    if(!canUpcastType(kv_from.keyType, kv_to.keyType))
                        return false;
                    if(!canUpcastType(kv_from.valueType, kv_to.valueType))
                        return false;
                }

                // each key in "to" that is required must be present in "from" as well
                // => if this fails, can early determine upcast not possible.
                for(const auto& kv : to_key_type_map) {
                    if(kv.second.alwaysPresent) {
                        // must be present and castable
                        if(from_key_type_map.find(kv.first) == from_key_type_map.end())
                            return false;
                        if(!canUpcastType(from_key_type_map[kv.first].keyType, kv.second.keyType))
                            return false;
                        if(!canUpcastType(from_key_type_map[kv.first].valueType, kv.second.valueType))
                            return false;
                    }
                }
                return true;
            } else {
                // check whether ALL from key types and value types can be upcasted to generic type
                auto dest_key_type = to.keyType();
                auto dest_value_type = to.valueType();
                auto pairs = from.get_struct_pairs();
                for(const auto& p : pairs) {
                    // for a dict[keytype,valuetype] keys are always maybe present, so no issue with upcasting.

                    if(!canUpcastType(p.keyType, dest_key_type))
                        return false;
                    if(!canUpcastType(p.valueType, dest_value_type))
                        return false;
                }
                return true;
            }
        }

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

    // moved to TypeHelper.h, deprecated.
//    Type unifyTypes(const python::Type &a, const python::Type &b, bool autoUpcast) {
//        // UNKNOWN type is not compatible
//        if(a == python::Type::UNKNOWN || b == python::Type::UNKNOWN) {
//            return python::Type::UNKNOWN;
//        }
//
//        // same type, return either one
//        if(a == b) {
//            return a;
//        }
//
//        if(a == python::Type::NULLVALUE) {
//            return python::Type::makeOptionType(b);
//        }
//
//        if(b == python::Type::NULLVALUE) {
//            return python::Type::makeOptionType(a);
//        }
//
//        // check for optional type
//        bool makeOption = false;
//        // underlyingType: remove outermost Option if it exists
//        python::Type aUnderlyingType = a;
//        python::Type bUnderlyingType = b;
//        if(a.isOptionType()) {
//            makeOption = true;
//            aUnderlyingType = a.getReturnType();
//        }
//
//        if(b.isOptionType()) {
//            makeOption = true;
//            bUnderlyingType = b.getReturnType();
//        }
//
//        // same underlying types? make option
//        if (aUnderlyingType == bUnderlyingType) {
//            return python::Type::makeOptionType(aUnderlyingType);
//        }
//
//        // both numeric types? upcast
//        if(autoUpcast) {
//            if(aUnderlyingType.isNumericType() && bUnderlyingType.isNumericType()) {
//                if(aUnderlyingType == python::Type::F64 || bUnderlyingType == python::Type::F64) {
//                    // upcast to F64 if either is F64
//                    if (makeOption) {
//                        return python::Type::makeOptionType(python::Type::F64);
//                    } else {
//                        return python::Type::F64;
//                    }
//                }
//                // at this point underlyingTypes cannot both be bool. Upcast to I64
//                if (makeOption) {
//                    return python::Type::makeOptionType(python::Type::I64);
//                } else {
//                    return python::Type::I64;
//                }
//            }
//        }
//
//        // list type? check if element type compatible
//        if(aUnderlyingType.isListType() && bUnderlyingType.isListType() && aUnderlyingType != python::Type::EMPTYLIST && bUnderlyingType != python::Type::EMPTYLIST) {
//            python::Type newElementType = unifyTypes(aUnderlyingType.elementType(), bUnderlyingType.elementType(),
//                                                     autoUpcast);
//            if(newElementType == python::Type::UNKNOWN) {
//                // incompatible list element type
//                return python::Type::UNKNOWN;
//            }
//            if(makeOption) {
//                return python::Type::makeOptionType(python::Type::makeListType(newElementType));
//            }
//            return python::Type::makeListType(newElementType);
//        }
//
//        // tuple type? check if every parameter type compatible
//        if(aUnderlyingType.isTupleType() && bUnderlyingType.isTupleType()) {
//            if (aUnderlyingType.parameters().size() != bUnderlyingType.parameters().size()) {
//                // tuple length differs
//                return python::Type::UNKNOWN;
//            }
//            std::vector<python::Type> newTuple;
//            for (size_t i = 0; i < aUnderlyingType.parameters().size(); i++) {
//                python::Type newElementType = unifyTypes(aUnderlyingType.parameters()[i],
//                                                         bUnderlyingType.parameters()[i], autoUpcast);
//                if(newElementType == python::Type::UNKNOWN) {
//                    // incompatible tuple element type
//                    return python::Type::UNKNOWN;
//                }
//                newTuple.emplace_back(newElementType);
//            }
//            if(makeOption) {
//                return python::Type::makeOptionType(python::Type::makeTupleType(newTuple));
//            }
//            return python::Type::makeTupleType(newTuple);
//        }
//
//        // dictionary type
//        if(aUnderlyingType.isDictionaryType() && bUnderlyingType.isDictionaryType()) {
//            auto key_t = unifyTypes(aUnderlyingType.keyType(), bUnderlyingType.keyType(), autoUpcast);
//            auto val_t = unifyTypes(aUnderlyingType.elementType(), bUnderlyingType.elementType(), autoUpcast);
//            if(key_t == python::Type::UNKNOWN || val_t == python::Type::UNKNOWN) {
//                return python::Type::UNKNOWN;
//            }
//            if(makeOption) {
//                return python::Type::makeOptionType(python::Type::makeDictionaryType(key_t, val_t));
//            } else {
//                return python::Type::makeDictionaryType(key_t, val_t);
//            }
//        }
//
//        // other non-supported types
//        return python::Type::UNKNOWN;
//    }

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
        std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        // unknown? -> no entry.
        if(_hash < 0)
            return {};

        std::set<Type> classes;
        std::deque<Type> q;

        // do not include this itself!
        auto directBaseClasses = factory._typeVec[factory._typeMap.at(_hash)]._baseClasses;
        for(const auto& c : directBaseClasses)
            q.push_back(c);

        // BFS
        while(!q.empty()) {
            auto t = q.front();
            q.pop_front();

            classes.insert(t);
            auto it = factory._typeMap.find(t._hash);
            assert(it != factory._typeMap.end());
            auto more = factory._typeVec[it->second]._baseClasses;
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
        std::vector<int> hashes;
        {
            std::lock_guard<std::mutex> lock(factory._typeMapMutex);
            for(const auto& keyval : factory._typeMap) {
                hashes.emplace_back(keyval.first);
            }
        }
        for(auto hash : hashes) {
            Type t;
            t._hash = hash;
            if(hash != _hash && isSubclass(t))
                classes.insert(t);
        }

        return std::vector<Type>(classes.cbegin(), classes.cend());
    }

    Type Type::byName(const std::string &name) {
        auto& factory = TypeFactory::instance();
        std::lock_guard<std::mutex> lock(factory._typeMapMutex);
        // slow linear search, is this good?
        for(const auto& keyval : factory._typeMap) {
            if(factory._typeVec[keyval.second]._desc == name) {
                Type t;
                t._hash = keyval.first;
                return t;
            }
        }
        return python::Type::UNKNOWN;
    }

    Type Type::makeConstantValuedType(const Type &underlying, const std::string &value) {
        return TypeFactory::instance().createOrGetConstantValuedType(underlying, value);
    }

    Type Type::makeRangeCompressedIntegerType(int64_t lower_bound, int64_t upper_bound) {
        return TypeFactory::instance().createOrGetRangeCompressedIntegerType(lower_bound, upper_bound);
    }

    Type Type::makeDelayedParsingType(const Type &underlying) {
        return TypeFactory::instance().createOrGetDelayedParsingType(underlying);
    }


    inline std::string decodeJSONStringGreedily(const std::string& str, size_t *length) {
        size_t pos = 0;
        std::stringstream ss;
        if(str.size() < 2) {
            if(length)
                *length = 0;
            return "";
        }

        // first char must be "
        if(str.front() != '\"')
            throw std::runtime_error("illegal char");
        pos++;

        while(pos < str.size()) {
            auto c = str[pos];

            if(pos + 1 < str.size()) {
                if(c == '\\' && str[pos + 1] == '\"') {
                    ss<<'\"';
                    pos += 2;
                    continue;
                }
            }

            // single char
            if(c == '\"')
                break;
            else {
                ss<<c;
                pos++;
            }
        }

        // check that last char is "
        if(str[pos] != '\"') {
            if(length)
                *length = 0;
            return "";
        } else {
            pos++;
        }

        if(length)
            *length = pos;
        return ss.str();
    }

    inline Type decodeEx(const std::string& s, size_t *end_position=nullptr, std::ostream* err_stream=nullptr) {
        if(s == "uninitialized") {
            Type t = Type::fromHash(-1);
            if(end_position)
                *end_position = s.length();
            return t;
        }

        // decoder fitting encode function below, super simple.
        if(s.length() == 0)
            return Type::UNKNOWN;

        // fetch primitive keywords for decoding
        size_t min_keyword_length = s.length();
        size_t max_keyword_length = 0;
        std::unordered_map<std::string, Type> keywords = TypeFactory::instance().get_primitive_keywords();

        // add (), {}, and [] as keywords
        keywords["()"] = python::Type::EMPTYTUPLE;
        keywords["[]"] = python::Type::EMPTYLIST;
        keywords["{}"] = python::Type::EMPTYDICT;

        // fix for bool
        keywords["bool"] = python::Type::BOOLEAN;
        keywords["boolean"] = python::Type::BOOLEAN;

        // special case, empty row
        keywords["Row[]"] = python::Type::EMPTYROW;

        for(const auto& kv : keywords) {
            min_keyword_length = std::min(min_keyword_length, kv.first.length());
            max_keyword_length = std::max(max_keyword_length, kv.first.length());
        }

        // go through string
        int numOpenParentheses = 0;
        int numOpenBrackets = 0;
        int numClosedParentheses = 0;
        int numClosedBrackets = 0;
        int numOpenSqBrackets = 0;
        int numClosedSqBrackets = 0;
        int pos = 0;
        std::stack<std::vector<python::Type> > expressionStack;
        std::stack<std::string> compoundStack;

        std::stack<std::vector<StructEntry>> kvStack;

        std::string json_constant_value;

        while(pos < s.length()) {

#ifndef NDEBUG
            // for debug:
            std::string remaining_string = s.substr(pos, std::string::npos);
#endif
            // check against all keywords, match longest keyword first!
            bool keyword_found = false;
            Type keyword_type = Type::UNKNOWN;
            std::string keyword = "";
            for(unsigned i = std::min(s.length() - pos + 1, max_keyword_length); i >= min_keyword_length; --i) {
                auto it = keywords.find(s.substr(pos, i));
                if(it != keywords.end()) {
                    // found keyword, append
                    keyword_type = it->second;
                    keyword = it->first;
                    keyword_found = true;
                    break;
                }
            }

            // check first for keyword, then for parentheses
            if(keyword_found) {
                if(expressionStack.empty()) {
                    expressionStack.push(std::vector<python::Type>({keyword_type}));
                    compoundStack.push("primitive");
                }
                else
                    expressionStack.top().push_back(keyword_type);
                pos += keyword.size(); // should be 3 for i64 e.g.
            } else if(s[pos] == '(' ) {
                numOpenParentheses++;

                // a new pair for struct is encountered when:
                // 1. compoundStack top is Struct
                // 2. expressionStack top is empty -> i.e. the top has been consumed as pair!
                // 3. don't push if the last pair is not filled out completely yet! -> i.e. a tuple type was encountered

                bool push_new_pair = false;
                if(!compoundStack.empty() && compoundStack.top() == "Struct"
                   && (expressionStack.empty() || expressionStack.top().empty()))
                    push_new_pair = true;

                // special case: tuple key
                if(!kvStack.empty()
                   && !kvStack.top().empty()
                   && kvStack.top().back().isUndefined())
                    push_new_pair = false;

                // if in struct compound mode -> push pairs
                if(push_new_pair) {
                    // push new pair
                    assert(!kvStack.empty());
                    kvStack.top().push_back(StructEntry());
                } else {
                    expressionStack.push(std::vector<python::Type>());
                    compoundStack.push("Tuple");
                }
                pos++;
            } else if(s[pos] == ')') {
                // must be a pair -> so take results from stack and push to pairs stack!
                numClosedParentheses++;
                pos++;

                if(numOpenParentheses < numClosedParentheses) {
                    if(err_stream)
                        *err_stream<<"parentheses (...) mismatch in encoded typestr '"<<s<<"'";
                    return Type::UNKNOWN;
                }

                // if in struct compound mode -> push pairs
                if(!compoundStack.empty() && (compoundStack.top() == "Struct" || compoundStack.top() == "Row")) {
                    // edit last pair.
                    assert(!kvStack.empty());
                    assert(!kvStack.top().empty());
                    assert(!expressionStack.empty());
                    assert(expressionStack.top().size() >= 2);
                    auto value_type = expressionStack.top().back();
                    expressionStack.top().pop_back();
                    auto key_type = expressionStack.top().back();
                    expressionStack.top().pop_back();

                    kvStack.top().back().keyType = key_type;
                    kvStack.top().back().valueType = value_type;

                    // special case: encode string (b.c. it's the raw string decoded right now!)
                    if(python::Type::STRING == key_type) {
                        kvStack.top().back().key = escape_to_python_str(kvStack.top().back().key);
                    }
                } else if(!compoundStack.empty() && compoundStack.top() == "Tuple") {
                    // in tuple mode, pop from stack and replace!
                    auto topVec = expressionStack.top();
                    auto compound_type = compoundStack.top();
                    Type t = TypeFactory::instance().createOrGetTupleType(topVec);

                    compoundStack.pop();
                    expressionStack.pop();

                    if(expressionStack.empty()) {
                        expressionStack.push({t});
                        compoundStack.push("primitive");
                    }
                    else
                        expressionStack.top().push_back(t);
                } else {
                   throw std::runtime_error("invalid type!");
                }

            } else if(s[pos] == '\'') {
                // decode '...'-> string
                std::string decoded_string = "";
                pos++;
                while(pos < s.size()) {
                    if(pos + 1 < s.size() && s[pos] == '\\' && s[pos + 1] == '\'') {
                        decoded_string += tuplex::char2str('\'');
                        pos += 2;
                    }  if(pos + 1 < s.size() && s[pos] == '\\' && s[pos + 1] == '\\') {
                        decoded_string += tuplex::char2str('\\');
                        pos += 2;
                    } else if(s[pos] == '\'') {
                        // string is done
                        pos++;
                        break;
                    } else {
                        decoded_string += tuplex::char2str(s[pos]);
                        pos++;
                    }
                }

                // skip any whitespace
                while(pos < s.size() && isspace(s[pos]))
                    pos++;
                // check if next one is ->
                bool alwaysPresent = true;
                if(s.substr(pos, 2) == "->") {
                    alwaysPresent = true;
                    pos += 2;
                } else if(s.substr(pos, 2) == "=>") {
                    alwaysPresent = false;
                    pos += 2;
                } else {
                    throw std::runtime_error("invalid pair found.");
                }

                // special case Row -> push new pair!
                if(!compoundStack.empty() && compoundStack.top() == "Row") {
                    kvStack.top().emplace_back();
                }

                // save onto last pair the string value!
                assert(!kvStack.empty());
                assert(!kvStack.top().empty());
                kvStack.top().back().key = decoded_string;
                kvStack.top().back().alwaysPresent = alwaysPresent;
            } else if(s[pos] == ']') {
                numClosedSqBrackets++;
                if(numOpenSqBrackets < numClosedSqBrackets) {
                    if(err_stream)
                        *err_stream << "square brackets [...] mismatch in encoded typestr '" <<s << "'";
                    return Type::UNKNOWN;
                }
                auto topVec = expressionStack.top();
                auto compound_type = compoundStack.top();
                Type t;
                if("List" == compound_type) {
                    t = TypeFactory::instance().createOrGetListType(topVec[0]);
                } else if("Tuple" == compound_type) {
                    t = TypeFactory::instance().createOrGetTupleType(topVec);
                } else if ("Option" == compound_type) {
                    t = TypeFactory::instance().createOrGetOptionType(topVec[0]); // order?? --> might need reverse...
                } else if("Function" == compound_type) {
                    t = TypeFactory::instance().createOrGetFunctionType(topVec[0], topVec[1]); // order?? --> might need reversion?
                } else if("Dict" == compound_type) {
                    t = TypeFactory::instance().createOrGetDictionaryType(topVec[0], topVec[1]); // order?? --> might need reversion?
                } else if ("Type" == compound_type) {
                    t = TypeFactory::instance().createOrGetTypeObjectType(topVec[0]);
                } else if("Struct" == compound_type) {
                    auto kv_pairs = kvStack.top();
                    kvStack.pop();
                    t = TypeFactory::instance().createOrGetStructuredDictType(kv_pairs);
                } else if("_Constant" == compound_type) {
                    auto underlying_type = topVec[0];
                    t = TypeFactory::instance().createOrGetConstantValuedType(underlying_type, json_constant_value);
                    json_constant_value = "";
                } else if("Row" == compound_type) {
                    auto kv_pairs = kvStack.top();
                    kvStack.pop();

                    // no key pairs? -> no string names given for row. Only integer indexing.
                    if(kv_pairs.empty()) {
                        t = TypeFactory::instance().createOrGetRowType(topVec);
                    } else {
                        assert(topVec.size() == kv_pairs.size());

                        // retrieve names
                        std::vector<std::string> names;
                        for(unsigned i = 0l; i < topVec.size(); ++i) {
                            names.push_back(kv_pairs[i].key);
                        }
                        t = TypeFactory::instance().createOrGetRowType(topVec, names);
                    }
                } else {
                    if(err_stream)
                        *err_stream << "Unknown compound type '" << compound_type << "' encountered, can't create compound type. Returning unknown.";
                    return Type::UNKNOWN;
                }
                compoundStack.pop();
                expressionStack.pop();

                if(expressionStack.empty()) {
                    expressionStack.push({t});
                    compoundStack.push("primitive");
                }
                else
                    expressionStack.top().push_back(t);
                pos++;
            } else if(s[pos] == '[') {
                // legacy, encodes a list
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("List");
                numOpenSqBrackets++;
                pos++;
            } else if(s[pos] == '{') {
                // legacy, should now be treated as Dict[...] except for empty dict
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("Dict");
                numOpenBrackets++;
                pos++;
            } else if(s[pos] == '}') {
                // legacy, should now be treated as Dict[...] except for empty dict

                numClosedBrackets++;
                if(numOpenBrackets < numClosedBrackets) {
                    if(err_stream)
                        *err_stream << "brackets {...} mismatch in encoded typestr '" << s << "'";
                    return Type::UNKNOWN;
                }
                auto topVec = expressionStack.top();
                auto compound_type = compoundStack.top();
                assert("Dict" == compound_type);
                auto t = TypeFactory::instance().createOrGetDictionaryType(topVec[0], topVec[1]); // order?? --> might need reversion?

                compoundStack.pop();
                expressionStack.pop();

                if(expressionStack.empty()) {
                    expressionStack.push({t});
                    compoundStack.push("primitive");
                }
                else
                    expressionStack.top().push_back(t);
                pos++;
            }  else if (s.substr(pos, 7).compare("Option[") == 0) {
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("Option");
                numOpenSqBrackets++;
                pos += 7;
            } else if (s.substr(pos, 6).compare("Tuple[") == 0) {
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("Tuple");
                numOpenSqBrackets++;
                pos += 6;
            } else if (s.substr(pos, 5).compare("Dict[") == 0) {
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("Dict");
                numOpenSqBrackets++;
                pos += 5;
            } else if (s.substr(pos, 9).compare("Function[") == 0) {
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("Function");
                numOpenSqBrackets++;
                pos += 9;
            } else if (s.substr(pos, 5).compare("List[") == 0) {
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("List");
                numOpenSqBrackets++;
                pos += 5;
            } else if(s.substr(pos, strlen("Struct[")).compare("Struct[") == 0) {
                // it's a compound struct type made up of a bunch of other types
                // need to decode pairs manually!
                // i.e., what is the next token? -> if ] => then empty dict!
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("Struct");
                kvStack.push({}); // new pair entry!
                numOpenSqBrackets++;
                pos += strlen("Struct[");
            } else if(s.substr(pos, strlen("Row[")).compare("Row[") == 0) {
                // similar decode to "Struct"
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("Row");
                kvStack.push({}); // new pair entry!
                numOpenSqBrackets++;
                pos += strlen("Row[");
            } else if(s[pos] == ',' || s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n') {
                // skip ,
                pos++;

                // special case _Constant -> value field comes!
                if(compoundStack.top() == "_Constant") {
                    // check that next comes value
                    if(s.substr(pos, strlen("value=")).compare("value=") != 0)
                        throw std::runtime_error("invalid _Constant formatting");
                    // decode the string (no nested structure allowed)
                    pos += strlen("value=");

                    // escape the json string...
                    size_t json_length = 0;
                    json_constant_value = decodeJSONStringGreedily(s.substr(pos), &json_length);
                    pos += json_length;
                }

            } else if(s.substr(pos, strlen("_Constant[")).compare("_Constant[") == 0) {
                expressionStack.push(std::vector<python::Type>());
                compoundStack.push("_Constant");
                numOpenSqBrackets++;
                pos += strlen("_Constant[");
            } else {
                std::stringstream ss;
                ss<<"unknown token '"<<s[pos]<<"' in encoded type str '"<<s<<"' encountered.";
                if(err_stream)
                    *err_stream << ss.str();
                return Type::UNKNOWN;
            }
        }
        if(end_position)
            *end_position = pos;
        assert(expressionStack.size() > 0);
        assert(expressionStack.top().size() > 0);
        return expressionStack.top().front();
    }

    Type Type::decode(const std::string& s) {
        std::stringstream err;
        auto t = decodeEx(s, nullptr,  &err);
        auto err_message = err.str();
        if(!err_message.empty()) {
            throw std::runtime_error("error while decoding type from string: " + err_message);
        }
        return t;
    }

    // TODO: more efficient encoding using binary representation?
    std::string Type::encode() const {
        if(_hash > 0) {
            auto& factory = TypeFactory::instance();
            // use super simple encoding scheme here.
            // -> i.e. primitives use desc
            // else, create compound type using <Name>[...]
            // this allows for easy & quick decoding.
            // => could even use quicker names for encoding the types

            // do not use isPrimitiveType(), ... etc. here
            // because these functions are for semantics...!
            factory._typeMapMutex.lock();
            const auto& entry = factory._typeVec[factory._typeMap.at(_hash)];
            auto abstract_type = entry._type;
            auto entry_desc = entry._desc;
            factory._typeMapMutex.unlock();
            switch(abstract_type) {
                case TypeFactory::AbstractType::PRIMITIVE: {
                    return entry_desc;
                }
                case TypeFactory::AbstractType::OPTION: {
                    return "Option[" + elementType().encode() + "]";
                }
                case TypeFactory::AbstractType::TUPLE: {
                    std::stringstream ss;
                    ss<<"Tuple[";
                    for(unsigned i = 0; i < parameters().size(); ++i) {
                        ss<<parameters()[i].encode();
                        if(i != parameters().size() - 1)
                            ss<<",";
                    }
                    ss<<"]";
                    return ss.str();
                }
                case TypeFactory::AbstractType::LIST: {
                    return "List[" + elementType().encode() + "]";
                }
                case TypeFactory::AbstractType::DICTIONARY: {
                    return "Dict[" + keyType().encode() + "," + valueType().encode() + "]";
                }
                case TypeFactory::AbstractType::STRUCTURED_DICTIONARY:
                case TypeFactory::AbstractType::ROW: {
                    return entry_desc;
                }
                case TypeFactory::AbstractType::FUNCTION: {
                    return "Function[" + getParamsType().encode() + "," + getReturnType().encode() + "]";
                }
                case TypeFactory::AbstractType::OPTIMIZED_CONSTANT: {
                     std::string s = "_Constant[" + underlying().desc() + ",value=";

                     // encode as json string (b.c. easy to parse)
                     s += tuplex::escape_for_json(constant());

                     s += "]";
                     return s;
                }
                default: {
#ifndef NDEBUG
                    // Logger::instance().defaultLogger().error("Unknown type " + desc() + " encountered, can't encode. Using unknown.");
                    std::cerr<<"Unknown type " + desc() + " encountered, can't encode. Using unknown."<<std::endl;
#endif
                    return Type::UNKNOWN.encode();
                }
            }
        } else if (_hash <= 0)
            return "unknown";
        else
            return "uninitialized";
    }

    bool Type::all_struct_pairs_optional() const {
        assert(isStructuredDictionaryType());

        for(auto p : get_struct_pairs())
            if(p.alwaysPresent)
                return false;
        return true;
    }

    bool Type::all_struct_pairs_always_present() const {
        assert(isStructuredDictionaryType());

        for(auto p : get_struct_pairs()) {
            if(!p.alwaysPresent)
                return false;
        }
        return true;
    }

    std::vector<python::Type> primitiveTypes(bool return_options_as_well) {
        std::vector<python::Type> v{python::Type::BOOLEAN, python::Type::I64, python::Type::F64,
                                    python::Type::STRING, python::Type::NULLVALUE, python::Type::EMPTYTUPLE,
                                    python::Type::EMPTYLIST, python::Type::EMPTYDICT};
        //python::Type::PYOBJECT};

        if(return_options_as_well) {
            // make everything optional
            auto num = v.size();
            for(unsigned i = 0; i < num; ++i) {
                v.push_back(python::Type::makeOptionType(v[i]));
            }
        }

        // create set to remove duplicates
        std::set<python::Type> S{v.begin(), v.end()};
        v = std::vector<python::Type>{S.begin(), S.end()};
        // sort
        std::sort(v.begin(), v.end());
        return v;
    }
}