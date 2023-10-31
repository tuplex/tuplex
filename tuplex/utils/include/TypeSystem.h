//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TYPESYSTEM_H
#define TUPLEX_TYPESYSTEM_H

#include "ExceptionCodes.h"

#include <initializer_list>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <mutex>
#include <TTuple.h>
#include <limits>
#include <unordered_map>

#include <boost/any.hpp>

#ifdef BUILD_WITH_CEREAL
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

namespace python {

    class Type;
    class TypeFactory;

    struct StructEntry;

    class Type {
        friend class TypeFactory;
        friend bool operator < (const Type& lhs, const Type& rhs);
        friend bool operator == (const Type& lhs, const Type& rhs);
        friend bool operator != (const Type& lhs, const Type& rhs);
    private:
        // id / hash of this type for type comparison
        // -1 is reserved for undefined type
        int _hash;
    public:
        static const Type UNKNOWN; //! dummy for unknown type
        static const Type VOID; //! ??
        static const Type I64; //! a 64 bit integer value
        static const Type F64; //! a 64 bit floating point value
        static const Type STRING; //! a UTF8 encoded string
        static const Type BOOLEAN; //! a boolean
        static const Type EMPTYTUPLE; //! special type for an empty tuple
        static const Type EMPTYDICT; //! special type for empty dict
        static const Type EMPTYLIST; //! special type for empty list
        static const Type EMPTYSET; //! special type for empty set
        static const Type NULLVALUE; //! special type for a nullvalue / None
        static const Type PYOBJECT; //! special type for any python object
        static const Type GENERICTUPLE; //! special type to accept ANY tuple object (helpful for symbol table)
        static const Type GENERICDICT; //! special type to accept ANY dictionary object
        static const Type GENERICLIST; //! special type to accept ANY list object
        static const Type MATCHOBJECT; //! python [re.match] regex match object
        static const Type RANGE; //! python [range] range object
        static const Type MODULE; //! generic module object, used in symbol table
        static const Type ITERATOR; //! iterator/generator type
        static const Type EMPTYITERATOR; //! special type for empty iterator
        static const Type TYPEOBJECT; // the type of a type object. -> i.e. generic type.

        static const Type EMPTYROW; // empty row (i.e., no columns)

        // define two special types, used in the inference to describe bounds
        // any is a subtype of everything
        static const Type ANY;
        // inf is a supertype of everything
        static const Type INF;
        // i.e. saying something like
        // x :> ANY, x :< INF sets no type bounds on x.

        Type():_hash(-1) {}
        Type(const Type& other):_hash(other._hash)  {
            // assert(_hash >= -1);
        }

        Type& operator = (const Type& other) {
            // assert(_hash >= -1);
            _hash = other._hash;
            return *this;
        }

        bool operator == (const Type& other) {
            return _hash == other._hash;
        }

        bool operator != (const Type& other) {
            return _hash != other._hash;
        }

        std::string desc() const;

        int hash() const { return _hash; }

        // for function types
        Type getReturnType() const;
        Type getParamsType() const {
            if(parameters().empty() && hasVariablePositionalArgs())
                return python::Type::GENERICTUPLE;
            return makeTupleType(parameters());
        }

        bool isTupleType() const;
        bool isFunctionType() const;
        bool isDictionaryType() const;
        bool isStructuredDictionaryType() const;
        bool isListType() const;
        bool isNumericType() const;
        bool isOptionType() const;
        bool isOptional() const;
        bool isOptimizedType() const;
        bool isSingleValued() const;
        bool hasVariablePositionalArgs() const;
        bool isExceptionType() const;
        bool isIteratorType() const;
        bool isConstantValued() const;
        bool isEmptyType() const;
        bool isTypeObjectType() const;
        bool isDictKeysType() const;
        bool isDictValuesType() const;
        bool isRowType() const;

        inline bool isGeneric() const {
            if(_hash == python::Type::PYOBJECT._hash ||
                    _hash == python::Type::GENERICTUPLE._hash ||
                    _hash == python::Type::GENERICLIST._hash ||
                    _hash == python::Type::GENERICDICT._hash)
                return true;
            if(isTupleType()) {
                for(auto p : parameters())
                    if(p.isGeneric())
                        return true;
                return false;
            }

            if(isListType() || isOptionType() || isDictKeysType() || isDictValuesType()) {
                if(elementType().isGeneric())
                    return true;
                return false;
            }

            if(isDictionaryType()) {
                if(keyType().isGeneric())
                    return true;
                if(valueType().isGeneric())
                    return true;
                return false;
            }

            if(isIteratorType()) {
                if(yieldType().isGeneric())
                    return true;
                return false;
            }

            return false;
        }
        /*!
         * create corresponding type without options (recursive!)
         * @return
         */
        Type withoutOptionsRecursive() const;

        /*!
         * removes most outer option (if it is an option).
         * @return the underlying type.
         */
        Type withoutOption() const;

        std::vector<Type> parameters() const;
        std::vector<Type> returnValues() const;

        // convenience functions for dictionaries
        Type keyType() const;
        Type valueType() const;
        // returns the element type in a list or within an option
        Type elementType() const;
        Type underlying() const;

        /*!
         * returns the underlying value of a constant type. Note that this the actual value (not a python string!)
         * @return the value as string.
         */
        std::string constant() const; // returns the underlying constant of the type (opt. HACK)

        /*!
         * return yield type of an iterator
         * @return
         */
        Type yieldType() const;

        /*!
         * checks whether type contains one or more of Unknown, Inf, Any.
         * @return
         */
        bool isIllDefined() const;

        /*!
         * checks whether type is of fixed size. I.e. also a tuple of fixed size datatypes will yield true
         * @return
         */
        bool isFixedSizeType() const;

        /*!
         * if tuple of nulls/empty dict etc.
         * @return
         */
        bool isZeroSerializationSize() const;

         /*!
         * checks whether given type is a primtive type. Currently true for bool, i64, double, str
         * @return
         */
        bool isPrimitiveType() const;

        /*!
         * check whether a given type is iterable. Currently true for iterator, list, tuple, string, range, dictionary, dict_keys, and dict_values.
         * @return
         */
        bool isIterableType() const;

        /*!
         * check whether this is a base class of derived. E.g. int.subclass(float) is true,
         * but float.subclass(int) is false
         * @param derived
         * @return
         */
        bool isSubclass(const Type& derived) const;

        /*!
         * retrieves a vector of all types which are base classes of this type
         * @return all types which are a base class
         */
        std::vector<Type> baseClasses() const;

        /*!
         * retrieves vector of all types which are derived from this type
         * @return vector of type, may be empty.
         */
        std::vector<Type> derivedClasses() const;

        // helper functions
        std::vector<StructEntry> get_struct_pairs() const;

        /*!
         * returns column names of rowtype
         * @return
         */
        std::vector<std::string> get_column_names() const;

        /*!
         * get a specific column type for an integer key (negative allowed
         * @param index
         * @return the type or INDEXERROR if invalid index for row type
         */
        python::Type get_column_type(int64_t index) const;

        /*!
         * similat to the integer version, but checks for keys
         * @param key
         * @return the type or INDEXERROR if invalid
         */
        python::Type get_column_type(const std::string& key) const;

        /*!
         * checks for row type whether there are column names or not
         * @return
         */
        bool has_column_names() const;

        static Type makeTupleType(std::initializer_list<Type> L);
        static Type makeTupleType(std::vector<Type> v);

        static Type makeFunctionType(const python::Type& argsType, const python::Type& retType);

        static Type makeDictionaryType(const python::Type& keyType, const python::Type& valType);

        static Type makeListType(const python::Type &elementType);

        /*!
        * create new row type (positional indexed row). This is an internal row helper type
        * @param column_types the types of each column in the row
        * @param column_names (optional) column names
        * @return new type for this specific row.
        */
        static Type makeRowType(const std::vector<python::Type>& column_types, const std::vector<std::string>& column_names={});

        /*!
         * creates a typeobject for underlying type type. I.e. str itself is a type object referring to string.
         * @param type
         * @return type object type (weird, isn't it?)
         */
        static Type makeTypeObjectType(const python::Type& type);

        // optimizing types (delayed parsing, range compression, ...)
        /*!
         * create a delayed parsing type, i.e. this helpful for small strings, integers having a small ASCII representation or
         * @param underlying which type the data actually represents (should be a primitive like bool, int, float, str)
         * @return the dummy type created.
         */
        static Type makeDelayedParsingType(const python::Type& underlying);

        /*!
         * create a range compressed integer type using lower & upper bound exclusively
         * @param lower_bound integer lower bound (inclusive!)
         * @param upper_bound integer upper bound (inclusive!)
         * @return the dummy type created.
         */
        static Type makeRangeCompressedIntegerType(int64_t lower_bound, int64_t upper_bound);

        /*!
         * create a constant valued type, i.e. this type can get folded via constant folding!
         * @param underlying what actual type this is representing.
         * @param value the constant value. Note: this needs to be decodable...
         * @return the dummy type created.
         */
        static Type makeConstantValuedType(const python::Type& underlying, const std::string& value);

        // TODO: could create dict compressed type as well..
        // static Type makeDictCompressedType()

        // TODO: could create delta-encoded type or so as well...

        static Type makeDictKeysViewType(const python::Type& dictType);
        static Type makeDictValuesViewType(const python::Type& dictType);

        /*!
         * create iterator type from yieldType.
         * @param yieldType
         * @return
         */
        static Type makeIteratorType(const python::Type &yieldType);

        /*!
         * create nullable type/option type from type.
         * @param type the type to be nullable.
         * @return If type is already a nullabble, type will be returned.
         */
        static Type makeOptionType(const python::Type& type);

        /*!
         * creates a (structured) dictionary with known keys.
         * @param kv_pairs
         * @return type created
         */
        static Type makeStructuredDictType(const std::vector<std::pair<boost::any, python::Type>>& kv_pairs);

        /*!
        * creates a (structured) dictionary with known keys.
        * @param kv_pairs
        * @return type created
        */
        static Type makeStructuredDictType(const std::vector<StructEntry>& kv_pairs);

        /*!
         * enclose type as tuple if it is a primitive type, if it is a tuple type, return the type itself
         * @param type
         * @return
         */
        static Type propagateToTupleType(const python::Type& type);


        /*!
         * computes upper/super type of two types. i.e. bool/int -> int
         * @param A
         * @param B
         * @return
         */
        static Type superType(const Type &A, const Type &B);

        /*!
         * construct type from hash
         * @param hash
         * @return
         */
        static Type fromHash(int hash) {
            Type t;
            t._hash = hash;
            return t;
        }

        static Type byName(const std::string& name);

        static Type decode(const std::string& s);
        std::string encode() const;

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive>
        inline void load(Archive &archive) {

            // simply encode/decode type
            std::string encoded_str = "";
            archive(encoded_str);
            auto t = Type::decode(encoded_str);
            _hash = t._hash; // using hash works...

//            TypeFactory::TypeEntry type_entry;
//            archive(_hash, type_entry);
//
//            // @TODO: this here is dangerous!
//            // => i.e. leads to TypeSystem out of sync!
//            // imagine a type system being out of sync with the one on a host machine. Now, any type needs to
//            // remap etc. -> difficult.
//            // better idea: simply overwrite map here
//
//            // Type registerOrGetType(const std::string& name,
//            //                               const AbstractType at,
//            //                               const std::vector<Type>& params = std::vector<Type>(),
//            //                               const python::Type& retval=python::Type::VOID,
//            //                               const std::vector<Type>& baseClasses = std::vector<Type>(),
//            //                               bool isVarLen=false,
//            //                               int64_t lower_bound=std::numeric_limits<int64_t>::min(),
//            //                               int64_t upper_bound=std::numeric_limits<int64_t>::max(),
//            //                               const std::string& constant="");
//            // !!! warning !!!
//            TypeFactory::instance()._typeMap[_hash] = TypeFactory::TypeEntry(type_entry._desc, type_entry._type, type_entry._params,
//                                                                             type_entry._ret, type_entry._baseClasses, type_entry._isVarLen,
//                                                                             type_entry._lower_bound,
//                                                                             type_entry._upper_bound,
//                                                                             type_entry._constant_value);
//
////        // register the type again
////        TypeFactory::instance().registerOrGetType(type_entry._desc, type_entry._type, type_entry._params,
////                                                  type_entry._ret, type_entry._baseClasses, type_entry._isVarLen,
////                                                  type_entry._lower_bound,
////                                                  type_entry._upper_bound,
////                                                  type_entry._constant_value);
        }


        template<class Archive>
        inline void save(Archive &archive) const {
//            // @TODO: this seems wrong, better: need to encode type as string and THEN decode!
//            // that would avoid the remapping problem...!
//            archive(_hash, TypeFactory::instance()._typeMap[_hash]);
            auto encoded_str = encode();
            archive(encoded_str);
        }
#endif

        bool all_struct_pairs_optional() const;
        bool all_struct_pairs_always_present() const;

        size_t get_column_count();
    };

     struct StructEntry { // an entry of a structured dict
        std::string key; // the value of the key, represented as string
        Type keyType; // type required to decode the string key
        Type valueType; // type what to store under key
        bool alwaysPresent; // whether this (key,value) pair is always present or not. if true, use ->, else use =>

        inline bool isUndefined() const {
            return key.empty() && keyType == Type() && valueType == Type();
        }

        StructEntry() : alwaysPresent(true) {}

        StructEntry(const StructEntry& other) : key(other.key), keyType(other.keyType), valueType(other.valueType), alwaysPresent(other.alwaysPresent) {}
        StructEntry(StructEntry&& other) : key(other.key), keyType(other.keyType), valueType(other.valueType), alwaysPresent(other.alwaysPresent) {}
        StructEntry& operator = (const StructEntry& other) {
            key = other.key;
            keyType = other.keyType;
            valueType = other.valueType;
            alwaysPresent = other.alwaysPresent;
            return *this;
        }
    };

    extern bool isLiteralType(const Type& type);

    inline bool operator < (const Type& lhs, const Type& rhs) { return lhs._hash < rhs._hash; }
    inline bool operator == (const Type& lhs, const Type& rhs) { return lhs._hash == rhs._hash; }
    inline bool operator != (const Type& lhs, const Type& rhs) { return lhs._hash != rhs._hash; }

    class TypeFactory {
        // hide internal interfaces and make them only available to Type
        friend class Type;
    private:

        enum class AbstractType {
            PRIMITIVE,
            FUNCTION,
            TUPLE,
            DICTIONARY,
            DICT_KEYS,
            DICT_VALUES,
            STRUCTURED_DICTIONARY,
            LIST,
            CLASS,
            OPTION, // for nullable
            ITERATOR,
            TYPE, // for type objects...
            ROW, // special case for internal row types...
            OPTIMIZED_CONSTANT, // constant value
            OPTIMIZED_DELAYEDPARSING, // dummy types to allow for certain optimizations
            OPTIMIZED_RANGECOMPRESSION // range compression
        };

        struct TypeEntry {
            std::string _desc;
            AbstractType _type;
            std::vector<Type> _params; //! parameters, i.e. tuple entries
            Type _ret; //! return value
            std::vector<Type> _baseClasses; //! base classes from left to right
            bool _isVarLen; // params.empty && _isVarlen => GENERICTUPLE

            // type specific meta-data
            // structured dict:
            std::vector<StructEntry> _struct_pairs; // pairs for structured dicts.

            // opt properties
            int64_t _lower_bound;
            int64_t _upper_bound;
            std::string _constant_value; // everything once was a string...

            TypeEntry()     {}
            TypeEntry(const std::string& desc,
                      const AbstractType at,
                        const std::vector<Type>& params,
                        const Type& ret,
                        const std::vector<Type>& baseClasses=std::vector<Type>{},
                        bool isVarLen=false,
                      const std::vector<StructEntry>& kv_pairs={},
                        int64_t lower_bound=std::numeric_limits<int64_t>::min(),
                        int64_t upper_bound=std::numeric_limits<int64_t>::max(),
                        const std::string& constant="") : _desc(desc), _type(at), _params(params),
                        _ret(ret), _baseClasses(baseClasses), _isVarLen(isVarLen),
                        _struct_pairs(kv_pairs),
                        _lower_bound(lower_bound),
                        _upper_bound(upper_bound),
                        _constant_value(constant),
                        _hash(-1) {}
            TypeEntry(const TypeEntry& other) : _desc(other._desc), _type(other._type), _params(other._params),
            _ret(other._ret), _baseClasses(other._baseClasses), _isVarLen(other._isVarLen),
            _struct_pairs(other._struct_pairs),
            _lower_bound(other._lower_bound), _upper_bound(other._upper_bound), _constant_value(other._constant_value), _hash(other._hash) {}
            std::string desc();

            int _hash;
        };

        int _hash_generator;

        // need threadsafe hashmap here...
        // either tbb's or the one from folly...
        std::vector<TypeEntry> _typeVec;
        std::unordered_map<int, unsigned> _typeMap; // points to typeVec
        std::unordered_map<std::string, unsigned> _typeMapByName; // points to typeVec
        mutable std::mutex _typeMapMutex;

        TypeFactory() : _hash_generator(0)  { _typeVec.reserve(256); }
        std::string getDesc(const int _hash) const;
        Type registerOrGetType(const std::string& name,
                               const AbstractType at,
                               const std::vector<Type>& params = std::vector<Type>(),
                               const python::Type& retval=python::Type::VOID,
                               const std::vector<Type>& baseClasses = std::vector<Type>(),
                               bool isVarLen=false,
                               const std::vector<StructEntry>& kv_pairs={},
                               int64_t lower_bound=std::numeric_limits<int64_t>::min(),
                               int64_t upper_bound=std::numeric_limits<int64_t>::max(),
                               const std::string& constant="");

        bool isFunctionType(const Type& t) const;
        bool isDictionaryType(const Type& t) const;
        bool isStructuredDictionaryType(const Type& t) const;
        bool isDictKeysType(const Type& t) const;
        bool isDictValuesType(const Type& t) const;
        bool isTupleType(const Type& t) const;
        bool isOptionType(const Type& t) const;
        bool isListType(const Type& t) const;
        bool isIteratorType(const Type& t) const;
        bool isConstantValued(const Type& t) const;
        bool isRowType(const Type& t) const;

        std::vector<Type> parameters(const Type& t) const;
        Type returnType(const Type& t) const;

        bool isFixedSizeType() const;
    public:

        ~TypeFactory();

        static TypeFactory& instance() {
            static TypeFactory theoneandonly;
            return theoneandonly;
        }

        /*!
         * returns a lookup map of all registered primitive type keywords (they can be created dynamically...)
         * @return map of keywords -> type.
         */
        std::unordered_map<std::string, Type> get_primitive_keywords() const;

        Type createOrGetPrimitiveType(const std::string& name, const std::vector<Type>& baseClasses=std::vector<Type>{});

        // right now, no tuples or other weird types...
        Type createOrGetFunctionType(const Type& param, const Type& ret=Type::EMPTYTUPLE);
        Type createOrGetDictionaryType(const Type& key, const Type& val);
        Type createOrGetDictKeysViewType(const Type& key);
        Type createOrGetDictValuesViewType(const Type& val);
        Type createOrGetListType(const Type& val);

        Type createOrGetStructuredDictType(const std::vector<std::pair<boost::any, python::Type>>& kv_pairs);
        Type createOrGetStructuredDictType(const std::vector<StructEntry>& kv_pairs);

        Type createOrGetTupleType(const std::initializer_list<Type> args);
        Type createOrGetTupleType(const TTuple<Type>& args);
        Type createOrGetTupleType(const std::vector<Type>& args);
        Type createOrGetOptionType(const Type& type);
        Type createOrGetIteratorType(const Type& yieldType);

        Type createOrGetConstantValuedType(const Type& underlying, const std::string& constant);
        Type createOrGetDelayedParsingType(const Type& underlying);
        Type createOrGetRangeCompressedIntegerType(int64_t lower_bound, int64_t upper_bound);

        Type createOrGetTypeObjectType(const Type& underlying);

        // special type for a row (string keys/integer positions -> type)

        /*!
         * create new row type (positional indexed row). This is an internal row helper type
         * @param column_types the types of each column in the row
         * @param column_names (optional) column names
         * @return new type for this specific row.
         */
        Type createOrGetRowType(const std::vector<python::Type>& column_types, const std::vector<std::string>& column_names={});

        Type getByName(const std::string& name);

        // helper function to connect type system to codegen
        Type getByHash(const int hash) const {
            Type t;
            t._hash = hash;
            return t;
        }

        std::string printAllTypes();
    };


    /*!
     * decode type from string (used in type inference)
     * i64, f64, str, bool supported as primitive types
     * also () for tuples
     * @param s string to be used for type decoding
     * @return decoded type or unknown if decoding error occurred
     */
    inline Type decodeType(const std::string& s) {
        return Type::decode(s);
    }


    /*!
     * checks whether all elements of the tuple type have the same type
     * @param t tuple type
     * @return true when tuple type is created over equal element types.
     */
    extern bool tupleElementsHaveSameType(const python::Type& tupleType);

    /*!
    * check whether types can be upcast
    * @param minor minor python type
    * @param major major python type
    * @return
    */
    extern bool canUpcastType(const python::Type& from, const python::Type& to);

    /*!
     * check whether types can be upcast
     * @param minor minor row type
     * @param major major row type
     * @return
     */
    extern bool canUpcastToRowType(const python::Type& minor, const python::Type& major);

    /*!
     * two types may be combined into one nullable type.
     * @param a
     * @param b
     * @return unknown if no combination is possible, else option or other type.
     */
    inline Type combineToNullableType(const python::Type& a, const python::Type& b) {

        // same type
        if(a == b)
            return a;

        // if any is unknown, unknown!
        if(python::Type::UNKNOWN == a)
            return a;
        if(python::Type::UNKNOWN == b)
            return b;


        // check if one of the types is None, then the type can be unified as option/nullable
        // note branch where both are null can't be because of above if
        if(a == python::Type::NULLVALUE && b != python::Type::NULLVALUE) {
            return python::Type::makeOptionType(b);
        } else if(a != python::Type::NULLVALUE && b == python::Type::NULLVALUE) {
            return python::Type::makeOptionType(a);
        } else if(a.isOptionType() || b.isOptionType()) {
            // one option and the other not?
            // logical XOR
            if(a.isOptionType() && !b.isOptionType()) {
                if(a.getReturnType() == b)
                    return a;
                else
                    return python::Type::UNKNOWN; // underlying not compatible
            } else if(!a.isOptionType() && b.isOptionType()) {
                if(b.getReturnType() == a)
                    return b;
                else
                    return python::Type::UNKNOWN; // underlying not compatible
            } else {
                // both are options, compatible underlying?
                if(a.getReturnType() == b.getReturnType())
                   return a; // should never be true
                else
                    return python::Type::UNKNOWN; // incompatible options
            }
        } else {
            // check whether upcasting in one direction works
            if(canUpcastType(a, b))
                return b;
            if(canUpcastType(b, a))
                return a;

            return python::Type::UNKNOWN; // different types, general case.
        }
    }

    /*!
     * check whether they have simple types (i.e. bool/int/float/string)
     * @param tupleType
     * @return
     */
    extern bool tupleElementsHaveSimpleTypes(const python::Type& tupleType);

    /*!
     * check whether tupleType has elementTypes belonging to the array given
     * @param tupleType
     * @param elementTypes
     * @return
     */
    extern bool tupleElementsHaveTypes(const python::Type& tupleType, const std::vector<python::Type>& elementTypes);

    /*!
     * computes the number of optional fields within a nested tuple type (recursively)
     * @param type of which to compute number of optional fields
     * @return number of optional fields.
     */
    inline size_t numOptionalFields(const python::Type &type) {
        if(type.isOptionType()) return 1;
        if(type.isTupleType()) {
            size_t ret = 0;
            for(const auto &t : type.parameters()) {
                ret += numOptionalFields(t);
            }
            return ret;
        }
        return 0;
    }

    /*!
     * a constant option can be simplified (i.e. remove polymoprhism)
     * @param underlying
     * @param constant
     * @return the simplified underlying type.
     */
    inline python::Type simplifyConstantOption(const python::Type& type) {
        if(!type.isConstantValued())
            return type;

        auto underlying = type.underlying();
        auto value = type.constant();
        if(underlying.isOptionType()) {
            // is the constant null? None?
            if(value == "None" || value == "null") {
                return python::Type::NULLVALUE; // simple, null-value is already a constant!
            } else
                return python::Type::makeConstantValuedType(underlying.elementType(), value);
        }
        return type;
    }

    inline python::Type simplifyConstantType(const python::Type& type) {
        if(!type.isConstantValued())
            return type;

        auto t = simplifyConstantOption(type);

        // special case: _Constant[null] --> null
        if(t.isConstantValued() && t.underlying() == python::Type::NULLVALUE)
            return python::Type::NULLVALUE;

        return t;
    }

    /*!
     * specializes a concrete type to one which could be a generic or is a composite type of generics. For example,
     * assume we have a concrete instance of f64 and a generic version of Option[f64], then the specialized type would be f64.
     * For the general dummy object, i64 and pyobject would return i64.
     * @param concrete a concrete type (could also have generics...)
     * @param generic a type with generics under which to specialize
     * @return the specialized type or UNKNOWN if specialization failed.
     */
    inline python::Type specializeGenerics(const python::Type& concrete, const python::Type& generic) {
        if(concrete == generic)
            return concrete;

        // specialize generics incl. options!
        if(!concrete.isOptionType() && generic.isOptionType()) {
            // specialize element type
            auto specializedElementType = specializeGenerics(concrete, generic.elementType());
            if(specializedElementType != python::Type::UNKNOWN)
                return specializedElementType;
        }
        if(concrete.isOptionType() && !generic.isOptionType()) {
            // specialize element type
            auto specializedElementType = specializeGenerics(concrete.elementType(), generic);
            if(specializedElementType != python::Type::UNKNOWN)
                return specializedElementType;
        }

        // specialize tuple
        if(concrete.isTupleType() && generic.isTupleType()) {
            // same number of params?
            if(generic != python::Type::GENERICTUPLE) {
                if(concrete.parameters().size() != generic.parameters().size())
                    return python::Type::UNKNOWN; // can't specialize...
                auto numElements = concrete.parameters().size();
                std::vector<python::Type> v;
                v.reserve(numElements);
                for(unsigned i = 0; i < numElements; ++i)
                    v.emplace_back(specializeGenerics(concrete.parameters()[i], generic.parameters()[i]));
                return python::Type::makeTupleType(v);
            }
        }

        // generic tuple?
        if(concrete.isTupleType() && generic == python::Type::GENERICTUPLE)
            return concrete;

        // empty tuple?
        if(concrete == python::Type::EMPTYTUPLE)
            return concrete; // empty tuple is already fully specialized!

        // dict
        if(concrete.isDictionaryType() && generic.isDictionaryType()) {
            if(concrete == python::Type::EMPTYDICT) // empty dict is already fully specialized!
                return python::Type::EMPTYDICT;


            if(generic != python::Type::GENERICDICT)
                return python::Type::makeDictionaryType(specializeGenerics(concrete.keyType(), generic.valueType()),
                                                        specializeGenerics(concrete.keyType(), generic.valueType()));
        }
        if(concrete.isDictionaryType() && generic == python::Type::GENERICDICT)
            return concrete;

        // list
        if(concrete.isListType() && generic.isListType()) {
            if(concrete == python::Type::EMPTYLIST) // empty list is already fully specialized!
                return python::Type::EMPTYLIST;

            if(concrete != python::Type::GENERICLIST) {
                return python::Type::makeListType(specializeGenerics(concrete.elementType(), generic.elementType()));
            }
        }
        if(concrete.isListType() && generic == python::Type::GENERICLIST)
            return concrete;

        // generic python object
        if(generic == python::Type::PYOBJECT)
            return concrete;

        return python::Type::UNKNOWN;
    }

    /*!
     * returns a core vector of types to support. Mainly used to write tests.
     * @return vector of types
     */
    extern std::vector<python::Type> primitiveTypes(bool return_options_as_well=false);

    inline bool isTupleOfConstants(const python::Type& tuple_type) {
        if(!tuple_type.isTupleType())
            return false;
        for(const auto t : tuple_type.parameters()) {
            if(!t.isConstantValued())
                return false;
        }
        return true;
    }

}


// make Type std hashable
namespace std {

    template <>
    struct hash<python::Type>
    {
        std::size_t operator()(const python::Type& t) const
        {
            return t.hash();
        }
    };

}

namespace tuplex {
    inline python::Type get_exception_type(const ExceptionCode& ec) {
        std::string name = "Exception";
        name = exceptionCodeToPythonClass(ec);
        auto t = python::TypeFactory::instance().getByName(name);
        assert(t != python::Type::UNKNOWN);
        return t;
    }
}


#endif //TUPLEX_TYPES_H
