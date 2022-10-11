//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TYPEHELPER_H
#define TUPLEX_TYPEHELPER_H

#include "TypeSystem.h"

namespace tuplex {

    /*!
    * retrieves the underlying type of an optimized type
    * @param optType
    * @return the unoptimized, underlying type. E.g., an integer for a range-compressed integer.
    */
    inline python::Type deoptimizedType(const python::Type& optType) {
        //@TODO: refactor all these functions/recursive things into something better...
        if(python::Type::UNKNOWN == optType)
            return python::Type::UNKNOWN;

         if(optType.isEmptyType())
                return optType;

        // only constant folding so far supported.
        // also perform nested deoptimize...
        if(optType.isConstantValued()) {
            return optType.underlying();
        }

        // compound type?
        if(optType.isOptionType()) {
            return python::Type::makeOptionType(deoptimizedType(optType.elementType()));
        }

        if(optType.isListType() && python::Type::EMPTYLIST != optType) {
            return python::Type::makeListType(deoptimizedType(optType.elementType()));
        }

        if(optType.isDictionaryType()&& python::Type::GENERICDICT != optType && python::Type::EMPTYDICT != optType && !optType.isStructuredDictionaryType()) {
            return python::Type::makeDictionaryType(deoptimizedType(optType.keyType()), deoptimizedType(optType.valueType()));
        }

        if(optType.isPrimitiveType())
            return optType;

        if(optType.isTupleType()) {
            auto params = optType.parameters();
            for(auto& param : params)
                param = deoptimizedType(param);
            return python::Type::makeTupleType(params);
        }

        if(optType == python::Type::PYOBJECT)
            return python::Type::PYOBJECT;

        // special builtin types...
        if(optType == python::Type::MATCHOBJECT || optType == python::Type::MODULE)
            return optType;

        if(optType.isExceptionType())
            return optType;

        // return generic types
        if(optType.isGeneric())
            return optType;


        throw std::runtime_error("unsupported type " + optType.desc() + " encountered in "
                                 + std::string(__FILE__) + ":" + std::to_string(__LINE__));
    }

    // this function checks whether types can be unified or not

    struct TypeUnificationPolicy {
        bool allowAutoUpcastOfNumbers;  ///! whether to upcast numeric types to a unified type when type conflicts, false by default
        bool treatMissingDictKeysAsNone; ///! whether to treat missing (key, value) pairs as None when unifying structured dictionaries
        bool allowUnifyWithPyObject; ///! when any of a, b is pyobject -> unify to pyobject.
        bool unifyMissingDictKeys; ///! when unifying dictionaries, create a maybe pair if possible to unify types or not.

        TypeUnificationPolicy() : allowAutoUpcastOfNumbers(false),
        treatMissingDictKeysAsNone(false),
        allowUnifyWithPyObject(false),
        unifyMissingDictKeys(false) {}

        static TypeUnificationPolicy defaultPolicy() { return TypeUnificationPolicy(); }
    };

    //    * @param allowAutoUpcastOfNumbers whether to upcast numeric types to a unified type when type conflicts, false by default
    //    * @param treatMissingDictKeysAsNone whether to treat missing (key, value) pairs as None when unifying structured dictionaries
    //    * @param allowUnifyWithPyObject when any of a, b is pyobject -> unify to pyobject.
    //    * @param unifyMissingDictKeys when unifying dictionaries, create a maybe pair if possible to unify types or not.

    /*!
    * return unified type for both a and b
    * e.g. a == [Option[[I64]]] and b == [[Option[I64]]] should return [Option[[Option[I64]]]]
    * return python::Type::UNKNOWN if no compatible type can be found
    * @param a (optional) primitive or list or tuple type
    * @param b (optional) primitive or list or tuple type
    * @param policy define using various switches how type unification should occur.
    * @return (optional) compatible type to which both a and b can be upcasted to given unification policy or UNKNOWN
    */
    extern python::Type unifyTypes(const python::Type& a,
                                   const python::Type& b,
                                   const TypeUnificationPolicy& policy=TypeUnificationPolicy::defaultPolicy());

    /*!
     * return unified type for both a and b
     * @param a
     * @param b
     * @param policy
     * @param os prints cause if unification fails
     * @return compatible type to which both a and b can be upcasted given unification policy or unknown. os provides failure reason.
     */
    extern python::Type unifyTypesEx(const python::Type& a,
                                   const python::Type& b,
                                   const TypeUnificationPolicy& policy,
                                   std::ostream& os);


    inline python::Type unifyTypes(const python::Type& a,
                                   const python::Type& b,
                                   bool allowNumericTypeUnification) {
        TypeUnificationPolicy policy;
        policy.allowAutoUpcastOfNumbers = allowNumericTypeUnification;
        return unifyTypes(a, b, policy);
    }

    /*!
    * special function to unify to a super type for two optimized types...
    * @param A
    * @param B
    * @return
    */
    inline python::Type unifyOptimizedTypes(const python::Type& A, const python::Type& B) {
        // trivial case
        if(A == B)
            return A;

        // i.e. ranges may get combined!

        // fallback - deoptimize
        return python::Type::superType(deoptimizedType(A), deoptimizedType(B));
    }

    /*!
     * compares semantic equality of two values stored as python strings
     * @param type which type to use to interpret the strings.
     * @param rhs string representation of type for a value
     * @param lhs string representation of type for a value
     * @return whether they're equal or not
     */
    extern bool semantic_python_value_eq(const python::Type& type, const std::string& rhs, const std::string& lhs);

    extern std::pair<python::Type, size_t> maximizeStructuredDictTypeCover(const std::vector<std::pair<python::Type, size_t>>& counts,
                                                                           double threshold,
                                                                           bool use_nvo,
                                                                           const TypeUnificationPolicy& t_policy);
}

#endif