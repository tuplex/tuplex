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

        // only constant folding so far supported.
        // also perform nested deoptimize...
        if(optType.isConstantValued()) {
            return optType.underlying();
        }

        // compound type?
        if(optType.isOptionType()) {
            return python::Type::makeOptionType(deoptimizedType(optType.elementType()));
        }

        if(optType.isListType()) {
            return python::Type::makeListType(deoptimizedType(optType.elementType()));
        }

        if(optType.isDictionaryType()) {
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

        throw std::runtime_error("unsupported type " + optType.desc() + " encountered in "
                                 + std::string(__FILE__) + ":" + std::to_string(__LINE__));
    }

    // this function checks whether types can be unified or not

    /*!
    * return unified type for both a and b
    * e.g. a == [Option[[I64]]] and b == [[Option[I64]]] should return [Option[[Option[I64]]]]
    * return python::Type::UNKNOWN if no compatible type can be found
    * @param a (optional) primitive or list or tuple type
    * @param b (optional) primitive or list or tuple type
    * @param allowAutoUpcastOfNumbers whether to upcast numeric types to a unified type when type conflicts, false by default
    * @return (optional) compatible type or UNKNOWN
    */
    inline python::Type unifyTypes(const python::Type& a, const python::Type& b, bool allowAutoUpcastOfNumbers=false) {
        using namespace std;

        // UNKNOWN types are not compatible
        if(a == python::Type::UNKNOWN || b == python::Type::UNKNOWN) {
            return python::Type::UNKNOWN;
        }

        if(a == b)
            return a;

        // special case: optimized types!
        // -> @TODO: can unify certain types, else just deoptimize.
        if(a.isOptimizedType() || b.isOptimizedType())
            return unifyTypes(deoptimizedType(a), deoptimizedType(b), allowAutoUpcastOfNumbers);

        if(a == python::Type::NULLVALUE)
            return python::Type::makeOptionType(b);

        if(b == python::Type::NULLVALUE)
            return python::Type::makeOptionType(a);

        // check for optional type
        bool makeOption = false;
        // underlyingType: remove outermost Option if it exists
        python::Type aUnderlyingType = a;
        python::Type bUnderlyingType = b;
        if(a.isOptionType()) {
            makeOption = true;
            aUnderlyingType = a.getReturnType();
        }

        if(b.isOptionType()) {
            makeOption = true;
            bUnderlyingType = b.getReturnType();
        }

        // same underlying types? make option
        if (aUnderlyingType == bUnderlyingType) {
            return python::Type::makeOptionType(aUnderlyingType);
        }

        // both numeric types? upcast
        if(allowAutoUpcastOfNumbers) {
            if(aUnderlyingType.isNumericType() && bUnderlyingType.isNumericType()) {
                if(aUnderlyingType == python::Type::F64 || bUnderlyingType == python::Type::F64) {
                    // upcast to F64 if either is F64
                    if (makeOption) {
                        return python::Type::makeOptionType(python::Type::F64);
                    } else {
                        return python::Type::F64;
                    }
                }
                // at this point underlyingTypes cannot both be bool. Upcast to I64
                if (makeOption) {
                    return python::Type::makeOptionType(python::Type::I64);
                } else {
                    return python::Type::I64;
                }
            }
        }

        // list type? check if element type compatible
        if(aUnderlyingType.isListType() && bUnderlyingType.isListType() && aUnderlyingType != python::Type::EMPTYLIST && bUnderlyingType != python::Type::EMPTYLIST) {
            python::Type newElementType = unifyTypes(aUnderlyingType.elementType(), bUnderlyingType.elementType(),
                                                     allowAutoUpcastOfNumbers);
            if(newElementType == python::Type::UNKNOWN) {
                // incompatible list element type
                return python::Type::UNKNOWN;
            }
            if(makeOption) {
                return python::Type::makeOptionType(python::Type::makeListType(newElementType));
            }
            return python::Type::makeListType(newElementType);
        }

        // tuple type? check if every parameter type compatible
        if(aUnderlyingType.isTupleType() && bUnderlyingType.isTupleType()) {
            if (aUnderlyingType.parameters().size() != bUnderlyingType.parameters().size()) {
                // tuple length differs
                return python::Type::UNKNOWN;
            }
            std::vector<python::Type> newTuple;
            for (size_t i = 0; i < aUnderlyingType.parameters().size(); i++) {
                python::Type newElementType = unifyTypes(aUnderlyingType.parameters()[i],
                                                         bUnderlyingType.parameters()[i], allowAutoUpcastOfNumbers);
                if(newElementType == python::Type::UNKNOWN) {
                    // incompatible tuple element type
                    return python::Type::UNKNOWN;
                }
                newTuple.emplace_back(newElementType);
            }
            if(makeOption) {
                return python::Type::makeOptionType(python::Type::makeTupleType(newTuple));
            }
            return python::Type::makeTupleType(newTuple);
        }

        // dictionary type
        if(aUnderlyingType.isDictionaryType() && bUnderlyingType.isDictionaryType()) {
            auto key_t = unifyTypes(aUnderlyingType.keyType(), bUnderlyingType.keyType(), allowAutoUpcastOfNumbers);
            auto val_t = unifyTypes(aUnderlyingType.elementType(), bUnderlyingType.elementType(), allowAutoUpcastOfNumbers);
            if(key_t == python::Type::UNKNOWN || val_t == python::Type::UNKNOWN) {
                return python::Type::UNKNOWN;
            }
            if(makeOption) {
                return python::Type::makeOptionType(python::Type::makeDictionaryType(key_t, val_t));
            } else {
                return python::Type::makeDictionaryType(key_t, val_t);
            }
        }

        // other non-supported types
        return python::Type::UNKNOWN;

        // old:
        //            if(!allowAutoUpcastOfNumbers) {
        //                // only NULL to any or element and option type allowed
        //                if (a == python::Type::NULLVALUE)
        //                    return python::Type::makeOptionType(b);
        //                if (b == python::Type::NULLVALUE)
        //                    return python::Type::makeOptionType(a);
        //
        //                // one is option type, the other not but the elementtype of the option type!
        //                if (a.isOptionType() && !b.isOptionType() && a.elementType() == b)
        //                    return a;
        //                if (b.isOptionType() && !a.isOptionType() && b.elementType() == a)
        //                    return b;
        //            } else {
        //                auto t = python::Type::superType(a, b);
        //                if(t != python::Type::UNKNOWN)
        //                    return t;
        //            }
        //
        //            // tuples, lists, dicts...
        //            if(a.isTupleType() && b.isTupleType() && a.parameters().size() == b.parameters().size()) {
        //                vector<python::Type> v;
        //                for(unsigned i = 0; i < a.parameters().size(); ++i) {
        //                    v.push_back(unifyTypes(a.parameters()[i], b.parameters()[i], allowAutoUpcastOfNumbers));
        //                    if(v.back() == python::Type::UNKNOWN)
        //                        return python::Type::UNKNOWN;
        //                }
        //                return python::Type::makeTupleType(v);
        //            }
        //
        //            if(a.isListType() && b.isListType()) {
        //                auto el = unifyTypes(a.elementType(), b.elementType(), allowAutoUpcastOfNumbers);
        //                if(el == python::Type::UNKNOWN)
        //                    return python::Type::UNKNOWN;
        //                return python::Type::makeListType(el);
        //            }
        //
        //            if(a.isDictionaryType() && b.isDictionaryType()) {
        //                auto key_t = unifyTypes(a.keyType(), b.keyType(), allowAutoUpcastOfNumbers);
        //                auto val_t = unifyTypes(a.valueType(), b.valueType(), allowAutoUpcastOfNumbers);
        //                if(key_t != python::Type::UNKNOWN && val_t != python::Type::UNKNOWN)
        //                    return python::Type::makeDictionaryType(key_t, val_t);
        //            }
        //            return python::Type::UNKNOWN;
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
}

#endif