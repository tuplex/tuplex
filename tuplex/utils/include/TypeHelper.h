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

            throw std::runtime_error("unsupported type " + optType.desc() + " encountered in "
            + std::string(__FILE__) + ":" + std::to_string(__LINE__));
        }

        // this function checks whether types can be unified or not
        inline python::Type unifyTypes(const python::Type& a, const python::Type& b, bool allowNumbers) {
            using namespace std;
            if(a == b)
                return a;

            // special case: optimized types!
            // -> @TODO: can unify certain types, else just deoptimize.
            if(a.isOptimizedType() || b.isOptimizedType())
                return unifyTypes(deoptimizedType(a), deoptimizedType(b), allowNumbers);


            if(!allowNumbers) {
                // only NULL to any or element and option type allowed
                if (a == python::Type::NULLVALUE)
                    return python::Type::makeOptionType(b);
                if (b == python::Type::NULLVALUE)
                    return python::Type::makeOptionType(a);

                // one is option type, the other not but the elementtype of the option type!
                if (a.isOptionType() && !b.isOptionType() && a.elementType() == b)
                    return a;
                if (b.isOptionType() && !a.isOptionType() && b.elementType() == a)
                    return b;
            } else {
                auto t = python::Type::superType(a, b);
                if(t != python::Type::UNKNOWN)
                    return t;
            }

            // tuples, lists, dicts...
            if(a.isTupleType() && b.isTupleType() && a.parameters().size() == b.parameters().size()) {
                vector<python::Type> v;
                for(unsigned i = 0; i < a.parameters().size(); ++i) {
                    v.push_back(unifyTypes(a.parameters()[i], b.parameters()[i], allowNumbers));
                    if(v.back() == python::Type::UNKNOWN)
                        return python::Type::UNKNOWN;
                }
                return python::Type::makeTupleType(v);
            }

            if(a.isListType() && b.isListType()) {
                auto el = unifyTypes(a.elementType(), b.elementType(), allowNumbers);
                if(el == python::Type::UNKNOWN)
                    return python::Type::UNKNOWN;
                return python::Type::makeListType(el);
            }

            if(a.isDictionaryType() && b.isDictionaryType()) {
                auto key_t = unifyTypes(a.keyType(), b.keyType(), allowNumbers);
                auto val_t = unifyTypes(a.valueType(), b.valueType(), allowNumbers);
                if(key_t != python::Type::UNKNOWN && val_t != python::Type::UNKNOWN)
                    return python::Type::makeDictionaryType(key_t, val_t);
            }
            return python::Type::UNKNOWN;
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