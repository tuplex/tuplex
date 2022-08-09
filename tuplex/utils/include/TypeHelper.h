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
    * @param treatMissingDictKeysAsNone whether to treat missing (key, value) pairs as None when unifying structured dictionaries
    * @param allowUnifyWithPyObject when any of a, b is pyobject -> unify to pyobject.
    * @return (optional) compatible type or UNKNOWN
    */
    inline python::Type unifyTypes(const python::Type& a,
                                   const python::Type& b,
                                   bool allowAutoUpcastOfNumbers=false,
                                   bool treatMissingDictKeysAsNone=false,
                                   bool allowUnifyWithPyObject=false) {
        using namespace std;

        // UNKNOWN types are not compatible
        if(a == python::Type::UNKNOWN || b == python::Type::UNKNOWN) {
            return python::Type::UNKNOWN;
        }

        if(a == b)
            return a;

        if((a == python::Type::PYOBJECT || b == python::Type::PYOBJECT))
            return allowUnifyWithPyObject ? python::Type::PYOBJECT : python::Type::UNKNOWN;

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

        // ---
        // structured dicts:
        if(aUnderlyingType.isStructuredDictionaryType() && bUnderlyingType.isDictionaryType()) {
            // are both of them structured dictionaries?
            if(aUnderlyingType.isStructuredDictionaryType() && bUnderlyingType.isStructuredDictionaryType()) {
                // ok, most complex unify setup --> need to check key pairs (independent of order!)

                auto a_pairs = aUnderlyingType.get_struct_pairs();
                auto b_pairs = bUnderlyingType.get_struct_pairs();

                // same number of elements? if not -> no cast possible!
                if(a_pairs.size() != b_pairs.size()) {
                    // treat missing as null?
                    if(!treatMissingDictKeysAsNone)
                        return python::Type::UNKNOWN;

                    // treat missing keys as null...
                    // => a bit more complex now
                    std::set<std::string> keys;
                    std::unordered_map<std::string, python::StructEntry> a_lookup;
                    std::unordered_map<std::string, python::StructEntry> b_lookup;
                    for(const auto& p : a_pairs) {
                        keys.insert(p.key);
                        a_lookup[p.key] = p;
                    }
                    for(const auto& p : b_pairs) {
                        keys.insert(p.key);
                        b_lookup[p.key] = p;
                    }

                    std::vector<python::StructEntry> uni_pairs;
                    uni_pairs.reserve(keys.size());
                    // go through both pair collections...
                    for(const auto& key : keys) {
                        python::StructEntry uni;
                        uni.key = key;

                        python::StructEntry a_pair;
                        a_pair.keyType = python::Type::NULLVALUE;
                        a_pair.valueType = python::Type::NULLVALUE;
                        python::StructEntry b_pair;
                        b_pair.keyType = python::Type::NULLVALUE;
                        b_pair.valueType = python::Type::NULLVALUE;
                        if(a_lookup.find(key) != a_lookup.end()) {
                            a_pair = a_lookup[key];
                        }
                        if(b_lookup.find(key) != b_lookup.end()) {
                            b_pair = b_lookup[key];
                        }

                        uni.keyType = unifyTypes(a_pair.keyType, b_pair.keyType, allowAutoUpcastOfNumbers,
                                                 treatMissingDictKeysAsNone, allowUnifyWithPyObject);
                        if(uni.keyType == python::Type::UNKNOWN)
                            return python::Type::UNKNOWN;
                        uni.valueType = unifyTypes(a_pair.valueType, b_pair.valueType, allowAutoUpcastOfNumbers,
                                                   treatMissingDictKeysAsNone, allowUnifyWithPyObject);
                        if(uni.valueType == python::Type::UNKNOWN)
                            return python::Type::UNKNOWN;
                        uni_pairs.push_back(uni);
                    }

                    // create combined struct type
                    return python::Type::makeStructuredDictType(uni_pairs);
                } else {
                    // go through pairs (note: they may be differently sorted, so sort first!)
                    std::sort(a_pairs.begin(), a_pairs.end(), [](const python::StructEntry& a, const python::StructEntry& b) {
                        return lexicographical_compare(a.key.begin(), a.key.end(), b.key.begin(), b.key.end());
                    });
                    std::sort(b_pairs.begin(), b_pairs.end(), [](const python::StructEntry& a, const python::StructEntry& b) {
                        return lexicographical_compare(a.key.begin(), a.key.end(), b.key.begin(), b.key.end());
                    });

                    // same size, check if keys are the same and types can be unified for each pair...
                   std::vector<python::StructEntry> uni_pairs;
                   for(unsigned i = 0; i < a_pairs.size(); ++i) {
                       if(a_pairs[i].key != b_pairs[i].key)
                           return python::Type::UNKNOWN;

                       python::StructEntry uni;
                       uni.key = a_pairs[i].key;
                       uni.keyType = unifyTypes(a_pairs[i].keyType, b_pairs[i].keyType, allowAutoUpcastOfNumbers,
                                                treatMissingDictKeysAsNone, allowUnifyWithPyObject);
                       if(uni.keyType == python::Type::UNKNOWN)
                           return python::Type::UNKNOWN;
                       uni.valueType = unifyTypes(a_pairs[i].valueType, b_pairs[i].valueType, allowAutoUpcastOfNumbers,
                                                treatMissingDictKeysAsNone, allowUnifyWithPyObject);
                       if(uni.valueType == python::Type::UNKNOWN)
                           return python::Type::UNKNOWN;
                       uni_pairs.push_back(uni);
                   }
                   return python::Type::makeStructuredDictType(uni_pairs);
                }
            } else {
                // easier: can unify if struct dict is homogenous when it comes to keys/values!
                // => do unify with pyobject?
                auto uni_key_type = unifyTypes(aUnderlyingType.keyType(), bUnderlyingType.keyType(),
                                               allowAutoUpcastOfNumbers, treatMissingDictKeysAsNone,
                                               allowUnifyWithPyObject);
                auto uni_value_type = unifyTypes(aUnderlyingType.valueType(), bUnderlyingType.valueType(),
                                               allowAutoUpcastOfNumbers, treatMissingDictKeysAsNone,
                                               allowUnifyWithPyObject);

                // if either is unknown -> can not unify
                if(uni_key_type == python::Type::UNKNOWN || uni_value_type == python::Type::UNKNOWN)
                    return python::Type::UNKNOWN;
                // else, create new dict structure of this!
                return python::Type::makeDictionaryType(uni_key_type, uni_value_type);
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