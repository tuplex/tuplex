//
// Created by Leonhard Spiegelberg on 8/10/22.
//

#include <TypeHelper.h>

namespace tuplex {

    // helper function to deal with struct dict types only
    static python::Type unifyStructuredDictTypes(const python::Type& aUnderlyingType, const python::Type& bUnderlyingType,
                                                 const TypeUnificationPolicy& policy) {
        assert(aUnderlyingType.isStructuredDictionaryType() && bUnderlyingType.isDictionaryType());

        // are both of them structured dictionaries?
        if(aUnderlyingType.isStructuredDictionaryType() && bUnderlyingType.isStructuredDictionaryType()) {
            // ok, most complex unify setup --> need to check key pairs (independent of order!)

            auto a_pairs = aUnderlyingType.get_struct_pairs();
            auto b_pairs = bUnderlyingType.get_struct_pairs();

            // same number of elements? if not -> no cast possible!
            if(a_pairs.size() != b_pairs.size()) {
                // treat missing as null or maybe pairs? if neither policy is set, can't unify
                if(!policy.treatMissingDictKeysAsNone && !policy.unifyMissingDictKeys)
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

                    assert(policy.treatMissingDictKeysAsNone || policy.unifyMissingDictKeys); // one must be true
                    // treat missing values as NULL
                    if(policy.treatMissingDictKeysAsNone)  { // this has precendence over unifyMissingDictKeys!
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

                        // if either is maybe present -> result is a maybe present
                        // but dicts can be always unified this way!
                        uni.alwaysPresent = a_pair.alwaysPresent && b_pair.alwaysPresent;

                        uni.keyType = unifyTypes(a_pair.keyType, b_pair.keyType, policy);
                        if(uni.keyType == python::Type::UNKNOWN)
                            return python::Type::UNKNOWN;
                        uni.valueType = unifyTypes(a_pair.valueType, b_pair.valueType, policy);
                        if(uni.valueType == python::Type::UNKNOWN)
                            return python::Type::UNKNOWN;
                    } else if(policy.unifyMissingDictKeys) {
                        // are both present?
                        // -> unify!
                        if(a_lookup.find(key) != a_lookup.end() && b_lookup.find(key) != b_lookup.end()) {
                            auto a_pair = a_lookup[key];
                            auto b_pair = b_lookup[key];
                            // if either is maybe present -> result is a maybe present
                            // but dicts can be always unified this way!
                            uni.alwaysPresent = a_pair.alwaysPresent && b_pair.alwaysPresent;

                            uni.keyType = unifyTypes(a_pair.keyType, b_pair.keyType, policy);
                            if(uni.keyType == python::Type::UNKNOWN)
                                return python::Type::UNKNOWN;
                            uni.valueType = unifyTypes(a_pair.valueType, b_pair.valueType, policy);
                            if(uni.valueType == python::Type::UNKNOWN)
                                return python::Type::UNKNOWN;
                        } else if(a_lookup.find(key) != a_lookup.end()) {
                            // only a is present
                            uni = a_lookup[key];
                            uni.alwaysPresent = false; // set to maybe
                        } else {
                            // b must be present
                            assert(b_lookup.find(key) != b_lookup.end());
                            // only b is present
                            uni = b_lookup[key];
                            uni.alwaysPresent = false; // set to maybe
                        }
                    }

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
                    // if either is maybe present -> result is a maybe present
                    // but dicts can be always unified this way!
                    uni.alwaysPresent = a_pairs[i].alwaysPresent && b_pairs[i].alwaysPresent;

                    uni.keyType = unifyTypes(a_pairs[i].keyType, b_pairs[i].keyType, policy);
                    if(uni.keyType == python::Type::UNKNOWN)
                        return python::Type::UNKNOWN;
                    uni.valueType = unifyTypes(a_pairs[i].valueType, b_pairs[i].valueType, policy);
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
                                           policy);
            auto uni_value_type = unifyTypes(aUnderlyingType.valueType(), bUnderlyingType.valueType(),
                                             policy);

            // if either is unknown -> can not unify
            if(uni_key_type == python::Type::UNKNOWN || uni_value_type == python::Type::UNKNOWN)
                return python::Type::UNKNOWN;
            // else, create new dict structure of this!
            return python::Type::makeDictionaryType(uni_key_type, uni_value_type);
        }
    }


    python::Type unifyTypes(const python::Type& a,
                            const python::Type& b,
                            const TypeUnificationPolicy& policy) {
        using namespace std;

        // UNKNOWN types are not compatible
        if(a == python::Type::UNKNOWN || b == python::Type::UNKNOWN) {
            return python::Type::UNKNOWN;
        }

        if(a == b)
            return a;

        if((a == python::Type::PYOBJECT || b == python::Type::PYOBJECT))
            return policy.allowUnifyWithPyObject ? python::Type::PYOBJECT : python::Type::UNKNOWN;

        // special case: optimized types!
        // -> @TODO: can unify certain types, else just deoptimize.
        if(a.isOptimizedType() || b.isOptimizedType())
            return unifyTypes(deoptimizedType(a), deoptimizedType(b),
                              policy);

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
        if(policy.allowAutoUpcastOfNumbers) {
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
        if(aUnderlyingType.isListType()
        && bUnderlyingType.isListType()
        && aUnderlyingType != python::Type::EMPTYLIST
        && bUnderlyingType != python::Type::EMPTYLIST) {
            python::Type newElementType = unifyTypes(aUnderlyingType.elementType(),
                                                     bUnderlyingType.elementType(),
                                                     policy);
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
                                                         bUnderlyingType.parameters()[i],
                                                         policy);
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

        // ---
        // structured dicts:
        if(aUnderlyingType.isStructuredDictionaryType() && bUnderlyingType.isDictionaryType())
            return unifyStructuredDictTypes(aUnderlyingType, bUnderlyingType, policy);
        if(aUnderlyingType.isDictionaryType() && bUnderlyingType.isStructuredDictionaryType())
            return unifyStructuredDictTypes(bUnderlyingType, aUnderlyingType, policy);

        // other dictionary types
        if(aUnderlyingType.isDictionaryType() && bUnderlyingType.isDictionaryType()) {
            auto key_t = unifyTypes(aUnderlyingType.keyType(), bUnderlyingType.keyType(), policy);
            auto val_t = unifyTypes(aUnderlyingType.valueType(), bUnderlyingType.valueType(), policy);
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
    }
}