//
// Created by Leonhard Spiegelberg on 8/10/22.
//

#include <TypeHelper.h>
#include <set>
#include "StringUtils.h"

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
                // and unify them!
                std::set<std::string> unique_keys;
                std::unordered_map<std::string, python::StructEntry> a_map;
                std::unordered_map<std::string, python::StructEntry> b_map;
                for(auto a_pair : a_pairs) {
                    unique_keys.insert(a_pair.key);
                    a_map[a_pair.key] = a_pair;
                }
                for(auto b_pair : b_pairs) {
                    unique_keys.insert(b_pair.key);
                    b_map[b_pair.key] = b_pair;
                }

                // go through keys & unify -> check for policy
                std::vector<python::StructEntry> uni_pairs;
                for(const auto& key : unique_keys) {

                    python::StructEntry uni;
                    uni.key = key;

                    // both pairs present?
                    auto a_it = a_map.find(key);
                    auto b_it = b_map.find(key);

                    if(a_it != a_map.end() && b_it != b_map.end()) {
                        auto a_pair = a_it->second;
                        auto b_pair = b_it->second;
                        // if either is maybe present -> result is a maybe present
                        // but dicts can be always unified this way!
                        uni.alwaysPresent = a_pair.alwaysPresent && b_pair.alwaysPresent;

                        uni.keyType = unifyTypes(a_pair.keyType, b_pair.keyType, policy);
                        if(uni.keyType == python::Type::UNKNOWN)
                            return python::Type::UNKNOWN;
                        uni.valueType = unifyTypes(a_pair.valueType, b_pair.valueType, policy);
                        if(uni.valueType == python::Type::UNKNOWN)
                            return python::Type::UNKNOWN;
                    } else if(a_it != a_map.end()) {
                        // only a is present
                        if(!policy.unifyMissingDictKeys)
                            return python::Type::UNKNOWN;
                        uni.alwaysPresent = false;
                        uni.keyType = a_it->second.keyType;
                        uni.valueType = a_it->second.valueType;
                    } else {
                        assert(b_it != b_map.end());
                        // only b is present
                        if(!policy.unifyMissingDictKeys)
                            return python::Type::UNKNOWN;
                        uni.alwaysPresent = false;
                        uni.keyType = b_it->second.keyType;
                        uni.valueType = b_it->second.valueType;
                    }

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

    static python::Type unifyStructuredDictTypesEx(const python::Type& aUnderlyingType, const python::Type& bUnderlyingType,
                                                 const TypeUnificationPolicy& policy,
                                                 std::ostream& os) {
        using namespace std;
        assert(aUnderlyingType.isStructuredDictionaryType() && bUnderlyingType.isDictionaryType());

        // are both of them structured dictionaries?
        if(aUnderlyingType.isStructuredDictionaryType() && bUnderlyingType.isStructuredDictionaryType()) {
            // ok, most complex unify setup --> need to check key pairs (independent of order!)

            auto a_pairs = aUnderlyingType.get_struct_pairs();
            auto b_pairs = bUnderlyingType.get_struct_pairs();

            // same number of elements? if not -> no cast possible!
            if(a_pairs.size() != b_pairs.size()) {
                // treat missing as null or maybe pairs? if neither policy is set, can't unify
                if(!policy.treatMissingDictKeysAsNone && !policy.unifyMissingDictKeys) {
                    os<<"[INCOMPATIBLE element count] can not unify struct type of "<<pluralize(a_pairs.size(), "element")
                    <<" with struct type of "<<pluralize(b_pairs.size(), "element")<<", details: "<<aUnderlyingType.desc()<<" and "<<bUnderlyingType.desc()<<endl;
                    return python::Type::UNKNOWN;
                }

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

                        uni.keyType = unifyTypesEx(a_pair.keyType, b_pair.keyType, policy, os);
                        if(uni.keyType == python::Type::UNKNOWN) {
                            os<<"[INCOMPATIBLE keytype] Can not unify incompatible key types "<<a_pair.keyType.desc()<<" and "<<b_pair.keyType.desc()<<" for key="<<a_pair.key<<endl;
                            return python::Type::UNKNOWN;
                        }

                        uni.valueType = unifyTypes(a_pair.valueType, b_pair.valueType, policy);
                        if(uni.valueType == python::Type::UNKNOWN) {
                            os<<"[INCOMPATIBLE valuetype] Can not unify incompatible value types "<<a_pair.valueType.desc()<<" and "<<b_pair.valueType.desc()<<" for key="<<a_pair.key<<endl;
                            return python::Type::UNKNOWN;
                        }
                    } else if(policy.unifyMissingDictKeys) {
                        // are both present?
                        // -> unify!
                        if(a_lookup.find(key) != a_lookup.end() && b_lookup.find(key) != b_lookup.end()) {
                            auto a_pair = a_lookup[key];
                            auto b_pair = b_lookup[key];
                            // if either is maybe present -> result is a maybe present
                            // but dicts can be always unified this way!
                            uni.alwaysPresent = a_pair.alwaysPresent && b_pair.alwaysPresent;

                            uni.keyType = unifyTypesEx(a_pair.keyType, b_pair.keyType, policy, os);
                            if(uni.keyType == python::Type::UNKNOWN) {
                                os<<"[INCOMPATIBLE keytype] Can not unify incompatible key types "<<a_pair.keyType.desc()<<" and "<<b_pair.keyType.desc()<<" for key="<<a_pair.key<<endl;
                                return python::Type::UNKNOWN;
                            }
                            uni.valueType = unifyTypesEx(a_pair.valueType, b_pair.valueType, policy, os);
                            if(uni.valueType == python::Type::UNKNOWN) {
                                os<<"[INCOMPATIBLE valuetype] Can not unify incompatible value types "<<a_pair.valueType.desc()<<" and "<<b_pair.valueType.desc()<<" for key="<<a_pair.key<<endl;
                                return python::Type::UNKNOWN;
                            }
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
                // and unify them!
                std::set<std::string> unique_keys;
                std::unordered_map<std::string, python::StructEntry> a_map;
                std::unordered_map<std::string, python::StructEntry> b_map;
                for(auto a_pair : a_pairs) {
                    unique_keys.insert(a_pair.key);
                    a_map[a_pair.key] = a_pair;
                }
                for(auto b_pair : b_pairs) {
                    unique_keys.insert(b_pair.key);
                    b_map[b_pair.key] = b_pair;
                }

                // go through keys & unify -> check for policy
                std::vector<python::StructEntry> uni_pairs;
                for(const auto& key : unique_keys) {

                    python::StructEntry uni;
                    uni.key = key;

                    // both pairs present?
                    auto a_it = a_map.find(key);
                    auto b_it = b_map.find(key);

                    if(a_it != a_map.end() && b_it != b_map.end()) {
                        auto a_pair = a_it->second;
                        auto b_pair = b_it->second;
                        // if either is maybe present -> result is a maybe present
                        // but dicts can be always unified this way!
                        uni.alwaysPresent = a_pair.alwaysPresent && b_pair.alwaysPresent;

                        uni.keyType = unifyTypesEx(a_pair.keyType, b_pair.keyType, policy, os);
                        if(uni.keyType == python::Type::UNKNOWN) {
                            os<<"[INCOMPATIBLE keytype] Can not unify incompatible key types "<<a_pair.keyType.desc()<<" and "<<b_pair.keyType.desc()<<" for key="<<a_pair.key<<endl;
                            return python::Type::UNKNOWN;
                        }
                        uni.valueType = unifyTypesEx(a_pair.valueType, b_pair.valueType, policy, os);
                        if(uni.valueType == python::Type::UNKNOWN) {
                            os<<"[INCOMPATIBLE valuetype] Can not unify incompatible value types "<<a_pair.valueType.desc()<<" and "<<b_pair.valueType.desc()<<" for key="<<a_pair.key<<endl;
                            return python::Type::UNKNOWN;
                        }
                    } else if(a_it != a_map.end()) {
                        // only a is present
                        if(!policy.unifyMissingDictKeys)
                            return python::Type::UNKNOWN;
                        uni.alwaysPresent = false;
                        uni.keyType = a_it->second.keyType;
                        uni.valueType = a_it->second.valueType;
                    } else {
                        assert(b_it != b_map.end());
                        // only b is present
                        if(!policy.unifyMissingDictKeys)
                            return python::Type::UNKNOWN;
                        uni.alwaysPresent = false;
                        uni.keyType = b_it->second.keyType;
                        uni.valueType = b_it->second.valueType;
                    }

                    uni_pairs.push_back(uni);
                }
                return python::Type::makeStructuredDictType(uni_pairs);
            }
        } else {
            // easier: can unify if struct dict is homogenous when it comes to keys/values!
            // => do unify with pyobject?
            auto uni_key_type = unifyTypesEx(aUnderlyingType.keyType(), bUnderlyingType.keyType(),
                                           policy, os);
            auto uni_value_type = unifyTypesEx(aUnderlyingType.valueType(), bUnderlyingType.valueType(),
                                             policy, os);

            // if either is unknown -> can not unify
            if(uni_key_type == python::Type::UNKNOWN || uni_value_type == python::Type::UNKNOWN) {
                if(python::Type::UNKNOWN == uni_key_type) {
                    os<<"[INCOMPATIBLE keytype] can not unify key types "<<aUnderlyingType.keyType().desc()<<" and "
                      <<bUnderlyingType.keyType().desc()<<" for homogenous dict"<<endl;
                }
                if(python::Type::UNKNOWN == uni_value_type) {
                    os<<"[INCOMPATIBLE valuetype] can not unify value types "<<aUnderlyingType.valueType().desc()<<" and "
                      <<bUnderlyingType.valueType().desc()<<" for homogenous dict"<<endl;
                }
                return python::Type::UNKNOWN;
            }

            // else, create new dict structure of this!
            return python::Type::makeDictionaryType(uni_key_type, uni_value_type);
        }
    }

    python::Type unifyTypesEx(const python::Type& a,
                              const python::Type& b,
                              const TypeUnificationPolicy& policy,
                              std::ostream& os) {
        using namespace std;

        // UNKNOWN types are not compatible
        if(a == python::Type::UNKNOWN || b == python::Type::UNKNOWN) {
            os<<"[UNKNOWN] can not unify "<<a.desc()<<" with "<<b.desc()<<endl;
            return python::Type::UNKNOWN;
        }

        if(a == b)
            return a;

        if((a == python::Type::PYOBJECT || b == python::Type::PYOBJECT)) {
            if(!policy.allowUnifyWithPyObject) {
                os<<"[NO PYOBJECT] can not unify "<<a.desc()<<" with "<<b.desc()<<endl;
            }
            return policy.allowUnifyWithPyObject ? python::Type::PYOBJECT : python::Type::UNKNOWN;
        }


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
                os<<"[INCOMPATIBLE LIST ELEMENT] can not create unified list type from element types "<<aUnderlyingType.desc()<<" and "<<bUnderlyingType.desc()<<endl;
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
                os<<"[INCOMPATIBLE tuples] can not unify tuples "<<aUnderlyingType.desc()<< " and "
                  <<bUnderlyingType.desc()<<" because they have size "
                  <<aUnderlyingType.parameters().size()<<" and "<<bUnderlyingType.parameters().size()<<endl;
                return python::Type::UNKNOWN;
            }
            std::vector<python::Type> newTuple;
            for (size_t i = 0; i < aUnderlyingType.parameters().size(); i++) {
                python::Type newElementType = unifyTypes(aUnderlyingType.parameters()[i],
                                                         bUnderlyingType.parameters()[i],
                                                         policy);
                if(newElementType == python::Type::UNKNOWN) {

                    os<<"[INCOMPATIBLE tuple elements] can not unify tuples "<<aUnderlyingType.desc()
                      <<" and "<<bUnderlyingType.desc()<<" because element at position "<<i<<" of "
                      <<aUnderlyingType.parameters()[i].desc()<<" is incompatible with "<<bUnderlyingType.parameters()[i].desc()<<endl;

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
            return unifyStructuredDictTypesEx(aUnderlyingType, bUnderlyingType, policy, os);
        if(aUnderlyingType.isDictionaryType() && bUnderlyingType.isStructuredDictionaryType())
            return unifyStructuredDictTypesEx(bUnderlyingType, aUnderlyingType, policy, os);

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

        os<<"[GENERAL] no unification strategy could be applied for "<<a.desc()<<" and "<<b.desc()<<endl;
        // other non-supported types
        return python::Type::UNKNOWN;
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

    bool semantic_python_value_eq(const python::Type& type, const std::string& rhs, const std::string& lhs) {

        if(type.isOptionType()) {
            // are both null?
            if(rhs == "null" && lhs == "null")
                return true;
            if(rhs == "null")
                return false;
            if(lhs == "null")
                return false;
            return semantic_python_value_eq(type.getReturnType(), rhs, lhs);
        }

        // depending on type, compare semantically...
        if(type == python::Type::STRING) {
            auto e_value = str_value_from_python_raw_value(rhs);
            auto k_value = str_value_from_python_raw_value(lhs);
            return e_value == k_value;
        } else if(type == python::Type::BOOLEAN) {
            auto e_value = parseBoolString(rhs);
            auto k_value = parseBoolString(lhs);
            return e_value == k_value;
        } else if(type == python::Type::I64) {
            auto e_value = parseI64String(rhs);
            auto k_value = parseI64String(lhs);
            return e_value == k_value;
        } else if(type == python::Type::F64) {
            auto e_value = parseF64String(rhs);
            auto k_value = parseF64String(lhs);
            return double_eq(e_value, k_value);
        }

        // else, simple compare.
        return rhs == lhs;
    }
}
