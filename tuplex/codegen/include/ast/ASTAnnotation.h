//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ASTANNOTATION_H
#define TUPLEX_ASTANNOTATION_H

#include <Base.h>
#include <utility>
#include <functional>
#include <memory>
#include <TypeSystem.h>
#include <TypeHelper.h>
#include <Field.h>
#include <ExceptionCodes.h>

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

namespace tuplex {
    enum class SymbolType {
        TYPE,
        VARIABLE,
        FUNCTION,
        EXTERNAL_PYTHON // used when type annotator visitor should look up symbol
    };

    /*!
     * class for a symbol, i.e. a name that is bound to some type. Can be a variable, a type or a function.
     * Each symbol can have an arbitrary number of attributes, i.e. accessed via name.attribute
     * Module is seen as variable having type module.
     */
    class Symbol : public std::enable_shared_from_this<Symbol> {
     public:
        ///! the identifier under which this symbol is available
        std::string name;

        ///! the qualifiedName under which this symbol can be referenced by the compiler
        std::string qualifiedName;

        ///! a list of types for which this symbol can be typed for
        std::vector<python::Type> types;

        ///! type of the symbol (function, variable, type)
        SymbolType symbolType;

        ///! if the symbol is the attribute of another one, pointer to access the parent.
        std::shared_ptr<Symbol> parent;

        ///! an optional abstract typer function which can be applied if the symboltype is function
        std::function<python::Type(const python::Type&)> functionTyper;

        ///! an optional abstract typer that takes the original type of the caller (e.g., for an attribute)
        ///! and provides then together with the parameterType symilar to functionTyper a concrete type for the attribute function
        std::function<python::Type(const python::Type&,const python::Type&)> attributeFunctionTyper;

        ///! optionally constant data associated with that symbol
        tuplex::Field constantData;

        /*!
         * shortcut function which only works if there is a single type candidate. Do not use this function on function types.
         * @return unique type of this symbol. Works i.e. for variables or types.
         */
        inline python::Type type() const {
            // special case: if function symbol or not unique, return UNKNOWN.
            if(symbolType == SymbolType::FUNCTION || types.size() != 1)
                return python::Type::UNKNOWN;

            assert(symbolType != SymbolType::FUNCTION); // better not use it on functions...
            assert(types.size() == 1);
            return types.front();
        }

        /*!
         * add a new typing for this symbol. Each symbol may have multiple candidates.
         * @param type which typing to add
         * @return true if typing is not yet stored in this symbol, false otherwise.
         */
        inline bool addTypeIfNotExists(const python::Type& type) {
            auto it = std::find(types.begin(), types.end(), type);
            if(it == types.end()) {
                types.push_back(type);
                return true;
            }
            return false;
        }

        /*!
         * the typing of a function might depend on the concrete parameter type. To allow for this flexibility,
         * either in types several function candidates are stored of which the first matching will be picked or
         * an abstract typer function can be used.
         * @param parameterType
         * @param specializedFunctionType
         * @return true if a specialized function type could be generated, false else.
         */
        bool findFunctionTypeBasedOnParameterType(const python::Type& parameterType, python::Type& specializedFunctionType);

        /*!
         * creates a symbol with type based on the field's type
         * @param name under which name to register the constant
         * @param f the field from which to inherit the type
         * @return symbol ptr to be added somewhere
         */
        static std::shared_ptr<Symbol> makeConstant(const std::string& name, const tuplex::Field& f) {
            auto sym = std::make_shared<Symbol>(name, f.getType());
            sym->constantData = f;
            return sym;
        }

        /*!
         * in order to lookup concrete implementation of a function, it's helpful to get a fully
         * qualified name. E.g., os.path.join is a fully qualified name.
         * @return string with . separating individual identifiers/names of symbols.
         */
        inline std::string fullyQualifiedName() const {
            using namespace std;
            std::string full_name = qualifiedName;
            auto sym = parent;
            while(sym) {
                full_name = sym->qualifiedName + "." + full_name;
                sym = sym->parent;
            }
            return full_name;
        }

    Symbol() : functionTyper([](const python::Type&){return python::Type::UNKNOWN;}),
               attributeFunctionTyper([](const python::Type&, const python::Type&){return python::Type::UNKNOWN;}) {}
    virtual ~Symbol()  {
        _attributes.clear();
        parent.reset();
    }

        Symbol(const Symbol& other) = delete;
        Symbol& operator = (const Symbol& other) = delete;

        std::vector<std::shared_ptr<Symbol>> attributes() const { return _attributes; }

        /*!
         * retrieve symbol stored as attribute under name.
         * @param name attribute name
         * @return the symbol if found, nullptr otherwise
         */
        std::shared_ptr<Symbol> findAttribute(const std::string& name) {
            auto it = std::find_if(_attributes.begin(), _attributes.end(), [&](const std::shared_ptr<Symbol>& sym) {
                return sym->name == name;
            });
            if(it != _attributes.end())
                return *it;
            return nullptr;
        }

        /*!
         * the typing of an attribute which is a function may be based on both the callerType and the parameters. I.e.,
         * this function helps to type an attribute x.a(p) where callerType = type(x) and parameterType = type(p) for some
         * symbol a which is this.
         * @param callerType
         * @param parameterType
         * @param specializedFunctionType where to store the concrete (non-generic!) output type!
         * @return true if a specialized function type could be generated, false else.
         */
        inline bool findAttributeFunctionType(const python::Type& callerType,
                                              const python::Type& parameterType,
                                              python::Type& specializedFunctionType) {
            // fallback based typing:
            // 1. check attribute typer
            // 2. check general typer
            auto typed_result = attributeFunctionTyper(callerType, parameterType);
            if(python::Type::UNKNOWN == typed_result) {
                typed_result = functionTyper(parameterType);
            }
            // check if result is valid, then take it
            if(typed_result != python::Type::UNKNOWN) {
                specializedFunctionType = typed_result;
                assertFunctionDoesNotReturnGeneric(specializedFunctionType);
                return true;
            }
            // typer did not yield a result, hence try stored funciton types incl. upcasting
            return findStoredTypedFunction(parameterType, specializedFunctionType);
        }

        /*!
         * add given symbol as new attribute. Will alter internal pointers.
         * @param attribute
         */
        void addAttribute(const std::shared_ptr<Symbol>& attribute) {
            assert(attribute.get() != this);
            if(!attribute)
                return;

            // check if exists, if not add!
            auto sym = findAttribute(attribute->name);
            if(sym) {
                for(const auto& t : attribute->types)
                    sym->addTypeIfNotExists(t);
            }

            attribute->parent = shared_from_this();
            _attributes.push_back(attribute);
        }

    Symbol(std::string _name,
           std::function<python::Type(const python::Type&)> typer) : name(_name), qualifiedName(_name),
           functionTyper(std::move(typer)), attributeFunctionTyper([](const python::Type&, const python::Type&){return python::Type::UNKNOWN;}), symbolType(SymbolType::FUNCTION) {}

    Symbol(std::string _name, python::Type _type) : name(_name), qualifiedName(_name),
    types{_type},
    symbolType(_type.isFunctionType() ? SymbolType::FUNCTION : SymbolType::VARIABLE),
    functionTyper([](const python::Type&) { return python::Type::UNKNOWN; }),
    attributeFunctionTyper([](const python::Type&, const python::Type&){return python::Type::UNKNOWN;}) {}

    Symbol(std::string _name, std::string _qualifiedName, python::Type _type, SymbolType _symbolType) : name(_name),
    qualifiedName(_qualifiedName),
    types{_type},
    symbolType(_symbolType),
    functionTyper([](const python::Type&) { return python::Type::UNKNOWN; }),
    attributeFunctionTyper([](const python::Type&, const python::Type&){return python::Type::UNKNOWN;}) {}

    #ifdef BUILD_WITH_CEREAL
        template <class Archive>
        void serialize(Archive &ar) { ar(name, qualifiedName, types, symbolType, parent, constantData); }
    #endif
     private:
        ///! i.e. to store something like re.search. re is then of module type. search will have a concrete function type.
        std::vector<std::shared_ptr<Symbol>> _attributes;

        /********* HELPER FUNCTIONS *************/

    inline bool findStoredTypedFunction(const python::Type& parameterType, python::Type& specializedFunctionType) {

        // typing using typer functions above failed, hence now search for concrete stored types.
        for(auto& type : types) {
            // found symbol, now check its type
            if(!type.isFunctionType())
                continue;

            auto tupleArgType = getTupleArg(type.getParamsType());

            // check if there's a direct type match => use that function then!
            if(parameterType == tupleArgType) {
                specializedFunctionType = type;
                assertFunctionDoesNotReturnGeneric(specializedFunctionType);
                return true;
            }
        }

        // no direct match was found. Check whether casting would work or partial matching.
        for(auto& type : types) {
            // found symbol, now check its type
            if (!type.isFunctionType())
                continue;

            auto tupleArgType = getTupleArg(type.getParamsType());

            // check if given parameters type is compatible with function type?
            // actual invocation is with parameterType
            // ==> can one upcast them to fit the defined one OR does is partially work?
            // e.g., when the function is defined for NULL, but we have opt?
            if (isTypeCompatible(parameterType, tupleArgType)) {
                specializedFunctionType = type;

                // specialize according to parameterType if it's a generic function so further typing works
                assert(!specializedFunctionType.getReturnType().isGeneric());
                if(specializedFunctionType.getParamsType().isGeneric()) {
                    auto specializedParams = python::specializeGenerics(parameterType, tupleArgType);
                    specializedFunctionType = python::Type::makeFunctionType(specializedParams,
                                                                             specializedFunctionType.getReturnType());
                }

                assertFunctionDoesNotReturnGeneric(specializedFunctionType);
                return true;
            }
        }

        return false;
    }

    /*!
     * helper function to check for compatibility, i.e. whether from type can be cast to to type.
     * @param from source type
     * @param to target type
     * @return whether types are compatibility, i.e. there's some conversion available.
     */
    inline bool isTypeCompatible(const python::Type& from, const python::Type& to) {

            // any is compatible to any other type!
            if(from == python::Type::ANY || to == python::Type::ANY) {
                // internal check: can't have both types any! that sounds bad!
                assert(from != python::Type::ANY || to != python::Type::ANY);
                return true;
            }

            // is one of them option type, one not?

            // check whether upcasting is an option for primitives?
            if(from.isPrimitiveType() && to.isPrimitiveType() && !from.isOptionType() && !to.isOptionType()) {
                return python::canUpcastType(from, to);
            }

            // from is null, and to is opt?
            if((from == python::Type::NULLVALUE && to.isOptionType()) || (to == python::Type::NULLVALUE && from.isOptionType()))
                return true;

            // one is opt, one not
            if(from.isOptionType() && !to.isOptionType())
                return isTypeCompatible(from.elementType(), to);
            if(!from.isOptionType() && to.isOptionType())
                return isTypeCompatible(from, to.elementType());
            // both opt
            if(from.isOptionType() && to.isOptionType())
                return isTypeCompatible(from.elementType(), to.elementType());

            // list? check for elements!
            if(from.isListType() && to.isListType()) {
                if(from != python::Type::EMPTYLIST && to != python::Type::EMPTYLIST &&
                    from != python::Type::GENERICLIST && to != python::Type::GENERICLIST)
                    return isTypeCompatible(from.elementType(), to.elementType());
            }

            // dict? check for elements
            if(from.isDictionaryType() && to.isDictionaryType()) {
                if(from != python::Type::EMPTYDICT && to != python::Type::EMPTYDICT &&
                    from != python::Type::GENERICDICT && to != python::Type::GENERICDICT)
                    return isTypeCompatible(from.keyType(), to.keyType()) && isTypeCompatible(from.valueType(), to.valueType());
            }

            // tuple? recurse
            if(from.isTupleType() && to.isTupleType()) {

                // this looks weird, maybe change one day.
                assert(from != python::Type::GENERICTUPLE && to != python::Type::GENERICTUPLE);

                if(from.parameters().size() != to.parameters().size())
                    return false;

                // check all elements of the tuple are type compatible.
                auto numElements = from.parameters().size();
                for(auto i = 0; i < numElements; ++i)
                    if(!isTypeCompatible(from.parameters()[i], to.parameters()[i]))
                        return false;
                return true;
            }

            // generics
            // => note: generic tuple is special!
            if((to.isTupleType() && from == python::Type::GENERICTUPLE) ||
                (to == python::Type::GENERICTUPLE && from.isTupleType()))
                return true;

            if((to.isDictionaryType() && from == python::Type::GENERICDICT) ||
                (to == python::Type::GENERICDICT && from.isDictionaryType()))
                return true;

            if((to.isListType() && from == python::Type::GENERICLIST) ||
                (to == python::Type::GENERICLIST && from.isListType()))
                return true;


            if(to == python::Type::PYOBJECT || from == python::Type::PYOBJECT)
                return true;

            return false;
        }

        inline bool typeBasedOnParameterType(const python::Type& parameterType, python::Type& specializedFunctionType) {
            // check if typer function is there?
            auto generic_result = functionTyper(parameterType);
            if(generic_result != python::Type::UNKNOWN) {
                specializedFunctionType = generic_result;

                assertFunctionDoesNotReturnGeneric(specializedFunctionType);
                return true;
            }

            for(auto& type : types) {
                // found symbol, now check its type
                if(!type.isFunctionType())
                    continue;

                auto tupleArgType = getTupleArg(type.getParamsType());

                // check if there's a direct type match => use that function then!
                if(parameterType == tupleArgType) {
                    specializedFunctionType = type;
                    assertFunctionDoesNotReturnGeneric(specializedFunctionType);
                    return true;
                }
            }

            // no direct match was found. Check whether casting would work or partial matching.
            for(auto& type : types) {
                // found symbol, now check its type
                if (!type.isFunctionType())
                    continue;

                auto tupleArgType = getTupleArg(type.getParamsType());

                // check if given parameters type is compatible with function type?
                // actual invocation is with parameterType
                // ==> can we upcast them to fit the defined one OR does is partially work?
                // e.g., when the function is defined for NULL, but we have opt?
                if (isTypeCompatible(parameterType, tupleArgType)) {
                    specializedFunctionType = type;

                    // specialize according to parameterType if it's a generic function so further typing works
                    assert(!specializedFunctionType.getReturnType().isGeneric());
                    if(specializedFunctionType.getParamsType().isGeneric()) {
                        auto specializedParams = python::specializeGenerics(parameterType, tupleArgType);
                        specializedFunctionType = python::Type::makeFunctionType(specializedParams,
                                                                                 specializedFunctionType.getReturnType());
                    }

                    assertFunctionDoesNotReturnGeneric(specializedFunctionType);
                    return true;
                }
            }

            return false;
        }

        /*!
         * check that function type is not using generics as return type
         * @param specializedFunctionType
         */
        inline void assertFunctionDoesNotReturnGeneric(const python::Type& specializedFunctionType) {
            // return type of function can't be generic
            assert(specializedFunctionType.isFunctionType());
            assert(specializedFunctionType.getReturnType() != python::Type::PYOBJECT &&
                specializedFunctionType.getReturnType() != python::Type::GENERICTUPLE &&
                specializedFunctionType.getReturnType() != python::Type::GENERICLIST
                // && specializedFunctionType.getReturnType() != python::Type::GENERICDICT
                );
        }

        /*!
         * tuples are used to encode function arguments. This may clash with generics, this function extracts
         * the correct tuple representation if users submit functions like i64 -> i64, i.e. it ensures that the paramsType
         * of a function type is ALWAYS a tuple type (or generictuple for a vararg function).
         * @param type parameter type of a function type.
         * @return a tuple type which encodes the parameter type properly.
         */
        inline python::Type getTupleArg(const python::Type& type) {
            // this here is a bit tricky because we convert the type from a function
            // => generictuple or emptytuple need to be treated special, else propagating to tuple type is fine
            if(type == python::Type::GENERICTUPLE || type == python::Type::EMPTYTUPLE)
                return type;

            return python::Type::propagateToTupleType(type);
        }
    };

    /*!
     * iterator-specific annotation for NIdentifier (identifiers with iteratorType) and NCall (iterator related function calls including iter(), zip(), enumerate(), next())
     * For an iterator generating NCall (iter(), zip() or enumerate()), its IteratorInfo saves info about the current call.
     * For an NIdentifier with _name=x, its IteratorInfo reveals how x was generated.
     * For NCall next() with _positionalArguments=x, its IteratorInfo is the same as x's.
     * Example:
     * x = iter("abcd") // both NIdentifier x and NCall iter() are annotated with *info1 = {"iter", str, {nullptr})}
     * y = zip(x, [1, 2]) // both NIdentifier y and NCall zip() are annotated with *info3 = {"zip", (Iterator[str], [I64]), {info1, info2}} where *info2 = {"iter", [I64], {nullptr}} since zip() implicitly converts any non-iteratorType member to an iterator
     * z = next(y) // NCall next() is annotated with info4 = info3
     */
    struct IteratorInfo {
        std::string iteratorName; // from which built-in function the iterator was generated, currently can be "iter", "zip", "enumerate".
        python::Type argsType; // concrete type of arguments of the iterator generating function.
        std::vector<std::shared_ptr<IteratorInfo>> argsIteratorInfo; // pointers to IteratorInfo of each argument.

        void deoptimize() {
            argsType = tuplex::deoptimizedType(argsType);
            for(auto& info : argsIteratorInfo) {
                if(info.get() == this)
                    continue;
                info->deoptimize();
            }
        }
    };

    // simple class used to annotate ast nodes
    class ASTAnnotation {
     public:

        ASTAnnotation() : numTimesVisited(0), symbol(nullptr), iMin(0), iMax(0), negativeValueCount(0), positiveValueCount(0), iteratorInfo(nullptr), typeStableCount(0), typeChangedAndStableCount(0), typeChangedAndUnstableCount(0), zeroIterationCount(0), deoptException(ExceptionCode::SUCCESS) {}
        ASTAnnotation(const ASTAnnotation& other) : numTimesVisited(other.numTimesVisited), iMin(other.iMin), iMax(other.iMax),
                                                    negativeValueCount(other.negativeValueCount),
                                                    positiveValueCount(other.positiveValueCount),
                                                    symbol(other.symbol), types(other.types),
                                                    iteratorInfo(other.iteratorInfo),
                                                    branchTakenSampleIndices(other.branchTakenSampleIndices),
                                                    typeStableCount(other.typeStableCount),
                                                    typeChangedAndStableCount(other.typeChangedAndStableCount),
                                                    typeChangedAndUnstableCount(other.typeChangedAndUnstableCount),
                                                    zeroIterationCount(other.zeroIterationCount),
                                                    deoptException(other.deoptException),
                                                    originalColumnName(other.originalColumnName) {}

        ///! how often was node visited? Helpful annotation for if-branches
        size_t numTimesVisited;
        ///! annotation for an if->_then or if->_else node: indices of samples that have taken this then/else branch
        std::set<size_t> branchTakenSampleIndices;

        ///! for integer/double nodes what is min/max range? => can be used for compression
        union {
            int64_t iMin;
            double dMin;
        };
        union {
            int64_t iMax;
            double dMax;
        };

        size_t negativeValueCount;
        size_t positiveValueCount;

        ExceptionCode deoptException;

        ///! assigning a symbol to an ASTNode and storing it makes codegeneration easier.
        std::shared_ptr<Symbol> symbol;

        ///! traced types
        std::vector<python::Type> types;

        ///! iterator-specific info
        std::shared_ptr<IteratorInfo> iteratorInfo;

        // type-stability example:
        // --------------------
        // x = 0
        // for i in range(100):
        //     x += 1
        //     x += 1.0
        // --------------------
        // type of x changed from i64 to f64 in the first iteration, but it remains f64 for the rest of the loop -> types are stable in this loop
        // --------------------
        // x = 0
        // for i in range(100):
        //     if i % 2 == 0:
        //         x = 'n'
        //     else:
        //         x = 1.0
        // --------------------
        // type of x changed in the first iteration, and are changing in the rest of the loop -> types are unstable in this loop
        ///! annotation for analyzing type-stability
        size_t typeStableCount; // types are stable from the first to second last iteration (since type change in the last iteration is not a problem)
        size_t typeChangedAndStableCount; // types changed in the first iteration but remain stable from the second iteration to the end
        size_t typeChangedAndUnstableCount; // types changed in the first iteration and changed again during the second iteration to the end
        size_t zeroIterationCount; // how many times the loop body was skipped (e.g. loop body is skipped in "for i in range(0)")

        //! stores stabilized types when first iteration of loop gets unrolled to maintain type stability
        std::unordered_map<std::string, python::Type> stabilizedTypes; // key: variable name, value: type of the variable at the end of the loop

        std::string originalColumnName; // this is a dummy before introducing a proper "row" dummy type, use this for rewriting

        inline python::Type majorityType() const {
            if(types.empty())
                return python::Type::UNKNOWN;

            std::unordered_map<python::Type, int> counts;
            for(const auto &t : types) {
                if(counts.find(t) == counts.end())
                    counts[t] = 0;
                counts[t]++;
            }
            int count = 0;
            python::Type t = types.front();
            for(const auto &kv : counts) {
                if(kv.second >= count) {
                    count = kv.second;
                    t = kv.first;
                }
            }
            return t;
        }

    #ifdef BUILD_WITH_CEREAL
        template <class Archive>
        void serialize(Archive &ar) { ar(numTimesVisited, iMin, iMax, negativeValueCount,
                                         positiveValueCount, symbol, types, deoptException, originalColumnName); }
    #endif

    };

}

#endif //TUPLEX_ASTANNOTATION_H
