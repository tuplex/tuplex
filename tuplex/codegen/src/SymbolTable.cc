//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <SymbolTable.h>
#include <TypeSystem.h>
#include <Logger.h>
#include <queue>

using namespace std;

namespace tuplex {
    std::shared_ptr<SymbolTable> SymbolTable::createFromEnvironment(const tuplex::ClosureEnvironment *globals) {
        // Note: refactored functionality from SymbolTableVisitor.{h,cc}
        auto table = new SymbolTable();
        if(!table)
            return nullptr;

        // add python builtins
        table->beginScope(ScopeType::BUILTIN, "builtins");
        table->addBuiltins();

        if(globals) {
            table->beginScope(ScopeType::GLOBAL, "closure");
            if(!globals->addToTable(*table))
                return nullptr;
        }

        table->beginScope(ScopeType::LOCAL, "module");

        return std::shared_ptr<SymbolTable>(table);
    }

    void SymbolTable::addBuiltins() {

        // add here types for functions that are known

        // builtin functions
        // they all have special behaviour
        //abs()	delattr()	hash()	memoryview()	set()
        //all()	dict()	help()	min()	setattr()
        //any()	dir()	hex()	next()	slice()
        //ascii()	divmod()	id()	object()	sorted()
        //bin()	enumerate()	input()	oct()	staticmethod()
        //bool()	eval()	int()	open()	str()
        //breakpoint()	exec()	isinstance()	ord()	sum()
        //bytearray()	filter()	issubclass()	pow()	super()
        //bytes()	float()	iter()	print()	tuple()
        //callable()	format()	len()	property()	type()
        //chr()	frozenset()	list()	range()	vars()
        //classmethod()	getattr()	locals()	repr()	zip()
        //compile()	globals()	map()	reversed()	__import__()
        //complex()	hasattr()	max()	round()
        // not all of them will be implemented, some just do not make sense...

        // current Scope type should be global
        assert(currentScopeType() == SymbolTable::ScopeType::BUILTIN);

        // Note: in the future, we might want to support type objects.
        // i.e. str, int, dict could be also referring to a type object when being used e.g.
        // in x = str
        // => this is not yet supported!

        // then, we could do things like
        // def f(x):
        //    t = str
        //    return t(x)

        // global functions
        addSymbol("dict", python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::GENERICDICT));

        addSymbol("int", python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::I64));
        addSymbol("int", python::Type::makeFunctionType(python::Type::BOOLEAN, python::Type::I64));
        addSymbol("int", python::Type::makeFunctionType(python::Type::I64, python::Type::I64));
        addSymbol("int", python::Type::makeFunctionType(python::Type::F64, python::Type::I64));
        addSymbol("int", python::Type::makeFunctionType(python::Type::STRING, python::Type::I64));

        addSymbol("float", python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::F64));
        addSymbol("float", python::Type::makeFunctionType(python::Type::BOOLEAN, python::Type::F64));
        addSymbol("float", python::Type::makeFunctionType(python::Type::I64, python::Type::F64));
        addSymbol("float", python::Type::makeFunctionType(python::Type::F64, python::Type::F64));
        addSymbol("float", python::Type::makeFunctionType(python::Type::STRING, python::Type::F64));

        addSymbol("bool", python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::BOOLEAN));
        addSymbol("bool", python::Type::makeFunctionType(python::Type::BOOLEAN, python::Type::BOOLEAN));
        addSymbol("bool", python::Type::makeFunctionType(python::Type::I64, python::Type::BOOLEAN));
        addSymbol("bool", python::Type::makeFunctionType(python::Type::F64, python::Type::BOOLEAN));
        addSymbol("bool", python::Type::makeFunctionType(python::Type::STRING, python::Type::BOOLEAN));

        addSymbol("str", python::Type::makeFunctionType(python::Type::NULLVALUE, python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::makeTupleType({python::Type::EMPTYTUPLE}),
                                                        python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::EMPTYDICT, python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::BOOLEAN, python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::I64, python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::F64, python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::STRING, python::Type::STRING));

        // tuple means an arbitrary number of elements
        // (tuple) means a single arbitrary tuple!

        // @TODO: genericdict, generictuple
        // string is special, because it also works for options!
        addSymbol("str", python::Type::makeFunctionType(python::Type::makeOptionType(python::Type::NULLVALUE),
                                                        python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(
                python::Type::makeOptionType(python::Type::makeTupleType({python::Type::EMPTYTUPLE})),
                python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::makeOptionType(python::Type::EMPTYDICT),
                                                        python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::makeOptionType(python::Type::BOOLEAN),
                                                        python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::makeOptionType(python::Type::I64),
                                                        python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::makeOptionType(python::Type::F64),
                                                        python::Type::STRING));
        addSymbol("str", python::Type::makeFunctionType(python::Type::makeOptionType(python::Type::STRING),
                                                        python::Type::STRING));

        addSymbol("len", python::Type::makeFunctionType(python::Type::STRING, python::Type::I64));
        auto n = findSymbol(std::string("len"))->fullyQualifiedName();
        addSymbol("len", python::Type::makeFunctionType(python::Type::makeTupleType({python::Type::GENERICTUPLE}), python::Type::I64));
        addSymbol("len", python::Type::makeFunctionType(python::Type::makeTupleType({python::Type::GENERICDICT}), python::Type::I64));
        addSymbol("len", python::Type::makeFunctionType(python::Type::makeTupleType({python::Type::GENERICLIST}), python::Type::I64));

        addSymbol("abs", python::Type::makeFunctionType(python::Type::BOOLEAN,
                                                        python::Type::I64)); // note that abs converts to i64
        addSymbol("abs", python::Type::makeFunctionType(python::Type::I64, python::Type::I64));
        addSymbol("abs", python::Type::makeFunctionType(python::Type::F64, python::Type::F64));

        // use functionTyper to dynamically infer function type for iterator-related functions (currently: iter, zip, enumerate, reversed, next)
        auto iterFunctionTyper = [this](const python::Type& parameterType) {

            if(parameterType.parameters().size() != 1) {
                // iter() currently supports single iterable as arguments only
                return python::Type::makeFunctionType(parameterType, python::Type::UNKNOWN);
            }

            auto iterableType = parameterType.parameters().front();

            if(iterableType.isIteratorType()) {
                // iter(iteratorType) simply returns iteratorType back
                return python::Type::makeFunctionType(parameterType, iterableType);
            }

            if(iterableType.isListType()) {
                if(iterableType == python::Type::EMPTYLIST) {
                    return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
                }
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(iterableType.elementType()));
            }

            if(iterableType == python::Type::STRING) {
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::STRING));
            }

            if(iterableType == python::Type::RANGE) {
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::I64));
            }

            if(iterableType.isDictionaryType()) {
                addCompileError(CompileError::TYPE_ERROR_ITER_CALL_WITH_DICTIONARY);
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(iterableType.keyType()));
            }

            if(iterableType.isTupleType()) {
                if(iterableType == python::Type::EMPTYTUPLE) {
                    return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
                }
                if(!tupleElementsHaveSameType(iterableType)) {
                    addCompileError(CompileError::TYPE_ERROR_ITER_CALL_WITH_NONHOMOGENEOUS_TUPLE);
                }
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(iterableType.parameters().front()));
            }
            return python::Type::makeFunctionType(parameterType, python::Type::UNKNOWN);
        };

        auto reversedFunctionTyper = [this](const python::Type& parameterType) {

            if(parameterType.parameters().size() != 1) {
                // reversed takes exactly one argument
                return python::Type::makeFunctionType(parameterType, python::Type::UNKNOWN);
            }

            auto sequenceType = parameterType.parameters().front();

            if(sequenceType.isListType()) {
                if(sequenceType == python::Type::EMPTYLIST) {
                    return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
                }
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(sequenceType.elementType()));
            }

            if(sequenceType == python::Type::STRING) {
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::STRING));
            }

            if(sequenceType == python::Type::RANGE) {
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::I64));
            }

            if(sequenceType.isDictionaryType()) {
                addCompileError(CompileError::TYPE_ERROR_ITER_CALL_WITH_DICTIONARY);
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(sequenceType.keyType()));
            }

            if(sequenceType.isTupleType()) {
                if(sequenceType == python::Type::EMPTYTUPLE) {
                    return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
                }
                if(!tupleElementsHaveSameType(sequenceType)) {
                    addCompileError(CompileError::TYPE_ERROR_ITER_CALL_WITH_NONHOMOGENEOUS_TUPLE);
                }
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(sequenceType.parameters().front()));
            }
            return python::Type::makeFunctionType(parameterType, python::Type::UNKNOWN);
        };

        auto zipFunctionTyper = [this](const python::Type& parameterType) {

            if(parameterType.parameters().empty()) {
                return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
            }

            // construct yield type tuple
            std::vector<python::Type> types;
            for (const auto& iterableType : parameterType.parameters()) {
                if(iterableType.isIteratorType()) {
                    if(iterableType == python::Type::EMPTYITERATOR) {
                        return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
                    }
                    types.push_back(iterableType.yieldType());
                } else if(iterableType.isListType()) {
                    if(iterableType == python::Type::EMPTYLIST) {
                        return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
                    }
                    types.push_back(iterableType.elementType());
                } else if(iterableType == python::Type::STRING) {
                    types.push_back(python::Type::STRING);
                } else if(iterableType == python::Type::RANGE) {
                    types.push_back(python::Type::I64);
                } else if(iterableType.isDictionaryType()) {
                    addCompileError(CompileError::TYPE_ERROR_ITER_CALL_WITH_DICTIONARY);
                    types.push_back(iterableType.keyType());
                } else if(iterableType.isTupleType()) {
                    if(iterableType == python::Type::EMPTYTUPLE) {
                        return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
                    }
                    if(!tupleElementsHaveSameType(iterableType)) {
                        addCompileError(CompileError::TYPE_ERROR_ITER_CALL_WITH_NONHOMOGENEOUS_TUPLE);
                    }
                    types.push_back(iterableType.parameters().front());
                } else {
                    return python::Type::makeFunctionType(parameterType, python::Type::UNKNOWN);
                }
            }

            return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::makeTupleType(types)));
        };

        auto enumerateFunctionTyper = [this](const python::Type& parameterType) {
            if(parameterType.parameters().size() != 1 && parameterType.parameters().size() != 2) {
                return python::Type::makeFunctionType(parameterType, python::Type::UNKNOWN);
            }

            auto iterableType = parameterType.parameters().front();
            if(iterableType.isIteratorType()) {
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::makeTupleType({python::Type::I64, iterableType.yieldType()})));
            }

            if(iterableType.isListType()) {
                if(iterableType == python::Type::EMPTYLIST) {
                    return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
                }
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::makeTupleType({python::Type::I64, iterableType.elementType()})));
            }

            if(iterableType == python::Type::STRING) {
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::makeTupleType({python::Type::I64, python::Type::STRING})));
            }

            if(iterableType == python::Type::RANGE) {
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::makeTupleType({python::Type::I64, python::Type::I64})));
            }

            if(iterableType.isDictionaryType()) {
                addCompileError(CompileError::TYPE_ERROR_ITER_CALL_WITH_DICTIONARY);
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::makeTupleType({python::Type::I64, iterableType.keyType()})));
            }

            if(iterableType.isTupleType()) {
                if(iterableType == python::Type::EMPTYTUPLE) {
                    return python::Type::makeFunctionType(parameterType, python::Type::EMPTYITERATOR);
                }
                if(!tupleElementsHaveSameType(iterableType)) {
                    addCompileError(CompileError::TYPE_ERROR_ITER_CALL_WITH_NONHOMOGENEOUS_TUPLE);
                }
                return python::Type::makeFunctionType(parameterType, python::Type::makeIteratorType(python::Type::makeTupleType({python::Type::I64, iterableType.parameters().front()})));
            }

            return python::Type::makeFunctionType(parameterType, python::Type::UNKNOWN);
        };

        auto nextFunctionTyper = [this](const python::Type& parameterType) {
            if(parameterType.parameters().size() == 1) {
                // always return yield type of an iterator
                auto iteratorType = parameterType.parameters().front();
                if(iteratorType.isIteratorType()) {
                    if(iteratorType == python::Type::EMPTYITERATOR) {
                        // will raise exception later, return dummy function type
                        return python::Type::makeFunctionType(parameterType, python::Type::I64);
                    }
                    return python::Type::makeFunctionType(parameterType, iteratorType.yieldType());
                }
            }

            if(parameterType.parameters().size() == 2) {
                // has default value
                auto iteratorType = parameterType.parameters()[0];
                auto defaultType = parameterType.parameters()[1];

                if(iteratorType.isIteratorType()) {
                    if(iteratorType == python::Type::EMPTYITERATOR) {
                        // always yield default type for empty iterator
                        return python::Type::makeFunctionType(parameterType, defaultType);
                    }
                    if(iteratorType.yieldType() != defaultType) {
                        addCompileError(CompileError::TYPE_ERROR_NEXT_CALL_DIFFERENT_DEFAULT_TYPE);
                    }
                    return python::Type::makeFunctionType(parameterType, iteratorType.yieldType());
                }
            }

            return python::Type::makeFunctionType(parameterType, python::Type::UNKNOWN);
        };

        addSymbol(make_shared<Symbol>("iter", iterFunctionTyper));
        addSymbol(make_shared<Symbol>("reversed", reversedFunctionTyper));
        addSymbol(make_shared<Symbol>("zip", zipFunctionTyper));
        addSymbol(make_shared<Symbol>("enumerate", enumerateFunctionTyper));
        addSymbol(make_shared<Symbol>("next", nextFunctionTyper));

        // TODO: other parameters? i.e. step size and Co?
        // also, boolean, float? etc.?
        addSymbol("range", python::Type::makeFunctionType(python::Type::I64, python::Type::RANGE));

        // attribute functions
        addBuiltinTypeAttribute(python::Type::STRING, "center",
                                python::Type::makeFunctionType(python::Type::makeTupleType({python::Type::I64}), python::Type::STRING));
        addBuiltinTypeAttribute(python::Type::STRING, "center",
                                python::Type::makeFunctionType(python::Type::makeTupleType({python::Type::I64, python::Type::STRING}), python::Type::STRING));
        addBuiltinTypeAttribute(python::Type::STRING, "lower",
                                python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::STRING));
        addBuiltinTypeAttribute(python::Type::STRING, "upper",
                                python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::STRING));

        // strip has two versions: 1. without string arg 2. with string arg
        for(auto name : std::vector<std::string>{"strip", "rstrip", "lstrip"}) {
            addBuiltinTypeAttribute(python::Type::STRING, name,
                                    python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::STRING));
            addBuiltinTypeAttribute(python::Type::STRING, name,
                                    python::Type::makeFunctionType(python::Type::STRING, python::Type::STRING));
        }

        addBuiltinTypeAttribute(python::Type::STRING, "swapcase",
                                python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::STRING));

        addBuiltinTypeAttribute(python::Type::STRING, "join",
                                python::Type::makeFunctionType(python::Type::makeListType(python::Type::STRING),
                                                               python::Type::STRING));
        addBuiltinTypeAttribute(python::Type::STRING, "split", python::Type::makeFunctionType(python::Type::STRING,
                                                                                              python::Type::makeListType(
                                                                                                      python::Type::STRING)));

        addBuiltinTypeAttribute(python::Type::STRING, "find",
                                python::Type::makeFunctionType(python::Type::STRING, python::Type::I64));
        addBuiltinTypeAttribute(python::Type::STRING, "rfind",
                                python::Type::makeFunctionType(python::Type::STRING, python::Type::I64));
        addBuiltinTypeAttribute(python::Type::STRING, "index",
                                python::Type::makeFunctionType(python::Type::STRING, python::Type::I64));
        addBuiltinTypeAttribute(python::Type::STRING, "rindex",
                                python::Type::makeFunctionType(python::Type::STRING, python::Type::I64));
        addBuiltinTypeAttribute(python::Type::STRING, "replace", python::Type::makeFunctionType(
                python::Type::makeTupleType({python::Type::STRING, python::Type::STRING}), python::Type::STRING));
        addBuiltinTypeAttribute(python::Type::STRING, "format",
                                python::Type::makeFunctionType(python::Type::GENERICTUPLE, python::Type::STRING));
        addBuiltinTypeAttribute(python::Type::STRING, "startswith",
                                python::Type::makeFunctionType(python::Type::STRING, python::Type::BOOLEAN));
        addBuiltinTypeAttribute(python::Type::STRING, "endswith",
                                python::Type::makeFunctionType(python::Type::STRING, python::Type::BOOLEAN));
        addBuiltinTypeAttribute(python::Type::STRING, "count",
                                python::Type::makeFunctionType(python::Type::STRING, python::Type::I64));
        addBuiltinTypeAttribute(python::Type::STRING, "isdecimal",
                                python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::BOOLEAN));
        addBuiltinTypeAttribute(python::Type::STRING, "isdigit",
                                python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::BOOLEAN));
        addBuiltinTypeAttribute(python::Type::STRING, "isalpha",
                                python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::BOOLEAN));
        addBuiltinTypeAttribute(python::Type::STRING, "isalnum",
                                python::Type::makeFunctionType(python::Type::EMPTYTUPLE, python::Type::BOOLEAN));

        // for dict, list, tuple use generic type version!

        // i.e. type depending on input

        // for pop/popitem things are actually a bit more complicated...
        // i.e. the default keyword may introduce an issue...
        // https://www.programiz.com/python-programming/methods/dictionary/pop

        // pop and popitem depend on the param type as well as what dictionary is used
        std::vector<python::Type> dict_types = {python::Type::BOOLEAN, python::Type::I64, python::Type::F64,
                                                python::Type::STRING};
        for (const auto &t1 : dict_types)
            for (const auto &t2 : dict_types) {

                auto dict_type = python::Type::makeDictionaryType(t1, t2);

                // create specialized dict type
                auto dict_sym = std::make_shared<Symbol>(dict_type.desc(), "dictionary", t1, SymbolType::TYPE);
                // add here symbol so popitem can be easily added.
                addSymbol(dict_sym);

                // pop
                // Note: the typer function here needs to return a function typing!
                auto pop_sym = std::make_shared<Symbol>("pop", [dict_type](const python::Type &params) {
                    // there should be 1 or 2 params
                    // dictionary.pop(key[, default])
                    switch (params.parameters().size()) {
                        case 1: {
                            // return value type of dict!
                            auto keyType = params.parameters().front();
                            // check key can be upcast to dict_type.keyType()!
                            if (!python::canUpcastType(keyType, dict_type.keyType()))
                                throw std::runtime_error("using pop with key type " + keyType.desc()
                                                         + " which can't be cast to dictionaries key type " +
                                                         dict_type.keyType().desc());
                            return python::Type::makeFunctionType(params, dict_type.valueType());
                        }
                        case 2: {
                            auto keyType = params.parameters()[0];
                            auto default_type = params.parameters()[1];
                            // check key can be upcast to t1.keyType()!
                            if (!python::canUpcastType(keyType, dict_type.keyType()))
                                throw std::runtime_error("using pop with key type " + keyType.desc()
                                                         + " which can't be cast to dictionaries key type " +
                                                         dict_type.keyType().desc());
                            // Note: is this casting actually correct?
                            // when {12 : 20}.pop(3, 3.0) is used, then shouldn't int be assumed?
                            // ==> when auto upcasting no!
                            // => Tuplex model, fail this row!
                            // if(autoUpcast && type is number) {
                            //     auto superType = python::Type::superType(default_type, dict_type.valueType());
                            //     if(superType == python::Type::UNKNOWN)
                            //         throw std::runtime_error("default_type " + default_type.desc() + " and value type " + dict_type.valueType() + " can't be unified");
                            //     return superType;
                            // }
                            return python::Type::makeFunctionType(params,
                                                                  dict_type.valueType()); // keep it simple, let Tuplex fail for the weird typing cases.
                            break;
                        }
                        default:
                            throw std::runtime_error("pop has zero arguments");
                            return python::Type::UNKNOWN;
                    }
                });
                dict_sym->addAttribute(pop_sym);

                // popitem
                addBuiltinTypeAttribute(dict_type, "popitem",
                                        python::Type::makeFunctionType(python::Type::EMPTYTUPLE,
                                                                       python::Type::makeTupleType({dict_type.keyType(), dict_type.valueType()})));

            }

        // for the weird case of the default object having different type than the dict value type, use tracing.

        // another good design for builtin functions could be:
        // class BuiltinFunction {
        // --> free standing functions
        //  type() ==>
        // };
        // class AttributeFunction {
        // functions operating on objects.
        // };
        // ==> how to hook up functions from defined objects??
        // which then bundles code generation, typing etc. => that might be easier to extent...
        // @TODO: is this wise?


        addBuiltinExceptionHierarchy();
    }

    void SymbolTable::addBuiltinExceptionHierarchy() {
        using namespace std;
        // taken from https://docs.python.org/3/library/exceptions.html
        // this is the complete hierarchy.
        // decode tree
        auto hierarchy = "BaseException\n"
                         " +-- SystemExit\n"
                         " +-- KeyboardInterrupt\n"
                         " +-- GeneratorExit\n"
                         " +-- Exception\n"
                         "      +-- StopIteration\n"
                         "      +-- StopAsyncIteration\n"
                         "      +-- ArithmeticError\n"
                         "      |    +-- FloatingPointError\n"
                         "      |    +-- OverflowError\n"
                         "      |    +-- ZeroDivisionError\n"
                         "      +-- AssertionError\n"
                         "      +-- AttributeError\n"
                         "      +-- BufferError\n"
                         "      +-- EOFError\n"
                         "      +-- ImportError\n"
                         "      |    +-- ModuleNotFoundError\n"
                         "      +-- LookupError\n"
                         "      |    +-- IndexError\n"
                         "      |    +-- KeyError\n"
                         "      +-- MemoryError\n"
                         "      +-- NameError\n"
                         "      |    +-- UnboundLocalError\n"
                         "      +-- OSError\n"
                         "      |    +-- BlockingIOError\n"
                         "      |    +-- ChildProcessError\n"
                         "      |    +-- ConnectionError\n"
                         "      |    |    +-- BrokenPipeError\n"
                         "      |    |    +-- ConnectionAbortedError\n"
                         "      |    |    +-- ConnectionRefusedError\n"
                         "      |    |    +-- ConnectionResetError\n"
                         "      |    +-- FileExistsError\n"
                         "      |    +-- FileNotFoundError\n"
                         "      |    +-- InterruptedError\n"
                         "      |    +-- IsADirectoryError\n"
                         "      |    +-- NotADirectoryError\n"
                         "      |    +-- PermissionError\n"
                         "      |    +-- ProcessLookupError\n"
                         "      |    +-- TimeoutError\n"
                         "      +-- ReferenceError\n"
                         "      +-- RuntimeError\n"
                         "      |    +-- NotImplementedError\n"
                         "      |    +-- RecursionError\n"
                         "      +-- SyntaxError\n"
                         "      |    +-- IndentationError\n"
                         "      |         +-- TabError\n"
                         "      +-- SystemError\n"
                         "      +-- TypeError\n"
                         "      +-- ValueError\n"
                         "      |    +-- UnicodeError\n"
                         "      |         +-- UnicodeDecodeError\n"
                         "      |         +-- UnicodeEncodeError\n"
                         "      |         +-- UnicodeTranslateError\n"
                         "      +-- Warning\n"
                         "           +-- DeprecationWarning\n"
                         "           +-- PendingDeprecationWarning\n"
                         "           +-- RuntimeWarning\n"
                         "           +-- SyntaxWarning\n"
                         "           +-- UserWarning\n"
                         "           +-- FutureWarning\n"
                         "           +-- ImportWarning\n"
                         "           +-- UnicodeWarning\n"
                         "           +-- BytesWarning\n"
                         "           +-- ResourceWarning";

        auto lines = core::splitLines(hierarchy, "\n");
        std::deque<python::Type> baseTypes;
        python::Type lastType = python::Type::UNKNOWN;
        for(auto line : lines) {
            size_t nameStartIdx = 0;
            int level = 0;
            while(!isalnum(line[nameStartIdx]) && nameStartIdx < line.length()) {
                nameStartIdx++;
            }
            level = nameStartIdx / 5;
            std::string name = line.substr(nameStartIdx);
            // no basetype?
            if(baseTypes.empty()) {
                baseTypes.push_back(python::TypeFactory::instance().createOrGetPrimitiveType(name));
                lastType = baseTypes.back();
            } else {

                // level increase or decrease?
                if(level == baseTypes.size()) {
                    // same level?
                    // cout<<"Adding "<<name<<" <- "<<baseTypes.back().desc()<<endl;
                    lastType = python::TypeFactory::instance().createOrGetPrimitiveType(name, {baseTypes.back()});
                } else if(level == baseTypes.size() + 1) {
                    // cout<<"Adding "<<name<<" <- "<<lastType.desc()<<endl;
                    baseTypes.push_back(lastType);
                    lastType = python::TypeFactory::instance().createOrGetPrimitiveType(name,{baseTypes.back()});
                } else {
                    // decrease!
                    // pop
                    baseTypes.pop_back();
                    // cout<<"Adding "<<name<<" <- "<<baseTypes.back().desc()<<endl;
                    lastType = python::TypeFactory::instance().createOrGetPrimitiveType(name, {baseTypes.back()});
                }
            }

            // add symbol entry!
            addSymbol(name, lastType);
        }
    }


    static std::string scopeTypeToString(SymbolTable::ScopeType st) {
        switch(st) {
            case SymbolTable::ScopeType::BUILTIN:
                return "builtin";
            case SymbolTable::ScopeType::GLOBAL:
                return "global";
            case SymbolTable::ScopeType::LOCAL:
                return "local";
            default:
                return "unknown";
        }
    }

    Symbol* SymbolTable::addSymbol(std::shared_ptr<Symbol> sym) {
        // add simple Symbol
        auto scope = currentScope();

        auto name = sym->name;

        auto it = scope->symbols.find(name);
        if(it == scope->symbols.end())
            scope->symbols[name] = sym;
        else {
            assert(it->second->qualifiedName == sym->qualifiedName);
            assert(it->second->symbolType == sym->symbolType);
            // check if type is contained in types
            for(const auto& type : sym->types)
                it->second->addTypeIfNotExists(type);
        }
        it = scope->symbols.find(name);
        return it->second.get();
    }

    Symbol* SymbolTable::addSymbol(const std::string &name, const python::Type &type) {
        return addSymbol(make_shared<Symbol>(name, type));
    }

    void SymbolTable::addBuiltinTypeAttribute(const python::Type &builtinType, const std::string &name,
                                              const python::Type &type) {
        // this seems wrong, need to perform the lookup directly...
        // use desc as name
        auto scope = currentScope();
        auto it = scope->symbols.find(builtinType.desc());
        if(it == scope->symbols.end()) {
            scope->symbols[builtinType.desc()] = make_shared<Symbol>(builtinType.desc(), builtinType.desc(), type, SymbolType::TYPE);
            it = scope->symbols.find(builtinType.desc());
            assert(it != scope->symbols.end());
        }
        auto sym_att = it->second->findAttribute(name);
        if(!sym_att) {
            it->second->addAttribute(make_shared<Symbol>(name, name, type, type.isFunctionType() ? SymbolType::FUNCTION : SymbolType::VARIABLE));
            sym_att = it->second->findAttribute(name);
        } else {
            // check that symbol type is compatible!
            auto symbolType = type.isFunctionType() ? SymbolType::FUNCTION : SymbolType::VARIABLE;
            if(symbolType != sym_att->symbolType)
                throw std::runtime_error("symbol can only have one kind of types associated with it!");
            assert(sym_att->qualifiedName == name);
            sym_att->name = name;
            sym_att->addTypeIfNotExists(type);
        }
        assert(sym_att);
        sym_att->parent = scope->symbols[name];
    }

    void SymbolTable::print(std::ostream &os) {
        os<<"SymbolTable\n";
        os<<"-----------\n";

        // go through scopes and print nice formatted table
        for(auto scope : _scopes) {
            // indent in terms of level
            std::string indent;
            for(int i = 0; i < scope->level; ++i)
                indent += "  ";

            // first comes now what type of scope there is
            os<<indent<<"> "<<scopeTypeToString(scope->type);
            if(!scope->name.empty())
                os<<"["<<scope->name<<"]";

            os<<" <\n";


            // get longest name
            size_t longest_name = 0;
            for(const auto& sym : scope->symbols)
                longest_name = std::max(longest_name, sym.second->fullyQualifiedName().length());

            // now all symbols incl. descriptions
            for(const auto& sym : scope->symbols) {
                os<<indent<<"  ";
                os<<sym.second->fullyQualifiedName();

                // how many spaces to print to format
                int fmtLength = sym.second->fullyQualifiedName().length();

                for(int i = 0; i < longest_name - fmtLength; ++i)
                    os<<" ";

                if(sym.second->fullyQualifiedName().empty())
                    os<<" ";
                for(const auto& type : sym.second->types)
                    os<<"  type: "<<type.desc()<<"\n";
            }

            // newline
            os<<"\n";
        }
        os<<std::endl;
    }

// should be used for simple type lookups!
    python::Type SymbolTable::lookupType(const std::string &symbol) {

        auto sym = findSymbol(symbol);
        if(sym)
            return sym->type();

        return python::Type::UNKNOWN;
    }

    static python::Type typeAttribute(std::shared_ptr<Symbol> sym, std::string attribute, python::Type parameterType) {
        if(sym) {
            auto attr_sym = sym->findAttribute(attribute);

            if(attr_sym) {
                // following branch will be taken for e.g. constants within a module, object etc.
                if(attr_sym->symbolType != SymbolType::FUNCTION)
                    // else, return single type
                    return attr_sym->type();
                python::Type funcType = python::Type::UNKNOWN;
                attr_sym->findFunctionTypeBasedOnParameterType(parameterType, funcType); // ignore ret value.
                return funcType;
            }
        }

        // not found
        return python::Type::UNKNOWN;
    }

    python::Type SymbolTable::findAttributeType(const python::Type &object_type,
                                                const std::string &attribute,
                                                const python::Type& parameterType) {
        // just check first scope (i.e. the global one)
        // @TODO: if users redefine scope this is wrong...

        assert(!_scopes.empty());
        assert(_scopes.front()->type == ScopeType::BUILTIN);

        python::Type resultType = python::Type::UNKNOWN;

        if(object_type.isFunctionType())
            throw std::runtime_error("we do not support attributes on functions yet");

        // find attribute of concrete type
        // is it custom defined?
        // if(object_type.isClassType()) {...}

        // general option switch
        auto type = object_type.isOptionType() ? object_type.withoutOptions() : object_type;

        // i.e. dict, str, int, float get symbols => they're builtins
        // they all have different typings
        // dict -> genericdict
        // str -> string
        // int -> i64
        // float -> f64
        // then do attribute check on them...

        // so we have different options:

        // name -> module: => module type, attribute would yield concrete functions, variables etc.
        // name -> function: => attribute not working etc.
        // name -> some variable/instance: => attribute would get whatever is defined for that type!

        // first, check specialized Tuplex builtin type names
        // derive name for builtins
        auto name = type.desc();
        auto sym = findSymbol(name);

        resultType = typeAttribute(sym, attribute, parameterType);
        if(resultType != python::Type::UNKNOWN)
            return resultType;

        // check generics
        assert(python::Type::EMPTYTUPLE.isTupleType() && python::Type::EMPTYLIST.isListType() && python::Type::EMPTYDICT.isDictionaryType());
        if(type.isTupleType() || type.isListType() || type.isDictionaryType()) {
            if(type.isTupleType() || type == python::Type::EMPTYTUPLE)
                name = python::Type::GENERICTUPLE.desc(); // generic tuple
            if(type.isListType() || type == python::Type::EMPTYLIST)
                name = python::Type::GENERICLIST.desc();
            if(type.isDictionaryType() || type == python::Type::EMPTYDICT)
                name = python::Type::GENERICDICT.desc();
            sym = findSymbol(name);
            resultType = typeAttribute(sym, attribute, parameterType);
        }

        return resultType;
    }

    python::Type SymbolTable::findClosestFunction(const std::string &name, const python::Type &parameterType) {
        // search stack from top to bottom
        for(auto it = _scopeStack.rbegin(); it != _scopeStack.rend(); ++it) {
            Scope* scope = *it;
            assert(scope);

            auto jt = scope->symbols.find(name);
            if(jt != scope->symbols.end()) {
                auto symbol = jt->second;
                python::Type matchType = python::Type::UNKNOWN;
                if(symbol->findFunctionTypeBasedOnParameterType(parameterType, matchType))
                    return matchType;
            }
        }

        return python::Type::UNKNOWN;
    }

// TODO: generic specialization! => i.e. replace with concrete result!
    python::Type SymbolTable::findFunctionType(const std::string &symbol, const python::Type &parameterType, bool withNullAsAny) {

        using namespace std;

        // ==> i.e. check first if for this exact type there is a function. If not, then remove all options and
        // try to find a function matching.
        // ==> this will induce an exception check in the code generated code
        if (parameterType.isOptional()) {
            // first, search with option type
            auto t = findClosestFunction(symbol, parameterType);
            if (t == python::Type::UNKNOWN)
                t = findClosestFunction(symbol, parameterType.withoutOptions());

            // depending on typing, this could happen!
            // // check if still unknown, then this means could not be deducted
            // if (t == python::Type::UNKNOWN)
            //     Logger::instance().defaultLogger().error("Could not find function named " + symbol
            //                                              + " with parameter type " + parameterType.desc() +
            //                                              " nor parameter type "
            //                                              + parameterType.withoutOptions().desc());
            return t;
        } else {

            auto t = findClosestFunction(symbol, parameterType);

            // special case: Because null optimization allows arbitrary Options to be replaced with NULL
            // this would lead to issues, hence for any nulls check presence if special option is enabled
            if(t == python::Type::UNKNOWN && withNullAsAny) {
                // replace all null values with any. thus matching is possible with the first one
                python::Type anyType = python::Type::ANY;
                if(parameterType.isTupleType()) {
                    // @TODO: recursive?? => null value opt only works for first layer.
                    vector<python::Type> params;
                    for(auto p : parameterType.parameters()) {
                        if(p == python::Type::NULLVALUE)
                            params.push_back(python::Type::ANY);
                        else
                            params.push_back(p);
                    }
                    anyType = python::Type::makeTupleType(params);
                }

                // match again, this time with any!
                t = findClosestFunction(symbol, anyType);
            }
            // this could happen depending on typing
            // if (t == python::Type::UNKNOWN)
            //    Logger::instance().defaultLogger().error("Could not find function named " + symbol
            //                                             + " with parameters " + parameterType.desc());
            return t;
        }
    }

    std::shared_ptr<Symbol> SymbolTable::findSymbol(const std::string &name) {
        // search stack from top to bottom
        for(auto it = _scopeStack.rbegin(); it != _scopeStack.rend(); ++it) {
            Scope* scope = *it;
            auto jt = scope->symbols.find(name);
            if(jt != scope->symbols.end())
                return jt->second;
        }
        return nullptr;
    }

    std::shared_ptr<Symbol> SymbolTable::findFullyQualifiedSymbol(const std::string &fullyQualifiedName) {
        // check whether . is contained
        auto stem_idx = fullyQualifiedName.find_first_of('.');

        // full stem?
        if(stem_idx == std::string::npos)
            return findSymbol(fullyQualifiedName);
        else {
            // find symbol with stem and then iterate over remaining string!
            auto stem = fullyQualifiedName.substr(0, stem_idx);
            auto tail = fullyQualifiedName.substr(stem_idx + 1);
            auto sym = findSymbol(stem);
            while(!tail.empty() && sym) {
                stem_idx = tail.find_first_of('.');
                stem = tail.substr(0, stem_idx);
                tail = stem_idx == std::string::npos ? "" : tail.substr(stem_idx + 1);

                if(sym->fullyQualifiedName() == fullyQualifiedName)
                    return sym;
                sym = sym->findAttribute(stem);
            }

            if(sym && sym->fullyQualifiedName() == fullyQualifiedName)
                return sym;
        }

        // exhausted the recursive search, could be an aliased symbol. Perform super slow search across all symbols...
        // @TODO: hashmap to speed this up?
        for(auto it = _scopeStack.rbegin(); it != _scopeStack.rend(); ++it) {
            Scope *scope = *it;
            assert(scope);

            // check within this scope & attributes...
            std::queue<std::shared_ptr<Symbol>> q;
            for(auto keyval : scope->symbols)
                q.push(keyval.second);

            while(!q.empty()) {
                auto sym = q.front(); q.pop();
                if(sym->fullyQualifiedName() == fullyQualifiedName)
                    return sym;

                for(auto attr : sym->attributes())
                    q.push(attr);
            }
        }

        // nothing found, abort.
        return nullptr;
    }
}