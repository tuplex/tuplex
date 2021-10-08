//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <UDF.h>
#include "../../adapters/cpython/include/PythonHelpers.h"


#include <graphviz/GraphVizGraph.h>
#include <IPrePostVisitor.h>
#include <unordered_map>
#include <IReplaceVisitor.h>
#include <ColumnRewriteVisitor.h>
#include <TraceVisitor.h>
#include <ApplyVisitor.h>

#ifndef NDEBUG
static int g_func_counter = 0;
#endif

namespace tuplex {

    bool UDF::_compilationEnabled = true;
    bool UDF::_allowNumericTypeUnification = true;

    UDF::UDF(const std::string &pythonLambdaStr,
             const std::string& pickledCode,
             const ClosureEnvironment& globals) : _isCompiled(false), _failed(false), _code(pythonLambdaStr), _pickledCode(pickledCode), _outputSchema(Schema::UNKNOWN), _inputSchema(Schema::UNKNOWN), _dictAccessFound(false), _rewriteDictExecuted(false) {

        // empty UDF.
        if(pythonLambdaStr.empty())
            return;

        _ast.setGlobals(globals);

        // only parse of code length > 0
        if(_code.length() > 0 && _compilationEnabled)
            // attempt to parse code
            // if it fails, make sure a backup solution in the form of pickled code is existing
            _isCompiled = _ast.parseString(_code, _allowNumericTypeUnification);

        // backup solution
        if(!_isCompiled && _pickledCode.length() == 0) {

            if(!Py_IsInitialized()) {
                Logger::instance().defaultLogger().error("trying to get pickled code, but python interpreter is not initialized");
                exit(1);
            }

            // important to get GIL for this
           python::lockGIL();

            // get pickled version of the function
            _pickledCode = python::serializeFunction(python::getMainModule(), _code);

            // release GIL here
           python::unlockGIL();

            if(0 == _pickledCode.length())
                _failed = true;
        }

        // if compiled, add globals
        if(isCompiled()) {
            _ast.setGlobals(globals);
        }
    }

    UDF& UDF::hintInputParameterType(const std::string &param, const python::Type &type) {
#warning "need to fix this in order to have more arbitrary aliasing etc. Right now only most up-level type hints supported. What about nested statements?"
        _ast.addTypeHint(param, type);
        return *this;
    }

    std::vector< std::tuple<std::string, python::Type> > UDF::getInputParameters() const {
        std::vector< std::tuple<std::string, python::Type> > v;

        // fetch names & types from AST
        auto names = _ast.getParameterNames();
        auto params = _ast.getParameterTypes();

        assert(names.size() == params.argTypes.size());

        for(int i = 0; i < names.size(); ++i) {
            v.push_back(std::make_tuple(names[i], params.argTypes[i]));
        }

        return v;
    }


    bool UDF::hintParams(std::vector<python::Type> hints, std::vector<std::tuple<std::string, python::Type> > params, bool silent, bool removeBranches) {

        // check that the number of hints matches the input parameters of the udf
        if(hints.size() != params.size()) {
            Logger::instance().logger("type inference").error("number of hints does not match number of arguments of UDF");
            return false;
        }

        int pos = 0;
        for(const auto& el : params) {
            std::string name = std::get<0>(el);
            python::Type type = std::get<1>(el);
            python::Type typeHint = hints[pos];

            // simple case: type is unknown, hint with type from schema!
            if(python::Type::UNKNOWN == type) {
                assert(python::Type::UNKNOWN != typeHint);
                hintInputParameterType(name, typeHint);
            } else {
                // more difficult case: check whether types are compatible (i.e. upcasting)
                // or not
                if(type != typeHint) {
                    // get super type
                    python::Type superType = python::Type::superType(type, typeHint);

                    // special case: projection pushdown may lead to one column surviving only,
                    // then (typeHint) = type -> this is ok, i.e. t[0] indexing
                    if(superType == python::Type::UNKNOWN && python::Type::propagateToTupleType(typeHint) == type) {
                        superType = type;
                        // set arg as tuple!
                        _ast.setUnpacking(false);
                    }


                    if(superType != python::Type::INF && superType != python::Type::UNKNOWN) {
                        hintInputParameterType(name, superType);
                    } else {
                        //if(!silent)
                            Logger::instance().logger("type inference").error("could not detect valid supertype for '"
                                                                              + type.desc() + "' and '"
                                                                              + typeHint.desc() + "'");

                        // NOTE: this error is typically produced when a UDF is "retyped", i.e. after pushdown
                        return false;
                    }
                }

                // i.e. input bool is compatible to int, float
                // or int is compatible to float
                // however, else this ain't true

            }
            pos++;
        }


        // after all the type hints, try to define final types
        auto res = _ast.defineTypes(silent, removeBranches);
        if(!_ast.getCompileErrors().empty()) {
            addCompileErrors(_ast.getCompileErrors());
        }
        return res;
    }


    UDF& UDF::removeTypes(bool removeAnnotations) {

        _inputSchema = Schema::UNKNOWN;
        _outputSchema = Schema::UNKNOWN;
        _ast.removeParameterTypes();

        // annotations as well?
        if(removeAnnotations) {
            ApplyVisitor av([](const ASTNode* node){ return true; }, [](ASTNode& node) {
                node.removeAnnotation();
            });
            _ast.getFunctionAST()->accept(av);
        }

        return *this;
    }

    Schema UDF::getInputSchema() const {
        // assert(_inputSchema.getRowType() != python::Type::UNKNOWN);
        return _inputSchema;
    }

    bool containsPyObjectType(const python::Type& t) {
        if (python::Type::PYOBJECT == t)
            return true;

        if (t.isTupleType()) {
            for (auto p : t.parameters())
                if (containsPyObjectType(p))
                    return true;
        }

        if (t.isDictionaryType()) {
            if (containsPyObjectType(t.keyType()))
                return true;
            if (containsPyObjectType(t.valueType()))
                return true;
        }

        if (t.isListType()) {
            if (containsPyObjectType(t.elementType()))
                return true;
        }

        if (t.isOptionType()) {
        if (containsPyObjectType(t.elementType()))
            return true;
        }
        if(t.isFunctionType()) {
            if(containsPyObjectType(t.getParamsType()))
                return true;
            if(containsPyObjectType(t.getReturnType()))
                return true;
        }

        return false;
    }

    bool UDF::hasPythonObjectTyping() const {
        // only follow active branches
        // @TOOD: partial compilation w. exceptions!!!
        // i.e. could speculate on parts that would compile
        // for now simply use apply visitor

#warning "work on this"
        // TODO: an optimization can be done, when objects are merely being passed around.
        // i.e., when let's say pyobjects are simply accessed but never produced by external functions
        // or symbols.
        // => a visitor should check that properly...
        // How can external, pyobjects be only produced? -> via calls...
        // the other way would be using operations, etc. yet, the compiler would error on that.
        // Thus forcing the fallback anyways?

        // @TODO: need to investigate this...
        // --> also in conjunction with unknown...
        bool foundPyObject = false;
        ApplyVisitor av([](const ASTNode* node) {
            return true;
        }, [&foundPyObject](ASTNode& n) {

            if(n.type() == ASTNodeType::Call) {
                auto call = (NCall*)&n;
                auto call_type = call->getInferredType();
                if(call_type.isFunctionType()) {
                    auto ret_type = call_type.getReturnType();
                    if(containsPyObjectType(ret_type))
                        foundPyObject = true;
                } else if(python::Type::PYOBJECT == call_type)
                    foundPyObject = true;

                // check pos args are not of pyobject, if they're default to fallback compilation...
                for(auto arg : call->_positionalArguments) {
                    if(arg->getInferredType() == python::Type::PYOBJECT)
                        foundPyObject = true;
                }
            }

            // attribute or identifier with external symbol?
            if(n.type() == ASTNodeType::Attribute) {
                auto attr = (NAttribute*)&n;
                if(attr->hasAnnotation() && attr->annotation().symbol) {
                    auto sym = attr->annotation().symbol;
                    if(sym->symbolType == SymbolType::EXTERNAL_PYTHON)
                        foundPyObject = true;
                }
            }
            if(n.type() == ASTNodeType::Identifier) {
                auto id = (NIdentifier*)&n;
                if(id->hasAnnotation() && id->annotation().symbol) {
                    auto sym = id->annotation().symbol;
                    if(sym->symbolType == SymbolType::EXTERNAL_PYTHON)
                        foundPyObject = true;
                }
            }
        });

        if(_ast.getFunctionAST())
            _ast.getFunctionAST()->accept(av);

        return foundPyObject;
    }

    bool UDF::hintInputSchema(const Schema &schema, bool removeBranches, bool printErrors) {

        _hintedInputSchema = schema;
        _inputSchema = Schema::UNKNOWN;

        _ast.setUnpacking(false);
        python::Type hintType = schema.getRowType();
        assert(hintType.isTupleType());

        // there are two cases now:
        // either the user accesses everything as a tuple or the first tuple gets unpacked (syntactical sugar)
        auto params = getInputParameters();

        if(0 == params.size()) {
            // empty tuple?
            Logger::instance().logger("type inference").warn("no param not yet implemented");
        } else if(1 == params.size()) {

            // two options
            // (1) param is used as tuple
            // when param is indexed or not used...
            // (2) param is used as primitive
            // --> this corresponds to the identifier associated with the single parameter
            // being never indexed or called upon with a function

            // for both cases the question is whether a valid typing can be found.
            // this is done with a succeed first approach
            // first, it is tested whether treating the single parameter as tuple works.
            // If this fails, then the parameter is unpacked and again the typing it tried
            // failure of this indicates a failure of the function.


            if(hintType.parameters().size() == 1) {
                // consider first a basic case:
                // programming wise, when the schema contains only a primitive type, i.e.
                // the schema is (i64), (f64), (bool), (string), ([i64]), ...
                // meaning there is a single element and it is not a tuple
                // then indexing it is stupid. I.e. avoid x[0], just write x.
                if(!hintType.parameters().front().isTupleType()) {
                    if(!hintParams({hintType.parameters()[0]}, params, true, removeBranches)) {
                        logTypingErrors(printErrors);
                        return false;
                    }
                    _ast.setUnpacking(true);
                    _inputSchema = Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType(_ast.getParameterTypes().argTypes));
                    _outputSchema = Schema(Schema::MemoryLayout::ROW, codegenTypeToRowType(_ast.getReturnType()));

                    if(hasPythonObjectTyping())
                        markAsNonCompilable();
                    return true;
                } else {
                    // hint with first element as tuple unpacked
                    if(!hintParams({hintType.parameters()[0]}, params, true, removeBranches)) {
                        logTypingErrors(printErrors);
                        return false;
                    }
                    // @todo: this is bad naming. should be rephrased to treat first arg as tuple or not
                    _ast.setUnpacking(false);
                    _inputSchema = Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType(_ast.getParameterTypes().argTypes));
                    _outputSchema = Schema(Schema::MemoryLayout::ROW, codegenTypeToRowType(_ast.getReturnType()));
                    if(hasPythonObjectTyping())
                        markAsNonCompilable();
                    return true;
                }
            } else {

                // note: params is one size!
                // hintType is not one size, i.e. make hinttype tuple
                assert(params.size() == 1);

                python::Type paramType = std::get<1>(params.front());

                if(paramType != hintType) {

                    // if paramType is not unknown (i.e. do not need to decode hinttype) make sure they're compatible here!
                    if(paramType != python::Type::UNKNOWN) {
                        auto tupledHintType = python::Type::makeTupleType({hintType});
                        if(tupledHintType == paramType)
                            hintType = tupledHintType;
                        // debug print, typically landing here for param rewrite in withcolumn after projection pushdown...
                        // std::cout<<"paramType: "<<paramType.desc()<<" hintType: "<<hintType.desc()<<std::endl;
                    }
                }

                // only hint with the full set is possible...
                if(!hintParams({hintType}, params, true, removeBranches)) {
                    return false;
                }
                _ast.setUnpacking(false);
                _inputSchema = Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType(_ast.getParameterTypes().argTypes));
                _outputSchema = Schema(Schema::MemoryLayout::ROW, codegenTypeToRowType(_ast.getReturnType()));
                if(hasPythonObjectTyping())
                    markAsNonCompilable();
                return true;
            }
        } else {
            // unpack > 1 parameter
           _ast.setUnpacking(true);

            // check whether hintType is really a tuple. If not, then something is wrong!
            assert(hintType.isTupleType());

            // strict constraint here is that the number of parameters in the hinttype must match args
            // if it is one tuple, unpack
            if(1 == hintType.parameters().size())
                hintType = hintType.parameters().front();


            // special case: it could happen that hintType is now a primitive.
            // ==> wrong number of arguments
            if(!hintType.isTupleType()) {
                Logger::instance().logger("type inference").error("UDF required "
                                                                  + std::to_string(params.size()) + " parameters, but previous function only yielded one parameter.");
                return false;
            }

            if(hintType.parameters().size() != params.size()) {
                Logger::instance().logger("type inference").error("number of parameters must match number of columns/tuple elements of previous operator");
                return false;
            }

            if(!hintParams(hintType.parameters(), params, true, removeBranches)) {
                // may fail, do not log
                //Logger::instance().logger("type inference").error("multi parameter type hinting not successful.");
                return false;
            }
            _inputSchema = Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType(_ast.getParameterTypes().argTypes));
            _outputSchema = Schema(Schema::MemoryLayout::ROW, codegenTypeToRowType(_ast.getReturnType()));
            if(hasPythonObjectTyping())
                markAsNonCompilable();
            return true;
        }
        return false;
    }

    python::Type UDF::codegenTypeToRowType(const python::Type &type) const {
        return pyTypeToRowType(type);
    }

    Schema UDF::getOutputSchema() const {

        // lazy schema
        if(_outputSchema == Schema::UNKNOWN) {
            // cg return type always returns the actual return type of the python function
            // in the schema this needs to be encapsulated by a tuple
            // NOTE right now, multiple returns not supported!
            // lambda syntax doesn't support this

            auto retType = _ast.getReturnType();
            return Schema(Schema::MemoryLayout::ROW, codegenTypeToRowType(retType));
        }

        return _outputSchema;
    }

    std::string UDF::getPickledCode() const {

        // empty udf? ==> return ""
        if(_pickledCode.empty() && _code.empty())
            return "";


        if(_pickledCode.length() == 0) {
            assert(_code.length() > 0);

            // important to get GIL for this
            python::lockGIL();

            // if closure has entries, need to add them to the module to pickle properly...
            auto mod = python::getMainModule();
            auto main_dict = PyModule_GetDict(mod);
            auto ce = getAnnotatedAST().globals();

            // for C-API ref check https://docs.python.org/3/library/functions.html#__import__

            // modules first
            for(auto m : ce.modules()) {
                auto sub_mod = PyImport_ImportModule(m.original_identifier.c_str());
                PyDict_SetItemString(main_dict, m.identifier.c_str(), sub_mod);
                if(PyErr_Occurred()) {
                    PyErr_Print();
                    PyErr_Clear();
                    python::unlockGIL();
                    throw std::runtime_error("failed to import module " + m.original_identifier);
                }
            }
            // then functions
            for(auto f : ce.functions()) {
                // need to fetch from imported module and add it
                // basically this is encoded
                // from ... import ...
                // => take care of special case *!
                auto nameList = PyList_New(1);
                PyList_SET_ITEM(nameList, 0, python::PyString_FromString(f.qualified_name.c_str()));
                auto module = PyImport_ImportModuleEx(f.package.c_str(), nullptr, nullptr, nameList);
                if(!module || PyErr_Occurred()) {
                    PyErr_Clear();
                    python::unlockGIL();
                    throw std::runtime_error("failed to import module " + f.package);
                }

                PyModule_AddObject(mod, f.identifier.c_str(), module);
            }
            // then constants
            for(auto c : ce.constants()) {
                auto obj = python::fieldToPython(c.value);
                PyDict_SetItemString(main_dict, c.identifier.c_str(), obj);
                if(PyErr_Occurred()) {
                    PyErr_Print();
                    PyErr_Clear();
                    python::unlockGIL();
                    throw std::runtime_error("failed to define constant " + c.identifier + " = " + c.value.toPythonString());
                }
            }

            // pickle code
            auto pickled_code = python::serializeFunction(mod, _code);
            // release GIL here
            python::unlockGIL();

            assert(pickled_code.length() > 0);
            return pickled_code;
        } else return _pickledCode;
    }

    codegen::CompiledFunction UDF::compile(codegen::LLVMEnvironment &env, bool allowUndefinedBehaviour, bool sharedObjectPropagation) {

        codegen::CompiledFunction cf;
        MessageHandler& logger = Logger::instance().logger("codegen");
        cf.function = nullptr;

        // first check whether this function is compilable or not
        if(!isCompiled()) {
            logger.error("Function must be compilable when calling compile");
            return cf;
        }

        codegen::AnnotatedAST& cg = getAnnotatedAST();

        // check that there is at least one parameter!

        // input type corresponds to how the function is called:
        cf.input_type = python::Type::makeTupleType(cg.getParameterTypes().argTypes);
        cf.output_type = cg.getReturnType();


        // check whether given output schema differs from output_type.
        // ==> could be that e.g. a resolver requests upcasting!
        // --> notify code generator of that.
        // a.) could either do this by inserting dummy ast nodes, or simply b.) coding it directly up
        if(cg.getRowType() != getOutputSchema().getRowType()) {

            // is it a primitive or a tuple?
            auto rt = cg.getReturnType();
            python::Type targetType = getOutputSchema().getRowType();
            if(rt.isPrimitiveType()) {
                assert(targetType.parameters().size() == 1);
                targetType = targetType.parameters().front();
            }

            // special case: It's a single-element tuple -> i.e. packed!
            // unpack one level, so the setReturnType function doesn't fail.
            if(rt.isTupleType() && rt.parameters().size() == 1) {
                targetType = targetType.parameters().front();
            }

            logger.info("upcasting function return type from " + rt.desc() + " to " + targetType.desc());
            cg.checkReturnError();
            cg.setReturnType(targetType);
            cf.output_type = cg.getReturnType();
        }

        if(0 == cf.input_type.parameters().size()) {
            logger.error("code generation for user supplied function failed. Lambda function needs at least one parameter.");
            return cf;
        }

        // compile UDF to LLVM IR Code
        if(!cg.generateCode(&env, allowUndefinedBehaviour, sharedObjectPropagation)) {
            // log error and abort processing.
            logger.error("code generation for user supplied function " + cg.getFunctionName() + " failed.");
            cf.function = nullptr; // invalidate func
            return cf;
        }

        // fetch function from module
        auto func = env.getModule()->getFunction(cg.getFunctionName());
        if(!func) {
            logger.error("could not retrieve LLVM function from module");
            cf.function = nullptr; // invalidate func
            return cf;
        }
        cf.function = func;

        return cf;
    }

    codegen::CompiledFunction UDF::compileFallback(codegen::LLVMEnvironment &env, llvm::BasicBlock *constructorBlock,
                                                   llvm::BasicBlock *destructorBlock) {
        using namespace llvm;
        auto& context = env.getContext();
        auto mod = env.getModule().get();
        assert(mod);

        assert(constructorBlock);
        assert(destructorBlock);

        codegen::CompiledFunction cf;
        MessageHandler& logger = Logger::instance().logger("codegen");
        cf.function = nullptr;

        // types:

        // transform to actual output type
        assert(getOutputSchema().getRowType().isTupleType());
        auto actual_output_type = getOutputSchema().getRowType().parameters().size() == 1 ?
                getOutputSchema().getRowType().parameters().front() :
                getOutputSchema().getRowType();
        cf.output_type = actual_output_type;
        cf.input_type = getInputSchema().getRowType();


        if(isCompiled()) {
            logger.error("generating fallback solution for compilable UDF.");
            return cf;
        }

        // we want the LLVM module to be complete (so it can be later shipped over the cluster)
        // hence, simply store pickled code as global variable h& deserialize in constructor blocl.

        if(_pickledCode.length() == 0) {
            // pickle again, here the code is required
            _pickledCode = getPickledCode();

            // failure?
            if(_pickledCode.empty()) {
                logger.error("no pickled code within UDF");
                return cf;
            }
        }

        std::string func_name = "unknown";

        // important to get GIL for this
       python::lockGIL();
        // deserialize python function & store pointer safely!!!
        // then perform call on PyObject*
        PyObject* pythonFunc = python::deserializePickledFunction(python::getMainModule(), _pickledCode.c_str(), _pickledCode.length());


        // check that python function has only a single arg or call into the multiparam version
        auto python_callback_name = "callPythonCodeSingleParam";
        if(1 != python::pythonFunctionPositionalArgCount(pythonFunc)) {
            python_callback_name = "callPythonCodeMultiParam";
        }
        cf.setPythonInvokeName(python_callback_name);

        // dict mode not yet supported...
        if(dictMode()) {
            logger.error("non-compilable python functions do currently not support dict access or hybrid access model");
            Py_XDECREF(pythonFunc);
            python::unlockGIL();
            return cf;
        }

        func_name = python::pythonFunctionGetName(pythonFunc);

        // release GIL here
        python::unlockGIL();

        // generate global variable for string + length
        IRBuilder<> builder(constructorBlock);
        builder.SetInsertPoint(constructorBlock->getFirstNonPHI()); // set to start
        // check if current instruction is a branching type, set insert point before that!
        auto inst = builder.GetInsertPoint();


        auto string_constant = builder.CreateGlobalStringPtr(_pickledCode, "pickled_" + func_name);
        llvm::Value* codeAsPickledString = builder.CreatePointerCast(string_constant, llvm::Type::getInt8PtrTy(context, 0));
        llvm::Value* codeLength = env.i64Const(_pickledCode.length());

        // call now deserialize function
        auto deserializeFuncType = FunctionType::get(Type::getInt8PtrTy(context, 0), {Type::getInt8PtrTy(context, 0),
                                                                                      Type::getInt64Ty(context)}, false);
        auto deserializeFunc = mod->getOrInsertFunction("deserializePythonFunction", deserializeFuncType);

        auto funcPtr = builder.CreateCall(deserializeFunc, {codeAsPickledString, codeLength});


        // call release function in destructor block
        builder.SetInsertPoint(destructorBlock);
        auto releaseFuncType = FunctionType::get(Type::getVoidTy(context), {Type::getInt8PtrTy(context, 0)}, false);
        auto releaseFunc = mod->getOrInsertFunction("releasePythonFunction", releaseFuncType);
        assert(releaseFunc);
        assert(funcPtr->getType() == Type::getInt8PtrTy(context, 0));
        builder.CreateCall(releaseFunc, {funcPtr});

        // store now function pointer for callPythonCode
        cf.function_ptr = funcPtr;

        // use a simple wrapper function to call to python
        // int32_t callPythonCode(PyObject* func, uint8_t** out, int64_t* out_size, uint8_t* input, int64_t input_size, int32_t in_typeHash, int32_t out_typeHash)
        auto wrapperFuncType = FunctionType::get(Type::getInt32Ty(context), {Type::getInt8PtrTy(context, 0),
                                                                             Type::getInt8PtrTy(context, 0)->getPointerTo(0),
                                                                             Type::getInt64PtrTy(context, 0),
                                                                             Type::getInt8PtrTy(context, 0),
                                                                             Type::getInt64Ty(context),
                                                                             Type::getInt32Ty(context),
                                                                             Type::getInt32Ty(context)}, false);

#if LLVM_VERSION_MAJOR < 9
        llvm::Function* wrapperFunc = (llvm::Function*)mod->getOrInsertFunction(python_callback_name, wrapperFuncType);
#else
        llvm::Function* wrapperFunc = cast<llvm::Function>(mod->getOrInsertFunction(python_callback_name, wrapperFuncType).getCallee());
#endif
        assert(wrapperFunc);
        cf.function = wrapperFunc;

        return cf;

    }

    // @TODO: put in separate file
    class LambdaAccessedColumnVisitor : public IPrePostVisitor {
    protected:
        virtual void postOrder(ASTNode *node) override {}
        virtual void preOrder(ASTNode *node) override;

        bool _tupleArgument;
        size_t _numColumns;
        bool _singleLambda;
        std::vector<std::string> _argNames;
        std::unordered_map<std::string, bool> _argFullyUsed;
        std::unordered_map<std::string, std::vector<size_t>> _argSubscriptIndices;

    public:
        LambdaAccessedColumnVisitor() : _tupleArgument(false),
        _numColumns(0), _singleLambda(false) {}


        std::vector<size_t> getAccessedIndices() const;
    };

    std::vector<size_t> LambdaAccessedColumnVisitor::getAccessedIndices() const {

        std::vector<size_t> idxs;

        // first check what type it is
        if(_tupleArgument) {
            assert(_argNames.size() == 1);
            std::string argName = _argNames.front();
            if(_argFullyUsed.at(argName)) {
                for(unsigned i = 0; i < _numColumns; ++i)
                    idxs.push_back(i);
            } else {
                return _argSubscriptIndices.at(argName);
            }
        } else {
            // simple, see which params are fully used.
            for(unsigned i = 0; i < _argNames.size(); ++i) {
                if(_argFullyUsed.at(_argNames[i]))
                    idxs.push_back(i);
            }
        }

        return idxs;
    }

    void LambdaAccessedColumnVisitor::preOrder(ASTNode *node) {
        switch(node->type()) {
            case ASTNodeType::Lambda: {
                assert(!_singleLambda);
                _singleLambda = true;

                NLambda* lambda = (NLambda*)node;
                auto itype = lambda->_arguments->getInferredType();
                assert(itype.isTupleType());

                // how many elements?
                _tupleArgument = itype.parameters().size() == 1 &&
                        itype.parameters().front().isTupleType() &&
                        itype.parameters().front() != python::Type::EMPTYTUPLE;
                _numColumns = _tupleArgument ? itype.parameters().front().parameters().size() : itype.parameters().size();

                // fetch identifiers for args
                for(auto argNode : lambda->_arguments->_args) {
                    assert(argNode->type() == ASTNodeType::Parameter);
                    NIdentifier* id = ((NParameter*)argNode)->_identifier;
                    _argNames.push_back(id->_name);
                    _argFullyUsed[id->_name] = false;
                    _argSubscriptIndices[id->_name] = std::vector<size_t>();
                }

                break;
            }

            case ASTNodeType::Function: {
                assert(!_singleLambda);
                _singleLambda = true;

                NFunction* func = (NFunction*)node;
                auto itype = func->_parameters->getInferredType();
                assert(itype.isTupleType());

                // how many elements?
                _tupleArgument = itype.parameters().size() == 1 &&
                                 itype.parameters().front().isTupleType() &&
                                 itype.parameters().front() != python::Type::EMPTYTUPLE;
                _numColumns = _tupleArgument ? itype.parameters().front().parameters().size() : itype.parameters().size();

                // fetch identifiers for args
                for(auto argNode : func->_parameters->_args) {
                    assert(argNode->type() == ASTNodeType::Parameter);
                    NIdentifier* id = ((NParameter*)argNode)->_identifier;
                    _argNames.push_back(id->_name);
                    _argFullyUsed[id->_name] = false;
                    _argSubscriptIndices[id->_name] = std::vector<size_t>();
                }

                break;
            }

            case ASTNodeType::Identifier: {
                NIdentifier* id = (NIdentifier*)node;
                if(parent()->type() != ASTNodeType::Parameter &&
                parent()->type() != ASTNodeType::Subscription) {
                    // the actual identifier is fully used, mark as such!
                    _argFullyUsed[id->_name] = true;
                }

                break;
            }

            // check whether function with single parameter which is a tuple is accessed.
            case ASTNodeType::Subscription: {
                NSubscription* sub = (NSubscription*)node;

                assert(sub->_value->getInferredType() != python::Type::UNKNOWN); // type annotation/tracing must have run for this...

                auto val_type = sub->_value->getInferredType();
                // access/rewrite makes only sense for dict/tuple types!
                // just simple stuff yet.
                if (sub->_value->type() == ASTNodeType::Identifier &&
                    (val_type.isTupleType() || val_type.isDictionaryType())) {
                    NIdentifier* id = (NIdentifier*)sub->_value;

                    // first check whether this identifier is in args,
                    // if not ignore.
                    if(std::find(_argNames.begin(), _argNames.end(), id->_name) != _argNames.end()) {
                        // no nested paths yet, i.e. x[0][2]
                        if(sub->_expression->type() == ASTNodeType::Number) {
                            NNumber* num = (NNumber*)sub->_expression;

                            // should be I64 or bool
                            assert(num->getInferredType() == python::Type::BOOLEAN ||
                                   num->getInferredType() == python::Type::I64);


                            // can save this one!
                            auto idx = num->getI64();

                            // negative indices should be converted to positive ones
                            if(idx < 0)
                                idx += _numColumns;
                            assert(idx >= 0 && idx < _numColumns);

                            _argSubscriptIndices[id->_name].push_back(idx);
                        } else {
                            // dynamic access into identifier, so need to push completely back.
                            _argFullyUsed[id->_name] = true;
                        }
                    }

                }

                break;
            }
        }
    }

    std::vector<size_t> UDF::getAccessedColumns() {

        // need valid input schema!
        assert(!(getInputSchema() == Schema::UNKNOWN));


        // empty UDF? ==> no accessed columns!
        if(empty())
            return std::vector<size_t>();

        // then, check if UDF is compilable or not.
        // if not, for now there is no way to introspect Python code.
        // hence, just leave it and use all columns!

        std::vector<size_t> idxs;
        auto numCols = getInputSchema().getRowType().parameters().size();
        if(!_isCompiled) {
            for(unsigned i = 0; i < numCols; ++i) {
                idxs.push_back(i);
            }
            return idxs;
        }


        // func is compiled, hence need to look at AST tree to find out.
        auto root = getAnnotatedAST().getFunctionAST();
        LambdaAccessedColumnVisitor acv;
        root->accept(acv);
        return acv.getAccessedIndices();
    }


    class RewriteVisitor : public virtual IReplaceVisitor {
    protected:
        ASTNode* replace(ASTNode* parent, ASTNode* node) override;

        bool _tupleArgument;
        size_t _numColumns;
        bool _singleLambda;
        std::unordered_map<size_t, size_t> _rewriteMap;
        std::vector<std::string> _argNames;

        python::Type _newInputType;
    public:
        RewriteVisitor(const std::unordered_map<size_t, size_t> &rewriteMap) : _tupleArgument(false),
        _numColumns(0), _singleLambda(false), _rewriteMap(rewriteMap) {}


        python::Type getRewrittenInputRowType() const { return _newInputType; }

        std::map<std::string, python::Type> getTypeHints() const {
            std::map<std::string, python::Type> m;
            if(_tupleArgument) {
                assert(_newInputType.parameters().size() == 1);
                m[_argNames.front()] = _newInputType.parameters().front();
            } else {
                size_t pos = 0;
                for(unsigned i = 0; i < _numColumns; ++i) {
                    if(_rewriteMap.find(i) != _rewriteMap.end()) {
                        assert(pos < _newInputType.parameters().size());
                        m[_argNames[i]] = _newInputType.parameters()[pos++];
                    }
                }
            }
            return m;
        }
    };

    ASTNode* RewriteVisitor::replace(ASTNode *parent, ASTNode *node) {

        if(!node)
            return nullptr;

        assert(parent); // must be always valid

        // small hack, b.c. replace visitor does not visit root node per se
        if(parent->type() == ASTNodeType::Lambda &&
            node->type() == ASTNodeType::ParameterList) {
            assert(!_singleLambda);
            _singleLambda = true;

            NLambda *lambda = (NLambda *)parent;
            auto itype = lambda->_arguments->getInferredType();
            assert(itype.isTupleType());

            // how many elements?
            _tupleArgument = itype.parameters().size() == 1 &&
                             itype.parameters().front().isTupleType() &&
                             itype.parameters().front() != python::Type::EMPTYTUPLE;
            _numColumns = _tupleArgument ? itype.parameters().front().parameters().size()
                                         : itype.parameters().size();

            // fetch identifiers for args
            for (auto argNode : lambda->_arguments->_args) {
                assert(argNode->type() == ASTNodeType::Parameter);
                NIdentifier *id = ((NParameter *) argNode)->_identifier;
                _argNames.push_back(id->_name);
            }

            _newInputType = itype;
        }

        if(parent->type() == ASTNodeType::Function &&
           node->type() == ASTNodeType::ParameterList) {
            assert(!_singleLambda);
            _singleLambda = true;

            NFunction *func = (NFunction*)parent;
            auto itype = func->_parameters->getInferredType();
            assert(itype.isTupleType());

            // how many elements?
            _tupleArgument = itype.parameters().size() == 1 &&
                             itype.parameters().front().isTupleType() &&
                             itype.parameters().front() != python::Type::EMPTYTUPLE;
            _numColumns = _tupleArgument ? itype.parameters().front().parameters().size()
                                         : itype.parameters().size();

            // fetch identifiers for args
            for (auto argNode : func->_parameters->_args) {
                assert(argNode->type() == ASTNodeType::Parameter);
                NIdentifier *id = ((NParameter *) argNode)->_identifier;
                _argNames.push_back(id->_name);
            }

            _newInputType = itype;
        }


        // two options: if tupleArgument, rewrite subscript access
        // if not, rewrite parameter list of lambda
        switch(node->type()) {

            case ASTNodeType::ParameterList: {
                NParameterList* paramList = (NParameterList*)node;

                // change type
                if(parent->type() == ASTNodeType::Lambda ||
                   parent->type() == ASTNodeType::Function) {

                    std::vector<python::Type> reducedArgTypes;
                    if(_tupleArgument) {
                        auto ftype = parent->getInferredType();
                        assert(ftype.isFunctionType());

                        // parameter type should be tuple with one element
                        auto ptype = ftype.getParamsType();
                        assert(ptype.isTupleType());
                        assert(ptype.parameters().size() == 1);
                        assert(paramList->getInferredType() == ptype);

                        // single argument
                        ptype = ptype.parameters().front();

                        // reduce
                        for(unsigned i = 0; i < ptype.parameters().size(); ++i) {
                            if(_rewriteMap.find(i) != _rewriteMap.end()) {
                                reducedArgTypes.push_back(ptype.parameters()[i]);
                            }
                        }

                        // create function type
                        ptype = python::Type::makeTupleType({python::Type::makeTupleType(reducedArgTypes)});
                        ftype = python::TypeFactory::instance()
                                .createOrGetFunctionType(ptype,
                                        ftype.getReturnType());
                        parent->setInferredType(ftype);
                        node->setInferredType(ptype);
                        _newInputType = ptype;

                        // update also single arg
                        assert(paramList->_args.size() == 1);
                        paramList->_args.front()->setInferredType(ptype);
                    } else {

                        auto ftype = parent->getInferredType();
                        assert(ftype.isFunctionType());

                        // parameter type should be tuple with numColumns elements
                        auto ptype = ftype.getParamsType();
                        assert(ptype.isTupleType());
                        assert(ptype.parameters().size() == _numColumns);
                        assert(_argNames.size() == _numColumns);
                        assert(paramList->_args.size() == _numColumns);

                        // need to replace paramlist with params to rewrite
                        // reduce
                        std::vector<ASTNode*> keepNodes;

                        for(unsigned i = 0; i < _numColumns; ++i) {
                            if(_rewriteMap.find(i) != _rewriteMap.end()) {
                                keepNodes.push_back(paramList->_args[i]);
                                reducedArgTypes.push_back(ptype.parameters()[i]);
                            } else {
                                delete paramList->_args[i];
                            }
                        }
                        paramList->_args = keepNodes;

                        // update type
                        ptype = python::Type::makeTupleType(reducedArgTypes);
                        ftype = python::TypeFactory::instance()
                                .createOrGetFunctionType(ptype,
                                                         ftype.getReturnType());
                        parent->setInferredType(ftype);
                        node->setInferredType(ptype);
                        _newInputType = ptype;
                    }
                }
                break;
            }

            case ASTNodeType::Subscription: {
                // change if tupleArgument
                if(_tupleArgument) {
                    // perform mapping
                    NSubscription* sub = (NSubscription*)node;
                    assert(sub->_value);
                    assert(sub->_expression);

                    // rewrite for special case only
                    // i.e. when value is a number & expression is the identifier of the function
                    assert(_argNames.size() == 1);
                    if(sub->_value->type() == ASTNodeType::Identifier &&
                    sub->_expression->type() == ASTNodeType::Number) {
                        // matches argname?
                        NIdentifier* id = (NIdentifier*)sub->_value;

                        if(id->_name == _argNames.front()) {
                            assert(sub->_expression->getInferredType() == python::Type::I64 ||
                            sub->_expression->getInferredType() == python::Type::BOOLEAN);

                            // there are two options:
                            // opt1: expression is a simple number
                            if(sub->_expression->type() == ASTNodeType::Number) {
                                auto old_idx = ((NNumber*)sub->_expression)->getI64();
                                // correct for negative indices
                                if(old_idx < 0)
                                    old_idx += _numColumns;


                                // assert(0 <= old_idx && old_idx < _numColumns);

                                auto it = _rewriteMap.find(old_idx);
                                int64_t new_idx = 0;
                                if(it == _rewriteMap.end()) {

                                    // rewrite algorithm relies on partial rewrites...
                                    new_idx = old_idx;

                                    //throw std::runtime_error("bad, idx not found in rewrite Map!");
                                    // i.e. this means, it probably is not required in the end result?
                                } else {
                                    new_idx = (int64_t)_rewriteMap[old_idx];
                                }

                                // replace
                                NNumber* num_new = new NNumber(std::to_string(new_idx));

                                delete sub->_expression;
                                sub->_expression = num_new;
                            } else {
                                // opt2: it is something else
                                // trick is to add a NBinaryOp to remap the index
                                throw std::runtime_error("dynamic indexing does not work with pushdown yet! Need to implement ops for that...");
                                #warning "For CSV Selection Pushdown, need to do this. Add this some time when there is time..."
                                // @TODO
                            }
                        }
                    }
                }
            }
        }

        // no replacement
        return node;
    }

    void UDF::rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) {

        // is UDF compilable? if not, can't rewrite (fallback)
        if(!isCompiled() && !empty())
            return;

        using namespace std;
        auto root = getAnnotatedAST().getFunctionAST();

        // empty UDF, skip.
        if(!root) {

            auto oldSchema = getInputSchema();
            auto old_input_types = oldSchema.getRowType().parameters();
            auto numColumns = oldSchema.getRowType().parameters().size();

            // update schema according to rewrite Map (i.e. go through in sorted fashion)
            vector<size_t> colsToKeep;
            for(auto keyval : rewriteMap) {
                if(keyval.first < numColumns)
                    colsToKeep.emplace_back(keyval.first);
            }
            sort(colsToKeep.begin(), colsToKeep.end());

            // new input type (account for reordering!)
            vector<python::Type> newInputTypes(colsToKeep.size());
            int i = 0;
            for(auto col : colsToKeep) {
                assert(0 <= col && col < numColumns);
                newInputTypes[i++] = old_input_types[col];
            }

            // create new input type
            auto new_input_type = python::Type::makeTupleType(newInputTypes);
            auto new_schema = Schema(oldSchema.getMemoryLayout(), new_input_type);
            setInputSchema(new_schema);
            setOutputSchema(new_schema);

            return;
        }

        // call the replace visitor
        RewriteVisitor rv(rewriteMap);
        root->accept(rv);
        auto oldSchema = getInputSchema();
        setInputSchema(Schema(oldSchema.getMemoryLayout(), rv.getRewrittenInputRowType()));

        // need to update type hints too. Else, everything gets overwritten from the code generator.
        auto hints = rv.getTypeHints();
        for(auto el : hints)
            getAnnotatedAST().addTypeHint(el.first, el.second);

        // call define types then again
        getAnnotatedAST().defineTypes(true);

#ifndef NDEBUG
#ifdef GENERATE_PDFS
        GraphVizGraph graph;
        graph.createFromAST(getAnnotatedAST().getFunctionAST(), true);
        graph.saveAsPDF(std::to_string(g_func_counter++) + "_06_rewritten_AST.pdf");
#else
        Logger::instance().defaultLogger().debug("saving rewritten Python AST to PDF skipped.");
#endif
#endif
    }

    bool UDF::rewriteDictAccessInAST(const std::vector<std::string> &columnNames, const std::string& parameterName) {

        // UDF compiled? if not, nothing to be done
        if(!isCompiled())
            return true;

        _dictAccessFound = false; // per default, false.
        _rewriteDictExecuted = true;
        // no column names given?
        // => skip
        if(columnNames.empty())
            return true;

        auto root = getAnnotatedAST().getFunctionAST();
        assert(root);


        // if no param is specified, attempt only for single param UDF!

        // only call when first arg is tuple type (else rewrite makes NO sense)
        // no type check here, because this function may be called before typing
        auto inputParams = getInputParameters();
        std::string parameter = ""; // empty string!

        // in inputParams?
        if(!parameterName.empty() && std::find_if(inputParams.begin(), inputParams.end(), [&](const std::tuple<std::string, python::Type>& t) {
            return std::get<0>(t) == parameterName; }) != inputParams.end())
            parameter = parameterName;
        else if(!parameterName.empty()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().debug("attempting to rewrite param " + parameterName + " which is not in param list...");
#endif
        }

        if(inputParams.size() == 1 && parameter.empty())
            parameter = std::get<0>(inputParams.front());

        // got a param to rewrite?
        if(!parameter.empty()) {
            ColumnRewriteVisitor crv(columnNames, parameter);
            root->accept(crv);

            if(crv.failed())
                return false;

            _dictAccessFound = crv.dictAccessFound();

#ifndef NDEBUG
#ifdef GENERATE_PDFS
            GraphVizGraph graph;
            graph.createFromAST(getAnnotatedAST().getFunctionAST());
            graph.saveAsPDF(std::to_string(g_func_counter++) + "_05_rewritten_AST.pdf");
#else
            Logger::instance().defaultLogger().debug("saving rewritten Python AST to PDF skipped.");
#endif
#endif
        } else {
            _dictAccessFound = false; // if more than one input param, no dictionary access!!
        }

        _rewriteDictExecuted = true;

        return true;
    }

    void UDF::saveASTToPDF(const std::string &filePath) {
        getAnnotatedAST().writeGraphToPDF(filePath);
    }

    bool UDF::dictMode() const {
        if(!_ast.getFunctionAST())
            return false;
#ifndef NDEBUG
        auto root = _ast.getFunctionAST();
        assert(root);
        auto funcName = root->type() == ASTNodeType::Function ? ((NFunction*)root)->_name->_name : _code;
        //std::cout<<"function "<<funcName<<(_dictAccessFound ? ": dict mode" : ": tuple mode")<<std::endl;
#endif
        return _dictAccessFound;
    }

    bool UDF::isPythonLambda() const {
        return _ast.getFunctionAST()->type() == ASTNodeType::Lambda;
    }

    std::string UDF::pythonFunctionName() const {
        if(isPythonLambda())
            return "<lambda>";
        else {
            auto root = _ast.getFunctionAST();
            assert(root->type() == ASTNodeType::Function);
            return dynamic_cast<NFunction*>(root)->_name->_name;
        }
    }

    void UDF::logTypingErrors(bool print) const {
        auto& logger = Logger::instance().logger("type inference");

        // store errors for access via lastTypingErrors()

        if(print) {
            logger.error("Could not infer a valid typing for UDF.");
            // go over messages & log them out as well
            for(auto msg : _ast.typingErrMessages())
                logger.error(msg);
        }
    }



    bool UDF::hintSchemaWithSample(const std::vector<PyObject *>& sample, const python::Type& inputRowType, bool acquireGIL) {
        TraceVisitor tv(inputRowType);

        auto funcNode = _ast.getFunctionAST();

        // trace all samples through AST and annotate
        // Note: it's the responsibility of the caller to only trace with an appropriate input schema...
        // => i.e. remove bad samples!

        if(acquireGIL)
            python::lockGIL();

        // add closure environment to tracer
        tv.setClosure(_ast.globals(), false);

        for(auto args : sample)
            tv.recordTrace(funcNode, args);
        if(acquireGIL)
            python::unlockGIL();

        // only exceptions produced by sample?
        // => no type can be inferred!


        Schema inputSchema;
        // fetch majority type from TraceVisitor
        if(inputRowType != python::Type::UNKNOWN)
            inputSchema = Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(inputRowType));
        else
            inputSchema = Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(tv.majorityInputType()));
        //_ast.setUnpacking(tv.unpackParams());

        auto row_type = tv.majorityOutputType().isExceptionType() ? tv.majorityOutputType() : python::Type::propagateToTupleType(tv.majorityOutputType());
        _outputSchema = Schema(Schema::MemoryLayout::ROW, row_type);

        // run type annotator on top of tree which has been equipped with annotations now
        hintInputSchema(Schema(Schema::MemoryLayout::ROW, inputSchema.getRowType()), false, false); // TODO: could do this directly in tracevisitor as well, but let's separate concerns here...

        _inputSchema = inputSchema; // somehow hintInputSchema overwrites current one? => Restore.

        return !_outputSchema.getRowType().isIllDefined();
    }

    static void py_error_throw(bool acquiredGIL) {
        if(PyErr_Occurred()) {
            PyErr_Clear();
        }
        if(acquiredGIL)
            python::unlockGIL();
        throw std::runtime_error("python error occured, details: ");
    }

    std::vector<PyObject*> UDF::executeBatchViaInterpreter(const std::vector<PyObject *> &in_rows,
                                                            const std::vector<std::string> &columns,
                                                            bool acquireGIL) const {
        if(_pickledCode.empty())
            throw std::runtime_error("no pickled code, fatal internal error");

        if(acquireGIL)
            python::lockGIL();
        auto pFunc = python::deserializePickledFunction(python::getMainModule(),
                                                        _pickledCode.c_str(), _pickledCode.size());
        py_error_throw(acquireGIL);
        if(!pFunc)
            throw std::runtime_error("bad function extract");

        std::vector<PyObject*> out_rows;

        for(auto in_row : in_rows) {
            // process via func
            auto pcr = dictMode()  && !columns.empty() ? python::callFunctionWithDictEx(pFunc, in_row, columns) :
                  python::callFunctionEx(pFunc, in_row);
            if(pcr.exceptionCode == ExceptionCode::SUCCESS)
                out_rows.emplace_back(pcr.res);
        }

        if(acquireGIL)
            python::unlockGIL();

        return out_rows;
    }
}