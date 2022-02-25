//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <TraceVisitor.h>
#include <stdexcept>

namespace tuplex {

    void TraceVisitor::recordTrace(ASTNode *node, PyObject *args) {
        assert(node && args);
        _args = args;
        _functionSeen = false;
        _evalStack.clear();
        _symbols.clear();

        // jump buf & handle error
        try {
            // call code with longjmp
            node->accept(*this);
        } catch(TraceException& exc) {
            // nothing todo...
        }

        // inc. counter
        _numSamplesProcessed++;
    }

    void TraceVisitor::fetchAndStoreError() {
        PyObject *type, *value, *traceback;
        PyErr_Fetch(&type, &value, &traceback);

        assert(type && value); // traceback might be nullptr for one liners.

        using namespace std;

        // https://docs.python.org/3/c-api/object.html
        PyObject* e_msg = PyObject_Str(value);
        PyObject* e_type = PyObject_GetAttrString(type, "__name__");
        PyObject* e_lineno = traceback ? PyObject_GetAttrString(traceback, "tb_lineno") : nullptr;
        auto exceptionMessage = python::PyString_AsString(e_msg);
        auto exceptionClass = python::PyString_AsString(e_type);
        auto exceptionLineNo = e_lineno ? PyLong_AsLong(e_lineno) : 0;
        auto exceptionCode = python::translatePythonExceptionType(type);
        Py_XDECREF(e_msg);
        Py_XDECREF(e_type);
        Py_XDECREF(e_lineno);
        // walk up traceback
        // PyObject *tbnext = traceback;
        // while(tbnext) {
        //     cout<<"tb line no"<<PyLong_AsLong(PyObject_GetAttrString(tbnext, "tb_lineno"))<<endl;
        //     cout<<"tb frame"<<python::PyString_AsString(PyObject_Str(PyObject_GetAttrString(tbnext, "tb_frame")))<<endl;
        //     tbnext = PyObject_GetAttrString(tbnext, "tb_next");
        // }

        Py_XDECREF(type);
        Py_XDECREF(value);
        Py_XDECREF(traceback);
        PyErr_Clear();

        // add to exceptions
        _exceptions.push_back(exceptionClass);
        // store unknown to mark which rows failed
        _retColTypes.push_back(vector<python::Type>{python::Type::UNKNOWN});
    }

    void TraceVisitor::errCheck() {
        // check for python errors & jump out
        if(PyErr_Occurred()) {
            // store result
            fetchAndStoreError();

            // lngjmp with error code 1
            throw TraceException();
        }
    }

    // what about variables???

    // leaf nodes
    void TraceVisitor::visit(NNone *node) {
        Py_INCREF(Py_None);
        addTraceResult(node, TraceItem(Py_None));
    }

    void TraceVisitor::visit(NNumber *node) {
        // leaf node, check string then issue python type
        if(node->getInferredType() == python::Type::I64)
            addTraceResult(node, TraceItem(PyLong_FromLongLong(node->getI64())));
        else if(node->getInferredType() == python::Type::F64)
            addTraceResult(node, TraceItem(PyFloat_FromDouble(node->getF64())));
        else throw std::runtime_error("weird, Number node type not defined");
    }

    void TraceVisitor::visit(NIdentifier *node) {

        // leave node

        // symbol lookup, if not found abort with name error!

        auto it = std::find_if(_symbols.begin(), _symbols.end(), [&](const TraceItem& ti) { return ti.name == node->_name; });

        if(it != _symbols.end()) {
            // a symbol was accessed. Is this symbol by chance an input parameter?
            if(it->flags & TI_FLAGS_INPUT_PARAMETER) {
                inc_access_path(it->name);
            }

            // push value of symbol onto eval stack!
            addTraceResult(node, *it);
        } else {
            // check module
            auto mainMod = python::getMainModule(); assert(mainMod);
            auto mainDict = PyModule_GetDict(mainMod); assert(mainDict);

            auto sym = PyDict_GetItemString(mainDict, node->_name.c_str());

            if(sym) {
                addTraceResult(node, TraceItem(sym, node->_name));
            } else {

                // check builtins
                auto builtins = PyDict_GetItemString(mainDict, "__builtins__");
                auto builtinDict = PyModule_GetDict(builtins); assert(builtinDict);

                sym = PyDict_GetItemString(builtinDict, node->_name.c_str());
                if(sym)
                    addTraceResult(node, TraceItem(sym, node->_name));
                else {
                    PyErr_SetString(PyExc_NameError, ("could not find identifier " + node->_name).c_str());

                    // i.e., could early exit function...
                    // error("todo: abort here with NameError exception because name " + node->_name + " was not found...");
                }
            }
        }

        errCheck();
    }

    void TraceVisitor::visit(NBoolean *node) {
       addTraceResult(node, TraceItem(node->_value ? Py_True : Py_False));
    }

    void TraceVisitor::visit(NString *node) {

        // stupid string preprocessing...
        auto val = node->value();

        // super simple, just push string node to stack!
        addTraceResult(node, TraceItem(python::PyString_FromString(val.c_str())));
    }

    // non-leaf nodes, recursive calls are carried out for these
    void TraceVisitor::visit(NParameter *node) {
        throw std::runtime_error("not yet supported");
        ApatheticVisitor::visit(node);
    }

    void TraceVisitor::visit(NParameterList *node) {
        throw std::runtime_error("not yet supported");
        ApatheticVisitor::visit(node);
    }

    void TraceVisitor::visit(NFunction *node) {
        unpackFunctionParameters(node->_parameters->_args);

        // run suite
        node->_suite->accept(*this);

        // // is there a return value?
        // logger().info("return type is: " + python::typeName(_retValue.value));
    }

    void TraceVisitor::visit(NBinaryOp *node) {
        ApatheticVisitor::visit(node);

        // @TODO: logical and and or operators.
        // => special treatment there.

        // there should be at least 2 nodes on the stack!
        assert(_evalStack.size() >= 2);

        auto right = _evalStack.back();
        _evalStack.pop_back();
        auto left = _evalStack.back();
        _evalStack.pop_back();

        // import module operator
        auto opMod = PyImport_ImportModule("operator"); // import operator
        assert(opMod);
        auto opModDict = PyModule_GetDict(opMod);
        assert(opModDict);

        // lookup operations (need a test for that)
        // i.e. map token type to function name in operator
        // module
        // cf. https://docs.python.org/3.9/library/operator.html#mapping-operators-to-functions
        std::unordered_map<TokenType, std::string> opLookup{{TokenType::PLUS, "add"},
                                                            {TokenType::AMPER, "and_"}, // bitwise and &
                                                            {TokenType::VBAR, "or_"}, // bitwise or |
                                                            {TokenType::CIRCUMFLEX, "xor"}, // bitwise xor ^
                                                            {TokenType::DOUBLESLASH, "floordiv"},
                                                            {TokenType::LEFTSHIFT, "lshift"},
                                                            {TokenType::PERCENT, "mod"},
                                                            {TokenType::STAR, "mul"},
                                                            {TokenType::DOUBLESTAR, "pow"},
                                                            {TokenType::RIGHTSHIFT, "rshift"},
                                                            {TokenType::MINUS, "sub"},
                                                            {TokenType::SLASH, "truediv"},
                                                            {TokenType::CIRCUMFLEX, "xor"}};


        auto it = opLookup.find(node->_op);
        if(it == opLookup.end())
            throw std::runtime_error("Operator " + opToString(node->_op) + " not yet supported in TraceVisitor");

        std::string op_name = it->second;

        auto func = PyDict_GetItemString(opModDict, op_name.c_str());
        assert(func);
        auto args = PyTuple_Pack(2, left.value, right.value);
        auto ret_obj = PyObject_Call(func, args, nullptr);
        // perform python operation & check for errors
        // confer https://docs.python.org/3/library/operator.html
        errCheck();

        // only add trace result if no err happened.
        addTraceResult(node, TraceItem(ret_obj));
    }

    void TraceVisitor::visit(NUnaryOp *node) {
        ApatheticVisitor::visit(node);

        // there should be at least one node on the stack!
        assert(_evalStack.size() >= 1);

        auto item = _evalStack.back();
        _evalStack.pop_back();

        // import module operator
        auto opMod = PyImport_ImportModule("operator"); // import operator
        assert(opMod);
        auto opModDict = PyModule_GetDict(opMod);
        assert(opModDict);

        // lookup operations (need a test for that)
        // i.e. map token type to function name in operator
        // module
        // cf. https://docs.python.org/3.9/library/operator.html#mapping-operators-to-functions
        std::unordered_map<TokenType, std::string> opLookup{{TokenType::PLUS, "pos"},
                                                            {TokenType::MINUS, "neg"},
                                                            {TokenType::TILDE, "inv"},
                                                            {TokenType::NOT, "not_"}};


        auto it = opLookup.find(node->_op);
        if(it == opLookup.end())
            throw std::runtime_error("Operator " + opToString(node->_op) + " not yet supported in TraceVisitor");

        std::string op_name = it->second;

        auto func = PyDict_GetItemString(opModDict, op_name.c_str());
        assert(func);
        auto args = PyTuple_Pack(1, item.value);
        addTraceResult(node, TraceItem(PyObject_Call(func, args, nullptr)));

        // perform python operation & check for errors
        // confer https://docs.python.org/3/library/operator.html

        errCheck();
    }

    void TraceVisitor::visit(NSuite *node) {
        for (auto & stmt : node->_statements) {
            // check whether statement can be optimized. Can be optimized iff result of optimizeNext is a different
            // memory address
            setLastParent(node);
            stmt->accept(*this);

            if(stmt->type() == ASTNodeType::Break) {
                _loopBreakStack.back() = true;
                break;
            }
            if(stmt->type() == ASTNodeType::Continue) {
                break;
            }
        }
    }

    void TraceVisitor::visit(NModule *node) {
        ApatheticVisitor::visit(node);
        throw std::runtime_error("not yet supported");
    }

    void TraceVisitor::unpackFunctionParameters(const std::vector<ASTNode*> &astArgs) {
        // visiting function first time? no nested support yet!
        if(!_functionSeen) {
            _functionSeen = true;

            std::vector<PyObject*> extractedArgs;

            // push arguments on stack
            _columnNames.clear();
            for(int i = 0; i < astArgs.size(); ++i) {
                assert(astArgs[i]->type() == ASTNodeType::Parameter);
                auto id = dynamic_cast<NIdentifier*>(dynamic_cast<NParameter*>(astArgs[i])->_identifier);
                _columnNames.push_back(id->_name);
            }

            if(astArgs.size() == 1) {
                auto id = dynamic_cast<NIdentifier*>(dynamic_cast<NParameter*>(astArgs.front())->_identifier);
                _symbols.emplace_back(TraceItem::param(_args, id->_name));
                extractedArgs.push_back(_args);
            } else {
                assert(PyTuple_Check(_args));

                size_t numProvidedArgs = PyTuple_Size(_args);
                for(int i = 0; i < std::min(numProvidedArgs, astArgs.size()); ++i) {
                    auto id = dynamic_cast<NIdentifier*>(dynamic_cast<NParameter*>(astArgs[i])->_identifier);
                    auto arg = PyTuple_GetItem(_args, i);
                    _symbols.emplace_back(TraceItem::param(arg, id->_name));
                    extractedArgs.push_back(arg);
                }
            }

            // record input types for schema inference!
            std::vector<python::Type> types;
            for(auto a : extractedArgs) {
                types.emplace_back(python::mapPythonClassToTuplexType(a));
            }
            _colTypes.emplace_back(types);
        } else throw std::runtime_error("no nested functions supported in tracer yet!");


        // if input row type is given, check!
        if(_inputRowType != python::Type::UNKNOWN) {
            if(python::Type::makeTupleType(_colTypes.back()) != _inputRowType) {

                // special case: coltypes could be single element & tuple!
                if(_colTypes.back().size() == 1 && _colTypes.back().front() == _inputRowType) {
                    // update colTypes accordingly!
                    for(auto& colType : _colTypes)
                        colType = _inputRowType.parameters();
                } else {
                    PyErr_SetString(PyExc_TypeError, "sample object given doesn't match input row type");
                }
            }
        }

        errCheck();
    }

    void TraceVisitor::visit(NLambda *node) {

        unpackFunctionParameters(node->_arguments->_args);

        // visit children
        node->_expression->accept(*this);

        // all good?

        // ==> then fetch value from eval stack
        if(_evalStack.size() != 1) {
            // no return!
            throw std::runtime_error("lambda did not return, wrong syntax??");
        }

        auto ti = _evalStack.back();
        _evalStack.pop_back();
        _retValue = ti;

        // print return value ==> annotate function with it! i.e. add slot for it!
        // @TODO: add annotation object (ptr) to astnodes!

        // record type
        auto retType = python::mapPythonClassToTuplexType(_retValue.value);
        if(retType.isTupleType() && !retType.parameters().empty()) {
            _retColTypes.emplace_back(retType.parameters());
        } else {
            _retColTypes.emplace_back(std::vector<python::Type>{retType});
        }
        // ==> types, branches etc.!!! ==> required for both dict + null value optimization...
    }

    void TraceVisitor::visit(NAwait *node) {
        ApatheticVisitor::visit(node);
        throw std::runtime_error("not yet supported");
    }

    void TraceVisitor::visit(NStarExpression *node) {
        ApatheticVisitor::visit(node);
        throw std::runtime_error("not yet supported");
    }

    void TraceVisitor::visit(NCompare *node) {
        using namespace std;

        ApatheticVisitor::visit(node);

        // there should _comps.size() + 1 elements on the stack
        assert(_evalStack.size() >= 1 + node->_ops.size());

        vector<TraceItem> ti_vals;
        for(int i = 0; i < node->_ops.size() + 1; ++i) {
            ti_vals.push_back(_evalStack.back()); _evalStack.pop_back();
        }
        std::reverse(ti_vals.begin(), ti_vals.end());

        // now truth value testing, single element?
        auto res = ti_vals.front();

        // IS and IS NOT are equivalent to id(L) == id(R) and id(L) != id(R).
        std::unordered_map<TokenType, int> cmpLookup{{TokenType::EQEQUAL, Py_EQ},
                                                     {TokenType::NOTEQUAL, Py_NE},
                                                     {TokenType::LESS, Py_LT},
                                                     {TokenType::LESSEQUAL, Py_LE},
                                                     {TokenType::GREATER, Py_GT},
                                                     {TokenType::GREATEREQUAL, Py_GE}};

        // eval
        for(int i = 0; i < node->_ops.size(); ++i) {
            auto op = node->_ops[i];

            // based on op, decide value of result.
            if(op == TokenType::IS || op == TokenType::ISNOT) {
                // `x is y` in Python is equivalent to `id(x) == id(y)`
                // so we just compare pointers for equality and return the corresponding PyBool.
                
                assert(i+1 < ti_vals.size());
                bool finalResult = (ti_vals[i+1].value == res.value);
                // invert result if op is ISNOT.
                finalResult = (op == TokenType::IS) ? finalResult : !finalResult;
                res.value = finalResult ? Py_True : Py_False;
            } else {
                auto it = cmpLookup.find(op);
                if(it == cmpLookup.end())
                        throw std::runtime_error("Operator " + opToString(op) + " not yet supported in TraceVisitor/NCompare");
                int opid = it->second;

                res.value = PyObject_RichCompare(res.value, ti_vals[i + 1].value, opid);
            }

            // NULL? ==> failure!
            assert(res.value);
        }

        addTraceResult(node, res);

        errCheck();
    }

    void TraceVisitor::visit(NIfElse *node) {

        // this here is one of the more interesting statements...
        // ==> why? because this is where tracing starts!

        // PyObject_IsTrue and PyObject_Not are the magic functions...

        // visit condition, then decide based on value where to continue
        node->_expression->accept(*this);

        // init annotations for both if and else (need to be there b.c. of typeannotator visitor)
        node->_then->annotation(); // creates annotation with visit count 0 if not existing
        if(node->_else)
            node->_else->annotation(); // creates annotation with visit count 0 if not existing

        // use PyObject_IsTrue for the condition! => do ONLY follow the branch which is attained (tracing mode!)
        assert(_evalStack.size() >= 1);
        auto cond = _evalStack.back(); _evalStack.pop_back();

        // always count visit on ifelse node, so pure if statement can be distinguished!
        node->annotation(); // create annotation with count 0, if it doesn't exist yet!
        node->annotation().numTimesVisited++;

        // Only visit branch for which condition is true
        // and add annotation which branch was visited
        if(PyObject_IsTrue(cond.value)) {
            // visit if block!
            node->_then->accept(*this);
            node->_then->annotation().numTimesVisited++; // inc after, important b.c. of errors!
            node->_then->annotation().branchTakenSampleIndices.insert(_numSamplesProcessed+1);
        } else {
            // check if else block is there. If so
            if(node->_else) {
                node->_else->accept(*this);
                node->_else->annotation().numTimesVisited++; // post-inc, important b.c. of errors!
                node->_else->annotation().branchTakenSampleIndices.insert(_numSamplesProcessed+1);
            }
        }
    }

    void TraceVisitor::visit(NTuple *node) {
        ApatheticVisitor::visit(node);

        std::vector<TraceItem> elements;
        for(unsigned i = 0; i < node->_elements.size(); ++i) {
            elements.emplace_back(_evalStack.back()); _evalStack.pop_back();
        }
        std::reverse(elements.begin(), elements.end());

        // form new tuple element!

        auto tupleObj = PyTuple_New(elements.size());
        for(int i = 0; i < elements.size(); ++i)
            PyTuple_SET_ITEM(tupleObj, i, elements[i].value);

        TraceItem ti(tupleObj);
        addTraceResult(node, ti);
    }

    void TraceVisitor::visit(NDictionary *node) {
        ApatheticVisitor::visit(node);
        throw std::runtime_error("dict in TraceVisitor.cc not yet supported");
    }

    void TraceVisitor::visit(NSubscription *node) {
        ApatheticVisitor::visit(node);
        // there should be two values on the evalStack! I.e. one for what is indexed, and one for the index
        assert(_evalStack.size() >= 2);

        auto ti_index = _evalStack.back(); _evalStack.pop_back();
        auto ti_value = _evalStack.back(); _evalStack.pop_back();

#ifndef NDEBUG
        // using namespace std;
        // cout<<"index: "; PyObject_Print(ti_index.value, stdout, 0);
        // cout<<endl;
        // cout<<"value: "; PyObject_Print(ti_value.value, stdout, 0);
        // cout<<endl;
#endif

        // special case: ti_value is input parameter! mark access path.
        if(ti_value.flags & TI_FLAGS_INPUT_PARAMETER) {
            assert(!ti_value.name.empty());
            Py_XINCREF(ti_index.value);
            auto key = python::PyString_AsString(ti_index.value);
            inc_access_path(ti_value.name + "[" + key + "]");
        }

        PyObject *res = nullptr;

        // index
        res = PyObject_GetItem(ti_value.value, ti_index.value);

        if(!res)
           errCheck();
        addTraceResult(node, TraceItem(res));
    }

    void TraceVisitor::visit(NReturn *node) {

        if(!node->_expression)
            error("UDFs should have a return value!");

        ApatheticVisitor::visit(node);

        // end evaluation
        assert(_evalStack.size() >= 1);

        // nothing more to evaluate...
        _retValue = _evalStack.back();
        _evalStack.pop_back();

        // record type
        auto retType = python::mapPythonClassToTuplexType(_retValue.value);
        if(retType.isTupleType() && !retType.parameters().empty()) {
            _retColTypes.emplace_back(retType.parameters());
        } else {
            _retColTypes.emplace_back(std::vector<python::Type>{retType});
        }
    }

    void TraceVisitor::visit(NAssign *node) {
        // simple: update symbol on left hand side ( can be single identifier only!)
        if(node->_target->type() != ASTNodeType::Identifier)
            error("only identifier as target for assign statement yet supported");

        NIdentifier* id = dynamic_cast<NIdentifier*>(node->_target); assert(id);

        // do not visit target, only visit value!
        node->_value->accept(*this);

        // for the expression there should be a value on the evalStack, pop it!
        assert(_evalStack.size() >= 1);

        auto ti = _evalStack.back(); _evalStack.pop_back();

        // update or insert new symbol!
        auto it = std::find_if(_symbols.begin(), _symbols.end(), [&id](const TraceItem& ti) {
            return ti.name == id->_name;
        });
        if(it == _symbols.end()) {
            _symbols.push_back(TraceItem(ti.value, id->_name));
        } else {
            // check if there is type change in loop
            for (auto & el : _symbolsTypeChangeStack) {
                if(std::find(el.first.begin(), el.first.end(), it->name) != el.first.end()) {
                    // symbol is created before loop
                    if(!el.second && it->value->ob_type->tp_name != ti.value->ob_type->tp_name) {
                        // type changed for it
                        el.second = true;
                    }
                }
            }

            it->value = ti.value; // update value of symbol!
        }
    }

    void TraceVisitor::visit(NCall *node) {
        using namespace std;

        ApatheticVisitor::visit(node);

        // TODO: better sys table visitor required here...
        auto numArgs = node->_positionalArguments.size();

        // eval stack should have all params
        assert(_evalStack.size() >= 1 + numArgs);

        // let's check what's on the stack...
        // ==> i.e. first is the function to call and then all the args (in wrong order!)
        auto ti_func = _evalStack.back(); _evalStack.pop_back();

        vector<TraceItem> ti_args;
        for(int i = 0; i < numArgs; ++i) {
            ti_args.push_back(_evalStack.back()); _evalStack.pop_back();
        }
        // reverse because of stack order!
        std::reverse(ti_args.begin(), ti_args.end());

        // now, eval func
        assert(ti_func.value);

        // throw TypeError
        if(!PyCallable_Check(ti_func.value))
            error(python::typeName(ti_func.value) + " is not callable");

        // create python args
        PyObject* args = PyTuple_New(numArgs);
        for(unsigned i = 0; i < numArgs; ++i) {
            Py_XINCREF(ti_args[i].value);
            PyTuple_SET_ITEM(args, i, ti_args[i].value);
        }

        auto res = PyObject_Call(ti_func.value, args, nullptr);

        // only add trace result if call succeeded.
        if(!res)
            errCheck(); // jumps out of control flow
        addTraceResult(node, TraceItem(res));
    }

    void TraceVisitor::visit(NAttribute *node) {
        // do not visit both children, just need to visit value.
        // attribute is a fixed identifier!
        assert(node->_attribute->type() == ASTNodeType::Identifier);
        auto attr = node->_attribute->_name;

        // visit value
        node->_value->accept(*this);

        assert(_evalStack.size() >= 1);
        auto ti_value = _evalStack.back(); _evalStack.pop_back();
        auto value = ti_value.value;

        // fetch attribute from python function!
        auto res = PyObject_GetAttrString(value, attr.c_str());

        // if res is null, attribute error!
        if(!res) {
            // use simple interface, could expand to use lineno & Co later...
            std::string mod_name = "unknown";

            // get module name
            auto name_obj = PyObject_GetAttrString(value, "__name__");
            if(name_obj) {
                mod_name = python::PyString_AsString(name_obj);
                Py_XDECREF(name_obj);
            }

            // blueprint: module 'numpy' has no attribute 'zeroes'
            auto exc_string = "module '" + mod_name + "' has no attribute '" + attr + "'";
            PyErr_SetString(PyExc_AttributeError, exc_string.c_str());
            // PyErr_SyntaxLocationEx(filename, lineno, col_offset); <-- get from AST

            errCheck();
        }
        // push

        addTraceResult(node, TraceItem(res));
    }

    void TraceVisitor::visit(NSlice *node) {
        if(node->_slices.size() != 1)
            error("only one slice so far supported!!!");

        ApatheticVisitor::visit(node);

        // make sure there is at least one slice
        assert(_evalStack.size() >= 2);

        // get slice
        auto ti_slice = _evalStack.back(); _evalStack.pop_back();

        auto ti_value = _evalStack.back(); _evalStack.pop_back();

        // getitem with slice
        auto res = PyObject_GetItem(ti_value.value, ti_slice.value);

        // @TODO: erorr??
        assert(res);

        addTraceResult(node, TraceItem(res));
    }

    void TraceVisitor::visit(NSliceItem *slicingItem) {

        PyObject *start = nullptr;
        PyObject *stop = nullptr;
        PyObject *step = nullptr;

        // construct python slice and push to eval stack!
        if (slicingItem->_start) {
            slicingItem->_start->accept(*this);
            // at least one on evalstack, pop
            assert(!_evalStack.empty());
            start = _evalStack.back().value; _evalStack.pop_back();
        }
        if (slicingItem->_end) {
            slicingItem->_end->accept(*this);
            assert(!_evalStack.empty());
            stop = _evalStack.back().value; _evalStack.pop_back();
        }
        if (slicingItem->_stride) {
            slicingItem->_stride->accept(*this);
            assert(!_evalStack.empty());
            step = _evalStack.back().value; _evalStack.pop_back();
        }

        // create slice item
        addTraceResult(slicingItem, TraceItem(PySlice_New(start, stop, step)));
    }

    python::Type TraceVisitor::majorityInputType() const {
        using namespace std;

        if(_inputRowType != python::Type::UNKNOWN)
            return _inputRowType;

        // go over all non-except rows
        unordered_map<python::Type, size_t> counts;
        for(int i = 0; i < _colTypes.size(); ++i) {
            if(_colTypes[i].empty() || _colTypes[i].front() == python::Type::UNKNOWN)
                continue;
            auto key = python::Type::makeTupleType(_colTypes[i]);
            auto it = counts.find(key);
            if(it == counts.end())
                counts[key] = 0;
            counts[key]++;
        }

        // maximum case
        python::Type t = python::Type::UNKNOWN;
        size_t cnt = 0;
        for(auto keyval : counts) {
            if(keyval.second >= cnt) {
                t = keyval.first;
                cnt = keyval.second;
            }
        }
        return t;
    }

    #include <type_traits>

    // Helper to determine whether there's a const_iterator for T.
    template<typename T>
    struct has_const_iterator
    {
    private:
        template<typename C> static char test(typename C::const_iterator*) { return '\0'; }
        template<typename C> static int  test(...) { return 0; }
    public:
        enum { value = sizeof(test<T>(0)) == sizeof(char) };
    };

    template<typename Container> typename std::enable_if<has_const_iterator<Container>::value,
            void>::type mostFrequentItem(Container const& container) {
        using namespace std;

        if(container.empty())
            throw std::runtime_error("do not call on empty container");

        typename Container::const_iterator pos;
        typename Container::const_iterator end(container.end());
        unordered_map<size_t, typename Container::value_type> m;
        for(pos = container.begin(); pos != end; ++pos) {
            auto it = m.find(*pos);
            if(it == m.end())
                m[*pos] = 0;
            m[*pos]++;
        }

        // get most frequent item from hashmap
        typename Container::value_type most_frequent = *container.begin();
        size_t most_counts = 0;
        for(auto kv : m) {
            if(kv.first >= most_counts) {
                most_frequent = kv.second;
                most_counts = kv.first;
            }
        }
        return most_frequent;
    }

    python::Type TraceVisitor::majorityOutputType() const {
        using namespace std;
        // go over all non-except rows
        unordered_map<python::Type, size_t> counts;
        for(int i = 0; i < _retColTypes.size(); ++i) {
            if(_retColTypes[i].empty() || _retColTypes[i].front() == python::Type::UNKNOWN)
                continue;
            auto key = python::Type::makeTupleType(_retColTypes[i]);
            auto it = counts.find(key);
            if(it == counts.end())
                counts[key] = 0;
            counts[key]++;
        }

        // maximum case
        python::Type t = python::Type::UNKNOWN;
        size_t cnt = 0;
        for(auto keyval : counts) {
            if(keyval.second >= cnt) {
                t = keyval.first;
                cnt = keyval.second;
            }
        }

        // special case, sample yielded only exceptions.
        // -> use majority exception type!
        if(_exceptions.size() == _numSamplesProcessed && !_exceptions.empty()) {
            // majority exception type!
            auto most_frequent_exception_name = mostFrequentItem(_exceptions);

            // get type
            auto exception_type = python::TypeFactory::instance().getByName(most_frequent_exception_name);
            if(exception_type == python::Type::UNKNOWN) {
                Logger::instance().defaultLogger().debug("Unknown exception type found, adding to TypeSystem");
                // @TODO: correct hierarchy here, for now simply use BaseException..
                auto base_type = python::TypeFactory::instance().getByName("BaseException");
                assert(base_type.isExceptionType());
                exception_type = python::TypeFactory::instance().createOrGetPrimitiveType(most_frequent_exception_name, {base_type});
            }

            return exception_type;
        }

        return t;
    }

    void TraceVisitor::addTraceResult(ASTNode *node, TraceItem item) {
        if(node) {
            // annotation
            node->annotation();

            // number?
            if(item.value == Py_True || item.value == Py_False) {
                int64_t val = item.value == Py_True ? 1 : 0;
                if(node->annotation().numTimesVisited == 0) { // init
                    node->annotation().iMin = std::numeric_limits<int64_t>::max();
                    node->annotation().iMax = std::numeric_limits<int64_t>::min();
                }
                node->annotation().iMin = std::min(node->annotation().iMin, val);
                node->annotation().iMax = std::max(node->annotation().iMax, val);
                if(val > 0)
                    node->annotation().positiveValueCount++;
                else if(val < 0)
                    node->annotation().negativeValueCount++;
            } else if(PyLong_CheckExact(item.value)) {
                int64_t val = PyLong_AsLongLong(item.value);

                // too big?
                if(PyErr_Occurred()) {
                    PyErr_Clear();
                    // TODO: could simply clamp to range,
                    // yet skip for now.
                    return;
                }

                if(node->annotation().numTimesVisited == 0) { // init
                    node->annotation().iMin = std::numeric_limits<int64_t>::max();
                    node->annotation().iMax = std::numeric_limits<int64_t>::min();
                }
                node->annotation().iMin = std::min(node->annotation().iMin, val);
                node->annotation().iMax = std::max(node->annotation().iMax, val);
                if(val > 0)
                    node->annotation().positiveValueCount++;
                else if(val < 0)
                    node->annotation().negativeValueCount++;
            } else if(PyFloat_CheckExact(item.value)) {
                double val = PyFloat_AS_DOUBLE(item.value);
                if(node->annotation().numTimesVisited == 0) { // init
                    node->annotation().dMin = std::numeric_limits<double>::max();
                    node->annotation().dMax = std::numeric_limits<double>::min();
                }
                node->annotation().iMin = std::min(node->annotation().dMin, val);
                node->annotation().iMax = std::max(node->annotation().dMax, val);
                if(val > 0.0)
                    node->annotation().positiveValueCount++;
                else if(val < 0.0)
                    node->annotation().negativeValueCount++;
            }

            node->annotation().numTimesVisited++;

            // translate type
            node->annotation().types.push_back(python::mapPythonClassToTuplexType(item.value));

        }
        // add to instruction stack.
        _evalStack.push_back(item);
    }


    void TraceVisitor::setClosure(const ClosureEnvironment &ce, bool acquireGIL) {
        // TODO: what about correct order??
        if(acquireGIL)
            python::lockGIL();

        // first, modules and functions imported from modules
        auto mod = python::getMainModule();
        auto main_dict = PyModule_GetDict(mod);
        for(const auto& m : ce.modules()) {
            // python import module!
            auto mod_name = python::PyString_FromString(m.original_identifier.c_str());
            auto imported_mod = PyImport_Import(mod_name);
            if(!imported_mod) {
                if(acquireGIL)
                    python::unlockGIL();
                throw std::runtime_error("could not find python module " + m.original_identifier);
            }
            // add to main dict
            PyDict_SetItemString(main_dict, m.identifier.c_str(), imported_mod);
        }

        for(auto f: ce.functions()) {
            // from module import ... as ...
            auto mod_name = python::PyString_FromString(f.package.c_str());
            auto imported_mod = PyImport_Import(mod_name);
            if(!imported_mod) {
                if(acquireGIL)
                    python::unlockGIL();
                throw std::runtime_error("could not find python module " + f.package);
            }

            // get function
            auto mod_dict = PyModule_GetDict(imported_mod);
            assert(mod_dict);
            auto func_obj = PyDict_GetItemString(mod_dict, f.qualified_name.c_str());
            if(!func_obj) {
                if(acquireGIL)
                    python::unlockGIL();
                throw std::runtime_error("could not find name " + f.qualified_name + " in module " + f.package);
            }
            // add
            PyDict_SetItemString(main_dict, f.identifier.c_str(), func_obj);
        }

        // then, constants
        for(auto c : ce.constants()) {
            auto obj = python::fieldToPython(c.value);

            if(!obj) {
                if(acquireGIL)
                    python::unlockGIL();
                throw std::runtime_error("could convert object " + c.value.desc() + " to python object ");
            }

            PyDict_SetItemString(main_dict, c.identifier.c_str(), obj);
        }

        if(acquireGIL)
            python::unlockGIL();

    }

    void TraceVisitor::visit(NFor* node) {
        assert(node && node->target && node->expression && node->suite_body);
        setLastParent(node);

        // expression type check
        auto exprType = node->expression->getInferredType();
        if (!(exprType.isListType() || exprType.isTupleType() || exprType == python::Type::STRING ||
              exprType == python::Type::RANGE || exprType.isIteratorType())) {
            addCompileError(CompileError::TYPE_ERROR_UNSUPPORTED_LOOP_TESTLIST_TYPE);
            return;
        }

        // find all loop variables in target
        auto targetASTType = node->target->type();
        std::vector<NIdentifier *> loopVariables;
        if (targetASTType == ASTNodeType::Identifier) {
            auto id = static_cast<NIdentifier *>(node->target);
            loopVariables.push_back(id);
        } else if (targetASTType == ASTNodeType::Tuple || targetASTType == ASTNodeType::List) {
            auto idTuple = getForLoopMultiTarget(node->target);
            loopVariables.resize(idTuple.size());
            std::transform(idTuple.begin(), idTuple.end(), loopVariables.begin(),
                           [](ASTNode *x) {return static_cast<NIdentifier *>(x); });
        } else {
            throw std::runtime_error("unsupported target type, cannot extract loop variables in TraceVisitor.cc");
        }

        // visit expression, should push a value to _evalStack
        node->expression->accept(*this);
        assert(!_evalStack.empty());

        // get expression value
        auto exprVal = _evalStack.back().value;
        _evalStack.pop_back();

        // create iterator from expression
        PyObject *exprIter = PyObject_GetIter(exprVal);
        if (!exprIter) {
            throw std::runtime_error("cannot create iterator from expression in TraceVisitor.cc");
        }

        // get names of all variables created before loop since variables created inside loop won't matter
        std::vector<std::string> varBeforeLoop;
        for (const auto &symbol : _symbols) {
            varBeforeLoop.push_back(symbol.name);
        }

        // typeChange: did types change during the first iteration?
        bool typeChange = false;
        // typeStable: are types stable starting from the second iteration?
        bool typeStable = true;

        // loop that iterates over an iterator https://docs.python.org/3/c-api/iter.html
        int iterationNum = 0;
        _symbolsTypeChangeStack.emplace_back(varBeforeLoop, false);
        _loopBreakStack.push_back(false);
        PyObject *item = nullptr;
        while ((item = PyIter_Next(exprIter))) {
            iterationNum++;

            if (PyList_Check(item)) {
                if (loopVariables.size() != PyList_Size(item)) {
                    throw std::runtime_error("target size does not match expression element size, cannot unpack expression in TraceVisitor.cc");
                }
                for (Py_ssize_t i = 0; i < loopVariables.size(); i++) {
                    auto id = loopVariables[i];
                    auto it = std::find_if(_symbols.begin(), _symbols.end(), [&id](const TraceItem &ti) {
                        return ti.name == id->_name;
                    });
                    if (it == _symbols.end()) {
                        _symbols.emplace_back(PyList_GET_ITEM(item, i), id->_name);
                    } else {
                        it->value = PyList_GET_ITEM(item, i);
                    }
                }
            } else if (PyTuple_Check(item)) {
                if (loopVariables.size() != PyTuple_Size(item)) {
                    throw std::runtime_error(
                            "target size does not match expression element size, cannot unpack expression in TraceVisitor.cc");
                }
                for (Py_ssize_t i = 0; i < loopVariables.size(); i++) {
                    auto id = loopVariables[i];
                    auto it = std::find_if(_symbols.begin(), _symbols.end(), [&id](const TraceItem &ti) {
                        return ti.name == id->_name;
                    });
                    if (it == _symbols.end()) {
                        _symbols.emplace_back(PyTuple_GET_ITEM(item, i), id->_name);
                    } else {
                        it->value = PyList_GET_ITEM(item, i);
                    }
                }
            } else {
                auto id = loopVariables.front();
                auto it = std::find_if(_symbols.begin(), _symbols.end(), [&id](const TraceItem &ti) {
                    return ti.name == id->_name;
                });
                if (it == _symbols.end()) {
                    _symbols.emplace_back(item, id->_name);
                } else {
                    it->value = item;
                }
            }

            node->suite_body->accept(*this);

            // check if there is type change in the previous iteration
            if(iterationNum == 1) {
                typeChange = _symbolsTypeChangeStack.back().second;
                _symbolsTypeChangeStack.back().second = false;
            } else if(typeStable && _symbolsTypeChangeStack.back().second) {
                typeStable = false;
            }

            Py_DECREF(item);

            if(_loopBreakStack.back()) {
                // a break statement was executed in the current loop
                break;
            }
        }
        _symbolsTypeChangeStack.pop_back();

        if(iterationNum == 0) {
            // loop body was not run
            node->annotation().zeroIterationCount++;
        }

        if(typeStable) {
            if(typeChange) {
                // types changed in the first iteration but are stable from the second iteration
                node->annotation().typeChangedAndStableCount++;
            } else {
                // type is stable for the entire loop
                node->annotation().typeStableCount++;
            }
        } else {
            // type unstable
            node->annotation().typeChangedAndUnstableCount++;
        }
        node->annotation().numTimesVisited++;

        if(!_loopBreakStack.back()) {
            if(node->suite_else) {
                node->suite_else->accept(*this);
            }
        }
        _loopBreakStack.pop_back();
    }

    void TraceVisitor::visit(NWhile* node) {
        assert(node && node->expression && node->suite_body);
        setLastParent(node);

        // get names of all variables created before loop since variables created inside loop won't matter
        std::vector<std::string> varBeforeLoop;
        for (const auto &symbol : _symbols) {
            varBeforeLoop.push_back(symbol.name);
        }

        // visit expression, should push a value to _evalStack
        node->expression->accept(*this);
        assert(!_evalStack.empty());

        // get expression value
        auto exprVal = _evalStack.back().value;
        _evalStack.pop_back();

        // typeChange: did types change during the first iteration?
        bool typeChange = false;
        // typeStable: are types stable starting from the second iteration?
        bool typeStable = true;

        int iterationNum = 0;
        _symbolsTypeChangeStack.emplace_back(varBeforeLoop, false);
        _loopBreakStack.push_back(false);
        while (PyObject_IsTrue(exprVal)) {
            iterationNum++;

            // visit loop body
            node->suite_body->accept(*this);

            if (iterationNum == 1) {
                typeChange = _symbolsTypeChangeStack.back().second;
                _symbolsTypeChangeStack.back().second = false;
            } else if(typeStable && _symbolsTypeChangeStack.back().second) {
                typeStable = false;
            }

            if(_loopBreakStack.back()) {
                // a break statement was executed in the current loop
                break;
            }

            // visit expression, should push a value to _evalStack
            node->expression->accept(*this);
            assert(!_evalStack.empty());

            // get expression value
            exprVal = _evalStack.back().value;
            _evalStack.pop_back();
        }
        _symbolsTypeChangeStack.pop_back();

        if(iterationNum == 0) {
            // loop body was not run
            node->annotation().zeroIterationCount++;
        }

        if(typeStable) {
            if(typeChange) {
                // types changed in the first iteration but are stable from the second to second last iteration
                node->annotation().typeChangedAndStableCount++;
            } else {
                // type is stable for the entire loop
                node->annotation().typeStableCount++;
            }
        } else {
            // type unstable
            node->annotation().typeChangedAndUnstableCount++;
        }
        node->annotation().numTimesVisited++;

        if(!_loopBreakStack.back()) {
            if(node->suite_else) {
                node->suite_else->accept(*this);
            }
        }
        _loopBreakStack.pop_back();
    }

    void TraceVisitor::visit(NRange *node) {
        assert(!node->_positionalArguments.empty());
        auto numArgs = node->_positionalArguments.size();

        // visit each number in range function arguments
        ApatheticVisitor::visit(node);
        assert(_evalStack.size() >= numArgs);

        // prepare args tuple for the range() call
        auto argsTuple = PyTuple_New(numArgs);
        for (size_t i = 0; i < numArgs; ++i) {
            // fill in args tuple from back to front
            PyTuple_SET_ITEM(argsTuple, numArgs-1-i, _evalStack.back().value);
            _evalStack.pop_back();
        }

        // import module builtins https://docs.python.org/3.9/library/builtins.html
        auto opMod = PyImport_ImportModule("builtins");
        assert(opMod);
        auto opModDict = PyModule_GetDict(opMod);
        assert(opModDict);

        // call range() with args tuple
        auto rangeFunction = PyDict_GetItemString(opModDict, "range");
        auto rangeObject = PyObject_Call(rangeFunction, argsTuple, nullptr);

        addTraceResult(node, TraceItem(rangeObject));
    }

    void TraceVisitor::visit(NList *node) {
        ApatheticVisitor::visit(node);
        auto numArgs = node->_elements.size();
        assert(_evalStack.size() >= numArgs);

        // create new list
        auto list = PyList_New(numArgs);
        for (size_t i = 0; i < node->_elements.size(); ++i) {
            // fill in list from back to front
            PyList_SET_ITEM(list, numArgs-1-i, _evalStack.back().value);
            _evalStack.pop_back();
        }

        addTraceResult(node, TraceItem(list));
    }

    std::vector<size_t> TraceVisitor::columnAccesses() const {
        // how many columns are there?
        std::vector<size_t> counts(columnCount(), 0);

        // now, let's get the column names
        if(_columnNames.size() == 1 && columnCount() != 1) {
            // special case: tuple access! decode x[0] etc.

            // hack here, could do more elaborate at some point...
            auto param_name = _columnNames.front();
            for(unsigned i = 0; i < columnCount(); ++i) {
                auto key = param_name + "[" + std::to_string(i) + "]";
                auto it = _inputAccessPaths.find(key);
                if(it != _inputAccessPaths.end()) {
                    counts[i] = it->second;
                }
            }
        } else {
            assert(_columnNames.size() == columnCount()); //! important !!!
            // simply go through column names and set result!
            for(unsigned i = 0; i < _columnNames.size(); ++i) {
                auto it = _inputAccessPaths.find(_columnNames[i]);
                if(it != _inputAccessPaths.end()) {
                    counts[i] = it->second;
                }
            }
        }
        return counts;
    }
}