//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <TypeAnnotatorVisitor.h>
#include <TypeSystem.h>
#include <TokenType.h>
#include <ApplyVisitor.h>
#include <ASTHelpers.h>

namespace tuplex {
    bool isPythonIntegerType(const python::Type& t ) {
        return t == python::Type::I64 || t == python::Type::BOOLEAN;
    }

    void TypeAnnotatorVisitor::visit(NModule *module) {
        throw std::runtime_error("not yet supported!");
    }

    void TypeAnnotatorVisitor::visit(NSuite *suite) {

        // visit statements until the first return is encountered!!!
        // => note: clean ast visitor could remove anything that comes after return!
        for (auto & stmt : suite->_statements) {
            // check whether statement can be optimized. Can be optimized iff result of optimizeNext is a different
            // memory address
            setLastParent(suite);
            stmt->accept(*this);
            // do not process statements after return (early exit suite)
            if(stmt->type() == ASTNodeType::Return) {
                suite->setInferredType(stmt->getInferredType());
                return;
            }

            // similarly, stop at a raise statement
            if(stmt->type() == ASTNodeType::Raise) {
                // set type of suite to exception being raised...
                // special case: if left side is NOT an exception, then set to TypeError
                // i.e. something like raise 20 produces a type error...
                // >>> raise 20
                //Traceback (most recent call last):
                //  File "<stdin>", line 1, in <module>
                //TypeError: exceptions must derive from BaseException
                auto raise = (NRaise*)stmt;
                if(!raise->_expression->getInferredType().isExceptionType()) {
                    suite->setInferredType(_symbolTable.lookupType("TypeError"));
                } else {
                    suite->setInferredType(raise->_expression->getInferredType());
                }
                return;
            }
        }

        // type of the suite is the type of the last statement!
        if(suite->_statements.size() > 0)
            suite->setInferredType(suite->_statements.back()->getInferredType());
    }

    void TypeAnnotatorVisitor::visit(NFunction* func) {
        // reset possible return types
        _funcReturnTypes.clear();

        // add parameters to current name table
        for(auto& arg : func->_parameters->_args) { // these were hinted upfront!
            auto param = dynamic_cast<NParameter*>(arg);
            _nameTable[param->_identifier->_name] = param->_identifier->getInferredType();
        }

        // visit the rest of the function
        ApatheticVisitor::visit(func);

        if(!_funcReturnTypes.empty()) {

            // try to combine return types (i.e. for none, this works!)
            // ==> if it fails, display err message.

            // go through all func types, and check whether they can be unified.
            auto combined_ret_type = _funcReturnTypes.front();
            for(int i = 1; i < _funcReturnTypes.size(); ++i)
                combined_ret_type = unifyTypes(combined_ret_type, _funcReturnTypes[i], _allowNumericTypeUnification);

            if(combined_ret_type == python::Type::UNKNOWN) {

                // speculation?
                bool speculate = !_returnTypeCounts.empty();
                if(speculate) {

                    // Note: this can be done using a greedy algorithm - perhaps we could also find something better.
                    // ALGORITHM
                    // => we could devise a correct algorithm for this.
                    // for now, use a trivial heuristic. simply pick the max element!
                    // then try to unify till no more possible. That's basically a greedy approach.
                    using namespace std;
                    vector<tuple<python::Type, size_t>> v;
                    assert(_funcReturnTypes.size() == _returnTypeCounts.size());
                    for(int i = 0; i < _funcReturnTypes.size(); ++i) {
                        v.push_back(make_tuple(_funcReturnTypes[i], _returnTypeCounts[i]));
                    }

                    // sort desc after counts.
                    std::sort(v.begin(), v.end(), [](const tuple<python::Type, size_t>& a, const tuple<python::Type, size_t>& b) {
                        return std::get<1>(a) > std::get<1>(b);
                    });

                    // top element?
                    auto best_so_far = std::get<0>(v.front());

                    for(int i = 1; i < v.size(); ++i) {
                        auto u_type = unifyTypes(best_so_far, std::get<0>(v[i]), _allowNumericTypeUnification);
                        if(u_type != python::Type::UNKNOWN)
                            best_so_far = u_type;
                    }

                    combined_ret_type = best_so_far;
                } else {
                    // check that all return values are the same, if not: error!!!
                    std::set<python::Type> unique_types(_funcReturnTypes.begin(), _funcReturnTypes.end());
                    std::vector<std::string> type_names;
                    for(const auto& t : unique_types)
                        type_names.emplace_back(t.desc());
                    std::stringstream ss;
                    ss<<"function "<<func->_name->_name<<" has different return types "<<tuplex::mkString(type_names, ", ")
                      <<". Need to speculate on return type.";
                    error(ss.str());
                    return;
                }
            }

            assert(combined_ret_type != python::Type::UNKNOWN); // make sure control flow does not else hit this!

            // update suite with combined type!
            func->_suite->setInferredType(combined_ret_type);

            bool autoUpcast = _allowNumericTypeUnification;

            // set return type of all return statements to combined type!
            // ==> they will need to expand the respective value, usually given via their expression.
            tuplex::ApplyVisitor av([](const ASTNode* n) { return n->type() == ASTNodeType::Return; },
                                    [combined_ret_type,autoUpcast](ASTNode& n) {

                                        // can upcast? => only then set. This allows in Blockgenerator to detect deviating return statements!
                                        if(n.getInferredType() == python::Type::UNKNOWN) // i.e. code that is never visited
                                            return;

                                        auto uni_type = unifyTypes(n.getInferredType(), combined_ret_type, autoUpcast);
                                        if(uni_type != python::Type::UNKNOWN)
                                            n.setInferredType(combined_ret_type);
                                    });
            func->_suite->accept(av);
        } else {
            //... return type is nothing... --> not allowed for function yet!
            // => altenative: type it as None, like python does.
            error("function " + func->_name->_name + " has no return type (I.e., None - likely due to missing return statement)."
                                                     " This is not allowed. If you want to explicitly return None, add a return statement.");
            return;
        }

        if(func->_parameters) {
            // create tuple type according to all params
            func->setInferredType(python::TypeFactory::instance().instance()
                                          .createOrGetFunctionType(func->_parameters->getInferredType(),
                                                                   func->_suite->getInferredType()));
        }
        else {
            func->setInferredType(python::TypeFactory::instance().instance()
                                          .createOrGetFunctionType(python::Type::EMPTYTUPLE,
                                                                   func->_suite->getInferredType()));
        }

        // annotate identifier of function with its type
        func->_name->setInferredType(func->getInferredType());

        _funcReturnTypes.clear();
    }

// type inference is a post action
    void TypeAnnotatorVisitor::visit(NUnaryOp *op) {
        ApatheticVisitor::visit(op);

        // result type of this operation is basically the operand's type
        // except for bool type, then it is int
        auto type = op->_operand->getInferredType();

        if(op->_op == TokenType::NOT)
            op->setInferredType(python::Type::BOOLEAN);
        else {
            // must be ~, - or +
            assert(op->_op == TokenType::TILDE || op->_op == TokenType::PLUS || op->_op == TokenType::MINUS);

            if(python::Type::BOOLEAN == type)
                type = python::Type::I64;
            op->setInferredType(type);
        }
    }

    void TypeAnnotatorVisitor::visit(NParameterList *params) {
        // first the recursive visit
        ApatheticVisitor::visit(params);

        // create tuple type out of this
        std::vector<python::Type> vTypes;
        for(auto p : params->_args)
            vTypes.push_back(p->getInferredType());

        params->setInferredType(python::TypeFactory::instance().createOrGetTupleType(vTypes));
    }

    void TypeAnnotatorVisitor::visit(NParameter* param) {
        ApatheticVisitor::visit(param);
        // the most distinct info that is available are type annotations, from these
        // the programmer can define directly the type
        if(param->_annotation) {
            // use annotation
            param->setInferredType(param->_annotation->getInferredType());
        } else {
            // check if a default value was given
            // this has prio over symboltable lookup
            if(param->_default) {
                param->setInferredType(param->_default->getInferredType());
            } else {
                // use type from identifier
                assert(param->_identifier); // an identifier is always needed! No support for anonymous identifiers yet!
                param->setInferredType(param->_identifier->getInferredType());
            }
        }

    }

    void TypeAnnotatorVisitor::init() {
        _annotationLookup = {{"int", python::Type::I64},
                             {"float", python::Type::F64},
                             {"str", python::Type::STRING},
                             {"bool", python::Type::BOOLEAN}};
    }

    void TypeAnnotatorVisitor::visit(NIdentifier* id) {
        ApatheticVisitor::visit(id);
        // @TODO: Change how annotations work. Basically, annotations are any expression.
        // for typing they only should be considered when adhering to typing.py module

        // check if in local name table, i.e. from variable declaration!
        // => this shadows everything, so should come first.
        if(_nameTable.find(id->_name) != _nameTable.end()) {
            id->setInferredType(_nameTable[id->_name]);
            if(_nameTable[id->_name].isIteratorType()) {
                // copy iterator-specific annotation for iterators
                id->annotation().iteratorInfo = _iteratorInfoTable[id->_name];
            }
            return;
        }


        // python is great, int can be both a function OR a type
        // hence, check if is here used as annotation
        if(parent()->type() == ASTNodeType::Parameter) {
#warning "refactor this annotationLookup stuff into a separate function!"
            // use annotation magic
            auto it = _annotationLookup.find(id->_name);
            if(it != _annotationLookup.end()) {
                id->setInferredType(it->second);
                return;
            } else {
                // here the symbol table lookup happens
                id->setInferredType(lookupType(id->_name));
                return;
            }
        } else {
            // try to set via symbol table
            // note this function looks up ANYTHING, might be multiple possible symbols...
            // special case of functions with different params is handled in next IF,
            // therefore exclude here b.c. functions should get specialized with their parameter type!
            auto sym = _symbolTable.findSymbol(id->_name);
            id->annotation().symbol = sym;
            if(sym && sym->symbolType != SymbolType::FUNCTION) {
                // should be variable!
                assert(sym->symbolType == SymbolType::VARIABLE || sym->symbolType == SymbolType::EXTERNAL_PYTHON);
                id->setInferredType(sym->types.front()); // use first type, code below will specialize further!

                return;
            }
        }

        // identifier was not found in symbol table.
        // check if it is a child of a call object and perform internal lookup!
        if(parent()) {
            // special case assign:
            if(parent()->type() == ASTNodeType::Assign) {
                NAssign* assign = (NAssign*)parent();

                // check if id equals target, if so update with value's type
                if(assign->_target == id) {
                    // add hint to table if not yet there
                    auto targetType = lookupType(id->_name);
                    auto valueType = assign->_value->getInferredType();

                    _nameTable[id->_name] = valueType;
                    id->setInferredType(valueType);
                    return;
                }
            }

            // special case call:
            if(parent()->type() == ASTNodeType::Call) {
                NCall *call = (NCall*)parent();

                if(call->_func == id) {
                    // lookup function with parameters

                    // when performing null-value optimization, could happen that symbol is there but no type match
                    // => type call as type error!
                    auto funcType = _symbolTable.findFunctionType(id->_name, _lastCallParameterType.top());
                    if(python::Type::UNKNOWN == funcType) {

                        // no typing possible, does the symbol exist though?
                        auto sym = _symbolTable.findSymbol(id->_name);
                        if(sym) {
                            // typing issue. => throw TypeError!
                            // => for NULL value opt, try to type again but this time treating all NULLs as any.
                            funcType = _symbolTable.findFunctionType(id->_name, _lastCallParameterType.top(), true);
                            if(python::Type::UNKNOWN == funcType)
                                error("symbol not compatible with nulls treated as any. Could not find type information for symbol " + id->_name);
                        } else {
                            error("could not find type information for symbol '" + id->_name + "'", "type annotator");
                        }
                    }
                    annotateIteratorRelatedCalls(id->_name, call);
                    id->setInferredType(funcType);
                    return;
                }
            }
        }

        // check if identifier is in symbol table, if not it's a missing identifier!
        if(python::Type::UNKNOWN == lookupType(id->_name)) {
            // do not add identifier if it has function as parent (this means basically it is the function name)
            if(   parent()->type() != ASTNodeType::Function
                  && parent()->type() != ASTNodeType::Lambda
                  && parent()->type() != ASTNodeType::Assign
                  // TODO this check is necessary for tuple assignment
                  && parent()->type() != ASTNodeType::Tuple)
                _missingIdentifiers.add(id->_name);
        }
    }

    void TypeAnnotatorVisitor::annotateIteratorRelatedCalls(const std::string &funcName, NCall* call) {
        auto iteratorInfo = std::make_shared<IteratorInfo>();
        if(funcName == "iter") {
            if (call->_positionalArguments.size() != 1) {
                error("iter() currently takes exactly 1 argument");
            }
            auto iterableType = _lastCallParameterType.top().parameters().front();
            iteratorInfo->iteratorName = "iter";
            iteratorInfo->argsType = iterableType;
            if(iterableType.isIteratorType()) {
                // argsIteratorInfo is iteratorInfo of the argument
                iteratorInfo->argsIteratorInfo = {call->_positionalArguments[0]->annotation().iteratorInfo};
            } else {
                // argument type is list/tuple/string/range, no child iteratorInfo to add
                iteratorInfo->argsIteratorInfo = {nullptr};
            }
            call->annotation().iteratorInfo = iteratorInfo;
        } else if(funcName == "reversed") {
            if (call->_positionalArguments.size() != 1) {
                error("reversed() takes exactly 1 argument");
            }
            auto seqType = _lastCallParameterType.top().parameters().front();
            if(seqType == python::Type::RANGE) {
                // treat any reversed range iterator as a normal range_iterator, since a reversed range object will be placed in the iterator struct later
                iteratorInfo->iteratorName = "iter";
            } else {
                // same argument object will be placed in the iterator struct later. Use "reversed" to hint the correct direction for updating index
                iteratorInfo->iteratorName = "reversed";
            }
            iteratorInfo->argsType = seqType;
            iteratorInfo->argsIteratorInfo = {nullptr};
            call->annotation().iteratorInfo = iteratorInfo;
        } else if(funcName == "zip") {
            auto iterablesType = _lastCallParameterType.top().parameters();
            auto arguments = call->_positionalArguments;
            assert(arguments.size() == iterablesType.size());
            iteratorInfo->iteratorName = "zip";
            iteratorInfo->argsType = _lastCallParameterType.top();
            std::vector<std::shared_ptr<IteratorInfo>> argsIteratorInfo = {};
            for (int i = 0; i < arguments.size(); ++i) {
                if(iterablesType[i].isIteratorType()) {
                    // add iteratorInfo of the current argument
                    argsIteratorInfo.push_back(call->_positionalArguments[i]->annotation().iteratorInfo);
                } else {
                    // current argument type is list/tuple/string/range, create new IteratorInfo
                    auto currIteratorInfo = std::make_shared<IteratorInfo>();
                    currIteratorInfo->iteratorName = "iter";
                    currIteratorInfo->argsType = iterablesType[i];
                    currIteratorInfo->argsIteratorInfo = {nullptr};
                    argsIteratorInfo.push_back(currIteratorInfo);
                }
            }
            iteratorInfo->argsIteratorInfo = argsIteratorInfo;
            call->annotation().iteratorInfo = iteratorInfo;
        } else if(funcName == "enumerate") {
            if (call->_positionalArguments.size() != 1 && call->_positionalArguments.size() != 2) {
                error("enumerate() takes 1 or 2 arguments");
            }
            auto iterableType = _lastCallParameterType.top().parameters().front();
            iteratorInfo->iteratorName = "enumerate";
            iteratorInfo->argsType = iterableType;
            if(iterableType.isIteratorType()) {
                // argsIteratorInfo is iteratorInfo of the argument
                iteratorInfo->argsIteratorInfo = {call->_positionalArguments[0]->annotation().iteratorInfo};
            } else {
                // current argument type is list/tuple/string/range, create new IteratorInfo
                auto currIteratorInfo = std::make_shared<IteratorInfo>();
                currIteratorInfo->iteratorName = "iter";
                currIteratorInfo->argsType = iterableType;
                currIteratorInfo->argsIteratorInfo = {nullptr};
                iteratorInfo->argsIteratorInfo = {currIteratorInfo};
            }
            call->annotation().iteratorInfo = iteratorInfo;
        } else if(funcName == "next") {
            if (call->_positionalArguments.empty()) {
                error("next() takes 1 or 2 arguments");
            }
            auto argIteratorInfo = call->_positionalArguments.front()->annotation().iteratorInfo;
            // copy iteratorInfo of the iterator argument to the outer next() call so that BlockGeneratorVisitor can use it
            call->annotation().iteratorInfo = call->_positionalArguments.front()->annotation().iteratorInfo;
        }
    }

    void TypeAnnotatorVisitor::visit(NStarExpression *se) {
        ApatheticVisitor::visit(se);
        se->setInferredType(se->_target->getInferredType());
    }

    void TypeAnnotatorVisitor::visit(NAwait *await) {
        ApatheticVisitor::visit(await);
        await->setInferredType(await->_target->getInferredType());
    }

    void TypeAnnotatorVisitor::visit(NReturn *ret) {
        ApatheticVisitor::visit(ret);
        // set type to None if return has no expression
        if(ret->_expression) {
            auto retType = ret->_expression->getInferredType();
            checkRetType(retType);
            ret->setInferredType(ret->_expression->getInferredType());
        } else {
            ret->setInferredType(python::Type::NULLVALUE); // None == NULL value
        }

        // add to possible func return types for later check
        _funcReturnTypes.emplace_back(ret->getInferredType());

        // annotation existing? => add count!
        if(ret->_expression->hasAnnotation()) {
            _returnTypeCounts.emplace_back(ret->_expression->annotation().numTimesVisited);
            assert(_funcReturnTypes.size() == _returnTypeCounts.size()); // check arrays are the same size...
        } else {
            // never visited ret type
            // this situation may arise i.e. for
            // if x:         <-- all samples visit this branch
            //    return 10
            // return 3.4    <-- no annotation (b.c. never visited)
            _returnTypeCounts.emplace_back(0);
        }
    }

    void TypeAnnotatorVisitor::visit(NLambda *lambda) {
        // add parameters to current scope
        for(auto& arg : lambda->_arguments->_args) { // these were hinted upfront!
            auto param = dynamic_cast<NParameter*>(arg);

            // add to name table
            _nameTable[param->_identifier->_name] = param->_identifier->getInferredType();
        }

        // visit the rest of the function
        ApatheticVisitor::visit(lambda);

        //==> infer here function type. Needs class for this...
        if(lambda->_arguments) {
            // create tuple type according to all params
            lambda->setInferredType(python::TypeFactory::instance().instance()
                                            .createOrGetFunctionType(lambda->_arguments->getInferredType(),
                                                                     lambda->_expression->getInferredType()));
        }
        else {
            lambda->setInferredType(python::TypeFactory::instance().instance()
                                            .createOrGetFunctionType(python::Type::EMPTYTUPLE,
                                                                     lambda->_expression->getInferredType()));
        }
    }

    void TypeAnnotatorVisitor::visit(NBinaryOp *op) {
        ApatheticVisitor::visit(op);

        // handle option types here:
        // ==> i.e. handle operation as non-optional one, exception in codegen unless operator is != or ==
        auto left_type = op->_left->getInferredType();
        auto right_type = op->_right->getInferredType();
        auto tt = op->_op;

        if(left_type.isOptionType() || right_type.isOptionType()) {
            // special check == or != operator?
            if(tt == TokenType::EQEQUAL || tt == TokenType::NOTEQUAL) {
                op->setInferredType(python::Type::BOOLEAN);
                return;
            } else {
                // get rid off options
                left_type = left_type.withoutOptions();
                right_type = right_type.withoutOptions();
            }
        }

        // special check because widespread error: withColumn vs. mapColumn
        // tuple + simple primitive and not compatible?
        // ==> @TODO!

        // a lot of manual checking is needed here now...
        // so far no custom object with operator overloading are yet defined
        // which makes it a bit easier
        op->setInferredType(binaryOpInference(op->_left, left_type, tt, op->_right, right_type));
    }

    void TypeAnnotatorVisitor::visit(NCompare *cmp) {
        ApatheticVisitor::visit(cmp);

        // first check if this is a simple statement, if so simply use the rhs's result
        if(cmp->_comps.size() == 0)
            cmp->setInferredType(cmp->_left->getInferredType());
        else
            // else it is a bool (b.c. it is a compare statement)
            cmp->setInferredType(python::Type::BOOLEAN);
    }

    python::Type TypeAnnotatorVisitor::binaryOpInference(ASTNode* left, const python::Type& a,
                                                         const TokenType tt, ASTNode* right,
                                                         const python::Type& b) {


        if(a.isOptional() || b.isOptional()) {
            error("can not type binary operation " + a.desc() + " " + opToString(tt) + " " + b.desc());
            return python::Type::UNKNOWN;
        }

        // only works for non-option types
        assert(!a.isOptional() && !b.isOptional());

        // different type, inference rules needed...
        // first, special cases for some operators
        // reference for this is https://docs.python.org/3/library/stdtypes.html
        // i.e. bitwise operations are only allowed for integers...
        if(isBitOperation(tt)) {
            // both must be integer or bool

            if(!isPythonIntegerType(a) || !isPythonIntegerType(b))
                error("invalid types found, must both be integer for a bit operation!");

            // |, &, ^ are bool if both are boolean
            if(a == python::Type::BOOLEAN && b == python::Type::BOOLEAN) {
                if(tt == TokenType::AMPER || tt == TokenType::VBAR || tt == TokenType::CIRCUMFLEX)
                    return python::Type::BOOLEAN;
            }

            // else, always integer.
            return python::Type::I64;
        }


        // special case: python3 division always leads to double
        if(TokenType::SLASH == tt)
            return python::Type::F64;


        // special case: power
        if(TokenType::DOUBLESTAR == tt) {

            // special case boolean ** boolean => is i64
            // because right side can't be negative, no speculation necessary.
            // Else, always need to speculate...
            if(right->getInferredType() == python::Type::BOOLEAN &&
               left->getInferredType() == python::Type::BOOLEAN)
                    return python::Type::I64;


            // three cases:

            // 1.) int ** non-negative int => int
            // 2.) int ** negative int => float
            // 3.) . ** . => float
            // make it here non-standard compliant by using float version throughout... ==> todo: tracing/sampling!!!

            if(isPythonIntegerType(a) && isPythonIntegerType(b)) {
                // is auto-upcasting allowed?
                if(_allowNumericTypeUnification)
                    return python::Type::F64; // do general pow operator as float!

                // for foldable expressions, ReduceExpressionsVisitor should have done already everything.
                // So, if a simple number is present, then we can decide statically.
                if(right->type() == ASTNodeType::Number) {
                    auto n = (NNumber*)right;
                    assert(n->getInferredType() == python::Type::I64);
                    auto val = n->getI64();
                    if(val >= 0)
                        return a;
                    else
                        return python::Type::F64;
                }

                // need tracing to speculate, i.e. if there's no type annotation on the b node, error
                assert(right);

                // @TODO: maybe a helper function to determine tracing status? this looks too hacky IMHO.
                if(right->annotation().numTimesVisited == 0) // this means no tracing occurred yet!
                    error("power operator ** needs type annotation, because speculation"
                          " is needed when the right operand is of type " + b.desc());
                else {
                    // check min/max of value and negative/positive count
                    if(right->annotation().iMin >= 0) // always greater than 0 in sample
                        return a == python::Type::BOOLEAN ? python::Type::I64 : a; // auto cast to integer in python.
                    else if(right->annotation().iMax < 0) // always less than 0 in sample
                        return python::Type::F64;
                    else if(right->annotation().negativeValueCount > right->annotation().positiveValueCount)
                        return python::Type::F64;
                    else
                        return a;
                }
                return python::Type::UNKNOWN;
            }

            // else, always results in float
            return python::Type::F64;
        }

        // General inference:
        // simplest case: same type => invariant
        // unknown / unknown combination may be problematic
        // also special case bool, bool with arithmetic operation needs to be accounted for!
        if(a == b) {
            // special case: bool op bool with op being an arithmetic operation will yield i64
            if (python::Type::BOOLEAN == a && isArithmeticOperation(tt))
                return python::Type::I64;
            else
                return a;
        }

        // if one is unknown and the other not, simply take the other's type!
        if(python::Type::UNKNOWN == a && python::Type::UNKNOWN != b)
            return b;
        if(python::Type::UNKNOWN != a && python::Type::UNKNOWN == b)
            return a;

        // int64 + float64 = float64
        if((python::Type::I64 == a && python::Type::F64 == b) ||
           (python::Type::I64 == b && python::Type::F64 == a))
            return python::Type::F64;

        if((python::Type::BOOLEAN == a && python::Type::F64 == b) ||
           (python::Type::BOOLEAN == b && python::Type::F64 == a))
            return python::Type::F64;

        if((python::Type::BOOLEAN == a && python::Type::I64 == b) ||
           (python::Type::BOOLEAN == b && python::Type::I64 == a))
            return python::Type::I64;

        // one possible operation for strings is multiplication!
        // that is n * s or s * n for multiplying the string multiple times
        if((TokenType::STAR == tt) && ((python::Type::STRING == a && (python::Type::I64 == b || python::Type::BOOLEAN == b)) ||
           (python::Type::STRING == b && (python::Type::I64 == a || python::Type::BOOLEAN == a))) )
            return python::Type::STRING;

        // another is formatting:: s % t -> formatted string
        if((python::Type::STRING == a) && (TokenType::PERCENT == tt) && (b.isTupleType() || python::Type::I64 == b || python::Type::F64 == b || python::Type::BOOLEAN == b))
            return python::Type::STRING;


        std::stringstream ss;
        ss<<"illegal binary op "<<opToString(tt)<<" found, types "<<a.desc()<<" and "<<b.desc()<<" do not work together";
        error(ss.str());
        return python::Type::UNKNOWN;
    }

    void TypeAnnotatorVisitor::visit(NCall *call) {
        // custom visit first posargs & then the rest
        setLastParent(call);
        // visit children if they exist first
        for(auto parg : call->_positionalArguments) {
            parg->accept(*this);
            setLastParent(call);
        }

        // now fetch type and store!
        _lastCallParameterType.push(call->getParametersInferredType());

        call->_func->accept(*this);

// @Todo: solve this later...
//    // call can also provide type info to underlying function, i.e.
//    // when declared or so...
//    // for this basically, look up symbol & add info
//    // call accept too
//    if(call->getParametersInferredType() != python::Type::UNKNOWN) {
//        // check if func is typed (note children were visited before!)
//        // if not, then provide for params type info
//        if(call->_func->getInferredType() == python::Type::UNKNOWN) {
//            if(call->_func->)
//        }
//    }

        // value & func have been visited
        // in standard languages, call->_func would now yield
        // the type of the function
        // and it could be checked with the input types.
        // however, Python is dynamically typed
        // so in order to solve this, we use a trick:
        // the actual typing happens dynamically here
        // i.e. type call->_func BASED ON call->_args

        // the symbolTable holds all necessary elements for this...

        // abort if no typing is available for function
        // if it's some general object, try to retype using concrete values...
        if(call->_func->getInferredType() == python::Type::UNKNOWN ||
        call->_func->getInferredType() == python::Type::PYOBJECT) {
            std::string name;
            if(call->_func->type() == ASTNodeType::Identifier)
                name = ((NIdentifier*)call->_func)->_name;

            // annotation available?
            // -> then create type from arguments + return type!
            if(call->_func->hasAnnotation()) {
                auto ret_type = call->_func->annotation().majorityType();

                std::vector<python::Type> param_types;
                // params type from positional arguments
                for(auto arg : call->_positionalArguments) {
                    auto arg_type = arg->getInferredType();
                    if(arg_type == python::Type::UNKNOWN)
                        fatal_error("Could not infer typing for callable " + name + ", positional arg has unknown type.");
                    param_types.push_back(arg_type);
                }

                auto func_type = python::Type::makeFunctionType(python::Type::makeTupleType(param_types), ret_type);
                call->_func->setInferredType(func_type);
            } else {
                fatal_error("Could not infer typing for callable " + name);
            }
        }

        assert(call->_func->getInferredType() != python::Type::UNKNOWN);
        for(auto arg : call->_positionalArguments)
            assert(arg->getInferredType() != python::Type::UNKNOWN);

        // type call with func result type.
        // check they are compatible
        auto ftype = call->_func->getInferredType();

        if(ftype.isFunctionType()) {
            assert(ftype.isFunctionType());
            auto retType = ftype.getReturnType();
            call->setInferredType(retType);
        }

        // remove func call
        _lastCallParameterType.pop();
    }

    void TypeAnnotatorVisitor::visit(NTuple* tuple) {
        ApatheticVisitor::visit(tuple);

        // check if empty, if so annotate with Emptytuple
        if(tuple->_elements.empty()) {
            tuple->setInferredType(python::Type::EMPTYTUPLE);
        } else {
            std::vector<python::Type> v;
            for(auto& el : tuple->_elements)
                v.push_back(el->getInferredType());
            python::Type t = python::Type::makeTupleType(v);
            tuple->setInferredType(t);
        }
    }

    void TypeAnnotatorVisitor::visit(NDictionary* dict) {
        ApatheticVisitor::visit(dict);

        // Try to make it Dictionary[Key, Val] type (if every pair has the same key type and val type, respectively)
        bool is_key_val = true;
        python::Type keyType, valType;
        if(dict->_pairs.size() > 0) {
            keyType = dict->_pairs[0].first->getInferredType();
            valType = dict->_pairs[0].second->getInferredType(); // save the key type, val type of the first pair
            for(const auto& p: dict->_pairs) { // check if every pair has the same key type, val type
                if(p.first->getInferredType() != keyType || p.second->getInferredType() != valType) {
                    is_key_val = false; // if they are not the same, then it is not of type Dictionary[Key, Val]
                    break;
                }
            }

            if(is_key_val) { // indicates whether every key, val have the same type
                dict->setInferredType(python::Type::makeDictionaryType(keyType, valType));
            }
            else {
                dict->setInferredType(python::Type::GENERICDICT);
            }
        } else {
            dict->setInferredType(python::Type::EMPTYDICT); // special empty dict case!
        }
    }

    void TypeAnnotatorVisitor::visit(NList* list) {
        ApatheticVisitor::visit(list);

        // check if empty, if so annotate with EMPTYLIST
        if(list->_elements.empty()) {
            list->setInferredType(python::Type::EMPTYLIST);
        } else {
            auto valType = list->_elements[0]->getInferredType();
            for(auto& el : list->_elements) {
                if(el->getInferredType().isListType() && el->getInferredType() != python::Type::EMPTYLIST) {
                    list->setInferredType(python::Type::makeListType(python::Type::PYOBJECT));
                    addCompileError(CompileError::TYPE_ERROR_LIST_OF_LISTS);
                    return;
                }
                if (el->getInferredType() != valType) {
                    list->setInferredType(python::Type::makeListType(python::Type::PYOBJECT));
                    addCompileError(CompileError::TYPE_ERROR_LIST_OF_MULTITYPES);
                    return;
                }
            }
            python::Type t = python::Type::makeListType(valType);
            list->setInferredType(t);
        }
    }

    void TypeAnnotatorVisitor::visit(NAttribute *attr) {
        auto& logger = Logger::instance().defaultLogger();

        // visit value (do not visit attribute, it is an identifier)
        attr->_value->accept(*this);
        // now check
        assert(attr->_attribute);
        assert(attr->_value);

        auto attribute_name = attr->_attribute->_name;
        auto object_name = attr->_value->getInferredType().desc(); // per default take the name

        // special case, value is identifier? use that!
        if(attr->_value->type() == ASTNodeType::Identifier) {
            object_name = static_cast<NIdentifier*>(attr->_value)->_name;
        }

        // in symbol table lookup what attribute of a value of type XYZ means
        auto object_type = attr->_value->getInferredType();

        if(object_type == python::Type::UNKNOWN) {
            std::stringstream ss;
            ss<<"failure in attribute visit "
              << "could not find " + object_name + "." + attribute_name + " in symbol table";

            // annotation available with types?
            if(attr->hasAnnotation()) {
                // directly take annotation and use to set types!
                object_type = attr->annotation().majorityType();
                if(object_type != python::Type::UNKNOWN) {
                    attr->setInferredType(object_type);
                }
            }

            if(object_type == python::Type::UNKNOWN) {
#ifndef NDEBUG
                logger.debug(ss.str());
#endif
                fatal_error(ss.str());
            }
        }

        if(object_type == python::Type::MODULE) {
            assert(attr->_value->type() == ASTNodeType::Identifier);
            // fetch symbol and check attributes!
            auto sym = _symbolTable.findSymbol(object_name);

            if(!sym)
                error("could not find module " + object_name + " in symbol table.");

            // special case: external python symbol and annotations available?
            if(sym->symbolType == SymbolType::EXTERNAL_PYTHON && attr->hasAnnotation()) {
                // check for annotation and use that to type everything...
                object_type = attr->annotation().majorityType();
                if(object_type != python::Type::UNKNOWN) {
                    attr->setInferredType(object_type);
                    attr->_attribute->setInferredType(object_type);
                    return;
                }
            }

            // else, try to find attribute symbol in table & fetch its type!
            auto attr_sym = sym->findAttribute(attribute_name);
            if(!attr_sym) {
                fatal_error("could not find identifier " + attribute_name + " in " + object_name);
            }

            // for debug message
            auto fully_qualified_name = attr_sym->fullyQualifiedName();

            // note: this might be unknown! then call needs to specialize it...
            // --> if single type and no generic type function, just use this type.
            // Might lead to typing issues later, but will be caught then.
            if(attr_sym->symbolType != SymbolType::FUNCTION) {
                attr->_attribute->setInferredType(attr_sym->type());
                attr->setInferredType(attr_sym->type());
            } else {
                python::Type matchType = python::Type::UNKNOWN;

                // find matching param type if call is used! else, use unknown dummies...
                if(!attr_sym->findFunctionTypeBasedOnParameterType(_lastCallParameterType.top(), matchType))
                    error("could not find type match for parameter type " + _lastCallParameterType.top().desc() + " in attribute " + fully_qualified_name);

                attr->_attribute->setInferredType(matchType);
                attr->setInferredType(matchType);
            }

            // add symbol to annotation
            attr->_attribute->annotation().symbol = attr_sym;
            attr->annotation().symbol = sym;
        } else {
            // builtin type attribute
            if(object_type == python::Type::UNKNOWN) {
                error("lookup error, could not type attribute");
                return;
            }
            assert(object_type != python::Type::UNKNOWN);
            auto type = _symbolTable.findAttributeType(object_type, attr->_attribute->_name, _lastCallParameterType.top());

            // unknown type but annotation available? -> use that one
            if(type == python::Type::UNKNOWN && attr->_attribute->hasAnnotation()) {
                type = attr->_attribute->annotation().majorityType();
            } else {
                attr->setInferredType(type);
            }
            attr->_attribute->setInferredType(type);
        }
    }

    void TypeAnnotatorVisitor::visit(NSubscription *sub) {
        ApatheticVisitor::visit(sub);

        // there are only two valid typings yet allowed:
        // either value must be a tuple or a string
        assert(sub->_value);
        assert(sub->_expression);

        auto type = sub->_value->getInferredType();
        auto index_type = sub->_expression->getInferredType();

        // this is a null check operation. I.e. strip option from either type or index type
        if(type.isOptionType())
            type = type.getReturnType();
        if(index_type.isOptionType())
            index_type = index_type.getReturnType();


        if(type.isTupleType()) {
            // static check: if it is a constant expression
            // index type must be bool or int
            // @Todo: later support weird slice object... ==> replace in AST tree via [::] syntax to make simpler...
            // TypeError: tuple indices must be integers or slices, not float

            if(!(python::Type::BOOLEAN == index_type || python::Type::I64 == index_type)) {
                sub->setInferredType(python::Type::UNKNOWN);
                error("type error: string indices must be bool or int");
            } else {
                // check whether expression is a literal (after constant folding), if so: different types are supported
                if(sub->_expression->type() == ASTNodeType::Number || sub->_expression->type() == ASTNodeType::Boolean) {
                    // get index
                    int i = INT32_MIN;
                    static_assert(sizeof(int) == 4, "int must be 32 bit");
                    if(sub->_expression->type() == ASTNodeType::Number)
                        i = std::stoi(static_cast<NNumber*>(sub->_expression)->_value);
                    else if(sub->_expression->type() == ASTNodeType::Boolean)
                        i = static_cast<NBoolean*>(sub->_expression)->_value;

                    // correct i for negative indices (only once!)
                    if(i < 0)
                        i = type.parameters().size() + i;

                    // now access ith member of the tuple
                    if(i < 0 || i >= type.parameters().size()) {
                        sub->setInferredType(python::Type::UNKNOWN);
                        error("Index error: tried to access element at position" + std::to_string(i));
                    } else {
                        sub->setInferredType(type.parameters()[i]);
                    }

                } else {

                    // indexing is done via a runtime type:
                    // ==> this means, we need to do normal case inference (not supported yet)
                    // or: if the tuple consists of the same types, we can determine the type

                    // special case: empty tuple always yields an index error
                    if(type.parameters().size() == 0) {
                        error("indexing an empty tuple not legal. Index error.");
                        sub->setInferredType(python::Type::UNKNOWN);
                    } else {
                        auto tuple_element_type = type.parameters().front();
                        bool same_type = true;
                        for(const auto& el : type.parameters())
                            if(el != tuple_element_type)
                                same_type = false;

                        if(same_type) {
                            sub->setInferredType(tuple_element_type);
                        } else {
                            error("tuple has different element types being accessed by a non literal expression. Normal sampling inference not yet implemented.");
                            sub->setInferredType(python::Type::UNKNOWN);
                        }
                    }
                }
            }
        } else if(python::Type::STRING == type) {
            // for strings, the only valid indexing types are bool or int
            // else python3 raises TypeError: string indices must be integers
            if(!(python::Type::BOOLEAN == index_type || python::Type::I64 == index_type)) {
                sub->setInferredType(python::Type::UNKNOWN);
                error("type error: string indices must be bool or int");
            } else {
                // return type of indexing a string is always a string
                sub->setInferredType(python::Type::STRING);
            }
        } else if(python::Type::GENERICDICT == type) {
            // TODO: GENERICDICT TO ALLOW DICT OUTPUT
            sub->setInferredType(python::Type::PYOBJECT);
        } else if(python::Type::EMPTYDICT == type) {
            // subscripting an empty dict will always yield a KeyError. I.e. warn and set generic object
            error("subscripting an empty dictionary will always yield a KeyError. Please fix code");
            sub->setInferredType(python::Type::UNKNOWN);
        } else if(type.isDictionaryType()) {
            sub->setInferredType(type.valueType());
        } else if(python::Type::EMPTYLIST == type) {
            error("subscripting an empty list will always yield an IndexError. Please fix code");
            sub->setInferredType(python::Type::UNKNOWN);
        } else if(type.isListType()) {
            sub->setInferredType(type.elementType());
        } else if (python::Type::MATCHOBJECT == type) {
            sub->setInferredType(python::Type::STRING);
        } else {
            std::stringstream errMessage;
            errMessage<<"subscript operation [] is performed on unsupported type "<<type.desc();
            error(errMessage.str());
            sub->setInferredType(python::Type::UNKNOWN);
        }
    }

    void TypeAnnotatorVisitor::visit(NSlice *slicing) {
        ApatheticVisitor::visit(slicing);
        assert(slicing->_value);

        auto type = slicing->_value->getInferredType();

        // option type ==> use element type for inference (exception needs to be generated)
        if(type.isOptionType())
            type = type.getReturnType();


        if(python::Type::STRING == type) {
            slicing->setInferredType(type);
        } else if(type.isTupleType()) {
            assert(slicing->_slices.front()->type() == ASTNodeType::SliceItem);
            auto sliceItem = (NSliceItem*)slicing->_slices.front();
            if((sliceItem->_start && sliceItem->_start->getInferredType() != python::Type::I64 && sliceItem->_start->getInferredType() != python::Type::BOOLEAN)
               || (sliceItem->_end && sliceItem->_end->getInferredType() != python::Type::I64 && sliceItem->_end->getInferredType() != python::Type::BOOLEAN)
               || (sliceItem->_stride && sliceItem->_stride->getInferredType() != python::Type::I64 && sliceItem->_stride->getInferredType() != python::Type::BOOLEAN)) {
                error("type error: slice indices must be bool or int");
            }
            else {
                if((!sliceItem->_start || sliceItem->_start->type() == ASTNodeType::Number || sliceItem->_start->type() == ASTNodeType::Boolean)
                   || (!sliceItem->_end || sliceItem->_end->type() == ASTNodeType::Number || sliceItem->_end->type() == ASTNodeType::Boolean)
                   || (!sliceItem->_stride || sliceItem->_stride->type() == ASTNodeType::Number || sliceItem->_stride->type() == ASTNodeType::Boolean)) {
                    static_assert(sizeof(int) == 4, "int must be 32 bit");

                    // Static slice value calculations: taken from BlockGeneratorVisitor::tupleStaticSliceInst
                    int start, end, stride, size;

                    size = (int)slicing->_value->getInferredType().parameters().size();

                    // stride value
                    if(!sliceItem->_stride) stride = 1;
                    else if(sliceItem->_stride->type() == ASTNodeType::Number) stride = (int)static_cast<NNumber *>(sliceItem->_stride)->getI64();
                    else stride = static_cast<NBoolean *>(sliceItem->_stride)->_value;

                    // correct start/end values
                    // case 1: (-inf, -len) => 0 // for negative stride, goes to -1
                    // case 2: [-len, -1] => +len
                    // case 3: [0, len-1] => +0
                    // case 4: [len, inf) => len // for negative stride, goes to len-1

                    // start value
                    if(sliceItem->_start) {
                        // get value
                        if (sliceItem->_start->type() == ASTNodeType::Number)
                            start = (int) static_cast<NNumber *>(sliceItem->_start)->getI64();
                        else start = static_cast<NBoolean *>(sliceItem->_start)->_value;

                        // correct value
                        if(start < -size) {
                            if(stride < 0) start = -1;
                            else start = 0;
                        }
                        else if(start <= -1) start += size;
                        else if(start >= size) {
                            if(stride < 0) start = size-1;
                            else start = size;
                        }
                    }
                    else {
                        if(stride < 0) start = size - 1;
                        else start = 0;
                    }

                    // get value of end
                    if(sliceItem->_end) {
                        // get value
                        if (sliceItem->_end->type() == ASTNodeType::Number)
                            end = (int) static_cast<NNumber *>(sliceItem->_end)->getI64();
                        else end = static_cast<NBoolean *>(sliceItem->_end)->_value;

                        // correct value
                        if(end < -size) {
                            if(stride < 0) end = -1;
                            else end = 0;
                        }
                        else if(end  <= -1) end += size;
                        else if(end >= size) {
                            if(stride < 0) end = size-1;
                            else end = size;
                        }
                    }
                    else {
                        if(stride < 0) end = -1;
                        else end = size;
                    }

                    // collect types
                    std::vector<python::Type> typeVec;
                    for(int i=start; stride * i < stride * end; i+=stride) {
                        typeVec.push_back(slicing->_value->getInferredType().parameters()[i]);
                    }
                    if(typeVec.empty()) slicing->setInferredType(python::Type::EMPTYTUPLE);
                    else slicing->setInferredType(python::Type::makeTupleType(typeVec));
                }
                else {
                    error("slicing is unsupported on non-static, non-evaluated expressions");
                }
            }

        } else {
            error("slicing operation is performed on unsupported type " + type.desc());
            slicing->setInferredType(python::Type::UNKNOWN);
        }
    }

    void TypeAnnotatorVisitor::assignHelper(NIdentifier *id, python::Type type) {
        // it is assign, so set type to value!
        id->setInferredType(type);

        // overwrite in current name table!
        _nameTable[id->_name] = type;
    }

    void TypeAnnotatorVisitor::visit(NAssign *assign) {
        ApatheticVisitor::visit(assign);

        // now interesting part comes
        // check what left side is

        // TODO cases
        /**
         * id = id
         * id, id, ... = id/val
         * id, id, ... = id, val, ... (SPECIAL CASE even here for a, b = b, a)
         */
        if(assign->_target->type() == ASTNodeType::Identifier) {
            // Single identifier case
            //@Todo: check that symbol table contains target!

            // then check if identifier is already within symbol table. If not, add!
            NIdentifier* id = (NIdentifier*)assign->_target;
            assignHelper(id, assign->_value->getInferredType());
            if(assign->_value->getInferredType().isIteratorType()) {
                id->annotation().iteratorInfo = assign->_value->annotation().iteratorInfo;
                _iteratorInfoTable[id->_name] = assign->_value->annotation().iteratorInfo;
            }
        } else if(assign->_target->type() == ASTNodeType::Tuple) {
            // now we have a tuple assignment!
            // the right hand side MUST be some unpackable thing. Currently this is a tuple but later we will
            // have lists as well
            NTuple *ids = (NTuple *) assign->_target;
            auto rhsInferredType = assign->_value->getInferredType();
            // TODO add support for dictionaries, etc.
            if (rhsInferredType.isTupleType()) {
                // get the types contained in our tuple
                std::vector<python::Type> tupleTypes = rhsInferredType.parameters();
                if(ids->_elements.size() != tupleTypes.size()) {
                    error("Incorrect number of arguments to unpack in assignment");
                }

                for(unsigned long i = 0; i < ids->_elements.size(); i ++) {
                    auto elt = ids->_elements[i];
                    if(elt->type() != ASTNodeType::Identifier) {
                        error("Trying to assign to a non identifier in a tuple");
                    }
                    NIdentifier *id = (NIdentifier *) elt;
                    // assign each identifier to the type in the tuple at the corresponding index
                    assignHelper(id, tupleTypes[i]);
                }
            } else if(rhsInferredType == python::Type::STRING) {
                for(const auto& elt : ids->_elements) {
                    if(elt->type() != ASTNodeType::Identifier) {
                        error("Trying to assign to a non identifier in a tuple");
                    }
                    NIdentifier *id = (NIdentifier *) elt;
                    assignHelper(id, python::Type::STRING);
                }
            } else {
                error("bad type annotation in tuple assign");
            }
        } else {
            error("only assignment to tuples/identifiers supported yet!!!");
        }
        // in all cases, set the type of the entire assign
        // TODO we def want this in the single identifier case, but in general?
        assign->setInferredType(assign->_target->getInferredType());
    }

    void TypeAnnotatorVisitor::resolveNameConflicts(const std::unordered_map<std::string, python::Type> &table) {
        for(auto kv : table) {
            auto name = kv.first;
            auto type = kv.second;
            // conflicts with before table?
            if(_nameTable.find(name) != _nameTable.end()) {
                if(_nameTable[name] != type) {
                    // can we unify types?
                    auto uni_type = unifyTypes(type, _nameTable[name], _allowNumericTypeUnification);
                    if(uni_type != python::Type::UNKNOWN)
                        _nameTable[name] = uni_type;
                    else {
                        // need to speculate on this if-branch!
                        error("need to speculate because of type conflict for variable "
                              + name + " which has been declared within a branch  as " + type.desc()
                              + ", declared previously as " + _nameTable[name].desc());
                    }
                }
            } else {
                // no conflict, simply add variable!
                // => unbound local error in case it's not contained in both if_table and else_table!
                _nameTable[name] = type;
            }
        }
    }

    void TypeAnnotatorVisitor::resolveNamesForIfStatement(std::unordered_map<std::string, python::Type> &if_table,
                                                          std::unordered_map<std::string, python::Type> &else_table) {
        using namespace std;

        // resolve conflicts between if and else table!
        set<std::string> ifelse_names;
        for(auto kv : if_table)ifelse_names.insert(kv.first);
        for(auto kv : else_table)ifelse_names.insert(kv.first);
        for(auto name : ifelse_names) {
            // exists in both tables?
            auto it = if_table.find(name);
            auto jt = else_table.find(name);
            if(it != if_table.end() && jt != else_table.end()) {
                // check if there's a conflict
                auto if_type = it->second;
                auto else_type = jt->second;

                if(if_type != else_type) {
                    // check if they can be unified
                    auto uni_type = unifyTypes(if_type, else_type, _allowNumericTypeUnification);
                    if(uni_type != python::Type::UNKNOWN) {
                        if_table[name] = uni_type;
                        else_table[name] = else_type;
                    } else {
                        // need to speculate!
                        error("need to speculate, because can't unify variable " + name +
                              " type " + if_type.desc() + " in if branch with its type " +
                              else_type.desc() + " in else branch.");
                    }
                } else {
                    // same type, simply assign!
                    _nameTable[name] = if_type;
                }
            }
        }

        // resolve conflicts from if/else with before table
        resolveNameConflicts(if_table);
        resolveNameConflicts(else_table);
    }

    void TypeAnnotatorVisitor::visit(NIfElse* ifelse) {
        using namespace std;

        auto visit_t = whichBranchToVisit(ifelse);
        auto visit_ifelse = std::get<0>(visit_t);
        auto visit_if = std::get<1>(visit_t);
        auto visit_else = std::get<2>(visit_t);

        // in speculation mode?
        bool speculate = ifelse->annotation().numTimesVisited > 0 || (ifelse->_else && ifelse->_else->annotation().numTimesVisited > 0);

        // backup name table, this is required to then deduce type conflicts:
        auto nameTable_before = _nameTable;
        unordered_map<string, python::Type> if_table;
        unordered_map<string, python::Type> else_table;

        // manually visit (from ApatheticVisitor but accounting for annotations)
        if(visit_ifelse) {

            // first expression
            ifelse->_expression->accept(*this); // this wont' create a new scope b.c. := not yet supported!

            // then, then block if desired
            if(visit_if) {
                ifelse->_then->accept(*this);
                if_table = _nameTable;
            }

            // reset name table
            _nameTable = nameTable_before;

            if(visit_else) {
                ifelse->_else->accept(*this);
                else_table = _nameTable;
            }
        } else return;

        // update scopes, i.e. unifying or exception check via annotation!
        // only if?
        if(!ifelse->isExpression()) {
            // we do have a if statement. Hence, now time to resolve type conflicts

            // first check whether a block is an exit block. Then, it doesn't matter what symbols get defined there
            if(isExitPath(ifelse->_then))
                if_table.clear();
            if(ifelse->_else && isExitPath(ifelse->_else))
                else_table.clear();

            // now, check which symbols have a potential type conflict with the previous table
            // note, we need to check also symbols defined within each for conflict!
            _nameTable = nameTable_before; // reset again.

            // speculative? I.e., when annotations exist.
            // @TODO: refactor with speculative processing indicator?
            if(speculate) {

                // TODO: refactor this into a function (i.e. the rule)
                // the speculation rule.
                // decide which branch to visit based on majority
                auto numTimesIfVisited = ifelse->_then->annotation().numTimesVisited;
                // for else, if else branch exists, take that annotation number. Else, simply check how often if is not visited.
                auto numTimesElseVisited = ifelse->_else ? ifelse->_else->annotation().numTimesVisited : ifelse->annotation().numTimesVisited - numTimesIfVisited;

                if(numTimesIfVisited >= numTimesElseVisited) {
                    visit_if = true;
                    visit_else = false;
                } else {
                    visit_if = false;
                    visit_else = ifelse->_else != nullptr; // can be also it's a single if statement!
                }

                // Note: also both can be false in the case of a single if!

                // => just take the updates from the branch that gets executed
                if(visit_if)
                    for(const auto& kv : if_table) {
                        _nameTable[kv.first] = kv.second;
                    }
                if(visit_else)
                    for(const auto& kv : else_table) {
                        _nameTable[kv.first] = kv.second;
                    }
            } else {
                // attempt type unification
                resolveNamesForIfStatement(if_table, else_table);
            }
        }

        assert(ifelse->_then && ifelse->_expression);
        // @Todo: make this work using the normal/exception case model.

        // both branches visited?
        if(visit_if && visit_else) {
            // for now limit to both return types being the same
            // check what type is there
            if(ifelse->_else) {
                // check whether types are compatible.
                auto iftype = ifelse->_then->getInferredType();
                auto elsetype = ifelse->_else->getInferredType();

                // expression? then this is fatal.
                // else, if we have exit branches, combining the type doesn't matter
                if(ifelse->isExpression()) {
                    // check:
                    if (iftype != elsetype) {
                        auto combined_type = unifyTypes(iftype, elsetype, _allowNumericTypeUnification);
                        if(combined_type == python::Type::UNKNOWN)
                            error("could not combine type " + iftype.desc() +
                                  " of if-branch with type " + elsetype.desc() + " of else-branch in if-else expression");
                        ifelse->setInferredType(combined_type);
                    } else
                        // take type from then branch
                        ifelse->setInferredType(ifelse->_then->getInferredType());
                } else {
                    // if it's not an expression, types do not matter.
#ifndef NDEBUG
                    // for better debugging convenience.
                    bool isIfExit = isExitPath(ifelse->_then);
                    bool isElseExit = ifelse->_else ? isExitPath((ifelse->_else)) : false;
#endif
                    ifelse->setInferredType(python::Type::NULLVALUE);
                }
            } else
                // take type from then branch
                ifelse->setInferredType(ifelse->_then->getInferredType());
        } else if(visit_if) {
            // only if branch visited => take this branch's type
            ifelse->setInferredType(ifelse->_then->getInferredType());
            if(ifelse->_else)
                ifelse->_else->setInferredType(python::Type::UNKNOWN);
        } else if(visit_else) {
            ifelse->setInferredType(ifelse->_else->getInferredType());
            ifelse->_then->setInferredType(python::Type::UNKNOWN);
        }
    }

    void TypeAnnotatorVisitor::visit(NRange* range) {
        ApatheticVisitor::visit(range);
        range->setInferredType(python::Type::RANGE);
    }

    void TypeAnnotatorVisitor::visit(NComprehension* comprehension) {

        // note that grammar specifies them as
        // comprehension ::=  assignment_expression comp_for
        // comp_for      ::=  ["async"] "for" target_list "in" or_test [comp_iter]
        // comp_iter     ::=  comp_for | comp_if
        // comp_if       ::=  "if" expression_nocond [comp_iter]

        // each comprehension adds variables to the name table,
        // i.e. for the target list!
        // target_list     ::=  target ("," target)* [","]
        // -> update!
        setLastParent(comprehension);
        assert(comprehension->target);
        assert(comprehension->iter);
        comprehension->iter->accept(*this); // iter needs to be visited first, for typing
        setLastParent(comprehension);

        // update name table based on iter
        auto target_list = comprehension->target;
        auto generator_type = comprehension->iter->getInferredType();
        switch(target_list->type()) {
            case ASTNodeType::Identifier: {
                // simplest case
                auto id = (NIdentifier*)target_list; assert(id);

                // iter type?
                // @TODO: https://github.com/LeonhardFS/Tuplex/issues/211
                // when addressing this, there should be an option to define the generator type based on implicit
                // iter object, e.g. on sequence objects like strings, dicts, tuples, lists, sets, ...
                // special case comprehension

                auto name = id->_name;

                if(generator_type.isListType()) {
                    _nameTable[name] = generator_type.elementType();
                } else if(generator_type == python::Type::STRING)
                    _nameTable[name] = python::Type::STRING;
                else if(generator_type == python::Type::RANGE) {
                    // this is wrong.
                    // could be i64 or f64. Wrong typing???
                    // @TODO: https://github.com/LeonhardFS/Tuplex/issues/211
                    Logger::instance().defaultLogger().debug("HACK: typing range per default as integer range. However, could be float too....");
                    _nameTable[name] = python::Type::I64;
                } else if(generator_type == python::Type::EMPTYLIST) {
                    _nameTable[name] = python::Type::UNKNOWN;
                } else if(generator_type.isListType()) {
                    _nameTable[name] = generator_type.elementType();
                } else if(generator_type == python::Type::EMPTYTUPLE) {
                    _nameTable[name] = python::Type::UNKNOWN;
                } else if(generator_type.isTupleType() && tupleElementsHaveSameType(generator_type)) {
                    _nameTable[name] = generator_type.parameters().front();
                } else {
                    // @TODO: https://github.com/LeonhardFS/Tuplex/issues/211
                    error("unsupported generator type " + generator_type.desc() + " encountered");
                    return;
                }

                id->setInferredType(_nameTable[name]);
                break;
            }
                // ----------------------------------------------------------------------------------------------------------
            case ASTNodeType::Tuple: {
                // @TODO:
                error("generators having multiple names as targets not yet supported. Please fix.");
                break;
            }
            default:
                error("can't assign to node of type" + target_list->_name + " from generator of type " + comprehension->getInferredType().desc());
        }

        // simply overrides vars in the target ast nodes b.c. nameTable was updated...
        comprehension->target->accept(*this);

        // visit if statements if they exist
        for(auto icond : comprehension->if_conditions) {
            assert(icond);
            setLastParent(comprehension);
            icond->accept(*this);
        }

        comprehension->setInferredType(python::Type::NULLVALUE); // not an executable astnode
    }

    void TypeAnnotatorVisitor::visit(NListComprehension* listComprehension) {
        // save and restore name table when comprehension is done.
        // @TODO: nested comprehensions?? => should be possible with this, because we do not save before comprehension objects
        // but rather for ListComprehension, ...
        auto name_backup = _nameTable;

        ApatheticVisitor::visit(listComprehension);
        for(const auto& generator: listComprehension->generators) { // check if any of the generators is an emptylist or empty tuple
            if(generator->iter->getInferredType() == python::Type::EMPTYLIST || generator->iter->getInferredType() == python::Type::EMPTYTUPLE) {
                listComprehension->setInferredType(python::Type::EMPTYLIST);
                return;
            }
        }

        listComprehension->setInferredType(python::Type::makeListType(listComprehension->expression->getInferredType()));

        // restore, because vars of comprehension do not count.
        _nameTable = name_backup;
    }

    void TypeAnnotatorVisitor::visit(NFor* forelse) {
        assert(forelse->target);
        assert(forelse->expression);
        assert(forelse->suite_body);
        forelse->expression->accept(*this);
        auto exprType = forelse->expression->getInferredType();
        auto targetASTType = forelse->target->type();

        assert(targetASTType == ASTNodeType::Identifier || targetASTType == ASTNodeType::Tuple || targetASTType == ASTNodeType::List);

        if(targetASTType == ASTNodeType::Identifier) {
            // target is identifier
            // expression can be list, string, range, or iterator
            auto id = static_cast<NIdentifier*>(forelse->target);
            if(exprType.isListType()) {
                if(exprType == python::Type::EMPTYLIST) {
                    _nameTable[id->_name] = python::Type::UNKNOWN;
                    id->setInferredType(python::Type::UNKNOWN);
                } else {
                    _nameTable[id->_name] = exprType.elementType();
                    id->setInferredType(exprType.elementType());
                }
            } else if(exprType.isTupleType()) {
                fatal_error("tuple should be handled through unrolling");
            } else if(exprType == python::Type::STRING) {
                _nameTable[id->_name] = exprType;
                id->setInferredType(exprType);
            } else if(exprType == python::Type::RANGE) {
                _nameTable[id->_name] = python::Type::I64;
                id->setInferredType(python::Type::I64);
            } else if(exprType.isIteratorType()) {
                _nameTable[id->_name] = exprType.yieldType();
                id->setInferredType(exprType.yieldType());
            } else {
                error("unsupported for loop expression type");
            }
        } else if(targetASTType == ASTNodeType::Tuple || targetASTType == ASTNodeType::List){
            // target is tuple of identifier (x, y)
            // expression should have (or can yield) the form [(type1, type2), (type1, type2), ...]
            auto idTuple = getForLoopMultiTarget(forelse->target);
            if(!(exprType.isListType() && exprType.elementType().isTupleType() ||
            exprType.isIteratorType() && (exprType.yieldType().isTupleType() || exprType.yieldType().isListType()))) {
                error("unsupported for loop expression type");
            }
            python::Type expectedTargetType;
            if(exprType.isListType()) {
                expectedTargetType = exprType.elementType();
            } else if(exprType.isIteratorType()) {
                expectedTargetType = exprType.yieldType();
            }

            forelse->target->setInferredType(expectedTargetType);
            if(expectedTargetType.isTupleType()) {
                auto idTypeTuple = expectedTargetType.parameters();
                if(idTuple.size() != idTypeTuple.size()) {
                    error("cannot unpack for loop expression");
                }
                for (int i = 0; i < idTuple.size(); i++) {
                    if(idTuple[i]->type() != ASTNodeType::Identifier) {
                        addCompileError(CompileError::TYPE_ERROR_MIXED_ASTNODETYPE_IN_FOR_LOOP_EXPRLIST);
                    }
                    auto element = static_cast<NIdentifier*>(idTuple[i]);
                    _nameTable[element->_name] = idTypeTuple[i];
                    element->setInferredType(idTypeTuple[i]);
                }
            } else if(expectedTargetType.isListType()) {
                auto idType = expectedTargetType.elementType();
                for (const auto& id : idTuple) {
                    if(id->type() != ASTNodeType::Identifier) {
                        addCompileError(CompileError::TYPE_ERROR_MIXED_ASTNODETYPE_IN_FOR_LOOP_EXPRLIST);
                    }
                    auto element = static_cast<NIdentifier*>(id);
                    _nameTable[element->_name] = idType;
                    element->setInferredType(idType);
                }
            } else {
                error("unexpected target type");
            }

        } else {
            fatal_error("unsupported AST node encountered in NFor");
        }

        forelse->suite_body->accept(*this);
        if (forelse->suite_else) {
            forelse->suite_else->accept(*this);
        }
    }

    void TypeAnnotatorVisitor::checkRetType(python::Type t) {

        if(t.isIteratorType()) {
            addCompileError(CompileError::TYPE_ERROR_RETURN_ITERATOR);
            return;
        }

        if(t.isListType() && !(t == python::Type::EMPTYLIST)) {
            if(t.elementType() == python::Type::PYOBJECT) {
                addCompileError(CompileError::TYPE_ERROR_RETURN_LIST_OF_MULTITYPES);
                error(compileErrorToStr(CompileError::TYPE_ERROR_RETURN_LIST_OF_MULTITYPES));
                return;
            }

            if(t.elementType().isTupleType() && t.elementType() != python::Type::EMPTYTUPLE) {
                addCompileError(CompileError::TYPE_ERROR_RETURN_LIST_OF_TUPLES);
                error(compileErrorToStr(CompileError::TYPE_ERROR_RETURN_LIST_OF_TUPLES));
                return;
            }
            if(t.elementType().isDictionaryType() && t.elementType() != python::Type::EMPTYDICT) {
                addCompileError(CompileError::TYPE_ERROR_RETURN_LIST_OF_DICTS);
                error(compileErrorToStr(CompileError::TYPE_ERROR_RETURN_LIST_OF_DICTS));
                return;
            }
            if(t.elementType().isListType() && t.elementType() != python::Type::EMPTYLIST) {
                addCompileError(CompileError::TYPE_ERROR_RETURN_LIST_OF_LISTS);
                error(compileErrorToStr(CompileError::TYPE_ERROR_RETURN_LIST_OF_LISTS));
                return;
            }
        }
        // recursively check into tuple
        if(t.isTupleType()) {
            for (const auto &tt : t.parameters()) {
                checkRetType(tt);
            }
        }
    }
}