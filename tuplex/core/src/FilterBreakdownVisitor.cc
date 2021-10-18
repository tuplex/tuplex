//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by rahuly first first on 02/13/19                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <FilterBreakdownVisitor.h>
#include <ASTHelpers.h>

namespace tuplex {
    void FilterBreakdownVisitor::getVarNames(ASTNode *node) {
        if (!node) return;
        std::vector<std::string> names;
        switch (node->type()) {
            case ASTNodeType::Lambda: {
                auto *lam = (NLambda *) node;
                names = lam->parameterNames();
                for(const auto &param : lam->_arguments->_args) {
                    if(param->type() == ASTNodeType::Parameter) {
                        auto p = (NParameter*)param.get();
                        _variablesToIgnore.insert(p->_identifier.get());
                    }
                }
                break;
            }
            case ASTNodeType::Function: {
                auto *func = (NFunction *) node;
                names = func->parameterNames();
                for(const auto &param : func->_parameters->_args) {
                    if(param->type() == ASTNodeType::Parameter) {
                        auto p = (NParameter*)param.get();
                        _variablesToIgnore.insert(p->_identifier.get());
                    }
                }
                break;
            }
            default: {
                return;
            }
        }
        for (int i = 0; i < names.size(); ++i)
            _varNames[names[i]] = i;
    }

    void FilterBreakdownVisitor::ignoreVariable(ASTNode *node) {
      _variablesToIgnore.insert(node);
      if(node->type() == ASTNodeType::Subscription) {
          auto subscript = (NSubscription*)node;
          if (subscript->_value->type() == ASTNodeType::Identifier)
              _variablesToIgnore.insert(subscript->_value.get());
          if(subscript->_expression->type() == ASTNodeType::Identifier)
              _variablesToIgnore.insert(subscript->_expression.get());
      }
    }

    void FilterBreakdownVisitor::preOrder(ASTNode *node) {
        if (!node) return;

        // if we get the function or lambda, let's save the param names!
        getVarNames(node);

        // fix for unary or binary logical op
        // -> only support if they have cmp, or and/or or not op as children
        switch (node->type()) {
            case ASTNodeType::BinaryOp: {
                auto binop = (NBinaryOp *) node;
                if (binop->_op == TokenType::AND || binop->_op == TokenType::OR)
                    if (!isLogicalOp(binop->_left.get()) && !isLogicalOp(binop->_right.get()))
                        error("Not a pure logical expression - can't break up");
                break;
            }
            case ASTNodeType::UnaryOp: {
                auto unop = (NUnaryOp *) node;
                if (unop->_op == TokenType::NOT)
                    if (!isLogicalOp(unop->_operand.get()))
                        error("Not a pure logical expression - can't break up");
                break;
            }
            case ASTNodeType::Compare: {
                auto cmp = dynamic_cast<NCompare*>(node);
                ignoreVariable(cmp->_left.get());
                for(const auto& c : cmp->_comps) ignoreVariable(c.get());
                break;
            }
        }
    }

    FilterBreakdownVisitor::IntervalCollection
    FilterBreakdownVisitor::fromCompare(const TokenType &op, ASTNode *right) {
        using namespace std;

        if (!right)
            return IntervalCollection();

        // var op right
        // what type of right node do we have?
        if ((right->type() == ASTNodeType::Number) ||
            ((right->type() == ASTNodeType::UnaryOp) && (((NUnaryOp *) right)->_operand->type() == ASTNodeType::Number))
            ) {
            NNumber *num;
            int64_t sign = 1;
            if(right->type() == ASTNodeType::Number) num = (NNumber *) right;
            else {
                auto unop = (NUnaryOp*)right;
                num = (NNumber*)unop->_operand.get();
                if (unop->_op == TokenType::MINUS) sign = -1;
            }

            // left op var
            switch (op) {
                case TokenType::EQEQUAL: {
                    Interval I;
                    if (right->getInferredType() == python::Type::I64) {
                        return IntervalCollection({Interval(sign * num->getI64(), sign * num->getI64())});
                    } else {
                        return IntervalCollection({Interval(sign * num->getF64(), sign * num->getF64())});
                    }

                    break;
                }
                case TokenType::GREATER:
                case TokenType::GREATEREQUAL: {
                    // x > right
                    Interval I;
                    if (right->getInferredType() == python::Type::I64) {
                        I.type = python::Type::I64;
                        return IntervalCollection(
                                {Interval(sign * num->getI64() + (op == TokenType::GREATER), numeric_limits<int64_t>::max())});
                    } else {
                        // we do not care about errs here, else add eps. Yet, original filter is being retained.
                        return IntervalCollection({Interval(sign * num->getF64(), numeric_limits<double>::max())});
                    }
                }
                case TokenType::LESS:
                case TokenType::LESSEQUAL: {
                    // x < right
                    if (right->getInferredType() == python::Type::I64) {
                        return IntervalCollection(
                                {Interval(numeric_limits<int64_t>::min(), sign * num->getI64() - (op == TokenType::LESS))});
                    } else {
                        // we do not care about errs here, else add eps. Yet, original filter is being retained.
                        return IntervalCollection({Interval(numeric_limits<double>::min(), sign * num->getF64())});
                    }
                    break;
                }
                case TokenType::NOTEQUAL: {
                    // two intervals
                    if (right->getInferredType() == python::Type::I64) {
                        return IntervalCollection({Interval(numeric_limits<int64_t>::min(), sign * num->getI64() - 1),
                                                   Interval(sign * num->getI64() + 1, numeric_limits<int64_t>::max())});
                    } else {
                        // eps?
                        // => don't care
                        // full interval, hence return empty collection.
                    }
                    break;
                }
            }
        }

        if (right->type() == ASTNodeType::String) {
            auto str = (NString *) right;

            // left op var
            switch (op) {
                case TokenType::EQEQUAL: {
                    return IntervalCollection({Interval(str->value(), str->value(), true, true)});
                }
                case TokenType::GREATER:
                case TokenType::GREATEREQUAL: {
                    // x > right
                    return IntervalCollection({Interval(str->value(), op == TokenType::GREATEREQUAL)});
                }
                case TokenType::LESS:
                case TokenType::LESSEQUAL: {
                    // x < right
                    return IntervalCollection({Interval("", str->value(), true, op == TokenType::LESSEQUAL)});
                }
                case TokenType::NOTEQUAL: {
                    // two intervals
                    return IntervalCollection({
                        Interval("", str->value(), true, false),
                        Interval(str->value(), false)
                    });
                }
            }
        }

        // list or tuple?
        if (right->type() == ASTNodeType::List || right->type() == ASTNodeType::Tuple) {
            // check if all elements are of same type
            vector<ASTNode *> elements;
            if (right->type() == ASTNodeType::List) {
                for (const auto &node : ((NList *) right)->_elements)
                    elements.push_back(node.get());
            }
            if (right->type() == ASTNodeType::Tuple) {
                for (const auto &node : ((NTuple *) right)->_elements)
                    elements.push_back(node.get());
            }

            // TODO: what about dict?
            if (elements.empty()) {
                // this is an always false statement. should be caught by ReduceExpressionsVisitor
                fatal_error("should get caught by reducing expressions!");
            }

            auto element_type = elements.front()->getInferredType();
            // upcast??
            // @TODO: ignore for now, else use this!!!
            for (const auto el : elements) {
                // all number + same type?
                if(!(el->type() == ASTNodeType::Number || el->type() == ASTNodeType::String)) {
                    return IntervalCollection(); // abort - not a constant
                }
                if (el->getInferredType() != element_type) {
                    return IntervalCollection(); // abort, not homogeneous type
                }
            }

            // all numbers + same type
            // => create point intervals!
            vector<Interval> intervals;
            for (const auto el : elements) {
                assert(el->type() == ASTNodeType::Number || el->type() == ASTNodeType::String);
                if(el->type() == ASTNodeType::Number) {
                    auto num = (NNumber *) el;

                    // NOTIN or IN?
                    if (op == TokenType::NOTIN) {
                        // care only about integers!
                        if (element_type == python::Type::I64) {
                            Interval I1;
                            I1.type = python::Type::I64;
                            I1.iMin = numeric_limits<int64_t>::min();
                            I1.iMax = num->getI64() - 1;
                            I1.empty = false;
                            Interval I2;
                            I2.type = python::Type::I64;
                            I2.iMin = num->getI64() + 1;
                            I2.iMax = numeric_limits<int64_t>::max();
                            I2.empty = false;
                            intervals.push_back(I1);
                            intervals.push_back(I2);
                        } // for float, do not care...
                    } else {
                        assert(op == TokenType::IN);
                        if (element_type == python::Type::I64) {
                            Interval I;
                            I.type = python::Type::I64;
                            I.iMin = num->getI64();
                            I.iMax = num->getI64();
                            I.empty = false;
                            intervals.push_back(I);
                        }
                    }
                } else {
                    assert(el->type() == ASTNodeType::String);
                    auto str = (NString*)el;
                    if (op == TokenType::NOTIN) {
                        intervals.emplace_back("", str->value(), true, false);
                        intervals.emplace_back(str->value(), false);
                    } else {
                        assert(op == TokenType::IN);
                        intervals.emplace_back(str->value(), str->value(), true, true);
                    }
                }
            }

            return IntervalCollection(intervals);
        }

        // return empty collection
        return IntervalCollection(std::vector<Interval>{});
    }

    void FilterBreakdownVisitor::addICToIdentifierIfValid(const NIdentifier* identifier, const IntervalCollection &ic, std::unordered_map<int64_t, IntervalCollection> &variableRanges) {
        // directly referring to a variable?
        auto it = _varNames.find(identifier->_name);
        if (it != _varNames.end()) {
            auto columnIndex = it->second;
            // intervals done, add them via and to current lookup
            auto jt = variableRanges.find(columnIndex);
            if (jt == variableRanges.end() && !ic.intervals.empty()) // do not push empty ones
                variableRanges[columnIndex] = ic;
            else
                variableRanges[columnIndex].logicalAnd(ic); // add via &&
        }
    }

    void FilterBreakdownVisitor::addICToSubscriptionIfValid(const NSubscription* subscript, const IntervalCollection &ic, std::unordered_map<int64_t, IntervalCollection> &variableRanges) {
        // on left side one of the column vars?
        if (subscript->_value->type() == ASTNodeType::Identifier &&
            (subscript->_expression->type() == ASTNodeType::Number ||
             subscript->_expression->type() == ASTNodeType::String)) {
            // static, so can extract info
            auto id_name = ((NIdentifier *) subscript->_value.get())->_name;
            // var?
            if (_varNames.find(id_name) != _varNames.end()) {
                // Note: when supporting assignments, change this!
                int columnIndex = -1;
                if (subscript->_expression->type() == ASTNodeType::Number) {
                    // only integer access works...
                    if (subscript->_expression->getInferredType() == python::Type::I64)
                        columnIndex = ((NNumber *) subscript->_expression.get())->getI64();
                } else {
                    // extract string & lookup index, if not successful leave..
                    auto it = _columnToIndexMap.find(((NString *) subscript->_expression.get())->value());
                    if (it != _columnToIndexMap.end())
                        columnIndex = it->second;
                }

                // extract worked and a column index was detected?
                if (columnIndex != -1) {
                    // intervals done, add them via and to current lookup
                    auto it = variableRanges.find(columnIndex);
                    if (it == variableRanges.end() && !ic.intervals.empty()) // do not push empty ones
                        variableRanges[columnIndex] = ic;
                    else
                        variableRanges[columnIndex].logicalAnd(ic); // add via &&
                }
            }
        }
    }

    void FilterBreakdownVisitor::postOrder(ASTNode *node) {
        using namespace std;

        if (!node) return;
        if (failed()) return; // not a filter that can be broken down

        // nodes, fail if desired...
        auto &logger = Logger::instance().logger("logical optimizer(filter breakdown)");

        switch (node->type()) {
            case ASTNodeType::Module:
            case ASTNodeType::Suite:
            case ASTNodeType::Function:
            case ASTNodeType::Lambda:
            case ASTNodeType::Parameter:
            case ASTNodeType::ParameterList: {
                // skip, handled in preOrder
                break;
            }

            case ASTNodeType::Return: {
                // single return -> fine.
                break;
            }

            case ASTNodeType::Assign: {
                // assigns are not yet supported!
                // => i.e. need name-table and co to decide this!
                // this is also applies to if... else.. because they require restore etc.
                error("contains an assign operator - can't break down filter");
                break;
            }
            case ASTNodeType::IfElse: {
                error("contains an if/else clause - can't break down filter");
                break;
            }

            // for range definitions, we care only about compare
            case ASTNodeType::Compare: {
                auto cmp = (NCompare *) node;

                // resulting structure is basically lookup of variable name to interval collection
                unordered_map<int64_t, IntervalCollection> variableRanges;

                // go through each entry and check whether it's an identifier or subscript of it!
                vector<ASTNode *> cmp_nodes{cmp->_left.get()};
                for (const auto &n : cmp->_comps)
                    cmp_nodes.push_back(n.get());

                for (int i = 0; i < cmp_nodes.size(); ++i) {
                    // for each side of operator check whether it's a literal to detect a possible range
                    // => note: cleanastvisitor + reduceexpressions visitor should have performed constant folding already!
                    auto leftNode = i > 0 ? cmp_nodes[i - 1] : nullptr;
                    auto rightNode = i != cmp_nodes.size() - 1 ? cmp_nodes[i + 1] : nullptr;

                    // construct interval based on operators
                    // first, leftnode interval

                    IntervalCollection ic;
                    if (leftNode)
                        ic.logicalAnd(fromCompare(flipCompareOrder(cmp->_ops[i - 1]), leftNode));
                    if (rightNode)
                        ic.logicalAnd(fromCompare(cmp->_ops[i], rightNode));


                    // does range apply to a variable?
                    // check if referring to input column or not
                    auto cur_node = cmp_nodes[i];
                    if (cur_node->type() == ASTNodeType::Identifier) {
                        addICToIdentifierIfValid((NIdentifier *)cur_node, ic, variableRanges);
                    }
                    if (cur_node->type() == ASTNodeType::Subscription) {
                        addICToSubscriptionIfValid((NSubscription *) cur_node, ic, variableRanges);
                    }
                }


                // remove all empty interval collections and push result
                // done inline above via && !ic.intervals.empty() checks...
                _rangesStack.push(variableRanges);
                break;
            }

                // logical and or operations?
            case ASTNodeType::BinaryOp: {
                auto binop = (NBinaryOp *) node;

                if (binop->_op == TokenType::AND) {
                    // get the two ranges
                    auto leftRange = stripEmpty(_rangesStack.top());
                    _rangesStack.pop();
                    auto rightRange = stripEmpty(_rangesStack.top());
                    _rangesStack.pop();

                    unordered_map<int64_t, IntervalCollection> merged_ranges = leftRange;
                    // add all from one, then merge in the other
                    for (const auto &keyval : rightRange) {
                        auto it = merged_ranges.find(keyval.first);
                        if (it != merged_ranges.end()) {
                            // logical and
                            merged_ranges[keyval.first].logicalAnd(keyval.second);
                        } else {
                            merged_ranges[keyval.first] = keyval.second;
                        }
                    }
                    _rangesStack.push(merged_ranges);
                } else if (binop->_op == TokenType::OR) {
                    // this is more difficult, basically ranges need to be combined via or
                    // get the two ranges
                    auto leftRange = stripEmpty(_rangesStack.top());
                    _rangesStack.pop();
                    auto rightRange = stripEmpty(_rangesStack.top());
                    _rangesStack.pop();


                    unordered_map<int64_t, IntervalCollection> merged_ranges = leftRange;
                    // add all from one, then merge in the other
                    for (const auto &keyval : rightRange) {
                        auto it = merged_ranges.find(keyval.first);
                        if (it != merged_ranges.end()) {
                            // logical and
                            merged_ranges[keyval.first].logicalOr(keyval.second);
                        } else {
                            merged_ranges[keyval.first] = keyval.second;
                        }
                    }
                    _rangesStack.push(merged_ranges);
                } else { error("unsupported binary operation: " + opToString(binop->_op)); }
                break;
            }

                // logical not?
            case ASTNodeType::UnaryOp: {
                auto unop = (NUnaryOp *) node;
                if(unop->_op == TokenType::NOT) {
                    // not => invert intervals!!!
                    auto range = stripEmpty(_rangesStack.top());
                    _rangesStack.pop();

                    // invert all intervals
                    std::unordered_map<int64_t, IntervalCollection> m;
                    for (auto kv : range) {
                        m[kv.first] = kv.second.logicalNot();
                    }
                    _rangesStack.push(m);
                }
                break;
            }

            case ASTNodeType::List:
            case ASTNodeType::Number:
            case ASTNodeType::String: {
                // don't care, handled somewhere else.
                // TODO: should ranges be not pushed for every single node??
                break;
            }

            case ASTNodeType::Subscription: {
                if(_variablesToIgnore.count(node) == 0) { // only add if this is not part of a compare
                    auto subscription = (NSubscription*)node;
                    std::vector<Interval> intervals;
                    if(subscription->getInferredType() == python::Type::I64) {
                        // everything but 0
                        intervals = {Interval(std::numeric_limits<int64_t>::min(), -1), Interval(1, std::numeric_limits<int64_t>::max())};
                    } else if(subscription->getInferredType() == python::Type::F64) {
                        // do nothing - we don't deal with single value comparison on float
                    } else if(subscription->getInferredType() == python::Type::STRING) {
                        intervals = {Interval("", false)}; // everything but empty string
                    } // do nothing otherwise; won't break correctness (because it's more inclusive)

                    // push this range
                    std::unordered_map<int64_t, IntervalCollection> r;
                    addICToSubscriptionIfValid(subscription, IntervalCollection(intervals), r);
                    _rangesStack.push(r);
                }
                break;
            }
            case ASTNodeType::Identifier: {
                if(_variablesToIgnore.count(node) == 0) { // only add if this is not part of a compare
                    auto identifier = (NIdentifier*)node;
                    std::vector<Interval> intervals;
                    if(identifier->getInferredType() == python::Type::I64) {
                        // everything but 0
                        intervals = {Interval(std::numeric_limits<int64_t>::min(), -1), Interval(1, std::numeric_limits<int64_t>::max())};
                    } else if(identifier->getInferredType() == python::Type::F64) {
                        // do nothing - we don't deal with single value comparison on float
                    } else if(identifier->getInferredType() == python::Type::STRING) {
                        intervals = {Interval("", false)}; // everything but empty string
                    } // do nothing otherwise; won't break correctness (because it's more inclusive)

                    // push this range
                    std::unordered_map<int64_t, IntervalCollection> r;
                    addICToIdentifierIfValid(identifier, IntervalCollection(intervals), r);
                    _rangesStack.push(r);
                }
                break;
            }

            default: {
                error("unsupported node type");
                break;
            }
        }
        // @TODO: missing is any assignment statements, variables etc.
        // => these have to be properly added!
    }
}