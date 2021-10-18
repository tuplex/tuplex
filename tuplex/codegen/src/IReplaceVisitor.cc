//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <IReplaceVisitor.h>
#include <cassert>

namespace tuplex {
    // avoid double free
    template<class T>
    void swap(std::unique_ptr<T> &unique_p, T* p) {
        if(unique_p.get() != p) {
            unique_p = std::unique_ptr<T>(p);
        }
    }

    void IReplaceVisitor::visit(NModule *module) {

        // special case fpr module, because it can be also an empty file
        if(module->_suite) {
            swap(module->_suite, replaceh(module, module->_suite.get()));
        }
    }

    void IReplaceVisitor::visit(NSuite *suite) {

        // one more optimization can be done:
        // I.e. all suite members can be flattened into this suite. Right now not done...
        // @TODO: do this

        // just call all children
        if(!suite->_statements.empty()) {
            for (auto it = suite->_statements.begin(); it != suite->_statements.end(); ++it) {

                // check whether statement can be optimized. Can be optimized iff result of optimizeNext is a different
                // memory address
                swap(*it, replaceh(suite, it->get()));
            }
        }
    }

    void IReplaceVisitor::visit(NCompare *cmp) {
        // visit children
        swap(cmp->_left, replaceh(cmp, cmp->_left.get()));

        // other side
        if(!cmp->_comps.empty())
            for(auto it = cmp->_comps.begin(); it != cmp->_comps.end(); ++it) {
                swap(*it, replaceh(cmp, it->get()));
            }
    }

    void IReplaceVisitor::visit(NFunction *function) {
        // @TODO: change such that types in AST can be only ASTNode! => optimizing the AST needs this flexibility!
        swap(function->_name, replaceh<NIdentifier>(function, function->_name.get()));
        swap(function->_parameters, replaceh<NParameterList>(function, function->_parameters.get()));
        if(function->_annotation)
            swap(function->_annotation, replaceh(function, function->_annotation.get()));
        assert(function->_suite);
        swap(function->_suite, replaceh(function, function->_suite.get()));

    }

//@TODO: fix optimization (removal of cmp) for parameterList!!!!
    void IReplaceVisitor::visit(NParameterList *paramList) {
        if(!paramList->_args.empty())
            for(auto it = paramList->_args.begin(); it != paramList->_args.end(); ++it) {
                swap(*it, replaceh(paramList, it->get()));
            }
    }

    void IReplaceVisitor::visit(NStarExpression *se) {
        swap(se->_target, replaceh(se, se->_target.get()));
    }

    void IReplaceVisitor::visit(NLambda *lambda) {
        swap(lambda->_arguments, replaceh<NParameterList>(lambda, lambda->_arguments.get()));
        swap(lambda->_expression, replaceh(lambda, lambda->_expression.get()));
    }

    void IReplaceVisitor::visit(NUnaryOp *op) {
        swap(op->_operand, replaceh(op, op->_operand.get()));
    }

    void IReplaceVisitor::visit(NBinaryOp *op) {
        swap(op->_left, replaceh(op, op->_left.get()));
        swap(op->_right, replaceh(op, op->_right.get()));
    }

    void IReplaceVisitor::visit(NParameter *param) {
        swap(param->_identifier, replaceh<NIdentifier>(param, param->_identifier.get()));
        if(param->_annotation)
            swap(param->_annotation, replaceh(param, param->_annotation.get()));
        if(param->_default)
            swap(param->_default, replaceh(param, param->_default.get()));
    }

    void IReplaceVisitor::visit(NAwait *await) {
        assert(await->_target);
        swap(await->_target, replaceh(await, await->_target.get()));
    }

    void IReplaceVisitor::visit(NTuple *tuple) {
        if(!tuple->_elements.empty()) {
            for(auto it = tuple->_elements.begin(); it != tuple->_elements.end(); ++it) {
                swap(*it, replaceh(tuple, it->get()));
            }
        }
    }

    void IReplaceVisitor::visit(NDictionary *dict) {
        if(!dict->_pairs.empty()) {
            for(auto it = dict->_pairs.begin(); it != dict->_pairs.end(); ++it) {
                swap(it->first, replaceh(dict, it->first.get()));
                swap(it->second, replaceh(dict, it->second.get()));
            }
        }
    }

    void IReplaceVisitor::visit(NList *list) {
        if(!list->_elements.empty()) {
            for(auto it = list->_elements.begin(); it != list->_elements.end(); ++it) {
                swap(*it, replaceh(list, it->get()));
            }
        }
    }

    void IReplaceVisitor::visit(NSubscription *sub) {
        assert(sub->_expression);
        swap(sub->_value, replaceh(sub, sub->_value.get()));
        swap(sub->_expression, replaceh(sub, sub->_expression.get()));
    }

    void IReplaceVisitor::visit(NReturn* ret) {
        if(ret->_expression)
            swap(ret->_expression, replaceh(ret, ret->_expression.get()));
    }

    void IReplaceVisitor::visit(NAssign* as) {
        assert(as->_target);
        assert(as->_value);

        swap(as->_value, replaceh(as, as->_value.get()));
        swap(as->_target, replaceh(as, as->_target.get()));
    }

    void IReplaceVisitor::visit(NCall* call) {
        // visit children
        for(auto& parg : call->_positionalArguments) {
            swap(parg, replaceh(call, parg.get()));
        }

        swap(call->_func, replaceh(call, call->_func.get()));
    }

    void IReplaceVisitor::visit(NAttribute *attr) {

        assert(attr->_value);
        assert(attr->_attribute);

        swap(attr->_value, replaceh(attr, attr->_value.get()));
        swap(attr->_attribute, replaceh<NIdentifier>(attr, attr->_attribute.get()));
    }

    void IReplaceVisitor::visit(NSlice *slicing) {
        assert(!slicing->_slices.empty());
        swap(slicing->_value, replaceh(slicing, slicing->_value.get()));
        if(!slicing->_slices.empty()) {
            for(auto it = slicing->_slices.begin(); it != slicing->_slices.end(); ++it) {
                swap(*it, replaceh(slicing, it->get()));
            }
        }
    }

    void IReplaceVisitor::visit(NSliceItem *slicingItem) {
        if(slicingItem->_start)
            swap(slicingItem->_start, replaceh(slicingItem, slicingItem->_start.get()));
        if(slicingItem->_end)
            swap(slicingItem->_end, replaceh(slicingItem, slicingItem->_end.get()));
        if(slicingItem->_stride)
            swap(slicingItem->_stride, replaceh(slicingItem, slicingItem->_stride.get()));
    }

    void IReplaceVisitor::visit(NIfElse *ife) {
        swap(ife->_expression, replaceh(ife, ife->_expression.get()));
        swap(ife->_then, replaceh(ife, ife->_then.get()));
        if(ife->_else)
            swap(ife->_else, replaceh(ife, ife->_else.get()));
    }

    void IReplaceVisitor::visit(NRange* range) {
        // visit children
        for(auto& parg : range->_positionalArguments) {
            swap(parg, replaceh(range, parg.get()));
        }
    }

    void IReplaceVisitor::visit(NComprehension* comprehension) {
        swap(comprehension->target, replaceh<NIdentifier>(comprehension, comprehension->target.get()));
        swap(comprehension->iter, replaceh(comprehension, comprehension->iter.get()));

        // visit children
        for(auto& icond : comprehension->if_conditions) {
            swap(icond, replaceh(comprehension, icond.get()));
        }
    }

    void IReplaceVisitor::visit(NListComprehension *listComprehension) {
        // visit children
        for(auto& gen : listComprehension->generators) {
            swap(gen, replaceh<NComprehension>(listComprehension, gen.get()));
        }

        swap(listComprehension->expression, replaceh(listComprehension, listComprehension->expression.get()));
    }

    void IReplaceVisitor::visit(NAssert* assert_) {
        swap(assert_->_expression, replaceh(assert_, assert_->_expression.get()));
        swap(assert_->_errorExpression, replaceh(assert_, assert_->_errorExpression.get()));
    }

    void IReplaceVisitor::visit(NRaise *raise) {
        swap(raise->_expression, replaceh(raise, raise->_expression.get()));
        swap(raise->_fromExpression, replaceh(raise, raise->_fromExpression.get()));
    }

    void IReplaceVisitor::visit(NWhile *node) {
        swap(node->expression, replaceh(node, node->expression.get()));
        swap(node->suite_body, replaceh(node, node->suite_body.get()));
        swap(node->suite_else, replaceh(node, node->suite_else.get()));
    }

    void IReplaceVisitor::visit(NFor* node) {
        swap(node->target, replaceh(node, node->target.get()));
        swap(node->expression, replaceh(node, node->expression.get()));
        swap(node->suite_body, replaceh(node, node->suite_body.get()));
        swap(node->suite_else, replaceh(node, node->suite_else.get()));
    }
}
