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
    void IReplaceVisitor::visit(NModule *module) {

        // special case fpr module, because it can be also an empty file
        if(module->_suite) {
            module->_suite = replaceh(module, module->_suite);
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
                *it = replaceh(suite, *it);
            }
        }
    }

    void IReplaceVisitor::visit(NCompare *cmp) {
        // visit children
        cmp->_left = replaceh(cmp, cmp->_left);

        // other side
        if(!cmp->_comps.empty())
            for(auto it = cmp->_comps.begin(); it != cmp->_comps.end(); ++it) {
                *it = replaceh(cmp, *it);
            }
    }

    void IReplaceVisitor::visit(NFunction *function) {
        // @TODO: change such that types in AST can be only ASTNode! => optimizing the AST needs this flexibility!
        function->_name = (NIdentifier*)replaceh(function, function->_name);
        function->_parameters = (NParameterList*)replaceh(function, function->_parameters);
        if(function->_annotation)
            function->_annotation = replaceh(function, function->_annotation);
        assert(function->_suite);
        function->_suite = replaceh(function, function->_suite);

    }

//@TODO: fix optimization (removal of cmp) for parameterList!!!!
    void IReplaceVisitor::visit(NParameterList *paramList) {
        if(!paramList->_args.empty())
            for(auto it = paramList->_args.begin(); it != paramList->_args.end(); ++it) {
                *it = replaceh(paramList, *it);
            }
    }

    void IReplaceVisitor::visit(NStarExpression *se) {
        se->_target = replaceh(se, se->_target);
    }

    void IReplaceVisitor::visit(NLambda *lambda) {
        lambda->_arguments = (NParameterList*)replaceh(lambda, lambda->_arguments);
        lambda->_expression = replaceh(lambda, lambda->_expression);
    }

    void IReplaceVisitor::visit(NUnaryOp *op) {
        op->_operand = replaceh(op, op->_operand);
    }

    void IReplaceVisitor::visit(NBinaryOp *op) {
        op->_left = replaceh(op, op->_left);
        op->_right = replaceh(op, op->_right);
    }

    void IReplaceVisitor::visit(NParameter *param) {
        param->_identifier = (NIdentifier*)replaceh(param, param->_identifier);
        if(param->_annotation)
            param->_annotation = replaceh(param, param->_annotation);
        if(param->_default)
            param->_default = replaceh(param, param->_default);
    }

    void IReplaceVisitor::visit(NAwait *await) {
        assert(await->_target);
        await->_target = replaceh(await, await->_target);
    }

    void IReplaceVisitor::visit(NTuple *tuple) {
        if(!tuple->_elements.empty()) {
            for(auto it = tuple->_elements.begin(); it != tuple->_elements.end(); ++it) {
                *it = replaceh(tuple, *it);
            }
        }
    }

    void IReplaceVisitor::visit(NDictionary *dict) {
        if(!dict->_pairs.empty()) {
            for(auto it = dict->_pairs.begin(); it != dict->_pairs.end(); ++it) {
                it->first = replaceh(dict, it->first);
                it->second = replaceh(dict, it->second);
            }
        }
    }

    void IReplaceVisitor::visit(NList *list) {
        if(!list->_elements.empty()) {
            for(auto it = list->_elements.begin(); it != list->_elements.end(); ++it) {
                *it = replaceh(list, *it);
            }
        }
    }

    void IReplaceVisitor::visit(NSubscription *sub) {
        assert(sub->_expression);
        sub->_value = replaceh(sub, sub->_value);
        sub->_expression = replaceh(sub, sub->_expression);
    }

    void IReplaceVisitor::visit(NReturn* ret) {
        if(ret->_expression)
            ret->_expression = replaceh(ret, ret->_expression);
    }

    void IReplaceVisitor::visit(NAssign* as) {
        assert(as->_target);
        assert(as->_value);

        as->_value = replaceh(as, as->_value);
        as->_target = replaceh(as, as->_target);
    }

    void IReplaceVisitor::visit(NCall* call) {
        // visit children
        for(auto& parg : call->_positionalArguments) {
            parg = replaceh(call, parg);
        }

        call->_func = replaceh(call, call->_func);
    }

    void IReplaceVisitor::visit(NAttribute *attr) {

        assert(attr->_value);
        assert(attr->_attribute);

        attr->_value = replaceh(attr, attr->_value);
        auto id = replaceh(attr, attr->_attribute);
        assert(id->type() == ASTNodeType::Identifier);
        attr->_attribute = (NIdentifier *) id;
    }

    void IReplaceVisitor::visit(NSlice *slicing) {
        assert(!slicing->_slices.empty());
        slicing->_value = replaceh(slicing, slicing->_value);
        if(!slicing->_slices.empty()) {
            for(auto it = slicing->_slices.begin(); it != slicing->_slices.end(); ++it) {
                *it = replaceh(slicing, *it);
            }
        }
    }

    void IReplaceVisitor::visit(NSliceItem *slicingItem) {
        if(slicingItem->_start)
            slicingItem->_start = replaceh(slicingItem, slicingItem->_start);
        if(slicingItem->_end)
            slicingItem->_end = replaceh(slicingItem, slicingItem->_end);
        if(slicingItem->_stride)
            slicingItem->_stride = replaceh(slicingItem, slicingItem->_stride);
    }

    void IReplaceVisitor::visit(NIfElse *ife) {
        ife->_expression = replaceh(ife, ife->_expression);
        ife->_then = replaceh(ife, ife->_then);
        if(ife->_else)
            ife->_else = replaceh(ife, ife->_else);
    }

    void IReplaceVisitor::visit(NRange* range) {
        // visit children
        for(auto& parg : range->_positionalArguments) {
            parg = replaceh(range, parg);
        }
    }

    void IReplaceVisitor::visit(NComprehension* comprehension) {
        comprehension->target = dynamic_cast<NIdentifier *>(replaceh(comprehension, comprehension->target));
        comprehension->iter = replaceh(comprehension, comprehension->iter);

        // visit children
        for(auto& icond : comprehension->if_conditions) {
            icond = replaceh(comprehension, icond);
        }
    }

    void IReplaceVisitor::visit(NListComprehension *listComprehension) {
        // visit children
        for(auto& gen : listComprehension->generators) {
            gen = dynamic_cast<NComprehension *>(replaceh(listComprehension, gen));
        }

        listComprehension->expression = replaceh(listComprehension, listComprehension->expression);
    }

    void IReplaceVisitor::visit(NAssert* assert_) {
        assert_->_expression = replaceh(assert_, assert_->_expression);
        assert_->_errorExpression = replaceh(assert_, assert_->_errorExpression);
    }

    void IReplaceVisitor::visit(NRaise *raise) {
        raise->_expression = replaceh(raise, raise->_expression);
        raise->_fromExpression = replaceh(raise, raise->_fromExpression);
    }

    void IReplaceVisitor::visit(NWhile *node) {
        node->expression = replaceh(node, node->expression);
        node->suite_body = replaceh(node, node->suite_body);
        node->suite_else = replaceh(node, node->suite_else);
    }

    void IReplaceVisitor::visit(NFor* node) {
        node->target = replaceh(node, node->target);
        node->expression = replaceh(node, node->expression);
        node->suite_body = replaceh(node, node->suite_body);
        node->suite_else = replaceh(node, node->suite_else);
    }
}