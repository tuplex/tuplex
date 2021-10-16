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
    // TODO: transfer ownership to replace, then return it back (i.e. pass and return unique_ptr from replace)
    template<class T>
    void swap(std::unique_ptr<T> &unique_p, T* p) {
        if(unique_p.get() != p) {
            unique_p = std::unique_ptr<T>(p);
        }
    }

    void IReplaceVisitor::visit(NModule *module) {

        // special case fpr module, because it can be also an empty file
        if(module->_suite) {
            module->_suite = replaceh(module, std::move(module->_suite));
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
                *it = replaceh(suite, std::move(*it));
            }
        }
    }

    void IReplaceVisitor::visit(NCompare *cmp) {
        // visit children
        cmp->_left = replaceh(cmp, std::move(cmp->_left));

        // other side
        if(!cmp->_comps.empty())
            for(auto it = cmp->_comps.begin(); it != cmp->_comps.end(); ++it) {
                *it = replaceh(cmp, std::move(*it));
            }
    }

    void IReplaceVisitor::visit(NFunction *function) {
        // @TODO: change such that types in AST can be only ASTNode! => optimizing the AST needs this flexibility!
        function->_name = replaceh<NIdentifier>(function, std::move(function->_name));
        function->_parameters = replaceh<NParameterList>(function, std::move(function->_parameters));
        if(function->_annotation)
            function->_annotation = replaceh(function, std::move(function->_annotation));
        assert(function->_suite);
        function->_suite = replaceh(function, std::move(function->_suite));

    }

//@TODO: fix optimization (removal of cmp) for parameterList!!!!
    void IReplaceVisitor::visit(NParameterList *paramList) {
        if(!paramList->_args.empty())
            for(auto it = paramList->_args.begin(); it != paramList->_args.end(); ++it) {
                *it = replaceh(paramList, std::move(*it));
            }
    }

    void IReplaceVisitor::visit(NStarExpression *se) {
        se->_target = replaceh(se, std::move(se->_target));
    }

    void IReplaceVisitor::visit(NLambda *lambda) {
        lambda->_arguments = replaceh<NParameterList>(lambda, std::move(lambda->_arguments));
        lambda->_expression = replaceh(lambda, std::move(lambda->_expression));
    }

    void IReplaceVisitor::visit(NUnaryOp *op) {
        op->_operand = replaceh(op, std::move(op->_operand));
    }

    void IReplaceVisitor::visit(NBinaryOp *op) {
        op->_left = replaceh(op, std::move(op->_left));
        op->_right = replaceh(op, std::move(op->_right));
    }

    void IReplaceVisitor::visit(NParameter *param) {
        param->_identifier = replaceh<NIdentifier>(param, std::move(param->_identifier));
        if(param->_annotation)
            param->_annotation = replaceh(param, std::move(param->_annotation));
        if(param->_default)
            param->_default = replaceh(param, std::move(param->_default));
    }

    void IReplaceVisitor::visit(NAwait *await) {
        assert(await->_target);
        await->_target = replaceh(await, std::move(await->_target));
    }

    void IReplaceVisitor::visit(NTuple *tuple) {
        if(!tuple->_elements.empty()) {
            for(auto it = tuple->_elements.begin(); it != tuple->_elements.end(); ++it) {
                *it = replaceh(tuple, std::move(*it));
            }
        }
    }

    void IReplaceVisitor::visit(NDictionary *dict) {
        if(!dict->_pairs.empty()) {
            for(auto it = dict->_pairs.begin(); it != dict->_pairs.end(); ++it) {
                it->first = replaceh(dict, std::move(it->first));
                it->second = replaceh(dict, std::move(it->second));
            }
        }
    }

    void IReplaceVisitor::visit(NList *list) {
        if(!list->_elements.empty()) {
            for(auto it = list->_elements.begin(); it != list->_elements.end(); ++it) {
                *it = replaceh(list, std::move(*it));
            }
        }
    }

    void IReplaceVisitor::visit(NSubscription *sub) {
        assert(sub->_expression);
        sub->_value = replaceh(sub, std::move(sub->_value));
        sub->_expression = replaceh(sub, std::move(sub->_expression));
    }

    void IReplaceVisitor::visit(NReturn* ret) {
        if(ret->_expression)
            ret->_expression = replaceh(ret, std::move(ret->_expression));
    }

    void IReplaceVisitor::visit(NAssign* as) {
        assert(as->_target);
        assert(as->_value);

        as->_value = replaceh(as, std::move(as->_value));
        as->_target = replaceh(as, std::move(as->_target));
    }

    void IReplaceVisitor::visit(NCall* call) {
        // visit children
        for(auto& parg : call->_positionalArguments) {
            parg = replaceh(call, std::move(parg));
        }

        call->_func = replaceh(call, std::move(call->_func));
    }

    void IReplaceVisitor::visit(NAttribute *attr) {

        assert(attr->_value);
        assert(attr->_attribute);

        attr->_value = replaceh(attr, std::move(attr->_value));
        attr->_attribute = replaceh<NIdentifier>(attr, std::move(attr->_attribute));
    }

    void IReplaceVisitor::visit(NSlice *slicing) {
        assert(!slicing->_slices.empty());
        slicing->_value = replaceh(slicing, std::move(slicing->_value));
        if(!slicing->_slices.empty()) {
            for(auto it = slicing->_slices.begin(); it != slicing->_slices.end(); ++it) {
                *it = replaceh(slicing, std::move(*it));
            }
        }
    }

    void IReplaceVisitor::visit(NSliceItem *slicingItem) {
        if(slicingItem->_start)
            slicingItem->_start = replaceh(slicingItem, std::move(slicingItem->_start));
        if(slicingItem->_end)
            slicingItem->_end = replaceh(slicingItem, std::move(slicingItem->_end));
        if(slicingItem->_stride)
            slicingItem->_stride = replaceh(slicingItem, std::move(slicingItem->_stride));
    }

    void IReplaceVisitor::visit(NIfElse *ife) {
        ife->_expression = replaceh(ife, std::move(ife->_expression));
        ife->_then = replaceh(ife, std::move(ife->_then));
        if(ife->_else)
            ife->_else = replaceh(ife, std::move(ife->_else));
    }

    void IReplaceVisitor::visit(NRange* range) {
        // visit children
        for(auto& parg : range->_positionalArguments) {
            parg = replaceh(range, std::move(parg));
        }
    }

    void IReplaceVisitor::visit(NComprehension* comprehension) {
        comprehension->target = replaceh<NIdentifier>(comprehension, std::move(comprehension->target));
        comprehension->iter = replaceh(comprehension, std::move(comprehension->iter));

        // visit children
        for(auto& icond : comprehension->if_conditions) {
            icond = replaceh(comprehension, std::move(icond));
        }
    }

    void IReplaceVisitor::visit(NListComprehension *listComprehension) {
        // visit children
        for(auto& gen : listComprehension->generators) {
            gen = replaceh<NComprehension>(listComprehension, std::move(gen));
        }

        listComprehension->expression = replaceh(listComprehension, std::move(listComprehension->expression));
    }

    void IReplaceVisitor::visit(NAssert* assert_) {
        assert_->_expression = replaceh(assert_, std::move(assert_->_expression));
        assert_->_errorExpression = replaceh(assert_, std::move(assert_->_errorExpression));
    }

    void IReplaceVisitor::visit(NRaise *raise) {
        raise->_expression = replaceh(raise, std::move(raise->_expression));
        raise->_fromExpression = replaceh(raise, std::move(raise->_fromExpression));
    }

    void IReplaceVisitor::visit(NWhile *node) {
        node->expression = replaceh(node, std::move(node->expression));
        node->suite_body = replaceh(node, std::move(node->suite_body));
        node->suite_else = replaceh(node, std::move(node->suite_else));
    }

    void IReplaceVisitor::visit(NFor* node) {
        node->target = replaceh(node, std::move(node->target));
        node->expression = replaceh(node, std::move(node->expression));
        node->suite_body = replaceh(node, std::move(node->suite_body));
        node->suite_else = replaceh(node, std::move(node->suite_else));
    }
}
