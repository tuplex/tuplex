//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ApatheticVisitor.h>

namespace tuplex {
    void ApatheticVisitor::visit(NModule *module) {
        // special case, module can be empty
        if(module->_suite) {
            _lastParent = module;
            module->_suite->accept(*this);
        }

    }

    void ApatheticVisitor::visit(NSuite *suite) {
        for (auto & stmt : suite->_statements) {
            // check whether statement can be optimized. Can be optimized iff result of optimizeNext is a different
            // memory address
            _lastParent = suite;
            stmt->accept(*this);
        }
    }

    void ApatheticVisitor::visit(NParameter *param) {
        _lastParent = param;
        param->_identifier->accept(*this);
        _lastParent = param;
        if(param->_annotation)param->_annotation->accept(*this);
        _lastParent = param;
        if(param->_default)param->_default->accept(*this);
    }

    void ApatheticVisitor::visit(NCompare *cmp) {
        // visit children
        _lastParent = cmp;
        cmp->_left->accept(*this);

        // other side
        if(!cmp->_comps.empty())
            for(auto it = cmp->_comps.begin(); it != cmp->_comps.end(); ++it) {
                _lastParent = cmp;
                (*it)->accept(*this);
            }
    }

    void ApatheticVisitor::visit(NFunction *function) {
        _lastParent = function;
        function->_name->accept(*this);
        _lastParent = function;
        if(function->_parameters)
            function->_parameters->accept(*this);
        _lastParent = function;
        if(function->_annotation)
            function->_annotation->accept(*this);
        _lastParent = function;
        assert(function->_suite);
        if(function->_suite)
            function->_suite->accept(*this);
    }

    void ApatheticVisitor::visit(NLambda *lambda) {
        _lastParent = lambda;
        if(lambda->_arguments)lambda->_arguments->accept(*this);
        _lastParent = lambda;
        lambda->_expression->accept(*this);
    }

    void ApatheticVisitor::visit(NUnaryOp *op) {
        _lastParent = op;
        op->_operand->accept(*this);
    }

    void ApatheticVisitor::visit(NBinaryOp *op) {
        _lastParent = op;
        op->_left->accept(*this);
        _lastParent = op;
        op->_right->accept(*this);
    }

    void ApatheticVisitor::visit(NParameterList* paramList) {
        for(auto& arg: paramList->_args) {
            _lastParent = paramList;
            arg->accept(*this);
        }
    }

    void ApatheticVisitor::visit(NStarExpression *se) {
        _lastParent = se;
        se->_target->accept(*this);
    }

    void ApatheticVisitor::visit(NAwait *await) {
        _lastParent = await;
        await->accept(*this);
    }

    void ApatheticVisitor::visit(NIfElse *ifelse) {
        _lastParent = ifelse;

        // first condition, then "then", then "else
        assert(ifelse->_expression);
        ifelse->_expression->accept(*this);
        assert(ifelse->_then);
        _lastParent = ifelse;
        ifelse->_then->accept(*this);

        // optional else
        if(ifelse->_else) {
            _lastParent = ifelse;
            ifelse->_else->accept(*this);
        }
    }


    void ApatheticVisitor::visit(NTuple* tuple) {
        if(!tuple->_elements.empty()) {
            for(auto &it : tuple->_elements) {
                _lastParent = tuple;
                it->accept(*this);
            }
        }
    }

    void ApatheticVisitor::visit(NDictionary* dict) {
        if(!dict->_pairs.empty()) {
            // visit the elements in order from left to right (as written), because ASTBuilderVisitor builds _pairs in this order
            for(auto& it : dict->_pairs) {
                _lastParent = dict;
                it.first->accept(*this);
                _lastParent = dict;
                it.second->accept(*this);
            }
        }
    }

    void ApatheticVisitor::visit(NList* list) {
        if(!list->_elements.empty()) {
            for(auto& it : list->_elements) {
                _lastParent = list;
                it->accept(*this);
            }
        }
    }

    void ApatheticVisitor::visit(NSubscription *sub) {
        assert(sub->_expression);
        _lastParent = sub;
        sub->_value->accept(*this);
        _lastParent = sub;
        sub->_expression->accept(*this);
    }


    void ApatheticVisitor::visit(NReturn* ret) {
        _lastParent = ret;
        if(ret->_expression)
            ret->_expression->accept(*this);
    }

    void ApatheticVisitor::visit(NAssign* as) {
        _lastParent = as;
        assert(as->_target);
        assert(as->_value);

        // note that there is a strict order how these need to be visited. I.e. values first then right to left for the targets
        as->_value->accept(*this);
        _lastParent = as;
        as->_target->accept(*this);
    }

    void ApatheticVisitor::visit(NCall* call) {
        _lastParent = call;
        assert(call->_func);

        // visit children if they exist first
        for(auto& parg : call->_positionalArguments) {
            assert(parg);
            parg->accept(*this);
            _lastParent = call;
        }

        // visit function then
        // important for typing...
        call->_func->accept(*this);
    }

    void ApatheticVisitor::visit(NAttribute* attr) {
        _lastParent = attr;
        assert(attr->_value);
        assert(attr->_attribute);

        // first visit value, then the attribute
        attr->_value->accept(*this);
        _lastParent = attr;
        attr->_attribute->accept(*this);
    }


    void ApatheticVisitor::visit(NSlice *slicing) {
        assert(slicing->_value);
        slicing->_value->accept(*this);
        if(!slicing->_slices.empty()) {
            for(auto& it : slicing->_slices)
                it->accept(*this);
        }
    }

    void ApatheticVisitor::visit(NSliceItem *slicingItem) {
        if (slicingItem->_start) {
            slicingItem->_start->accept(*this);
        }
        if (slicingItem->_end) {
            slicingItem->_end->accept(*this);
        }
        if (slicingItem->_stride) {
            slicingItem->_stride->accept(*this);
        }
    }

    void ApatheticVisitor::visit(NRange* range) {
        _lastParent = range;

        // visit children if they exist first
        for(auto& parg : range->_positionalArguments) {
            assert(parg);
            parg->accept(*this);
            _lastParent = range;
        }
    }

    void ApatheticVisitor::visit(NComprehension* comprehension) {
        _lastParent = comprehension;
        assert(comprehension->target);
        assert(comprehension->iter);

        comprehension->iter->accept(*this); // iter needs to be visited first, for typing
        _lastParent = comprehension;
        comprehension->target->accept(*this);
        _lastParent = comprehension;
        // visit if statements if they exist
        for(auto& icond : comprehension->if_conditions) {
            assert(icond);
            icond->accept(*this);
            _lastParent = comprehension;
        }
    }

    void ApatheticVisitor::visit(NListComprehension* listComprehension) {
        _lastParent = listComprehension;
        assert(listComprehension->expression);

        // visit generators if they exist first
        for(auto& gen : listComprehension->generators) {
            assert(gen);
            gen->accept(*this);
            _lastParent = listComprehension;
        }

        // visit expression
        listComprehension->expression->accept(*this);
    }

    void ApatheticVisitor::visit(NAssert* assert_) {
        _lastParent = assert_;
        if(assert_->_expression)
            assert_->_expression->accept(*this);
        if(assert_->_errorExpression)
            assert_->_errorExpression->accept(*this);
    }

    void ApatheticVisitor::visit(NRaise* raise) {
        _lastParent = raise;
        if(raise->_expression)
            raise->_expression->accept(*this);
        if(raise->_fromExpression)
            raise->_fromExpression->accept(*this);
    }

    void ApatheticVisitor::visit(NWhile* node) {
        _lastParent = node;
        if(node->expression)
            node->expression->accept(*this);
        if(node->suite_body)
            node->suite_body->accept(*this);
        if(node->suite_else)
            node->suite_else->accept(*this);
    }

    void ApatheticVisitor::visit(NFor* node) {
        _lastParent = node;
        if(node->target)
            node->target->accept(*this);
        if(node->expression)
            node->expression->accept(*this);
        if(node->suite_body)
            node->suite_body->accept(*this);
        if(node->suite_else)
            node->suite_else->accept(*this);
    }

    void ApatheticVisitor::visit(NBreak* node) {
        _lastParent = node;
    }

    void ApatheticVisitor::visit(NContinue* node) {
        _lastParent = node;
    }
}