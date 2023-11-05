//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <visitors/ColumnRewriteVisitor.h>
#include <Utils.h>
#include <ExceptionCodes.h>

namespace tuplex {

    std::string processString(const std::string &val) {
        assert(val.length() >= 2);
        // only simple strings supported, i.e. those that start with ' ' or " "
        assert((val[0] == '\'' && val[val.length() - 1] == '\'')
               || (val[0] == '\"' && val[val.length() - 1] == '\"'));

        return val.substr(1, val.length() - 2);
    }


    ASTNode *ColumnRewriteVisitor::rewriteNameToIndex(tuplex::NSubscription *sub) {
        if(sub->_value->type() == ASTNodeType::Identifier &&
           sub->_expression->type() == ASTNodeType::String) {
            auto id = (NIdentifier*)sub->_value.get();
            auto str = (NString*)sub->_expression.get();

            // rewrite if matches param
            if(id->_name == _parameter) {

                // special case: if id is of row type, do not perform any action
                if(PARAM_USE_ROW_TYPE)
                    return sub;

                // exchange expression with number (index in column names array)
                auto colName = str->value();

                int idx = indexInVector(colName, _columnNames);
                if(idx < 0) {
                    // column not found -> this could happen e.g. for JSON files/CSV files where this hasn't been detected.
                    // emit warning and annotate node to become a deoptimization (python) node!
                    auto& ann = sub->annotation();
                    ann.deoptException = ExceptionCode::GENERALCASEVIOLATION;
                    warning("could not find column '" + colName + "' in dataset. Emitting deoptimization trigger.");
                    return sub;
                    //error("could not find column '" + colName + "' in dataset.");
                }

                // make true, access found
                _dictAccessFound = true;

                assert(idx < _columnNames.size());

                // only rewrite in non-dry mode
                if(_rewrite) {
                    // special case: If there is a single column, do not use param[idx],
                    //               but just return param due to unpacking
                    if(_columnNames.size() == 1)
                        return id->clone();
                    else {
                        auto new_sub = new NSubscription(id, new NNumber(static_cast<int64_t>(idx)));

                        // store in annotation original column name
                        new_sub->annotation().originalColumnName = colName; // <-- actual value
                        return new_sub;
                    }
                }
            }
        }

        // return as is, no rewrite possible.
        return sub;
    }

    ASTNode *ColumnRewriteVisitor::rewriteIndexToName(tuplex::NSubscription *sub) {
        if(sub->_value->type() == ASTNodeType::Identifier &&
           sub->_expression->type() == ASTNodeType::Number) {
            // check if boolean/integer access
            auto num = (NNumber*)sub->_expression.get();
            auto id = (NIdentifier*)sub->_value.get();
            auto index = num->getI64();

            // does id->_name match parameter name? if so, rewrite! (what about name re-assigns? make them work later...)
            if(id->_name != _parameter) // <-- this is not 100% correct. I.e., should have proper set of names that correspond to parameter, handle case where name gets reassigned etc. Special attention to if case.. @TODO: make this more correct.
                return sub;

            // special case: if id is of row type, do not perform any action
            if(PARAM_USE_ROW_TYPE)
                return sub;

            // does an annotation exist?
            if(sub->hasAnnotation() && !sub->annotation().originalColumnName.empty()) {
                return new NSubscription(id, new NString(escape_to_python_str(sub->annotation().originalColumnName)));
            }

            auto alt_index = projectedIdxToOriginalIdx(index);
            auto alt_name = _columnNames[alt_index];

            index = alt_index;

            // check whether index is in column range, then replace!
            if(index < 0)
                index += _columnNames.size(); // negative index correction
            if(index >= 0 && index < _columnNames.size()) {
                _dictAccessFound = true;

                if(_rewrite) {
                    auto name = _columnNames[index];

                    // replace with NString node
                    return new NSubscription(id, new NString(escape_to_python_str(name)));
                }
            }
        }

        // other option: constant valued access!
        // @TODO.

        return sub;
    }


    ASTNode* ColumnRewriteVisitor::replace(ASTNode *parent, ASTNode* node) {
        if(!node)
            return nullptr;

        switch(node->type()) {
            case ASTNodeType::Subscription: {

                auto sub = (NSubscription*)node;

                // @TODO: there might be issues when variable is redefined!
                // i.e.
                // def f(x):
                //      y = x['columnA']
                //      x = 12
                //      return y
                // this could be problematic with if...else... blocks because of the scoping,
                // so future version should take account of this...
                // check if value is an identifier matching the parameter


                // @TODO: what about dynamic lookups?
                // i.e. check AFTER typing to do this
                // e.g.
                // def f(x):
                //      y = 'columnA'
                //      return x[y]
                if(_mode == ColumnRewriteMode::NAME_TO_INDEX)
                    return rewriteNameToIndex(sub);

                if(_mode == ColumnRewriteMode::INDEX_TO_NAME)
                    return rewriteIndexToName(sub);

                return node;
            }
            default:
                return node;
        }
    }
}