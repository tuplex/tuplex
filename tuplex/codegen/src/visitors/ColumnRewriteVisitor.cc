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

namespace tuplex {

    std::string processString(const std::string &val) {
        assert(val.length() >= 2);
        // only simple strings supported, i.e. those that start with ' ' or " "
        assert((val[0] == '\'' && val[val.length() - 1] == '\'')
               || (val[0] == '\"' && val[val.length() - 1] == '\"'));

        return val.substr(1, val.length() - 2);
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

                if(sub->_value->type() == ASTNodeType::Identifier &&
                   sub->_expression->type() == ASTNodeType::String) {
                    auto id = (NIdentifier*)sub->_value.get();
                    auto str = (NString*)sub->_expression.get();

                    // rewrite if matches param
                    if(id->_name == _parameter) {
                        // exchange expression with number (index in column names array)

                        auto colName = str->value();

                        int idx = indexInVector(colName, _columnNames);
                        if(idx < 0)
                            error("could not find column '" + colName + "' in dataset.");

                        // make true, access found
                        _dictAccessFound = true;

                        // only rewrite in non-dry mode
                        if(_rewrite) {
                            // special case: If there is a single column, do not use param[idx],
                            //               but just return param due to unpacking
                            if(_columnNames.size() == 1)
                                return id->clone();
                            else
                                return new NSubscription(id, new NNumber(static_cast<int64_t>(idx)));
                        }
                    }
                }

                return node;
            }
            default:
                return node;
        }
    }
}