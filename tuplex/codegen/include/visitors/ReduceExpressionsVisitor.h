//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_REDUCEEXPRESSIONSVISITOR_H
#define TUPLEX_REDUCEEXPRESSIONSVISITOR_H

#include <visitors/IReplaceVisitor.h>
#include <ClosureEnvironment.h>

namespace tuplex {
    /*!
 * reduces expressions with literals only as far as possible...
 */
    class ReduceExpressionsVisitor : public IReplaceVisitor {
    protected:
        // for globals, simply replace their identifiers!
        const tuplex::ClosureEnvironment &_closure;
        std::set<std::string> _currentFunctionLocals; // all locally declared vars in this function
        std::vector<std::string> _currentFunctionParams; // all parameters of the current function

        // contains return values
        int _numReductions;

        ASTNode *replace(ASTNode *parent, ASTNode *node) override;

        int _numErrors;
        int _numWarnings;

        void error(const std::string &message);

        ASTNode *binop_replace(NBinaryOp *op);

        ASTNode *cmp_replace(NCompare *op);

    public:
        ReduceExpressionsVisitor(const tuplex::ClosureEnvironment &closure) : _closure(closure),
                                                                              _numReductions(0), _numErrors(0),
                                                                              _numWarnings(0) {}

        int getNumPerformedReductions() { return _numReductions; }
    };
}


#endif //TUPLEX_REDUCABLEEXPRESSIONSVISITOR_H