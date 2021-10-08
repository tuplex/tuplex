//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ASTHELPERS_H
#define TUPLEX_ASTHELPERS_H


// this file contains various helper functions to deal with AST trees
#include <ASTNodes.h>
#include <Field.h>

namespace tuplex {
    /*!
 * checks whether AST node is a literal
 * @param root
 * @return
 */
    extern bool isLiteral(ASTNode *root);

/*!
 * helper function to determine whether this node is a static literal valie. If the recursive option is specified, checks whether the expression is static
 * @param root
 * @param recursive
 * @return
 */
    extern bool isStaticValue(ASTNode *root, bool recursive=false);


/*!
 * returns all variable names to which values are assigned to within this function.
 * DOES NOT RECURSE INTO functions defined within
 * @param root a function node. Either NLambda or NFunction
 * @return set of names
 */
    extern std::set<std::string> getFunctionVariables(ASTNode* root);

/*!
 * return all function parameters names in order
 * @param root
 * @return vector
 */
    extern std::vector<std::string> getFunctionParameters(ASTNode* root);

/*!
 * convert field to AST Node
 * @param f
 * @return
 */
    extern ASTNode* fieldToAST(const tuplex::Field& f);

/*!
 * check whether the nodes below that node all result in an exit path, i.e. return.
 * needed for escape analysis of assignment operations.
 * @param root
 * @return
 */
    extern bool isExitPath(ASTNode* root);

    /*!
     * check whether the [root] represents a logical operation
     * @param root
     * @return
     */
    extern bool isLogicalOp(ASTNode* root);

/*!
 * get the vector of identifiers when target in for loop is a tuple or a list
 * @param target (NTuple or NList)
 * @return vector of identifiers
 */
    extern std::vector<ASTNode *> getForLoopMultiTarget(ASTNode* target);
}

#endif //TUPLEX_ASTHELPERS_H