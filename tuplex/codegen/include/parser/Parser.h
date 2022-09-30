//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PARSER_H
#define TUPLEX_PARSER_H

#include "ast/ASTNodes.h"

namespace tuplex {

    /*!
     * parses a Python3 string into AST
     * @param code python code as str
     * @return nullptr if parse was not successful
     */
    extern ASTNode* parseToAST(const std::string& code);


    extern void printParseTree(const std::string& code, std::ostream& os);
}

#endif //TUPLEX_PARSER_H