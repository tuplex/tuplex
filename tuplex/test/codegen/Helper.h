//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_HELPER_H
#define TUPLEX_HELPER_H

#include <memory>
#include <string>
#include <ast/ASTNodes.h>

// test function
extern void testDefCode(const std::string& code,
                 const std::map<std::string, python::Type>& typeHints,
                 const python::Type& expectedReturnType);
#endif //TUPLEX_HELPER_H