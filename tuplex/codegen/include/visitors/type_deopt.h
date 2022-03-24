//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 3/24/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TYPEDEOPT_H
#define TUPLEX_TYPEDEOPT_H

#include <TypeSystem.h>
#include <parser/TokenType.h>

namespace tuplex {

    // for optimization reasons, tuplex introduces "optimized" types, i.e. types restricted to certain
    // optimizations. Sometimes though they need to get deoptimized.
    // the following functions do that.
    // I.e., either a result can be obtained by combining the optimized types -> return true
    // or not, return false. In this case, the types are "deoptimized"

    bool deopt_binary_op(python::Type& optimized_result, python::Type& lhs_type, python::Type& rhs_type, const TokenType& tt);
    bool deopt_unary_op(python::Type& optimized_result, python::Type& type, const TokenType&& tt);
    bool deopt_cmp_op(python::Type& optimized_result, std::vector<python::Type>& types);

//    bool deopt_tuple_op(std::vector<python::Type>& types);
//    bool deopt_list_op(std::vector<python::Type>& types)''
}

#endif