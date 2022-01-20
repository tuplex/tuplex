//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/1/2022                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_EXCEPTIONINFO_H
#define TUPLEX_EXCEPTIONINFO_H

namespace tuplex {
    struct ExceptionInfo {
        size_t numExceptions;
        size_t exceptionIndex;
        size_t exceptionOffset;

        ExceptionInfo() :
            numExceptions(0),
            exceptionIndex(0),
            exceptionOffset(0) {}

        ExceptionInfo(size_t numExps,
                      size_t expIndex,
                      size_t expOffset) :
                      numExceptions(numExps),
                      exceptionIndex(expIndex),
                      exceptionOffset(expOffset) {}
    };
}

#endif //TUPLEX_EXCEPTIONINFO_H