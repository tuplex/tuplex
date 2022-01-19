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
    /*!
    * Holds mapping information from input partitions to their corresponding exception partitions.
    */
    struct ExceptionInfo {
        size_t numExceptions; //! number of exceptions associated with partition
        size_t exceptionIndex; //! starting index in partition vector to read exceptions from
        size_t exceptionOffset; //! starting offset within partition to read exceptions from

        ExceptionInfo() :
                numExceptions(0),
                exceptionIndex(0),
                exceptionOffset(0) {}

        ExceptionInfo(size_t numExceptions, size_t exceptionIndex, size_t exceptionOffset) :
                numExceptions(numExceptions),
                exceptionIndex(exceptionIndex),
                exceptionOffset(exceptionOffset) {}
    };
}

#endif //TUPLEX_EXCEPTIONINFO_H