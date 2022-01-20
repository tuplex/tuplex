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
     * Struct to hold information that maps input partitions to input exceptions that occur within them.
     *
     * Explanation:
     * Each input partition is passed the same vector of all input exceptions that occured during data parallelization
     * or caching. Thus, each input partition is must know how many input exceptions occur in its partition, the index
     * of the input exception partition where its first exception occurs, and the offset into that partition where the
     * first exception occurs. These values are held in this struct and each input partition is mapped to an ExceptionInfo.
     */
    struct ExceptionInfo {
        size_t numExceptions; //! number of exceptions that occur within a single input partition
        size_t exceptionIndex; //! index into a vector of input exception partitions that holds the first input exception
        size_t exceptionOffset; //! offset into the first input exception partition where the first exception occurs.

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