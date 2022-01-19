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

    /*
     * Holds mapping information from input partitions to their corresponding exception partitions.
     */
    class ExceptionInfo {
    private:
        size_t _numExceptions;
        size_t _exceptionIndex;
        size_t _exceptionOffset;
    public:
        ExceptionInfo() :
                _numExceptions(0),
                _exceptionIndex(0),
                _exceptionOffset(0) {}

        ExceptionInfo(size_t numExceptions, size_t exceptionIndex, size_t exceptionOffset) :
                _numExceptions(numExceptions),
                _exceptionIndex(exceptionIndex),
                _exceptionOffset(exceptionOffset) {}

        /*!
         * number of exceptions associated with partition
         * @return size_t
         */
        size_t numExceptions() const { return _numExceptions; }

        /*!
         * starting index in partition vector to read exceptions from
         * @return size_t
         */
        size_t exceptionIndex() const { return _exceptionIndex; }

        /*!
         * starting offset within partition to read exceptions from
         * @return size_t
         */
        size_t exceptionOffset() const { return _exceptionOffset; }

        /*!
         * set number of exceptions
         * @param numExceptions
         */
        void setNumExceptions(size_t numExceptions) { _numExceptions = numExceptions; }

        /*!
         * set exception index
         * @param exceptionIndex
         */
        void setExceptionIndex(size_t exceptionIndex) { _exceptionIndex = exceptionIndex; }

        /*!
         * set exception offset
         * @param exceptionOffset
         */
        void setExceptionOffset(size_t exceptionOffset) { _exceptionOffset = exceptionOffset; }
    };
}

#endif //TUPLEX_EXCEPTIONINFO_H