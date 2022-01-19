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
    class ExceptionInfo {
    private:
        size_t _numExceptions;
        size_t _exceptionIndex;
        size_t _exceptionOffset;
        size_t _rowNumOffset;
    public:
        ExceptionInfo() :
                _numExceptions(0),
                _exceptionIndex(0),
                _exceptionOffset(0),
                _rowNumOffset(0) {}

        ExceptionInfo(size_t numExceptions, size_t exceptionIndex, size_t exceptionOffset, size_t rowNumOffset) :
                      _numExceptions(numExceptions),
                      _exceptionIndex(exceptionIndex),
                      _exceptionOffset(exceptionOffset),
                      _rowNumOffset(rowNumOffset) {}

        size_t numExceptions() const { return _numExceptions; }
        size_t exceptionIndex() const { return _exceptionIndex; }
        size_t exceptionOffset() const { return _exceptionOffset; }
        size_t rowNumOffset() const { return _rowNumOffset; }

    };
}

#endif //TUPLEX_EXCEPTIONINFO_H
