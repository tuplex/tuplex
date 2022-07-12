//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TEXTREADER_H
#define TUPLEX_TEXTREADER_H

#include <URI.h>
#include "physical/codegen/CodeDefs.h"
#include "physical/execution/FileInputReader.h"
#include <cassert>

namespace tuplex {
    class TextReader : public FileInputReader {
    public:
        TextReader() = delete;
        void read(const URI& inputFilePath) override;

        size_t inputRowCount() const override { return _numRowsRead; }

        TextReader(void *userData,
                   codegen::cells_row_f rowFunctor) : _userData(userData), _rowFunctor(rowFunctor), _rangeStart(0),
                                                      _rangeEnd(0), _numRowsRead(0) {}

        void setRange(size_t start, size_t end) {
            assert(start <= end); // 0,0 is allowed
            _rangeStart = start;
            _rangeEnd = end;
        }

        void setFunctor(codegen::cells_row_f functor) {
            _rowFunctor = functor;
        }

    private:
        void*   _userData;
        codegen::cells_row_f _rowFunctor;
        size_t _rangeStart;
        size_t _rangeEnd;
        size_t _numRowsRead;
    };
}

#endif //TUPLEX_TEXTREADER_H