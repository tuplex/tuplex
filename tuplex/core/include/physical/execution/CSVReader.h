//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CSVREADER_H
#define TUPLEX_CSVREADER_H

#include <URI.h>
#include "physical/codegen/CodeDefs.h"
#include "FileInputReader.h"
#include <cassert>
#include <jit/RuntimeInterface.h>
#include <stdexcept>

namespace tuplex {
    class CSVReader : public FileInputReader {
    public:
        CSVReader() = delete;
        void read(const URI& inputFilePath) override;
        size_t inputRowCount() const override { return _numRowsRead;}
        virtual ~CSVReader()   {}

        CSVReader(void *userData,
                  codegen::cells_row_f rowFunctor,
                  bool makeParseErrorsInternal, // set to true for null-value optimization!
                  int64_t csvOperatorID,
                  codegen::exception_handler_f exceptionHandler,
                  const size_t numColumns,
                  const char delimiter,
                  const char quotechar = '"',
                  const std::vector<bool>& columnsToKeep=std::vector<bool>{}) : _userData(userData), _rowFunctor(rowFunctor), _makeParseErrorsInternal(makeParseErrorsInternal), _operatorID(csvOperatorID), _exceptionHandler(exceptionHandler), _numColumns(numColumns), _delimiter(delimiter),
                                                _quotechar(quotechar), _rangeStart(0), _rangeEnd(0), _columnsToKeep(columnsToKeep), _numRowsRead(0) {}

        CSVReader(void *userData,
                  codegen::cells_row_f rowFunctor,
                  const size_t numColumns,
                  const char delimiter,
                  const char quotechar = '"') : _userData(userData), _rowFunctor(rowFunctor), _operatorID(-1), _makeParseErrorsInternal(false), _exceptionHandler(nullptr), _numColumns(numColumns), _delimiter(delimiter),
                                                _quotechar(quotechar), _rangeStart(0), _rangeEnd(0), _numRowsRead(0) {}

        void setRange(size_t start, size_t end) {
            assert(start <= end); // 0,0 is allowed
            _rangeStart = start;
            _rangeEnd = end;
        }

        void setHeader(const std::vector<std::string> &header) { //@TODO: this is CSV specific! change it!
            _header = header;
            if(!_header.empty()) {
                if(_header.size() != _numColumns)
                    throw std::runtime_error("header size and num columns do not match up!");
            }
        }

        void setFunctor(codegen::cells_row_f functor) {
            _rowFunctor = functor;
        }

    private:
        void*   _userData;
        codegen::cells_row_f _rowFunctor;
        bool _makeParseErrorsInternal;
        int64_t _operatorID; // operator ID of the reader (passed down to exception handler)
        codegen::exception_handler_f _exceptionHandler;
        size_t _numColumns;
        char _delimiter;
        char _quotechar;
        size_t numColumns; /// what to expect in terms of column count, may be changed
        std::vector<std::string> _header;
        std::vector<bool> _columnsToKeep; /// used for projection pushdown, i.e. when serializing exceptions out
        size_t _rangeStart;
        size_t _rangeEnd;
        size_t _numRowsRead;
    };

    /*!
     * serialize to buffer contents of a csv parse for later resolution (i.e. when type mismatch etc. occured)
     * @param numCells
     * @param cells
     * @param sizes
     * @param buffer_size
     * @param allocator
     * @return buffer containing data
     */
    extern char* serializeParseException(int64_t numCells, char **cells, int64_t* sizes, size_t *buffer_size, decltype(malloc) allocator=runtime::rtmalloc);
}

#endif //TUPLEX_COMPILEDCSVREADER_H