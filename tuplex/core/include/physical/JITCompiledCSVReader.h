//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_JITCOMPILEDCSVREADER_H
#define TUPLEX_JITCOMPILEDCSVREADER_H

#include "FileInputReader.h"
#include "CodeDefs.h"
#include <codegen/CodegenHelper.h>
#include <Base.h>
#include <cstdint>

namespace tuplex {

    // @TODO: buffer size is 32kb max. for a row. this should be made dynamic based on sample/changed or buffer should be resized.

   // a completely compiled JIT reader
    class JITCompiledCSVReader : public FileInputReader {
    public:
        JITCompiledCSVReader() = delete;
        JITCompiledCSVReader(void* userData,
                          codegen::read_block_f functor,
                          const size_t numColumns,
                          const char delimiter,
                          const char quotechar='"',
                          size_t bufferSize=1024 * 128) : _userData(userData), _functor(functor), _numColumns(numColumns), _bufferSize(bufferSize), _delimiter(delimiter), _quotechar(quotechar), _rangeStart(0), _rangeEnd(0), _inputBuffer(nullptr), _num_normal_rows(0), _num_bad_rows(0)  {}
        ~JITCompiledCSVReader() override {
            if(_inputBuffer)
                delete [] _inputBuffer;
            _inputBuffer = nullptr;
        }
        void read(const URI& inputFilePath) override;
        size_t inputRowCount() const override { return _num_normal_rows + _num_bad_rows; }

        void setRange(size_t start, size_t end) {
            assert(start < end || (start == 0 && end == 0));
            _rangeStart = start;
            _rangeEnd = end;
        }

        void setHeader(const std::vector<std::string>& header) { //@TODO: this is CSV specific! change it!
            _header = header;
            if(!_header.empty()) {
                if(_header.size() != _numColumns)
                    throw std::runtime_error("header size and num columns do not match up!");
            }
        }

        void setFunctor(codegen::read_block_f functor) {
            _functor = functor;
        }

    private:
        // the function to be called => includes the CSV parser + eventual, other elements.
        void* _userData; // used to pass task
        codegen::read_block_f _functor;

        size_t _numColumns;
        size_t _bufferSize;
        size_t _inBufferLength;
        uint8_t* _inputBuffer;
        char _delimiter;
        char _quotechar;

        int64_t  _num_normal_rows, _num_bad_rows; // rows, needed for exception handling.

        // optional: set input range
        size_t _rangeStart;
        size_t _rangeEnd;
        std::vector<std::string> _header;


        /*!
         * read file using standard IO provided by VFS (i.e. when using S3)
         * @param uri
         */
        void readBuffered(const URI& uri);

        /*!
         * read file by memory mapping it (faster)
         * @param uri
         */
        void readMapped(const URI& uri);

        /*!
         * parses header and compares to stored one throwing exception if it doesn;t match
         * @param buffer
         * @param buffer_size
         * @return bytes read for header
         */
        size_t parseAndCompareHeader(uint8_t* buffer, size_t buffer_size);

        // check whether to use range (if so they must be different)
        inline bool useRange() { return _rangeStart < _rangeEnd; }
        void moveInputBuffer(size_t bytesConsumed);

        // consume up to bufferLength bytes from input Buffer
        // @param eof check whether this is the last buffer available for a file
        size_t consume(bool eof);
    };
}

#endif //TUPLEX_JITCOMPILEDCSVREADER_H