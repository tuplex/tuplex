//
// Created by Leonhard Spiegelberg on 3/18/22.
//

#ifndef TUPLEX_JSONREADER_H
#define TUPLEX_JSONREADER_H

#include "FileInputReader.h"
#include <physical/codegen/CodegenHelper.h>
#include <physical/codegen/CodeDefs.h>

namespace tuplex {
    // @March: implement here
    // cf. TextReader, OrcReader, CSVReader(that's the most complex one)
    // cf. OrcReaderTest.cc etc. on how to test this end-to-end

    // later: column pushdown etc., will have to think about good interface for this
    class JsonReader : public FileInputReader {
    public:
        JsonReader() = delete;
        JsonReader(void *userData, codegen::read_block_f rowFunctor, size_t bufferSize);

        // use here VirtualFileSystem when reading files...
        //@March: for testing, just don't call the rowFunctor yet, but rather construct a Row object and then print row.desc();
        void read(const URI& inputFilePath) override;

        size_t inputRowCount() const override;

        // read all valid JSON lines from >= start to the line that just ends after end.
        void setRange(size_t start, size_t end);
        void setFunctor(codegen::read_block_f functor);

    private:
        codegen::read_block_f _functor;
        void* _userData;
        size_t _rangeStart;
        size_t _rangeEnd;

        // internal read buffer
        size_t _bufferSize;
        size_t _inBufferLength;
        uint8_t* _inputBuffer;

        // row counters, needed for exception handling.
        int64_t  _num_normal_rows, _num_bad_rows;

        void moveInputBuffer(size_t bytesConsumed, size_t simd_security_bytes);

        size_t consume(bool eof);
    };
}
#endif //TUPLEX_JSONREADER_H
