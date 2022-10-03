//
// Created by Leonhard Spiegelberg on 3/18/22.
//

#ifndef TUPLEX_JSONREADER_H
#define TUPLEX_JSONREADER_H

#include "FileInputReader.h"
#include <CodegenHelper.h>
#include "CodeDefs.h"

namespace tuplex {
    // @March: implement here
    // cf. TextReader, OrcReader, CSVReader(that's the most complex one)
    // cf. OrcReaderTest.cc etc. on how to test this end-to-end

    // later: column pushdown etc., will have to think about good interface for this
    class JsonReader : public FileInputReader {
    public:
        JsonReader() = delete;
        JsonReader(void *userData, codegen::read_block_f rowFunctor);

        // use here VirtualFileSystem when reading files...
        //@March: for testing, just don't call the rowFunctor yet, but rather construct a Row object and then print row.desc();
        void read(const URI& inputFilePath) override;

        size_t inputRowCount() const override;

        // read all valid JSON lines from >= start to the line that just ends after end.
        void setRange(size_t start, size_t end);
        void setFunctor(codegen::read_block_f functor);
    };
}
#endif //TUPLEX_JSONREADER_H
