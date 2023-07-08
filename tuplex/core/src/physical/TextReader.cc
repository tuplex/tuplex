//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/TextReader.h>
#include <VirtualFileSystem.h>
#include <Logger.h>
#include <RuntimeInterface.h>
#include <ExceptionCodes.h>

// use simd intrinsics or ARM Neon translation layer
#if (defined __x86_64__)
#include <nmmintrin.h>
#elif (defined __arm64__)
#include <third_party/sse2neon/sse2neon.h>
#else
#error "unsupported platform for intrinsics"
#endif

namespace tuplex {

    class BufferedFileReader {
        std::unique_ptr<VirtualFile> _file;

        size_t _readPos;
        size_t _numBytesInBuf;
        size_t _numBytesRead;

        __m128i _newline_chars;

        static const size_t maxBufSize = 131072 - 16 - 1; // assumption that this is large enough
        char _buf[131072];

        inline void readBytes(size_t writePos) {
            if(!_file->eof()) {
                _file->read(&_buf[writePos], maxBufSize - writePos, &_numBytesInBuf);
                _readPos = writePos;
                _numBytesInBuf += writePos;
            }
        }

        __attribute__((always_inline)) int findNextNewline() {
            // returns the offset of the first '\r' or '\n' or '\0' in the next 16 characters (16 => not found)
            auto n = _mm_cmpistri(_newline_chars, _mm_loadu_si128((__m128i *)&_buf[_readPos]), _SIDD_UBYTE_OPS|_SIDD_CMP_EQUAL_ANY|_SIDD_POSITIVE_POLARITY);
            return n;
        }

    public:
        BufferedFileReader() = delete;

        explicit BufferedFileReader(const URI &inputFilePath, size_t rangeStart) : _file(
                VirtualFileSystem::open_file(inputFilePath, VirtualFileMode::VFS_READ)), _readPos(0), _numBytesInBuf(0),
                                                                _numBytesRead(0) {
            // set up new line characters (basically first bytes, rest 0)
            // __v16qi vq = {'\n', '\r', '\0', '\0'};
            // _newline_chars = (__m128i) vq;

            // following is portable way when v16qi is not known.
            int32_t i = 0;
            char bytes[] = {'\n', '\r', '\0', '\0'};
            memcpy(&i, bytes, 4); // <-- i should be 3338
            _newline_chars = _mm_setr_epi32(i, 0x0, 0x0, 0x0);

            // zero out the end of the array
            memset(&_buf[maxBufSize], 0, 16 + 1);

            // set up beginning of file read
            _file->seek(rangeStart);
            readBytes(0);
        }

        bool eof() { return _readPos == _numBytesInBuf; }
        int numBytesRead() { return _numBytesRead; }

        char readChar() { return _buf[_readPos]; }

        size_t readAndAdvance(size_t n, size_t &start) {
            if(n == 0) return 0;
            size_t ret = 0;
            if(_numBytesInBuf - _readPos > n) {
                _readPos += n;
                ret = n;
            }
            else { // need to wrap the line around, pin start -> end of line
                ret = _numBytesInBuf - _readPos;
                size_t curlen = _numBytesInBuf - start;
                _numBytesInBuf = curlen;
                _readPos = curlen;
                std::memmove(_buf, &_buf[start], curlen); // move the current line to beginning of buffer
                readBytes(curlen);
                start = 0;
            }
            _numBytesRead += ret;
            return ret;
        }

        // store the start index (in _buf) of the line, and return the length
        size_t readLine(size_t &start) {
            start = _readPos;
            size_t len = 0;
            while(true) {
                int n;
                while (!eof() && ((n = findNextNewline()) == 16)) {
                    len += readAndAdvance(n, start);
                }
                if(eof()) return len; // no terminating newline
                len += readAndAdvance(n, start);

                // advance past the cr/lf
                auto foundChar = readChar();
                readAndAdvance(1, start);

                // lf line ending, return
                if(foundChar == '\n') return len;

                // check whether we are at a crlf ending
                if(eof()) { // file ended with carriage return, not crlf
                    len += 1;
                    return len;
                }
                if(readChar() == '\n') { // yes!
                    readAndAdvance(1, start);
                    return len;
                }
                len += 1; // no, just a carriage return in the string
            }
        }

        const char * const getBuffer() { return _buf; }
    };

    void TextReader::read(const URI &inputFilePath) {
        using namespace std;

        _numRowsRead = 0;

        // check that functor is valid
        if (!_rowFunctor)
            throw std::runtime_error("functor not initialized");

        // open up file and read in pieces
        BufferedFileReader bufFile(inputFilePath, _rangeStart);

        // find starting position
        if(_rangeStart != 0) {
            // special case: _rangeStart = 0 ==> beginning of file
            // otherwise, need to find the next newline
            size_t tmp;
            bufFile.readLine(tmp);
        }
        // at start position, read one row at a time until we pass the rangeEnd
        char **cells = new char*[1];
        int64_t *cell_sizes = new int64_t[1];
        cells[0] = nullptr;
        int rowNumber = 0;
        bool one_chunk = (_rangeStart == 0 && _rangeEnd == 0);
        while(!bufFile.eof() && (one_chunk || (bufFile.numBytesRead() < _rangeEnd - _rangeStart))) {
            // get the line into runtime memory
            size_t start;
            auto len = bufFile.readLine(start);

            cells[0] = (char*)runtime::rtmalloc(len + 1);
            cells[0][len] = 0;
            cell_sizes[0] = len + 1;
            memcpy(cells[0], &bufFile.getBuffer()[start], len);

            // call functor
            auto resCode = i64ToEC(_rowFunctor(_userData, rowNumber, cells, cell_sizes));
            _numRowsRead++;
            if(resCode != ExceptionCode::SUCCESS) {

                // output limit reached?
                if(ExceptionCode::OUTPUT_LIMIT_REACHED == resCode)
                    break;

                // this should not happen in text-reader...
                std::cerr<<"TextReader failure (should not happen), Row "<<rowNumber<<" exception: "<<exceptionCodeToString(resCode)<<std::endl;
            }

            // fetch contents
            runtime::rtfree_all(); // reset memory
            rowNumber++;
        }

        delete [] cells;
        delete [] cell_sizes;
        runtime::rtfree_all();
    }
}