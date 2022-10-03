//
// Created by leonhard on 10/3/22.
//

#include <physical/JsonReader.h>
#include <simdjson.h>
#include <JSONUtils.h>
#include <JsonStatistic.h>

namespace tuplex {
    JsonReader::JsonReader(void *userData, codegen::read_block_f rowFunctor) : _functor(rowFunctor), _userData(userData) {
        setRange(0, 0);

        _bufferSize = 0;
        _inBufferLength = 0;
        _inputBuffer = nullptr;

        _num_normal_rows = 0;
        _num_bad_rows= 0;
    }

    void JsonReader::setRange(size_t start, size_t end) {
        assert(start <= end);
        _rangeStart = start;
        _rangeEnd = end;
    }

    void JsonReader::read(const tuplex::URI &inputFilePath) {
        using namespace std;

        if(!_functor)
            throw std::runtime_error("functor not initialized for JsonReader");

        static size_t simd_security_bytes = simdjson::SIMDJSON_PADDING;


        // open file
        auto fp = VirtualFileSystem::open_file(inputFilePath, VirtualFileMode::VFS_READ);
        if(!fp)
            throw std::runtime_error("could not open " + inputFilePath.toPath() + " in read mode.");

        bool useRange = _rangeStart < _rangeEnd;
        if(useRange && _rangeStart != 0) {
            // seek file to range start
            fp->seek(static_cast<int64_t>(_rangeStart));
        }

        // init internal buffer
        // init buffers
        if(_inputBuffer)
            delete [] _inputBuffer;

        // read a buffer in, if it's the first block search for start!
        _inputBuffer = new uint8_t[_bufferSize + simd_security_bytes];
        memset(_inputBuffer, 0, _bufferSize + simd_security_bytes);
        _inBufferLength = 0;
        size_t rangeBytesRead = 0;

        bool firstBlock = true;
        while(!fp->eof()) {
            // fill buffer with start
            size_t bytesToRead = _bufferSize - _inBufferLength;
            assert(bytesToRead <= _bufferSize);
            size_t bytesRead = 0;
            fp->read(_inputBuffer + _inBufferLength, bytesToRead, &bytesRead);
            _inBufferLength += bytesRead;
            assert(bytesRead <= bytesToRead);
            // set for simdjson parser some bytes after buffer to zero (i.e. zero escaped output)
            // in the init _inputBuffer was actually allocated with 16 extra bytes to secure this
            // this is required because of the SSE42 instructions
            memset(_inputBuffer + _inBufferLength, 0, simd_security_bytes);


            // address header & co
            if(firstBlock) {
                // if range is used and rangestart is different than zero detect start and move buffer
                int jsonStartOffset = 0;
                if(useRange) {

                    // when rangestart is 0, there's nothing to do
                    if(_rangeStart == 0) {
                        // skip, nothing to do. know already the proper range start!
                    } else {
                        // _inputBuffer contains a couple bytes before the start of the range, need for look back
                        auto j_offset = findNLJsonStart((const char*)_inputBuffer, _inBufferLength);
                        // abort if not start could be found
                        if(j_offset < 0)
                            throw std::runtime_error("could not find json start in JsonReader, aborting task");
                        moveInputBuffer(static_cast<size_t>(j_offset), simd_security_bytes); // move to correct offset
                    }
                }

                // update range start
                _rangeStart += jsonStartOffset;


                firstBlock = false;
            }

            // it could happen that rangeStart is now > rangeEnd, if so then read is done
            if(useRange && _rangeEnd < _rangeStart)
                return;

            // clamp to range when ranges are used, i.e. do parse till the first valid line after range end or file end, whatever is shorter
            if(useRange) {
                if(rangeBytesRead + _inBufferLength > (_rangeEnd - _rangeStart)) {
                    // reached end of parsing range. need to find end of parsing range
                    // best idea here is to do manual parsing 2x, i.e. use helper function to find end & then cut it off!

                    auto remainingToParse = (_rangeEnd - _rangeStart) - rangeBytesRead;

                    const char *p = (const char*)_inputBuffer; // NOTE: this code here works WHEN inputbuffer is at record start state!
                    auto endp = p + _inBufferLength;

                    int64_t maxOffset = 0;
                    while(maxOffset < remainingToParse) {
                        // get offset to next line.
                        auto offset = findNLJsonStart(p, endp - p);
                        assert(offset >= 0);
                        // release fix
                        offset = std::max(static_cast<int64_t>(0), std::min(static_cast<int64_t>(endp - p), offset));
                        // std::cout<<"position: "<<maxOffset<<" , offset to next line: "<<offset<<endl;
                        // std::cout<<string((const char*)_inputBuffer).substr(maxOffset + offset, 256)<<endl;
                        p += offset;
                        maxOffset += offset;
                    }

                    // zero out buffer after maxOffset
                    _inBufferLength = maxOffset;
                    // important to cut off here!
                    memset(_inputBuffer + _inBufferLength, 0, 16); // important for parsing!
                    rangeBytesRead += consume(true);
                    break;
                }
            }

            // somewhere in this loop there is an error!

            // consume buffer (by function)
            // get eof file to consume function...
            size_t bytesConsumed = consume(fp->eof());

            assert(bytesConsumed <= _bufferSize);

            if(0 == bytesConsumed && _inBufferLength == _bufferSize) { //@TODO: test for this!!!
                // this case is assumed if the line is larger than the buffer!!!
                // --> needs to be handled separately
                cerr<<"line might not fit into buffer here, need to handle this case separately!!!"<<endl;
                cout<<"no bytes consumed, stopping task"<<endl;
                break;
            }

            if(0 == bytesConsumed) {
                cerr<<"0 bytes consumed..."<<endl;
            }

            moveInputBuffer(bytesConsumed, simd_security_bytes);
            rangeBytesRead += bytesConsumed;

            // check if range expired
            if(useRange && rangeBytesRead > (_rangeEnd - _rangeStart))
                break;
        }
    }

    void JsonReader::setFunctor(codegen::read_block_f functor) {
        _functor = functor;
    }

    size_t JsonReader::inputRowCount() const {
        return _num_normal_rows + _num_bad_rows;
    }

    void JsonReader::moveInputBuffer(size_t bytesConsumed, size_t simd_security_bytes) {
        if(bytesConsumed == 0)
        return;

        assert(bytesConsumed <= _inBufferLength);

        // copy whatever was consumed back to beginning & resume loop
        // don't use memcpy here, might be unsafe. instead use memmove!
        memmove(_inputBuffer, _inputBuffer + bytesConsumed, _inBufferLength - bytesConsumed);
        _inBufferLength = _inBufferLength - bytesConsumed;
        memset(_inputBuffer + _inBufferLength, 0, simd_security_bytes); // important for parsing!
    }

    size_t JsonReader::consume(bool eof) {
        using namespace std;

        // new version using functor
        // check whether at eof

        // value of endPtr
        auto endPtr = _inputBuffer + _inBufferLength - 1;
        auto endPtrVal = *endPtr;

        assert(_inBufferLength > 0);
        auto bytesParsed = _functor(_userData, _inputBuffer, _inBufferLength, &_num_normal_rows, &_num_bad_rows, !eof);

        return bytesParsed;
    }
}