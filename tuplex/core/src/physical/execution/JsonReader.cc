//
// Created by leonhard on 10/3/22.
//

#include <physical/execution/JsonReader.h>
#include <simdjson.h>
#include <JSONUtils.h>
#include <JsonStatistic.h>
#include <physical/experimental/JsonHelper.h>
#include <jit/RuntimeInterface.h>

namespace tuplex {

//    // helper func to decvelop parsing
//    int64_t dummy_functor(void* userData, const uint8_t* buf, int64_t buf_size, int64_t* out_normal_rows, int64_t* out_bad_rows, int8_t is_last_part) {
//        using namespace codegen;
//
//        // open up JSON parsing
//        auto j = JsonParser_init();
//        if(!j)
//            return -1;
//
//        int64_t row_number = 0;
//
//        JsonParser_open(j, reinterpret_cast<const char *>(buf), buf_size);
//        while (JsonParser_hasNextRow(j)) {
//            if (JsonParser_getDocType(j) != JsonParser_objectDocType()) {
//                // BADPARSE_STRINGINPUT
//                auto line = JsonParser_getMallocedRow(j);
//                free(line);
//            }
//
//            auto doc = *j->it;
//            JsonItem *obj = nullptr;
//            uint64_t rc = JsonParser_getObject(j, &obj);
//            if (rc != 0)
//                break; // --> don't forget to release stuff here!
//
//            // release all allocated things
//            JsonItem_Free(obj);
//
//            runtime::rtfree_all();
//            row_number++;
//            JsonParser_moveToNextRow(j);
//        }
//
//        size_t size = j->stream.size_in_bytes();
//        size_t truncated_bytes = j->stream.truncated_bytes();
//
//        JsonParser_close(j);
//        JsonParser_free(j);
//
//        if(out_normal_rows)
//            *out_normal_rows = row_number;
//        if(out_bad_rows)
//            *out_bad_rows = 0;
//
//        // returns how many bytes are parsed...
//        return buf_size - truncated_bytes;
//    }

    JsonReader::JsonReader(void *userData, codegen::read_block_f rowFunctor, size_t bufferSize) : _functor(rowFunctor), _userData(userData) {
        setRange(0, 0);

        static const auto SIMDJSON_MINIMUM = 4 * 1024 * 1024ul; // per default, simdjson uses 1MB.

        _bufferSize = std::max(bufferSize, SIMDJSON_MINIMUM); // should be at least whatever simdjson wants
        _inBufferLength = 0;
        _inputBuffer = nullptr;

        _num_normal_rows = 0;
        _num_bad_rows= 0;

        // // dummy for testing
        // _functor = dummy_functor;

        _rangeStart = 0;
        _rangeEnd = 0;
        _originalRangeStart = 0;
        _originalRangeEnd = 0;
    }

    void JsonReader::setRange(size_t start, size_t end) {
        assert(start <= end);
        _rangeStart = start;
        _rangeEnd = end;

        _originalRangeStart = start;
        _originalRangeEnd = end;
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
                        jsonStartOffset = j_offset;
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
                        auto offset = findNLJsonOffsetToNextLine(p, endp - p);
                        runtime::rtfree_all();
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

        // range bytes read
        if(useRange) {
           // std::cout<<"actual range read: "<<_rangeStart<<"-"<<_rangeStart + rangeBytesRead<<std::endl;
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

        // Logger::instance().defaultLogger().debug("calling memmove with bytesConsumed=" + std::to_string(bytesConsumed) + " and inbufferlength=" + std::to_string(_inBufferLength));
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
        
        if(!_functor) {
            throw std::runtime_error("functor in JsonReader is nullptr, compile error?");
        }
        
        auto bytesParsed = _functor(_userData, _inputBuffer, _inBufferLength, &_num_normal_rows, &_num_bad_rows, !eof);

        // if bytes are negative, error!
        if(bytesParsed < 0) {
            auto ecCode = i64ToEC(-bytesParsed);

            // special case: early terminate!
            if(ExceptionCode::OUTPUT_LIMIT_REACHED == ecCode)
                // special case!
                throw std::runtime_error("All ok here, just need to add the necessary logic...");

            throw std::runtime_error("Json read failed with code " + std::to_string(-bytesParsed));
        }

        return bytesParsed;
    }

    JsonReader::~JsonReader() {
        if(_inputBuffer)
            delete [] _inputBuffer;

        _inBufferLength = 0;
        _inputBuffer = nullptr;

        _num_normal_rows = 0;
        _num_bad_rows= 0;
    }
}
