//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/codegen/JITCompiledCSVReader.h>
#include <CSVUtils.h>
#include <Logger.h>
#include <iostream>
#include <StringUtils.h>
#include <Utils.h>
#include <Base.h>

namespace tuplex {

    void JITCompiledCSVReader::read(const URI &inputFilePath) {

        // @TODO: in mapped, need to adjust chunking
        // //// use buffered for S3 and co
        // if(inputFilePath.isLocal()) {
        //     Logger::instance().defaultLogger().info("mmap on " + inputFilePath.toPath());
        //     readMapped(inputFilePath);
        // } else {
        //     Logger::instance().defaultLogger().info("buffered read on " + inputFilePath.toPath());
        //     readBuffered(inputFilePath);
        // }

        readBuffered(inputFilePath); // the mmap is not worth it, disable.
    }

    size_t JITCompiledCSVReader::parseAndCompareHeader(uint8_t *buffer, size_t buffer_size) {
        // skip header if set...
        if(!_header.empty()) {
            std::vector<std::string> row;
            size_t bytesParsed;
            parseRow((char*)buffer, (char*)(buffer + buffer_size), row, bytesParsed);

            // error handling:
            // does header match?
            // ==> else schema mismatch of file!
            bool headersMatch = row.size() == _header.size();
            if(headersMatch) {
                for(int i = 0; i < row.size(); ++i)
                    if(row[i] != _header[i])
                        headersMatch = false;
            }

            if(!headersMatch)
                throw std::runtime_error("headers do not match! Task failure!");

            // all ok, move input buffer by the amount of bytes needed.
            return bytesParsed;
        }
        return 0;
    }

    void JITCompiledCSVReader::readMapped(const URI &uri) {
        // check that functor is valid
        if(!_functor)
            throw std::runtime_error("functor not initialized");

        // iterate over input file, fill up buffer and call consume
        auto fp = VirtualFileSystem::map_file(uri);
        if(!fp)
            throw std::runtime_error("could not memory map " + uri.toPath());

        // determine start/endptr using ranges & account for header

        auto startPtr = fp->getStartPtr();
        auto endPtr = fp->getEndPtr();
        unsigned long fileSize = endPtr - startPtr;
        bool eof = false;

        // address header & co
        // if range is used and rangestart is different than zero detect start and move buffer
        int csvStartOffset = 0;
        if(useRange()) {
            startPtr += _rangeStart;

            // now find csv start
            csvStartOffset = csvFindLineStart((const char*)startPtr, std::min(fileSize, _bufferSize),
                                              _numColumns, _delimiter, _quotechar);

            // if rangeStart = 0, force parser to start at zero.
            if(_rangeStart == 0)
                csvStartOffset = 0;

            if(csvStartOffset < 0) {
                auto bufSearchSize = std::min(fileSize, _bufferSize);
                std::stringstream ss;
                ss<<std::string(__FILE__)<<":"<<__LINE__<<"Could not find csv start, aborting task.\n";
                ss<<"Searched following buffer ("<<bufSearchSize<<" B) assuming "<<_numColumns<<" columns:\n";
                ss<<fromCharPointers((const char*)startPtr, (const char*)startPtr + std::min(fileSize, _bufferSize))<<std::endl;
                throw std::runtime_error(ss.str());
            }

            startPtr += csvStartOffset; // start offset
        }

        // skip header (note that if no range is set _rangeStart is automatically 0...)
        if(_rangeStart == 0) {
            auto bytesParsed = parseAndCompareHeader(startPtr, fileSize);
            startPtr += bytesParsed;
        }

        // get end
        if(useRange()) {
            // else, just use endptr
            if(_rangeEnd < fileSize) {
                // get offset to next line
                auto p = fp->getStartPtr() + _rangeEnd;
                auto offset = csvOffsetToNextLine(reinterpret_cast<const char *>(p), std::min(_bufferSize, (unsigned long)(endPtr - p)));
                endPtr = p + offset;
            }

            eof = endPtr >= fp->getEndPtr();
        } else
            eof = true;

        // one call to functor for last buffer!
        eof = true;
        _functor(_userData, startPtr, endPtr - startPtr + 1, &_num_normal_rows, &_num_bad_rows, !eof);

        fp->close();
    }

    void JITCompiledCSVReader::readBuffered(const tuplex::URI &uri) {
        // check that functor is valid
        if(!_functor)
            throw std::runtime_error("functor not initialized");

        using namespace std;

#ifndef NDEBUG
        std::string idString = "CSV read " + std::to_string(_rangeStart) + "-" + std::to_string(_rangeEnd) + ":";
#endif

        // iterate over input file, fill up buffer and call consume
        auto fp = VirtualFileSystem::open_file(uri, VirtualFileMode::VFS_READ);
        if(!fp)
            throw std::runtime_error("could not open " + uri.toPath() + " in read mode.");

        // init buffers
        if(_inputBuffer)
            delete [] _inputBuffer;

        // add +16 zero padded bytes to buffer size in order to secure extra spot
        _inputBuffer = new uint8_t[_bufferSize + 16];
        memset(_inputBuffer, 0, _bufferSize + 16);
        _inBufferLength = 0;
        size_t rangeBytesRead = 0;

        int readBeforeSize = 16; // read 16 bytes before rangeStart, must be > 2 for lookback if the first field is hit accidentally


        bool useRange = this->useRange(); // important to call this here

        // if ranges are used, seek to rangeStart
        if(useRange && _rangeStart != 0) {
            assert(_rangeStart > readBeforeSize);
            // seek file to range start
            fp->seek(_rangeStart - readBeforeSize);
        }

        bool firstBlock = true;
        while(!fp->eof()) {
            // fill buffer with start
            size_t bytesToRead = _bufferSize - _inBufferLength;
            assert(bytesToRead <= _bufferSize);
            size_t bytesRead = 0;
            fp->read(_inputBuffer + _inBufferLength, bytesToRead, &bytesRead);
            _inBufferLength += bytesRead;
            assert(bytesRead <= bytesToRead);
            // set 16 bytes after buffer to zero (i.e. zero escaped output)
            // in the init _inputBuffer was actually allocated with 16 extra bytes to secure this
            // this is required because of the SSE42 instructions
            memset(_inputBuffer + _inBufferLength, 0, 16);


            // address header & co
            if(firstBlock) {
                // if range is used and rangestart is different than zero detect start and move buffer
                int csvStartOffset = 0;
                if(useRange) {

                    // when rangestart is 0, nothing todo
                    if(_rangeStart == 0) {
                        // nothing todo
                    } else {
                        // _inputBuffer contains a couple bytes before the start of the range, need for look back
                        auto info = findLineStart((const char*)_inputBuffer, _inBufferLength, readBeforeSize, _numColumns, _delimiter, _quotechar);

                        // @TODO: if this fails, a possible cause might be that a row doesn't fit into the buffer.
                        // should resize and use a couple retries. postponed for now, just pick the right value.
                        if(!info.valid)
                            throw std::runtime_error("could not find csv start in JITCompiledCSVReader, aborting task");
                        csvStartOffset = info.offset;
                        moveInputBuffer(csvStartOffset + readBeforeSize); // move to correct offset
#ifndef NDEBUG
                        Logger::instance().defaultLogger().info(idString + " clamped start to " + std::to_string(_rangeStart + csvStartOffset));
#endif
                    }
                }

                // skip header (note that if no range is set _rangeStart is automatically 0...)
                if(_rangeStart == 0) {
                    // skip header if set...
                    auto bytesParsed = parseAndCompareHeader(_inputBuffer, _inBufferLength);
                    // all ok, move input buffer by the amount of bytes needed.
                    moveInputBuffer(bytesParsed);
                    _rangeStart += bytesParsed;
                }

                // update range start
                _rangeStart += csvStartOffset;


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

                    int maxOffset = 0;
                    while(maxOffset < remainingToParse) {
                        // get offset to next line.
                        int offset = csvOffsetToNextLine(p, endp - p);
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
#ifdef TRACE_PARSER
            // print buffer for debug
            cout<<"input buffer:\n"<<fromCharPointers((char*)_inputBuffer, (char*)(_inputBuffer + _inBufferLength))<<endl;
#endif

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

            moveInputBuffer(bytesConsumed);


#ifdef TRACE_PARSER
            cout<<"inbuffer length: "<<_inBufferLength<<endl;
            //cout<<"new input buffer:\n"<<_inputBuffer<<endl;
#endif
            rangeBytesRead += bytesConsumed;

            // check if range expired
            if(useRange && rangeBytesRead > (_rangeEnd - _rangeStart))
                break;
        }

#ifndef NDEBUG
        if(useRange) {
            std::stringstream ss;
            ss<<idString<<" read bytes from "<<fp->getURI().toPath()<<" range: "<<_rangeStart<<" - "<<_rangeStart + rangeBytesRead;
            Logger::instance().defaultLogger().info(ss.str());

            // debug checks
            auto fsize = fp->size();

            // either the read range is larger than the range End xor
            // the rangeend is larger than the actual file size
            assert(_rangeStart + rangeBytesRead >= _rangeEnd || _rangeEnd > fsize);
        }
        Logger::instance().defaultLogger().info("CSV read done: " + pluralize(_num_normal_rows, "normal row") + " / " + pluralize(_num_bad_rows, "exceptional row"));
#endif
    }

    size_t JITCompiledCSVReader::consume(bool eof) {
        using namespace std;

#ifdef TRACE_PARSER
        if(eof)
            cout<<"EOF reached"<<endl;
#endif

        // new version using functor
        // check whether at eof

        // value of endPtr
        auto endPtr = _inputBuffer + _inBufferLength - 1;
        auto endPtrVal = *endPtr;

        assert(_inBufferLength > 0);
        auto bytesParsed = _functor(_userData, _inputBuffer, _inBufferLength, &_num_normal_rows, &_num_bad_rows, !eof);

        // trick: negative means ECCdode! -> i.e. abort, no need to parse further
        if(bytesParsed < 0) {
            auto ecCode = -bytesParsed;
            throw std::runtime_error("found ecCode " + std::to_string(ecCode)); // <-- TODO: add logic
        }

#ifdef TRACE_PARSER
        cout<<"processed "<<bytesParsed<<"/"<<_inBufferLength<<" bytes"<<endl;
        cout<<"parsed num rows: "<<num_normal_rows<< " normal, "<<num_bad_rows<<" exceptions"<<endl;
#endif

        return bytesParsed;
    }

    void JITCompiledCSVReader::moveInputBuffer(size_t bytesConsumed) {
        if(bytesConsumed == 0)
            return;

        assert(bytesConsumed <= _inBufferLength);

        // copy whatever was consumed back to beginning & resume loop
        // don't use memcpy here, might be unsafe. instead use memmove!
        // memcpy(_inputBuffer, _inputBuffer + bytesConsumed, _inBufferLength - bytesConsumed);
        memmove(_inputBuffer, _inputBuffer + bytesConsumed, _inBufferLength - bytesConsumed);
        _inBufferLength = _inBufferLength - bytesConsumed;
        memset(_inputBuffer + _inBufferLength, 0, 16); // important for parsing!
    }
}