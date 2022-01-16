//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/CSVReader.h>
#include <VirtualFileSystem.h>
#include <physical/csvmonkey.h>
#include <Logger.h>
#include <RuntimeInterface.h>
#include <StringUtils.h>
#include <CSVUtils.h>
#include <Base.h>

namespace tuplex {
    // class for manual cursor (block based from file)
    class VFCSVStreamCursor : public csvmonkey::BufferedStreamCursor {
    public:
        VFCSVStreamCursor() = delete;

        explicit VFCSVStreamCursor(const URI &uri, char delimiter, char quotechar, size_t numColumns = 0,
                                   size_t rangeStart = 0, size_t rangeEnd = 0) :
                _delimiter(delimiter), _quotechar(quotechar),
                _file(VirtualFileSystem::open_file(uri, VirtualFileMode::VFS_READ)), _numColumns(numColumns),
                _rangeStart(rangeStart), _rangeEnd(rangeEnd), _curFilePos(0) {

            if(!_file)
                throw std::runtime_error("could not open file " + uri.toPath());

            ensure(16 * 1024); // 16KB buffer

            if(_rangeStart < _rangeEnd)
                seekToStart();
        }

        void consume(size_t n) override {
            auto delta = std::min(n, write_pos_ - read_pos_);
            read_pos_ += delta;
            _curFilePos += delta; // important for range based!
            CSM_DEBUG("consume(%lu); new size: %lu", n, size())
        }

        size_t curFilePos() const {
            return _curFilePos;
        }

        /**
         * actual start of range that is been read
         * @return i.e. where csv offset starts
         */
        size_t rangeStartRead() const { return _rangeStart; }

        /*!
         * actual end of range reading.
         * @return
         */
        size_t rangeEndRead() const { assert(_curFilePos >= _rangeEnd); return _curFilePos; }

        ssize_t readmore() override {

            // Note: current version of CSV reader expects last line to be newline delimited. need to fix that!

            // eof should return -1 to stop reading more chars
            if (_file->eof()) {

                // consume remaining bytes
                if(size() > 0) {

                    // check the left characters in the buffer. If they are only '\r', '\n' or '\0'
                    // consume them and stop parse
                    bool all_escape_chars = true;
                    for(int i = 0; i < size(); ++i) {
                        char c = vec_[read_pos_ + i];
                        if(c != '\r' && c != '\n' && c != 0)
                            all_escape_chars = false;
                    }

                    if(all_escape_chars) {
                        consume(size());
                        assert(write_pos_ == read_pos_);
                        // force to zero
                        write_pos_ = read_pos_ = 0;
                        return 0;
                    }

                    // set everything after write pos to zero
                    memset(&vec_[write_pos_], 0, vec_.size() - write_pos_);

                    // check if null terminated, if not update with both endline + newline
                    if(vec_[write_pos_] != '\0' && vec_[write_pos_] != '\n') {
                        assert(write_pos_ + 2 < vec_.size());
                        // fill in 0 and newline (because of csvmonkey bug)
                        vec_[write_pos_] = '\n';
                        vec_[write_pos_+1] = 0;
                        write_pos_++;
                    }

                    // prune write pos zero
                    while(write_pos_ >= read_pos_ && vec_[write_pos_ + 1] == '\0')
                        write_pos_--;

                    return 0;
                } else
                    return -1;
            }

            // end reached? ==> return remaining in vec!
            // note check on rangeEnd so if rangeStart == rangeEnd, b
            if(_rangeEnd > 0 && _curFilePos >= _rangeEnd)
                return -1; // done

            size_t bytesRead = 0;
            // ranges defined?
            if(_rangeStart < _rangeEnd) {

                // fill up buffer with next content
                // read one less than buf capacity to end file with 0
                // note that 33 is important to safeguard buffer! (+1 for '\0' + 32 for 2x 16 byte SIMD execution)
                auto nbytes_max_to_read = vec_.size() - write_pos_ - 33; // fill buffer completely up
                _file->read(&vec_[write_pos_], nbytes_max_to_read, &bytesRead);

                // note: need to provide as much overhead as required!!!
                auto remainingToParse = _rangeEnd - _curFilePos;
                if(bytesRead > remainingToParse) {

                    if(0 == remainingToParse)
                        return -1; // done

                    const char* p = (const char*)buf();
                    auto endp = p + bytesRead;

                    int maxOffset = 0;
                    while(maxOffset < remainingToParse) {
                        // get offset to next line.
                        int offset = csvOffsetToNextLine(p, endp - p, _delimiter, _quotechar);
                        p += offset;
                        maxOffset += offset;
                    }

                    // zero out buffer after maxOffset !!!
                    assert(write_pos_ + maxOffset < vec_.size());
                    vec_[write_pos_ + maxOffset] = 0;

                    return maxOffset;
                } else {
                    //_curFilePos += bytesRead;
                    // check if eof, then fill up with 0
                    if (_file->eof()) {
                        vec_[write_pos_ + bytesRead] = 0;
                        bytesRead++;
                    }

                    return bytesRead;
                }

            } else {
                // just read full file
                // read one less than buf capacity to end file with 0
                auto nbytes_max_to_read = vec_.size() - write_pos_ - 33;
                _file->read(&vec_[write_pos_], nbytes_max_to_read, &bytesRead);

                // check if eof, then fill up with newline and 0
                if (_file->eof()) {

                    size_t pos = write_pos_ + bytesRead;
                    vec_[pos] = '\0'; // zero out.
                    while(pos >= read_pos_ && vec_[pos] == '\0')
                        pos--;
                    // check whether newline delimited, if not add & correct position accordingly
                    if(vec_[pos] != '\r' && vec_[pos] != '\n') {
                        vec_[++pos] = '\n';
                        vec_[++pos] = 0;
                        return pos;
                    }
                }

                return bytesRead;
            }
        }

        VirtualFile *file() const { return _file.get(); }

    private:
        char _delimiter;
        char _quotechar;
        std::unique_ptr<VirtualFile> _file;
        size_t _numColumns;
        size_t _rangeStart;
        size_t _rangeEnd;
        size_t _curFilePos;


        int getChunkStart(int num_resizes_left=5) {

            // reset buffer
            write_pos_ = 0;
            read_pos_ = 0;

            // special case: _rangeStart is 0, i.e. at beginning of file, no need to infer chunk start.
            if(_rangeStart == 0) {
                size_t bytesRead = 0;
                auto nbytes_max_to_read = vec_.size() - write_pos_ - 33; // fill buffer completely up
                _file->read(&vec_[write_pos_], nbytes_max_to_read, &bytesRead);
                write_pos_ = bytesRead;
                return 0;
            }

            // fill initial buffer starting from rangeStart
            assert(_rangeStart >= 128); // sanity check

            int readBeforeSize = 16; // read 16 bytes before rangeStart, must be > 2 for lookback if the first field is hit accidentally

            // find start
            _file->seek(_rangeStart - readBeforeSize);

            size_t bytesRead = 0;
            // read one less than buf capacity to end file with 0

            // note that 33 is important to safeguard buffer! (+1 for '\0' + 32 for 2x 16 byte SIMD execution)
            assert(write_pos_ == 0);
            auto nbytes_max_to_read = vec_.size() - write_pos_ - 33; // fill buffer completely up
            _file->read(&vec_[write_pos_], nbytes_max_to_read, &bytesRead);

#ifndef NDEBUG
            // // uncomment to trace possible bugs in CSV chunking.
            // auto chunkInfoSize = 128;
            // auto chunkInfo = fromCharPointers((const char*)&vec_[read_pos_], (const char*)&vec_[read_pos_] + chunkInfoSize);
            // std::cout<<"inferring data from:"<<std::endl;
            // std::cout<<chunkInfo<<std::endl;
            // for(int i = 0; i < readBeforeSize; ++i)
            //     std::cout<<"~";
            // std::cout<<"^";
            // for(int i = readBeforeSize; i < chunkInfoSize; ++i)
            //     std::cout<<"~";
            // std::cout<<std::endl;
#endif

            // find start of chunk

            auto info = findLineStart((const char*)&vec_[0], bytesRead, readBeforeSize,
                                      _numColumns, _delimiter, _quotechar);

            if(!info.valid) {

                // Note: this code doesn't work yet b.c. of bad seeking, redo better in future release
//                // one possible reason why this fails could be that the input buffer doesn't hold enough data, i.e. resize buffer
//                if(num_resizes_left > 0) {
//                    // double buf and retry
//                    ensure(2 * vec_.size());
//                    Logger::instance().defaultLogger().info("could not find csv chunk start, retrying with larger buffer (CSVReader.cc)");
//                    return getChunkStart(num_resizes_left - 1);
//                } else {

                auto bufSearchSize = bytesRead;
                std::stringstream ss;
                ss<<std::string(__FILE__)<<":"<<__LINE__<<"Could not find csv start, aborting task.\n";
                ss<<"Searched following buffer ("<<bufSearchSize<<" B) assuming "<<_numColumns<<" columns:\n";
                ss<<fromCharPointers((const char*)&vec_[0], (const char*)&vec_[0] + bufSearchSize)<<std::endl;

#ifndef NDEBUG
                // for quicker jumping into the error...
                auto info = findLineStart((const char*)&vec_[0], bytesRead, readBeforeSize,
                                          _numColumns, _delimiter, _quotechar);
#endif

                throw std::runtime_error(ss.str());
//                }
            }


            auto csvStartOffset = info.offset;

            // update range Start
            _rangeStart += csvStartOffset;
            //write_pos_ = std::min(bytesRead + _file->eof(), _rangeEnd - _rangeStart);
            write_pos_ = bytesRead;
            // move input by start offset + readbefore buffer
            read_pos_ = csvStartOffset + readBeforeSize;


#ifndef NDEBUG
             // chunkInfo = fromCharPointers((const char*)&vec_[read_pos_], (const char*)&vec_[read_pos_] + chunkInfoSize);
             // std::cout<<"first line to parse is:"<<std::endl;
             // std::cout<<chunkInfo<<std::endl;
#endif
            return csvStartOffset;
        }


        void seekToStart() {
            // reset bufferedstreamcursor
            reset();

            _curFilePos = 0;

            if(_rangeStart == 0 && _rangeEnd == 0)
                return;

            // if ranges, fill!
            if(_rangeStart < _rangeEnd) {

#ifndef NDEBUG
                // sanity check
                // range should be at least 256bytes for a row!
                if(_rangeEnd - _rangeStart < 256)
                    Logger::instance().defaultLogger().debug("extremely small range of " + std::to_string(_rangeEnd - _rangeStart) + " requested, merge with other part?");
#endif
                getChunkStart();

                _curFilePos = _rangeStart;

                // check if eof, then fill up with 0
                if (_file->eof())
                    vec_[write_pos_] = 0;
            }

            // make sure rangeEnd is higher than rangeStart
            if(_rangeEnd != 0) {
                if(_rangeEnd <= _rangeStart) {
                    // buffer to short to hold line
                    Logger::instance().defaultLogger().warn("range can't hold a single line, skipping task");
                }
            }
        }
    };
}


namespace tuplex {
    char* serializeParseException(int64_t numCells,
            char **cells,
            int64_t* sizes,
            size_t *buffer_size,
            std::vector<bool> colsToSerialize,
            decltype(malloc) allocator) {
        assert(cells && sizes && buffer_size && allocator);
        assert(numCells >= 0);

        // empty columns, serialize all!
        if(colsToSerialize.empty())
            for(int i = 0; i < numCells; ++i)
                colsToSerialize.push_back(true);

        auto numCellsToSerialize = 0;
        for(auto c : colsToSerialize)
            numCellsToSerialize += c;

        // special row format for CSV operator exceptions
        // => i.e. first int64_t cell counts, then cell_counts x sizes int64_t with cell sizes + offsets
        size_t buf_size = sizeof(int64_t) + numCells * sizeof(int64_t);
        for(int i = 0; i < numCells; ++i)
            if(colsToSerialize[i])
                buf_size += sizes[i];

        assert(allocator);
        auto buf_ptr = (char*)allocator(buf_size);
        auto buf = buf_ptr;
        *(int64_t*)buf = numCellsToSerialize;
        buf += sizeof(int64_t);

        // write size info
        size_t acc_size = 0;
        int pos = 0;
        for(int i = 0; i < numCells; ++i) {
            if(colsToSerialize[i]) {
                uint64_t info = (uint64_t)sizes[i] & 0xFFFFFFFF;

                // offset = jump + acc size
                uint64_t offset = (numCellsToSerialize - pos) * sizeof(int64_t) + acc_size;
                *(uint64_t*)buf = (info << 32u) | offset;
                memcpy(buf_ptr + sizeof(int64_t) * (numCellsToSerialize + 1) + acc_size, cells[i], sizes[i]);

                // memcmp check?
                assert(memcmp(buf + offset, cells[i], sizes[i]) == 0);

                buf += sizeof(int64_t);
                acc_size += sizes[i];
                pos++;
            }
        }

        if(buffer_size)
            *buffer_size = buf_size;

        return buf_ptr;
    }

    void CSVReader::read(const URI& inputFilePath) {
        using namespace std;

        _numRowsRead = 0;

        // skip task
        if(_rangeEnd > 0 && _rangeEnd <= _rangeStart) {
            Logger::instance().defaultLogger().warn("invalid range, skipping task");
            return;
        }

        assert(_numColumns > 0); // reading only makes sense for at least one col

        // skip, empty file! ==> exceptions not realized!
        if(_numColumns == 0) {
            Logger::instance().defaultLogger().warn("numcols in reader is zero, exceptions won't be processed. skip file.");
            return;
        }

        // check that functor is valid
        if(!_rowFunctor)
            throw std::runtime_error("functor not initialized");

        // create cursor
        VFCSVStreamCursor cursor(inputFilePath, _delimiter, _quotechar, _numColumns, _rangeStart, _rangeEnd);

        // debug: find actual start range
        auto actual_byte_start = cursor.curFilePos();

        // read using csvmonkey
        csvmonkey::CsvReader<> reader(cursor, _delimiter, _quotechar);
        auto &row = reader.row();

        // compare header only for file start!
        if(!_header.empty() && _rangeStart == 0) {
            // read first row, if empty file all good
            if(!reader.read_row()) // empty file
                return;

            // compare file against header
            vector<string> cells;
            for(int i = 0; i < row.count; ++i) {
                if(row.cells[i].ptr)
                    cells.emplace_back(row.cells[i].as_str());
                else
                    cells.emplace_back("");
            }

            // compare
            stringstream ss;
            ss<<"file "<<inputFilePath.toPath()<<" header and stored header do not match.\nDetails:\n";
#ifndef NDEBUG
            ss<<"header #cells: "<<cells.size()<<endl;
            ss<<"store header #cells: "<<_header.size()<<endl;
            ss<<"        cell | stored_header_cell"<<endl;
            for(int i = 0; i < std::min(cells.size(), _header.size()); ++i) {
                ss<<"cell #"<<i<<": "<<cells[i]<<" | "<<_header[i]<<endl;
            }
#endif
            auto errMessage = ss.str();
            if(cells.size() != _header.size())
                throw std::runtime_error(errMessage);

            for(int i = 0; i < cells.size(); ++i)
                if(cells[i] != _header[i])
                    throw std::runtime_error(errMessage);

            // nothing left to do, header skipped.
        }

        // check if range is already exhausted
        if(_rangeEnd != 0 && cursor.curFilePos() >= _rangeEnd)
            return;

        // iterate over rows
        char **cells = new char*[_numColumns];
        int64_t *cell_sizes = new int64_t[_numColumns];
        for(int i = 0; i < _numColumns; ++i)
            cells[i] = nullptr;
        const char *empty_str = "";

        size_t rowNumber = 0;
        while(reader.read_row()) {
            // fetch content of row
            if(row.count != _numColumns) {
                // full row as exception (schema mismatch)
                // fetch line from parse (add up chars)
                string line = fromCharPointers(row.cells[0].ptr, reader.ptr());
                assert(row.count != 0);

                // produce badcsvparse input...
                // Todo: serialize whole row as exception...
                auto resCode = ExceptionCode::BADPARSE_STRING_INPUT;

                // serializeParse exception => this is expensive
                char **cells = new char*[row.count];
                int64_t *cell_sizes = new int64_t[row.count];
                for(int i = 0; i < row.count; ++i)
                    cells[i] = nullptr;
                const char *empty_str = "";
                // get strings, then do value conversions etc. in code generated code...
                for(int i = 0; i < row.count; ++i) {
                    // note as_str should dequote cell already...
                    if(row.cells[i].ptr) {
                        auto cell = row.cells[i].as_str();
                        cell_sizes[i] = cell.length() + 1;

                        // copy using malloc or runtime memory?
                        // ==> copy here required because cell goes out of scope, so string
                        // gets destructed...
                        cells[i] = (char*)runtime::rtmalloc(cell.length() + 1);
                        memcpy(cells[i], cell.c_str(), cell.length() + 1); // +1 for zero terminate...
                    } else {
                        cells[i] = const_cast<char*>(empty_str);
                        cell_sizes[i] = 1; // it's size! not length...
                    }
                }

                // now call exception serialize
                size_t exception_buf_size = 0;
                auto exception_buf = serializeParseException(row.count, cells, cell_sizes, &exception_buf_size, _columnsToKeep, runtime::rtmalloc);

                // upgrade parse errors for slow path resolution
                if(_makeParseErrorsInternal)
                    resCode = ExceptionCode::BADPARSE_STRING_INPUT;
#ifndef NDEBUG
                // // memory will be freed from runtime, do not bother...
                // std::cout<<"Row "<<rowNumber<<" exception: "<<exceptionCodeToString(resCode)<<std::endl;
                // std::cout<<"    => serialized bad row to memory: "<<exception_buf_size<<" bytes"<<std::endl;
#endif
                // call exception handler with params
                // params are:  void* userData,
                //              int64_t exceptionCode,
                //              int64_t exceptionOperatorID,
                //              int64_t rowNumber,
                //              uint8_t* buf,
                //              int64_t buf_size
                if(_exceptionHandler)
                    _exceptionHandler(_userData, ecToI64(resCode), _operatorID, rowNumber, reinterpret_cast<uint8_t*>(exception_buf), exception_buf_size);

            } else {


//#define CSVREADER_USE_MALLOC
#ifdef CSVREADER_USE_MALLOC
                for(int i = 0; i < _numColumns; ++i)
                    cells[i] = nullptr;

#endif

                // get strings, then do value conversions etc. in code generated code...
                for(int i = 0; i < _numColumns; ++i) {

#ifndef NDEBUG
                    // int num_debug_cols = 8;
                    // if(i < num_debug_cols) {
                    //     auto cell = row.cells[i].ptr ? row.cells[i].as_str() : string();
                    //     cout<<cell<<" | ";
                    // }
                    // if(i == num_debug_cols)cout<<endl;
#endif
                    // note as_str should dequote cell already...
                    if(row.cells[i].ptr) {
                        auto cell = row.cells[i].as_str();
                        cell_sizes[i] = cell.length() + 1;

                        // copy using malloc or runtime memory?
                        // ==> copy here required because cell goes out of scope, so string
                        // gets destructed...
#ifdef CSVREADER_USE_MALLOC
                        cells[i] = (char*)malloc(cell.length() + 1);
#else
                        cells[i] = (char*)runtime::rtmalloc(cell.length() + 1);
#endif
                        memcpy(cells[i], cell.c_str(), cell.length() + 1); // +1 for zero terminate...
                    } else {
                        cells[i] = const_cast<char*>(empty_str);
                        cell_sizes[i] = 1; // it's size! not length...
                    }
                }

                // call functor
                auto resCode = i64ToEC(_rowFunctor(_userData, rowNumber, cells, cell_sizes));
                _numRowsRead++;
                if(resCode != ExceptionCode::SUCCESS) {
                    using namespace std;
                    // serialize

                    // output limit reached?
                    if(ExceptionCode::OUTPUT_LIMIT_REACHED == resCode)
                        break;

                    // TODO here: for 311 need to put row onto separate exception stack!
                    // ==> save parse information here!
                    // or simpler: If parse error, then save all cells as string output!
                    // serialize as CSV operator error(i.e. serialize with cells + sizes!

                    // now call exception serialize
                    size_t exception_buf_size = 0;
                    auto exception_buf = serializeParseException(row.count, cells, cell_sizes, &exception_buf_size, _columnsToKeep, runtime::rtmalloc);

                    // upgrade parse errors for slow path resolution
                    if(_makeParseErrorsInternal)
                        resCode = ExceptionCode::BADPARSE_STRING_INPUT;

#ifndef NDEBUG
                    // // memory will be freed from runtime, do not bother...
                    // std::cout<<"Row "<<rowNumber<<" exception: "<<exceptionCodeToString(resCode)<<std::endl;
                    // std::cout<<"    => serialized bad row to memory: "<<exception_buf_size<<" bytes"<<std::endl;
#endif

                    // @TODO: am not sure whether pipeline functor has handler or not...
                    // should not have one.
                    // call exception handler with params
                    // params are:  void* userData,
                    //              int64_t exceptionCode,
                    //              int64_t exceptionOperatorID,
                    //              int64_t rowNumber,
                    //              uint8_t* buf,
                    //              int64_t buf_size
                    if(_exceptionHandler)
                        _exceptionHandler(_userData, ecToI64(resCode), _operatorID, rowNumber, reinterpret_cast<uint8_t*>(exception_buf), exception_buf_size);
                }

#ifdef CSVREADER_USE_MALLOC
                for(int i = 0; i < _numColumns; ++i) {
                    if(cells[i] && cells[i] != empty_str) {
                        free(cells[i]);
                        cells[i] = nullptr;
                    }
                }
#else
                // fetch contents
                runtime::rtfree_all(); // reset memory
#endif
            }
            rowNumber++;

            // check whether curFilePos (i.e. the one where current offset is) of cursor is larger than rangeEnd, if so end parse
            // when ranges are used
            if(_rangeEnd != 0 && cursor.curFilePos() >= _rangeEnd)
                break;
        }

        delete [] cells;
        delete [] cell_sizes;
        runtime::rtfree_all();

        auto actual_byte_end = cursor.curFilePos();

#ifndef NDEBUG
        {
            std::stringstream ss;
            ss<<"Read CSV from "<<inputFilePath.toString()<<":"
            <<actual_byte_start<<"-"<<actual_byte_end<<" ("
            <<_rangeStart<<"-"<<_rangeEnd<<") "<<pluralize(rowNumber, "row");
            Logger::instance().defaultLogger().debug(ss.str());
        }
#endif
    }
}