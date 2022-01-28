//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifdef BUILD_WITH_AWS
#include <S3File.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <boost/interprocess/streams/bufferstream.hpp>
#include <stdexcept>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <StringUtils.h>


#include <Timer.h>

// @TODO: use global allocator!
// ==> make customizable

// from https://github.com/TileDB-Inc/TileDB/blob/dev/tiledb/sm/filesystem/s3.cc
/**
 * Return the exception name and error message from the given outcome object.
 *
 * @tparam R AWS result type
 * @tparam E AWS error type
 * @param outcome Outcome to retrieve error message from
 * @return Error message string
 */
template <typename R, typename E>
std::string outcome_error_message(const Aws::Utils::Outcome<R, E>& outcome, const std::string& uri="") {

    // special case: For public buckets just 403 is emitted, which is hard to decode
    if(outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN) {
        // access issue
        std::stringstream ss;
        ss<<outcome.GetError().GetMessage()<<" - this may be the result of accessing a public bucket with"
                                             " requester pay mode. Set tuplex.aws.requesterPay to true when initializing"
                                             " the context. Also make sure the object in the public repo has a proper"
                                             " ACL set. I.e., to make it publicly available use "
                                             "`aws s3api put-object-acl --bucket <bucket> --key <path> --acl public-read"
                                             " --request-payer requester`";
        return ss.str();
    }

    // improve error messaging:
    std::string aws_exception = outcome.GetError().GetExceptionName().c_str();
    std::string aws_message = outcome.GetError().GetMessage().c_str();

    tuplex::trim(aws_exception);
    tuplex::trim(aws_message);

    std::stringstream ss;
    if(!uri.empty())
        ss<<"S3 error for "<<uri<<" ";
    if(!aws_exception.empty())
        ss<<"Exception "<<aws_exception<<", ";
    if(!aws_message.empty())
        ss<<aws_message;
    else
        ss<<"Unknown AWS error code";

    return ss.str();
}


namespace tuplex {
    void S3File::init() {
        _buffer = nullptr;
        _bufferPosition = 0;
        _bufferLength = 0;
        _fileSize = 0;
        _bufferedAbsoluteFilePosition = 0;
        _filePosition = 0;
        _partNumber = 0; // set to 0

        // S3 files can only operate on read xor write mode
        if(_mode & VirtualFileMode::VFS_WRITE && _mode & VirtualFileMode::VFS_READ)
            throw std::runtime_error("S3 files can't be read/write at the same time");

        _fileUploaded = false;


#ifndef NDEBUG
        //debug:
        _requestTime = 0.0;
#endif
    }

    bool S3File::is_open() const {
        return false;
    }

    void S3File::lazyUpload() {
        assert(_mode & VirtualFileMode::VFS_WRITE || _mode & VirtualFileMode::VFS_OVERWRITE);

        // check if buffer is valid, if so upload via PutRequest
        if(_buffer && !_fileUploaded) {

            // check if multipart upload (_partNumber != 0)
            if(_partNumber > 0) {
                // upload last part
                uploadPart();

                // finish multipart upload
                completeMultiPartUpload();
            } else {
                // simple put request
                // upload via simple putrequest
                Aws::S3::Model::PutObjectRequest put_req;
                put_req.SetBucket(_uri.s3Bucket().c_str());
                put_req.SetKey(_uri.s3Key().c_str());
                put_req.SetContentLength(_bufferLength);
                put_req.SetRequestPayer(_requestPayer);

                auto content_type = _uri.s3GetMIMEType();
                if(!content_type.empty()) {
                    put_req.SetContentType(content_type.c_str());
                }

                // body
                auto stream = std::shared_ptr<Aws::IOStream>(new boost::interprocess::bufferstream((char*)_buffer, _bufferLength));
                put_req.SetBody(stream);

                // perform upload request
                auto outcome = _s3fs.client().PutObject(put_req);
                _s3fs._putRequests++;
                if(!outcome.IsSuccess()) {
                    MessageHandler& logger = Logger::instance().logger("s3fs");
                    auto err_msg = outcome_error_message(outcome, _uri.toString());
                    logger.error(err_msg);
                    throw std::runtime_error(err_msg);
                }
                _s3fs._bytesTransferred += _bufferLength;
            }

            _fileUploaded = true;
        }
    }

    VirtualFileSystemStatus S3File::close() {

        // in write mode?
        if(_mode & VirtualFileMode::VFS_WRITE || _mode & VirtualFileMode::VFS_OVERWRITE)
            lazyUpload();

        return VirtualFileSystemStatus::VFS_OK;
    }

    void S3File::initMultiPartUpload() {
        MessageHandler& logger = Logger::instance().logger("s3fs");


        Aws::S3::Model::CreateMultipartUploadRequest req;
        req.SetBucket(_uri.s3Bucket().c_str());
        req.SetKey(_uri.s3Key().c_str());
        req.SetRequestPayer(_requestPayer);
        auto content_type = _uri.s3GetMIMEType();
        if(!content_type.empty()) {
            req.SetContentType(content_type.c_str());
        }

        auto outcome = _s3fs.client().CreateMultipartUpload(req);
        _s3fs._multiPartPutRequests++;
        // count as put request

        if(!outcome.IsSuccess()) {
            auto err_msg = outcome_error_message(outcome, _uri.toString());
            logger.error(err_msg);
            throw std::runtime_error(err_msg);
        }

        _uploadID = outcome.GetResult().GetUploadId();
        // use this to find out the abort date, issue warning if it is more than 3 days in the future!
        // outcome.GetResult().GetAbortDate()
        auto abort_date = outcome.GetResult().GetAbortDate();
        auto now_data = Aws::Utils::DateTime::Now();
        auto time_diff = Aws::Utils::DateTime::Diff(abort_date, now_data);
        std::chrono::hours warningThreshold{24 * 3}; // warn after 3 days
        if(time_diff > std::chrono::duration_cast<std::chrono::milliseconds>(warningThreshold)) {
            logger.warn(std::string("multipart upload requests will expire earliest on ") +
            abort_date.ToLocalTimeString(Aws::Utils::DateFormat::ISO_8601).c_str());
        }

        _partNumber = 1; // set part number to 1 (first allowed AWS value)
    }


    void S3File::uploadPart() {
        MessageHandler& logger = Logger::instance().logger("s3fs");

        assert(_partNumber > 0); // if this is zero, need to all init before!
        assert(_buffer);

        // skip empty buffer for second time
        if(_bufferLength == 0 && _partNumber > 1)
            return;

        Aws::S3::Model::UploadPartRequest req;
        //@Todo: what about content MD5???
        req.SetBucket(_uri.s3Bucket().c_str());
        req.SetKey(_uri.s3Key().c_str());
        req.SetUploadId(_uploadID);
        req.SetPartNumber(_partNumber);
        req.SetContentLength(_bufferLength);
        req.SetRequestPayer(_requestPayer);

        auto stream = std::shared_ptr<Aws::IOStream>(new boost::interprocess::bufferstream((char*)_buffer, _bufferLength));
        req.SetBody(stream);

        auto outcome = _s3fs.client().UploadPart(req);
        _s3fs._multiPartPutRequests++;
        _s3fs._bytesTransferred += _bufferLength;
        if(!outcome.IsSuccess()) {
            auto err_msg = outcome_error_message(outcome, _uri.toString());
            logger.error(err_msg);
            throw std::runtime_error(err_msg);
        }

        // record upload
        Aws::S3::Model::CompletedPart completed_part;
        completed_part.SetETag(outcome.GetResult().GetETag());
        completed_part.SetPartNumber(_partNumber);
        _parts.emplace_back(completed_part);

        // reset buffer
        _bufferPosition = 0;
        _bufferLength = 0;
        _partNumber++;
    }

    void S3File::completeMultiPartUpload() {
        // use this here to list open multipart upload requests
        //aws s3api list-multipart-uploads --bucket <bucket name>

        MessageHandler& logger = Logger::instance().logger("s3fs");

        // issue complete upload request
        Aws::S3::Model::CompleteMultipartUploadRequest req;
        req.SetBucket(_uri.s3Bucket().c_str());
        req.SetKey(_uri.s3Key().c_str());
        req.SetUploadId(_uploadID);
        req.SetRequestPayer(_requestPayer);

        Aws::S3::Model::CompletedMultipartUpload upld;
        for(auto part : _parts)
            upld.AddParts(part);

        req.SetMultipartUpload(std::move(upld));

        auto outcome = _s3fs.client().CompleteMultipartUpload(req);
        _s3fs._closeMultiPartUploadRequests++;
        if(!outcome.IsSuccess()) {
            auto err_msg = outcome_error_message(outcome, _uri.toString());
            logger.error(err_msg);
            throw std::runtime_error(err_msg);
        }
    }

    VirtualFileSystemStatus S3File::write(const void *buffer, uint64_t bufferSize) {

        // make sure file is not yet uploaded
        if(_fileUploaded) {
            throw std::runtime_error("file has been already uploaded. Did you call write after close?");
        }

        // two options: either buffer is empty OR full
        if(!_buffer) {
            // allocate new buffer with size & fill up with data
            _buffer = new uint8_t[_bufferSize];
            memcpy(_buffer, buffer, bufferSize);
            _bufferLength += bufferSize;
            _bufferPosition += bufferSize;
        } else {
            // two cases: there is enough space left in buffer or buffer might run full
            if(_bufferSize >= bufferSize + _bufferLength) {
                memcpy(_buffer + _bufferPosition, buffer, bufferSize);
                _bufferPosition += bufferSize;
                _bufferLength += bufferSize;
            } else {
                // need to do multipart upload!

                // check if multipart was already initiated
                if(0 == _partNumber) {
                    // init multipart upload and upload first part
                    initMultiPartUpload();

                    // check if limit of 10,000 was reached. If so, abort!
                    uploadPart();
                } else {
                    // append another multipart upload part
                    uploadPart();
                }
            }

            return VirtualFileSystemStatus::VFS_NOTYETIMPLEMENTED;
        }

        return VirtualFileSystemStatus::VFS_OK;
    }


    // fast tiny read (do not advance internal pointers)
    VirtualFileSystemStatus S3File::readOnly(void *buffer, uint64_t nbytes, size_t *bytesRead) const {

        // short cut for empty read
        if(nbytes == 0) {
            if(bytesRead)
                *bytesRead = 0;
            return VirtualFileSystemStatus::VFS_OK;
        }

        // shortcut: is buffer filled and nbytes available?
        // --> no need to query again!
        if(_buffer && _bufferPosition + nbytes <= _bufferLength) {
            memcpy(buffer, _buffer + _bufferPosition, nbytes);
            if(bytesRead)
                *bytesRead = nbytes;
            return VirtualFileSystemStatus::VFS_OK;
        }

        // check if file size has been queried/filled.
        // --> required to clamp request to avoid invalid range!
        size_t fileSize = _fileSize;
        if(!_buffer && fileSize == 0) {
            // ==> fill in file size
            fileSize = s3GetContentLength(this->_s3fs.client(), this->_uri);
        }

        // clamp nbytes
        if(_filePosition + nbytes > fileSize) {
            nbytes = fileSize - _filePosition;
        }

        // simply issue here one direct request
        size_t retrievedBytes = 0;
        // range header
        std::string range = "bytes=" + std::to_string(_filePosition) + "-" + std::to_string(_filePosition + nbytes - 1);
        // make AWS S3 part request to uri
        // check how to retrieve object in poarts
        Aws::S3::Model::GetObjectRequest req;
        req.SetBucket(_uri.s3Bucket().c_str());
        req.SetKey(_uri.s3Key().c_str());
        // retrieve byte range according to http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        req.SetRange(range.c_str());
        req.SetRequestPayer(_requestPayer);

        // Get the object ==> Note: this s3 client is damn slow, need to make it faster in the future...
        auto get_object_outcome = _s3fs.client().GetObject(req);
        _s3fs._getRequests++;

        if (get_object_outcome.IsSuccess()) {
            auto result = get_object_outcome.GetResultWithOwnership();

            // extract extracted byte range + size
            // syntax is: start-inclend/fsize
            auto cr = result.GetContentRange();
            auto idxSlash = cr.find_first_of('/');
            auto idxMinus = cr.find_first_of('-');
            // these are kind of weird, they are already requested range I presume
            size_t fileSize = std::strtoull(cr.substr(idxSlash + 1).c_str(), nullptr, 10);
            retrievedBytes = result.GetContentLength();

            // Get an Aws::IOStream reference to the retrieved file
            auto &retrieved_file = result.GetBody();
            // copy contents
            retrieved_file.read((char*)buffer, retrievedBytes);

            // note: for ascii files there might be an issue regarding the file ending!!!
            _s3fs._bytesReceived += retrievedBytes;
        } else {
            MessageHandler& logger = Logger::instance().logger("s3fs");
            auto err_msg = outcome_error_message(get_object_outcome, _uri.toString());
            logger.error(err_msg);
            throw std::runtime_error(err_msg);
        }

        if(bytesRead)
            *bytesRead = retrievedBytes;

        return VirtualFileSystemStatus::VFS_OK;
    }


    VirtualFileSystemStatus S3File::read(void *buffer, uint64_t nbytes, size_t* outBytesRead) const {
        assert(buffer);

        // empty buffer? => fill!
        if(!_buffer)
            const_cast<S3File*>(this)->fillBuffer(_bufferSize); // try to request full buffer

        // check how many bytes are available in buffer
        assert(_bufferPosition <= _bufferLength);
        size_t bytesAvailable = _bufferLength - _bufferPosition;
        size_t bytesRead = 0;
        assert(_buffer);

        uint8_t* dest = (uint8_t*)buffer;

        // Todo: better condition is I think filePos < fileSize
        int64_t capacity = nbytes; // how many bytes can be written to buffer safely

        // bring capacity to 0
        while(capacity > 0) {
            // there are more bytesAvailable than requested (capacity) => consume capacity
            if(capacity <= bytesAvailable) {
                memcpy(dest, _buffer + _bufferPosition, capacity);
                bytesRead += capacity;
                const_cast<S3File*>(this)->_bufferPosition += capacity;
                capacity = 0;
            } else {
                // there are less bytesAvailable than the capacity still to fill,
                // fill whatever is there & decrease capacity to fill by it
                memcpy(dest, _buffer + _bufferPosition, bytesAvailable);
                bytesRead +=  bytesAvailable; // move how many bytes were read
                dest += bytesAvailable; // move position where to copy things
                capacity -= bytesAvailable; // decrease capacity
                const_cast<S3File*>(this)->_bufferPosition += bytesAvailable; // move buffer to end (necessary to avoid infinity loop)

                // now there are two options: 1) file already exhausted, no need to refill
                // 2) still data left, refill buffer
                if(_bufferedAbsoluteFilePosition >= _fileSize) // exhausted, leave loop
                    break;

                // fill buffer up again
                // reset buffer pos & length (i.e. invalidate buffer)
                const_cast<S3File*>(this)->_bufferPosition = 0;
                const_cast<S3File*>(this)->_bufferLength = 0;
                const_cast<S3File*>(this)->fillBuffer(_bufferSize); // try to request full buffer
                bytesAvailable = _bufferLength - _bufferPosition;

                assert(bytesAvailable > 0);
            }


            assert(capacity >= 0);
        }

        // output if desired
        if(outBytesRead)
            *outBytesRead = bytesRead;

        return VirtualFileSystemStatus::VFS_OK;
    }


    size_t S3File::fillBuffer(size_t bytesToRequest) {
        bytesToRequest = std::min(_bufferSize - _bufferPosition, bytesToRequest);
        size_t retrievedBytes = 0;

        if(0 == bytesToRequest)
            return 0;

        // create buffer if not existing
        if(!_buffer) {
            _buffer = new uint8_t[_bufferSize];

            _bufferPosition = 0;
            _bufferLength = 0;
            _fileSize = 0;
        } else {
            // shortcut: if eof reached, then do not perform request
            if(_bufferedAbsoluteFilePosition >= _fileSize)
                return 0;
        }

        // range header

        // make sure file size is not 0
        if(_fileSize == 0 && !_buffer)
            _fileSize = s3GetContentLength(_s3fs.client(), _uri);

        size_t range_end = std::min(_bufferedAbsoluteFilePosition + bytesToRequest - 1, _fileSize - 1);
        std::string range = "bytes=" + std::to_string(_bufferedAbsoluteFilePosition) + "-" + std::to_string(range_end);
        // make AWS S3 part request to uri
        // check how to retrieve object in poarts
        Aws::S3::Model::GetObjectRequest req;
        req.SetBucket(_uri.s3Bucket().c_str());
        req.SetKey(_uri.s3Key().c_str());
        // retrieve byte range according to http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        req.SetRange(range.c_str());
        req.SetRequestPayer(_requestPayer);

        Timer timer;
        // Get the object
        // std::cout<<">> S3 read request..."; std::cout.flush();
        auto get_object_outcome = _s3fs.client().GetObject(req);
        // std::cout<<" done!"<<std::endl;
        _s3fs._getRequests++;
#ifndef NDEBUG
        _requestTime += timer.time();
#endif
        if (get_object_outcome.IsSuccess()) {
            auto result = get_object_outcome.GetResultWithOwnership();

            // extract extracted byte range + size
            // syntax is: start-inclend/fsize
            auto cr = result.GetContentRange();
            auto idxSlash = cr.find_first_of('/');
            auto idxMinus = cr.find_first_of('-');
            // these are kind of weird, they are already requested range I presume
            // size_t rangeStart = std::strtoull(cr.substr(0, idxMinus).c_str(), nullptr, 10);
            // size_t rangeEnd = std::strtoull(cr.substr(idxMinus + 1, idxSlash).c_str(), nullptr, 10);
            size_t fileSize = std::strtoull(cr.substr(idxSlash + 1).c_str(), nullptr, 10);
            retrievedBytes = result.GetContentLength();
            _fileSize = fileSize;

            // Get an Aws::IOStream reference to the retrieved file
            auto &retrieved_file = result.GetBody();
            // copy contents & move cursors
            assert(_bufferPosition + retrievedBytes <= _bufferSize);
            retrieved_file.read((char*)(_buffer + _bufferPosition), retrievedBytes);
            _bufferLength += retrievedBytes;
            _bufferedAbsoluteFilePosition += retrievedBytes;

            // in bounds check
            assert(_bufferPosition + _bufferLength <= _bufferSize);

            // note: for ascii files there might be an issue regarding the file ending!!!
            _s3fs._bytesReceived += retrievedBytes;
        } else {
            MessageHandler& logger = Logger::instance().logger("s3fs");
            auto err_msg = outcome_error_message(get_object_outcome, _uri.toString());
            logger.error(err_msg);
            throw std::runtime_error(err_msg);
        }
        return retrievedBytes;
    }

    size_t S3File::size() const {

        // check if buffer empty, if so fill initially
        if(!_buffer)
            // hack: use lazy buffer for requests
            const_cast<S3File*>(this)->fillBuffer(_bufferSize); // try to request full buffer

        // after the first time fillBuffer was called, fileSize is populated
        return _fileSize;
    }

    S3File::~S3File() {
        close();

        if(_buffer)
            delete [] _buffer;
        _buffer = nullptr;

        // // print
        // std::cout<<"request Time on "<<_uri.toPath()<<": "<<_requestTime<<"s "<<std::endl;
    }

    bool S3File::eof() const {
        // note that buffer must be initialized, even for empty files!
        // when is end of file reached?
        // buffer is filled, filePos == fileSize and _bufferPosition reached buffer Length
        return _buffer &&
               _bufferedAbsoluteFilePosition == _fileSize &&
               _bufferLength == _bufferPosition;
    }

    VirtualFileSystemStatus S3File::seek(int64_t delta) {

        // new file pos (clamp)
        // is file size known?
        if(!_buffer && _fileSize == 0) { // not 100% correct, but we can live with additional request for empty files...
            _fileSize = s3GetContentLength(_s3fs.client(), _uri);
        }

        // clamp delta
        int64_t curPos = _filePosition;
        int64_t newPos = curPos + delta;
        if(newPos < 0)
            newPos = 0;
        if(newPos > _fileSize)
            newPos = _fileSize;
        delta = newPos - curPos;
        if(0 == delta)
            return VirtualFileSystemStatus::VFS_OK;

        // check if buffer is valid
        if(_buffer) {
            // can delta be consumed by moving buffer pos only?
            if(delta < 0) {
                if(_bufferPosition >= std::abs(delta)) {
                    _bufferPosition += delta;
                    _filePosition += delta;
                    return VirtualFileSystemStatus::VFS_OK;
                } else {
                    // need to move back more bytes -> i.e. request new buffer!
                    _bufferPosition = 0;
                    _filePosition += delta;
                    _bufferedAbsoluteFilePosition += delta;
                    fillBuffer(_bufferSize);
                }
            } else {
                if(_bufferPosition + delta <= _bufferLength) {
                    _bufferPosition += delta;
                    _filePosition += delta;
                    return VirtualFileSystemStatus::VFS_OK;
                } else {
                    // need to move forward more bytes -> i.e. request new buffer!
                    _bufferPosition = 0;
                    _filePosition += delta;
                    _filePosition += delta;
                    _bufferedAbsoluteFilePosition += delta;
                    fillBuffer(_bufferSize);
                }
            }
        } else {
           // no buffer, so move both fileposition and buffered pos
           _bufferPosition = 0;
           _bufferedAbsoluteFilePosition += delta;
           _filePosition += delta;
        }

        return VirtualFileSystemStatus::VFS_OK;
    }
}

#endif