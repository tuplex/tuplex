//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_S3FILE_H
#define TUPLEX_S3FILE_H
#ifdef BUILD_WITH_AWS

#include "IFileSystemImpl.h"
#include "S3FileSystemImpl.h"
#include <Logger.h>
#include <Base.h>

#include <aws/s3/model/CompletedPart.h>

namespace tuplex {

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
    std::string format_s3_outcome_error_message(const Aws::Utils::Outcome<R, E>& outcome, const std::string& uri="") {

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

    class S3File : public VirtualFile {
    public:
        S3File() = delete;

        S3File(S3FileSystemImpl &fs, const URI &uri, VirtualFileMode mode, Aws::S3::Model::RequestPayer requestPayer) : _s3fs(fs),
                                                                                                VirtualFile::VirtualFile(uri, mode),
                                                                                                _requestPayer(requestPayer), _bufferSize(5 * 1024 * 1024 + 100) {
            // 1024 * 1024 * 5 + 100; ///! for debug reasons, set 5MB + 100B buffer

            // in release mode use larger buffer
#ifdef NDEBUG
           _bufferSize = 1024 * 1024 * 64; ///! size of the buffer, set here to 64MB buffer
#endif

            init();
        }

        virtual ~S3File();

        void open() {}

        VirtualFileSystemStatus write(const void* buffer, uint64_t bufferSize) override;
        VirtualFileSystemStatus read(void* buffer, uint64_t nbytes, size_t* bytesRead) const override;
        VirtualFileSystemStatus readOnly(void* buffer, uint64_t nbytes, size_t *bytesRead) const override;
        VirtualFileSystemStatus close() override;

        VirtualFileSystemStatus seek(int64_t delta) override;


        /*!
         * if in read mode, check whether a request was already made
         * if in write mode, check whether file is flushed or not yet.
         * @return
         */
        bool is_open() const override;

        size_t size() const override;

        bool eof() const override;

        size_t bufferSize() {
            return _bufferSize;
        }

    private:

        /*!
         * requests to fill the buffer with up to bytesToRequest.
         * @param bytesToRequest how many bytes to request. Must be smaller than _bufferSize - _bufferPosition
         * @return number of bytes filled in
         */
        size_t fillBuffer(size_t bytesToRequest);

        void init();
        S3FileSystemImpl& _s3fs;

        uint8_t *_buffer; ///! buffers
        size_t _bufferLength; ///! how many valid bytes are stored in buffer
        size_t _bufferSize;

        // buffer size should be more than 5MB!
        // this is because parts needs to be at least 5MB for multipart upload
        // limits: https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
        // Item	Specification
        // Maximum object size	                                        5 TB
        // Maximum number of parts per upload	                        10,000
        // Part numbers	                                                1 to 10,000 (inclusive)
        // Part size	                                                5 MB to 5 GB, last part can be < 5 MB
        // Maximum number of parts returned                             1000
        // for a list parts request
        // Maximum number of multipart uploads                          1000
        // returned in a list multipart uploads request
        //static_assert(_bufferSize > 5 * 1024 * 1024, "because of part limit buffer should be at least 5MB");

        // for dev purposes, choose smaller size
//        static const size_t _bufferSize = 256; ///! size of the buffer, set here to 32MB buffer
        size_t _bufferPosition; ///! position in the buffer
        size_t _fileSize; ///! lazily set file size from request
        size_t _bufferedAbsoluteFilePosition; ///! global filePosition (for parts request or multipart upload)
        size_t _filePosition; // global filePosition (moved by read/write).

        // variables for uploading files to S3
        void lazyUpload();
        bool _fileUploaded;

        /*!
         * uploads current buffer to S3 (as multipart upload) if full. Also makes sure a buffer of additional space could fit (in any case)
         */
        bool uploadAndResetBufferIfFull(size_t additional_space_required);

        uint16_t _partNumber; // current part number (1 to 10,000 incl). If 0, no multipart upload
        Aws::String _uploadID; // multipart upload ID
        std::vector<Aws::S3::Model::CompletedPart> _parts;
        void initMultiPartUpload();
        bool uploadPart(); // uploads current buffer & resets everything
        void completeMultiPartUpload(); // issues complete Upload request

        Aws::S3::Model::RequestPayer _requestPayer;

//#ifndef NDEBUG
        // debug
        double _requestTime;
//#endif

        template <typename R, typename E>
        std::string outcome_error_message(const Aws::Utils::Outcome<R, E>& outcome, const std::string& uri="") const {
            auto s3_details = format_s3_outcome_error_message(outcome, uri);

            std::stringstream ss;
            ss<<"S3 Filesystem error:\n"
               <<"\tbuf pos: "<<_bufferPosition
               <<"\n\tbuf size: "<<_bufferSize
               <<"\n\tbuf length: "<<_bufferLength
               <<"\n\tfile pos: "<<_filePosition
               <<"\n\tpart no: "<<_partNumber;
            if(_partNumber > 0)
                ss<<"\n\tmultipart id: "<<_uploadID;
            ss<<"\ndetails:\n"<<s3_details;
            return ss.str();
        }
    };
}


#endif
#endif //TUPLEX_S3FILE_H