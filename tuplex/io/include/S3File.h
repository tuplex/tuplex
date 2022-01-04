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
    class S3File : public VirtualFile {
    public:
        S3File() = delete;

        S3File(S3FileSystemImpl &fs, const URI &uri, VirtualFileMode mode, Aws::S3::Model::RequestPayer requestPayer) : _s3fs(fs),
                                                                                                VirtualFile::VirtualFile(uri, mode),
                                                                                                _requestPayer(requestPayer) { init(); }

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
        static const size_t _bufferSize = 1024 * 1024 * 32; ///! size of the buffer, set here to 32MB buffer

//        static const size_t _bufferSize = 1024 * 1024 * 5 + 100; ///! for debug reasons, set 5MB + 100B buffer


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
        static_assert(_bufferSize > 5 * 1024 * 1024, "because of part limit buffer should be at least 5MB");

        // for dev purposes, choose smaller size
//        static const size_t _bufferSize = 256; ///! size of the buffer, set here to 32MB buffer
        size_t _bufferPosition; ///! position in the buffer
        size_t _fileSize; ///! lazily set file size from request
        size_t _bufferedAbsoluteFilePosition; ///! global filePosition (for parts request or multipart upload)
        size_t _filePosition; // global filePosition (moved by read/write).

        // variables for uploading files to S3
        void lazyUpload();
        bool _fileUploaded;

        uint16_t _partNumber; // current part number (1 to 10,000 incl). If 0, no multipart upload
        Aws::String _uploadID; // multipart upload ID
        std::vector<Aws::S3::Model::CompletedPart> _parts;
        void initMultiPartUpload();
        void uploadPart(); // uploads current buffer & resets everything
        void completeMultiPartUpload(); // issues complete Upload request

        Aws::S3::Model::RequestPayer _requestPayer;

#ifndef NDEBUG
        // debug
        double _requestTime;
#endif
    };
}


#endif
#endif //TUPLEX_S3FILE_H