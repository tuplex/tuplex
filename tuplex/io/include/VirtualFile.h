//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_VIRTUALFILE_H
#define TUPLEX_VIRTUALFILE_H

#include "IFileSystemImpl.h"
#include "VirtualFileSystemBase.h"

namespace tuplex {
    class VirtualFile;
    class VirtualFileSystem;

    /*!
     * abstract base class for a file system specific Implementation
     */
    class VirtualFile {
    public:
        VirtualFile() : _uri(""), _mode(VFS_READ)   {}
        VirtualFile(const URI& uri, VirtualFileMode mode) : _uri(uri), _mode(mode) {}
        virtual ~VirtualFile() = default;

        /*!
         * (thread-safe!) writes buffer to URI location, immediate persistance
         * @param buffer data buffer, must be non-null
         * @param bufferSize number of bytes to write
         * @return status of write operation
         */
        virtual VirtualFileSystemStatus write(const void* buffer, uint64_t bufferSize) = 0;

        /*!
         * reads up to nbytes bytes towards buffer.
         * @param buffer memory location where to store bytes
         * @param nbytes maximum number of bytes to read
         * @param bytesRead actual bytes read. Always set to at least 0, even in error case
         * @return status of read operation
         */
        virtual VirtualFileSystemStatus read(void* buffer, uint64_t nbytes, size_t *bytesRead=nullptr) const = 0;

        /*!
         * this functions reads only the required bytes without buffering. Might be faster, e.g. for S3 requests
         * @param buffer
         * @param nbytes
         * @param bytesRead
         * @return
         */
        virtual VirtualFileSystemStatus readOnly(void* buffer, uint64_t nbytes, size_t *bytesRead=nullptr) const {
            return read(buffer, nbytes, bytesRead);
        }

        /*!
         * closes file
         * @return status of close operation
         */
        virtual VirtualFileSystemStatus close() = 0;

        /*!
         * move file cursor by delta from current position
         * @param delta how many bytes to move forward (positive) or backward(negative). gets autoclmaped
         * @return if seek was successful
         */
        virtual VirtualFileSystemStatus seek(int64_t delta) = 0;

        /*!
         * returns size in bytes of file
         * @return
         */
        virtual size_t size() const = 0;

        URI getURI() const { return _uri; }

        virtual bool is_open() const = 0;

        /*!
         * indicates whether end-of-file was reached or not.
         * @return
         */
        virtual bool eof() const = 0;
    protected:
        URI                     _uri;
        VirtualFileMode         _mode;
    };
}
#endif //TUPLEX_VIRTUALFILE_H