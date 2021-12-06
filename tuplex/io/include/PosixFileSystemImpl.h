//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_POSIXFILESYSTEMIMPL_H
#define TUPLEX_POSIXFILESYSTEMIMPL_H

#include "IFileSystemImpl.h"

// 64 KB internal io buffer size
// note: buffer size sould be multiple of 512 bytes + 8 bytes for hash table
// according to https://www.drdobbs.com/using-lib-c-and-io-and-performance/199101391
#define POSIX_IOBUF_SIZE ((64*1024)+8)

namespace tuplex {
    class PosixFileSystemImpl : public IFileSystemImpl {
    private:
        bool validPrefix(const URI& uri);

        /*!
         * C-API file operations for speed
         */
        class PosixFile : public VirtualFile {
        private:
            FILE *_fh;

            // internal buffer
            char *_buf;
        public:
            PosixFile() : _fh(nullptr), _buf(nullptr)  {}
            PosixFile(const URI& uri,
                      VirtualFileMode mode) : VirtualFile::VirtualFile(uri, mode),
                                              _fh(nullptr), _buf(nullptr) { open(); }

            ~PosixFile() override { PosixFile::close(); }

            void open();
            VirtualFileSystemStatus write(const void* buffer, uint64_t bufferSize) override;
            VirtualFileSystemStatus read(void* buffer, uint64_t nbytes, size_t* bytesRead) const override;
            VirtualFileSystemStatus close() override;
            bool is_open() const override { return _fh != nullptr; }

            size_t size() const override;

            bool eof() const override;

            VirtualFileSystemStatus seek(int64_t delta) override;
        };

        /*!
         * Posix mmap functionality
         */
        class PosixMappedFile : public VirtualMappedFile {
        private:
            uint8_t *_start_ptr;
            uint8_t *_end_ptr;
            uint8_t *_guarded_start_ptr;
            uint8_t *_guard_page_ptr;
            size_t _page_size;

            bool _usesMemoryMapping;

            bool mapMemory();

            bool readToMemory();
        public:
            PosixMappedFile() : _start_ptr(nullptr),
                                _end_ptr(nullptr),
                                _guarded_start_ptr(nullptr),
                                _guard_page_ptr(nullptr),
                                _page_size(0),
                                _usesMemoryMapping(false) {}
            PosixMappedFile(const URI& uri) : VirtualMappedFile::VirtualMappedFile(uri),
                                              _start_ptr(nullptr),
                                              _end_ptr(nullptr),
                                              _guarded_start_ptr(nullptr),
                                              _guard_page_ptr(nullptr),
                                              _page_size(0),
                                              _usesMemoryMapping(false) { open(); }

            ~PosixMappedFile() override { PosixMappedFile::close(); }

            void open();

            uint8_t* getStartPtr() override { return _start_ptr; }
            uint8_t* getEndPtr() override { return _end_ptr; }

            VirtualFileSystemStatus close() override;
            bool is_open() const override { return _start_ptr != nullptr; }
        };
    public:
        VirtualFileSystemStatus create_dir(const URI& uri) override;
        VirtualFileSystemStatus remove(const URI& uri) override;

        // how to design this? better with smart pointer? --> probably
        // make VirtualFile an abstract class then...
        std::unique_ptr<VirtualFile> open_file(const URI& uri, VirtualFileMode vfm) override;
        VirtualFileSystemStatus touch(const URI& uri, bool overwrite) override;
        VirtualFileSystemStatus file_size(const URI& uri, uint64_t& size) override;
        VirtualFileSystemStatus ls(const URI& parent, std::vector<URI>& uris) override;
        std::unique_ptr<VirtualMappedFile> map_file(const URI &uri) override;
        std::vector<URI> glob(const std::string& pattern) override;
        static VirtualFileSystemStatus copySingleFile(const URI& src, const URI& target, bool overwrite=true);

        // use this...
        // or https://linux.die.net/man/3/ftw
        // https://www.bfilipek.com/2019/04/dir-iterate.html#from-cposix
        /*!
         * returns all files within a folder/under a dir
         * @param dir
         * @return URIs to files
         */
        static std::vector<URI> expandFolder(const URI& dir);

    };
}
#endif //TUPLEX_POSIXFILESYSTEMIMPL_H