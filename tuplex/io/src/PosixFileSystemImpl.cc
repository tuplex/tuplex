//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <PosixFileSystemImpl.h>
#include <cassert>
#include <MessageHandler.h>
#include <Logger.h>
#include <boost/system/error_code.hpp>
#include <boost/filesystem/operations.hpp>
#include <Utils.h>
#include <sys/mman.h>
#include <glob.h>
#include <stdexcept>
#include <sys/stat.h>
#include <dirent.h>

#ifdef LINUX
// use cstdio extensions to disable locking on FILE streams
#include <stdio_ext.h>
#include <sys/sendfile.h>
#else
// MacOS
#include <copyfile.h>
#endif

#include <FileUtils.h>
#include <dirent.h>

// Note: could improve performance prob further by
// just using native POSIX calls. they're faster...

namespace tuplex {

    bool PosixFileSystemImpl::validPrefix(const URI &uri) {
        // no prefix or file:// are valid prefixes for this file system
        auto prefix = uri.prefix();
        return prefix.length() == 0 || prefix == "file://";
    }

    VirtualFileSystemStatus PosixFileSystemImpl::create_dir(const URI &uri) {
        MessageHandler& logger = Logger::instance().logger("posix filesystem");

        if(!validPrefix(uri))
            return VirtualFileSystemStatus::VFS_INVALIDPREFIX;

        // if path exists, no error
        if(uri.exists())
            return VirtualFileSystemStatus::VFS_OK;

        // create dir via boost
        boost::system::error_code ec;
        boost::filesystem::create_directories(uri.toPath(), ec);
        if(ec) {
            logger.error(ec.message());
            return VirtualFileSystemStatus::VFS_IOERROR;
        }

        return VirtualFileSystemStatus::VFS_OK;
    }

    VirtualFileSystemStatus PosixFileSystemImpl::remove(const URI &uri) {
        MessageHandler& logger = Logger::instance().logger("posix filesystem");

        if(!validPrefix(uri))
            return VirtualFileSystemStatus::VFS_INVALIDPREFIX;

        // remove via boost
        boost::system::error_code ec;
        boost::filesystem::remove_all(uri.toPath(), ec);
        if(ec) {
            logger.error(ec.message());
            return VirtualFileSystemStatus::VFS_IOERROR;
        }

        return VirtualFileSystemStatus::VFS_OK;
    }


    VirtualFileSystemStatus PosixFileSystemImpl::file_size(const URI &uri, uint64_t &size) {
        MessageHandler& logger = Logger::instance().logger("posix filesystem");

        if(!validPrefix(uri))
            return VirtualFileSystemStatus::VFS_INVALIDPREFIX;

        if(!uri.exists()) {
            size = 0;
            return VirtualFileSystemStatus::VFS_FILENOTFOUND;
        }

        // check via boost
        boost::system::error_code ec;
        size = 0;
        auto file_size = boost::filesystem::file_size(uri.toPath(), ec);
        if(ec) {
            logger.error(ec.message());
            return VirtualFileSystemStatus::VFS_IOERROR;
        }
        size = file_size;
        return VirtualFileSystemStatus::VFS_OK;
    }

    VirtualFileSystemStatus PosixFileSystemImpl::ls(const URI &parent, std::vector <URI>& uris) {
        MessageHandler& logger = Logger::instance().logger("posix filesystem");

        // extract local path (i.e. remove file://)
        auto local_path = parent.toString().substr(parent.prefix().length());

        // make sure no wildcard is present yet --> not supported!
        auto prefix = findLongestPrefix(local_path);

        if(prefix.length() !=local_path.length()) {
            logger.error("path " + local_path + " contains Unix wildcard characters, not supported yet");
            return VirtualFileSystemStatus::VFS_IOERROR;
        }

        // use dirent to read dir...
        auto dir_it = opendir(local_path.c_str());
        if(!dir_it)
            return VirtualFileSystemStatus::VFS_IOERROR;
        struct dirent *file_entry = nullptr;
        while((file_entry = readdir(dir_it))) {
            // remove duplicate / runs?
            auto path = eliminateSeparatorRuns(local_path + "/" + file_entry->d_name + (file_entry->d_type == DT_DIR ? "/" : ""));
            URI uri("file://" + path); // no selection of type etc. (Link/dir/...)

            // exclude . and ..
            if(strcmp(file_entry->d_name, ".") == 0 || strcmp(file_entry->d_name, "..") == 0)
                continue;
            uris.push_back(uri);
        }
        closedir(dir_it);

        return VirtualFileSystemStatus::VFS_OK;
    }

    VirtualFileSystemStatus PosixFileSystemImpl::touch(const URI &uri, bool overwrite) {
        if(!validPrefix(uri))
            return VirtualFileSystemStatus::VFS_INVALIDPREFIX;

        if(uri.exists() && !overwrite) {
            return VirtualFileSystemStatus::VFS_FILEEXISTS;
        }

        auto mode = overwrite ? VFS_OVERWRITE | VFS_WRITE : VFS_WRITE;
        if(!open_file(uri, mode))
            return VirtualFileSystemStatus::VFS_IOERROR;


        return VirtualFileSystemStatus::VFS_OK;
    }

    // cf. https://herbsutter.com/2013/05/29/gotw-89-solution-smart-pointers/
    std::unique_ptr<VirtualFile> PosixFileSystemImpl::open_file(const URI &uri, VirtualFileMode vfm) {
        MessageHandler& logger = Logger::instance().logger("posix filesystem");

        // check whether parent dir exists, if not create dirs!
        auto parent_path = parentPath(uri.toPath());
        if((VirtualFileMode::VFS_WRITE == vfm || VirtualFileMode::VFS_OVERWRITE == vfm) &&
        !fileExists(parent_path)) {
            logger.debug("parent dir " + parent_path + " does not exist, creating it.");
            create_dir(parent_path);
            assert(isDirectory(parent_path));
        }

        std::unique_ptr<VirtualFile> ptr(new PosixFile(uri, vfm));
        auto file = (PosixFile*)ptr.get();

        if(!file) {
            logger.error("could not open file at location " + uri.toString());
            return nullptr;
        }

        assert(file);
        if(!file->is_open()) {
            logger.error("file not open at location " + uri.toString());
            return nullptr;
        }

        return ptr;
    }


    void PosixFileSystemImpl::PosixFile::open() {
        MessageHandler& logger = Logger::instance().logger("posix filesystem");

        assert(!_fh);

        auto path = _uri.toString().substr(_uri.prefix().length());
        std::string mode = "";
        if(_mode & VFS_READ)
            mode = "rb";
        else if(_mode & VFS_WRITE || _mode & VFS_OVERWRITE) {
            mode = "wb";
//            if(_mode & VFS_OVERWRITE)
//                mode = "wb";
//            else
//                mode = "rb+"; // @Todo: doesn;t work for /tmp?
        }
        else if(_mode & VFS_APPEND && _mode & VFS_WRITE)
            mode = "ab";
        else if(_mode & VFS_APPEND && _mode & VFS_READ && _mode && VFS_WRITE)
            mode = "a+";
        else {
            _fh = nullptr;
            logger.error("unknown mode or illegal combination encountered");
            return;
        }

        _fh = fopen(path.c_str(), mode.c_str());

        if(_fh) {
            // set buf size
            _buf = (char*)malloc(POSIX_IOBUF_SIZE);
            if(_buf) {
                setvbuf(_fh, _buf, _IOFBF, POSIX_IOBUF_SIZE);
            }

#ifdef LINUX
            // unlock file handle on GNU/Linux
        __fsetlocking (_fh, FSETLOCKING_BYCALLER); // in stdio.h, linux extension. Avoid locking.
#endif
        }
    }

    VirtualFileSystemStatus PosixFileSystemImpl::PosixFile::write(const void *buffer, uint64_t bufferSize) {
        if(!_fh)
            return VirtualFileSystemStatus::VFS_IOERROR;

        assert(buffer);

#ifdef LINUX
        auto bytesWritten = fwrite_unlocked(buffer, 1, bufferSize, _fh);
#else
        auto bytesWritten = fwrite(buffer, 1, bufferSize, _fh);
#endif

        return  bytesWritten == bufferSize ? VirtualFileSystemStatus::VFS_OK
                                                  : VirtualFileSystemStatus::VFS_IOERROR;
    }

    VirtualFileSystemStatus PosixFileSystemImpl::PosixFile::read(void *buffer, uint64_t nbytes,
                                                                 size_t* outBytesRead) const {
        if(!_fh)
            return VirtualFileSystemStatus::VFS_IOERROR;

        assert(buffer);

#ifdef LINUX
        size_t bytesRead = fread_unlocked(buffer, 1, nbytes, _fh);
#else
        size_t bytesRead = fread(buffer, 1, nbytes, _fh);
#endif

        if(outBytesRead)
            *outBytesRead = bytesRead;

        // ok, if up to nbytes are returned
        return  bytesRead <= nbytes ? VirtualFileSystemStatus::VFS_OK
                                           : VirtualFileSystemStatus::VFS_IOERROR;
    }

    bool PosixFileSystemImpl::PosixFile::eof() const {
#ifdef LINUX
        return feof_unlocked(_fh);
#else
        return feof(_fh);
#endif
    }
    VirtualFileSystemStatus PosixFileSystemImpl::PosixFile::close() {
        if(_fh)
            fclose(_fh);
        _fh = nullptr;

        if(_buf) {
            free(_buf);
            _buf = nullptr;
        }

        return VirtualFileSystemStatus::VFS_OK;
    }

    size_t PosixFileSystemImpl::PosixFile::size() const {
        uint64_t _size_ = 0;
        // supress warning
        VirtualFileSystem::fromURI(_uri).file_size(_uri, _size_);
        return _size_;
    }

    std::unique_ptr<VirtualMappedFile> PosixFileSystemImpl::map_file(const URI &uri) {
        std::unique_ptr<VirtualMappedFile> ptr(new PosixMappedFile(uri));
        auto file = (PosixMappedFile*)ptr.get();
        assert(file);
        if(!file->is_open()) {
            //delete file;
            return nullptr;
        }

        return ptr;
    }

    void PosixFileSystemImpl::PosixMappedFile::open() {
        MessageHandler& logger = Logger::instance().logger("posix filesystem");
        // try to memory map file,
        // if this fails default by loading whole file to some memory region
        _usesMemoryMapping = mapMemory();

        if(!_usesMemoryMapping) {
            logger.warn("Memory mapping failed, defaulting to default io read/write. Loading full file to memory.");

            // allocate memory
            try {
                readToMemory();
            } catch(std::bad_alloc& ba) {
                logger.warn(std::string("Could not allocate memory for file. Details: ") + ba.what());
            }
        }
    }

    bool PosixFileSystemImpl::PosixMappedFile::readToMemory() {
        MessageHandler& logger = Logger::instance().logger("posix filesystem");

        auto path = _uri.toString().substr(_uri.prefix().length());
        int fd = ::open(path.c_str(), O_RDONLY);
        if(-1 == fd) {
            logger.error(std::string("Could not open file. Details: ")
                         + strerror(errno));
            return false;
        }

        struct stat st;
        if(fstat(fd, &st) == -1) {
            ::close(fd);
            logger.error(std::string("Could not get file statistics. Details: ")
                         + strerror(errno));
            return false;
        }

        _page_size = static_cast<unsigned long>(sysconf(_SC_PAGE_SIZE));
        unsigned long page_mask = _page_size - 1;

        // rounded size to 16 bytes for faster instructions
        size_t rounded = (st.st_size & page_mask)
                         ? ((st.st_size & ~page_mask) + _page_size)
                         : static_cast<unsigned long>(st.st_size);

        assert(st.st_size <= rounded);


        // alloc memory, don't use new here. malloc is safer for larger memory regions if allocation
        // fails.
        _start_ptr = (uint8_t*)malloc(rounded);
        if(!_start_ptr) {
            logger.error("could not load full file to memory.");
            return false;
        }

        memset(_start_ptr, 0, rounded);
        _end_ptr = _start_ptr + st.st_size;

        // read to memory via PosixFile
        PosixFile pf(_uri, VFS_READ);

        size_t bytesRead = 0;
        pf.read(_start_ptr, st.st_size, &bytesRead);

        // failed?
        if(bytesRead != st.st_size) {
            std::stringstream ss;
            ss<<"Read "<<bytesRead<<" bytes, but expected to read "<<st.st_size<<" bytes."
              <<"Number of bytes read does not match with size of file, deleting memory."
              <<" Did not succeed in mapping file to memory.";
            logger.error(ss.str());
            free(_start_ptr);
            _start_ptr = nullptr;
            _end_ptr = nullptr;
            return false;
        }

        return true;
    }

    bool PosixFileSystemImpl::PosixMappedFile::mapMemory() {
        MessageHandler& logger = Logger::instance().logger("posix filesystem");

        auto path = _uri.toString().substr(_uri.prefix().length());
        int fd = ::open(path.c_str(), O_RDONLY);
        if(-1 == fd) {
            logger.error(std::string("Could not open file. Details: ")
                                                  + strerror(errno));
            return false;
        }

        struct stat st;
        if(fstat(fd, &st) == -1) {
            ::close(fd);
            logger.error(std::string("Could not get file statistics. Details: ")
                                                  + strerror(errno));
            return false;
        }

        // from csvmonkey
        // UNIX sucks. We can't use MAP_FIXED to ensure a guard page appears
        // after the file data because it'll silently overwrite mappings for
        // unrelated stuff in RAM (causing bizarro unrelated errors and
        // segfaults). We can't rely on the kernel's map placement behaviour
        // because it varies depending on the size of the mapping (guard page
        // ends up sandwiched between .so mappings, data file ends up at bottom
        // of range with no space left before first .so). We can't parse
        // /proc/self/maps because that sucks and is nonportable and racy. We
        // could use random addresses pumped into posix_mem_offset() but that
        // is insane and likely slow and non-portable and racy.
        //
        // So that leaves us with: make a MAP_ANON mapping the size of the
        // datafile + the guard page, leaving the kernel to pick addresses,
        // then use MAP_FIXED to overwrite it. We can't avoid the MAP_FIXED
        // since there would otherwise be a race between the time we
        // mmap/munmap to find a usable address range, and another thread
        // performing the same operation. So here we exploit crap UNIX
        // semantics to avoid a race.

        _page_size = static_cast<unsigned long>(sysconf(_SC_PAGE_SIZE));
        unsigned long page_mask = _page_size - 1;

        size_t rounded = (st.st_size & page_mask)
                         ? ((st.st_size & ~page_mask) + _page_size)
                         : static_cast<unsigned long>(st.st_size);

#ifndef NDEBUG
        // logger info:
        logger.info("mapping file (" + std::to_string(st.st_size) + " bytes) to memory (" + std::to_string(rounded) + " bytes)");
        assert(rounded >= st.st_size);

        // @Todo: should memset to 0 the guard page...
#endif

        // memory map file and get start pointer
        _guarded_start_ptr = (uint8_t*)mmap(nullptr, rounded + _page_size, PROT_READ,
                                            MAP_ANON|MAP_PRIVATE, 0, 0);

        if(!_guarded_start_ptr) {
            ::close(fd);
            logger.error(std::string("Could not get memory map file. Details: ")
                                                  + strerror(errno));
            return false;
        }

        _guard_page_ptr = _guarded_start_ptr + rounded;

        _start_ptr = (uint8_t *)mmap(_guarded_start_ptr, static_cast<unsigned long>(st.st_size), PROT_READ,
                                     MAP_SHARED|MAP_FIXED, fd, 0);
        ::close(fd);

        // check if guard page was successful
        if(_start_ptr != _guarded_start_ptr) {
            std::stringstream ss;
            ss<<"memory mapping error, could not place data below guard page "
              <<_guard_page_ptr<<" at "<<_guarded_start_ptr
              <<", got "<<_start_ptr;
            Logger::instance().logger("io").error(ss.str());
            return false;
        }

        madvise(_start_ptr, st.st_size, MADV_SEQUENTIAL);
        madvise(_start_ptr, st.st_size, MADV_WILLNEED);
        _end_ptr = _start_ptr + st.st_size;

        return true;
    }

    VirtualFileSystemStatus PosixFileSystemImpl::PosixMappedFile::close() {
        // unmap memory region
        if(_usesMemoryMapping) {
            // unmap both pointers
            if(_start_ptr)
                ::munmap(_start_ptr, _end_ptr - _start_ptr);

            if(_guard_page_ptr)
                ::munmap(_guard_page_ptr, _page_size);

        } else {
            // delete memory
            if(_start_ptr)
                free(_start_ptr);
        }
        // reset all values
        _start_ptr = nullptr;
        _end_ptr = nullptr;
        _guard_page_ptr = nullptr;
        _guarded_start_ptr = nullptr;

        return VirtualFileSystemStatus::VFS_OK;
    }

    std::vector<URI> PosixFileSystemImpl::glob(const std::string &pattern) {
        using namespace std;
        MessageHandler& logger = Logger::instance().logger("posix filesystem");

        // make sure no , in pattern
        assert(pattern.find(',') == string::npos);

        const char* pattern_str = pattern.c_str();
        // starts with file://?
        if(0 == pattern.rfind("file://", 0))
            pattern_str += strlen("file://");

        // from https://stackoverflow.com/questions/8401777/simple-glob-in-c-on-unix-system
        // glob struct resides on the stack
        glob_t glob_result;
        memset(&glob_result, 0, sizeof(glob_result));

        // do the glob operation
        int return_value = ::glob(pattern_str, GLOB_TILDE | GLOB_MARK, NULL, &glob_result);
        if(return_value != 0) {
            globfree(&glob_result);

            // special case, no match
            if(GLOB_NOMATCH == return_value) {
                logger.warn("did not find any files for pattern '" + pattern + "'");
                return std::vector<URI>();
            }

            stringstream ss;
            ss << "glob() failed with return_value " << return_value << endl;
            throw std::runtime_error(ss.str());
        }

        // collect all the filenames into a std::list<std::string>
        vector<URI> uris;
        for(size_t i = 0; i < glob_result.gl_pathc; ++i) {
            uris.emplace_back(URI(std::string("file://") + glob_result.gl_pathv[i]));
        }

        // cleanup
        globfree(&glob_result);

        // done
        return uris;
    }

    VirtualFileSystemStatus PosixFileSystemImpl::copySingleFile(const URI &src, const URI &target, bool overwrite) {
        assert(src.isLocal() && target.isLocal());

        // overwrite?
        if(!overwrite) {
            if(fileExists(target.withoutPrefix()))
                return VirtualFileSystemStatus::VFS_FILEEXISTS;
        }

        if(src == target)
            return VirtualFileSystemStatus::VFS_OK;

        // if / is contained, make sure path exists, if not create dir
        if(std::string::npos != target.withoutPrefix().find("/")) {
            auto vfs = VirtualFileSystem::fromURI(target.parent());
            auto rc = vfs.create_dir(target.parent());
            if(rc != VirtualFileSystemStatus::VFS_OK)
                return VirtualFileSystemStatus::VFS_IOERROR;
        }

        // check that src exists
        if(!src.exists())
            return VirtualFileSystemStatus::VFS_FILENOTFOUND;

#ifdef LINUX
        // use sendfile only on Linux, on macOS it's broken
        // https://gist.github.com/gustavorv86/595127b9c22144f92b74f71a36098a4c
        auto fd_in = open(src.withoutPrefix().c_str(), O_RDONLY);
        if(-1 == fd_in)
            return VirtualFileSystemStatus::VFS_IOERROR;
        auto fd_out = creat(target.withoutPrefix().c_str(), 0660);
        if(-1 == fd_out) {
            close(fd_in);
            return VirtualFileSystemStatus::VFS_IOERROR;
        }
        struct stat s_stat = {0};
        fstat(fd_in, &s_stat);
        off_t n_bytes = 0;
        // https://man7.org/linux/man-pages/man2/sendfile.2.html
        auto rc = sendfile(fd_out, fd_in, &n_bytes, s_stat.st_size);
        close(fd_in);
        close(fd_out);
#else
        auto s = copyfile_state_alloc();
        int rc = copyfile(src.withoutPrefix().c_str(), target.withoutPrefix().c_str(), s, COPYFILE_ALL);
        copyfile_state_free(s);

        if(rc < 0)
            return VirtualFileSystemStatus::VFS_IOERROR;
#endif

        return VirtualFileSystemStatus::VFS_OK;
    }

    VirtualFileSystemStatus PosixFileSystemImpl::PosixFile::seek(int64_t delta) {
        return fseek(this->_fh, delta, SEEK_CUR) ? VirtualFileSystemStatus::VFS_OK : VirtualFileSystemStatus::VFS_IOERROR;
    }

    std::vector<URI> PosixFileSystemImpl::expandFolder(const URI &dir) {
        using namespace std;
        using namespace boost::filesystem;
        vector<URI> uris;

        if(!dir.exists())
            return uris;

        for(recursive_directory_iterator it(dir.withoutPrefix().c_str()), end; it != end; ++it) {
            if(is_regular_file(it->path()))
                uris.push_back(URI(weakly_canonical(it->path()).c_str()));
        }
        return uris;
    }
}