//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Partition.h>
#include <Utils.h>

namespace tuplex {

    // init all atomic metric counters
    std::atomic_int64_t Partition::_swapInCount(0);
    std::atomic_int64_t Partition::_swapOutCount(0);
    std::atomic_int64_t Partition::_swapInBytesRead(0);
    std::atomic_int64_t Partition::_swapOutBytesWritten(0);

    const uint8_t* Partition::lockRaw() {

        assert(_owner);

        TRACE_LOCK("partition " + uuidToString(_uuid));
        std::this_thread::yield();
        _mutex.lock();
        _locked = true;

        // atomic check here...
        // first check whether memory pointer is valid
        // if not, recover partition!
        if(!_arena) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("recovering partition");
#endif
            _owner->recoverPartition(this);
        }

        // this locks listmutex, therefore update before partition!
        // update last access time
        _owner->makeRecentlyUsed(this);

        assert(_arena);

        return _arena;
    }

    void Partition::unlock() {
        TRACE_UNLOCK("partition " + uuidToString(_uuid));
        _mutex.unlock();
        _locked = false;
    }

    uint8_t* Partition::lockWriteRaw() {
        // must be the thread who allocated this
        assert(_owner->getThreadID() == std::this_thread::get_id());

        TRACE_LOCK("partition " + uuidToString(_uuid));
        std::this_thread::yield();
        _mutex.lock();
        _locked = true;

        // first check whether memory pointer is valid
        // if not, recover partition!
        if(!_arena)
            _owner->recoverPartition(this);
        assert(_arena);

        // this locks mutexes of Executor. They should lock before partition mutex, i.e. make them come first.
        // update last access time
        _owner->makeRecentlyUsed(this);

        return _arena;
    }

    void Partition::unlockWrite() {
        TRACE_UNLOCK("partition " + uuidToString(_uuid));
        _mutex.unlock();
        _locked = false;
    }

    bool Partition::saveToFile(const URI& partitionURI) {
//        auto uuid = uuidToString(_uuid);
//        auto vfs = VirtualFileSystem::fromURI(partitionURI);
//
//        // create file & write partition contents to it
//        std::unique_ptr<VirtualFile> file = vfs.open_file(partitionURI, VFS_WRITE | VFS_OVERWRITE);
//        if(!file) {
//            std::stringstream ss;
//            ss<<"Could not save partition "<<uuid<<" to path "<<partitionURI.toString();
//            _owner->logger().error(ss.str());
//            return false;
//        }
//
//        auto status = file.get()->write(_arena, (uint64_t)_size);
//
//        if(status != VirtualFileSystemStatus::VFS_OK) {
//            assert(file);
//            _owner->logger().error("Could not save partition " + uuid + " to path " + file.get()->getURI().toPath());
//
//            return false;
//        }

        auto path = partitionURI.toString().substr(7);

        // does file exist already?
        // => fail
        if(fileExists(path)) {
            throw std::runtime_error("partition file under " + path + " already exists.");
        }

        FILE *pFile = fopen(path.c_str(), "wb");
        if(!pFile) {
            handle_file_error("failed to evict partition to " + path);
            return false;
        }

        // write to file
        fwrite(&_bytesWritten, sizeof(uint64_t), 1, pFile);
        fwrite(_arena, _size, 1, pFile);

        fclose(pFile);

        _swapOutCount++;
        _swapOutBytesWritten += _size + sizeof(uint64_t);

        return true;
    }

    void Partition::loadFromFile(const tuplex::URI &uri) {

        auto path = uri.toString().substr(7);

        if(!fileExists(path)) {
            throw std::runtime_error("could not find file under path " + path);
        }

        FILE *pFile = fopen(path.c_str(), "rb");
        if(!pFile) {
            handle_file_error("failed to load evicted partition from " + path);
            return;
        }

        // read from file
        fread(&_bytesWritten, sizeof(uint64_t), 1, pFile);
        fread(_arena, _size, 1, pFile);

        fclose(pFile);

        // remove file b.c. it's now loaded
        if(0 != remove(path.c_str())) {
            throw std::runtime_error("failed removing file from path " + path);
        }

        // update metric counters
        _swapInCount++;
        _swapInBytesRead += _size + sizeof(uint64_t);

//        auto vfs = VirtualFileSystem::fromURI(uri);
//        uint64_t file_size = 0;
//        vfs.file_size(uri, file_size);
//
//        assert(_size >= file_size); // this should not fail!!!
//
//        // read file to mem ptr
//        auto file = vfs.open_file(uri, VFS_READ);
//
//        uint64_t bytesRead = 0;
//
//        auto status = file->read(_arena, file_size, bytesRead);
//
//        if(bytesRead != file_size || status != VirtualFileSystemStatus::VFS_OK) {
//            _owner->logger().error("could not recover partition from " + uri.toString());
//            return;
//        }
    }

    void Partition::swapIn(uint8_t *memory, const tuplex::URI &swapFileURI) {
        assert(memory);

        TRACE_LOCK("partition " + uuidToString(_uuid));
        std::this_thread::yield();
        _mutex.lock();

        // check file name?
        assert(swapFileURI == _localFilePath);

        _arena = memory;
        loadFromFile(swapFileURI);

        // write back numrows (lazy executed)
        _numRows = *((int64_t *)_arena); // lazy update

        _swappedToFile = false;
        TRACE_UNLOCK("partition " + uuidToString(_uuid));
        _mutex.unlock();
    }

    void Partition::swapOut(tuplex::BitmapAllocator &allocator, const tuplex::URI &swapFileURI) {
        TRACE_LOCK("partition " + uuidToString(_uuid));

        std::this_thread::yield();
        _mutex.lock();

        // read back numrows (lazy executed)
        _numRows = *((int64_t *)_arena); // lazy update

        saveToFile(swapFileURI);
        allocator.free(_arena);
        _arena = nullptr;
        _swappedToFile = true;
        _localFilePath = swapFileURI.toPath();
        TRACE_UNLOCK("partition " + uuidToString(_uuid));
        _mutex.unlock();
    }

    void Partition::free(tuplex::BitmapAllocator &allocator) {
        TRACE_LOCK("partition " + uuidToString(_uuid));
        std::this_thread::yield();
        _mutex.lock();
        if(_arena)
            allocator.free(_arena);
        _arena = nullptr;
        TRACE_UNLOCK("partition " + uuidToString(_uuid));
        _mutex.unlock();
    }

    void Partition::invalidate() {

        // also make sure this partition does not live forever.
        // these partitions are destroyed when the context is released.
        if(!isImmortal()) {
            // hence only free partition if it is not immortal
            _owner->freePartition(this);
        }
    }
}