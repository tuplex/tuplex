//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PARTITION_H
#define TUPLEX_PARTITION_H

#include "Schema.h"
#include "../../utils/include/Timer.h"
#include "../../utils/include/Utils.h"



#include <atomic>
#ifdef __GNUC__
    // GCC6 does not about std::atomic_int64_t, define here
    namespace std {
        using atomic_int64_t=std::atomic<int64_t>;
    }
#endif


#include <Executor.h>

// new partition

namespace tuplex {

    class Executor;
    class Partition;

    class Partition {
    private:
        uint8_t *_arena; //! pointer to memory region. nullptr means it is saved on disk.
        size_t _size;

        int64_t         _dataSetID; //! identifies dataset to which this partition belongs to
        int64_t         _contextID; //! identifies to which context this partitions belongs to

        uniqueid_t  _uuid;
        Executor* const   _owner; // who owns this partition?

        // may implement more efficient solution than simple mutual access,
        // i.e. multiple readers/single writer concurrency
        std::recursive_mutex _mutex; // exclusive mutex for ManagedPartition

        std::atomic_bool        _active; //! whether partition holds data that is somewhere used
        std::atomic_bool        _immortal; //! if a partition is immortal, then invalidating it won't free its memory.
        std::atomic_bool        _locked;

        std::atomic_int64_t     _lastAccessTime; // timestamp used for access (later used for swapping out partitions
        // to disk, if not enough space is available)

        bool isCached() const { return _arena == nullptr; }

        /*!
         * saves partition to file
         * @param partitionURI where to store partition
         * @return true if successful else false
         */
        bool saveToFile(const URI& partitionURI);

        void loadFromFile(const URI& uri);

        int64_t                 _numRows;
        uint64_t                _bytesWritten;

        Schema _schema; //! Schema of the partition. May be optimized away later.

        // variables when being swapped out.
        std::string _localFilePath;
        bool        _swappedToFile;

        // metrics
        // static variables holding information on how many swapIns/swapOuts happened
        static std::atomic_int64_t _swapInCount;
        static std::atomic_int64_t _swapOutCount;
        static std::atomic_int64_t _swapInBytesRead;
        static std::atomic_int64_t _swapOutBytesWritten;
    public:

        Partition(Executor* const owner,
                  uint8_t* memory,
                  size_t size,
                  const Schema& schema,
                  const int dataSetID,
                  const int contextID) : _owner(owner),
                                         _arena(memory),
                                         _size(size),
                                         _uuid(getUniqueID()),
                                         _active(false),
                                         _immortal(false),
                                         _locked(false),
                                         _numRows(0),
                                         _bytesWritten(0),
                                         _schema(schema),
                                         _dataSetID(dataSetID),
                                         _contextID(contextID),
                                         _swappedToFile(false) {
            // memory MUST point to a valid location
            assert(memory);

            // write to memory
            setNumRows(0);
        }

        ~Partition() {
            assert(!_locked);
        }

        /*!
         * lock memory belonging to partition for (exclusive) write ownership
         * @param allowForeignOwnerAccess this is a dangeours flag, when set to true, no check is carried out when a non-owning thread accesses this partition.
         * @return memory pointer. Nullptr if there was a recovery error
         */
        uint8_t* lockWriteRaw(bool allowForeignOwnerAccess=false);

        /*!
         * unlock write ownership
         */
        void unlockWrite();

        /*!
         * lock for read only
         * @return memory pointer. Nullptr if there was a recovery error
         */
        const uint8_t* lockRaw();

        /*!
         * retrieves the pointer for the start of the actual data space
         * @return memory pointer. Nullptr if there was a recovery error
         */
        const uint8_t* lock() { return lockRaw() + sizeof(int64_t); }

        /*!
         * retrieves the pointer for the start of the actual data space
         * @return memory pointer. Nullptr if there was a recovery error
         */
        uint8_t* lockWrite() { return lockWriteRaw() + sizeof(int64_t); }

        /*!
         * unlock read only
         */
        void unlock();

        size_t size() const { return _size; }

        uint64_t bytesWritten() const { return _bytesWritten; }
        void setBytesWritten(uint64_t bytesWritten) { _bytesWritten = bytesWritten; }

        /*!
         * return how much capacity is left, i.e. how many bytes can be actually written
         * @return
         */
        size_t capacity() const { return _size - sizeof(int64_t); }

        uniqueid_t uuid() const { return _uuid; }

        int64_t contextID() const { return _contextID; }

        /*!
         * checks whether memory of this partition is currently locked
         * @return
         */
        bool isLocked() const { return _locked; }

        /*!
         * checks whether partition is active,
         * i.e. used in current stage or required by another stager in the future
         * @return
         */
        bool isActive() const { return _active; }

        /*!
         * checks whether partition is free, i.e. can be used
         * @return
         */
        bool isFree() const {

            if(isCached())
                return false;

            if(isLocked())
                return false;
            if(isImmortal())
                return false;
            if(isActive())
                return false;
            return true;
        }

        bool isImmortal() const { return _immortal; }

        void makeImmortal() { _immortal = true; }

        /*!
         * swaps out contents by saving them to a swap file
         * @param allocator allocator on which arena was allocated
         * @param swapFileURI uri to the file to use for the swap
         */
        void swapOut(BitmapAllocator& allocator, const URI& swapFileURI);

        /*!
         * swaps in a file to a given memory region which then replaces this partition's region
         * @param memory
         * @param swapFileURI
         */
        void swapIn(uint8_t* memory, const URI& swapFileURI);

        void free(BitmapAllocator& allocator);

        /*!
         * THIS IS NOT THREADSAFE
         * an executor can call this on the partition to signal the memory manager, that this partition is no longer
         * required by any running process
         */
        void invalidate();

        size_t getNumRows() {
            _mutex.lock();

            size_t res = 0;
            if(!_arena)
                res = _numRows; // field to save when partition was written to memory
            else {
                _numRows = *((int64_t *)_arena); // lazy update
                res = *((int64_t *)_arena);
            }
            _mutex.unlock();
            return res;
        }

        void setNumRows(const size_t numRows) {
            _mutex.lock();

            _numRows = numRows;

            // save to memptr
            if(_arena) {
                *((int64_t*)_arena) = numRows;
            }

            _mutex.unlock();
        }


        int64_t getDataSetID() const { return _dataSetID; }

        void setDataSetID(const int64_t id) { _dataSetID = id; }

        Schema schema() const { return _schema; }

        void setSchema(const Schema& schema) {

#ifndef _NDEBUG
            if(_schema != Schema::UNKNOWN)
                Logger::instance().defaultLogger().warn("Setting schema of a partition whose schema is well-defined. Why?");
#endif

            _schema = schema; }

        const Executor* owner() const { return _owner; }

        /*
         * reset Partition statistics. SHOULD NOT BE CALLED when computation is active, because though internally
         * backed by atomics this function is not threadsafe (no mutex used here)
         */
        static void resetStatistics() {
            _swapInCount = 0;
            _swapOutCount = 0;
            _swapInBytesRead = 0;
            _swapOutBytesWritten =0;
        }

        /*!
         * how often partitions were swapped in from disk spill
         * @return number of times disk spill (in) occurred
         */
        static size_t statisticSwapInCount()  { return _swapInCount; }

        /*!
         * how often partitions were swapped to disk
         * @return number of times disk spill (out) occurred
         */
        static size_t statisticSwapOutCount()  { return _swapOutCount; }

        /*!
         * how many bytes were read back during disk spill
         * @return amount of bytes
         */
        static size_t statisticSwapInBytesRead() { return _swapInBytesRead; }

        /*!
        * how many bytes were written in total during disk spill
        * @return amount of bytes
        */
        static size_t statisticSwapOutBytesWritten() { return _swapOutBytesWritten; }


        // for debug asserts.
        friend class Executor;
    };

}

#endif //TUPLEX_PARTITION_H