//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_BITMAPALLOCATOR_H
#define TUPLEX_BITMAPALLOCATOR_H

#include <thread>
#include <atomic>

namespace tuplex {
    // implement a bitmap allocator according to https://eatplayhate.me/2010/09/04/memory-management-from-the-ground-up-2-foundations/
    class BitmapAllocator {
    private:

        std::mutex _mutex; // memory management is mutual

        size_t _arenaSize;
        std::atomic<uint8_t*> _arena;

        enum {
            FREE_BLOCK = 0,
            BOUNDARY_BLOCK = 1,
            USED_BLOCK = 2
        };

        // @TODO
        // make this more efficient with smaller blocks...?
        // i.e. use 2 bits for one block (decreases size by factor of 4)
        unsigned char* _bitmap;

        size_t _numBlocks;
        size_t _blockSize;

// debug
//        void print_bitmap() {
//            for(int i = 0; i <_numBlocks; i++) {
//                using namespace std;
//                cout<<"Block "<<i<<": ";
//                if(_bitmap[i] == FREE_BLOCK)
//                    cout<<"free";
//                if(_bitmap[i] == BOUNDARY_BLOCK)
//                    cout<<"boundary";
//                if(_bitmap[i] == USED_BLOCK)
//                    cout<<"used block";
//                cout<<endl;
//            }
//        }
    public:

        BitmapAllocator(size_t size, const size_t blockSize) {
            std::lock_guard<std::mutex> lock(_mutex);

            // check whether multiple
            if(size % blockSize != 0) {
                Logger::instance().logger("memory").warn("size is not a multiple of blockSize. Rounding up");
                size = ((size / blockSize) + 1) * blockSize;
            }

            _arenaSize = size;
            _blockSize = blockSize;
            _numBlocks = _arenaSize / _blockSize;


            assert(_numBlocks > 0);

            // for simplicity bitmap is large (decrease later)
            // @TODO
            _bitmap = (unsigned char*)::malloc(_numBlocks);
            _arena = (uint8_t*)::malloc(_arenaSize);

            std::stringstream ss;
            ss<<"allocated bitmap managed memory region ("
              <<sizeToMemString(_arenaSize)<<", "
              <<sizeToMemString(_blockSize)<<" block size)";
            Logger::instance().logger("memory").info(ss.str());

            if(!_bitmap || !_arena) {
                Logger::instance().logger("memory").error("could not allocate memory");
                if(_bitmap)
                    ::free(_bitmap);
                if(_arena)
                    ::free(_arena);

                exit(1);
            }

            // set all blocks to be free
            for(unsigned i = 0; i < _numBlocks; ++i)
                _bitmap[i] = FREE_BLOCK;
        }

        ~BitmapAllocator() {
            std::lock_guard<std::mutex> lock(_mutex);

            // make sure memory regions are not used somewhere else...
            if(_bitmap)
                ::free(_bitmap);
            if(_arena)
                ::free(_arena);
        }

        // returns nullptr if alloc failed or size = 0!
        void* alloc(const size_t size) {
            std::lock_guard<std::mutex> lock(_mutex);

            assert(size <= _arenaSize);

            if(0 == size)
                return nullptr;

            auto blocksRequired = size / _blockSize + ((size % _blockSize) > 0 ? 1 : 0);

            // linear search for free blocks
            auto location = 0;
            while(location <= _numBlocks - blocksRequired) {
                // count number of available blocks
                auto availableBlocks = 0;
                for(unsigned  i = 0; i < blocksRequired; ++i) {
                    if(_bitmap[i + location] != FREE_BLOCK) // not free?
                        break;
                    availableBlocks++;
                }

                // matching number of free blocks found?
                if(availableBlocks == blocksRequired) {
                    void* ptr = _arena + _blockSize * location;

                    // first block should be marked as special boundary block...?
                    // mark blocks as used in bitmap
                    _bitmap[location] = BOUNDARY_BLOCK;
                    for(unsigned i = 1; i < blocksRequired; ++i)
                        _bitmap[location + i] = USED_BLOCK;


                    // debug: memset to 0
                    memset(ptr, 0, size);

                    return ptr;
                } else
                    location += availableBlocks + 1;
            }

            // alloc failed
            return nullptr;
        }

        // free function
        void free(void *ptr) {
            std::lock_guard<std::mutex> lock(_mutex);

            if(nullptr == ptr) {
                Logger::instance().defaultLogger().warn("freeing empty pointer. Weird?");
                return;
            }

            // get block index from ptr
            auto arena_offset = (size_t)((uint8_t*)ptr - _arena);
            auto block_index = arena_offset / _blockSize;

            // valid?
            assert(block_index < _numBlocks);

            assert(arena_offset % _blockSize == 0);
            assert(_bitmap[block_index] == BOUNDARY_BLOCK); // is also a boundary block...?

            // free blocks
            _bitmap[block_index++] = FREE_BLOCK;
            while(block_index < _numBlocks && _bitmap[block_index]== USED_BLOCK) {
                _bitmap[block_index++] = FREE_BLOCK;
            }
        }

        size_t allocatedSize(void *ptr) {
            std::lock_guard<std::mutex> lock(_mutex);

            if(nullptr == ptr)
                return 0;

            // get block index from ptr
            auto arena_offset = (size_t)((uint8_t*)ptr - _arena);
            auto block_index = arena_offset / _blockSize;

            // valid?
            assert(block_index < _numBlocks);

            assert(arena_offset % _blockSize == 0);
            assert(_bitmap[block_index] == BOUNDARY_BLOCK); // is also a boundary block...?

            // free blocks
            size_t size = blockSize();
            block_index++;
            while(block_index < _numBlocks && _bitmap[block_index]== USED_BLOCK) {
                block_index++;
                size += blockSize();
            }
            return size;
        }

        size_t size() const { return _arenaSize; }
        size_t blockSize() const { return _blockSize; }
    };
}

#endif //TUPLEX_BITMAPALLOCATOR_H