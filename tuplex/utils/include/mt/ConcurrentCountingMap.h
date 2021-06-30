//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CONCURRENTCOUNTINGMAP_H
#define TUPLEX_CONCURRENTCOUNTINGMAP_H

#include <thread>
#include <memory>
#include <atomic>
#include <cstdint>
#include <functional>

namespace tuplex {

    // multi-thread counting hashmap

    template<typename KeyType=uint64_t, typename ValueType=uint64_t> class ConcurrentCountMap {
    public:
        struct Entry {
            std::atomic<KeyType> key;
            std::atomic<ValueType> count;
        };

        static const KeyType INVALID_KEY = std::numeric_limits<KeyType>::max();
        static const ValueType INVALID_VALUE = std::numeric_limits<KeyType>::max();

    private:
        Entry *_entries;
        size_t _size;

        void clear() {
            for(unsigned i = 0; i < _size; ++i) {
                _entries[i].count = 0;
                _entries[i].key = INVALID_KEY;
            }
        }


        inline uint32_t hash(uint32_t key) {
            key ^= key >> 16;
            key *= 0x85ebca6b;
            key ^= key >> 13;
            key *= 0xc2b2ae35;
            key ^= key >> 16;
            return key;
        }

        inline uint64_t hash(uint64_t key) {
            key ^= key >> 33;
            key *= 0xff51afd7ed558ccd;
            key ^= key >> 33;
            key *= 0xc4ceb9fe1a85ec53;
            key ^= key >> 33;
            return key;
        }

    public:
        ConcurrentCountMap(size_t size) {
            // must be power of 2
            assert((size & (size - 1)) == 0);   // Must be a power of 2

            _size = size;
            _entries = new Entry[_size];
            clear();
        }

        ~ConcurrentCountMap() {
            assert(_entries);

            // make sure no thread is accessing the map
            delete [] _entries;
            _entries = nullptr;
        }

        /*!
         * set entry with key to value
         * @param key
         * @param value
         * @return key when hashmap was not full. If hashmap was full INVALID_KEY is returned.
         */
        KeyType set(KeyType key, ValueType value) {
            assert(key != INVALID_KEY);
            assert(value != INVALID_VALUE);

            uint32_t num_tries = 0;
            for(uint32_t idx = hash(key); num_tries < _size; idx++) {
                idx &= _size - 1; // fast mod
                KeyType prevKey = INVALID_KEY;

                // swap prevKey
                if(_entries[idx].key.compare_exchange_strong(prevKey, key, std::memory_order_relaxed)) {
                    // store
                    _entries[idx].count.store(value, std::memory_order_relaxed);
                    return key;
                }
                num_tries++;
            }

            // exception?
            if(num_tries >= _size)
                return INVALID_KEY;

            return INVALID_KEY;
        }

        /*!
         * incs value associated with key by delta
         * @param key
         * @param delta
         * @return incremented value result or INVALID_VALUE if not found
         */
        ValueType inc(KeyType key, ValueType delta = 1) {
            assert(key != INVALID_KEY);

            int num_tries = 0;
            for(uint32_t idx = hash(key); num_tries < _size; idx++) {
                idx &= _size - 1; // fast mod

                uint32_t probedKey = _entries[idx].key.load(std::memory_order_relaxed);
                if(probedKey == key)
                    return _entries[idx].count += delta; // atomic +=
                if(probedKey == INVALID_KEY)
                    return set(key, delta); // not yet in there, so set to delta initially
                num_tries++;
            }

            return INVALID_VALUE;
        }

        /*!
         * gets value for key
         * @param key
         * @return value. If key not found return INVALID_VALUE
         */
        ValueType get(KeyType key) {
            assert(key != INVALID_KEY);

            int num_tries = 0;
            for(uint32_t idx = hash(key); num_tries < _size; idx++) {
                idx &= _size - 1; // fast mod

                KeyType probedKey = _entries[idx].key.load(std::memory_order_relaxed);
                if(probedKey == key)
                    return _entries[idx].count.load(std::memory_order_relaxed);
                if(probedKey == INVALID_KEY)
                    return INVALID_VALUE;
                num_tries++;
            }

            return INVALID_VALUE;
        }

    };

}

#endif //TUPLEX_COUNTINGMAP_H