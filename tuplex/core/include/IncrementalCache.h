//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_INCREMENTALCACHE_H
#define TUPLEX_INCREMENTALCACHE_H

#include <logical/LogicalOperator.h>
#include <Partition.h>

namespace tuplex {

    class IncrementalCacheEntry;
    class IncrementalCSVEntry;

    class IncrementalCacheEntry {
    private:
        LogicalOperator* _pipeline;
        std::vector<Partition*> _exceptionPartitions;
        std::vector<Partition*> _generalPartitions;
        std::vector<Partition*> _fallbackPartitions;
        size_t _startFileNumber;
    public:
        IncrementalCacheEntry(LogicalOperator* pipeline,
                              const std::vector<Partition*>& exceptionPartitions,
                              const std::vector<Partition*>& generalPartitions,
                              const std::vector<Partition*>& fallbackPartitions,
                              size_t startFileNumber);

//        ~IncrementalCacheEntry();

        LogicalOperator* pipeline() const {
            return _pipeline;
        }

        void setExceptionPartitions(const std::vector<Partition*>& exceptionPartitions) { _exceptionPartitions = exceptionPartitions; }

        std::vector<Partition*> exceptionPartitions() const {
            return _exceptionPartitions;
        }

        std::vector<Partition*> generalPartitions() const {
            return _generalPartitions;
        }

        std::vector<Partition*> fallbackPartitions() const {
            return _fallbackPartitions;
        }

        size_t startFileNumber() const {
            return _startFileNumber;
        }
    };

    class IncrementalCache {
    private:
        std::unordered_map<std::string, IncrementalCacheEntry*> _cache;
    public:
        ~IncrementalCache() {
            clear();
        }

        void addEntry(const std::string& key, IncrementalCacheEntry* entry);

        IncrementalCacheEntry* getEntry(const std::string& key) const {
            auto elt = _cache.find(key);
            if (elt == _cache.end())
                return nullptr;
            return elt->second;
        }

        void clear() {
            _cache.clear();
        }

        static std::string newKey(LogicalOperator* pipeline);
    };

}

#endif //TUPLEX_INCREMENTALCACHE_H