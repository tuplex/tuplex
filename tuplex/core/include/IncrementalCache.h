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

    /*!
     * Holds information about pipeline execution to use for incremental exception resolution
     */
    class IncrementalCacheEntry {
    private:
        LogicalOperator* _pipeline;
        std::vector<Partition*> _normalPartitions;
        std::vector<Partition*> _exceptionPartitions;
        std::vector<Partition*> _generalPartitions;
        std::vector<Partition*> _fallbackPartitions;
        std::vector<PartitionGroup> _partitionGroups;
        size_t _startFileNumber;
    public:
        /*!
         * Incremental cache entry for merge out of order
         * @param pipeline original logical plan
         * @param exceptionPartitions exception rows
         * @param generalPartitions general rows
         * @param fallbackPartitions fallback rows
         * @param startFileNumber next available file number
         */
        IncrementalCacheEntry(LogicalOperator* pipeline,
                              const std::vector<Partition*>& exceptionPartitions,
                              const std::vector<Partition*>& generalPartitions,
                              const std::vector<Partition*>& fallbackPartitions,
                              size_t startFileNumber);

        /*!
         * Incremental cache entry for merge in order
         * @param pipeline original logical plan
         * @param normalPartitions normal rows
         * @param exceptionPartitions exception rows
         * @param partitionGroups mapping of normal rows to exception rows
         */
        IncrementalCacheEntry(LogicalOperator *pipeline,
                              const std::vector<Partition*>& normalPartitions,
                              const std::vector<Partition*>& exceptionPartitions,
                              const std::vector<PartitionGroup>& partitionGroups);

        ~IncrementalCacheEntry();

        LogicalOperator* pipeline() const {
            return _pipeline;
        }

        void setExceptionPartitions(const std::vector<Partition*>& exceptionPartitions) { _exceptionPartitions = exceptionPartitions; }

        std::vector<PartitionGroup> partitionGroups() const { return _partitionGroups; }

        std::vector<Partition*> normalPartitions() const { return _normalPartitions; }

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

    /*!
     * Maps pipelines to their cached results
     */
    class IncrementalCache {
    private:
        std::unordered_map<std::string, IncrementalCacheEntry*> _cache;
    public:
        ~IncrementalCache() {
            clear();
        }

        /*!
         * Add entry to the cache
         * @param key string representation of the pipeline
         * @param entry entry to store
         */
        void addEntry(const std::string& key, IncrementalCacheEntry* entry);

        /*!
         * Retrieve entry from the cache
         * @param key string representation of the pipeline
         * @return cache entry
         */
        IncrementalCacheEntry* getEntry(const std::string& key) const {
            auto elt = _cache.find(key);
            if (elt == _cache.end())
                return nullptr;
            return elt->second;
        }

        /*!
         * Reset incremental cache entries
         */
        void clear() {
            _cache.clear();
        }

        /*!
         * convert pipeline to unique string
         * @param pipeline original logical plan
         * @return 
         */
        static std::string newKey(LogicalOperator* pipeline);
    };

}

#endif //TUPLEX_INCREMENTALCACHE_H