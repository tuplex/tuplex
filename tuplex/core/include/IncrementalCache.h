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

    struct StageResult {
        StageResult():
        normalPartitions(std::vector<Partition*>{}),
        exceptionPartitions(std::vector<Partition*>{}),
        generalPartitions(std::vector<Partition*>{}),
        fallbackPartitions(std::vector<Partition*>{}),
        partitionGroups(std::vector<PartitionGroup>{}),
        outputMode("") {}

        StageResult(const std::vector<Partition*>& normalPartitions,
                    const std::vector<Partition*>& exceptionPartitions,
                    const std::vector<Partition*>& generalPartitions,
                    const std::vector<Partition*>& fallbackPartitions,
                    const std::vector<PartitionGroup>& partitionGroups,
                    std::string outputMode):
                    normalPartitions(normalPartitions),
                    exceptionPartitions(exceptionPartitions),
                    generalPartitions(generalPartitions),
                    fallbackPartitions(fallbackPartitions),
                    partitionGroups(partitionGroups),
                    outputMode(outputMode) {}

        std::vector<Partition*> normalPartitions;
        std::vector<Partition*> exceptionPartitions;
        std::vector<Partition*> generalPartitions;
        std::vector<Partition*> fallbackPartitions;
        std::vector<PartitionGroup> partitionGroups;
        std::string outputMode;
    };

    /*!
     * Holds information about pipeline execution to use for incremental exception resolution
     */
    class IncrementalCacheEntry {
    private:
        LogicalOperator* _pipeline;
        std::unordered_map<size_t, StageResult> _stageResults;
        size_t _startFileNumber;
    public:
        IncrementalCacheEntry(LogicalOperator* pipeline);

        ~IncrementalCacheEntry();

        LogicalOperator* pipeline() const { return _pipeline; }

        void setStageResult(
                size_t stageNumber,
                const std::vector<Partition*>& normalPartitions,
                const std::vector<Partition*>& exceptionPartitions,
                const std::vector<Partition*>& generalPartitions,
                const std::vector<Partition*>& fallbackPartitions,
                const std::vector<PartitionGroup>& partitionGroups,
                std::string outputMode) {
            _stageResults[stageNumber] = StageResult(normalPartitions, exceptionPartitions, generalPartitions, fallbackPartitions, partitionGroups, outputMode);
        }

        std::vector<Partition*> normalPartitions(size_t stageNumber) { return _stageResults[stageNumber].normalPartitions; }

        std::vector<Partition*> exceptionPartitions(size_t stageNumber) { return _stageResults[stageNumber].exceptionPartitions; }

        std::vector<Partition*> generalPartitions(size_t stageNumber) { return _stageResults[stageNumber].generalPartitions; }

        std::vector<Partition*> fallbackPartitions(size_t stageNumber) { return _stageResults[stageNumber].fallbackPartitions; }

        std::vector<PartitionGroup> partitionGroups(size_t stageNumber) { return _stageResults[stageNumber].partitionGroups; }

        std::string outputMode(size_t stageNumber) { return _stageResults[stageNumber].outputMode; }

        void setStartFileNumber(size_t startFileNumber) { _startFileNumber = startFileNumber; }

        size_t startFileNumber() const { return _startFileNumber; }
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