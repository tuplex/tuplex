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
        partitionGroups(std::vector<PartitionGroup>{}) {}

        StageResult(const std::vector<Partition*>& normalPartitions,
                    const std::vector<Partition*>& exceptionPartitions,
                    const std::vector<Partition*>& generalPartitions,
                    const std::vector<Partition*>& fallbackPartitions,
                    const std::vector<PartitionGroup>& partitionGroups):
                    normalPartitions(normalPartitions),
                    exceptionPartitions(exceptionPartitions),
                    generalPartitions(generalPartitions),
                    fallbackPartitions(fallbackPartitions),
                    partitionGroups(partitionGroups) {}

        std::vector<Partition*> normalPartitions;
        std::vector<Partition*> exceptionPartitions;
        std::vector<Partition*> generalPartitions;
        std::vector<Partition*> fallbackPartitions;
        std::vector<PartitionGroup> partitionGroups;
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

        void setStageResult(size_t stageNumber,
                            const std::vector<Partition*>& normalPartitions,
                            const std::vector<Partition*>& exceptionPartitions,
                            const std::vector<Partition*>& generalPartitions,
                            const std::vector<Partition*>& fallbackPartitions,
                            const std::vector<PartitionGroup>& partitionGroups) {
            auto elt = _stageResults.find(stageNumber);
            if (elt != _stageResults.end())
                _stageResults.erase(stageNumber);
            _stageResults[stageNumber] = StageResult(normalPartitions, exceptionPartitions, generalPartitions, fallbackPartitions, partitionGroups);
        }

        std::vector<Partition*> normalPartitions(size_t stageNumber) { return _stageResults[stageNumber].normalPartitions; }

        std::vector<Partition*> exceptionPartitions(size_t stageNumber) { return _stageResults[stageNumber].exceptionPartitions; }

        std::vector<Partition*> generalPartitions(size_t stageNumber) { return _stageResults[stageNumber].generalPartitions; }

        std::vector<Partition*> fallbackPartitions(size_t stageNumber) { return _stageResults[stageNumber].fallbackPartitions; }

        std::vector<PartitionGroup> partitionGroups(size_t stageNumber) { return _stageResults[stageNumber].partitionGroups; }

        size_t startFileNumber() const { return _startFileNumber; }

        void setPipeline(LogicalOperator* pipeline);

        void setStartFileNumber(size_t startFileNumber) { _startFileNumber = startFileNumber; }
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

        void setStageResult(LogicalOperator* pipeline,
                            size_t stageNumber,
                            const std::vector<Partition*>& normalPartitions,
                            const std::vector<Partition*>& exceptionPartitions,
                            const std::vector<Partition*>& generalPartitions,
                            const std::vector<Partition*>& fallbackPartitions,
                            const std::vector<PartitionGroup>& partitionGroups) {
            auto key = pipelineToString(pipeline);
            if (_cache.find(key) == _cache.end())
                _cache[key] = new IncrementalCacheEntry(pipeline);
            _cache[key]->setStageResult(stageNumber,
                                        normalPartitions,
                                        exceptionPartitions,
                                        generalPartitions,
                                        fallbackPartitions,
                                        partitionGroups);
            _cache[key]->setPipeline(pipeline);
        }

        void setStartFileNumber(LogicalOperator* pipeline,
                                size_t startFileNumber) {
            auto key = pipelineToString(pipeline);
            if (_cache.find(key) == _cache.end())
                _cache[key] = new IncrementalCacheEntry(pipeline);
            _cache[key]->setStartFileNumber(startFileNumber);
            _cache[key]->setPipeline(pipeline);
        }

        /*!
         * Retrieve entry from the cache
         * @param key string representation of the pipeline
         * @return cache entry
         */
        IncrementalCacheEntry* getEntry(LogicalOperator* pipeline) const {
            auto elt = _cache.find(pipelineToString(pipeline));
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
        static std::string pipelineToString(LogicalOperator* pipeline);
    };

}

#endif //TUPLEX_INCREMENTALCACHE_H