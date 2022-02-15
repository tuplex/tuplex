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
        std::vector<PartitionGroup> _partitionGroups;
    public:
        IncrementalCacheEntry(LogicalOperator* pipeline,
              const std::vector<Partition*>& exceptionPartitions,
              const std::vector<Partition*>& generalPartitions,
              const std::vector<Partition*>& fallbackPartitions,
              const std::vector<PartitionGroup>& partitionGroups):
              _pipeline(pipeline),
              _exceptionPartitions(exceptionPartitions),
              _generalPartitions(generalPartitions),
              _fallbackPartitions(fallbackPartitions),
              _partitionGroups(partitionGroups) {}

        ~IncrementalCacheEntry();

        LogicalOperator* pipeline() const {
            return _pipeline;
        }

        std::vector<Partition*> exceptionPartitions() const {
            return _exceptionPartitions;
        }

        std::vector<Partition*> generalPartitions() const {
            return _generalPartitions;
        }

        std::vector<Partition*> fallbackPartitions() const {
            return _fallbackPartitions;
        }

        std::vector<PartitionGroup> partitionGroups() const {
            return _partitionGroups;
        }
    };

    class IncrementalCSVEntry : public IncrementalCacheEntry {
        class Metadata;
    private:
        std::vector<Metadata> _fileMetadata;
    public:
        IncrementalCSVEntry(LogicalOperator* pipeline,
                 const std::vector<Partition*>& exceptionPartitions,
                 const std::vector<Partition*>& generalPartitions,
                 const std::vector<Partition*>& fallbackPartitions,
                 const std::vector<PartitionGroup>& partitionGroups,
                 const std::vector<Metadata>& fileMetadata):
                IncrementalCacheEntry(pipeline, exceptionPartitions, generalPartitions, fallbackPartitions, partitionGroups),
                  _fileMetadata(fileMetadata)  {}

        ~IncrementalCSVEntry() {
            IncrementalCacheEntry::~IncrementalCacheEntry();
        }

        std::vector<Metadata> fileMetadata() const {
            return _fileMetadata;
        }
    };

    class IncrementalCSVEntry::Metadata {
    private:
        std::vector<size_t> _rowIndices;
    public:
        Metadata(const std::vector<size_t>& rowIndices): _rowIndices(rowIndices) {}

        std::vector<size_t> rowIndices() const {
            return _rowIndices;
        }
    };

    class IncrementalCache {
    private:
        std::unordered_map<std::string, IncrementalCacheEntry*> _cache;
    public:
        ~IncrementalCache() {
            clear();
        }

        void addEntry(const std::string& key, IncrementalCacheEntry* entry) {
            _cache[key] = entry;
        }

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