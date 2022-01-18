//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/9/2022                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_INCREMENTALCACHE_H
#define TUPLEX_INCREMENTALCACHE_H

#include <Partition.h>
#include <logical/LogicalOperator.h>

namespace tuplex {

    class Partition;
    class LogicalOperator;

    class CacheEntry {
    private:
        LogicalOperator *_pipeline;
        std::vector<Partition*> _outputPartitions;
        std::vector<std::tuple<size_t, PyObject*>> _outputPyObjects;
        std::vector<Partition*> _exceptionPartitions;
        std::vector<Partition*> _generalCasePartitions;
        std::unordered_map<std::string, std::tuple<size_t, size_t, size_t>> _partitionToExceptionsMap;
    public:
        void setPipeline(LogicalOperator *pipeline) { _pipeline = pipeline; }
        void setOutputPartitions(const std::vector<Partition*> &outputPartitions) { _outputPartitions = outputPartitions; }
        void setOutputPyObjects(const std::vector<std::tuple<size_t, PyObject*>> &outputPyObjects) { _outputPyObjects = outputPyObjects; }
        void setExceptionPartitions(const std::vector<Partition*> &exceptionPartitions) { _exceptionPartitions = exceptionPartitions; }
        void setGeneralCasePartitions(const std::vector<Partition*> &generalCasePartitions) { _generalCasePartitions = generalCasePartitions; }
        void setPartitionToExceptionsMap(const std::unordered_map<std::string, std::tuple<size_t, size_t, size_t>> &partitionToExceptionsMap) { _partitionToExceptionsMap = partitionToExceptionsMap; }

        LogicalOperator *pipeline() { return _pipeline; }
        std::vector<Partition*> outputPartitions() { return _outputPartitions; }
        std::vector<std::tuple<size_t, PyObject*>> outputPyObjects() { return _outputPyObjects; }
        std::vector<Partition*> exceptionPartitions() { return _exceptionPartitions; }
        std::vector<Partition*> generalCasePartitions() { return _generalCasePartitions; }
        std::unordered_map<std::string, std::tuple<size_t, size_t, size_t>> partitionToExceptionsMap() { return _partitionToExceptionsMap; }
    };

    class IncrementalCache {
    private:
        std::unordered_map<std::string, CacheEntry*> cache;

    public:
        void addCacheEntry(LogicalOperator *pipeline,
                           const std::vector<Partition *> &outputPartitions,
                           const std::vector<std::tuple<size_t, PyObject*>> &outputPyObjects,
                           const std::vector<Partition*> &exceptionPartitions,
                           const std::vector<Partition*> &generalCasePartitions,
                           const std::unordered_map<std::string, std::tuple<size_t, size_t, size_t>> &partitionToExceptionsMap);

        CacheEntry *getCacheEntry(LogicalOperator *action) const;
    };
}

#endif //TUPLEX_INCREMENTALCACHE_H
