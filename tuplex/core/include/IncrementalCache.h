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
#include <ExceptionInfo.h>

namespace tuplex {

    class Partition;
    class LogicalOperator;

    class CacheEntry {
    private:
        LogicalOperator *_pipeline;
        std::vector<Partition*> _outputPartitions;
        std::vector<Partition*> _exceptionPartitions;
        std::unordered_map<std::string, ExceptionInfo*> _exceptionsMap;
    public:
        void setPipeline(LogicalOperator *pipeline) { _pipeline = pipeline; }
        void setOutputPartitions(const std::vector<Partition*> &outputPartitions) { _outputPartitions = outputPartitions; }
        void setExceptionPartitions(const std::vector<Partition*> &exceptionPartitions) { _exceptionPartitions = exceptionPartitions; }
        void setExceptionsMap(const std::unordered_map<std::string, ExceptionInfo*> &exceptionsMap) { _exceptionsMap = exceptionsMap; }

        LogicalOperator *pipeline() { return _pipeline; }
        std::vector<Partition*> outputPartitions() { return _outputPartitions; }
        std::vector<Partition*> exceptionPartitions() { return _exceptionPartitions; }
        std::unordered_map<std::string, ExceptionInfo*> exceptionsMap() { return _exceptionsMap; }
    };

    class IncrementalCache {
    private:
        std::unordered_map<std::string, CacheEntry*> _cache;

    public:
        void clear();

        void addCacheEntry(LogicalOperator *pipeline,
                           const std::vector<Partition *> &outputPartitions,
                           const std::vector<Partition*> &exceptionPartitions,
                           const std::unordered_map<std::string, ExceptionInfo*> &exceptionsMap);

        CacheEntry *getCacheEntry(LogicalOperator *action) const;
    };
}

#endif //TUPLEX_INCREMENTALCACHE_H
