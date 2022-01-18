//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/9/2022                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <IncrementalCache.h>

namespace tuplex {
    void IncrementalCache::clear() {
        _cache.clear();
    }

    void IncrementalCache::addCacheEntry(LogicalOperator *pipeline,
                       const std::vector<Partition *> &outputPartitions,
                       const std::vector<std::tuple<size_t, PyObject*>> &outputPyObjects,
                       const std::vector<Partition*> &exceptionPartitions,
                       const std::vector<Partition*> &generalCasePartitions,
                       const std::unordered_map<std::string, std::tuple<size_t, size_t, size_t>> &partitionToExceptionsMap) {
        auto cacheEntry = new CacheEntry();
        for (auto p : outputPartitions)
            p->makeImmortal();
        for (auto p : exceptionPartitions)
            p->makeImmortal();
        for (auto p : generalCasePartitions)
            p->makeImmortal();


        cacheEntry->setPipeline(pipeline->clone());
        cacheEntry->setOutputPartitions(outputPartitions);
        cacheEntry->setOutputPyObjects(outputPyObjects);
        cacheEntry->setExceptionPartitions(exceptionPartitions);
        cacheEntry->setGeneralCasePartitions(generalCasePartitions);
        cacheEntry->setPartitionToExceptionsMap(partitionToExceptionsMap);

        _cache["1"] = cacheEntry;
    }

    CacheEntry *IncrementalCache::getCacheEntry(LogicalOperator *action) const {
        auto elt = _cache.find("1");
        if (elt == _cache.end()) {
            return nullptr;
        }
        return elt->second;
    }

}