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
                       const std::vector<Partition*> &exceptionPartitions,
                       const std::unordered_map<std::string, ExceptionInfo *> &exceptionsMap) {
        auto cacheEntry = new CacheEntry();
        for (auto p : outputPartitions)
            p->makeImmortal();
        for (auto p : exceptionPartitions)
            p->makeImmortal();

        cacheEntry->setPipeline(pipeline->clone());
        cacheEntry->setOutputPartitions(outputPartitions);
        cacheEntry->setExceptionPartitions(exceptionPartitions);
        cacheEntry->setExceptionsMap(exceptionsMap);

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