//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <IncrementalCache.h>

#include <utility>

namespace tuplex {

    IncrementalCacheEntry::IncrementalCacheEntry(
            LogicalOperator* pipeline,
            const std::vector<Partition*>& exceptionPartitions,
            const std::vector<Partition*>& generalPartitions,
            const std::vector<Partition*>& fallbackPartitions,
            size_t startFileNumber) {
        _pipeline = pipeline->clone();
        _exceptionPartitions = exceptionPartitions;
        _generalPartitions = generalPartitions;
        _fallbackPartitions = fallbackPartitions;
        _startFileNumber = startFileNumber;
    }

    void IncrementalCache::addEntry(const std::string& key, IncrementalCacheEntry* entry) {
        auto elt = _cache.find(key);
        if (elt != _cache.end())
            _cache.erase(key);

        _cache[key] = entry;
    }
//
//    IncrementalCacheEntry::~IncrementalCacheEntry() {
//        delete _pipeline;
//    }

    std::string IncrementalCache::newKey(LogicalOperator* pipeline) {
        assert(pipeline);
        std::stringstream ss;

        std::queue<LogicalOperator*> q;
        q.push(pipeline);
        while (!q.empty()) {
            auto cur = q.front(); q.pop();
            if (cur->type() != LogicalOperatorType::RESOLVE && cur->type() != LogicalOperatorType::IGNORE) {
                ss << std::to_string(static_cast<int>(cur->type()));
            }
            for (const auto& p : cur->parents()) {
                q.push(p);
            }
        }

        return ss.str();
    }
}