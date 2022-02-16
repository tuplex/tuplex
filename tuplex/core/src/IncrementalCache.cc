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
            std::vector<Partition*> exceptionPartitions,
            std::vector<Partition*> generalPartitions,
            std::vector<Partition*> fallbackPartitions,
            size_t startFileNumber) {
        _pipeline = pipeline->clone();
        _exceptionPartitions = std::move(exceptionPartitions);
        _generalPartitions = std::move(generalPartitions);
        _fallbackPartitions = std::move(fallbackPartitions);
        _startFileNumber = startFileNumber;
    }

    IncrementalCacheEntry::~IncrementalCacheEntry() {
        delete _pipeline;
//        for (auto &p : _exceptionPartitions)
//            p->invalidate();
//        _exceptionPartitions.clear();
//        for (auto &p : _generalPartitions)
//            p->invalidate();
//        _generalPartitions.clear();
//        for (auto &p : _fallbackPartitions)
//            p->invalidate();
//        _fallbackPartitions.clear();
    }

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