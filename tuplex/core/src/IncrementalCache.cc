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

    IncrementalCacheEntry::IncrementalCacheEntry(LogicalOperator* pipeline) {
        _pipeline = pipeline->clone();
    }

    IncrementalCacheEntry::~IncrementalCacheEntry() {
        delete _pipeline;
    }

    void IncrementalCache::addEntry(const std::string& key, IncrementalCacheEntry* entry) {
        auto elt = _cache.find(key);
        if (elt != _cache.end())
            _cache.erase(key);

        _cache[key] = entry;
    }

    std::string IncrementalCache::newKey(LogicalOperator* pipeline) {
        std::stringstream ss;
        for (const auto & p : pipeline->parents())
            ss << newKey(p);
        if (pipeline->type() != LogicalOperatorType:: RESOLVE && pipeline->type() != LogicalOperatorType::IGNORE)
            ss << std::to_string(static_cast<int>(pipeline->type()));

        return ss.str();
    }
}