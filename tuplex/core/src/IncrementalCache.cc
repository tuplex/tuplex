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
        _startFileNumber = 0;
    }

    IncrementalCacheEntry::~IncrementalCacheEntry() {
        delete _pipeline;
    }

    void IncrementalCacheEntry::setPipeline(LogicalOperator *pipeline) {
        delete _pipeline;
        _pipeline = pipeline->clone();
    }

    std::string IncrementalCache::pipelineToString(LogicalOperator* pipeline) {
        std::stringstream ss;
        for (const auto & p : pipeline->parents())
            ss << pipelineToString(p);
        if (pipeline->type() != LogicalOperatorType::RESOLVE && pipeline->type() != LogicalOperatorType::IGNORE)
            ss << std::to_string(static_cast<int>(pipeline->type()));

        return ss.str();
    }
}