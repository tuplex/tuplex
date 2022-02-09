//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/CacheOperator.h>

namespace tuplex {


    // @TODO: need to save exception counts as well, so later stages can generate appropriate code!
    // => caching might also help, because if no exceptions are present no slow-code path needs to be executed/compiled!
    // => for join(.....cache()) case an upgrade must be compiled I fear...
    void CacheOperator::copyMembers(const LogicalOperator *other) {
        assert(other->type() == LogicalOperatorType::CACHE);

        LogicalOperator::copyMembers(other);
        auto cop = (CacheOperator*)other;
        setSchema(other->getOutputSchema());
        _normalPartitions = cop->cachedNormalPartitions();
        _generalPartitions = cop->cachedGeneralPartitions();
        _fallbackPartitions = cop->cachedFallbackPartitions();
        _partitionGroups = cop->partitionGroups();

        _optimizedSchema = cop->_optimizedSchema;
        _cached = cop->_cached;
        _normalRowCount = cop->_normalRowCount;
        _generalRowCount = cop->_generalRowCount;
        _fallbackRowCount = cop->_fallbackRowCount;
        _columns = cop->_columns;
        _sample = cop->_sample;
        _storeSpecialized = cop->_storeSpecialized;
    }

    LogicalOperator* CacheOperator::clone() {
        auto copy = new CacheOperator(parent()->clone(), _storeSpecialized, _memoryLayout);
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
    }

    CacheOperator * CacheOperator::cloneWithoutParents() const {
        auto copy = new CacheOperator(); // => no parents!
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
    }

    int64_t CacheOperator::cost() const {
        // is operator cached? => return combined cost!
        // @NOTE: could make exceptions more expensive than normal rows
        if(isCached()) {
            return _generalRowCount + _fallbackRowCount + _normalRowCount;
        } else {
            // return parent cost
            return parent()->cost();
        }
    }

    void CacheOperator::setResult(const std::shared_ptr<ResultSet> &rs) {
        using namespace std;

        _cached = true;

        // fetch both partitions (consume) from resultset + any unresolved exceptions
        _normalPartitions = rs->normalPartitions();
        for(auto p : _normalPartitions)
            p->makeImmortal();

        _generalPartitions = rs->generalPartitions();
        for(auto p : _generalPartitions)
            p->makeImmortal();

        _fallbackPartitions = rs->fallbackPartitions();
        for(auto p : _fallbackPartitions)
            p->makeImmortal();

        _partitionGroups = rs->partitionGroups();

        // check whether partitions have different schema than the currently set one
        // => i.e. they have been specialized.
        if(!_normalPartitions.empty()) {
            _optimizedSchema = _normalPartitions.front()->schema();
            assert(_optimizedSchema != Schema::UNKNOWN);
        }

        // if exceptions are empty, then force output schema to be the optimized one as well!
        if(_generalPartitions.empty())
            setSchema(_optimizedSchema);

        // because the schema might have changed due to the result, need to update the dataset!
        if(getDataSet())
            getDataSet()->setSchema(getOutputSchema());

        // print out some statistics about cached data
        size_t normalBytesWritten = 0;
        size_t normalCapacity = 0;
        size_t normalRows = 0;
        size_t generalBytesWritten = 0;
        size_t generalCapacity = 0;
        size_t generalRows = 0;
        size_t fallbackBytesWritten = 0;
        size_t fallbackCapacity = 0;
        size_t fallbackRows = 0;

        for(const auto &p : _normalPartitions) {
            normalRows += p->getNumRows();
            normalBytesWritten += p->bytesWritten();
            normalCapacity += p->size();
        }
        for(const auto &p : _generalPartitions) {
            generalRows += p->getNumRows();
            generalBytesWritten += p->bytesWritten();
            generalCapacity += p->size();
        }
        for(const auto &p : _fallbackPartitions) {
            fallbackRows += p->getNumRows();
            fallbackBytesWritten += p->bytesWritten();
            fallbackCapacity += p->size();
        }


        _normalRowCount = normalRows;
        _generalRowCount = generalRows;
        _fallbackRowCount = fallbackRows;

        stringstream ss;
        ss<<"Cached "<<pluralize(normalRows, "common row")
          <<" ("<<pluralize(generalRows, "general row") << ")"
          <<" ("<<pluralize(fallbackRows, "fallback row")
          <<"), memory usage: "<<sizeToMemString(normalBytesWritten)
          <<"/"<<sizeToMemString(normalCapacity)<<" ("
          <<sizeToMemString(generalBytesWritten)
          <<"/"<<sizeToMemString(generalCapacity)<<")"
          <<" ("<<sizeToMemString(normalBytesWritten)<<"/"<<sizeToMemString(normalCapacity)<<")";
        Logger::instance().defaultLogger().info(ss.str());

#ifndef NDEBUG
        // print schema
        Logger::instance().defaultLogger().info("CACHED common case schema: " + _optimizedSchema.getRowType().desc());
        Logger::instance().defaultLogger().info("CACHED general case schema: " + getOutputSchema().getRowType().desc());
#endif
    }

    size_t CacheOperator::getTotalCachedRows() const {
        size_t totalCachedRows = 0;
        for(const auto &p : _normalPartitions) {
            totalCachedRows += p->getNumRows();
        }
        for(const auto &p : _generalPartitions) {
            totalCachedRows += p->getNumRows();
        }
        for (const auto &p : _fallbackPartitions) {
            totalCachedRows += p->getNumRows();
        }
        return totalCachedRows;
    }
}