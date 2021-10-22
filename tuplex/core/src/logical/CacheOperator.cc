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
        _normalCasePartitions = cop->cachedPartitions();
        _generalCasePartitions = cop->cachedExceptions();
        // copy python objects and incref for each!
        _py_objects = cop->_py_objects;
        python::lockGIL();
        for(auto obj : _py_objects)
            Py_XINCREF(obj);
        python::unlockGIL();
        _optimizedSchema = cop->_optimizedSchema;
        _cached = cop->_cached;
        _normalCaseRowCount = cop->_normalCaseRowCount;
        _generalCaseRowCount = cop->_generalCaseRowCount;
        _columns = cop->_columns;
        _sample = cop->_sample;
        _storeSpecialized = cop->_storeSpecialized;
    }

    std::shared_ptr<LogicalOperator> CacheOperator::clone() {
        auto copy = new CacheOperator(parent()->clone(), _storeSpecialized, _memoryLayout);
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }

    std::shared_ptr<CacheOperator> CacheOperator::cloneWithoutParents() const {
        auto copy = new CacheOperator(); // => no parents!
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return std::shared_ptr<CacheOperator>(copy);
    }

    int64_t CacheOperator::cost() const {
        // is operator cached? => return combined cost!
        // @NOTE: could make exceptions more expensive than normal rows
        if(isCached()) {
            return _generalCaseRowCount + _normalCaseRowCount;
        } else {
            // return parent cost
            return parent()->cost();
        }
    }

    void CacheOperator::setResult(const std::shared_ptr<ResultSet> &rs) {
        using namespace std;

        _cached = true;

        // fetch both partitions (consume) from resultset + any unresolved exceptions
        _normalCasePartitions = rs->partitions();
        for(auto p : _normalCasePartitions)
            p->makeImmortal();

        // @TODO: there are two sorts of exceptions here...
        // i.e. separate normal-case violations out from the rest
        // => these can be stored separately for faster processing!
        // @TODO: right now, everything just gets cached...

        _generalCasePartitions = rs->exceptions();
        for(auto p : _generalCasePartitions)
            p->makeImmortal();

        // check whether partitions have different schema than the currently set one
        // => i.e. they have been specialized.
        if(!_normalCasePartitions.empty()) {
            _optimizedSchema = _normalCasePartitions.front()->schema();
            assert(_optimizedSchema != Schema::UNKNOWN);
        }

        // if exceptions are empty, then force output schema to be the optimized one as well!
        if(_generalCasePartitions.empty())
            setSchema(_optimizedSchema);

        // because the schema might have changed due to the result, need to update the dataset!
        if(getDataSet())
            getDataSet()->setSchema(getOutputSchema());

        // print out some statistics about cached data
        size_t cachedPartitionsMemory = 0;
        size_t totalCachedPartitionsMemory = 0;
        size_t totalCachedRows = 0;
        size_t cachedExceptionsMemory = 0;
        size_t totalCachedExceptionsMemory = 0;
        size_t totalCachedExceptions = 0;

        int pos = 0;
        for(auto p : _normalCasePartitions) {
            totalCachedRows += p->getNumRows();
            cachedPartitionsMemory += p->bytesWritten();
            totalCachedPartitionsMemory += p->size();
            pos++;
        }
        for(auto p : _generalCasePartitions) {
            totalCachedExceptions += p->getNumRows();
            cachedExceptionsMemory += p->bytesWritten();
            totalCachedExceptionsMemory += p->size();
        }

        _normalCaseRowCount = totalCachedRows;
        _generalCaseRowCount = totalCachedExceptions;

        stringstream ss;
        ss<<"Cached "<<pluralize(totalCachedRows, "common row")
          <<" ("<<pluralize(totalCachedExceptions, "general row")
          <<"), memory usage: "<<sizeToMemString(cachedPartitionsMemory)
          <<"/"<<sizeToMemString(totalCachedPartitionsMemory)<<" ("
          <<sizeToMemString(cachedExceptionsMemory)
          <<"/"<<sizeToMemString(totalCachedExceptionsMemory)<<")";
        Logger::instance().defaultLogger().info(ss.str());

#ifndef NDEBUG
        // print schema
        Logger::instance().defaultLogger().info("CACHED common case schema: " + _optimizedSchema.getRowType().desc());
        Logger::instance().defaultLogger().info("CACHED general case schema: " + getOutputSchema().getRowType().desc());
#endif
    }

    size_t CacheOperator::getTotalCachedRows() const {
        size_t totalCachedRows = 0;
        for(auto p : _normalCasePartitions) {
            totalCachedRows += p->getNumRows();
        }
        for(auto p : _generalCasePartitions) {
            totalCachedRows += p->getNumRows();
        }
        return totalCachedRows;
    }
}