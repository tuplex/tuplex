//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/ResultSet.h>
#include <limits>

namespace tuplex {
    ResultSet::ResultSet(const Schema& schema,
            const std::vector<Partition*>& partitions,
            const std::vector<Partition*>& exceptions,
            const std::unordered_map<std::string, std::tuple<size_t, size_t, size_t>>& partitionToExceptionsMap,
            const std::vector<std::tuple<size_t, PyObject*>> pyobjects,
            int64_t maxRows) : ResultSet::ResultSet() {
        for(Partition *p : partitions)
            _partitions.push_back(p);

        _pyobjects = std::deque<std::tuple<size_t, PyObject*>>(pyobjects.begin(), pyobjects.end());
        _exceptions = exceptions;
        _partitionToExceptionsMap = partitionToExceptionsMap;
        _curRowCounter = 0;
        _totalRowCounter = 0;
        _byteCounter = 0;
        _schema = schema;
        _maxRows = maxRows < 0 ? std::numeric_limits<int64_t>::max() : maxRows;
        _rowsRetrieved = 0;
    }

    void ResultSet::clear() {
        for(auto partition : _partitions)
            partition->invalidate();
        _partitions.clear();
        for(auto partition : _exceptions)
            partition->invalidate();

        _curRowCounter = 0;
        _byteCounter = 0;
        _maxRows = 0;
        _rowsRetrieved = 0;
    }

    bool ResultSet::hasNextRow() {

        // all rows already retrieved?
        if(_rowsRetrieved >= _maxRows)
            return false;

        // empty?
        if(_partitions.empty() && _pyobjects.empty())
            return false;
        else {
            // partitions empty?
            if(_partitions.empty())
                return true;
            else if(_pyobjects.empty()) {
                assert(_partitions.size() > 0);
                assert(_partitions.front());

                // still one row left?
                return _curRowCounter < _partitions.front()->getNumRows();
            } else {
                return true; // there's for sure at least one object left!
            }
        }

    }


    bool ResultSet::hasNextPartition() const {
        // all rows already retrieved?
        if(_rowsRetrieved >= _maxRows)
            return false;

        // empty?
        if(_partitions.empty())
            return false;
        else {
            assert(_partitions.size() > 0);
            assert(_partitions.front());

            // still one row left?
            return _curRowCounter < _partitions.front()->getNumRows();
        }
    }

    Partition* ResultSet::getNextPartition() {
        if(_partitions.empty())
            return nullptr;

        assert(_partitions.size() > 0);

        Partition *first = _partitions.front();
        assert(_schema == first->schema());

        auto numRows = first->getNumRows();
        _rowsRetrieved += numRows;

        _partitions.pop_front();
        _curRowCounter = 0;
        _byteCounter = 0;

        return first;
    }

    std::vector<Row> ResultSet::getRows(size_t limit) {
        using namespace std;

        if(0 == limit)
            return vector<Row>{};

        vector<Row> v;

        // reserve up to 32k (do NOT always execute because special case upper limit is used)
        if(limit < 32000)
            v.reserve(limit);

        // do a quick check whether there are ANY pyobjects, if not deserialize quickly!
        if(_pyobjects.empty()) {

            if(_partitions.empty())
                return vector<Row>{};

            Deserializer ds(_schema);
            for(int i = 0; i < limit;) {

                // all exhausted
                if(_partitions.empty())
                    break;

                // get number of rows in first partition
                Partition *first = _partitions.front();
                auto num_rows = first->getNumRows();
                // how many left to retrieve?
                auto num_to_retrieve_from_partition = std::min(limit - i, num_rows - _curRowCounter);
                if(num_to_retrieve_from_partition <= 0)
                    break;

                // make sure partition schema matches stored schema
                assert(_schema == first->schema());

                // thread safe version (slow)
                // get next element of partition
                const uint8_t* ptr = first->lock();
                for(int j = 0; j < num_to_retrieve_from_partition; ++j) {
                    auto row = Row::fromMemory(ds, ptr + _byteCounter, first->capacity() - _byteCounter);
                    _byteCounter += row.serializedLength();
                    _curRowCounter++;
                    _rowsRetrieved++;
                    _totalRowCounter++;
                    v.push_back(row);
                }

                // thread safe version (slow)
                // deserialize
                first->unlock();

                i += num_to_retrieve_from_partition;

                // get next Partition ready when current one is exhausted
                if(_curRowCounter == first->getNumRows())
                    removeFirstPartition();
            }

            v.shrink_to_fit();
            return v;
        } else {
            // fallback solution:
            // @TODO: write faster version with proper merging!

             std::vector<Row> v;
             while (hasNextRow() && v.size() < limit) {
                 v.push_back(getNextRow());
             }
             v.shrink_to_fit();
             return v;
        }
    }

    Row ResultSet::getNextRow() {
        // merge rows from objects
        if(!_pyobjects.empty()) {
            auto row_number = std::get<0>(_pyobjects.front());
            auto obj = std::get<1>(_pyobjects.front());

            // partitions empty?
            // => simply return next row. no fancy merging possible
            // else merge based on row number.
            if(_partitions.empty() || row_number <= _totalRowCounter) {
                // merge
                python::lockGIL();
                auto row = python::pythonToRow(obj);
                python::unlockGIL();
                _pyobjects.pop_front();
                _rowsRetrieved++;

                // update row counter (not for double indices which could occur from flatMap!)
                if(_pyobjects.empty())
                    _totalRowCounter++;
                else {
                    auto next_row_number = std::get<0>(_pyobjects.front());
                    if(next_row_number != row_number)
                        _totalRowCounter++;
                }

                return row;
            }
        }

        // check whether entry is available, else return empty row
        if(_partitions.empty())
            return Row();

        assert(_partitions.size() > 0);
        Partition *first = _partitions.front();

        // make sure partition schema matches stored schema
        assert(_schema == first->schema());

        Row row;

        // thread safe version (slow)
        // get next element of partition
        const uint8_t* ptr = first->lock();

        row = Row::fromMemory(_schema, ptr + _byteCounter, first->capacity() - _byteCounter);

        // thread safe version (slow)
        // deserialize
        first->unlock();

        _byteCounter += row.serializedLength();
        _curRowCounter++;
        _rowsRetrieved++;
        _totalRowCounter++;

        // get next Partition ready when current one is exhausted
        if(_curRowCounter == first->getNumRows())
            removeFirstPartition();

        return row;
    }

    size_t ResultSet::rowCount() const {
        size_t count = 0;
        for(const auto& partition : _partitions) {
            count += partition->getNumRows();
        }
        return count + _pyobjects.size();
    }

    void ResultSet::removeFirstPartition() {
        assert(_partitions.size() > 0);
        Partition *first = _partitions.front();
        assert(first);

        // invalidate partition
#ifndef NDEBUG
        Logger::instance().defaultLogger().info("ResultSet invalidates partition " + hexAddr(first) + " uuid " + uuidToString(first->uuid()));
#endif
        first->invalidate();

        // remove partition (is now processed)
        _partitions.pop_front();
        _curRowCounter = 0;
        _byteCounter = 0;
    }
}