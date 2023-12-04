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
            const std::vector<Partition*>& normalPartitions,
            const std::vector<Partition*>& generalPartitions,
            const std::vector<Partition*>& fallbackPartitions,
            const std::vector<PartitionGroup>& partitionGroups,
            int64_t maxRows) : ResultSet::ResultSet() {
        for (const auto &group : partitionGroups)
            _partitionGroups.push_back(group);

        for (const auto &p : normalPartitions)
            _remainingNormalPartitions.push_back(p);
        for (const auto &p : generalPartitions)
            _remainingGeneralPartitions.push_back(p);
        for (const auto &p : fallbackPartitions)
            _remainingFallbackPartitions.push_back(p);

        _curNormalRowCounter = 0;
        _curNormalByteCounter = 0;
        _curGeneralRowCounter = 0;
        _curGeneralByteCounter = 0;
        _curFallbackRowCounter = 0;
        _curFallbackByteCounter = 0;
        _normalRowCounter = 0;
        _generalRowCounter = 0;
        _fallbackRowCounter = 0;
        _totalRowCounter = 0;

        _schema = schema;
        _maxRows = maxRows < 0 ? std::numeric_limits<int64_t>::max() : maxRows;
    }

    void clearPartitions(std::list<Partition*>& partitions) {
        for (auto &partition : partitions) {
            partition->invalidate();
        }
        partitions.clear();
    }

    void ResultSet::clear() {
        clearPartitions(_remainingNormalPartitions);
        clearPartitions(_currentNormalPartitions);
        clearPartitions(_remainingGeneralPartitions);
        clearPartitions(_currentGeneralPartitions);
        clearPartitions(_remainingFallbackPartitions);
        clearPartitions(_currentFallbackPartitions);
        _partitionGroups.clear();

        _curNormalRowCounter = 0;
        _curNormalByteCounter = 0;
        _curGeneralRowCounter = 0;
        _curGeneralByteCounter = 0;
        _curFallbackRowCounter = 0;
        _curFallbackByteCounter = 0;
        _normalRowCounter = 0;
        _generalRowCounter = 0;
        _fallbackRowCounter = 0;
        _totalRowCounter = 0;
        _maxRows = 0;
    }

    bool ResultSet::hasNextNormalPartition() const {
        // all rows already retrieved?
        if (_totalRowCounter >= _maxRows)
            return false;

        // empty?
        if (_currentNormalPartitions.empty() && _remainingNormalPartitions.empty()) {
            return false;
        } else if (!_currentNormalPartitions.empty()) {
            return _curNormalRowCounter < _currentNormalPartitions.front()->getNumRows();
        } else {
            return _remainingNormalPartitions.front()->getNumRows() > 0;
        }
    }

    bool ResultSet::hasNextGeneralPartition() const {
        // all rows already retrieved?
        if (_totalRowCounter >= _maxRows)
            return false;

        // empty?
        if (_currentGeneralPartitions.empty() && _remainingGeneralPartitions.empty()) {
            return false;
        } else if (!_currentGeneralPartitions.empty()) {
            return _curGeneralRowCounter < _currentGeneralPartitions.front()->getNumRows();
        } else {
            return _remainingGeneralPartitions.front()->getNumRows() > 0;
        }
    }

    bool ResultSet::hasNextFallbackPartition() const {
        // all rows already retrieved?
        if (_totalRowCounter >= _maxRows)
            return false;

        // empty?
        if (_currentFallbackPartitions.empty() && _remainingFallbackPartitions.empty()) {
            return false;
        } else if (!_currentFallbackPartitions.empty()) {
            return _curFallbackRowCounter < _currentFallbackPartitions.front()->getNumRows();
        } else {
            return _remainingFallbackPartitions.front()->getNumRows() > 0;
        }
    }

    Partition* ResultSet::getNextGeneralPartition() {
        if (_currentGeneralPartitions.empty() && _remainingGeneralPartitions.empty())
            return nullptr;

        Partition *first = nullptr;
        if (!_currentGeneralPartitions.empty()) {
            first = _currentGeneralPartitions.front();
            _currentGeneralPartitions.pop_front();
        } else {
            first = _remainingGeneralPartitions.front();
            _remainingGeneralPartitions.pop_front();
        }

        auto numRows = first->getNumRows();
        _totalRowCounter += numRows;
        _generalRowCounter += numRows;

        _curGeneralRowCounter = 0;
        _curGeneralByteCounter = 0;

        return first;
    }

    Partition* ResultSet::getNextFallbackPartition() {
        if (_currentFallbackPartitions.empty() && _remainingFallbackPartitions.empty())
            return nullptr;

        Partition *first = nullptr;
        if (!_currentFallbackPartitions.empty()) {
            first = _currentFallbackPartitions.front();
            _currentFallbackPartitions.pop_front();
        } else {
            first = _remainingFallbackPartitions.front();
            _remainingFallbackPartitions.pop_front();
        }

        auto numRows = first->getNumRows();
        _totalRowCounter += numRows;
        _fallbackRowCounter += numRows;

        _curFallbackRowCounter = 0;
        _curFallbackByteCounter = 0;

        return first;
    }

    Partition* ResultSet::getNextNormalPartition() {
        if (_currentNormalPartitions.empty() && _remainingNormalPartitions.empty())
            return nullptr;

        Partition *first = nullptr;
        if (!_currentNormalPartitions.empty()) {
            first = _currentNormalPartitions.front();
            _currentNormalPartitions.pop_front();
        } else {
            first = _remainingNormalPartitions.front();
            _remainingNormalPartitions.pop_front();
        }

        auto numRows = first->getNumRows();
        _totalRowCounter += numRows;
        _normalRowCounter += numRows;

        _curNormalRowCounter = 0;
        _curNormalByteCounter = 0;

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
        if(_currentGeneralPartitions.empty() && _remainingGeneralPartitions.empty() && _currentFallbackPartitions.empty() && _remainingFallbackPartitions.empty()) {
            if (_currentNormalPartitions.empty() && _remainingNormalPartitions.empty())
                return vector<Row>{};

            for (const auto &p : _remainingNormalPartitions)
                _currentNormalPartitions.push_back(p);

            Deserializer ds(_schema);
            for(int i = 0; i < limit;) {

                // all exhausted
                if(_currentNormalPartitions.empty())
                    break;

                // get number of rows in first partition
                Partition *first = _currentNormalPartitions.front();
                auto num_rows = first->getNumRows();
                // how many left to retrieve?
                auto num_to_retrieve_from_partition = std::min(limit - i, num_rows - _curNormalRowCounter);
                if(num_to_retrieve_from_partition <= 0)
                    break;

                // make sure partition schema matches stored schema
                assert(_schema == first->schema());

                // thread safe version (slow)
                // get next element of partition
                const uint8_t* ptr = first->lock();
                for(int j = 0; j < num_to_retrieve_from_partition; ++j) {
                    auto row = Row::fromMemory(ds, ptr + _curNormalByteCounter, first->capacity() - _curNormalByteCounter);
                    _curNormalByteCounter += row.serializedLength();
                    _curNormalRowCounter++;
                    _totalRowCounter++;
                    _normalRowCounter++;
                    v.push_back(row);
                }

                // thread safe version (slow)
                // deserialize
                first->unlock();

                i += num_to_retrieve_from_partition;

                // get next Partition ready when current one is exhausted
                if(_curNormalRowCounter == first->getNumRows())
                    removeFirstNormalPartition();
            }

            v.shrink_to_fit();
            return v;
        } else {
             while (hasNextRow() && v.size() < limit) {
                 v.push_back(getNextRow());
             }
             v.shrink_to_fit();
             return v;
        }
    }

    bool ResultSet::hasNextNormalRow() {
        if (!_currentNormalPartitions.empty() && _curNormalRowCounter < _currentNormalPartitions.front()->getNumRows())
            return true;
        for (const auto &p : _remainingNormalPartitions)
            if (p->getNumRows() > 0)
                return true;
        return false;
    }

    bool ResultSet::hasNextGeneralRow() {
        if (!_currentGeneralPartitions.empty() && _curGeneralRowCounter < _currentGeneralPartitions.front()->getNumRows())
            return true;
        for (const auto &p : _remainingGeneralPartitions)
            if (p->getNumRows() > 0)
                return true;
        return false;
    }

    bool ResultSet::hasNextFallbackRow() {
        if (!_currentFallbackPartitions.empty() && _curFallbackRowCounter < _currentFallbackPartitions.front()->getNumRows())
            return true;
        for (const auto &p : _remainingFallbackPartitions)
            if (p->getNumRows() > 0)
                return true;
        return false;
    }

    bool ResultSet::hasNextRow() {
        // all rows already retrieved?
        if(_totalRowCounter >= _maxRows)
            return false;

        return hasNextNormalRow() || hasNextGeneralRow() || hasNextFallbackRow();
    }

    Row ResultSet::getNextRow() {

        // trace with a lot of print statements
        //auto&logger = Logger::instance().logger("python");
        std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"enter getNexRow "<<std::endl;
        if (_currentNormalPartitions.empty() && _currentFallbackPartitions.empty() && _currentGeneralPartitions.empty()) {
            // logger.info("all current partitions empty, retrieving next partition:");

            // all partitions are exhausted return empty row as default value
            if (_partitionGroups.empty()) {
                std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"partition groups are empty, returning empty row "<<std::endl;
                // logger.info("no partitions left, returnig empty row");
                return Row();
            }

            _normalRowCounter = 0;
            _generalRowCounter = 0;
            _fallbackRowCounter = 0;
            //logger.info("get first partition group");
            auto group = _partitionGroups.front();
            _partitionGroups.pop_front();
            for (int i = group.normalPartitionStartIndex; i < group.normalPartitionStartIndex + group.numNormalPartitions; ++i) {
                if(_remainingNormalPartitions.empty())
                    break; // TODO: need to fix for take (?)
                _currentNormalPartitions.push_back(_remainingNormalPartitions.front());
                _remainingNormalPartitions.pop_front();
            }
            for (int i = group.generalPartitionStartIndex; i < group.generalPartitionStartIndex + group.numGeneralPartitions; ++i) {
                if(_remainingGeneralPartitions.empty())
                    break; // TODO: need to fix for take (?)
                _currentGeneralPartitions.push_back(_remainingGeneralPartitions.front());
                _remainingGeneralPartitions.pop_front();
            }
            for (int i = group.fallbackPartitionStartIndex; i < group.fallbackPartitionStartIndex + group.numFallbackPartitions; ++i) {
                if(_remainingFallbackPartitions.empty())
                    break; // TODO: need to fix for take (?)
                _currentFallbackPartitions.push_back(_remainingFallbackPartitions.front());
                _remainingFallbackPartitions.pop_front();
            }
            //logger.info("getting next row after update");
            std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"next partiton group update done, calling getNextRow recursively. "<<std::endl;
            return getNextRow();
        } else if (_currentNormalPartitions.empty() && _currentFallbackPartitions.empty()) {
            std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"normal + fallback are empty, get next general row "<<std::endl;
            // only general rows remain, return next general row
            return getNextGeneralRow();
        } else if (_currentNormalPartitions.empty() && _currentGeneralPartitions.empty()) {
            std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"normal + general are empty, get next fallback row "<<std::endl;
            // only fallback rows remain, return next fallback row
            return getNextFallbackRow();
        } else if (_currentFallbackPartitions.empty() && _currentGeneralPartitions.empty()) {
            std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"general + fallback are empty, get next normal row "<<std::endl;
            // only normal rows remain, return next normal row
            return getNextNormalRow();
        } else if (_currentFallbackPartitions.empty()) {
            std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"no fallback left, get next normal or general row "<<std::endl;
            // only normal and general rows remain, compare row index
            // emit normal rows until reached current general ind
            if (_normalRowCounter + _generalRowCounter < currentGeneralRowInd()) {
                std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"get normal row "<<std::endl;
                return getNextNormalRow();
            } else {
                std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"get general row "<<std::endl;
                return getNextGeneralRow();
            }
        }  else if (_currentGeneralPartitions.empty()) {
            std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"no general left, get next normal or fallback row "<<std::endl;
            // only normal and fallback rows remain, compare row index
            // emit normal rows until reached current fallback ind
            if (_normalRowCounter + _generalRowCounter + _fallbackRowCounter < currentFallbackRowInd()) {
                std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"get normal row "<<std::endl;
                return getNextNormalRow();
            } else {
                std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"get fallback row "<<std::endl;
                return getNextFallbackRow();
            }
        } else {

            std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"3-way compare, get either normal, general or fallback "<<std::endl;

            // all three cases remain, three-way row comparison
            auto generalRowInd = currentGeneralRowInd();
            auto fallbackRowInd = currentFallbackRowInd();
            if (_normalRowCounter + _generalRowCounter < generalRowInd && _normalRowCounter + _generalRowCounter + _fallbackRowCounter < fallbackRowInd) {
                std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"get normal row "<<std::endl;
                return getNextNormalRow();
            } else if (generalRowInd <= fallbackRowInd) {
                std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"get general row "<<std::endl;
                return getNextGeneralRow();
            } else {
                std::cerr<<__FILE__<<":"<<__LINE__<<":: "<<"get fallback row "<<std::endl;
                return getNextFallbackRow();
            }
        }
    }

    int64_t ResultSet::currentFallbackRowInd() {
        assert(!_currentFallbackPartitions.empty());
        auto p = _currentFallbackPartitions.front();
        auto ptr = p->lock() + _curFallbackByteCounter;
        auto rowInd = *((int64_t*) ptr);
        p->unlock();
        return rowInd;
    }

    int64_t ResultSet::currentGeneralRowInd() {
        assert(!_currentGeneralPartitions.empty());
        auto p = _currentGeneralPartitions.front();
        auto ptr = p->lock() + _curGeneralByteCounter;
        auto rowInd = *((int64_t*) ptr);
        p->unlock();
        return rowInd;
    }

    Row ResultSet::getNextNormalRow() {
        assert (!_currentNormalPartitions.empty());
        auto p = _currentNormalPartitions.front();
        assert(_schema == p->schema());

        auto ptr = p->lock() + _curNormalByteCounter;
        auto capacity = p->capacity() - _curNormalByteCounter;
        auto row = Row::fromMemory(_schema, ptr, capacity);
        p->unlock();

        _curNormalByteCounter += row.serializedLength();
        _curNormalRowCounter++;
        _totalRowCounter++;
        _normalRowCounter++;

        if (_curNormalRowCounter == p->getNumRows()) {
            removeFirstNormalPartition();
        }

        return row;
    }

    Row ResultSet::getNextGeneralRow() {
        assert (!_currentGeneralPartitions.empty());
        auto p = _currentGeneralPartitions.front();
        assert(_schema == p->schema());

        auto prevRowInd = currentGeneralRowInd();
        _curGeneralByteCounter += 4 * sizeof(int64_t);
        auto ptr = p->lock() + _curGeneralByteCounter;
        auto capacity = p->capacity() - _curGeneralByteCounter;
        auto row = Row::fromMemory(_schema, ptr, capacity);
        p->unlock();

        _curGeneralByteCounter += row.serializedLength();
        _curGeneralRowCounter++;

        if (_curGeneralRowCounter == p->getNumRows()) {
            removeFirstGeneralPartition();
        }

        _totalRowCounter++;
        if (_currentGeneralPartitions.empty() || currentGeneralRowInd() > prevRowInd) {
            _generalRowCounter++;
        }

        return row;
    }

    Row ResultSet::getNextFallbackRow() {
        assert (!_currentFallbackPartitions.empty());

        auto prevRowInd = currentFallbackRowInd();
        auto p = _currentFallbackPartitions.front();
        auto ptr = p->lock() + _curFallbackByteCounter;
        auto pyObjectSize = ((int64_t *) ptr)[3]; ptr += 4 * sizeof(int64_t);

        python::lockGIL();
        auto row = python::pythonToRow(python::deserializePickledObject(python::getMainModule(), (char *) ptr, pyObjectSize));
        python::unlockGIL();

        p->unlock();

        _curFallbackByteCounter += pyObjectSize + 4*sizeof(int64_t);
        _curFallbackRowCounter++;

        if (_curFallbackRowCounter == p->getNumRows()) {
            removeFirstFallbackPartition();
        }

        _totalRowCounter++;
        if (_currentFallbackPartitions.empty() || currentFallbackRowInd() > prevRowInd) {
            _fallbackRowCounter++;
        }

        return row;
    }

    size_t ResultSet::rowCount() const {
        size_t count = 0;
        for (const auto& partition : _currentNormalPartitions)
            count += partition->getNumRows();
        for (const auto& partition : _remainingNormalPartitions)
            count += partition->getNumRows();
        for (const auto& partition : _currentGeneralPartitions)
            count += partition->getNumRows();
        for (const auto& partition : _remainingGeneralPartitions)
            count += partition->getNumRows();
        for (const auto& partition : _currentFallbackPartitions)
            count += partition->getNumRows();
        for (const auto& partition : _remainingFallbackPartitions)
            count += partition->getNumRows();
        return count;
    }

    void ResultSet::removeFirstGeneralPartition() {
        assert(!_currentGeneralPartitions.empty());
        Partition *first = _currentGeneralPartitions.front();
        assert(first);

        // invalidate partition
#ifndef NDEBUG
        Logger::instance().defaultLogger().info("ResultSet invalidates partition " + hexAddr(first) + " uuid " + uuidToString(first->uuid()));
#endif
        first->invalidate();

        _currentGeneralPartitions.pop_front();
        _curGeneralRowCounter = 0;
        _curGeneralByteCounter = 0;
    }

    void ResultSet::removeFirstFallbackPartition() {
        assert(!_currentFallbackPartitions.empty());
        Partition *first = _currentFallbackPartitions.front();
        assert(first);

        // invalidate partition
#ifndef NDEBUG
        Logger::instance().defaultLogger().info("ResultSet invalidates partition " + hexAddr(first) + " uuid " + uuidToString(first->uuid()));
#endif
        first->invalidate();

        // remove partition (is now processed)
        _currentFallbackPartitions.pop_front();
        _curFallbackRowCounter = 0;
        _curFallbackByteCounter = 0;
    }

    void ResultSet::removeFirstNormalPartition() {
        assert(!_currentNormalPartitions.empty());
        Partition *first = _currentNormalPartitions.front();
        assert(first);

        // invalidate partition
#ifndef NDEBUG
        Logger::instance().defaultLogger().info("ResultSet invalidates partition " + hexAddr(first) + " uuid " + uuidToString(first->uuid()));
#endif
        first->invalidate();

        // remove partition (is now processed)

        _currentNormalPartitions.pop_front();
        _curNormalRowCounter = 0;
        _curNormalByteCounter = 0;
    }

    size_t ResultSet::fallbackRowCount() const {
        size_t count = 0;
        for (const auto &p : _currentFallbackPartitions)
            count += p->getNumRows();
        for (const auto&p : _remainingFallbackPartitions)
            count += p->getNumRows();
        return count;
    }
}