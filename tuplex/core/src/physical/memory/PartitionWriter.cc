//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/memory/PartitionWriter.h>


static const unsigned scaleFactor = 2;

namespace tuplex {

    void PartitionWriter::makeSpace(size_t bytesRequired) {
        if(bytesRequired > _capacityLeft) {
            // alloc new partition
            auto requiredSize = std::max(_defaultPartitionSize, sizeof(int64_t) + scaleFactor * bytesRequired);

            if(!_currentPartition) {
                _currentPartition = _executor->allocWritablePartition(requiredSize, _schema, _dataSetID, _contextID);
            } else {
                // add current partition to output list
                _currentPartition->unlockWrite();
                _outputPartitions.emplace_back(_currentPartition);
                _currentPartition = _executor->allocWritablePartition(requiredSize, _schema, _dataSetID, _contextID);
            }

            // lock & init
            _ptr = _currentPartition->lockWriteRaw();
            _numRowsPtr = (int64_t*)_ptr;
            *_numRowsPtr = 0;
            _ptr += sizeof(int64_t);
            _capacityLeft = _currentPartition->capacity();
        }

        assert(_currentPartition);
        assert(bytesRequired <= _capacityLeft);
    }

    bool PartitionWriter::writeRow(const tuplex::Row &row) {
        // check rows match
        if(python::Type::propagateToTupleType(row.getRowType()) != _schema.getRowType()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().error("attempting to write row with type " +
            row.getRowType().desc() + " to partition with type " + _schema.getRowType().desc());
#endif
            return false;
        }

        size_t bytesRequired = row.serializedLength();

        // write to current partition...
        makeSpace(bytesRequired);

        // write and decrease capacity

        size_t length = row.serializeToMemory(_ptr, _capacityLeft);
        assert(length == bytesRequired);

        *_numRowsPtr = *_numRowsPtr + 1;
        _ptr += bytesRequired;
        _capacityLeft -= bytesRequired;

        return true;
    }

    bool PartitionWriter::writeData(const uint8_t *data, size_t size) {
        // write to current partition...
        makeSpace(size);

        // write and decrease capacity
        memcpy(_ptr, data, size);

        *_numRowsPtr = *_numRowsPtr + 1;
        _ptr += size;
        _capacityLeft -= size;

        return true;
    }

    void PartitionWriter::close() {
        if(_currentPartition) {
            if(_currentPartition->isLocked())
                _currentPartition->unlockWrite();
            _outputPartitions.emplace_back(_currentPartition);
        }

        _currentPartition = nullptr;
        _ptr = nullptr;
        _numRowsPtr = nullptr;
        _capacityLeft = 0;
    }

    std::vector<Partition*> PartitionWriter::getOutputPartitions(bool consume) {
        close(); // close writer (may be reopened using makeSpace)

        if(consume) {
            auto out = _outputPartitions;
            _outputPartitions.clear();
            return out;
        } else return _outputPartitions;
    }

    std::vector<Partition*> rowsToPartitions(Executor *executor, int64_t dataSetID, int64_t contextID, const std::vector<Row>& rows) {
        if(rows.empty())
            return std::vector<Partition*>();

        auto schema = rows.front().getSchema();

        PartitionWriter pw(executor, schema, dataSetID, contextID, executor->blockSize());

        for(auto row : rows) {
            pw.writeRow(row);
        }

        return pw.getOutputPartitions();
    }
}