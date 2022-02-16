//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/IExceptionableTask.h>

namespace tuplex {

    void IExceptionableTask::serializeException(const int64_t exceptionCode, const int64_t exceptionOperatorID,
                                                const int64_t rowNumber, const uint8_t *data, const size_t size) {
        using namespace std;


        // make space in helper
        size_t totalSize = sizeof(int64_t) * 4 + size;

        auto owner = this->owner();
        assert(owner);
        auto desc = _exceptionRowSchema.getRowType().desc();
        {
            std::stringstream ss;
            ss << "Making space for " << std::to_string(totalSize);
            Logger::instance().defaultLogger().info(ss.str());
        }
        makeSpace(owner, _exceptionRowSchema, totalSize);

        size_t out_size = 0;
        auto buffer = serializeExceptionToMemory(exceptionCode, exceptionOperatorID, rowNumber, data, size, &out_size);
        assert(out_size == totalSize);
        // write data out
        memcpy(_lastPtr, buffer, out_size);
        _lastPtr += out_size;
        free(buffer);

        incNumRows();

        // add to counts
        auto key = make_tuple(exceptionOperatorID, i32ToEC(exceptionCode));
        auto it = _exceptionCounts.find(key);
        if(it == _exceptionCounts.end())
            _exceptionCounts[key] = 0;
        _exceptionCounts[key]++;
    }

    void IExceptionableTask::makeSpace(Executor *owner, const Schema& schema, size_t size) {
        assert(owner);

        // special case: no partitions yet
        if(!_startPtr) {
            // alloc
            Partition *partition = owner->allocWritablePartition(size + sizeof(int64_t), schema, -1, _contextID);

            assert(partition);

            _startPtr = partition->lockWriteRaw();
            *((int64_t*)_startPtr) = 0;
            _lastPtr = _startPtr + sizeof(int64_t);

            _exceptions.push_back(partition);
        } else {
            assert(_lastPtr >= _startPtr);
            size_t currentSize = _lastPtr - _startPtr;

            Partition *last = _exceptions.back();
            // enough space? If not get new partition
            if(currentSize + size > last->size()) {
                // alloc new
                last->unlockWrite();
                last->setBytesWritten(bytesWritten());

                Partition *partition = owner->allocWritablePartition(size + sizeof(int64_t), schema, -1, _contextID);

                assert(partition);

                _startPtr = partition->lockWriteRaw();
                *((int64_t*)_startPtr) = 0;
                _lastPtr = _startPtr + sizeof(int64_t);

                _exceptions.push_back(partition);
            }
        }
    }

    void IExceptionableTask::incNumRows() {
        *((int64_t*)_startPtr) = *((int64_t*)_startPtr) + 1;
    }

    void IExceptionableTask::unlockAll() {
        if(!_exceptions.empty()) {
            _exceptions.back()->unlockWrite();
            _exceptions.back()->setBytesWritten(bytesWritten());
        }

    }
}