//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IEXCEPTIONABLETASK_H
#define TUPLEX_IEXCEPTIONABLETASK_H

#include "IExecutorTask.h"
#include <unordered_map>

namespace tuplex {


    // helper function to serialize, deserialize exceptions to memory

    inline uint8_t* serializeExceptionToMemory(int64_t ecCode, int64_t opID, int64_t row, const uint8_t* buf, size_t bufSize, size_t* output_size=nullptr, decltype(malloc) alloc=malloc) {
        auto buffer = (uint8_t*)alloc(4 * sizeof(int64_t) + bufSize);
        assert(buffer);
        // write data out
        int64_t* ib = (int64_t*)buffer;
        *ib = row;
        *(ib + 1) = ecCode;
        *(ib + 2) = opID;
        *(ib + 3) = bufSize;
        memcpy(buffer + 4 * sizeof(int64_t), buf, bufSize);

        if(output_size)
            *output_size = 4 * sizeof(int64_t) + bufSize;
        return buffer;
    }

    inline size_t deserializeExceptionFromMemory(const uint8_t* mem, int64_t* ecCode, int64_t* opID, int64_t *rowNumber, uint8_t const ** buf, size_t* bufSize) {
        assert(mem);
        int64_t* ib = (int64_t*)mem;
        if(rowNumber)
            *rowNumber = ib[0];
        if(ecCode)
            *ecCode = ib[1];
        if(opID)
            *opID = ib[2];
        size_t eSize = ib[3];
        if(buf)
            *buf = mem + sizeof(int64_t) * 4;
        if(bufSize)
            *bufSize = eSize;
        return eSize + 4 * sizeof(int64_t);
    }

    /*!
     * new class, serializes row number, operator ID, row causing exception to partition
     */
    class IExceptionableTask : public IExecutorTask {
    public:
        IExceptionableTask(const Schema &rowSchema, int64_t contextID) : _exceptionRowSchema(rowSchema), _lastPtr(nullptr),
                                                      _startPtr(nullptr), _contextID(contextID) {}

        virtual std::vector<Partition *> getExceptions() const { return _exceptions; }

        /*!
         * returns the number of exceptions in this task, hashed after operatorID and exception code
         * @return
         */
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>
        exceptionCounts() const { return _exceptionCounts; }

        Schema getExceptionSchema() const { return _exceptionRowSchema; }

        void freePartitions() override {}

    protected:
        virtual void unlockAll();

        Schema inputSchema() const { return _exceptionRowSchema; }

        void serializeException(const int64_t exceptionCode, const int64_t exceptionOperatorID,
                                const int64_t rowNumber, const uint8_t *data, const size_t size);

        /*!
         * explicitly set exception array
         * @param v
         */
        void setExceptions(const std::vector<Partition *> &v) {
            _exceptions = v;
        }

        int64_t contextID() const { return _contextID; }

    private:
        Schema _exceptionRowSchema;
        std::vector<Partition *> _exceptions;

        // exception counts (required for sampling etc. later)
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> _exceptionCounts;

        // helps for serializing stuff
        uint8_t *_lastPtr;
        uint8_t *_startPtr;

        int64_t _contextID;

        void makeSpace(Executor *owner, const Schema &schema, size_t size);

        void incNumRows();

        uint64_t bytesWritten() const {
            auto res = _lastPtr - _startPtr;
            assert(res >= sizeof(int64_t));
            return res - sizeof(int64_t);
        }
    };
}
#endif //TUPLEX_IEXCEPTIONABLETASK_H