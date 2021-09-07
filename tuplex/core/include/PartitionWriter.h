//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PARTITIONWRITER_H
#define TUPLEX_PARTITIONWRITER_H

#include <Executor.h>
#include <Schema.h>
#include <Partition.h>
#include <Logger.h>
#include <Row.h>

namespace tuplex {

    class Executor;

    /*!
     * helper class to easily write data to a couple of partitions.
     */
    class PartitionWriter {
    private:
        std::vector<Partition*> _outputPartitions;
        Executor *_executor; //! executor to use for allocating memory
        Schema _schema; //! output schema of partitions
        int64_t _dataSetID; //! dataset ID to which partitions shall belong to
        size_t _defaultPartitionSize; //! default size to alloc new partitions with


        Partition* _currentPartition;
        size_t _capacityLeft;
        uint8_t* _ptr;
        int64_t* _numRowsPtr;

        void makeSpace(size_t bytesRequired);
        void close();
    public:

        /*!
         * create a new PartitionWriter object
         * @param executor executor to use for allocating memory
         * @param schema output schema of partitions
         * @param defaultPartitionSize default size to alloc new partitions with
         */
        PartitionWriter(Executor *executor,
                const Schema& schema,
                int64_t dataSetID,
                size_t defaultPartitionSize) : _executor(executor),
                                               _schema(schema),
                                               _dataSetID(dataSetID),
                                               _defaultPartitionSize(defaultPartitionSize),
                                               _currentPartition(nullptr),
                                               _capacityLeft(0),
                                               _ptr(nullptr),
                                               _numRowsPtr(nullptr){
            assert(executor);
        }


        ~PartitionWriter() {
            close();
        }

        bool writeSerializedRow(const uint8_t* readPtr,
                                                 const uint64_t length);

        bool writeRow(const Row& row);

        bool writeData(const uint8_t* data, size_t size);

        template<class Iterator> bool writeData(Iterator first, Iterator last);

        std::vector<Partition*> getOutputPartitions(bool consume=true);
    };

    template<class Iterator> bool PartitionWriter::writeData(Iterator first, Iterator last) {
        using value_type = typename std::iterator_traits<Iterator>::value_type;

        Serializer serializer;

        Iterator it = first;

        // debug assert, make sure schema match!
        assert(serializationSchema(*(it)).getRowType() == _schema.getRowType());

        while(it != last) {
            // check if there is enough space to write one element to memory
            value_type value = *(it);
            serializer.reset().append(value);

            auto size = serializer.length();
            // write to current partition...
            makeSpace(size);

            // write and decrease capacity
            serializer.serialize(_ptr, size);

            *_numRowsPtr = *_numRowsPtr + 1;
            _ptr += size;
            _capacityLeft -= size;
        }

        return true;
    }

    extern std::vector<Partition*> rowsToPartitions(Executor *executor, int64_t dataSetID, const std::vector<Row>& rows);
}

#endif //TUPLEX_PARTITIONWRITER_H