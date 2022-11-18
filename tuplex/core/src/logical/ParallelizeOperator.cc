//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/ParallelizeOperator.h>

namespace tuplex {
    ParallelizeOperator::ParallelizeOperator(const Schema& schema,
            const std::vector<Partition*>& normalPartitions,
            const std::vector<std::string>& columns,
            const SamplingMode& sampling_mode) : _normalPartitions(normalPartitions),
                                                 _columnNames(columns),
                                                 _samplingMode(sampling_mode) {

        setOutputSchema(schema);

        // parallelize operator holds data in memory for infinite lifetime.
        // => make partitions immortal
        for(auto& partition : _normalPartitions)
            partition->makeImmortal();

        // get sample
        initSample();
    }

    void ParallelizeOperator::initSample() {
        _sample.clear();

        // todo: general python objects from parallelize...
        if(!_normalPartitions.empty()) {
           auto maxRows = getDataSet() ? getDataSet()->getContext()->getOptions().SAMPLE_MAX_DETECTION_ROWS() : MAX_TYPE_SAMPLING_ROWS; // @TODO: change this variable/config name

           // fetch up to maxRows from partitions!
           auto schema = _normalPartitions.front()->schema();
           Deserializer ds(schema);
           size_t rowCount = 0;
           size_t numBytesRead = 0;
           for(auto p : _normalPartitions) {
               const uint8_t* ptr = p->lockRaw();
               auto partitionRowCount = *(int64_t*)ptr;
               ptr += sizeof(int64_t);
               numBytesRead = sizeof(int64_t);

               // decode rows
               for(unsigned i = 0; i < partitionRowCount && rowCount < maxRows; ++i) {
                   auto row = Row::fromMemory(ds, ptr, p->size() - numBytesRead);
                   numBytesRead += row.serializedLength();
                   ptr += row.serializedLength();
                   _sample.push_back(row);
                   rowCount++;
               }

               p->unlock();
           }
        }
    }

    std::vector<tuplex::Partition*> ParallelizeOperator::getNormalPartitions() {
        return _normalPartitions;
    }

    bool ParallelizeOperator::good() const {
        return true;
    }

    std::vector<Row> ParallelizeOperator::getSample(const size_t num) const {
        // samples exist?
        if(_normalPartitions.empty() || 0 == num) {
            return std::vector<Row>();
        }

        // retrieve whatever is there.
        return std::vector<Row>(_sample.begin(), _sample.begin() + std::min(num, _sample.size()));

        // // go through partitions and retrieve additional samples if stored sample is not enough.
        // if(num <= _firstRowsSample.size()) {
        //
        // } else {
        //     throw std::runtime_error("not yet implemented, please chose smaller sample size");
        //     return std::vector<Row>();
        // }

        // deprecated
        // std::vector<Row> v;
        // for(auto partition : _normalCasePartitions) {
        //     auto numRowsInPartition = partition->getNumRows();
        //     const uint8_t* ptr = partition->lock();
        //     size_t byteCounter = 0;
        //     for(unsigned i = 0; i < numRowsInPartition; ++i) {
        //         auto row = Row::fromMemory(schema(), ptr + byteCounter, partition->capacity() - byteCounter);
        //         byteCounter += row.serializedLength();
        //
        //         v.push_back(row);
        //         if(v.size() == num) {
        //             // unlock & return
        //             partition->unlock();
        //             return v;
        //         }
        //     }
        //     partition->unlock();
        // }
        //
        // // this returns min of num and available samples automatically
        // return v;
    }

    std::shared_ptr<LogicalOperator> ParallelizeOperator::clone(bool cloneParents) {
        auto copy = new ParallelizeOperator(getOutputSchema(), _normalPartitions, columns(), _samplingMode);
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        copy->setFallbackPartitions(_fallbackPartitions);
        copy->setPartitionGroups(_partitionGroups);
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }

    int64_t ParallelizeOperator::cost() const {
        // use #rows stored in partitions
        int64_t numRows = 0;
        for(const auto& p : _normalPartitions)
            numRows += p->getNumRows();
        for(const auto& p : _fallbackPartitions)
            numRows += p->getNumRows();
        return numRows;
    }
}