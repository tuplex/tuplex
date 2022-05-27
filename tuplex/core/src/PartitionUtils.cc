//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by March Boonyapaluk first on 4/19/2021                                                                   //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "PartitionUtils.h"

namespace tuplex {

    void trimPartitionsToLimit(std::vector<Partition *> &partitions,
                               size_t topLimit,
                               size_t bottomLimit,
                               TransformStage *tstage,
                               Executor *exec) {
        std::vector<Partition *> limitedPartitions, limitedTailPartitions;

        // check top output limit, adjust partitions if necessary
        size_t numTopOutputRows = 0;
        Partition *lastTopPart = nullptr;
        size_t clippedTop = 0;
        for (auto partition: partitions) {
            numTopOutputRows += partition->getNumRows();
            lastTopPart = partition;
            if (numTopOutputRows >= topLimit) {
                // clip last partition & leave loop
                clippedTop = topLimit - (numTopOutputRows - partition->getNumRows());
                assert(clippedTop <= partition->getNumRows());
                break;
            } else if (partition == partitions.back()) {
                // last partition, mark full row, but don't put to output set yet to avoid double put
                clippedTop = partition->getNumRows();
                break;
            } else {
                // put full partition to output set
                limitedPartitions.push_back(partition);
            }
        }

        // check the bottom output limit, adjust partitions if necessary
        size_t numBottomOutputRows = 0;
        size_t clippedBottom = 0;
        for (auto it = partitions.rbegin(); it != partitions.rend(); it++) {
            auto partition = *it;
            numBottomOutputRows += partition->getNumRows();

            if (partition == lastTopPart) {
                // the bottom and the top partitions are overlapping
                clippedBottom = bottomLimit - (numBottomOutputRows - partition->getNumRows());
                if (clippedTop + clippedBottom >= partition->getNumRows()) {
                    // if top and bottom range intersect, use full partitions
                    clippedTop = partition->getNumRows();
                    clippedBottom = 0;
                }
                break;
            } else if (numBottomOutputRows >= bottomLimit) {
                // clip last partition & leave loop
                auto clipped = bottomLimit - (numBottomOutputRows - partition->getNumRows());
                assert(clipped <= partition->getNumRows());
                if (clipped > 0) {
                    Partition *newPart = newPartitionWithSkipRows(partition, partition->getNumRows() - clipped, tstage,
                                                                  exec);
                    assert(newPart->getNumRows() == clipped);
                    limitedTailPartitions.push_back(newPart);
                }
                partition->invalidate();
                break;
            } else {
                // put full partition to output set
                limitedTailPartitions.push_back(partition);
            }
        }

        // push the middle partition
        if (lastTopPart != nullptr && (clippedTop > 0 || clippedBottom > 0)) {
            assert(clippedTop + clippedBottom <= lastTopPart->getNumRows());

            // split into two partitions with both top and bottom are in the same partition
            Partition *lastBottomPart = nullptr;

            if (clippedBottom != 0) {
                lastBottomPart = newPartitionWithSkipRows(lastTopPart, lastTopPart->getNumRows() - clippedBottom,
                                                          tstage, exec);
            }

            if (clippedTop != 0) {
                lastTopPart->setNumRows(clippedTop);
                limitedPartitions.push_back(lastTopPart);
            } else {
                lastTopPart->invalidate();
            }

            if (lastBottomPart != nullptr) {
                limitedPartitions.push_back(lastBottomPart);
            }
        }

        if (partitions.size() != limitedPartitions.size() + limitedTailPartitions.size()) {
            // partition is changed, we need to change the partition grouping too
            std::vector<PartitionGroup> oldGrouping = tstage->partitionGroups();
            std::vector<PartitionGroup> newGrouping;
            size_t new_normal_num = limitedPartitions.size() + limitedTailPartitions.size();
            // remove all normal partition, put new one at the front
            newGrouping.push_back(PartitionGroup(new_normal_num, 0, 0, 0, 0, 0));
            for (auto gp: oldGrouping) {
                gp.numNormalPartitions = 0;
                newGrouping.push_back(gp);
            }

            tstage->setPartitionGroups(newGrouping);
        }

        // merge the head and tail partitions
        partitions.clear();
        partitions.insert(partitions.end(), limitedPartitions.begin(), limitedPartitions.end());
        partitions.insert(partitions.end(), limitedTailPartitions.rbegin(), limitedTailPartitions.rend());
    }

    Partition *newPartitionWithSkipRows(Partition *p_in, size_t numToSkip, TransformStage *tstage, Executor *exec) {
        auto ptr = p_in->lockRaw();
        auto num_rows = *((int64_t *) ptr);
        assert(numToSkip < num_rows);

        ptr += sizeof(int64_t);
        size_t numBytesToSkip = 0;

        Deserializer ds(tstage->outputSchema());
        for (unsigned i = 0; i < numToSkip; ++i) {
            Row r = Row::fromMemory(ds, ptr, p_in->capacity() - numBytesToSkip);
            ptr += r.serializedLength();
            numBytesToSkip += r.serializedLength();
        }

        Partition *p_out = exec->allocWritablePartition(p_in->size() - numBytesToSkip + sizeof(int64_t),
                                                        tstage->outputSchema(), tstage->outputDataSetID(),
                                                        tstage->context().id());
        assert(p_out->capacity() >= p_in->size() - numBytesToSkip);

        auto ptr_out = p_out->lockWriteRaw();
        *((int64_t *) ptr_out) = p_in->getNumRows() - numToSkip;
        ptr_out += sizeof(int64_t);
        memcpy((void *) ptr_out, ptr, p_in->size() - numBytesToSkip);
        p_out->setNumRows(p_in->getNumRows() - numToSkip);
        p_out->unlock();

        p_in->unlock();

        return p_out;
    }
} // namespace tuplex