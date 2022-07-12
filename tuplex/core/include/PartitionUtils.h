//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by March Boonyapaluk first on 4/19/2021                                                                   //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PARTITIONUTILS_H
#define TUPLEX_PARTITIONUTILS_H

#include <vector>
#include <physical/TransformStage.h>
#include <Executor.h>

namespace tuplex {
    /*!
     * Trim list of partitions so that it includes up to the first n rows and the last m rows
     * if n + m > number of rows in input partitions, the partitions will remain unchanged
     * @param partitions [in,out] the list of partitions to trim
     * @param topLimit n, the number of top rows to include
     * @param bottomLimit m, the number of bottom rows to include
     * @param tstage pointer to transform stage, might be used to generate new partition
     * @param exec pointer to executor, might be used to allocate new partition
     */
    void trimPartitionsToLimit(std::vector<Partition *> &partitions, size_t topLimit, size_t bottomLimit,
                               TransformStage *tstage, Executor *exec);

    /*!
     * Create a newly allocated partition with the same data as the specified partition, but with the first n rows removed
     * @param p_in the input partition
     * @param numToSkip number of rows to remove from the new partition
     * @param tstage pointer to transform stage, used to generate new partition
     * @param exec pointer to executor, used to allocate new partition
     * @return the new partition
     */
    Partition *newPartitionWithSkipRows(Partition *p_in,
                                        size_t numToSkip,
                                        TransformStage *tstage,
                                        Executor *exec);

}

#endif //TUPLEX_PARTITIONUTILS_H
