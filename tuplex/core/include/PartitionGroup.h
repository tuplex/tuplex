//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/1/2021                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PARTITIONGROUP_H
#define TUPLEX_PARTITIONGROUP_H

/*!
 * Maintains a grouping of normal, fallback, and general partitions that arise from the
 * same executor in order to be reprocessed after a cache occurs.
 */
namespace tuplex {
    struct PartitionGroup {
        /*!
         * Groups normal, general, and fallback partitions into groups, with all partitions that originated from a single
         * task being grouped together.
         * @param numNormalPartitons number of normal partitions in group
         * @param normalPartitionStartIndex starting index in list of all normal partitions
         * @param numGeneralPartitions number of general partitions in group
         * @param generalPartitionStartIndex starting index in list of all general partitions
         * @param numFallbackPartitions number of fallback partitions in group
         * @param fallbackPartitionStartIndex starting index in list of all fallback partitions
         */
        PartitionGroup(size_t numNormalPartitions, size_t normalPartitionStartIndex,
                       size_t numGeneralPartitions, size_t generalPartitionStartIndex,
                       size_t numFallbackPartitions, size_t fallbackPartitionStartIndex):
                numNormalPartitions(numNormalPartitions), normalPartitionStartIndex(normalPartitionStartIndex),
                numGeneralPartitions(numGeneralPartitions), generalPartitionStartIndex(generalPartitionStartIndex),
                numFallbackPartitions(numFallbackPartitions), fallbackPartitionStartIndex(fallbackPartitionStartIndex) {}

       /*!
        * Initialize empty struct with all values set to zero.
        */
        PartitionGroup() :
                numNormalPartitions(0), numGeneralPartitions(0), numFallbackPartitions(0),
                normalPartitionStartIndex(0), generalPartitionStartIndex(0), fallbackPartitionStartIndex(0) {}

        size_t numNormalPartitions;
        size_t normalPartitionStartIndex;
        size_t numGeneralPartitions;
        size_t generalPartitionStartIndex;
        size_t numFallbackPartitions;
        size_t fallbackPartitionStartIndex;
    };
}

#endif //TUPLEX_PARTITIONGROUP_H
