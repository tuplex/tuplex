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

namespace tuplex {
    struct PartitionGroup {
    public:
        /*!
         * Groups normal, general, and fallback partitions into groups, with all partitions that originated from a single
         * task being grouped together.
         * @param numNormalPartitons number of normal partitions in group
         * @param normalPartitionStartInd starting index in list of all normal partitions
         * @param numGeneralPartitions number of general partitions in group
         * @param generalPartitionStartInd starting index in list of all general partitions
         * @param numFallbackPartitions number of fallback partitions in group
         * @param fallbackPartitionStartInd starting index in list of all fallback partitions
         */
        PartitionGroup(size_t numNormalPartitons, size_t normalPartitionStartInd,
                       size_t numGeneralPartitions=0, size_t generalPartitionStartInd=0,
                       size_t numFallbackPartitions=0, size_t fallbackPartitionStartInd=0,
                       size_t numExceptionPartitions=0, size_t exceptionPartitionStartInd=0):
                       numNormalPartitions(numNormalPartitons), normalPartitionStartInd(normalPartitionStartInd),
                       numGeneralPartitions(numGeneralPartitions), generalPartitionStartInd(generalPartitionStartInd),
                       numFallbackPartitions(numFallbackPartitions), fallbackPartitionStartInd(fallbackPartitionStartInd),
                       numExceptionPartitions(numExceptionPartitions), exceptionPartitionStartInd(exceptionPartitionStartInd) {}

       /*!
        * Initialize empty struct with all values set to zero.
        */
        PartitionGroup() :
            numNormalPartitions(0), numGeneralPartitions(0), numFallbackPartitions(0),
            normalPartitionStartInd(0), generalPartitionStartInd(0), fallbackPartitionStartInd(0),
            numExceptionPartitions(0), exceptionPartitionStartInd(0) {}

        size_t numNormalPartitions;
        size_t normalPartitionStartInd;
        size_t numGeneralPartitions;
        size_t generalPartitionStartInd;
        size_t numFallbackPartitions;
        size_t fallbackPartitionStartInd;
        size_t numExceptionPartitions;
        size_t exceptionPartitionStartInd;
    };
}

#endif //TUPLEX_PARTITIONGROUP_H
