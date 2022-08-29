//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include <Context.h>
#include "TestUtils.h"

class ExceptionsTest : public PyTest {};

TEST_F(ExceptionsTest, Basic) {
    using namespace tuplex;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    Context c(opts);

    std::vector<Row> inputRows({Row(1), Row(2), Row(0), Row(4), Row(5)});
    auto res = c.parallelize(inputRows).map(UDF("lambda x: 1 // x if x == 0 else x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1")).collectAsVector();
    std::vector<Row> expectedOutput({Row(1), Row(2), Row(-1), Row(4), Row(5)});
    ASSERT_EQ(res.size(), expectedOutput.size());
    for (int i = 0; i < expectedOutput.size(); ++i)
        EXPECT_EQ(res[i].toPythonString(), expectedOutput[i].toPythonString());
}

TEST_F(ExceptionsTest, Debug) {
    using namespace tuplex;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    opts.set("tuplex.partitionSize", "40B");
    Context c(opts);

    std::vector<Row> inputData({
        Row(1), Row(2), Row(0), Row(4),
        Row(5), Row(6), Row(0), Row(8),
    });

    auto res = c.parallelize(inputData).map(UDF("lambda x: 1 // x if x == 0 else x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1")).collectAsVector();
    std::vector<Row> expectedRes({Row(1), Row(2), Row(-1), Row(4), Row(5), Row(6), Row(-1), Row(8)});
    ASSERT_EQ(res.size(), expectedRes.size());
    for (int i = 0; i < expectedRes.size(); ++i) {
        EXPECT_EQ(res[i].toPythonString(), expectedRes[i].toPythonString());
    }
}

typedef bool (*filter_t)(int64_t);

void processPartition(filter_t filter, int64_t* normalPartition, int64_t* totalFilterCounter, int64_t* totalNormalRowCounter, int64_t* totalGeneralRowCounter, int64_t* totalFallbackRowCounter,
                      uint8_t** generalPartitions, int64_t numGeneralPartitons, int64_t* generalIndexOffset, int64_t* generalRowOffset, int64_t* generalByteOffset,
                      uint8_t** fallbackPartitions, int64_t numFallbackPartitons, int64_t* fallbackIndexOffset, int64_t *fallbackRowOffset, int64_t* fallbackByteOffset) {
    uint8_t *curGeneralPtr;
    int64_t curGeneralNumRows = 0;
    if (*generalIndexOffset < numGeneralPartitons) {
        curGeneralPtr = generalPartitions[*generalIndexOffset];
        curGeneralNumRows = *((int64_t*)curGeneralPtr);
        curGeneralPtr += sizeof(int64_t) + *generalByteOffset;
    }

    uint8_t *curFallbackPtr;
    int64_t curFallbackNumRows = 0;
    if (*fallbackIndexOffset < numFallbackPartitons) {
        curFallbackPtr = fallbackPartitions[*fallbackIndexOffset];
        curFallbackNumRows = *((int64_t*)curFallbackPtr);
        curFallbackPtr += sizeof(int64_t) + *fallbackByteOffset;
    }

    int64_t numNormalRows = normalPartition[0];
    for (int normalRowCountVar = 1; normalRowCountVar < numNormalRows + 1; ++normalRowCountVar) {
        int64_t curNormalRow = normalPartition[normalRowCountVar];
        if (filter(curNormalRow)) {
            int64_t curNormalRowInd = normalRowCountVar + *totalNormalRowCounter;

            while (*generalRowOffset < curGeneralNumRows && *((int64_t*)curGeneralPtr) < curNormalRowInd + *totalGeneralRowCounter) {
                *curGeneralPtr -= *totalFilterCounter;
                curGeneralPtr += 4 * sizeof(int64_t) + ((int64_t*)curGeneralPtr)[3];
                *generalByteOffset += 4 * sizeof(int64_t) + ((int64_t*)curGeneralPtr)[3];
                *generalRowOffset += 1;
                *totalGeneralRowCounter += 1;

                if (*generalRowOffset == curGeneralNumRows && *generalIndexOffset < numGeneralPartitons - 1) {
                    *generalIndexOffset += 1;
                    *generalRowOffset = 0;
                    *generalByteOffset = 0;
                    curGeneralPtr = generalPartitions[*generalIndexOffset];
                    curGeneralNumRows = *curGeneralPtr;
                    curGeneralPtr += sizeof(int64_t);
                }
            }

            while (*fallbackRowOffset < curFallbackNumRows && *((int64_t*)curFallbackPtr) < curNormalRowInd + *totalGeneralRowCounter + *totalFallbackRowCounter) {
                *curFallbackPtr -= *totalFilterCounter;
                curFallbackPtr += 4 * sizeof(int64_t) + ((int64_t*)curFallbackPtr)[3];
                *fallbackByteOffset += 4 * sizeof(int64_t) + ((int64_t*)curFallbackPtr)[3];
                *fallbackRowOffset += 1;
                *totalFallbackRowCounter += 1;

                if (*fallbackRowOffset == curFallbackNumRows && *fallbackIndexOffset < numFallbackPartitons - 1) {
                    *fallbackIndexOffset += 1;
                    *fallbackRowOffset = 0;
                    *fallbackByteOffset = 0;
                    curFallbackPtr = fallbackPartitions[*fallbackIndexOffset];
                    curFallbackNumRows = *curFallbackPtr;
                    curFallbackPtr += sizeof(int64_t);
                }
            }

            *totalFilterCounter += 1;
        }
    }
    *totalNormalRowCounter += numNormalRows;
}

void processPartitions(filter_t filter, int64_t** normalPartitions, int64_t numNormalPartitions, uint8_t** generalPartitions, int64_t numGeneralPartitions, uint8_t** fallbackPartitions, int64_t numFallbackPartitions) {
    int64_t totalNormalRowCounter = 0;
    int64_t totalGeneralRowCounter = 0;
    int64_t totalFallbackRowCounter = 0;
    int64_t totalFilterCounter = 0;

    int64_t generalIndexOffset = 0;
    int64_t generalByteOffset = 0;
    int64_t generalRowOffset = 0;
    int64_t fallbackIndexOffset = 0;
    int64_t fallbackByteOffset = 0;
    int64_t fallbackRowOffset = 0;
    for (int i = 0; i < numNormalPartitions; ++i) {
        processPartition(filter, normalPartitions[i],
                         &totalFilterCounter, &totalNormalRowCounter, &totalGeneralRowCounter, &totalFallbackRowCounter,
                         generalPartitions, numGeneralPartitions, &generalIndexOffset, &generalRowOffset, &generalByteOffset,
                         fallbackPartitions, numFallbackPartitions, &fallbackIndexOffset, &fallbackRowOffset, &fallbackByteOffset);
    }

    if (generalIndexOffset < numGeneralPartitions) {
        auto curGeneralPtr = generalPartitions[generalIndexOffset];
        auto numRowsInPartition = *((int64_t*)curGeneralPtr);
        curGeneralPtr += sizeof(int64_t) + generalByteOffset;
        while (generalRowOffset < numRowsInPartition) {
            *((int64_t*)curGeneralPtr) -= totalFilterCounter;
            curGeneralPtr += 4 * sizeof(int64_t) + ((int64_t*)curGeneralPtr)[3];
            generalRowOffset += 1;

            if (generalRowOffset == numRowsInPartition && generalIndexOffset < numGeneralPartitions - 1) {
                generalIndexOffset += 1;
                curGeneralPtr = generalPartitions[generalIndexOffset];
                numRowsInPartition = *((int64_t*)curGeneralPtr);
                curGeneralPtr += sizeof(int64_t);
                generalByteOffset = 0;
                generalRowOffset = 0;
            }
        }
    }

    if (fallbackIndexOffset < numFallbackPartitions) {
        auto curFallbackPtr = fallbackPartitions[fallbackIndexOffset];
        auto numRowsInPartition = *((int64_t*)curFallbackPtr);
        curFallbackPtr += sizeof(int64_t) + fallbackByteOffset;
        while (fallbackRowOffset < numRowsInPartition) {
            *((int64_t*)curFallbackPtr) -= totalFilterCounter;
            curFallbackPtr += 4 * sizeof(int64_t) + ((int64_t*)curFallbackPtr)[3];
            fallbackRowOffset += 1;

            if (fallbackRowOffset == numRowsInPartition && fallbackIndexOffset < numFallbackPartitions - 1) {
                fallbackIndexOffset += 1;
                curFallbackPtr = fallbackPartitions[fallbackIndexOffset];
                numRowsInPartition = *((int64_t*)curFallbackPtr);
                curFallbackPtr += sizeof(int64_t);
                fallbackByteOffset = 0;
                fallbackRowOffset = 0;
            }
        }
    }
}

bool filter2(int64_t row) {
    return row % 3 == 0;
}

TEST_F(ExceptionsTest, Algo) {
    int64_t n1[] = {15, 1, 2, 3, 5, 6, 7, 9, 10, 11, 13, 14, 15, 17, 18, 19};
    int64_t n2[] = {3, 21, 22, 23};
    int64_t *normalPartitions[] = {n1, n2};
    int64_t numNormalPartitions = 2;

    int64_t g1[] = {4,
                    0, -1, -1, 8, -1,
                    4, -1, -1, 8, -1,
                    8, -1, -1, 8, -1,
                    12, -1, -1, 8, -1};
    int64_t g2[] = {3,
                    16, -1, -1, 8, -1,
                    20, -1, -1, 8, -1,
                    24, -1, -1, 8, -1};
    uint8_t *generalPartitions[] = {(uint8_t*)g1, (uint8_t*)g2};
    int64_t numGeneralPartitions = 2;

    uint8_t *fallbackPartitions[] = {};
    int64_t numFallbackPartitions = 0;

    processPartitions(filter2, normalPartitions, numNormalPartitions, generalPartitions, numGeneralPartitions, fallbackPartitions, numFallbackPartitions);


    std::cout << "Done";
}


bool filter1(int64_t row) {
    return true;
}

TEST_F(ExceptionsTest, ProcessDebug) {
    int64_t n1[] = {2, 1, 2};
    int64_t *normalPartitions[] = {n1};
    int64_t numNormalPartitions = 1;

    int64_t g1[] = {3,
                    1, -1, -1, 8, -1,
                    2, -1, -1, 8, -1,
                    3, -1, -1, 8, -1};
    uint8_t *generalPartitions[] = {(uint8_t*)g1};
    int64_t numGeneralPartitions = 1;

    uint8_t *fallbackPartitions[] = {};
    int64_t numFallbackPartitions = 0;

    processPartitions(filter1, normalPartitions, numNormalPartitions, generalPartitions, numGeneralPartitions, fallbackPartitions, numFallbackPartitions);
}