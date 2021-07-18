//
// Created by Colby Anderson on 6/2/21.
//

#include <PartitionWriter.h>
#include "Row.h"

// pair<pair<index # into partitions, row # into partition>, pair<beg. offset, end. offset>>
using PartitionSortType = std::pair<std::pair<size_t, size_t>, std::pair<size_t, size_t>>;

// gets the given number of bytes allocated for a specific
// fixed python type
static size_t fixedLengthTypeSerializationSize(const python::Type& type) {
    assert(type.isNumericType());
    if (type.isNumericType()) {
        return sizeof(int64_t);
    } else {
        throw std::runtime_error("Error: Couldn't recognize fixed type");
    }
}

// offsets of the form <offset of beginning of row from start of partition...
// ..., offset of end of row from start of partition> are dynamically
// calculated by iterating through the rows of the partition and getting
// the serialized lengths
static std::vector<PartitionSortType> dynamicallyComputeOffsets(const tuplex::Schema& schema, const size_t partitionCapacity, const uint8_t* ptr, const int64_t numRows, int partitionNum = 0) {
    std::vector<PartitionSortType> offsets;
    offsets.reserve(numRows);
    size_t totalAllocated = 0;
    for (size_t i = 0; i < numRows; i++) {
        tuplex::Row row = tuplex::Row::fromMemory(schema, ptr + totalAllocated, partitionCapacity - totalAllocated);
        size_t length = row.serializedLength();
        auto offset = std::make_pair(totalAllocated, totalAllocated + length);
        offsets.emplace_back(std::make_pair(std::make_pair(partitionNum, i), offset));
        totalAllocated += length;
    }
    return offsets;
}

// offsets of the form <offset of beginning of row from start of partition...
// ..., offset of end of row from start of partition> are statically computed
// by first calculating how many bytes each column type takes up and adding this
// number up to be the total number of bytes the row takes up. Then, the offsets
// are computed from the row length information since each row should be the same
// number of bytes.
static std::vector<PartitionSortType> staticallyComputeOffsets(const std::vector<python::Type>& colTypes, int64_t numRows, int partitionNum = 0) {
    std::vector<PartitionSortType> offsets;
    int bytesPerRow = 0;
    for (const python::Type& colType : colTypes) {
        bytesPerRow += fixedLengthTypeSerializationSize(colType);
    }
    int totalAllocated = 0;
    for (int i = 0; i < numRows; i++) {
        auto offset = std::make_pair(totalAllocated, totalAllocated + bytesPerRow);
        offsets.emplace_back(std::make_pair(std::make_pair(partitionNum, i), offset));
        totalAllocated += bytesPerRow;
    }
    return offsets;
}

// offsets of the form <offset of beginning of row from start of partition...
// ..., offset of end of row from start of partition> are computed either
// statically or dynamically. If the types of each column are a fixed length
// type, this function will call a helper function to statically generate
// the offsets. Otherwise, it will can a helper function to dynamically
// generate the offsets.
std::vector<PartitionSortType> computeOffsets(const tuplex::Schema& schema, const size_t partitionCapacity, const uint8_t* ptr, int64_t numRows, int partitionNum = 0) {
    python::Type type = schema.getRowType();
    std::vector<python::Type> colTypes = type.parameters();
    auto aa = python::Type::GENERICTUPLE.hash();
    auto aab = python::Type::GENERICLIST.hash();
    auto aabb = python::Type::GENERICDICT.hash();
    auto aabbb = python::Type::ANY.hash();
    auto aabbbb = python::Type::BOOLEAN.hash();
    auto aac = python::Type::EMPTYTUPLE.hash();
    auto aad = python::Type::EMPTYDICT.hash();
    auto aae = python::Type::EMPTYLIST.hash();
    auto aaf = python::Type::VOID.hash();
    auto aag = python::Type::F64.hash();
    auto aah = python::Type::INF.hash();
    auto aai = python::Type::MATCHOBJECT.hash();
    auto aaj = python::Type::PYOBJECT.hash();
    auto aaq = python::Type::MODULE.hash();
    auto aar = python::Type::NULLVALUE.hash();
    auto aas = python::Type::RANGE.hash();
    auto aat = python::Type::UNKNOWN.hash();
    auto aau = python::Type::STRING.hash();
    auto fixedTypeComparator = [&](const python::Type& type) {
        return !type.isFixedSizeType();
    };
    std::string dd;
    for (int i = 0; i < colTypes.size(); i++) {
        dd = colTypes[i].desc();
    }
    auto x = std::find_if(colTypes.begin(), colTypes.end(), fixedTypeComparator);
//    auto y = std::find(colTypes.begin(), colTypes.end(), python::Type::);
    if (x != colTypes.end()) {
        // there are var length types
        return dynamicallyComputeOffsets(schema, partitionCapacity, ptr, numRows, partitionNum);
    } else {
        // no var length types
        return staticallyComputeOffsets(colTypes, numRows, partitionNum);
    }
}

// creates order of columns that a user wants to sort by
std::vector<size_t> genComparisonOrder(int numColumns, std::vector<size_t> order) {
    std::vector<size_t> ret;
    if (order.empty()) {
        for (int i = 0; i < numColumns; i++) {
            ret.push_back(i);
        }
    } else {
        return order;
    }
    return ret;
    //    for (int i = 0; i < numColumns; i++) {
    //        if (std::find(indicesToSort.begin(), indicesToSort.end(), i) == indicesToSort.end()) {
    //            indicesToSort.push_back(i);
    //        }
    //    }
    //    std::vector<int> ret(indicesToSort.begin(), indicesToSort.end());
    //    return ret;
}

std::vector<size_t> genComparisonOrderEnum(int numColumns, std::vector<size_t> orderEnum) {
    std::vector<size_t> ret;
    if (orderEnum.empty()) {
        for (int i = 0; i < numColumns; i++) {
            ret.push_back(i);
        }
    } else {
        return orderEnum;
    }
    return ret;
    //
    //    for (int i = 0; i < numColumns; i++) {
    //        if (std::find(indicesToSort.begin(), indicesToSort.end(), i) == indicesToSort.end()) {
    //            indicesToSort.push_back(i);
    //        }
    //    }
    //    std::vector<int> ret(indicesToSort.begin(), indicesToSort.end());
    //    return ret;
}



struct TuplexSortComparator {
    // constructor
    // std::vector<const uint8_t*> partitionPtrs: the partitions that
    // the tuplex sort comparator is sorting (order is important, it should
    // never change)
    // const tuplex::Schema& schema: schema of the above partitions
    // bool sortAsc: whethere to sort ascending or descending
    // const std::vector<size_t>& indicesToSort: the columns to sort by
    TuplexSortComparator(
            std::vector<const uint8_t*> partitionPtrs,
            const tuplex::Schema& schema, std::vector<size_t> order, std::vector<size_t> orderEnum) :
            partitionPtrs(std::move(partitionPtrs)), schema(schema) {
        int numColumns = schema.getRowType().parameters().size();
        // generates the order of columns that the partition should be sorted by
        this->order = genComparisonOrder(numColumns, order);
        this->orderEnum = genComparisonOrderEnum(numColumns, orderEnum);
    }

    //    class SortBy(Enum):
    //            ASCENDING = 1
    //    DESCENDING = 2
    //    ASCENDING_LENGTH = 3
    //    DESCENDING_LENGTH = 4
    //    ASCENDING_LEXICOGRAPHICALLY = 5
    //    DESCENDING_LEXICOGRAPHICALLY = 6


    TuplexSortComparator() = delete;

    // returns true or false depending on which row should be sorted
    bool operator() (PartitionSortType l, PartitionSortType r) const {
        // gets the "left" row
        tuplex::Row lRow = tuplex::Row::fromMemory(schema, partitionPtrs.at(l.first.first) + l.second.first,
                                                   l.second.second - l.second.first);
        // gets the "right" row
        tuplex::Row rRow = tuplex::Row::fromMemory(schema, partitionPtrs.at(r.first.first) + r.second.first,
                                                   r.second.second - r.second.first);
        // gets a list of the types for each column
        std::vector<python::Type> colTypes = schema.getRowType().parameters();
        int counter = 0;
        while (counter != order.size()) {
            // gets the current column to sort by
            int currentColIndex = order.at(counter);
            // gets the current type of this current column
            python::Type colType = colTypes.at(currentColIndex);
            // sorting for integer
            counter++;
            if (colType == python::Type::I64) {
                int64_t lInt = lRow.getInt(currentColIndex);
                int64_t rInt = rRow.getInt(currentColIndex);
                if (lInt == rInt) {continue;} // edge case where there is no other column to sort by
                else if (orderEnum.at(counter-1) == 3) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lInt;
                    o2 << rInt;
                    std::string lStrInt = o1.str();
                    std::string rStrInt = o2.str();
                    return lStrInt.length() < rStrInt.length();
                }
                else if (orderEnum.at(counter-1) == 4) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lInt;
                    o2 << rInt;
                    std::string lStrInt = o1.str();
                    std::string rStrInt = o2.str();
                    return lStrInt.length() > rStrInt.length();
                }
                else if (orderEnum.at(counter-1) == 5) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lInt;
                    o2 << rInt;
                    std::string lStrInt = o1.str();
                    std::string rStrInt = o2.str();
                    return lStrInt < rStrInt;
                }
                else if (orderEnum.at(counter-1) == 6) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lInt;
                    o2 << rInt;
                    std::string lStrInt = o1.str();
                    std::string rStrInt = o2.str();
                    return lStrInt > rStrInt;
                }
                else if (orderEnum.at(counter-1) == 2) {
                    return lInt > rInt;
                }
                else {
                    return lInt < rInt;
                }
                // sorting for floats
            } else if (colType == python::Type::F64) {
                double lDub = lRow.getDouble(currentColIndex);
                double rDub = rRow.getDouble(currentColIndex);
                if (lDub == rDub) {continue;}
                else if (orderEnum.at(counter-1) == 3) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lDub;
                    o2 << rDub;
                    std::string lStrDub = o1.str();
                    std::string rStrDub = o2.str();
                    return lStrDub.length() < rStrDub.length();
                }
                else if (orderEnum.at(counter-1) == 4) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lDub;
                    o2 << rDub;
                    std::string lStrDub = o1.str();
                    std::string rStrDub = o2.str();
                    return lStrDub.length() > rStrDub.length();
                }
                else if (orderEnum.at(counter-1) == 5) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lDub;
                    o2 << rDub;
                    std::string lStrDub = o1.str();
                    std::string rStrDub = o2.str();
                    return lStrDub < rStrDub;
                }
                else if (orderEnum.at(counter-1) == 6) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lDub;
                    o2 << rDub;
                    std::string lStrDub = o1.str();
                    std::string rStrDub = o2.str();
                    return lStrDub > rStrDub;
                }
                else if (orderEnum.at(counter-1) == 2) {
                    return lDub > rDub;
                }
                else {
                    return lDub < rDub;
                }
                // sorting for strings
            } else if (colType == python::Type::STRING) {
                std::string lStr = lRow.getString(currentColIndex);
                std::string rStr = rRow.getString(currentColIndex);
                if (lStr == rStr) {continue;}
                else if (orderEnum.at(counter-1) == 2 || orderEnum.at(counter-1) == 5) {
                    return lStr > rStr;
                } else if (orderEnum.at(counter-1) == 3) {
                    return lStr.length() < rStr.length();
                } else if (orderEnum.at(counter-1) == 4) {
                    return lStr.length() > rStr.length();
                } else {
                    return lStr < rStr;
                }
                // sorting for booleans
            } else if (colType == python::Type::BOOLEAN) {
                bool lBool = lRow.getBoolean(currentColIndex);
                bool rBool = rRow.getBoolean(currentColIndex);
                if (lBool == rBool) {continue;}
                else if (orderEnum.at(counter-1) == 3) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lBool;
                    o2 << rBool;
                    std::string lStrBool = o1.str();
                    std::string rStrBool = o2.str();
                    return lStrBool.length() < rStrBool.length();
                }
                else if (orderEnum.at(counter-1) == 4) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lBool;
                    o2 << rBool;
                    std::string lStrBool = o1.str();
                    std::string rStrBool = o2.str();
                    return lStrBool.length() > rStrBool.length();
                }
                else if (orderEnum.at(counter-1) == 5) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lBool;
                    o2 << rBool;
                    std::string lStrBool = o1.str();
                    std::string rStrBool = o2.str();
                    return lStrBool < rStrBool;
                }
                else if (orderEnum.at(counter-1) == 6) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lBool;
                    o2 << rBool;
                    std::string lStrBool = o1.str();
                    std::string rStrBool = o2.str();
                    return lStrBool > rStrBool;
                }
                else if (orderEnum.at(counter-1) == 2) {
                    return lBool > rBool;
                }
                else {
                    return lBool < rBool;
                }
                // doesn't support other types yet
            }
            // TODO: Test Handle Generic Tuple
            else if (colType == python::Type::GENERICTUPLE) {
                bool lBool = lRow.getTuple(currentColIndex).numElements();
                bool rBool = rRow.getTuple(currentColIndex).numElements();
                if (lBool == rBool) {continue;}
                else if (orderEnum.at(counter-1) == 4) {
                    return lBool > rBool;
                }
                else {
                    return lBool < rBool;
                }
                // doesn't support other types yet
            }


                // TODO: Test Handle Generic List
//            else if (colType == python::Type::GENERICLIST) {
//                bool lBool = lRow.getList(currentColIndex).numElements();
//                bool rBool = rRow.getList(currentColIndex).numElements();
//                if (lBool == rBool) {continue;}
//                else if (orderEnum.at(counter-1) == 4) {
//                    return lBool > rBool;
//                }
//                else {
//                    return lBool < rBool;
//                }
//                // doesn't support other types yet
//            }
            else {
                std::cout << "ERROR" << std::endl;
            }
        }
        return false;
    }
    // pointers to each partition that the tuplex sort comparator
    // will be sorting on
    std::vector<const uint8_t*> partitionPtrs;
    // schema of the partitions that the rows belong too
    tuplex::Schema schema;
    // the order of columns to sort by
    std::vector<size_t> order;
    std::vector<size_t> orderEnum;
};

// standardSort sorts a partition's offsets using the TuplexSortComparator
std::vector<PartitionSortType> standardSort(
        std::vector<PartitionSortType> offsets, const uint8_t* ptr,
        const tuplex::Schema& schema, std::vector<size_t> order, std::vector<size_t> orderEnum) {

    std::vector<const uint8_t*> partitionPtrs {ptr};
    // call std::sort using TuplexSortComparator
    std::sort(offsets.begin(), offsets.end(), TuplexSortComparator(partitionPtrs, schema, order, orderEnum));
    return offsets;
}


// reconstructPartition takes in an old partition as well as the sorted
// offsets, and creates a new partition that is a sorted version of the
// old partition (based on the inputted sorted offsets)
// const tuplex::Partition* partition: the old partition
// const std::vector<PartitionSortType>& sortedOffsets: the sorted offsets
// of the rows of the old partition
// const uint8_t* readPtr: ptr to the start of the old partition
// tuplex::Executor* executor: executor that the partition belongs to
// const int dataSetID: dataSetID for the new partition. Default set to
// -1
tuplex::Partition* reconstructPartition(
        const tuplex::Partition* partition,
        const std::vector<PartitionSortType>& sortedOffsets,
        const uint8_t* readPtr, tuplex::Executor* executor,
        const int dataSetID = -1) {
    tuplex::Schema schema = partition->schema();
    auto requiredSize = partition->size();
    // set up new partition to write to
    tuplex::Partition* outputPartition = executor->allocWritablePartition(requiredSize, schema, dataSetID);
    auto cc = outputPartition->schema();
    // lock (write) partition
    auto writePtr = outputPartition->lockWriteRaw();
    // copy the first value of the partition which represents the number
    // of rows that the partition has
    memcpy(writePtr, readPtr - sizeof(int64_t), sizeof(int64_t));
    // increment the write pointer to point to where the first row
    // should be
    writePtr += sizeof(int64_t);
    for (const PartitionSortType sortedOffset : sortedOffsets) {
        // copy the row from the old partition
        memcpy(writePtr, readPtr + sortedOffset.second.first, sortedOffset.second.second - sortedOffset.second.first);
        // update the writeptr to point to where the next row should go
        writePtr += sortedOffset.second.second - sortedOffset.second.first;
    }
    // release the write lock
    outputPartition->unlockWrite();

    return outputPartition;
}


// mergeKPartitions takes in a list of sorted partitions and returns a list
// of partitions in sorted order. It does this by first adding the first row/offset
// of each sorted partition to a special minimum priority queue. Then the minimum
// of these values is written since it is guaranteed to be the most minimum value
// since it was "the minimum of the minimums". Then, the next row/offset from the
// partition where the first row was written, is then added to the heap. This process
// repeats till the heap is empty (meaning all partition rows have been gone through).
// std::vector<tuplex::Partition*> partitions: this is a list of pointers
// to sorted partitions that should be merged.
// tuplex::Executor* executor: this is the executor that the partitions belong
// to
// bool sortAsc: specifies whether to sort ascending or descending. The default
// is set to true (sort ascending)
// const std::vector<size_t>& indicesToSort: this is an ordered list of indices
// to sort by. For example, <3, 2> would mean sort by column 3, then 2, then 1.
// The default is set to an empty list (which implies sort columns by order)
std::vector<tuplex::Partition*> mergeKPartitions(
        std::vector<tuplex::Partition*> partitions,
        tuplex::Executor* executor, std::vector<size_t> order, std::vector<size_t> orderEnum) {
    // base cases: if there is only one partition, then it should already
    // be sorted according to the assumptions of this function.
    // if there are no partitions, then there is nothing to sort
    if (partitions.size() == 1 || partitions.empty()) {
        return partitions;
    }

    tuplex::Schema schema = partitions.at(0)->schema();
    // a list of offsets for each partition, where offsets for a particular
    // partitions are the offsets for each row
    std::vector<std::vector<PartitionSortType>> offsetsPerPartition;
    //
    std::vector<int64_t> numRowsPerPartition;
    // pointers to the start of each partition
    std::vector<const uint8_t*> partitionPtrs;

    for (int i = 0; i < partitions.size(); i++) {
        // lock (read only) each partition
        const uint8_t* partitionPtr = partitions.at(i)->lockRaw();
        // the pointer of the partition initially points to an int64 of how
        // many rows the partition contains. we have to increment pointer by
        // this size in order to point to the first row of the partition
        int64_t numRows = *(int64_t*)partitionPtr;
        numRowsPerPartition.push_back(numRows);
        partitionPtr += sizeof(int64_t);
        // store the pointer to the first row
        partitionPtrs.push_back(partitionPtr);
        std::vector<PartitionSortType> offsets = computeOffsets(partitions.at(i)->schema(), partitions.at(i)->capacity(), partitionPtr, numRows, i);
        offsetsPerPartition.push_back(offsets);
    }
    // define the comparator to use in the heap
    for (int i = 0; i < orderEnum.size(); i++) {
        if (orderEnum[i] == 1) {
            orderEnum[i] = 2;
        }
        else if (orderEnum[i] == 2) {
            orderEnum[i] = 1;
        }
        else if (orderEnum[i] == 3) {
            orderEnum[i] = 4;
        }
        else if (orderEnum[i] == 4) {
            orderEnum[i] = 3;
        }
        else if (orderEnum[i] == 5) {
            orderEnum[i] = 6;
        }
        else if (orderEnum[i] == 6) {
            orderEnum[i] = 5;
        }
    }
    auto a = TuplexSortComparator(partitionPtrs, schema, order, orderEnum);
    // define the minimum priority queue that will be used to do the k-way merge
    std::priority_queue<PartitionSortType, std::vector<PartitionSortType>, TuplexSortComparator> frontierIndices(a);
    // add the first offset of each partition to the minimum priority queue
    for (int i = 0; i < partitions.size(); i++) {
        frontierIndices.push(std::move(offsetsPerPartition[i].at(0)));
    }
    // define a partition writer for writing to new partitions
    tuplex::PartitionWriter pw(executor, schema, -1, 32);
    while (!frontierIndices.empty()) {
        // get the next smallest value of all the partitions (top of heap)
        PartitionSortType min = frontierIndices.top();
        // get the row corresponding with the offset
        tuplex::Row row = tuplex::Row::fromMemory(schema, partitionPtrs.at(min.first.first) + min.second.first,
                                                  min.second.first + min.second.second);
        // write this row in new partition
        pw.writeRow(row);
        // remove top of heap
        frontierIndices.pop();
        // the partition that this offset came from, if that was the last offset
        // meaning it was the last row, no need to add the next row/offset of this
        // partition to the heap since there is no next row
        if (min.first.second + 1 >= numRowsPerPartition.at(min.first.first)) {
            continue;
        }
        // the partition that this offset came from, add the next row/offset from
        // this partition to the heap
        frontierIndices.push(offsetsPerPartition.at(min.first.first).at(min.first.second + 1));
    }

    // release the read only lock from all partitions
    for (int i = 0; i < partitions.size(); i++) {
        partitions[i]->unlock();
    }
    return pw.getOutputPartitions();
}

// sortSinglePartition sorts a single partition by first computing the offsets
// of each row of the partition, then by rearranging these offsets so that
// they represent the sorted order of the partition. Lastly, we write to a new
// partition in the order of these sorted offsets.
// tuplex::Partition* partition: this is a pointer to the partition to be
// sorted
// tuplex::Executor* executor: this is the executor that the partition belongs
// to
// bool sortAsc: specifies whether to sort ascending or descending. The default
// is set to true (sort ascending)
// const std::vector<size_t>& indicesToSort: this is an ordered list of indices
// to sort by. For example, <3, 2> would mean sort by column 3, then 2, then 1.
// The default is set to an empty list (which implies sort columns by order)
tuplex::Partition* sortSinglePartition(
        tuplex::Partition* partition,
        tuplex::Executor* executor, std::vector<size_t> order, std::vector<size_t> orderEnum) {
    // partition is locked (read only) so no other threads mutate the partition during sorting
    // read only lock is used since sorting is not in place
    const uint8_t* ptr = partition->lockRaw();
    assert(ptr != nullptr);
    // cast the pointer to a pointer to an int64, then dereference the pointer
    // to actually get the int64 value
    int64_t numRows = *(int64_t*)ptr;

    // First value of the partition is an int64 representing the number of rows
    // in the partition. Next value represents the first row. So add sizeof(int64_t)
    // to ptr in order for ptr to point to the first row.
    ptr += sizeof(int64_t);

    tuplex::Schema schema = partition->schema();

    // Computes the offsets for the rows of the partition. Represented as
    // < <index into a partition list, row #>, <beginning offset, ending offset> >
    // For a single partition, it will always be in the form:
    // < <0, row #>, <beginning offset, ending offset> >
    // since there is never a list of partitions to index into
    std::vector<PartitionSortType> offsets = computeOffsets(partition->schema(), partition->capacity(), ptr, numRows);

    // sorts the previously computed offsets, so that now the offsets
    // (representing row locations) are now in sorted order.
    std::vector<PartitionSortType> sortedOffsets = standardSort(offsets, ptr, schema, order, orderEnum);

    // Now that the offsets are in sorted order, we just need to iterate
    // through these offsets, loading the rows in, and writing them to a new
    // partition in that order
    tuplex::Partition* outputPartition = reconstructPartition(partition, sortedOffsets, ptr, executor);

    // free up our read only lock on the partition
    partition->unlock();
    return outputPartition;
}


// sortMultiplePartitions sorts multiple partitions with selective columns
// to sort by as well as a preference to sort ascending vs descending.
// std::vector<tuplex::Partition*> partitions: this is a list of pointers
// to the partitions that should be sorted
// tuplex::Executor* executor: this is the executor that the partitions belong
// to
// bool sortAsc: specifies whether to sort ascending or descending. The default
// is set to true (sort ascending)
// const std::vector<size_t>& indicesToSort: this is an ordered list of indices
// to sort by. For example, <3, 2> would mean sort by column 3, then 2, then 1.
// The default is set to an empty list (which implies sort columns by order)
std::vector<tuplex::Partition*> sortMultiplePartitions(
        std::vector<tuplex::Partition*> partitions,
        tuplex::Executor* executor, std::vector<size_t> order, std::vector<size_t> orderEnum) {
    // sorts each individual partition (not in place)
    for (auto & partition : partitions) {
        partition = sortSinglePartition(partition, executor, order, orderEnum);
    }
    // does a k-way merge of sorted partitions
    return mergeKPartitions(partitions, executor, order, orderEnum);
}