//
// Created by Colby Anderson on 6/2/21.
//

#include <PartitionWriter.h>
#include "Row.h"
#include <SortBy.h>
//#include <PythonSerializer.h>
#include "PythonSerializer_private.h"
// pair<pair<index # into partitions, row # into partition>, pair<beg. offset, end. offset>>
using PartitionSortType = std::pair<std::pair<size_t, size_t>, std::pair<size_t, size_t>>;

double globtime = 0;

// gets the given number of bytes allocated for a specific
// fixed python type
static size_t fixedLengthTypeSerializationSize(const python::Type& type) {
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

    tuplex::Timer timer;

    std::vector<PartitionSortType> offsets;
    offsets.reserve(numRows);
    size_t totalAllocated = 0;
    for (size_t i = 0; i < numRows; i++) {
        tuplex::Row row = tuplex::Row::fromMemory(schema, ptr + totalAllocated, partitionCapacity - totalAllocated);
//        std::cout << "Comp. Offset - Row: " << row.toPythonString() << std::endl;
        size_t length = row.serializedLength();
//        std::cout << "Comp. Offset - Length: " << length << std::endl;
        auto offset = std::make_pair(totalAllocated, totalAllocated + length);
        offsets.emplace_back(std::make_pair(std::make_pair(partitionNum, i), offset));
        totalAllocated += length;
    }

    std::cout << "\t\t\tFinished dynamicallyComputeOffsets in " << timer.time() << "s" << std::endl;


    return offsets;
}

// offsets of the form <offset of beginning of row from start of partition...
// ..., offset of end of row from start of partition> are statically computed
// by first calculating how many bytes each column type takes up and adding this
// number up to be the total number of bytes the row takes up. Then, the offsets
// are computed from the row length information since each row should be the same
// number of bytes.
static std::vector<PartitionSortType> staticallyComputeOffsets(const std::vector<python::Type>& colTypes, int64_t numRows, int partitionNum = 0) {

    tuplex::Timer timer;

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

    std::cout << "\t\t\tFinished staticallyComputeOffsets in " << timer.time() << "s" << std::endl;

    return offsets;
}

// offsets of the form <offset of beginning of row from start of partition...
// ..., offset of end of row from start of partition> are computed either
// statically or dynamically. If the types of each column are a fixed length
// type, this function will call a helper function to statically generate
// the offsets. Otherwise, it will can a helper function to dynamically
// generate the offsets.
std::vector<PartitionSortType> computeOffsets(const tuplex::Schema& schema, const size_t partitionCapacity, const uint8_t* ptr, int64_t numRows, int partitionNum = 0) {

    tuplex::Timer timer;

    python::Type type = schema.getRowType();
    std::vector<python::Type> colTypes = type.parameters();
    auto fixedTypeComparator = [&](const python::Type& type) {
        return (!type.isFixedSizeType() ||  type.isTupleType() || type.isListType() || type.isDictionaryType() || type.isOptionType());
    };
    // return !isFix || isTup
    // tup.....false || true   -> true
    // int...false || false      -> false
    // str....true || false      -> true
    auto x = std::find_if(colTypes.begin(), colTypes.end(), fixedTypeComparator);
    if (x != colTypes.end()) {
        // there are var length types
        auto dd = dynamicallyComputeOffsets(schema, partitionCapacity, ptr, numRows, partitionNum);
        std::cout << "\t\tFinished computeOffsets in " << timer.time() << "s" << std::endl;
        return dd;
    } else {
        // no var length types
        auto dd = staticallyComputeOffsets(colTypes, numRows, partitionNum);
        std::cout << "\t\tFinished computeOffsets in " << timer.time() << "s" << std::endl;
        return dd;
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

std::vector<tuplex::SortBy> genComparisonOrderEnum(int numColumns, std::vector<tuplex::SortBy> orderEnum) {
    std::vector<tuplex::SortBy> ret;
    if (orderEnum.empty()) {
        for (int i = 0; i < numColumns; i++) {
            ret.push_back(tuplex::SortBy::ASCENDING);
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

size_t calcBitmapSize(const std::vector<bool> &bitmap) {
    int num_nullable_fields = 0;
    for (auto b : bitmap)
        num_nullable_fields += b;

    // compute how many bytes are required to store the bitmap!
    size_t num = 0;
    while (num_nullable_fields > 0) {
        num++;
        num_nullable_fields -= 64;
    }
    return num * sizeof(int64_t); // multiple of 64bit because of alignment
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
            const tuplex::Schema& schema, std::vector<size_t> colIndicesInOrderToSortBy, std::vector<tuplex::SortBy> orderEnum) :
            partitionPtrs(std::move(partitionPtrs)), schema(schema) {
        int numColumns = schema.getRowType().parameters().size();
        // generates the order of columns that the partition should be sorted by
        this->colIndicesInOrderToSortBy = genComparisonOrder(numColumns, colIndicesInOrderToSortBy);
        this->orderEnum = genComparisonOrderEnum(numColumns, orderEnum);
        // get flattened type representation

        // determine from flattened Schema which fields are varlength
        auto params = schema.getRowType().parameters();
        for (int i = 0; i < params.size(); ++i) {
            auto el = params[i];
            _requiresBitmap.push_back(el.isOptionType());
        }
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
        tuplex::Timer timer;
//        if (it()) {
//            auto left_val;
//            auto right_val;
//            if (basicType()) {
//                left_val = *(partitionPtrs.at(l.first.first) + l.second.first + x * 8);
//                right_val = *(partitionPtrs.at(r.first.first) + r.second.first + x * 8);
//            } else {
//                // get offset info
//                // get ptr and length
//            }
//            // comparison
//        }
//        globtime += timer.time();
//        return left_val < right_val;

        // gets the "left" row
//        tuplex::Row lRow = tuplex::Row::fromMemory(schema, partitionPtrs.at(l.first.first) + l.second.first,
//                                                   l.second.second - l.second.first);
//        std::cout <<  "Left Row: " << lRow.toPythonString() << std::endl;
//        // gets the "right" row
//        tuplex::Row rRow = tuplex::Row::fromMemory(schema, partitionPtrs.at(r.first.first) + r.second.first,
//                                                   r.second.second - r.second.first);
//        std::cout << "Right Row: " << rRow.toPythonString() << std::endl;
//         gets a list of the types for each column
        std::vector<python::Type> colTypes = schema.getRowType().parameters();
        int counter = 0;
//        globtime += timer.time();
        while (counter != colIndicesInOrderToSortBy.size()) {
            // gets the current column to sort by
            int currentColIndex = colIndicesInOrderToSortBy.at(counter);
            // gets the current type of this current column
            python::Type colType = colTypes.at(currentColIndex);
            if (colType.isOptionType()) {
                colType = colType.elementType();
            }
            // sorting for integer
            counter++;
            if (colType == python::Type::I64) {
                int64_t lInt = *(partitionPtrs.at(l.first.first) + l.second.first + currentColIndex * 8);
                int64_t rInt = *(partitionPtrs.at(r.first.first) + r.second.first + currentColIndex * 8);
                if (lInt == rInt) {globtime += timer.time(); continue;} // edge case where there is no other column to sort by
                else if (orderEnum.at(counter-1) == tuplex::SortBy::ASCENDING_LENGTH) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lInt;
                    o2 << rInt;
                    std::string lStrInt = o1.str();
                    std::string rStrInt = o2.str();
                    globtime += timer.time();
                    return lStrInt.length() < rStrInt.length();
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING_LENGTH) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lInt;
                    o2 << rInt;
                    std::string lStrInt = o1.str();
                    std::string rStrInt = o2.str();
                    globtime += timer.time();
                    return lStrInt.length() > rStrInt.length();
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::ASCENDING_LEXICOGRAPHICALLY) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lInt;
                    o2 << rInt;
                    std::string lStrInt = o1.str();
                    std::string rStrInt = o2.str();
                    globtime += timer.time();
                    return lStrInt < rStrInt;
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lInt;
                    o2 << rInt;
                    std::string lStrInt = o1.str();
                    std::string rStrInt = o2.str();
                    globtime += timer.time();
                    return lStrInt > rStrInt;
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING) {
                    globtime += timer.time();
                    return lInt > rInt;
                }
                else {
                    globtime += timer.time();
                    return lInt < rInt;
                }
                // sorting for floats
            } else if (colType == python::Type::F64) {
                double lDub = *(partitionPtrs.at(l.first.first) + l.second.first + currentColIndex * 8);
                double rDub = *(partitionPtrs.at(r.first.first) + r.second.first + currentColIndex * 8);
                if (lDub == rDub) {globtime += timer.time(); continue;}
                else if (orderEnum.at(counter-1) == tuplex::SortBy::ASCENDING_LENGTH) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lDub;
                    o2 << rDub;
                    std::string lStrDub = o1.str();
                    std::string rStrDub = o2.str();
                    globtime += timer.time();
                    return lStrDub.length() < rStrDub.length();
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING_LENGTH) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lDub;
                    o2 << rDub;
                    std::string lStrDub = o1.str();
                    std::string rStrDub = o2.str();
                    globtime += timer.time();
                    return lStrDub.length() > rStrDub.length();
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::ASCENDING_LEXICOGRAPHICALLY) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lDub;
                    o2 << rDub;
                    std::string lStrDub = o1.str();
                    std::string rStrDub = o2.str();
                    globtime += timer.time();
                    return lStrDub < rStrDub;
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lDub;
                    o2 << rDub;
                    std::string lStrDub = o1.str();
                    std::string rStrDub = o2.str();
                    globtime += timer.time();
                    return lStrDub > rStrDub;
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING) {
                    globtime += timer.time();
                    return lDub > rDub;
                }
                else {
                    globtime += timer.time();
                    return lDub < rDub;
                }
                // sorting for strings
            } else if (colType == python::Type::STRING) {
//                std::cout << "kaka" << std::endl;
//                tuplex::Row lRow = tuplex::Row::fromMemory(schema, partitionPtrs.at(l.first.first) + l.second.first,
//                                                           l.second.second - l.second.first);
//                std::string lStr2 = lRow.getString(currentColIndex);
//                std::cout << "lStr2: " << lStr2 << std::endl;
                uint64_t lOffset = *((uint64_t *)((uint8_t *) partitionPtrs.at(l.first.first) + l.second.first + currentColIndex * sizeof(int64_t) +calcBitmapSize(_requiresBitmap)));
//                std::cout << "lOffset (0): " <<lOffset << std::endl;
                uint64_t rOffset = *((uint64_t *)((uint8_t *) partitionPtrs.at(r.first.first) + r.second.first + currentColIndex * sizeof(int64_t)+calcBitmapSize(_requiresBitmap)));
                int64_t lLen = ((lOffset & 0xFFFFFFFFul << 32) >> 32) - 1;
//                std::cout << "lLen: " << lLen << std::endl;
                int64_t rLen = ((rOffset & 0xFFFFFFFFul << 32) >> 32) - 1;
                lOffset = lOffset & 0xFFFFFFFFul;
//                std::cout << "lOffset: " << lOffset << std::endl;
                rOffset = rOffset & 0xFFFFFFFFul;
                uint8_t *lPtr = (uint8_t *) partitionPtrs.at(l.first.first) + l.second.first + currentColIndex * 8 + calcBitmapSize(_requiresBitmap) + lOffset;
//                std::cout << "lPtr: " << lPtr << std::endl;
                uint8_t *rPtr = (uint8_t *) partitionPtrs.at(r.first.first) + r.second.first + currentColIndex * 8 + calcBitmapSize(_requiresBitmap) + rOffset;
//                assert(lLen >= 0);
//                assert(rLen >= 0);
                char *Lcstr = (char *) lPtr;
                char *Rcstr = (char *) rPtr;
                std::string lStr;
                std::string rStr;
                // check that string is properly terminated with '\0'
                if (Lcstr[lLen] != '\0') {
                    Logger::instance().logger("memory").error(
                            "corrupted memory found. Left. Could not extract varlen string");
                    lStr = std::string("NULL");
                } else {
                    lStr = std::string((const char *) lPtr);
                }
                if (Rcstr[rLen] != '\0') {
                    Logger::instance().logger("memory").error(
                            "corrupted memory found. Right. Could not extract varlen string");
                    rStr = std::string("NULL");
                } else {
                    rStr = std::string((const char *) rPtr);
                }
//                std::cout << "lStr: " << lStr << std::endl;

                // abaci
//                assert(false);
//
//                std::string lStr = lRow.getString(currentColIndex);
//                std::string rStr = rRow.getString(currentColIndex);
                if (lStr == rStr) {globtime += timer.time(); continue;}
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING || orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY) {
                    globtime += timer.time();
                    return lStr > rStr;
                } else if (orderEnum.at(counter-1) == tuplex::SortBy::ASCENDING_LENGTH) {
                    globtime += timer.time();
                    return lStr.length() < rStr.length();
                } else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING_LENGTH) {
                    globtime += timer.time();
                    return lStr.length() > rStr.length();
                } else {
                    globtime += timer.time();
                    return lStr < rStr;
                }
                // sorting for booleans
            } else if (colType == python::Type::BOOLEAN) {
                bool lBool = *(partitionPtrs.at(l.first.first) + l.second.first + currentColIndex * 8) > 0;
                bool rBool = *(partitionPtrs.at(r.first.first) + r.second.first + currentColIndex * 8) > 0;
                if (lBool == rBool) {globtime += timer.time(); continue;}
                else if (orderEnum.at(counter-1) == tuplex::SortBy::ASCENDING_LENGTH) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lBool;
                    o2 << rBool;
                    std::string lStrBool = o1.str();
                    std::string rStrBool = o2.str();
                    globtime += timer.time();
                    return lStrBool.length() < rStrBool.length();
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING_LENGTH) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lBool;
                    o2 << rBool;
                    std::string lStrBool = o1.str();
                    std::string rStrBool = o2.str();
                    globtime += timer.time();
                    return lStrBool.length() > rStrBool.length();
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::ASCENDING_LEXICOGRAPHICALLY) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lBool;
                    o2 << rBool;
                    std::string lStrBool = o1.str();
                    std::string rStrBool = o2.str();
                    globtime += timer.time();
                    return lStrBool < rStrBool;
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY) {
                    std::ostringstream o1;
                    std::ostringstream o2;
                    o1 << lBool;
                    o2 << rBool;
                    std::string lStrBool = o1.str();
                    std::string rStrBool = o2.str();
                    globtime += timer.time();
                    return lStrBool > rStrBool;
                }
                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING) {
                    globtime += timer.time();
                    return lBool > rBool;
                }
                else {
                    globtime += timer.time();
                    return lBool < rBool;
                }
//                 doesn't support other types yet
            }
            // TODO: Test Handle Generic Tuple
            else if (colType.isTupleType()) {
                globtime += timer.time();
                return true;
            }
            else if (colType == python::Type::EMPTYTUPLE) {
                globtime += timer.time();
                return true;
                // doesn't support other types yet
            }
            // TODO: getList not defined in Row.cc, only declared in Row.h
//            else if (colType.isListType()) {
//                size_t lBool = lRow.getList(currentColIndex).numElements();
//                size_t rBool = rRow.getList(currentColIndex).numElements();
//                if (lBool == rBool) {continue;}
//                else if (orderEnum.at(counter-1) == 4) {
//                    return lBool > rBool;
//                }
//                else {
//                    return lBool < rBool;
//                }
//                // doesn't support other types yet
//            }
            else if (colType == python::Type::EMPTYLIST) {
                globtime += timer.time();
                return true;
                // doesn't support other types yet
            }
            else if (colType.isDictionaryType()) {
//                     wrap 351/352 with a cjson function that will take in the ptr
//                     and cast to obj. then call method to get length
//                     ask Rahul if any problems arise
//                ));
//PyDict_
//                auto lBool = PyByteArray_Size(tuplex::cpython::PyDict_FromCJSON(cJSON_Parse((char*)lRow.get(currentColIndex).getPtr())));
//                auto rBool = PyByteArray_Size(tuplex::cpython::PyDict_FromCJSON(cJSON_Parse((char*)rRow.get(currentColIndex).getPtr())));
//                auto lBool = PyByteArray_Size(tuplex::cpython::createPyDictFromMemory((uint8_t*)lRow.get(currentColIndex).getPtr()));
//                auto rBool = PyByteArray_Size(tuplex::cpython::createPyDictFromMemory((uint8_t*)rRow.get(currentColIndex).getPtr()));
//                if (lBool == rBool) {continue;}
//                else if (orderEnum.at(counter-1) == tuplex::SortBy::DESCENDING_LENGTH) {
//                    return lBool > rBool;
//                }
//                else {
//                    return lBool < rBool;
//                }
                // doesn't support other types yet
            }
            else if (colType == python::Type::EMPTYDICT) {
                globtime += timer.time();
                return true;
                // doesn't support other types yet
            }
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
    std::vector<size_t> colIndicesInOrderToSortBy;
    std::vector<tuplex::SortBy> orderEnum;
    std::vector<bool> _requiresBitmap;
};

// standardSort sorts a partition's offsets using the TuplexSortComparator
std::vector<PartitionSortType> standardSort(
        std::vector<PartitionSortType> offsets, const uint8_t* ptr,
        const tuplex::Schema& schema, std::vector<size_t> colIndicesInOrderToSortBy, std::vector<tuplex::SortBy> orderEnum) {

    tuplex::Timer timer;

    std::vector<const uint8_t*> partitionPtrs {ptr};
    // call std::sort using TuplexSortComparator
    std::sort(offsets.begin(), offsets.end(), TuplexSortComparator(partitionPtrs, schema, colIndicesInOrderToSortBy, orderEnum));

    std::cout << "\t\tFinished standardSort in " << timer.time() << "s" << std::endl;


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
        const uint8_t* readPtr, int64_t num_rows, tuplex::Executor* executor,
        const int dataSetID = -1) {

    tuplex::Timer timer;

    tuplex::Schema schema = partition->schema();
//    tuplex::PartitionWriter pw(executor, schema, -1, 32);
//    for (const PartitionSortType sortedOffset : sortedOffsets) {
//        tuplex::Row row = tuplex::Row::fromMemory(schema, readPtr + sortedOffset.second.first,
//                                                  sortedOffset.second.second - sortedOffset.second.first);
//        printf("Writing...Partition Number: %d. Row Number: %d. Starting Offset: %d. Ending Offset: %d. Length: %d.\n",
//               sortedOffset.first.first, sortedOffset.first.second,
//               sortedOffset.second.first, sortedOffset.second.second, row.serializedLength());
//        pw.writeRow(row);
//    }
//    auto part =  pw.getOutputPartitions().at(0);
//    part->setNumRows(sortedOffsets.size());
//    return part;
    auto requiredSize = partition->size();
    // set up new partition to write to
    tuplex::Partition* outputPartition = executor->allocWritablePartition(requiredSize, schema, dataSetID);
    auto cc = outputPartition->schema();
    // lock (write) partition
    auto writePtr = outputPartition->lockWriteRaw();
    // copy the first value of the partition which represents the number
    // of rows that the partition has
    *((int64_t*)writePtr) = num_rows;
    // increment the write pointer to point to where the first row
    // should be
    memcpy(writePtr, readPtr-sizeof(int64_t), sizeof(int64_t));
    writePtr += sizeof(int64_t);
    for (const PartitionSortType sortedOffset : sortedOffsets) {
        // copy the row from the old partition
//        printf("Writing...Partition Number: %d. Row Number: %d. Starting Offset: %d. Ending Offset: %d.\n",
//               sortedOffset.first.first, sortedOffset.first.second,
//               sortedOffset.second.first, sortedOffset.second.second);
        memcpy(writePtr, readPtr + sortedOffset.second.first, sortedOffset.second.second - sortedOffset.second.first);
        // update the writeptr to point to where the next row should go
        writePtr += sortedOffset.second.second - sortedOffset.second.first;
    }
    // release the write lock
    outputPartition->unlockWrite();

    std::cout << "\t\tFinished reconstructPartition in " << timer.time() << "s" << std::endl;


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
        tuplex::Executor* executor, std::vector<size_t> colIndicesInOrderToSortBy, std::vector<tuplex::SortBy> orderEnum) {

    tuplex::Timer timer;

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
        if (orderEnum[i] == tuplex::SortBy::ASCENDING) {
            orderEnum[i] = tuplex::SortBy::DESCENDING;
        }
        else if (orderEnum[i] == tuplex::SortBy::DESCENDING) {
            orderEnum[i] = tuplex::SortBy::ASCENDING;
        }
        else if (orderEnum[i] == tuplex::SortBy::ASCENDING_LENGTH) {
            orderEnum[i] = tuplex::SortBy::DESCENDING_LENGTH;
        }
        else if (orderEnum[i] == tuplex::SortBy::DESCENDING_LENGTH) {
            orderEnum[i] = tuplex::SortBy::ASCENDING_LENGTH;
        }
        else if (orderEnum[i] == tuplex::SortBy::ASCENDING_LEXICOGRAPHICALLY) {
            orderEnum[i] = tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY;
        }
        else if (orderEnum[i] == tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY) {
            orderEnum[i] = tuplex::SortBy::ASCENDING_LEXICOGRAPHICALLY;
        }
    }
    auto a = TuplexSortComparator(partitionPtrs, schema, colIndicesInOrderToSortBy, orderEnum);
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

        // **** DEBUG:  PRINTING OUT PROBLEM ROW
//        if (row.getInt(3) == 27) {
//            std::cout << "mergeKPartitions Write - Row: " << row.toPythonString() << std::endl;
//            size_t length = row.serializedLength();
//            std::cout << "mergeKPartitions Write - Row (mem): " << std::endl;
//            for (int i = 0; i < row.serializedLength() - 8; i++) {
//                auto x = *(partitionPtrs.at(min.first.first) + min.second.first + i);
//                std::cout<< (int)x;
//                i += 7;
//            }
//            std::cout << "\n";
//        }
//
        // ***** END DEBUG

//        auto bbool = pw.writeRow(row);

        auto bbool = pw.writeSerializedRow(row.getRowType(), partitionPtrs.at(min.first.first) + min.second.first,
                                        min.second.second - min.second.first);

        if (!bbool) {
            std::cout << "Uh oh: couldn't write row" << std::endl;
        }
        // remove top of heap
        frontierIndices.pop();
        // the partition that this offset came from, if that was the last offset
        // meaning it was the last row, no need to add the next row/offset of this
        // partition to the heap since there is no next row
        if (min.first.second + 1 == numRowsPerPartition.at(min.first.first)) {
            continue;
        } else {
            // the partition that this offset came from, add the next row/offset from
            // this partition to the heap
            frontierIndices.push(offsetsPerPartition.at(min.first.first).at(min.first.second + 1));
        }
    }

    // release the read only lock from all partitions
    for (int i = 0; i < partitions.size(); i++) {
        partitions[i]->unlock();
    }

    auto partitionsRes = pw.getOutputPartitions();

    // **** DEBUG:  PRINTING OUT PROBLEM ROW

//    for (const auto & p : partitionsRes) {
//        std::cout << "Partition Rows: " << std::endl;
//        const uint8_t* partitionPtr = p->lockRaw();
//        // the pointer of the partition initially points to an int64 of how
//        // many rows the partition contains. we have to increment pointer by
//        // this size in order to point to the first row of the partition
//        int64_t numRows = *(int64_t*)partitionPtr;
//        numRowsPerPartition.push_back(numRows);
//        partitionPtr += sizeof(int64_t);
//        auto totalAllocated = 0;
//        auto partitionCapacity = p->capacity();
//        for (size_t i = 0; i < numRows; i++) {
//            tuplex::Row row = tuplex::Row::fromMemory(schema, partitionPtr + totalAllocated, partitionCapacity - totalAllocated);

//            if (row.getInt(3) == 0) {
//                std::cout << "mergeKPartitions End - Row: " << row.toPythonString() << std::endl;
//                size_t length = row.serializedLength();
//                std::cout << "mergeKPartitions End - Row (mem): " << std::endl;
//                for (int i = 0; i < row.serializedLength() - 8; i++) {
//                    auto x = *(partitionPtr + totalAllocated + i);
//                    std::cout<< (int)x;
//                    i += 7;
//                }
//                std::cout << "\n";
//            }
//
//            totalAllocated += row.serializedLength();
//        }
//        p->unlock();
//    }
//
    // ***** END DEBUG

    std::cout << "\tFinished mergeKPartitions in " << timer.time() << "s" << std::endl;


    return partitionsRes;
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
        tuplex::Executor* executor, std::vector<size_t> colIndicesInOrderToSortBy, std::vector<tuplex::SortBy> orderEnum) {

    tuplex::Timer timer;

    // partition is locked (read only) so no other threads mutate the partition during sorting
    // read only lock is used since sorting is not in place
    const uint8_t* ptr = partition->lockRaw();
    assert(ptr != nullptr);
    // cast the pointer to a pointer to an int64, then dereference the pointer
    // to actually get the int64 value
    int64_t numRows = *(int64_t*)ptr;
//    int64_t numRows = 3;
//    int i =0;
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
    std::vector<PartitionSortType> sortedOffsets = standardSort(offsets, ptr, schema, colIndicesInOrderToSortBy, orderEnum);

    // Now that the offsets are in sorted order, we just need to iterate
    // through these offsets, loading the rows in, and writing them to a new
    // partition in that order
    tuplex::Partition* outputPartition = reconstructPartition(partition, sortedOffsets, ptr, numRows, executor);

    // free up our read only lock on the partition
    partition->unlock();

    // **** DEBUG:  PRINTING OUT PROBLEM ROW

//    const uint8_t* partitionPtr = outputPartition->lockRaw();
//    int64_t numRows5 = *(int64_t*)partitionPtr;
//    partitionPtr += sizeof(int64_t);
//    auto totalAllocated = 0;
//    auto partitionCapacity = outputPartition->capacity();
//    for (size_t i = 0; i < numRows5; i++) {
//        tuplex::Row row = tuplex::Row::fromMemory(schema, partitionPtr + totalAllocated, partitionCapacity - totalAllocated);
//        if (row.getInt(3) == 27) {
//            std::cout << "sortSinglePartition - Row: " << row.toPythonString() << std::endl;
//            std::cout << "sortSinglePartition - Row (mem): " << std::endl;
//            for (int i = 0; i < row.serializedLength() - 8; i++) {
//                auto x = *(partitionPtr + totalAllocated + i);
//                std::cout<< (int)x;
//                i += 7;
//            }
//            std::cout << "\n";
//        }
//        totalAllocated += row.serializedLength();
//    }
//    outputPartition->unlock();

    // ***** END DEBUG

    std::cout << "\tFinished sortSinglePartition in " << timer.time() << "s" << std::endl;

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
        tuplex::Executor* executor, std::vector<size_t> colIndicesInOrderToSortBy, std::vector<tuplex::SortBy> orderEnum) {

    // **** DEBUG:  PRINTING OUT PROBLEM ROW

//    auto schema = partitions.at(0)->schema();
//    for (const auto & p : partitions) {
//        const uint8_t* partitionPtr = p->lockRaw();
//        int64_t numRows = *(int64_t*)partitionPtr;
//        partitionPtr += sizeof(int64_t);
//        auto totalAllocated = 0;
//        auto partitionCapacity = p->capacity();
//        for (size_t i = 0; i < numRows; i++) {
//            tuplex::Row row = tuplex::Row::fromMemory(schema, partitionPtr + totalAllocated, partitionCapacity - totalAllocated);
//            if (row.getInt(3) == 27) {
//                std::cout << "sortMultiplePartitions - Row: " << row.toPythonString() << std::endl;
//                std::cout << "sortMultiplePartitions - Row (mem): " << std::endl;
//                for (int i = 0; i < row.serializedLength() - 8; i++) {
//                    auto x = *(partitionPtr + totalAllocated + i);
//                    std::cout<< (int)x;
//                    i += 7;
//                }
//                std::cout << "\n";
//            }
//            totalAllocated += row.serializedLength();
//        }
//        p->unlock();
//    }




    // ***** END DEBUG

    tuplex::Timer timer;

    // sorts each individual partition (not in place)
    for (auto & partition : partitions) {
        partition = sortSinglePartition(partition, executor, colIndicesInOrderToSortBy, orderEnum);
    }
//    return partitions;
    // does a k-way merge of sorted partitions
    auto dd =  mergeKPartitions(partitions, executor, colIndicesInOrderToSortBy, orderEnum);
    std::cout << "Finished sortMultiplePartitions in " << timer.time() << "s" << std::endl;
    std::cout << "Time spent in sort comparator: " << globtime << "s" << std::endl;
    return dd;
}