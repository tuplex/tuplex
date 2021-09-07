//
// Created by Colby Anderson on 3/9/21.
//
#include "TestUtils.h"
#include <PartitionWriter.h>
#include <SortBy.h>
#include <string>
#include <fstream>
#include <vector>
#include <utility>
#include <stdexcept>
#include <sstream>

class SortTest : public PyTest {};


//    class SortBy(Enum):
//            ASCENDING = 1
//    DESCENDING = 2
//    ASCENDING_LENGTH = 3
//    DESCENDING_LENGTH = 4
//    ASCENDING_LEXICOGRAPHICALLY = 5
//    DESCENDING_LEXICOGRAPHICALLY = 6

std::vector<tuplex::Partition*> constructTestPartitions(
        tuplex::Executor* executor, const std::vector<tuplex::Row>& rows,
        const python::Type& type, int size = 1024
) {
    tuplex::Schema schema = tuplex::Schema(tuplex::Schema::MemoryLayout::ROW, type);
    tuplex::PartitionWriter pw(executor, schema, -1, size);

    for (const auto& row : rows) {
        pw.writeRow(row);
    }
    return pw.getOutputPartitions(true);
}

bool checkEqualPartitions(std::vector<tuplex::Partition*> l,
                          std::vector<tuplex::Partition*> r) {
    if (l.size() != r.size() || l.size()  == 0) {
        return false;
    }
    for (int i = 0; i < l.size(); i++) {
        auto lPtr = l[i]->lockRaw();
        int64_t lNumRows = *(int64_t*)lPtr;
        lPtr += sizeof(int64_t);

        auto rPtr = r[i]->lockRaw();
        int64_t rNumRows = *(int64_t*)rPtr;
        rPtr += sizeof(int64_t);
        if (lNumRows != rNumRows) {
            l[i]->unlock();
            r[i]->unlock();
            return false;
        }

        if (l[i]->schema() != r[i]->schema()) {
            l[i]->unlock();
            r[i]->unlock();
            return false;
        }
        tuplex::Schema schema = l[i]->schema();
        int lTotalAllocated = 0;
        int rTotalAllocated = 0;

        for (int j = 0; j < lNumRows; j++) {
            tuplex::Row lRow = tuplex::Row::fromMemory(schema, lPtr, l[i]->capacity() - lTotalAllocated);
            tuplex::Row rRow = tuplex::Row::fromMemory(schema, rPtr, r[i]->capacity() - rTotalAllocated);
            //            printf("L0: %d, L1: %d, R0: %d, R1: %d\n", lRow.getInt(0), lRow.getInt(1), rRow.getInt(0), rRow.getInt(1));
            //            printf("L0: %d, R0: %d\n", lRow.getBoolean(0), rRow.getBoolean(0));
            //            printf("L0: %d, R0: %d\n", lRow.getInt(0), rRow.getInt(0));
            //            printf("L0: %s, R0: %s\n", lRow.getString(0).c_str(), rRow.getString(0).c_str());
            int lLength = lRow.serializedLength();
            int rLength = rRow.serializedLength();
            lTotalAllocated += lLength;
            rTotalAllocated += rLength;
            lPtr += lLength;
            rPtr += rLength;
            //            continue;
            std::vector<python::Type> colTypes = schema.getRowType().parameters();
            for (int k = 0; k < colTypes.size(); k++) {
                python::Type colType = colTypes.at(k);
                if (colType == python::Type::I64) {
                    if (lRow.getInt(k) != rRow.getInt(k)) {
                        l[i]->unlock();
                        r[i]->unlock();
                        return false;
                    }
                } else if (colType == python::Type::F64) {
                    if (lRow.getDouble(k) != rRow.getDouble(k)) {
                        l[i]->unlock();
                        r[i]->unlock();
                        return false;
                    }
                } else if (colType == python::Type::STRING) {
                    if (lRow.getString(k) != rRow.getString(k)) {
                        l[i]->unlock();
                        r[i]->unlock();
                        return false;
                    }
                } else if (colType == python::Type::BOOLEAN) {
                    if (lRow.getBoolean(k) != rRow.getBoolean(k)) {
                        l[i]->unlock();
                        r[i]->unlock();
                        return false;
                    }
                }
            }

        }
        l[i]->unlock();
        r[i]->unlock();
    }
    return true;
}

// single partition. 1 column. integers. ascending.
// no duplicates.
TEST_F(SortTest, SinglePartitionSort1ColumnAscInt) {
    using namespace tuplex;
    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(9),
            tuplex::Row(4),
            tuplex::Row(5),
            tuplex::Row(1),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(1),
            tuplex::Row(4),
            tuplex::Row(5),
            tuplex::Row(9),
    };
    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}


TEST_F(SortTest, SinglePartitionTupleSort) {
    using namespace tuplex;
    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(Tuple(0, 3)),
            tuplex::Row(Tuple(10, 3)),
            tuplex::Row(Tuple(0, 13)),
            tuplex::Row(Tuple(110, 3)),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(Tuple(0, 3)),
            tuplex::Row(Tuple(10, 3)),
            tuplex::Row(Tuple(0, 13)),
            tuplex::Row(Tuple(110, 3)),
    };
    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::makeTupleType(std::vector<python::Type>{python::Type::I64, python::Type::I64})});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

TEST_F(SortTest, SinglePartitionEmptyTupleSort) {
    using namespace tuplex;
    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(Field::empty_tuple()),
            tuplex::Row(Field::empty_tuple()),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(Field::empty_tuple()),
            tuplex::Row(Field::empty_tuple()),
    };
    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collectAsVector();

    EXPECT_TRUE(sortedPartitions.size() == 2);
}
// TODO: waiting
//TEST_F(SortTest, SinglePartitionListSort) {
//    using namespace tuplex;
//    Context c(microTestOptions());
//    tuplex::Executor* executor = c.getDriver();
//
//    std::vector<tuplex::Row> rows = {
//            tuplex::Row(List(4, 5, 2)),
//            tuplex::Row(List(9, 2)),
//            tuplex::Row(List(4, 5, 2)),
//            tuplex::Row(List(9, 2, 1, 2)),
//    };
//    std::vector<tuplex::Row> sortedRows = {
//            tuplex::Row(List(9, 2, 1, 2)),
//            tuplex::Row(List(4, 5, 2)),
//            tuplex::Row(List(4, 5, 2)),
//            tuplex::Row(List(9, 2)),
//    };
//    std::vector<size_t> order = {0};
//    std::vector<size_t> orderEnums = {1};
//    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();
//
//    const python::Type& type = python::Type::makeTupleType({python::Type::makeListType(python::Type::I64)});
//    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);
//
//    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
//}

TEST_F(SortTest, SinglePartitionEmptyListSort) {
    using namespace tuplex;
    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(Field::empty_list()),
            tuplex::Row(Field::empty_list()),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(Field::empty_list()),
            tuplex::Row(Field::empty_list()),
    };
    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collectAsVector();

    EXPECT_TRUE(sortedPartitions.size() == 2);
}

TEST_F(SortTest, FlightSampleTest9999) {
    using namespace tuplex;
    ContextOptions co = microTestOptions();
    co.set("tuplex.partitionSize", "32MB");
    co.set("tuplex.webui.enable", "False");
    co.set("tuplex.executorMemory", "1G");
    co.set("tuplex.driverMemory", "1G");
    co.set("tuplex.runTimeMemory", "32MB");
    co.set("tuplex.useLLVMOptimizer", "True");
    co.set("tuplex.optimizer.nullValueOptimization", "True");
    co.set("tuplex.csv.selectionPushdown", "True");
    co.set("tuplex.optimizer.generateParser", "True");
    co.set("tuplex.optimizer.mergeExceptionsInOrder", "False");
    co.set("tuplex.csv.filterPushdown", "True");
    co.set("tuplex.executorCount", "0");
    Context c(co);
//    auto file_name = "../resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv";
    auto file_name = "../resources/pipelines/flights/sortsample.csv";
    auto output_path = "/Users/nathanealpitt/Desktop/tuplex/tuplex/test/core/flightoutput.csv";
    c.csv(
            file_name
    ).sort({3, 15, 2}, {tuplex::SortBy::ASCENDING, tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY, tuplex::SortBy::DESCENDING}).tocsv(URI(output_path));

//    std::ifstream myFile(output_path);
//    std::vector<std::string> row;
//    std::vector<std::string> prev_row;
//    std::string line, word;
//    std::getline(myFile, line);
//    while (myFile.good()) {
//        row.clear();
//        // read an entire row and
//        // store it in a string variable 'line'
//        std::getline(myFile, line);
//        // used for breaking words
//        std::stringstream s(line);
//        // read every column data of a row and
//        // store it in a string variable, 'word'
//        while (std::getline(s, word, ',')) {
//            // add all the column data
//            // of a row to a vector
//            row.push_back(word);
//        }
//        if (!prev_row.empty()) {
//            EXPECT_TRUE(std::stoi(prev_row.at(3)) <= std::stoi(row.at(3)));
//        }
//        prev_row = row;
//
//    }
}

TEST_F(SortTest, FlightSampleTest0) {
    using namespace tuplex;
    ContextOptions co = microTestOptions();
    co.set("tuplex.partitionSize", "32MB");
    co.set("tuplex.webui.enable", "False");
    co.set("tuplex.executorMemory", "1G");
    co.set("tuplex.driverMemory", "1G");
    co.set("tuplex.runTimeMemory", "32MB");
    co.set("tuplex.useLLVMOptimizer", "True");
    co.set("tuplex.optimizer.nullValueOptimization", "True");
    co.set("tuplex.csv.selectionPushdown", "True");
    co.set("tuplex.optimizer.generateParser", "True");
    co.set("tuplex.optimizer.mergeExceptionsInOrder", "False");
    co.set("tuplex.csv.filterPushdown", "True");
    co.set("tuplex.executorCount", "0");
    Context c(co);
    auto file_name = "../resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv";
//    auto file_name = "../resources/pipelines/flights/sortsample.csv";
    auto output_path = "/Users/nathanealpitt/Desktop/tuplex/tuplex/test/core/flightoutput.csv";
    c.csv(
            file_name
            ).sort({15, 3, 2}, {tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY, tuplex::SortBy::ASCENDING, tuplex::SortBy::DESCENDING}).tocsv(URI(output_path));

//    std::ifstream myFile(output_path);
//    std::vector<std::string> row;
//    std::vector<std::string> prev_row;
//    std::string line, word;
//    std::getline(myFile, line);
//    while (myFile.good()) {
//        row.clear();
//        // read an entire row and
//        // store it in a string variable 'line'
//        std::getline(myFile, line);
//        // used for breaking words
//        std::stringstream s(line);
//        // read every column data of a row and
//        // store it in a string variable, 'word'
//        while (std::getline(s, word, ',')) {
//            // add all the column data
//            // of a row to a vector
//            row.push_back(word);
//        }
//        if (!prev_row.empty()) {
//            EXPECT_TRUE(std::stoi(prev_row.at(3)) <= std::stoi(row.at(3)));
//        }
//        prev_row = row;
//
//    }
}

TEST_F(SortTest, FlightSampleTest1) {
    using namespace tuplex;
    ContextOptions co = microTestOptions();
    co.set("tuplex.partitionSize", "32MB");
    co.set("tuplex.webui.enable", "False");
    co.set("tuplex.executorMemory", "1G");
    co.set("tuplex.driverMemory", "1G");
    co.set("tuplex.runTimeMemory", "32MB");
    co.set("tuplex.useLLVMOptimizer", "True");
    co.set("tuplex.optimizer.nullValueOptimization", "True");
    co.set("tuplex.csv.selectionPushdown", "True");
    co.set("tuplex.optimizer.generateParser", "True");
    co.set("tuplex.optimizer.mergeExceptionsInOrder", "False");
    co.set("tuplex.csv.filterPushdown", "True");
    co.set("tuplex.executorCount", "0");
    Context c(co);
    auto file_name = "../resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv";
//    auto file_name = "../resources/pipelines/flights/sortsample.csv";
    auto output_path = "/Users/nathanealpitt/Desktop/tuplex/tuplex/test/core/flightoutput.csv";
    c.csv(
            file_name
    ).sort({4, 3}, {tuplex::SortBy::DESCENDING, tuplex::SortBy::ASCENDING}).tocsv(URI(output_path));

    std::ifstream myFile(output_path);
    std::vector<std::string> row;
    std::vector<std::string> prev_row;
    std::string line, word;
    std::getline(myFile, line);
    while (myFile.good()) {
        row.clear();
        // read an entire row and
        // store it in a string variable 'line'
        std::getline(myFile, line);
        // used for breaking words
        std::stringstream s(line);
        // read every column data of a row and
        // store it in a string variable, 'word'
        while (std::getline(s, word, ',')) {
            // add all the column data
            // of a row to a vector
            row.push_back(word);
        }
        if (!prev_row.empty()) {
            std::cout << "Start" << std::endl;
            std::cout << prev_row.at(4) << std::endl;
            std::cout << row.at(4) << std::endl;
            std::cout << prev_row.at(3) << std::endl;
            std::cout << row.at(3) << std::endl;
            std::cout << "End" << std::endl;
            EXPECT_TRUE(std::stoi(prev_row.at(4)) >= std::stoi(row.at(4)));
            if (std::stoi(prev_row.at(4)) == std::stoi(row.at(4))) {
                EXPECT_TRUE(std::stoi(prev_row.at(3)) <= std::stoi(row.at(3)));
            }
        }
        prev_row = row;

    }
}

TEST_F(SortTest, FlightSampleTest2) {
    using namespace tuplex;
    ContextOptions co = microTestOptions();
    co.set("tuplex.partitionSize", "32MB");
    co.set("tuplex.webui.enable", "False");
    co.set("tuplex.executorMemory", "1G");
    co.set("tuplex.driverMemory", "1G");
    co.set("tuplex.runTimeMemory", "32MB");
    co.set("tuplex.useLLVMOptimizer", "True");
    co.set("tuplex.optimizer.nullValueOptimization", "True");
    co.set("tuplex.csv.selectionPushdown", "True");
    co.set("tuplex.optimizer.generateParser", "True");
    co.set("tuplex.optimizer.mergeExceptionsInOrder", "False");
    co.set("tuplex.csv.filterPushdown", "True");
    Context c(co);
    auto file_name = "../resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv";
//    auto file_name = "../resources/pipelines/flights/sortsample.csv";
    auto output_path = "/Users/nathanealpitt/Desktop/tuplex/tuplex/test/core/flightoutput.csv";

    c.csv(
            file_name
    ).sort({3, 15}, {tuplex::SortBy::ASCENDING_LENGTH, tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY}).tocsv(URI(output_path));

    std::ifstream myFile(output_path);
    std::vector<std::string> row;
    std::vector<std::string> prev_row;
    std::string line, word;
    std::getline(myFile, line);
    while (myFile.good()) {
        row.clear();
        // read an entire row and
        // store it in a string variable 'line'
        std::getline(myFile, line);
        // used for breaking words
        std::stringstream s(line);
        // read every column data of a row and
        // store it in a string variable, 'word'
        while (std::getline(s, word, ',')) {
            // add all the column data
            // of a row to a vector
            row.push_back(word);
        }
        if (!prev_row.empty()) {
            EXPECT_TRUE(prev_row.at(3).length() <= row.at(3).length());
            if (prev_row.at(3).length() == row.at(3).length()) {
                EXPECT_TRUE(prev_row.at(15) >= row.at(15));
            }
        }
        prev_row = row;

    }
}

// TODO: dict tests
//TEST_F(SortTest, SinglePartitionEmptyDictSort) {
//    using namespace tuplex;
//    Context c(microTestOptions());
//    tuplex::Executor* executor = c.getDriver();
//
//    std::vector<tuplex::Row> rows = {
//            tuplex::Row(Field::from_str_data("{\"key1\": \"val1\", \"key2\": \"val2\"}", python::Type::makeDictionaryType(python::Type::STRING, python::Type::STRING)),
//            tuplex::Row(List()),
//    };
//    std::vector<tuplex::Row> sortedRows = {
//            tuplex::Row(List()),
//            tuplex::Row(List()),
//    };
//    std::vector<size_t> order = {0};
//    std::vector<size_t> orderEnums = {1};
//    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collectAsVector();
//
//    EXPECT_TRUE(sortedPartitions.size() == 2);
//}

//TEST_F(SortTest, SinglePartitionDictSort) {
//    using namespace tuplex;
//    Context c(microTestOptions());
//    tuplex::Executor* executor = c.getDriver();
//
//    std::vector<tuplex::Row> rows = {
//            tuplex::Row(Tuple(0, 3)),
//            tuplex::Row(Tuple(10, 3)),
//            tuplex::Row(Tuple(0, 13)),
//            tuplex::Row(Tuple(110, 3)),
//    };
//    std::vector<tuplex::Row> sortedRows = {
//            tuplex::Row(Tuple(0, 3)),
//            tuplex::Row(Tuple(10, 3)),
//            tuplex::Row(Tuple(0, 13)),
//            tuplex::Row(Tuple(110, 3)),
//    };
//    std::vector<size_t> order = {0};
//    std::vector<size_t> orderEnums = {1};
//    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();
//
//    const python::Type& type = python::Type::makeTupleType({python::Type::makeTupleType(std::vector<python::Type>{python::Type::I64, python::Type::I64})});
//    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);
//
//    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
//}
//
// single partition. 1 column. integers. descending.
// no duplicates.
TEST_F(SortTest, SinglePartitionSort1ColumnDesInt) {
    using namespace tuplex;
    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();
    std::vector<tuplex::Row> rows = {
            tuplex::Row(2),
            tuplex::Row(7),
            tuplex::Row(3),
            tuplex::Row(9),
            tuplex::Row(5),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(9),
            tuplex::Row(7),
            tuplex::Row(5),
            tuplex::Row(3),
            tuplex::Row(2),
    };
    // is 2, 7, 3, 9, 5
    // should be 9, 7, 5, 3, 2

    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::DESCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}


// single partition. 1 column. integers. descending.
// duplicates.
TEST_F(SortTest, SinglePartitionSort1ColumnDesIntDup) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2),
            tuplex::Row(7),
            tuplex::Row(3),
            tuplex::Row(7),
            tuplex::Row(5),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(7),
            tuplex::Row(7),
            tuplex::Row(5),
            tuplex::Row(3),
            tuplex::Row(2),
    };
    // is 2, 7, 3, 7, 5
    // should be 7, 7, 5, 3, 2

    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::DESCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// single partition. 1 column. integers. descending.
// duplicates.
TEST_F(SortTest, SinglePartitionSort1ColumnDesIntDup33) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2),
            tuplex::Row(7),
            tuplex::Row(5),
            tuplex::Row(2),
            tuplex::Row(3),
            tuplex::Row(-7),
            tuplex::Row(-5),
            tuplex::Row(-5),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(7),
            tuplex::Row(5),
            tuplex::Row(3),
            tuplex::Row(2),
            tuplex::Row(2),
            tuplex::Row(-5),
            tuplex::Row(-5),
            tuplex::Row(-7),
    };

    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::DESCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));

}


// single partition. 2 column. integers. descending.
// duplicates.
TEST_F(SortTest, SinglePartitionSort2ColumnDesIntDup) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2, 4),
            tuplex::Row(7, 1),
            tuplex::Row(3, 0),
            tuplex::Row(7, 2),
            tuplex::Row(5, 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(7, 2),
            tuplex::Row(7, 1),
            tuplex::Row(5, 4),
            tuplex::Row(3, 0),
            tuplex::Row(2, 4),
    };
    // is (2, 4), (7, 1), (3, 0), (7, 2), (5, 4)
    // should be (7, 2), (7, 1), (5, 4), (3, 0), (2, 4)

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::DESCENDING, tuplex::SortBy::DESCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// single partition. 2 column. integers. descending.
// duplicates.
TEST_F(SortTest, SinglePartitionSort2ColumnDesIntDup2) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2, 0),
            tuplex::Row(7, 1),
            tuplex::Row(2, 0),
            tuplex::Row(7, 2),
            tuplex::Row(1, 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(7, 2),
            tuplex::Row(7, 1),
            tuplex::Row(2, 0),
            tuplex::Row(2, 0),
            tuplex::Row(1, 4),
    };
    // is (2, 0), (7, 1), (2, 0), (7, 2), (1, 4)
    // should be (7, 2), (7, 1), (2, 0), (2, 0), (1, 4)

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::DESCENDING, tuplex::SortBy::DESCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// single partition. 2 column. integers. ascending.
// duplicates.
TEST_F(SortTest, SinglePartitionSort2ColumnAscIntDup2) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2, 0),
            tuplex::Row(7, 1),
            tuplex::Row(2, 0),
            tuplex::Row(7, 2),
            tuplex::Row(1, 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(1, 4),
            tuplex::Row(2, 0),
            tuplex::Row(2, 0),
            tuplex::Row(7, 1),
            tuplex::Row(7, 2),
    };
    // is (2, 0), (7, 1), (2, 0), (7, 2), (1, 4)
    // should be (1, 4), (2, 0), (2, 0), (7, 1), (7, 2)

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// single partition. 2 column. integers. ascending.
// duplicates.
TEST_F(SortTest, SinglePartitionSort2ColumnAscIntDup) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2, 4),
            tuplex::Row(7, 1),
            tuplex::Row(3, 2),
            tuplex::Row(3, 0),
            tuplex::Row(5, 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(2, 4),
            tuplex::Row(3, 0),
            tuplex::Row(3, 2),
            tuplex::Row(5, 4),
            tuplex::Row(7, 1),
    };
    // is (2, 4), (7, 1), (3, 2), (3, 0), (5, 4)
    // should be (2, 4), (3, 0), (3, 2), (5, 4), (7, 1),

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

TEST_F(SortTest, SinglePartitionLengthAscending) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row("10", 4),
            tuplex::Row("003", 1),
            tuplex::Row("4", 2),
            tuplex::Row("1224", 0),
            tuplex::Row("333333", 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row("4", 2),
            tuplex::Row("10", 4),
            tuplex::Row("003", 1),
            tuplex::Row("1224", 0),
            tuplex::Row("333333", 4),
    };
    // is (2, 4), (7, 1), (3, 2), (3, 0), (5, 4)
    // should be (2, 4), (3, 0), (3, 2), (5, 4), (7, 1),

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING_LENGTH, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::STRING, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

TEST_F(SortTest, SinglePartitionLengthDescending) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row("10", 4),
            tuplex::Row("003", 1),
            tuplex::Row("4", 2),
            tuplex::Row("1224", 0),
            tuplex::Row("333333", 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row("333333", 4),
            tuplex::Row("1224", 0),
            tuplex::Row("003", 1),
            tuplex::Row("10", 4),
            tuplex::Row("4", 2),
    };
    // is (2, 4), (7, 1), (3, 2), (3, 0), (5, 4)
    // should be (2, 4), (3, 0), (3, 2), (5, 4), (7, 1),

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::DESCENDING_LENGTH, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::STRING, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

TEST_F(SortTest, SinglePartitionBasicLexicoAsc) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();
    std::vector<tuplex::Row> rows = {
            tuplex::Row("aa"),
            tuplex::Row("d"),
            tuplex::Row("bbb"),
            tuplex::Row("c"),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row("aa"),
            tuplex::Row("bbb"),
            tuplex::Row("c"),
            tuplex::Row("d"),
    };
    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING_LEXICOGRAPHICALLY};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::STRING});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

TEST_F(SortTest, SinglePartitionLengthCastAscending) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(10, 4),
            tuplex::Row(300, 1),
            tuplex::Row(4, 2),
            tuplex::Row(1224, 0),
            tuplex::Row(333333, 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(4, 2),
            tuplex::Row(10, 4),
            tuplex::Row(300, 1),
            tuplex::Row(1224, 0),
            tuplex::Row(333333, 4),
    };
    // is (2, 4), (7, 1), (3, 2), (3, 0), (5, 4)
    // should be (2, 4), (3, 0), (3, 2), (5, 4), (7, 1),

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING_LENGTH, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}


TEST_F(SortTest, SinglePartitionLengthCastDescending) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(10, 4),
            tuplex::Row(300, 1),
            tuplex::Row(4, 2),
            tuplex::Row(1224, 0),
            tuplex::Row(333333, 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(333333, 4),
            tuplex::Row(1224, 0),
            tuplex::Row(300, 1),
            tuplex::Row(10, 4),
            tuplex::Row(4, 2),
    };
    // is (2, 4), (7, 1), (3, 2), (3, 0), (5, 4)
    // should be (2, 4), (3, 0), (3, 2), (5, 4), (7, 1),

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::DESCENDING_LENGTH, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

TEST_F(SortTest, SinglePartitionLexicoCastAscending) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(10, 4),
            tuplex::Row(300, 1),
            tuplex::Row(4, 2),
            tuplex::Row(1224, 0),
            tuplex::Row(333333, 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(10, 4),
            tuplex::Row(1224, 0),
            tuplex::Row(300, 1),
            tuplex::Row(333333, 4),
            tuplex::Row(4, 2),
    };
    // is (2, 4), (7, 1), (3, 2), (3, 0), (5, 4)
    // should be (2, 4), (3, 0), (3, 2), (5, 4), (7, 1),

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING_LEXICOGRAPHICALLY, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}


TEST_F(SortTest, SinglePartitionLexicoCastDescending) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(10, 4),
            tuplex::Row(300, 1),
            tuplex::Row(4, 2),
            tuplex::Row(1224, 0),
            tuplex::Row(333333, 4),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(4, 2),
            tuplex::Row(333333, 4),
            tuplex::Row(300, 1),
            tuplex::Row(1224, 0),
            tuplex::Row(10, 4),
    };
    // is (2, 4), (7, 1), (3, 2), (3, 0), (5, 4)
    // should be (2, 4), (3, 0), (3, 2), (5, 4), (7, 1),

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::DESCENDING_LEXICOGRAPHICALLY, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}




// single partition. 2 column. integers. ascending.
// duplicates.
TEST_F(SortTest, SinglePartitionSort2ColumnAscIntDup22) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2, 4), // 16 bytes
            tuplex::Row(7, 1), // 16 bytes
            tuplex::Row(3, 2), // 16 bytes
            tuplex::Row(3, 0), // 16 bytes
            tuplex::Row(5, 4), // 16 bytes
            tuplex::Row(19, 0), // 16 bytes
            tuplex::Row(5, 4), // 16 bytes
            tuplex::Row(-99, 4), // 16 bytes
            tuplex::Row(-3, 1), // 16 bytes
            tuplex::Row(5, -5), // 16 bytes
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(-99, 4),
            tuplex::Row(-3, 1),
            tuplex::Row(2, 4),
            tuplex::Row(3, 0),
            tuplex::Row(3, 2),
            tuplex::Row(5, -5),
            tuplex::Row(5, 4),
            tuplex::Row(5, 4),
            tuplex::Row(7, 1),
            tuplex::Row(19, 0),
    };
    // is (2, 4), (7, 1), (3, 2), (3, 0), (5, 4), (19, 0), (5, 4), (-99, 4), (-3, 1), (5, -5)
    // should be (-99, 4), (-3, 1), (2, 4), (3, 0), (3, 2), (5, -5), (5, 4), (5, 4), (7, 1), (19, 0)
    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// multiple partition. 2 column. integers. ascending.
// duplicates.
// fails with mixing up (5, -5) and (5, 4)
TEST_F(SortTest, MultiplePartitionSort2ColumnAscIntDup) {
    using namespace tuplex;

    ContextOptions co = microTestOptions();
    co.set("tuplex.partitionSize", "64B");
    Context c(co);
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2, 4), // 16 bytes
            tuplex::Row(7, 1), // 16 bytes
            tuplex::Row(3, 2), // 16 bytes
            tuplex::Row(3, 0), // 16 bytes
            tuplex::Row(5, 4), // 16 bytes
            tuplex::Row(19, 0), // 16 bytes
            tuplex::Row(5, 4), // 16 bytes
            tuplex::Row(-99, 4), // 16 bytes
            tuplex::Row(-3, 1), // 16 bytes
            tuplex::Row(5, -5), // 16 bytes
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(-99, 4),
            tuplex::Row(-3, 1),
            tuplex::Row(2, 4),
            tuplex::Row(3, 0),
            tuplex::Row(3, 2),
            tuplex::Row(5, -5),
            tuplex::Row(5, 4),
            tuplex::Row(5, 4),
            tuplex::Row(7, 1),
            tuplex::Row(19, 0),
    };
    // is (2, 4), (7, 1), (3, 2), (3, 0), (5, 4), (19, 0), (5, 4), (-99, 4), (-3, 1), (5, -5)
    // should be (-99, 4), (-3, 1), (2, 4), (3, 0), (3, 2), (5, -5), (5, 4), (5, 4), (7, 1), (19, 0)

    std::vector<size_t> order = {0, 1};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type, 64);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// multiple partition. 2 column. integers. ascending.
// duplicates. custom order
TEST_F(SortTest, MultiplePartitionSort2ColumnAscIntDupCstmOrdr22) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2, 4), // 16 bytes
            tuplex::Row(7, 1), // 16 bytes
            tuplex::Row(3, 2), // 16 bytes
            tuplex::Row(3, 0), // 16 bytes
            tuplex::Row(5, 4), // 16 bytes
            tuplex::Row(19, 0), // 16 bytes
            tuplex::Row(5, 4), // 16 bytes
            tuplex::Row(-99, 4), // 16 bytes
            tuplex::Row(-3, 1), // 16 bytes
            tuplex::Row(5, -5), // 16 bytes
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(5, -5),
            tuplex::Row(3, 0),
            tuplex::Row(19, 0),
            tuplex::Row(-3, 1),
            tuplex::Row(7, 1),
            tuplex::Row(3, 2),
            tuplex::Row(-99, 4),
            tuplex::Row(2, 4),
            tuplex::Row(5, 4),
            tuplex::Row(5, 4),
    };

    std::vector<size_t> order = {1, 0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING, tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// multiple partition. 2 column. integers. ascending.
// duplicates. custom order
TEST_F(SortTest, MultiplePartitionSort2ColumnAscIntDupCstmOrdr) {
    using namespace tuplex;

    ContextOptions co = microTestOptions();
    co.set("tuplex.partitionSize", "64B");
    Context c(co);
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(2, 4), // 16 bytes
            tuplex::Row(7, 1), // 16 bytes
            tuplex::Row(3, 2), // 16 bytes
            tuplex::Row(3, 0), // 16 bytes
            tuplex::Row(5, 4), // 16 bytes
            tuplex::Row(19, 0), // 16 bytes
            tuplex::Row(5, 4), // 16 bytes
            tuplex::Row(-99, 4), // 16 bytes
            tuplex::Row(-3, 1), // 16 bytes
            tuplex::Row(5, -5), // 16 bytes
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(5, -5),
            tuplex::Row(3, 0),
            tuplex::Row(19, 0),
            tuplex::Row(-3, 1),
            tuplex::Row(7, 1),
            tuplex::Row(3, 2),
            tuplex::Row(-99, 4),
            tuplex::Row(2, 4),
            tuplex::Row(5, 4),
            tuplex::Row(5, 4),
    };

    std::vector<size_t> order = {1, 0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING, tuplex::SortBy::ASCENDING};
    auto sortedPartitions00 = c.parallelize(rows).sort(order, orderEnums).collectAsVector();
    std::vector<int> acai;
    for (int i = 0; i < sortedPartitions00.size(); i++) {
        acai.push_back(sortedPartitions00.at(i).getInt(1));
    }
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64, python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type, 64);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// single partition. 1 column. boolean. ascending.
// no duplicates.
TEST_F(SortTest, SinglePartitionSort1ColumnAscBool) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(false),
            tuplex::Row(true),
            tuplex::Row(true),
            tuplex::Row(false),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(false),
            tuplex::Row(false),
            tuplex::Row(true),
            tuplex::Row(true),
    };

    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::BOOLEAN});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// single partition. 1 column. float. ascending.
// no duplicates.
TEST_F(SortTest, SinglePartitionSort1ColumnAscFloat) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row(9.7),
            tuplex::Row(4.2),
            tuplex::Row(5.43),
            tuplex::Row(4.19),
            tuplex::Row(1.3333),
            tuplex::Row(4.1999),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row(1.3333),
            tuplex::Row(4.19),
            tuplex::Row(4.1999),
            tuplex::Row(4.2),
            tuplex::Row(5.43),
            tuplex::Row(9.7),
    };

    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::F64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}

// single partition. 1 column. strings. ascending.
// no duplicates.
TEST_F(SortTest, SinglePartitionSort1ColumnAscStr) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    std::vector<tuplex::Row> rows = {
            tuplex::Row("ca"),
            tuplex::Row("b"),
            tuplex::Row("z"),
            tuplex::Row("c"),
            tuplex::Row("dbba"),
            tuplex::Row("az"),
    };
    std::vector<tuplex::Row> sortedRows = {
            tuplex::Row("az"),
            tuplex::Row("b"),
            tuplex::Row("c"),
            tuplex::Row("ca"),
            tuplex::Row("dbba"),
            tuplex::Row("z"),
    };

    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING};
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::STRING});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}



// single partition. 1 column. strings. ascending.
// no duplicates.
TEST_F(SortTest, MultiplePartitionSort1ColRandNum) {
    using namespace tuplex;

    Context c(microTestOptions());
    tuplex::Executor* executor = c.getDriver();

    // generate random numbers
    std::vector<int> randomNums;
    for (int i = 0; i < 100; i++) {
        randomNums.push_back(rand());
        //        printf("%d\n", randomNums.at(i));
    }
    std::vector<int> sortedRandomNums = randomNums;
    std::sort(sortedRandomNums.begin(), sortedRandomNums.end());
    std::vector<tuplex::Row> rows;
    for (int i = 0; i < 100; i++) {
        rows.push_back(tuplex::Row(randomNums.at(i)));
    }
    std::vector<tuplex::Row> sortedRows;
    for (int i = 0; i < 100; i++) {
        sortedRows.push_back(tuplex::Row(sortedRandomNums.at(i)));
    }

    std::vector<size_t> order = {0};
    std::vector<tuplex::SortBy> orderEnums = {tuplex::SortBy::ASCENDING};

    auto sortedPartitions00 = c.parallelize(rows).sort(order, orderEnums).collectAsVector();
    std::vector<int> acai;
    for (int i = 0; i < sortedPartitions00.size(); i++) {
        acai.push_back(sortedPartitions00.at(i).getInt(0));
    }
    auto sortedPartitions = c.parallelize(rows).sort(order, orderEnums).collect()->partitions();

    const python::Type& type = python::Type::makeTupleType({python::Type::I64});
    std::vector<tuplex::Partition*> sortedPartitions2 = constructTestPartitions(executor, sortedRows, type, 256);

    EXPECT_TRUE(checkEqualPartitions(sortedPartitions, sortedPartitions2));
}