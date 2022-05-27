//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "TestUtils.h"
#include <physical/ResultSet.h>
#include <PartitionWriter.h>

class ResultSetTest : public PyTest {
protected:
    std::shared_ptr<tuplex::Executor> driver;
    tuplex::ContextOptions options;
public:
    // init function
    void SetUp() override {
        PyTest::SetUp();
        options = microTestOptions();
        driver = tuplex::LocalEngine::instance().getDriver(options.DRIVER_MEMORY(),
                                          options.PARTITION_SIZE(),
                                          options.RUNTIME_MEMORY(),
                                          options.RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE(),
                                          options.SCRATCH_DIR());
    }

    tuplex::Partition* allocPartition(const python::Type& rowType, int dataSetID) {
        assert(driver);
        return driver->allocWritablePartition(options.PARTITION_SIZE(), tuplex::Schema(tuplex::Schema::MemoryLayout::ROW, rowType), dataSetID, 0);
    }

    std::vector<tuplex::Partition*> rowsToPartitions(std::vector<tuplex::Row> rows) {
        using namespace tuplex;

        // make sure rows have all the same type
        if(rows.empty())
            return std::vector<tuplex::Partition*>{};

        auto first_type = rows.front().getRowType();
        for(const auto& r : rows)
            EXPECT_EQ(r.getRowType(), first_type);

        // now write via partition writer
        tuplex::PartitionWriter pw(driver.get(), Schema(Schema::MemoryLayout::ROW, first_type), 0, 0,
                                   options.PARTITION_SIZE());
        for(const auto& r : rows)
            pw.writeRow(r);
        return pw.getOutputPartitions();
    }

    std::vector<tuplex::Partition*> pyObjectsToPartitions(const std::vector<std::tuple<size_t, PyObject*>>& pyObjects) {
        using namespace tuplex;

        std::vector<Partition*> partitions;
        if (pyObjects.empty()) {
            return partitions;
        }

        Schema schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::STRING}));
        Partition* partition = allocPartition(schema.getRowType(), -1);
        auto rawPtr = (int64_t*)partition->lockWriteRaw();
        *rawPtr = 0;
        auto ptr = (uint8_t*)(rawPtr + 1);
        size_t numBytesSerialized = 0;

        python::lockGIL();
        for (auto &row: pyObjects) {
            auto rowNum = std::get<0>(row);
            auto pyObj = std::get<1>(row);
            auto ecCode = -1;
            auto opID = -1;
            auto pickledObject = python::pickleObject(python::getMainModule(), pyObj);
            auto pickledObjectSize = pickledObject.size();
            size_t requiredBytes = sizeof(int64_t) * 4 + pickledObjectSize;

            if (partition->capacity() < numBytesSerialized + requiredBytes) {
                partition->unlockWrite();
                partitions.push_back(partition);
                partition = allocPartition(schema.getRowType(), -1);
                rawPtr = (int64_t *) partition->lockWriteRaw();
                *rawPtr = 0;
                ptr = (uint8_t*)(rawPtr + 1);
                numBytesSerialized = 0;
            }

            *((int64_t*)ptr) = rowNum; ptr += sizeof(int64_t);
            *((int64_t*)ptr) = ecCode; ptr += sizeof(int64_t);
            *((int64_t*)ptr) = opID; ptr += sizeof(int64_t);
            *((int64_t*)ptr) = pickledObjectSize; ptr += sizeof(int64_t);
            memcpy(ptr, pickledObject.c_str(), pickledObjectSize); ptr += pickledObjectSize;

            *rawPtr += 1;
            numBytesSerialized += requiredBytes;
        }
        python::unlockGIL();

        partition->unlockWrite();
        partitions.push_back(partition);

        return partitions;
    }
};

TEST_F(ResultSetTest, NoPyObjects) {

    // regular ResultTest test
    using namespace tuplex;
    using namespace std;

    vector<string> strs{"test", "", "abc", "def", "hello world", "Coca Cola", "12345"};

    vector<Row> sample_rows;
    int N = 100;
    for(int i = 0; i < N; ++i) {
        // fill with random data
        sample_rows.push_back(Row(rand() % 256, rand() % 256 * 0.1 - 1.0, strs[rand() % strs.size()]));
    }
    auto partitions = rowsToPartitions(sample_rows);
    std::vector<PartitionGroup> partitionGroups;
    for(int i = 0; i < partitions.size(); ++i) {
        partitions[i]->makeImmortal();
        partitionGroups.push_back(PartitionGroup(1, i, 0, 0, 0, 0));
    }

    auto rsA = make_shared<ResultSet>(Schema(Schema::MemoryLayout::ROW, sample_rows.front().getRowType()), partitions, std::vector<Partition*>{}, std::vector<Partition*>{}, partitionGroups);
    EXPECT_EQ(rsA->rowCount(), sample_rows.size());

    // check correct order returned
    int pos = 0;
    while(rsA->hasNextRow()) {
        EXPECT_EQ(rsA->getNextRow().toPythonString(), sample_rows[pos++].toPythonString());
    }
    EXPECT_EQ(pos, sample_rows.size());

    // now limit result set to 17 rows, check this works as well!
    int Nlimit = 17;
    auto rsB = make_shared<ResultSet>(Schema(Schema::MemoryLayout::ROW, sample_rows.front().getRowType()), partitions,
                                      std::vector<Partition*>{},
                                      std::vector<Partition*>{},
                                      partitionGroups,
                                      Nlimit);
    pos = 0;
    while(rsB->hasNextRow()) {
        EXPECT_EQ(rsB->getNextRow().toPythonString(), sample_rows[pos++].toPythonString());
    }
    EXPECT_EQ(pos, Nlimit);

    // release partitions!
    for(auto p : partitions) {
        driver->freePartition(p);
    }
}

TEST_F(ResultSetTest, WithPyObjects) {
    // this test includes merging in of python objects (i.e. object conversion when desired!)
    using namespace tuplex;
    using namespace std;

    int pos = 0;

    vector<Row> rows = {Row(10), Row(20), Row(30)};
    auto partitions = rowsToPartitions(rows);
    for(auto p : partitions)
        p->makeImmortal();

    python::lockGIL();
    // test A: rows with pyobjects at start
    vector<tuple<size_t, PyObject*>> objsA{make_tuple(0ul, PyLong_FromLong(5)),
                                           make_tuple(0ul, PyLong_FromLong(7))};

    // test B: rows with pyobjects scattered in the middle
    vector<tuple<size_t, PyObject*>> objsB{make_tuple(1ul, PyLong_FromLong(15)),
                                           make_tuple(1ul, PyLong_FromLong(17)),
                                           make_tuple(3ul, PyLong_FromLong(23))};

    // test C: rows with pyobjects at the end
    vector<tuple<size_t, PyObject*>> objsC{make_tuple(3ul, PyLong_FromLong(35)),
                                           make_tuple(3ul, PyLong_FromLong(37))};

    // test D: only pyobjects
    vector<tuple<size_t, PyObject*>> objsD{make_tuple(0ul, PyLong_FromLong(-1)),
                                           make_tuple(0ul, PyLong_FromLong(0)),
                                           make_tuple(0ul, PyLong_FromLong(1))};

    python::unlockGIL();

    vector<Row> refA = {Row(5), Row(7), Row(10), Row(20), Row(30)};
    vector<Row> refB = {Row(10), Row(15), Row(17), Row(20), Row(23), Row(30)};
    vector<Row> refC = {Row(10), Row(20), Row(30), Row(35), Row(37)};
    vector<Row> refD = {Row(-1), Row(0), Row(1)};

    auto partitionGroups = std::vector<PartitionGroup>{PartitionGroup(1,0,0,0,1,0)};

    // TEST A:
    // -----------------
    auto rsA = make_shared<ResultSet>(Schema(Schema::MemoryLayout::ROW, rows.front().getRowType()),
                                      partitions,
                                      std::vector<Partition*>{},
                                      pyObjectsToPartitions(objsA),
                                      partitionGroups);
    EXPECT_EQ(rsA->rowCount(), objsA.size() + rows.size());
    pos = 0;
    while(rsA->hasNextRow()) {
        ASSERT_LT(pos, refA.size());
        EXPECT_EQ(rsA->getNextRow().toPythonString(), refA[pos++].toPythonString());
    }

    // TEST B:
    // -----------------
    auto rsB = make_shared<ResultSet>(Schema(Schema::MemoryLayout::ROW, rows.front().getRowType()),
                                      partitions,
                                      std::vector<Partition*>{},
                                      pyObjectsToPartitions(objsB),
                                      partitionGroups);
    EXPECT_EQ(rsB->rowCount(), objsB.size() + rows.size());
    pos = 0;
    while(rsB->hasNextRow()) {
        ASSERT_LT(pos, refB.size());
        auto rstr = rsB->getNextRow().toPythonString();
        EXPECT_EQ(rstr, refB[pos++].toPythonString());
    }

    // TEST C:
    // -----------------
    auto rsC = make_shared<ResultSet>(Schema(Schema::MemoryLayout::ROW, rows.front().getRowType()),
                                      partitions,
                                      std::vector<Partition*>{},
                                      pyObjectsToPartitions(objsC),
                                      partitionGroups);
    EXPECT_EQ(rsC->rowCount(), objsC.size() + rows.size());
    pos = 0;
    while(rsC->hasNextRow()) {
        ASSERT_LT(pos, refC.size());
        EXPECT_EQ(rsC->getNextRow().toPythonString(), refC[pos++].toPythonString());
    }

    partitionGroups = std::vector<PartitionGroup>{PartitionGroup(0, 0, 0, 0, 1, 0)};

    // TEST D:
    // -------
    // only pyobjects.
    // => trick here: just use them up.
    // don't care for the rest...
    auto rsD = make_shared<ResultSet>(Schema(Schema::MemoryLayout::ROW, rows.front().getRowType()),
                                      std::vector<Partition*>{},
                                      std::vector<Partition*>{},
                                      pyObjectsToPartitions(objsD),
                                      partitionGroups);
    EXPECT_EQ(rsD->rowCount(), objsD.size());
    pos = 0;
    while(rsD->hasNextRow()) {
        ASSERT_LT(pos, refD.size());
        EXPECT_EQ(rsD->getNextRow().toPythonString(), refD[pos++].toPythonString());
    }
}

TEST_F(ResultSetTest, takeMultipleRows) {
    using namespace tuplex;
    using namespace std;

    vector<Row> rows;
    int N = 50000;

    for(int i = 0; i < N; ++i) {
        rows.push_back(Row(i, i * i));
    }

    auto partitions = rowsToPartitions(rows);
    for(auto p : partitions)
        p->makeImmortal();

    // create result set and retrieve different amount of rows

    auto rs = make_shared<ResultSet>(Schema(Schema::MemoryLayout::ROW, rows.front().getRowType()),
                                     partitions,
                                     std::vector<Partition*>{});
    ASSERT_EQ(rs->rowCount(), N);

    vector<int> test_vals{0, 10, (int)partitions.front()->getNumRows() - 10, int(1.5 * partitions.front()->getNumRows()), 42, 7800};

    int glob_offset = 0;
    for(auto tv : test_vals) {
        auto v = rs->getRows(tv);
        ASSERT_EQ(v.size(), tv);
        if(v.size() > 0) {
            for(int i = 0; i < v.size(); ++i)
                EXPECT_EQ(v[i], rows[i + glob_offset]);
        }
        glob_offset += tv;
    }
}