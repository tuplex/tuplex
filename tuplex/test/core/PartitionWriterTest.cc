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
#include "TestUtils.h"
#include <physical/memory/PartitionWriter.h>
#include <Context.h>

class PartitionWriterTest : public TuplexTest {};

TEST_F(PartitionWriterTest, BasicRowWrite) {
    using namespace tuplex;

    Context c(microTestOptions());
    Schema schema = Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::I64, python::Type::I64}));
    PartitionWriter pw(c.getDriver(), schema, -1, 0, 1024);

    auto outputs = pw.getOutputPartitions(true);
    ASSERT_EQ(outputs.size(), 0);

    // write
    pw.writeRow(Row(11, 12));
    pw.writeRow(Row(-1, -2));
    pw.writeRow(Row(2, 32));

    outputs = pw.getOutputPartitions(true);

    ASSERT_EQ(outputs.size(), 1);

    auto output = outputs.front();

    EXPECT_EQ(output->getNumRows(), 3);
    EXPECT_EQ(output->getDataSetID(), -1);


    std::vector<std::string> ref{"(11,12)", "(-1,-2)", "(2,32)"};

    // check contents
    auto numRows = output->getNumRows();
    const uint8_t* ptr = output->lock();
    auto endptr = ptr + output->capacity();
    for(unsigned i = 0; i < numRows; ++i) {
        Row row = Row::fromMemory(schema, ptr, endptr - ptr);

        EXPECT_EQ(row.toPythonString(), ref[i]);

        ptr += row.serializedLength();
    }
    output->unlock();
}