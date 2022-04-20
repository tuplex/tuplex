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
#include <ErrorDataSet.h>
#include <sstream>
#include "TestUtils.h"

class DataSetTest : public PyTest {};

TEST_F(DataSetTest, DataSetShow) {
    using namespace tuplex;

    Context c(testOptions());
    DataSet &ds = c.parallelize({1, 2, 3, 4});
    std::ostringstream output;
    ds.show(-1, output);

    std::string expectedShow =
        "+----------+\n"
        "| Column_0 |\n"
        "+----------+\n"
        "| 1        |\n"
        "+----------+\n"
        "| 2        |\n"
        "+----------+\n"
        "| 3        |\n"
        "+----------+\n"
        "| 4        |\n"
        "+----------+\n";
    std::cout << expectedShow << std::endl;
    EXPECT_EQ(output.str(), expectedShow);
}

TEST_F(DataSetTest, DataSetShowII) {
    using namespace tuplex;

    Context c(testOptions());
    DataSet &ds = c.parallelize({1, 2, 3, 4});
    std::ostringstream output;
    ds.show(1, output);

    std::string expectedShow =
            "+----------+\n"
            "| Column_0 |\n"
            "+----------+\n"
            "| 1        |\n"
            "+----------+\n";
    std::cout << expectedShow << std::endl;
    EXPECT_EQ(output.str(), expectedShow);
}

TEST_F(DataSetTest, ErrorDataSetShow) {
    using namespace tuplex;

    Context c(testOptions());

    DataSet &ds = c.makeError("Error");
    std::ostringstream output;
    ds.show(-1, output);

    EXPECT_EQ(output.str(), "");
}