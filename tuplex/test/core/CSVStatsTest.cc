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
#include <CSVStatistic.h>

using namespace tuplex;

// header has one field missing...
// --> should not lead to crash!
TEST(CSVStats, IncompatibleHeader) {
    auto test_str = "a,b,c\n"
                    "1,2,3,FAST ETL!\n"
                    "4,5,6,FAST ETL!\n"
                    "7,8,9,FAST ETL!";

    ContextOptions co = ContextOptions::defaults();
    CSVStatistic csvstat(co.CSV_SEPARATORS(), co.CSV_COMMENTS(),
                         co.CSV_QUOTECHAR(), co.CSV_MAX_DETECTION_MEMORY(),
                         co.CSV_MAX_DETECTION_ROWS(), co.NORMALCASE_THRESHOLD());

    csvstat.estimate(test_str, strlen(test_str));

    EXPECT_TRUE(csvstat.type() == python::Type::makeTupleType({python::Type::I64,
                                                               python::Type::I64,
                                                               python::Type::I64,
                                                               python::Type::STRING}));
}


// helper function to get file stats
CSVStatistic getFileCSVStat(const URI& uri, const std::vector<std::string>& null_values=std::vector<std::string>{}) {
    using namespace std;

    ContextOptions co = ContextOptions::defaults();

    // disable null inference
    CSVStatistic csvstat(co.CSV_SEPARATORS(), co.CSV_COMMENTS(),
                         co.CSV_QUOTECHAR(), co.CSV_MAX_DETECTION_MEMORY(),
                         co.CSV_MAX_DETECTION_ROWS(), co.NORMALCASE_THRESHOLD(),null_values);

    auto vfs = VirtualFileSystem::fromURI(uri);
    Logger::instance().defaultLogger().info("testing on file " + uri.toString());
    auto vf = vfs.map_file(uri);
    if(!vf) {
        Logger::instance().defaultLogger().error("could not open file " + uri.toString());
        return csvstat;
    }
    size_t size = 0;
    vfs.file_size(uri, reinterpret_cast<uint64_t&>(size));
    size_t sampleSize = 0;
    // determine sample size
    if(size > co.CSV_MAX_DETECTION_MEMORY())
        sampleSize = core::floorToMultiple(std::min(co.CSV_MAX_DETECTION_MEMORY(), size), 16ul);
    else {
        sampleSize = core::ceilToMultiple(std::min(co.CSV_MAX_DETECTION_MEMORY(), size), 16ul);
    }

    assert(sampleSize % 16 == 0); // needs to be divisible by 16...

    // copy out memory from file for analysis
    char* start = new char[sampleSize + 1];
    // memset last 16 bytes to 0
    assert(start + sampleSize - 16ul >= start);
    assert(sampleSize >= 16ul);
    std::memset(start + sampleSize - 16ul, 0, 16ul);

    std::memcpy(start, vf->getStartPtr(), sampleSize);
    auto end = start + sampleSize;
    start[sampleSize] = 0; // important!

    csvstat.estimate(start, std::min(sampleSize, size), option<bool>::none, option<char>::none, true);
    delete [] start;
    return csvstat;
}

TEST(CSVStats, FileInference) {
    // go over bunch of files and infer type
    // specify here uri of the file & its type

    auto car_type = python::Type::makeTupleType({python::Type::I64,
                                                 python::Type::STRING,
                                                 python::Type::STRING,
                                                 python::Type::STRING,
                                                 python::Type::STRING});
    std::vector<std::pair<URI, python::Type>> pairs = {{URI("../resources/spark-csv/bool.csv"),
                                                               python::Type::makeTupleType({python::Type::BOOLEAN})},
                                                       {URI("../resources/spark-csv/cars.csv"), car_type},
                                                       {URI("../resources/spark-csv/cars-alternative.csv"), car_type},
                                                       {URI("../resources/spark-csv/cars-blank-column-name.csv"), car_type},
                                                       {URI("../resources/spark-csv/cars-malformed.csv"), python::Type::makeTupleType({python::Type::I64,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING,
                                                                                                                                       python::Type::STRING})},
                                                       {URI("../resources/spark-csv/cars-unbalanced-quotes.csv"), python::Type::makeTupleType({python::Type::STRING,
                                                                                                                                               python::Type::STRING,
                                                                                                                                               python::Type::STRING,
                                                                                                                                               python::Type::STRING,
                                                                                                                                               python::Type::STRING})},
                                                       {URI("../resources/spark-csv/cars-null.csv"), car_type},
                                                       {URI("../resources/spark-csv/comments.csv"), python::Type::makeTupleType({python::Type::I64,
                                                                                                                                 python::Type::I64,
                                                                                                                                 python::Type::I64,
                                                                                                                                 python::Type::I64,
                                                                                                                                 python::Type::F64,
                                                                                                                                 python::Type::STRING})}
                                                       };


    for(auto p : pairs) {
        auto uri = std::get<0>(p);
        auto expected_type = std::get<1>(p);

        auto csvstat = getFileCSVStat(uri);

        // check type
        Logger::instance().defaultLogger().info("type of " + uri.toPath() + " is: " + csvstat.type().desc());
        Logger::instance().defaultLogger().info("expected type is: " + expected_type.desc());
        EXPECT_TRUE(csvstat.type() == expected_type);
    }
}


TEST(CSVStats, RareValueManyNulls) {
    using namespace std;

    string path = "CSVStats.RareValueManyNulls.csv";

    FILE *fp = fopen(path.c_str(), "w");

    for(int i = 0; i < 10; ++i)
        fprintf(fp, ",\n");
    fprintf(fp, "1.0,\n");
    for(int i = 0; i < 10; ++i)
        fprintf(fp, ",\n");

    fclose(fp);


    auto csvstat = getFileCSVStat(path, {"", "NULL"});
    auto expected_type = python::Type::makeTupleType({python::Type::makeOptionType(python::Type::F64), python::Type::makeOptionType(python::Type::STRING)});

    // check type
    Logger::instance().defaultLogger().info("type is: " + csvstat.type().desc());
    EXPECT_TRUE(csvstat.type() == expected_type);
}