//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <vector>
#include <CSVUtils.h>
#include "gtest/gtest.h"


#include <string>
#include <fstream>
#include <streambuf>
#include <CSVUtils.h>

// from https://stackoverflow.com/questions/2602013/read-whole-ascii-file-into-c-stdstring/2602060
std::string fileToString(const std::string& path) {
    std::ifstream t(path);

    if(!t.good())
        throw std::runtime_error("file '" + path + "' not found.");

    std::string str;

    t.seekg(0, std::ios::end);
    str.reserve(t.tellg());
    t.seekg(0, std::ios::beg);

    str.assign((std::istreambuf_iterator<char>(t)),
               std::istreambuf_iterator<char>());

    return str;
}


TEST(CSVUtils, parseRowEmpty) {
    using namespace std;
    using namespace tuplex;

    string test = "";
    auto start = test.c_str();
    auto end = test.c_str() + test.length();

    vector<string> res;
    size_t num;
    parseRow(start, end, res, num);

    EXPECT_EQ(res.size(), 0);
}

TEST(CSVUtils, parseEmptyFields) {
    using namespace std;
    using namespace tuplex;

    string test = ",";
    auto start = test.c_str();
    auto end = test.c_str() + test.length();

    vector<string> res;
    size_t num;
    parseRow(start, end, res, num);

    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0].length(), 0);
}

TEST(CSVUtils, parseNonEscapedFields) {
    using namespace std;
    using namespace tuplex;

    // note the \n at the end. Else, such a raw string will produce a CSV Underrun.
    string test = "hello, world\n";
    auto start = test.c_str();
    auto end = test.c_str() + test.length();

    // now with csv monkey
    auto fp = fopen("CSVUtils.parseNonEscapedFields.csv", "w");
    fwrite(start, test.length(), 1, fp);
    fclose(fp);

//    int fd = open("test.csv", O_RDONLY);
//    assert(fd != -1);
//    csvmonkey::FdStreamCursor stream(fd);
//    csvmonkey::CsvReader reader(stream);
//    int rows = 0;
//    csvmonkey::CsvCursor &row = reader.row();
//    reader.read_row();

    vector<string> res;
    size_t num;
    auto ec = parseRow(start, end, res, num, ',');
    EXPECT_EQ(ec, ExceptionCode::SUCCESS);
    EXPECT_EQ(res.size(), 2);
    EXPECT_EQ(res[0], "hello");
    EXPECT_EQ(res[1], " world");
}

// use tests from https://github.com/maxogden/csv-spectrum
// ==> make CSV parsing more stable!!!


std::vector<std::string> parseCSVString(const std::string& s) {
    using namespace tuplex;
    std::vector<std::string> fields;
    size_t numBytes = 0;
    auto ec = parseRow(s.c_str(), s.c_str() + s.length(), fields, numBytes, ',');
    EXPECT_EQ(ec, ExceptionCode::SUCCESS);
    return fields;
}

template<typename T> void EXPECT_EQ_ARRAY(const std::vector<T>& a, const std::vector<T>& b) {
    ASSERT_EQ(a.size(), b.size());
    for(int i = 0; i < a.size(); ++i)
        EXPECT_EQ(a[i], b[i]);
}

TEST(CSVUtils, ManyCases) {
    using namespace std;

    // gmock doesn't work here yet. Ignore for now
    //    EXPECT_THAT(parseCSVString("a"), ::testing::ElementsAre("a"));
    //    EXPECT_THAT(parseCSVString("a,b"), ::testing::ElementsAre("a", "b"));


    EXPECT_EQ_ARRAY(parseCSVString(""), {});
    EXPECT_EQ_ARRAY(parseCSVString("a"), {"a"});
    EXPECT_EQ_ARRAY(parseCSVString("a,b"), {"a", "b"});
    EXPECT_EQ_ARRAY(parseCSVString("a,b,"), {"a", "b"});
    EXPECT_EQ_ARRAY(parseCSVString(","), {""});
    EXPECT_EQ_ARRAY(parseCSVString("a,b\n"), {"a", "b"});
      EXPECT_EQ_ARRAY(parseCSVString("\na,b\n"), {"a", "b"}); // greedy skip of empty lines at start

    // quoted fields
    EXPECT_EQ_ARRAY(parseCSVString("\"\""), {""});
    EXPECT_EQ_ARRAY(parseCSVString("\"hello\""), {"hello"});
    EXPECT_EQ_ARRAY(parseCSVString("\"hello\",\"test\""), {"hello", "test"});
}

TEST(CSVUtils, MultiLine) {
    using namespace std;

    string test_str = "a,b,c\n"
                    "1,2,3,FAST ETL!\n"
                    "4,5,6,FAST ETL!\n"
                    "7,8,9,FAST ETL!";

    using namespace tuplex;
    std::vector<std::string> fields;
    size_t numBytes = 0;
    auto ec = parseRow(test_str.c_str(), test_str.c_str() + test_str.length(), fields, numBytes, ',');
    EXPECT_EQ(ec, ExceptionCode::SUCCESS);
    EXPECT_EQ(numBytes, 6);
    EXPECT_EQ_ARRAY(fields, {"a", "b", "c"});
}

TEST(CSVUtils, MultiLineII) {
    using namespace std;

    string test_str = "\n"
                      "year,make,model,comment,blank\n"
                      "\"2012\",\"Tesla\",\"S\",\"No comment\",\n"
                      "\n"
                      "1997,Ford,E350,\"Go get one now they are going fast\",\n"
                      "2015,Chevy,Volt\n"
                      "";

    using namespace tuplex;
    std::vector<std::string> fields;
    size_t numBytes = 0;
    auto ec = parseRow(test_str.c_str(), test_str.c_str() + test_str.length(), fields, numBytes, ',');
    EXPECT_EQ(ec, ExceptionCode::SUCCESS);
    EXPECT_EQ_ARRAY(fields, {"year", "make", "model", "comment", "blank"});
}

TEST(CSVUtils, ZillowDirty) {
    using namespace std;
    using namespace tuplex;

    string file_path = "../resources/zillow_dirty_sample.csv";

    string test_str = fileToString(file_path);

    std::vector<std::string> fields;
    size_t numBytes = 0;
    auto ec = parseRow(test_str.c_str(), test_str.c_str() + test_str.length(), fields, numBytes, ',');

    EXPECT_EQ(ec, ExceptionCode::SUCCESS);

    EXPECT_EQ_ARRAY(fields, {"title","address","city","state","postal_code","price","facts and features","real estate provider","url","sales_date"});

    // another line
    fields.clear();
    ec = parseRow(test_str.c_str() + numBytes, test_str.c_str() + test_str.length(), fields, numBytes, ',', '"', true);
    EXPECT_EQ(ec, ExceptionCode::SUCCESS);

    EXPECT_EQ_ARRAY(fields, {"House For Sale","7 Parker St","WOBURN","MA","1801.0","\"$489,000\"","\"3 bds , 1 ba , 1,560 sqft\"","J. Mulkerin Realty","https://www.zillow.com/homedetails/7-Parker-St-Woburn-MA-01801/56391529_zpid/","Open: Sat. 11am-1pm"});
}

//@TODO: spectrum-csv tests


// test for parsing rows into internal row object
TEST(CSVUtils, ParseBufferToRows) {
    auto buf = "a,b,c\n"
               "1,3.4,hello\n"
               "NULL,9,9,,9\n";

    auto rows = tuplex::parseRows(buf, buf + strlen(buf), {"", "NULL"});
    ASSERT_EQ(rows.size(), 3);
    EXPECT_EQ(rows[0].getNumColumns(), 3);
    EXPECT_EQ(rows[0].getRowType().desc(), "(str,str,str)");
    EXPECT_EQ(rows[1].getNumColumns(), 3);
    EXPECT_EQ(rows[1].getRowType().desc(), "(i64,f64,str)"); // 1 is mapped as i64!
    EXPECT_EQ(rows[2].getNumColumns(), 5);
    EXPECT_EQ(rows[2].getRowType().desc(), "(null,i64,i64,null,i64)");
}