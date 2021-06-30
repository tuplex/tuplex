//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 5/24/18                                                                  //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include "TestUtils.h"
#include <Context.h>
#include <Utils.h>
#include <vector>

//#include <boost/filesystem>
#include <fstream>
#include <boost/filesystem/operations.hpp>

using namespace tuplex;

TEST(CSVDataset, NonexistingFile) {
    Context c(testOptions());

    auto v = c.csv("../resources/doesnotexist.csv").collectAsVector();

    EXPECT_EQ(v.size(), 0);
}

TEST(CSVDataset, ParseWithoutHeader) {
    Context c(testOptions());

    auto v = c.csv("../resources/test.csv").collectAsVector();

    EXPECT_EQ(v.size(), 3);
}

// construct fake file & check whether right order is returned
TEST(CSVDataSet, ParseWithMultiplePartitions) {
    python::initInterpreter();

    python::unlockGIL();

    // write temp file
    FILE *fp = fopen("test.csv", "w");
    ASSERT_TRUE(fp);
    fprintf(fp, "columnA,columnB\n");
    auto N = 50000;
    for(int i = 0; i < N; ++i) {
        fprintf(fp, "%d,%d\n", i, i % 7);
    }
    fclose(fp);

    Context c(microTestOptions());
    auto v = c.csv("test.csv").mapColumn("columnB", UDF("lambda x: x * x")).collectAsVector();

    ASSERT_EQ(v.size(), N);

    printRows(v);

    // make sure order is correct!
    for(int i = 0; i < N; ++i) {
        EXPECT_EQ(v[i].getInt(0), i);
        EXPECT_EQ(v[i].getInt(1), (i % 7) * (i % 7));
    }

    python::lockGIL();
    python::closeInterpreter();
}

TEST(CSVDataSet, ParseWithMultiplePartitionsLimit) {
    python::initInterpreter();

    python::unlockGIL();

    // write temp file
    FILE *fp = fopen("test.csv", "w");
    ASSERT_TRUE(fp);
    fprintf(fp, "columnA,columnB\n");
    auto N = 50000;
    for(int i = 0; i < N; ++i) {
        fprintf(fp, "%d,%d\n", i, i % 7);
    }
    fclose(fp);

    size_t limit = 5;
    Context c(microTestOptions());
    auto v = c.csv("test.csv").mapColumn("columnB", UDF("lambda x: x * x")).takeAsVector(limit);

    ASSERT_EQ(v.size(), limit);

    printRows(v);

    // make sure order is correct!
    for(int i = 0; i < limit; ++i) {
        EXPECT_EQ(v[i].getInt(0), i);
        EXPECT_EQ(v[i].getInt(1), (i % 7) * (i % 7));
    }

    python::lockGIL();
    python::closeInterpreter();
}

////
//// Created by Leonhard Spiegelberg on 5/24/18.
////
//#include "gtest/gtest.h"
//#include <Context.h>
//#include <Utils.h>
//#include <vector>
//
//
//
//
//
//// cases
//// c some char (not " or ,)
//// assume cursor is in the middle
//// c c c        ==> can't decide
//// c c "        ==> can't decide
//// c c ,        ==> can decide by looking next char
//
//// c " c        ==> invalid
//// c " "        ==> in quoted field
//// c " ,        ==> can decide (next field)
//
//// c , c        ==> unquoted field starts after
//// c , "        ==> quoted field starts
//// c , ,
//
//// " c c
//// " c "
//// " c ,
//
//// " " c
//// " " "
//// " " ,
//
//// " , c
//// " , "
//// " , ,
//
//// , c c
//// , c "
//// , c ,
//
//// , " c
//// , " "
//// , " ,
//
//// , , c
//// , , "
//// , , ,
//
//
////const char* findNextCellStart(const char *start, const char *ptr, const char *end, char delimiter, char quotechar) {
////
////    // need to find status of parser at current position
////    auto p = ptr;
////    auto prev = ptr - 1;
////    auto next = ptr + 1;
////
////    if(*prev != quotechar && *p == quotechar && *next == quotechar) {
////        // p is in quoted cell
////    }
////
////    if(*prev == )
////
////
////    return nullptr;
////}
//
//WHAT TODO:
//
//small files: one thread parses them
//larger files: chunk them and parse them in two passes:
//pass 1) determine start / end of tasks --> i.e. this is one task, it spawns other tasks.
//pass 2) thread pool.
//
///*!
// * starts to scan from the given position up until end to determine cell start
// * @param ptr
// * @param end
// * @param delimiter
// * @param quotechar
// * @return nullptr if no cell begin was reached
// */
////const char* findNextCellStart(const char *start, const char *ptr, const char *end, char delimiter, char quotechar) {
////    // difficulty is to determine whether current ptr is in quoted cell or not.
////    // this is actually not possible to determine without searching both directions
////
////    assert(ptr < end);
////
////    // NEW DESIGN:
////
////    int numQuoteCharsSeen = 0;
////    // need to determine initial count of quote chars seen
////    // i.e. scan to the left
////    if(*ptr != quotechar)
////        numQuoteCharsSeen = 0;
////    else {
////        // important with start.
////        const char *p = ptr - 1;
////        numQuoteCharsSeen = 0;
////        while(p >= start && *p == quotechar) {
////            numQuoteCharsSeen++;
////            p--;
////        }
////    }
////    int qcs = numQuoteCharsSeen; // used for searching
////
////    // now go through the data until the end
////    // lookahead 1 for quoted
////    const char *p = ptr;
////    const char *quoted_pos = nullptr;
////    while(p < end - 1) {
////
////        // update how many quote chars have been seen
////        if(*p == quotechar)
////            qcs++;
////        else
////            qcs = 0;
////
////        auto pp = p + 1;
////
////        // case I: ", with , being the current one
////        if((qcs & 0x1)
////           && *p == quotechar
////           && *pp == delimiter) {
////            //pquoted = p + 2;
////            return p + 2;
////        }
////
////        // case II: "\n or "\r with \n or \r being the current char
////        if((qcs & 0x1)
////           && *p == quotechar
////           && (*pp == '\n' || *pp == '\r')) {
////
////            // for this to be a valid end of line
////            // the next char must be either a non quote char, end must be reached or an odd number of quotechars to follow
////            auto ppp = p + 2;
////            if(ppp == end)
////                return p + 2;
////            else if(ppp < end) {
////                if(quotechar == *ppp) {
////                    int qqcs = 1;
////                    while(ppp < end && *ppp == quotechar) {
////                        ++ppp;
////                        qqcs++;
////                    }
////
////                    // odd number?
////                    if(qqcs & 0x1)
////                        return p + 2;
////                    // else, continue search...
////
////                } else {
////                    // not a quote char, save to compare with unquoted
////                    quoted_pos = p;
////                }
////            } else {
////                // can't decide where cell is...
////                return nullptr;
////            }
////        }
////
////        ++p;
////    }
////
////    // special case:
////    // ends with " -->
////    if(*(end - 1) == quotechar)
////        return end + 1;
////
////    // now search for , or EOF iff no quoted version was found
////    p = ptr;
////    const char *unquoted_pos = nullptr;
////    while(p < end && !unquoted_pos) {
////        // if delimiter or newline is found, break
////        if(*p == delimiter)
////            unquoted_pos = p + 1;
////        if(*p == '\n' || *p == '\r')
////            unquoted_pos = p + 1;
////        ++p;
////    }
////
////    // comparing against
////    if(quoted_pos && unquoted_pos) {
////        // return larger
////        return quoted_pos + 2;
////    }
////
////    return unquoted_pos;
////
////    // nothing found...
////    // return end + 1
////    return end + 1;
////}
//
//// multiple test strings for determining cell start position
//TEST(CSVParser, CellStartUnquotedCell) {
//    std::string test = "Hello world,NEXT";
//    for(int i = 0; i < strlen("Hello world"); ++i) {
//        auto cell_start = findNextCellStart(test.c_str(),
//                                            test.c_str() + i,
//                                            test.c_str() + test.length(),
//                                            ',',
//                                            '"');
//        EXPECT_EQ(cell_start, test.c_str() + strlen("Hello world") + 1);
//    }
//
//}
//
//TEST(CSVParser, CellStartQuotedCell) {
//    std::string test = "\"Hello world\",NEXT";
//    for(int i = 0; i < strlen("\"Hello world\""); ++i) {
//        auto cell_start = findNextCellStart(test.c_str(),
//                                            test.c_str() + i,
//                                            test.c_str() + test.length(),
//                                            ',',
//                                            '"');
//        EXPECT_EQ(cell_start, test.c_str() + strlen("\"Hello world\"") + 1);
//    }
//
//}
//
//TEST(CSVParser, CellStartQuotedCellAtEOF) {
//    std::string test = "\"Hello world\"";
//    for(int i = 0; i < strlen("\"Hello world\""); ++i) {
//        auto cell_start = findNextCellStart(test.c_str(),
//                                            test.c_str() + i,
//                                            test.c_str() + test.length(),
//                                            ',',
//                                            '"');
//        EXPECT_EQ(cell_start, test.c_str() + strlen("\"Hello world\""));
//    }
//
//}
//
//TEST(CSVParser, CellStartQuotedCellAtEnd) {
//    std::string test = "\"Hello world\"\n\"test\"\n";
//    for(int i = 0; i < strlen("\"Hello world\""); ++i) {
//        std::cout<<"i: "<<i<<std::endl;
//        i = 12;
//        auto cell_start = findNextCellStart(test.c_str(),
//                                            test.c_str() + i,
//                                            test.c_str() + test.length(),
//                                            ',',
//                                            '"');
//        EXPECT_EQ(cell_start, test.c_str() + strlen("\"Hello world\"") + 1);
//    }
//}
//
//TEST(CSVParser, CellStartQuotedCellAtWithSpecialChars) {
//    std::string test = "\"\n\n\"\",\"\"\"\",,\n\"\"\n\"\"\"\",\"\"\n,,\",NEXT";
//    for(int i = 0; i < strlen("\"\n\n\"\",\"\"\"\",,\n\"\"\n\"\"\"\",\"\"\n,,\""); ++i) {
//        auto cell_start = findNextCellStart(test.c_str(),
//                                            test.c_str() + i,
//                                            test.c_str() + test.length(),
//                                            ',',
//                                            '"');
//        EXPECT_EQ(cell_start, test.c_str() + strlen("\"\n\n\"\",\"\"\"\",,\n\"\"\n\"\"\"\",\"\"\n,,\"") + 1);
//    }
//}
//
//TEST(CSVParser, CellStartQuotedCellAtWithSpecialCharsAtEnd) {
//    std::string test = "\"\n\n\"\",\"\"\"\",,\n\"\"\n\"\"\"\",\"\"\n,,\"";
//    for(int i = 0; i < strlen("\"\n\n\"\",\"\"\"\",,\n\"\"\n\"\"\"\",\"\"\n,,\""); ++i) {
//        auto cell_start = findNextCellStart(test.c_str(),
//                                            test.c_str() + i,
//                                            test.c_str() + test.length(),
//                                            ',',
//                                            '"');
//        EXPECT_EQ(cell_start, test.c_str() + strlen("\"\n\n\"\",\"\"\"\",,\n\"\"\n\"\"\"\",\"\"\n,,\"") + 1);
//    }
//}
//
//TEST(CSVParser, CellMixed) {
//    std::string test = "a,b,\"\"\"\",c";
//    std::vector<int> ref{2, 2, 4, 4, 9, 9, 9, 9, 10, 10};
//    for(int i = 0; i < test.length(); ++i) {
//        std::cout<<"i "<<i<<std::endl;
//        auto cell_start = findNextCellStart(test.c_str(),
//                                            test.c_str() + i,
//                                            test.c_str() + test.length(),
//                                            ',',
//                                            '"');
//        EXPECT_EQ(cell_start, test.c_str() + ref[i]);
//    }
//}
//
//TEST(CSVParser, CellStartSmallCSVFile) {
//    std::string test = "a,b\n"
//            "1,\"ha \n"
//            "\"\"ha\"\" \n"
//            "ha\"\n"
//            "3,4";
//    std::vector<int> ref{2, 2, 4, 4,
//                         6, 6, 23, 23, 23, 23, 23,
//                         23, 23, 23, 23, 23, 23, 23, 23,
//                         23, 23, 23, 23,
//                         24, 24, 25};
//    for(int i = 0; i < test.length(); ++i) {
//        std::cout<<"i "<<i<<std::endl;
//        auto cell_start = findNextCellStart(test.c_str(),
//                                            test.c_str() + i,
//                                            test.c_str() + test.length(),
//                                            ',',
//                                            '"');
//        EXPECT_EQ(cell_start, test.c_str() + ref[i]);
//    }
//}
//
//
//// So, the problem is as following:
//// It is not possible to determine the start position of a cell
//// --> only solution: fetch next row start from current cell (pretend to be in quoted cell).
//// attempt parse. If fails, start over with fetching next row from current cell (pretend to be in unquoted cell)
//// --> parse again
//// one of these should work. If not, then again, skip to next row. After this is done, go to next row.