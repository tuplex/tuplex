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
#include <Context.h>
#include <cstdio>

class CSVSelectionPushDown : public PyTest {};

using namespace tuplex;

TEST_F(CSVSelectionPushDown, SimpleMap) {
    FILE *file = fopen(testName + ".csv", "w");
    fprintf(file, "a,b,c,d\n");
    fprintf(file, "1,2,3,4\n");
    fprintf(file, "5,6,7,8\n");
    fprintf(file, "9,10,11,12\n");
    fclose(file);

    auto co = microTestOptions();
    co.set("tuplex.csv.selectionPushdown", "true");
    Context c(co);
    auto v = c.csv(testName + ".csv").map(UDF("lambda x: x[2]")).collectAsVector();
    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].getInt(0), 3);
    EXPECT_EQ(v[1].getInt(0), 7);
    EXPECT_EQ(v[2].getInt(0), 11);

    remove(testName + ".csv");
}

TEST_F(CSVSelectionPushDown, SimpleFilterAndMap) {
    FILE *file = fopen(testName + ".csv", "w");
    fprintf(file, "a,b,c,d\n");
    fprintf(file, "1,2,3,4\n");
    fprintf(file, "2,6,7,8\n");
    fprintf(file, "2,10,11,12\n");
    fprintf(file, "1,2,3,4\n");
    fprintf(file, "2,2,3,4\n");
    fclose(file);


    auto co = microTestOptions();
    co.set("tuplex.csv.selectionPushdown", "true");
    Context c(co);
    auto v = c.csv(testName + ".csv").filter(UDF("lambda x: x[0] == 2")).map(UDF("lambda x: x[-1]")).collectAsVector();
    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].getInt(0), 8);
    EXPECT_EQ(v[1].getInt(0),12);
    EXPECT_EQ(v[2].getInt(0), 4);

    remove(testName + ".csv");
}

// same as the test before, but this time not with tuple syntax.
TEST_F(CSVSelectionPushDown, SimpleFilterAndMapII) {
    FILE *file = fopen(testName + ".csv", "w");
    fprintf(file, "a,b,c,d\n");
    fprintf(file, "1,2,3,4\n");
    fprintf(file, "2,6,7,8\n");
    fprintf(file, "2,10,11,12\n");
    fprintf(file, "1,2,3,4\n");
    fprintf(file, "2,2,3,4\n");
    fclose(file);


    auto co = microTestOptions();
    co.set("tuplex.csv.selectionPushdown", "true");
    Context c(co);
    auto v = c.csv(testName + ".csv").filter(UDF("lambda a,b,c,d: a == 2")).map(UDF("lambda x,y,z, w: w")).collectAsVector();
    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].getInt(0), 8);
    EXPECT_EQ(v[1].getInt(0),12);
    EXPECT_EQ(v[2].getInt(0), 4);

    remove(testName + ".csv");
}

// mixed syntax
TEST_F(CSVSelectionPushDown, SimpleFilterAndMapIII) {
    FILE *file = fopen(testName + ".csv", "w");
    fprintf(file, "a,b,c,d\n");
    fprintf(file, "1,2,3,4\n");
    fprintf(file, "2,6,7,8\n");
    fprintf(file, "2,10,11,12\n");
    fprintf(file, "1,2,3,4\n");
    fprintf(file, "2,2,3,4\n");
    fclose(file);

    auto co = microTestOptions();
    co.set("tuplex.csv.selectionPushdown", "true");
    Context c(co);
    auto v = c.csv(testName + ".csv").filter(UDF("lambda a,b,c,d: a == 2")).map(UDF("lambda x: x[-2]")).collectAsVector();
    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].getInt(0), 7);
    EXPECT_EQ(v[1].getInt(0),11);
    EXPECT_EQ(v[2].getInt(0), 3);

    remove(testName + ".csv");
}