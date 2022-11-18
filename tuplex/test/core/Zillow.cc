//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Python.h>
#include <gtest/gtest.h>
#include <ContextOptions.h>
#include <vector>
#include <Utils.h>
#include <Context.h>
#include "TestUtils.h"
#include <CSVUtils.h>
#include <CSVStatistic.h>

class ZillowTest : public PyTest {};


TEST_F(ZillowTest, LargeDirtyFileParse) {
    using namespace tuplex;
    using namespace std;

    Context c(testOptions());

    // TODO: when not using a restricted test environment like azure pipelines
    //       run test on replicated dirty zillow data!
    auto z_path = "../resources/zillow_dirty.csv";
    auto extractType_c = "def extractType(x):\n"
                         "   t = x['title'].lower()\n"
                         "   type = 'unknown'\n"
                         "   if 'condo' in t or 'apartment' in t:\n"
                         "       type = 'condo'\n"
                         "   if 'house' in t:\n"
                         "       type = 'house'\n"
                         "   return type";

    c.csv(z_path).withColumn("type", UDF(extractType_c))
     .filter(UDF("lambda x: x['type'] == 'house'"))
     .tocsv("dirty_cleaned.csv");
}

TEST_F(ZillowTest, Thresholding) {

    using namespace tuplex;
    using namespace std;
    ContextOptions co = ContextOptions::defaults();

    vector<string> null_values{""};

    CSVStatistic csvstat(co.CSV_SEPARATORS(), co.CSV_COMMENTS(),
                         co.CSV_QUOTECHAR(), co.SAMPLE_MAX_DETECTION_MEMORY(),
                         co.SAMPLE_MAX_DETECTION_ROWS(), co.NORMALCASE_THRESHOLD(), null_values);

    // load a sample
    string path = "../resources/zillow_dirty.csv";

    auto content = fileToString(path);

    csvstat.estimate(content.c_str(), std::min(memStringToSize("1MB"), content.length()));

    cout<<csvstat.columns()<<endl;
    cout<<csvstat.type().desc()<<endl;

    // detected type for full zillow sample is: (str,Option[str],Option[str],Option[str],Option[f64],Option[str],str,null,str,str) when NULL value optimization is enabled...

}