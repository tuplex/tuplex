//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <URI.h>
#include "gtest/gtest.h"
#include "../../utils/include/Utils.h"
#include "VirtualFileSystem.h"
#include <regex>

using namespace tuplex;


TEST(MemString, Basic) {
    EXPECT_EQ(memStringToSize("987"), 987);
    EXPECT_EQ(memStringToSize("987MB"), 987 * 1024 * 1024);
    EXPECT_EQ(memStringToSize("98mb15b"), 98 * 1024 * 1024 + 15);
    EXPECT_EQ(memStringToSize("7.5GB"), 7.5 * 1024 * 1024 * 1024);
}

TEST(MemString, Malformed) {
    EXPECT_EQ(memStringToSize("7643a"), 0);
}

TEST(VecUtils, Reverse) {
    std::vector<int> vRes1({1, 2, 3, 4, 5});
    std::vector<int> vRes2({1, 2});

    std::vector<int> v1({5, 4, 3, 2, 1});
    std::vector<int> v2({2, 1});
    reverseVector(v1);
    reverseVector(v2);

    EXPECT_EQ(vRes1, v1);
    EXPECT_EQ(vRes2, v2);
}

TEST(VecUtils, SortAsc) {
    using namespace std;
    using namespace tuplex;
    EXPECT_TRUE(isSortedAsc(vector<int>{}));
    EXPECT_TRUE(isSortedAsc(vector<int>{1, 2, 3}));
    EXPECT_TRUE(isSortedAsc(vector<int>{1}));
    EXPECT_TRUE(isSortedAsc(vector<int>{-10, 20, 90}));
    EXPECT_FALSE(isSortedAsc(vector<int>{-90, 89, 0}));
    EXPECT_FALSE(isSortedAsc(vector<int>{5, 4, 3}));
    EXPECT_FALSE(isSortedAsc(vector<int>{1, 2, 4, 5, 4, 3, 2, 1}));
}

TEST(URI, INVALID) {
    using namespace tuplex;

    URI uriHDFS = URI("hdfs://jfhjg/kfjgkg");
    URI uriLOCAL = URI("file://jfhjg/kfjgkg");
    URI uriS3 = URI("s3://jfhjg/kfjgkg");

    EXPECT_FALSE(URI::INVALID == uriHDFS);
    EXPECT_FALSE(URI::INVALID == uriLOCAL);
    EXPECT_FALSE(URI::INVALID == uriS3);
}

TEST(URI, equal) {
    using namespace tuplex;

    URI uriA = URI("file://test/test/test.txt");
    URI uriB = URI("file://test/test/test.txt");
    URI uriC = URI("file://test/test/test123.txt");
    URI uriD = URI("hdfs://test/test/test.txt");

    EXPECT_TRUE(uriA == uriB);
    EXPECT_FALSE(uriA == uriC);
    EXPECT_FALSE(uriA == uriD);
}

TEST(Regex, ExtractTimeout) {
    using namespace std;

    string test_log = "4194304, \"allowNumericTypeUnification\":false, \"useInterpreterOnly\":false, \"useCompiledGeneralPath\":true, \"opportuneGeneralPathCompilation\":true, \"useOptimizer\":true, \"sampleLimitCount\":30000, \"strataSize\":1024, \"samplesPerStrata\":1, \"useFilterPromotion\":true, \"useConstantFolding\":true, \"normalCaseThreshold\":0.9, \"exceptionSerializationMode\":2, \"s3PreCacheSize\":0}\n[2023-04-13 23:39:28.516] [Lambda worker] [info] Invoking WorkerApp fallback\n[2023-04-13 23:39:28.518] [Lambda worker] [info] Found 1 input URIs to process\n[2023-04-13 23:39:28.518] [Lambda worker] [info] *** hyperspecialization active ***\n[2023-04-13 23:39:28.518] [Lambda worker] [info] -- specializing to s3://tuplex-public/data/flights_all/flights_on_time_performance_1991_11.csv:0-168157034\n[2023-04-13 23:39:28.518] [Lambda worker] [info] fast code path size before hyperspecialization: 0.00 B\n[2023-04-13 23:39:28.518] [hyper specializer] [info] specializing code to file s3://tuplex-public/data/flights_all/flights_on_time_performance_1991_11.csv:0-168157034\n[2023-04-13 23:39:28.520] [hyper specializer] [info] Decompressed Code context from 19.24 KB to 176.79 KB\n[2023-04-13 23:39:28.600] [hyper specializer] [info] Deserialization of Code context took 0.080392s\n[2023-04-13 23:39:28.600] [hyper specializer] [info] Total Stage Decode took 0.081773s\n[2023-04-13 23:39:28.600] [hyper specializer] [info] number of input columns before hyperspecialization: 23\n[2023-04-13 23:39:29.758] [global] [info] sampled s3://tuplex-public/data/flights_all/flights_on_time_performance_1991_11.csv:0-168157034 on 32.00 MB\n[2023-04-13 23:39:29.781] [global] [info] s3 request time spent on s3://tuplex-public/data/flights_all/flights_on_time_performance_1991_11.csv: 1.13522s\n[2023-04-13 23:39:29.782] [fileinputoperator] [info] Sample fetch done.\n[2023-04-13 23:39:29.782] [fileinputoperator] [info] Filling sample cache for csv operator took 1.181649s (1 entry)\n[2023-04-13 23:39:30.394] [global] [info] sampled s3://tuplex-public/data/flights_all/flights_on_time_performance_1991_11.csv:0-168157034 on 32.00 MB\n[2023-04-13 23:39:30.423] [global] [info] s3 request time spent on s3://tuplex-public/data/flights_all/flights_on_time_performance_1991_11.csv: 0.588503s\n[2023-04-13 23:39:31.808] [global] [info] sampled s3://tuplex-public/data/flights_all/flights_on_time_performance_1991_11.csv:0-168157034 on 32.00 MB\n[2023-04-13 23:39:31.830] [global] [info] s3 request time spent on s3://tuplex-public/data/flights_all/flights_on_time_performance_1991_11.csv: 1.25688s\n[2023-04-13 23:39:31.940] [logical] [info] sampled 156 rows in 2.157589s (13.830699ms / row)\n[2023-04-13 23:39:31.953] [fileinputoperator] [info] Extracting stratified row sample took 2.170851s (sampling mode=FIRST_ROWS|LAST_ROWS|FIRST_FILE|LAST_FILE|SINGLE_THREADED)\n[2023-04-13 23:39:31.954] [hyper specializer] [info] sampling (setInputFiles) took 3.353478s (limit=30000)\n[2023-04-13 23:39:31.954] [global] [warning] requested 10000 rows for sampling, but only 156 stored. Consider decreasing sample size. Returning all available rows.\n[2023-04-13 23:39:31.960] [specializing stage optimizer] [info] Row type before retype: (i64,i64,i64,i64,i64,str,i64,i64,str,str,i64,str,str,i64,f64,i64,f64,f64,null,null,null,null,null)\n[2023-04-13 23:39:31.960] [specializing stage optimizer] [info] Row type after retype: (i64,i64,i64,i64,i64,str,i64,i64,str,str,i64,str,str,i64,f64,i64,f64,f64,null,null,null,null,null)\n[2023-04-13 23:39:31.966] [global] [error] output schema for withColumn operator is not well-defined, propagation error?\n[2023-04-13 23:39:31.968] [global] [error] output schema for map operator is not well-defined, propagation error?\n[2023-04-13 23:39:31.969] [global] [error] output schema for filter operator is not well-defined, propagation error?\n2023-04-13T23:39:43.521Z cd248eea-2265-49c9-9909-7c7741984b81 Task timed out after 15.02 seconds\n\nEND RequestId: cd248eea-2265-49c9-9909-7c7741984b81\nREPORT RequestId: cd248eea-2265-49c9-9909-7c7741984b81\tDuration: 15022.38 ms\tBilled Duration: 15000 ms\tMemory Size: 10000 MB\tMax Memory Used: 330 MB\t\n";

    auto output = extractTimeoutStr(test_log);
    EXPECT_EQ(output, "15.02");
}

// other test log:
TEST(regex, ExtractExitCode) {
    using namespace std;

    string test_log = "START RequestId: 5d48655b-8551-4eb6-a455-3972c5134c6f Version: $LATEST\nPrevious invocation recevied unrecoverable signal, shutting down this Lambda container via exit(0).\nRequestId: 5d48655b-8551-4eb6-a455-3972c5134c6f Error: Runtime exited with error: exit status 1\nRuntime.ExitError\nEND RequestId: 5d48655b-8551-4eb6-a455-3972c5134c6f\nREPORT RequestId: 5d48655b-8551-4eb6-a455-3972c5134c6f\tDuration: 39.68 ms\tBilled Duration: 40 ms\tMemory Size: 10000 MB\tMax Memory Used: 317 MB\t\n";
    auto output = extractExitCodeStr(test_log);
    EXPECT_EQ(output, "1");
}