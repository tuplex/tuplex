//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifdef BUILD_WITH_AWS

#include "TestUtils.h"
#include <ee/aws/AWSLambdaBackend.h>
#include <AWSCommon.h>
#include <VirtualFileSystem.h>
#include <PosixFileSystemImpl.h>
#include <FilePart.h>

#include "FullPipelines.h"

class AWSTest : public PyTest {
protected:

    void SetUp() override {
        PyTest::SetUp();

        using namespace tuplex;

        // to speedup testing, if we anyways skip the tests, can skip init here too.
        // !!! Dangerous !!!
#ifndef SKIP_AWS_TESTS
        initAWS(AWSCredentials::get(), NetworkSettings(), true);
        VirtualFileSystem::addS3FileSystem();
#endif
    }
};

TEST_F(AWSTest, BucketOperations) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace tuplex;
    using namespace std;

    // checks whether connection to test bucket can be established or not
    auto root_folder = URI(std::string("s3://") + S3_TEST_BUCKET);
    VirtualFileSystem vfs = VirtualFileSystem::fromURI(root_folder);
    vector<URI> found_uris = vfs.globAll(root_folder.toPath());

    cout<<"found "<<found_uris.size()<<" files"<<endl;
    //EXPECT_EQ(rc, VirtualFileSystemStatus::VFS_OK);

    // remove all files within bucket

    // now create test file, download etc. compare contents

    // need file upload/download...
}

TEST_F(AWSTest, FolderCopy) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace tuplex;
    using namespace std;

    // this here is to test several copy functions
    // local -> local
    // local -> s3
    // s3 -> local
    // s3 -> s3

    auto json_sample="{\"test\":\"hello world\"}";
    auto csv_sample="A,B,C\n1,2,3\n4,5,6\n7,8,9\n";

    auto s3TestPrefix = string("s3://") + S3_TEST_BUCKET + "/";

    // test all combos
    for(auto inPrefix : vector<string>{"file://", s3TestPrefix})
        for(auto outPrefix : vector<string>{"file://", s3TestPrefix}) {
            auto input_fs = VirtualFileSystem::fromURI(inPrefix);
            auto output_fs = VirtualFileSystem::fromURI(outPrefix);
            input_fs.create_dir(inPrefix + "test_in_folder/subfolder");
            stringToFile(inPrefix + "test_in_folder/test.csv", csv_sample);
            stringToFile(inPrefix + "test_in_folder/sample.json", json_sample);
            stringToFile(inPrefix + "test_in_folder/subfolder/sub1.csv", csv_sample);
            stringToFile(inPrefix + "test_in_folder/subfolder/sub2.json", json_sample);

            VirtualFileSystemStatus rc;
            string content;
            // first, copy single file
            // part I: single file
            output_fs.remove(outPrefix + "test_out_folder");
            rc = VirtualFileSystem::copy(inPrefix + "test_in_folder/test.csv", outPrefix + "test_out_folder/test.csv");
            EXPECT_EQ(rc, VirtualFileSystemStatus::VFS_OK);
            content = fileToString(outPrefix + "test_out_folder/test.csv");
            EXPECT_EQ(content, csv_sample);
            output_fs.remove(outPrefix + "test_out_folder");

            // part II: pattern, i.e. only json files
            rc = VirtualFileSystem::copy(inPrefix + "test_in_folder/*.json", outPrefix + "test_out_folder/");
            EXPECT_EQ(rc, VirtualFileSystemStatus::VFS_OK);
            content = fileToString(outPrefix + "test_out_folder/sample.json");
            EXPECT_EQ(content, json_sample);
            output_fs.remove(outPrefix + "test_out_folder");

            // part III: copy multiple targets at once
            rc = VirtualFileSystem::copy(inPrefix + "test_in_folder/*.json," + inPrefix + "test_in_folder/*/*.json", outPrefix + "test_out_folder");
            EXPECT_EQ(rc, VirtualFileSystemStatus::VFS_OK);
            content = fileToString(outPrefix + "test_out_folder/sample.json");
            EXPECT_EQ(content, json_sample);
            content = fileToString(outPrefix + "test_out_folder/subfolder/sub2.json");
            EXPECT_EQ(content, json_sample);
            output_fs.remove(outPrefix + "test_out_folder");

            // part IV: copy single file matching subfolder pattern
            rc = VirtualFileSystem::copy(inPrefix + "test_in_folder/*/*.json", outPrefix + "test_out_folder");
            EXPECT_EQ(rc, VirtualFileSystemStatus::VFS_OK);
            content = fileToString(outPrefix + "test_out_folder");
            EXPECT_EQ(content, json_sample);
            output_fs.remove(outPrefix + "test_out_folder");

            // part V: copy full folder
            rc = VirtualFileSystem::copy(inPrefix + "test_in_folder/*", outPrefix + "test_out_folder");
            EXPECT_EQ(rc, VirtualFileSystemStatus::VFS_OK);
            content = fileToString(outPrefix + "test_out_folder/sample.json");
            EXPECT_EQ(content, json_sample);
            content = fileToString(outPrefix + "test_out_folder/test.csv");
            EXPECT_EQ(content, csv_sample);
            content = fileToString(outPrefix + "test_out_folder/subfolder/sub2.json");
            EXPECT_EQ(content, json_sample);
            content = fileToString(outPrefix + "test_out_folder/subfolder/sub1.csv");
            EXPECT_EQ(content, csv_sample);
            output_fs.remove(outPrefix + "test_out_folder");
        }
}

TEST_F(AWSTest, FileUploadAndDownload) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace tuplex;
    using namespace std;

    auto root_folder = URI(std::string("s3://") + S3_TEST_BUCKET);
    VirtualFileSystem vfs = VirtualFileSystem::fromURI(root_folder);

    // upload file & download again, then compare
    URI local_path("../resources/pipelines/311/311-service-requests.csv");
    auto content = fileToString(local_path);

    // upload
    URI s3uri(root_folder.toPath() + "/311-service-requests.csv");
    auto upload_handle = vfs.s3UploadFile(local_path.toPath(), s3uri, "text/csv");
    EXPECT_TRUE(upload_handle);

    // download
    auto download_handle = vfs.s3DownloadFile(s3uri, "test.csv");
    EXPECT_TRUE(download_handle);

    // read and compare
    auto test_content = fileToString("test.csv");
    auto rc = strcmp(content.c_str(), test_content.c_str());
    EXPECT_EQ(rc, 0);

    // glob test
    auto files = vfs.globAll(root_folder.toPath() + "/*");
    ASSERT_GE(files.size(), 1);
    auto it = std::find(files.begin(), files.end(), s3uri);
    EXPECT_NE(it, files.end());

    // delete path
    auto status = vfs.remove(s3uri);
    EXPECT_EQ(status, VirtualFileSystemStatus::VFS_OK);

    // glob again to test file was properly removed
    files = vfs.globAll(root_folder.toPath() + "/*");
    it = std::find(files.begin(), files.end(), s3uri);
    EXPECT_EQ(it, files.end());
}

TEST_F(AWSTest, SimpleLambdaInvoke) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;

    Context c(microLambdaOptions());

    // computes some simple function in the cloud
    vector<Row> data;
    vector<Row> ref;
    int N = 5;
    for(int i = 0; i < N; ++i) {
        data.push_back(Row(i));
        ref.push_back(Row(i, i*i));
    }

    auto v = c.parallelize(data).map(UDF("lambda x: (x, x*x)")).collectAsVector();
    ASSERT_EQ(v.size(), N);
    for(int i = 0; i < N; ++i)
        EXPECT_EQ(v[i].toPythonString(), ref[i].toPythonString());
}

TEST_F(AWSTest, MultipleLambdaInvoke) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;

    Context c(microLambdaOptions());

    // computes some simple function in the cloud
    vector<Row> data;
    vector<Row> ref;
    int N = 5;
    for(int i = 0; i < N; ++i) {
        data.push_back(Row(i));
        ref.push_back(Row(i, i*i));
    }

    auto v = c.parallelize(data).map(UDF("lambda x: (x, x*x)")).collectAsVector();
    ASSERT_EQ(v.size(), N);
    for(int i = 0; i < N; ++i)
        EXPECT_EQ(v[i].toPythonString(), ref[i].toPythonString());

    // 2nd invocation
    v = c.parallelize(data).map(UDF("lambda x: (x, x*x)")).collectAsVector();
    ASSERT_EQ(v.size(), N);
    for(int i = 0; i < N; ++i)
        EXPECT_EQ(v[i].toPythonString(), ref[i].toPythonString());
}

TEST_F(AWSTest, RequesterPays) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;

    Context c(microLambdaOptions());

    // make sure this is public??
    auto v = c.csv("s3://tuplex-public/test.csv").collectAsVector();
    ASSERT_GT(v.size(), 0);
}


TEST_F(AWSTest, ReadSingleCSVFile) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;

    Context c(microLambdaOptions());

    // make sure this is public??
    auto v = c.csv("s3://tuplex-public/test.csv").collectAsVector();
    ASSERT_GT(v.size(), 0);
}

// c.csv('s3://tuplex-public/data/100GB/zillow_00001.csv').show(5)

TEST_F(AWSTest, ShowFromSingleFile) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;
    auto opt = microLambdaOptions();
    opt.set("tuplex.aws.lambdaMemory", "6432");
    Context c(opt);

    // make sure this is public??
    c.csv("s3://tuplex-public/data/100GB/zillow_00001.csv").show(5);
}

TEST_F(AWSTest, BucketList) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;

    Context c(microLambdaOptions());

    vector<URI> uris;
    auto vfs = VirtualFileSystem::fromURI("s3://");

    vfs.ls("s3://tuplex-public", uris);

    for(auto uri : uris) {
        cout<<uri.toString()<<endl;
    }



    // list buckets
    vfs.ls("s3://", uris);
    for(auto uri : uris) {
        cout<<uri.toString()<<endl;
    }
    uris.clear();
    vfs.ls("s3:///", uris); // <-- special case, list here too!
    for(auto uri : uris) {
        cout<<uri.toString()<<endl;
    }
    uris.clear();


//    // make sure this is public??
//
//    // check single file -> single file.
//    // check folder
//
//
//    // create glob pattern from ls pattern.
//    // -> split into parts from ,
//
//    // this is completely incorrect...
//    // ls retrieves folders AND files...
//    // -> need to make this work properly using s3walk...
//
//    std::string pattern = "s3://tuplex-public/test.csv,s3://tuplex-public";
//    // "s3://tuplex-public,s3://tuplex-public/*")
//    std::string glob_pattern;
//    splitString(pattern, ',', [&glob_pattern](std::string subpattern) {
//        if(!glob_pattern.empty())
//            glob_pattern += ",";
//       glob_pattern += subpattern + "," + subpattern + "/*";
//    });
//    std::cout<<"matching using: "<<glob_pattern<<endl;
//    auto uris = VirtualFileSystem::globAll(glob_pattern);
//
//    // unique paths? sort? ==> yes.
//
//
//    for(auto uri : uris) {
//        cout<<uri.toString()<<endl;
//    }
//    auto v = c.ls("s3://tuplex-public");
//
//    for(auto el : v) {
//        cout<<el<<endl;
//    }
    //ASSERT_GT(v.size(), 0);
    uris.clear();
}

TEST_F(AWSTest, FileSplitting) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;

    // splitting a single part across 6 threads!
    URI partURI("s3://tuplex-public/data/100GB/zillow_00001.csv:0-62500637");
    FilePart fp;
    fp.size = 250002549;
    decodeRangeURI(partURI.toString(), fp.uri, fp.rangeStart, fp.rangeEnd);

    auto parts = splitIntoEqualParts(6, {fp}, 1024 * 1024);
    for(auto tp : parts) {
        auto tfp = tp.front();
        std::cout<<encodeRangeURI(tfp.uri, tfp.rangeStart, tfp.rangeEnd)<<std::endl;
    }

//    // glob 100GB files
//
//    auto inputFiles = "s3://tuplex-public/data/100GB/*.csv"; // 100GB of data
//
//    vector<URI> uris;
//    vector<size_t> sizes;
//
//    VirtualFileSystem::walkPattern(URI(inputFiles), [&](void *userData, const tuplex::URI &uri, size_t size) {
//        uris.push_back(uri);
//        sizes.push_back(size);
//        return true;
//    });
//
//    cout<<"Found "<<pluralize(uris.size(), "file")<<endl;
//
//    // split into parts...
//
//    int N = 800;
//    auto parts = splitIntoEqualParts(N, uris, sizes);
//    ASSERT_EQ(parts.size(), N);
//
//    // print out first part (that's the weird one!)
//    size_t totalFirstBytes = 0;
//    for(auto p : parts.front()) {
//        totalFirstBytes += p.part_size();
//        std::cout<<"- "<<p.uri.toString()<<std::endl;
//    }
//    std::cout<<"first part got: "<<totalFirstBytes<<" bytes "<<sizeToMemString(totalFirstBytes)<<std::endl;
//
//    // merge parts now together & redistribute again
//    std::vector<FilePart> mergedParts;
//    for(auto pit = parts.begin() + 1; pit != parts.end(); ++pit)
//        std::copy(pit->begin(), pit->end(), std::back_inserter(mergedParts));
//    EXPECT_EQ(mergedParts.size(), 799 );
//
//    // redistribute:

}

TEST_F(AWSTest, LambdaCounts) {
    using namespace tuplex;

    EXPECT_EQ(lambdaCount({}), 0);
    EXPECT_EQ(lambdaCount({1, 1, 1}), 4);
    EXPECT_EQ(lambdaCount({2, 1}), 5);
    EXPECT_EQ(lambdaCount({2, 2, 1}), 11);

    // offset and part add up test
    EXPECT_EQ(1 + lambdaCount({2, 1}) + lambdaCount({2, 1}), lambdaCount({2, 2, 1}));

    std::cout<<"Lambda {4, 4, 4, 4, 4}: "<<lambdaCount({4, 4, 4, 4, 4})<<std::endl;
}


TEST_F(AWSTest, FlightBasedJoin) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;

    auto opt = microLambdaOptions();

    // startegies:
    // 1. no-op Lambda spin out experiment
    opt.set("tuplex.aws.lambdaInvokeOthers", "true");
    opt.set("tuplex.aws.lambdaMemory", "10000");
    opt.set("tuplex.aws.maxConcurrency", "120");
    opt.set("tuplex.aws.lambdaThreads", "4"); // AWS EMR compatible setting

    // just edit one...
    opt.set("tuplex.aws.lambdaInvocationStrategy", "direct");

    string inputFiles = "s3://tuplex-public/data/100GB/zillow_00001.csv";
    string outputDir = string("s3://") + S3_TEST_BUCKET + "/tests/" + testName + "/zillow_output.csv";
    Context ctx(opt);

    // for join always multiple options:
    // local to remote?
    // remote to remote?

    string airport_uri = "s3://tuplex-public/data/flights/GlobalAirportDatabase.txt";
    string flights_uri = "s3://tuplex-public/data/flights_on_time_performance_2009_01.10k.csv";

    auto& ds = ctx.csv(airport_uri,
                        vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                       "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                       "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                        option<bool>::none, option<char>(':'));

    auto& ds_final = ctx.csv(flights_uri).renameColumn("ORIGIN", "Origin").renameColumn("DEST", "Dest")
            .leftJoin(ds, std::string("Origin"), std::string("IATACode"),std::string(), std::string(), std::string("Origin"), std::string())
            .leftJoin(ds, std::string("Dest"), std::string("IATACode"),std::string(), std::string(), std::string("Dest"), std::string())
            .selectColumns({"OriginAirportName", "DestAirportName", "OriginCountry", "DestCountry", "OriginLatitudeDegrees", "DestLatitudeDegrees"});

    ds_final.show(5);
}

// zillow Pipeline on AWS Lambda (incl. various options -> multithreading, self-invocation, ...)
TEST_F(AWSTest, FullZillowPipeline) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;

    auto opt = microLambdaOptions();

    // startegies:
    // 1. no-op Lambda spin out experiment
    opt.set("tuplex.aws.lambdaInvokeOthers", "true");
    opt.set("tuplex.aws.lambdaMemory", "10000");
    opt.set("tuplex.aws.maxConcurrency", "120");
    opt.set("tuplex.aws.lambdaThreads", "4"); // AWS EMR compatible setting

    opt.set("tuplex.aws.lambdaInvocationStrategy", "direct");
    opt.set("tuplex.useInterpreterOnly", "true");

//    auto inputFiles = "s3://tuplex-public/data/100GB/*.csv"; // 100GB of data
    string inputFiles = "s3://tuplex-public/data/100GB/zillow_00001.csv";
    string outputDir = string("s3://") + S3_TEST_BUCKET + "/tests/" + testName + "/zillow_output.csv";
    Context ctx(opt);

    Timer timer;
    auto ds = zillowPipeline(ctx, inputFiles);
    ds.tocsv(outputDir);
    cout<<"Lambda zillow took: "<<timer.time()<<endl;
    // 2. get S3 thing working



//    // Experiment 1: plain, single-threaded option -> 1792MB
//
//    // use 6 threads and 10GB of RAM
//    opt.set("tuplex.aws.lambdaMemory", "10000");
//    opt.set("tuplex.aws.maxConcurrency", "4");
//    string inputFiles = "s3://tuplex-public/data/100GB/zillow_00001.csv";
//
//    // now more complex test:
//    opt.set("tuplex.aws.lambdaMemory", "10000");
//    opt.set("tuplex.aws.maxConcurrency", "400");
//    inputFiles = "s3://tuplex-public/data/100GB/*.csv"; // 100GB of data
//
//
//    // now more complex test: --> this will result in Resource temporarily unavailable... -> request again!
//    opt.set("tuplex.aws.lambdaMemory", "10000");
//    opt.set("tuplex.aws.maxConcurrency", "800");
//    inputFiles = "s3://tuplex-public/data/100GB/*.csv"; // 100GB of data
//
//    // Something broken in splitPArts functioN!!!
//
//    // ==> Need to fix that!!!
//
//
//
//    //    opt.set("tuplex.aws.maxConcurrency", "800");
//
//    // TOOD: test over single file...
//
//
//    // Re broken pipe, try out maybe: options.httpOptions.installSigPipeHandler = true;
//
//
////    inputFiles = "s3://tuplex-public/data/100GB/*.csv";



//    string outputDir = string("s3://") + S3_TEST_BUCKET + "/tests/" + testName + "/zillow_output.csv";
//    Context ctx(opt);
//
//    Timer timer;
//    auto ds = zillowPipeline(ctx, inputFiles);
//    ds.tocsv(outputDir);
//    cout<<"Lambda zillow took: "<<timer.time()<<endl;
}

#endif // BUILD_WITH_AWS