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


// zillow Pipeline on AWS Lambda (incl. various options -> multithreading, self-invocation, ...)
TEST_F(AWSTest, FullZillowPipeline) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif

    using namespace std;
    using namespace tuplex;




    Context c(microLambdaOptions());
}


#endif // BUILD_WITH_AWS