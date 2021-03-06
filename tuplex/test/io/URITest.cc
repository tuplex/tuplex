//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <VirtualFileSystem.h>
#include <URI.h>
#include <FileUtils.h>
#include "FileSystemUtils.h"

TEST(URI, parent) {
    // test parent function for various URIs
    using namespace tuplex;

    // inspired from Pathlib path.
    EXPECT_EQ(URI("s3://tuplex/test").parent().toPath(), URI("s3://tuplex").toPath());
    EXPECT_EQ(URI("s3://tuplex/test/").parent().toPath(), URI("s3://tuplex").toPath());
    EXPECT_EQ(URI("s3://tuplex").parent().toPath(), URI("s3://.").toPath());
    EXPECT_EQ(URI("/tuplex/test").parent().toPath(), URI("/tuplex").toPath());
    EXPECT_EQ(URI("/tuplex/test/").parent().toPath(), URI("/tuplex").toPath());
    EXPECT_EQ(URI("/tuplex/test////////////").parent().toPath(), URI("/tuplex").toPath());
    EXPECT_EQ(URI("/tuplex").parent().toPath(), URI("/").toPath());
    EXPECT_EQ(URI("tuplex/test").parent().toPath(), URI("tuplex").toPath());
    EXPECT_EQ(URI("tuplex/test/").parent().toPath(), URI("tuplex").toPath());
    EXPECT_EQ(URI("tuplex").parent().toPath(), URI(".").toPath());
}

TEST(URI, EliminateSeparatorRuns) {
    using namespace tuplex;

    EXPECT_EQ(eliminateSeparatorRuns("//file////////test/hello/"), "/file/test/hello/");
    EXPECT_EQ(eliminateSeparatorRuns("file://", std::string("file://").length()), "file://");
    EXPECT_EQ(eliminateSeparatorRuns("file://"), "file:/");

}

TEST(URI, ListPosix) {
    using namespace tuplex;
    using namespace std;

    auto vfs = VirtualFileSystem::fromURI("file:///usr");

    vector<URI> uris;
    vfs.ls("file:///", uris);
    for(auto uri : uris)
        cout<<uri.toString()<<endl;
}

TEST(URI, OutputSpecification) {
    using namespace tuplex;
    // make sure output specification does work...
    // create new temp folder & base output testing on it!

    // basic tests: ., .. and so on should not be allowed!
    EXPECT_FALSE(validateOutputSpecification("/proc"));
    EXPECT_FALSE(validateOutputSpecification("."));
    EXPECT_FALSE(validateOutputSpecification(".."));
    EXPECT_FALSE(validateOutputSpecification("../.."));
    EXPECT_FALSE(validateOutputSpecification(".././.."));
    EXPECT_FALSE(validateOutputSpecification("/"));
    EXPECT_FALSE(validateOutputSpecification("/usr"));

    // empty new temp folder
    auto temp_folder = create_temporary_directory();
    ASSERT_TRUE(!temp_folder.empty());

    EXPECT_TRUE(validateOutputSpecification(temp_folder));

    // check 1: place folder in directory -> violates empty folder principle!
    auto temp_in_temp_folder = create_temporary_directory(temp_folder);
    ASSERT_TRUE(!temp_in_temp_folder.empty());
    EXPECT_FALSE(validateOutputSpecification(temp_folder));

    // check 2: file in folder
    temp_folder = create_temporary_directory();
    ASSERT_TRUE(!temp_folder.empty());
    EXPECT_TRUE(validateOutputSpecification(temp_folder));

    stringToFile(temp_folder + "/test.txt", "Hello world!");
    EXPECT_FALSE(validateOutputSpecification(temp_folder));

    // check 3: overwriting a file is ok!
    EXPECT_TRUE(validateOutputSpecification(temp_folder + "/test.txt"));
}

TEST(URI, Writable) {
    using namespace tuplex;
    EXPECT_TRUE(isWritable("."));

    auto non_existing_path = uniqueFileName();
    EXPECT_TRUE(isWritable("test.txt"));
    EXPECT_TRUE(isWritable(non_existing_path));
}

#ifdef BUILD_WITH_AWS
TEST(URI, CorrectS3Behavior) {
    using namespace tuplex;
    EXPECT_EQ(URI("s3://tuplex-public").s3Key(), "");
    EXPECT_EQ(URI("s3://tuplex-public").s3Bucket(), "tuplex-public");
    EXPECT_EQ(URI("s3://tuplex-public/test").s3Key(), "test");
    EXPECT_EQ(URI("s3://tuplex-public/test").s3Bucket(), "tuplex-public");
    EXPECT_EQ(URI("s3://").s3Key(), "");
    EXPECT_EQ(URI("s3://").s3Bucket(), "");
}
#endif

