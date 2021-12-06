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