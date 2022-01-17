//
// Created by Leonhard Spiegelberg on 1/4/22.
//

#include "FileSystemUtils.h"
#ifdef BUILD_WITH_AWS

#include <AWSCommon.h>
#include <VirtualFileSystem.h>

#ifndef S3_TEST_BUCKET
// define dummy to compile
#ifdef SKIP_AWS_TESTS
#define S3_TEST_BUCKET "tuplex-test"
#endif

#warning "need S3 Test bucket to run these tests"
#endif

static const std::string s3TestBase = "s3://" + std::string(S3_TEST_BUCKET) + "/tests";

class S3Tests : public ::testing::Test {
protected:
    std::string testName;

    void SetUp() override {
        using namespace tuplex;

        // init S3 file system
        auto cred = AWSCredentials::get();
        NetworkSettings ns;
        initAWS(cred, ns, true);
        VirtualFileSystem::addS3FileSystem(cred.access_key, cred.secret_key, cred.session_token, cred.default_region, ns, false, true);
        testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
    }
};

#ifndef SKIP_AWS_TESTS

TEST_F(S3Tests, FileSeek) {
    using namespace tuplex;

    // test file, write some stuff to it.
    URI file_uri(s3TestBase + "/" + testName + "/test.bin");
    auto file = VirtualFileSystem::open_file(file_uri, VirtualFileMode::VFS_OVERWRITE);

    // write 5 numbers
    for(int i = 0; i < 5; ++i) {
        int64_t x = i;
        file->write(&x, sizeof(int64_t));
    }
    file->close();
    file = nullptr;

    // 1. Basic Read of S3 file.
    // perform a couple checks
    file = VirtualFileSystem::open_file(file_uri, VirtualFileMode::VFS_READ);
    ASSERT_TRUE(file);
    EXPECT_EQ(file->size(), 5 * sizeof(int64_t));
    // read contents
    int64_t *buf = new int64_t[10];
    memset(buf, 0, sizeof(int64_t) * 10);
    size_t nbytes_read = 0;
    file->readOnly(buf, 5 * sizeof(int64_t), &nbytes_read);
    EXPECT_EQ(nbytes_read, 5 * sizeof(int64_t));
    for(int i = 0; i < 10; ++i) {
        if(i < 5)
            EXPECT_EQ(buf[i], i);
        else
            EXPECT_EQ(buf[i], 0);
    }
    file->close();

    // now perform file seeking!
    file = VirtualFileSystem::open_file(file_uri, VirtualFileMode::VFS_READ);
    ASSERT_TRUE(file);
    // seek forward 8 bytes!
    file->seek(sizeof(int64_t));

    // now read in bytes
    memset(buf, 0, sizeof(int64_t) * 5);
    file->readOnly(buf, 5 * sizeof(int64_t), &nbytes_read);
    EXPECT_EQ(nbytes_read, 4 * sizeof(int64_t));
    for(int i = 1; i < 10; ++i) {
        if(i < 5)
            EXPECT_EQ(buf[i - 1], i);
        else
            EXPECT_EQ(buf[i - 1], 0);
    }
}

#endif

#endif
