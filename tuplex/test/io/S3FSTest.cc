//
// Created by Leonhard Spiegelberg on 1/4/22.
//

#include "FileSystemUtils.h"
#include "S3File.h"

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

TEST_F(S3Tests, FileUploadLargerThanInternal) {
    // tests S3 writing capabilities
    using namespace tuplex;

    EXPECT_GE(S3File::INTERNAL_BUFFER_SIZE(), 0);

    auto internal_buf_size = S3File::INTERNAL_BUFFER_SIZE();

    // write S3 file that's larger than internal size
    auto test_buf_size = 2 * internal_buf_size;
    auto test_buf = new uint8_t[test_buf_size];
    memset(test_buf, 42, test_buf_size);

    auto s3_test_path = s3TestBase + "/" + testName + "/larger_than_internal_buf.bin";

    auto vfs = VirtualFileSystem::fromURI(URI("s3://"));

    // write parts...
    auto file = vfs.open_file(s3_test_path, VirtualFileMode::VFS_OVERWRITE);
    ASSERT_TRUE(file);
    file->write(test_buf, test_buf_size);
    file->close();

    // check file was written correctly
    uint64_t file_size;
    vfs.file_size(s3_test_path, file_size);
    EXPECT_EQ(file_size, test_buf_size);
}

TEST_F(S3Tests, MimickError) {
    // tests S3 writing capabilities
    using namespace tuplex;

    EXPECT_GE(S3File::INTERNAL_BUFFER_SIZE(), 0);

    auto internal_buf_size = S3File::INTERNAL_BUFFER_SIZE();

    // write S3 file that's larger than internal size
    size_t test_buf_size = 1.2 * internal_buf_size;
    auto test_buf = new uint8_t[test_buf_size];
    memset(test_buf, 42, test_buf_size);

    auto s3_test_path = s3TestBase + "/" + testName + "/mimick.bin";

    auto vfs = VirtualFileSystem::fromURI(URI("s3://"));

    // write parts...
    auto file = vfs.open_file(s3_test_path, VirtualFileMode::VFS_OVERWRITE);
    ASSERT_TRUE(file);
    int64_t test_num = 20;
    file->write(&test_num, 8);
    file->write(&test_num, 8);
    file->write(test_buf, test_buf_size);
    file->close();

    // check file was written correctly
    uint64_t file_size;
    vfs.file_size(s3_test_path, file_size);
    EXPECT_EQ(file_size, test_buf_size + 2 * 8);
}

TEST_F(S3Tests, FileUploadMultiparts) {
    // tests S3 writing capabilities
    using namespace tuplex;

    EXPECT_GE(S3File::INTERNAL_BUFFER_SIZE(), 0);

    auto internal_buf_size = S3File::INTERNAL_BUFFER_SIZE();

    // write S3 file that's larger than internal size
    size_t test_buf_size = 2 * internal_buf_size;
    auto test_buf = new uint8_t[test_buf_size];
    memset(test_buf, 42, test_buf_size);

    auto s3_test_path = s3TestBase + "/" + testName + "/multiparts.bin";

    auto vfs = VirtualFileSystem::fromURI(URI("s3://"));

    // write parts...
    auto file = vfs.open_file(s3_test_path, VirtualFileMode::VFS_OVERWRITE);
    ASSERT_TRUE(file);

    // test some edge cases when writing

    size_t part_size = 0.75 * internal_buf_size;
    file->write(test_buf, 0);
    file->write(test_buf, part_size);
    file->write(test_buf + part_size, internal_buf_size);
    file->write(test_buf + part_size + internal_buf_size, test_buf_size - (part_size + internal_buf_size));
    file->close();

    // check file was written correctly
    uint64_t file_size;
    vfs.file_size(s3_test_path, file_size);
    EXPECT_EQ(file_size, test_buf_size);
}

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
