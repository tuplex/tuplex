//
// Created by Leonhard Spiegelberg on 1/4/22.
//

#include "FileSystemUtils.h"
#include "S3File.h"
#include <ContextOptions.h>
#include <Timer.h>

#ifdef BUILD_WITH_AWS

#include <AWSCommon.h>
#include <VirtualFileSystem.h>

#ifndef S3_TEST_BUCKET
// define dummy to compile
#ifdef SKIP_AWS_TESTS
#define S3_TEST_BUCKET "tuplex-test"
#endif

#include <S3Cache.h>

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

TEST_F(S3Tests, RangesToComplete) {
    using namespace tuplex;
    using namespace std;

    // code to check which ranges are required to complete a request
    URI uri("s3://tuplex-public/data/github_daily/2013-10-15.json");
    std::vector<std::tuple<URI, size_t, size_t>> existing_ranges;
    std::vector<std::tuple<URI, size_t, size_t>> res;

    // check 0: no existing range, i.e. full range required
    res = required_requests(uri, 100, 500, existing_ranges);
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(std::get<1>(res.front()), 100);
    EXPECT_EQ(std::get<2>(res.front()), 500);

    // check 1: full range
    existing_ranges.clear();
    existing_ranges.emplace_back(make_tuple(uri, 100, 500));
    existing_ranges.emplace_back(make_tuple(uri, 0, 10)); // irrelevant range
    existing_ranges.emplace_back(make_tuple(uri, 350, 600)); // irrelevant range

    // request range 200, 400. -> contained with 100, 500. so no additional required.
    res = required_requests(uri, 100, 500, existing_ranges);
    EXPECT_EQ(res.size(), 0);

    // check 2a: overlapping range -> missing range at end
    existing_ranges.clear();
    existing_ranges.emplace_back(make_tuple(uri, 0, 350));
    res = required_requests(uri, 100, 500, existing_ranges);
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(std::get<1>(res.front()), 350);
    EXPECT_EQ(std::get<2>(res.front()), 500);

    // check 2b: overlapping range -> missing range at start
    existing_ranges.clear();
    existing_ranges.emplace_back(make_tuple(uri, 100, 600));
    res = required_requests(uri, 0, 500, existing_ranges);
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(std::get<1>(res.front()), 0);
    EXPECT_EQ(std::get<2>(res.front()), 100);

    // check 3: missing ranges (no overlapping segments)
    // 0 - 1000 requested. but only [200, 400], [500, 600] and [950, 1200] there
    existing_ranges.clear();
    existing_ranges.emplace_back(make_tuple(uri, 950, 1200));
    existing_ranges.emplace_back(make_tuple(uri, 200, 400));
    existing_ranges.emplace_back(make_tuple(uri, 500, 600));
    // required requests are: [0, 200], [400, 500], [600, 950]
    res = required_requests(uri, 0, 1000, existing_ranges);
    ASSERT_EQ(res.size(), 3);
    // sort res after start
    std::sort(res.begin(), res.end(), [](const tuple<URI, size_t, size_t>& lhs,
                                         const tuple<URI, size_t, size_t>& rhs) {
        auto lhs_start = std::get<1>(lhs);
        auto rhs_start = std::get<1>(rhs);
        return lhs_start < rhs_start;
    });
    EXPECT_EQ(std::get<1>(res[0]), 0);
    EXPECT_EQ(std::get<2>(res[0]), 200);
    EXPECT_EQ(std::get<1>(res[1]), 400);
    EXPECT_EQ(std::get<2>(res[1]), 500);
    EXPECT_EQ(std::get<1>(res[2]), 600);
    EXPECT_EQ(std::get<2>(res[2]), 950);
}

TEST_F(S3Tests, MiniPreCaching) {
    using namespace tuplex;
    using namespace std;
    {
// init AWS SDK to get access to S3 filesystem
        auto &logger = Logger::instance().logger("aws");
        auto aws_credentials = AWSCredentials::get();
        auto options = ContextOptions::defaults();
        Timer timer;
        bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
        logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
    }

    auto &cache = S3FileCache::instance();
    size_t max_cache_size = 2 * 1024 * 1024; // 2MB
    cache.reset(max_cache_size);

    cache.setFS(*VirtualFileSystem::getS3FileSystemImpl());

    auto test_uri = URI("s3://tuplex-public/data/github_daily_sample/2013-10-15.json.sample");

    // use basically the ranges from above to test (case 3)
    // existing_ranges.emplace_back(make_tuple(uri, 950, 1200));
    // existing_ranges.emplace_back(make_tuple(uri, 200, 400));
    // existing_ranges.emplace_back(make_tuple(uri, 500, 600));
    cache.put(test_uri, 950, 1200);
    cache.put(test_uri, 200, 400);
    cache.put(test_uri, 500, 600);

    // mow get full range 0-1000
    auto test_buf = new uint8_t[1000];

    size_t bytes_read = 0;
    cache.get(test_buf, 1000, test_uri, 0, 1000, &bytes_read);
    EXPECT_EQ(bytes_read, 1000);
    delete [] test_buf;
}

TEST_F(S3Tests, PreCaching) {
    using namespace tuplex;
    using namespace std;
    {
// init AWS SDK to get access to S3 filesystem
        auto &logger = Logger::instance().logger("aws");
        auto aws_credentials = AWSCredentials::get();
        auto options = ContextOptions::defaults();
        Timer timer;
        bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
        logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
    }

    auto &cache = S3FileCache::instance();
    size_t max_cache_size = 2 * 1024 * 1024; // 2MB
    cache.reset(max_cache_size);

    cache.setFS(*VirtualFileSystem::getS3FileSystemImpl());

    // get data from S3 uri (this caches it as well)
    auto test_uri = URI("s3://tuplex-public/data/github_daily/2013-10-15.json");
    // auto test_uri = URI("s3://tuplex-public/data/github_daily_sample/2013-10-15.json.sample");

    // first, get reference buffer (this may take a while)
    Timer timer;
    size_t uri_size;
    auto vfs = VirtualFileSystem::fromURI(test_uri);
    vfs.file_size(test_uri, reinterpret_cast<uint64_t&>(uri_size));
    cout<<"size of "<<test_uri.toPath()<<" is: "<<uri_size<<" bytes."<<endl;
    ASSERT_NE(uri_size, 0);
    uint8_t* ref_buf = new uint8_t[uri_size];

    // read into ref_buf
    auto ref_file = vfs.open_file(test_uri, VirtualFileMode::VFS_READ | VirtualFileMode::VFS_TEXTMODE);
    ASSERT_TRUE(ref_file);
    size_t bytes_read = 0;
    ref_file->read(ref_buf, uri_size, &bytes_read);
    ref_file->close();
    cout<<"Reading ref file of size "<<uri_size<<" took "<<timer.time()<<"s ("<<bytes_read<<" bytes read)"<<endl;
    // calculate S3 rate in MB / s
    double s3ReadSpeed = ((bytes_read / (1024 * 1024.0)) / timer.time());
    cout<<"S3 read speed: "<<s3ReadSpeed<<"MB/s"<<endl;

    EXPECT_EQ(bytes_read, uri_size);

    // use file cache now
    auto s3impl = vfs.getS3FileSystemImpl();
    ASSERT_TRUE(s3impl);
    s3impl->activateReadCache(memStringToSize("1G")); // use 1G cache for now

    // put a bunch of futures into the cache (in 32 MB chunks)
    timer.reset();
    vector<future<size_t>> futures;
    futures.emplace_back(cache.putAsync(test_uri, 0, 256 * 1024)); // first cache line
    futures.emplace_back(cache.putAsync(test_uri, uri_size - 256 * 1024, uri_size));

    // fill with chunk size
    size_t chunk_size = 32 * 1024 * 1024; // 32MB
    size_t total = 0;
    while(total < uri_size) {
        futures.emplace_back(cache.putAsync(test_uri, total, total + chunk_size));
        total += chunk_size;
    }

    // wait for all futures
    for(auto& f : futures)
        f.wait();
    cout<<"parallel/async fill of S3 cache took "<<timer.time()<<"s."<<endl;
    // calculate S3 rate in MB / s
    s3ReadSpeed = ((uri_size + (2 * 256 * 1024.0)) / (1024 * 1024.0)) / timer.time();
    cout<<"S3 read speed: "<<s3ReadSpeed<<"MB/s"<<endl;

    // now read into test buf and run then memcmp
    auto test_buf = new uint8_t[uri_size];
    // test
    cache.get(test_buf, uri_size, test_uri, 20, 200, &bytes_read); // get 180 bytes

    // direct cache test
    timer.reset();
    cache.get(test_buf, uri_size, test_uri, 0, uri_size, &bytes_read);
    cout<<"Reading from cache took: "<<timer.time()<<"s."<<endl;
    EXPECT_EQ(bytes_read, uri_size);
    timer.reset();
    auto ret = memcmp(ref_buf, test_buf, uri_size);
    cout<<"memcmp result is: "<<ret<<" (took "<<timer.time()<<"s to compare)"<<endl;
    EXPECT_EQ(ret, 0);

    // memset 0 and read as file
    memset(test_buf, 0, uri_size);
    timer.reset();
    auto test_file = vfs.open_file(test_uri, VirtualFileMode::VFS_READ | VirtualFileMode::VFS_TEXTMODE);
    ASSERT_TRUE(test_file);
    bytes_read = 0;
    test_file->read(test_buf, uri_size, &bytes_read);
    test_file->close();
    cout<<"Reading test file of size "<<uri_size<<" took "<<timer.time()<<"s ("<<bytes_read<<" bytes read)"<<endl;
    EXPECT_EQ(bytes_read, uri_size);
    timer.reset();
    ret = memcmp(ref_buf, test_buf, uri_size);
    cout<<"memcmp result is: "<<ret<<" (took "<<timer.time()<<"s to compare)"<<endl;
    EXPECT_EQ(ret, 0);

    delete [] test_buf;
    delete [] ref_buf;
}

TEST_F(S3Tests, FileCache) {
    using namespace tuplex;

    {
        // init AWS SDK to get access to S3 filesystem
        auto& logger = Logger::instance().logger("aws");
        auto aws_credentials = AWSCredentials::get();
        auto options = ContextOptions::defaults();
        Timer timer;
        bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
        logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
    }

    auto& cache = S3FileCache::instance();
    size_t max_cache_size = 2 * 1024 * 1024; // 2MB
    cache.reset(max_cache_size);

    cache.setFS(*VirtualFileSystem::getS3FileSystemImpl());

    // get data from S3 uri (this caches it as well)
    std::string test_uri = "s3://tuplex-public/data/github_daily/2013-10-15.json";

    // put sync buffer in (i.e. prefill buffer!)
    // fill 5x 1MB into buffer
    Timer timer;
    size_t chunk_size = 1024 * 1024;
    for(unsigned i = 0; i < 5; ++i)
        cache.put(test_uri, i * chunk_size, (i + 1) * chunk_size); // 1MB cache
    std::cout<<"sync fill of cache took "<<timer.time()<<"s"<<std::endl;

    // now same, async fill
    cache.reset();
    timer.reset();
    std::vector<std::future<size_t>> futures;
    for(unsigned i = 0; i < 5; ++i)
        futures.emplace_back(cache.putAsync(test_uri, i * chunk_size, (i + 1) * chunk_size)); // 1MB cache
    // wait for futures
    for(auto& f : futures)
        f.wait();
    std::cout<<"async fill of full cache took "<<timer.time()<<"s"<<std::endl;

    size_t uri_size = cache.file_size(test_uri);
    std::cout<<"uri size: "<<uri_size<<" bytes"<<std::endl;

    // read a buffer
    size_t buf_capacity = 512 * 1024; // buf smaller than get.
    auto buf = new uint8_t[buf_capacity];
    cache.get(buf, buf_capacity, test_uri, 0, 1024 * 1024);

    delete [] buf;

    // now an actual usage check: Use s3cache to quickly issue a request for sampling and other request for storing full data
    cache.reset(1024 * 1024 * 1024); // 1G cache
    futures.clear();
    timer.reset();
    std::cout<<"starting fetching from S3:"<<std::endl;
    futures.emplace_back(cache.putAsync(test_uri, 0, chunk_size)); // 1MB cache
    futures.emplace_back(cache.putAsync(test_uri, chunk_size, uri_size)); // rest of the file (get as much as possible!!!)
    // wait till the first future (for sampling is done)
    futures.front().wait();
    // now call get on the chunks
    buf = new uint8_t[chunk_size];
    cache.get(buf, chunk_size, test_uri, 0, chunk_size);
    std::cout<<"got first chunk in "<<timer.time()<<std::endl;
    futures.back().wait();
    std::cout<<"all futures waited for in "<<timer.time()<<std::endl;
    delete [] buf;
}


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
