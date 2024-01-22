//
// Created by leonhard on 11/21/22.
//

#ifndef TUPLEX_S3CACHE_H
#define TUPLEX_S3CACHE_H

#ifdef BUILD_WITH_AWS

#include <thread>
#include <mutex>
#include <future>
#include <URI.h>
#include <optional.h>
#include "S3FileSystemImpl.h"

namespace tuplex {

    /*!
     * this is a helper class to cache S3 requests. It can be prefilled with data as well using
     * separate threads to speed up overall query processing and minimize costs!
     */
    class S3FileCache {
    public:
        /*!
         * resets cache (by emptying everything). Should be used between queries to guarantee consistency.
         * @param maxSize maximum size of cache in bytes.
         */
        void reset(size_t maxSize=128 * 1024 * 1024);
        inline void setFS(S3FileSystemImpl& fs, bool requesterPays=true) {
            _s3fs = &fs;
            if(requesterPays)
                _requestPayer = Aws::S3::Model::RequestPayer::requester;
            else
                _requestPayer = Aws::S3::Model::RequestPayer::NOT_SET;
        }
        static S3FileCache& instance() {
            static S3FileCache the_one_and_only;
            return the_one_and_only;
        }

        // explicit ranges
        uint8_t* get(const URI& uri, size_t range_start, size_t range_end, option<size_t> uri_size = option<size_t>::none);
        uint8_t* put(const URI& uri, size_t range_start, size_t range_end, size_t* bytes_written =nullptr, option<size_t> uri_size = option<size_t>::none);
        std::future<size_t> putAsync(const URI& uri, size_t range_start, size_t range_end);

        // write to external buffer.
        bool get(uint8_t* buf, size_t buf_capacity,
                 const URI& uri, size_t range_start,
                 size_t range_end,
                 size_t* bytes_written=nullptr);

        // helper for full file.
        uint8_t* get(const URI& uri) { return get(uri, 0, 0); }
        uint8_t* put(const URI& uri) { return put(uri, 0, 0); }

        size_t file_size(const URI& uri);

        void free(uint8_t* buf);

        inline size_t cacheSize() const { return _cacheSize; }
    private:
        std::mutex _mutex; // everything for this cache needs to be thread-safe.

        S3FileCache() : _maxSize(128 * 1024 * 1024), _s3fs(nullptr) {
            // 128MB default cache size...
        }

        ~S3FileCache() {
            reset(0); // empty everything...
        }

        size_t _maxSize; // maximum aggregate size in bytes of cache.

        struct CacheEntry {
            size_t range_start;
            size_t range_end;

            uint8_t* buf;
            URI uri;
            size_t uri_size;
            uint64_t timestamp;
            inline size_t size() const { return range_end - range_start; }

            CacheEntry() : buf(nullptr), range_start(0), range_end(0), timestamp(0), uri_size(0) {}
            CacheEntry(const CacheEntry& other) = delete;
            CacheEntry(CacheEntry&& other) {
                buf = other.buf; other.buf = nullptr;
                range_start = other.range_start;
                range_end = other.range_end;

                uri = other.uri;
                uri_size = other.uri_size;
                timestamp = other.timestamp;
            }

            CacheEntry& operator = (CacheEntry&& other) = default;

            ~CacheEntry() {
                if(buf)
                    delete [] buf;
                buf = nullptr;
            }
        };

        std::vector<CacheEntry> _chunks;
        std::atomic<size_t> _cacheSize;

        bool pruneBy(size_t size);

        S3FileSystemImpl* _s3fs; // weak ptr.
        Aws::S3::Model::RequestPayer _requestPayer;
        CacheEntry s3Read(const URI& uri, size_t range_start, size_t range_end);

        /*!
         * computes which requests would need to be issued to make sure the range requested is covered.
         */
        std::vector<std::tuple<URI, size_t, size_t>> requiredRequests(const URI& uri, size_t range_start, size_t range_end);

    };

    extern std::vector<std::tuple<URI, size_t, size_t>> required_requests(const URI& uri, size_t range_start, size_t range_end, const std::vector<std::tuple<URI, size_t, size_t>>& existing_ranges);
}


#endif
#endif //TUPLEX_S3CACHE_H
