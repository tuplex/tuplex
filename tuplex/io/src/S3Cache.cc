//
// Created by leonhard on 12/5/22.
//

#include <S3Cache.h>
#include <algorithm>
#include <future>
#include "Logger.h"
#include "Timer.h"
#include "S3File.h"

namespace tuplex {
    void S3FileCache::reset(size_t maxSize) {

        // delete all cache entries (+ abort any existing runs?)
        std::lock_guard<std::mutex> lock(_mutex);

        _maxSize = maxSize;
        _cacheSize = 0;
    }


    uint8_t* S3FileCache::put(const URI &uri, size_t range_start, size_t range_end, option<size_t> uri_size) {

        URI target_uri = uri;
        size_t custom_range_start = 0, custom_range_end = 0;
        decodeRangeURI(uri.toPath(), target_uri, custom_range_start, custom_range_end);

        // manipulate range_start, range_end to be valid

        // lock and check whether content exists
        {
            std::lock_guard<std::mutex> lock(_mutex);
            auto it = std::find_if(_chunks.begin(), _chunks.end(), [target_uri, range_start, range_end](const CacheEntry& chunk) {
                bool in_range = chunk.range_start <= range_start && range_end <= chunk.range_end;
                return chunk.uri == target_uri && in_range && chunk.buf;
            });

            // chunk found? then return...
            if(it != _chunks.end())
                return it->buf + (range_start - it->range_start);

        }

        // not found, hence request via S3 and put into array!
        // first though, check size of array
        auto requested_size = range_end - range_start;
        if(requested_size > _maxSize) {
            // ignore... to large to store.
            return nullptr;
        } else if(requested_size + cacheSize() > _maxSize) {
            std::lock_guard<std::mutex> lock(_mutex);
            pruneBy(requested_size);
        } else {
            // ok, can store.
            auto chunk = s3Read(uri, range_start, range_end);
            {
                std::lock_guard<std::mutex> lock(_mutex);
                auto ptr = chunk.buf;
                _chunks.emplace_back(std::move(chunk));
                return ptr;
            }
        }

        return nullptr;
    }

    std::future<uint8_t *>
    S3FileCache::putAsync(const URI &uri, size_t range_start, size_t range_end, option<size_t> uri_size) {
        auto f = std::future<uint8_t*>();
        return f;
    }

    // from https://codereview.stackexchange.com/questions/206686/removing-by-indices-several-elements-from-a-vector
    template <typename Iter, typename Index_iter>
    Iter removeIndicesStendhal(Iter first, Iter last, Index_iter ifirst, Index_iter ilast) {
        if (ifirst == ilast || first == last) return last;

        auto out = std::next(first, *ifirst);
        auto in  = std::next(out);
        while (++ifirst != ilast) {
            if (*std::prev(ifirst) + 1 == *ifirst) {
                ++in; continue;
            }
            out = std::move(in, std::next(first, *ifirst), out);
            in  = std::next(first, *ifirst + 1);
        }
        return std::move(in, last, out);
    }

    bool S3FileCache::pruneBy(size_t size) {
        if(size > _maxSize)
            return false;

        // if size exceeds cache, clear everything
        if(size >= cacheSize()) {
            _chunks.clear();
            return true;
        }

        // LRU, kick out as many chunks till size if reached.
        std::vector<std::tuple<unsigned, uint64_t>> indices_with_timestamps;
        for(unsigned i = 0; i < _chunks.size(); ++i) {
            indices_with_timestamps.push_back(std::make_tuple(i, _chunks[i].timestamp));
        }

        // sort after timestamp
        std::sort(indices_with_timestamps.begin(), indices_with_timestamps.end(), [](const std::tuple<unsigned, uint64_t>& lhs,
                const std::tuple<unsigned, uint64_t>& rhs) {
            return std::get<1>(lhs) < std::get<1>(rhs);
        });

        // go through and erase elements
        std::vector<size_t> indices_to_remove;
        size_t size_so_far = 0;
        for(auto t : indices_with_timestamps) {
            auto idx = std::get<0>(t);
            indices_to_remove.push_back(idx);
            size_so_far += _chunks[idx].size();
            if(size_so_far >= size)
                break;
        }

        removeIndicesStendhal(_chunks.begin(), _chunks.end(),
                              indices_to_remove.begin(), indices_to_remove.end());

        return true;
    }

    bool S3FileCache::get(uint8_t *buf, size_t buf_capacity, const URI &uri, size_t range_start, size_t range_end,
                          size_t* bytes_written) {
        auto& logger = Logger::instance().logger("s3cache");
        if(!buf) {
            logger.debug("invalid buffer");
            return false;
        }

        // correct for ranges etc.
        URI target_uri = uri;
        size_t custom_range_start = 0, custom_range_end = 0;
        decodeRangeURI(uri.toPath(), target_uri, custom_range_start, custom_range_end);
        assert(custom_range_start == 0 && custom_range_end == 0); // there should be no other range...

        auto requested_size = range_end - range_start;
        if(requested_size > buf_capacity)
            range_end = range_start + buf_capacity;

        // is buf fully OR partially contained within cache?
        // -> then short cut. Else, fully read buf.
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = std::find_if(_chunks.begin(), _chunks.end(), [target_uri, range_start, range_end](const CacheEntry& chunk) {
            bool in_range = chunk.range_start <= range_start && range_end <= chunk.range_end;
            return chunk.uri == target_uri && in_range && chunk.buf;
        });

        if(it != _chunks.end()) {
            // contained, memcpy buffer content
            auto offset = range_start - it->range_start;
            assert(offset <= it->size());
            assert(range_end >= range_start);
            assert(range_end - range_start <= buf_capacity);
            size_t max_size_to_copy = std::min(it->size() - offset, range_end - range_start);
            max_size_to_copy = std::min(max_size_to_copy, buf_capacity);
            memcpy(buf, it->buf + offset, max_size_to_copy);
            if(bytes_written)
                *bytes_written = max_size_to_copy;
            return true;
        }

        // no chunk found, need to fetch and store if there's space - else kick out as much as possible.
        // could optimize with partial fetch etc.
        // but for now, simply fetch the concrete block demanded.

        auto entry = s3Read(uri, range_start, range_end);

        if(!entry.buf)
            return false;

        // copy directly & output
        size_t max_size_to_copy = std::min(entry.size(), range_end - range_start);
        max_size_to_copy = std::min(max_size_to_copy, buf_capacity);
        memcpy(buf, entry.buf, max_size_to_copy);
        if(bytes_written)
            *bytes_written = max_size_to_copy;

        return true;
    }

    // simplify design maybe for Lambda. I.e., needs to be explicitly put called!
    // get doesn't automatically place an entry in the cache.
    // -> this would make it easier...


    S3FileCache::CacheEntry S3FileCache::s3Read(const URI &uri, size_t range_start, size_t range_end) {
        CacheEntry entry;

        auto& logger = Logger::instance().logger("s3fs");

        if(!_s3fs)
            throw std::runtime_error("Trying to use S3Cache without an initialized S3 Filesystem");

        // issue a S3 read (part) request -> will contain all data etc.
// simply issue here one direct request
        size_t retrievedBytes = 0;
        size_t nbytes = range_end - range_start;
        // range header
        std::string range = "bytes=" + std::to_string(range_start) + "-" + std::to_string(range_start + nbytes - 1);
        // make AWS S3 part request to uri
        // check how to retrieve object in poarts
        Aws::S3::Model::GetObjectRequest req;
        req.SetBucket(uri.s3Bucket().c_str());
        req.SetKey(uri.s3Key().c_str());
        // retrieve byte range according to http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        req.SetRange(range.c_str());
        req.SetRequestPayer(_requestPayer);

        // Get the object ==> Note: this s3 client is damn slow, need to make it faster in the future...
        Timer timer;
        auto get_object_outcome = _s3fs->client().GetObject(req);
        _s3fs->_getRequests++;
        logger.info("Requested from S3 in " + std::to_string(timer.time()) + "s");

        if (get_object_outcome.IsSuccess()) {
            auto result = get_object_outcome.GetResultWithOwnership();

            // extract extracted byte range + size
            // syntax is: start-inclend/fsize
            auto cr = result.GetContentRange();
            auto idxSlash = cr.find_first_of('/');
            auto idxMinus = cr.find_first_of('-');
            // these are kind of weird, they are already requested range I presume
            size_t fileSize = std::strtoull(cr.substr(idxSlash + 1).c_str(), nullptr, 10);
            retrievedBytes = result.GetContentLength();

            // Get an Aws::IOStream reference to the retrieved file
            auto &retrieved_file = result.GetBody();

            if(0 == retrievedBytes) {
                logger.debug("empty range");
                return entry;
            }

            // alloc buffer
            entry.buf = new uint8_t[retrievedBytes];
            entry.range_start = range_start;
            entry.range_end = range_start + retrievedBytes;
            entry.uri = uri;
            entry.uri_size = fileSize;

            // copy contents
            retrieved_file.read((char*)entry.buf, retrievedBytes);

            // note: for ascii files there might be an issue regarding the file ending!!!
            _s3fs->_bytesReceived += retrievedBytes;
        } else {
            auto s3_details = format_s3_outcome_error_message(get_object_outcome, uri.toPath());
            auto err_msg = s3_details;
            logger.error(err_msg);
            throw std::runtime_error(err_msg);
        }

        return entry;
    }

    // cf. https://raw.githubusercontent.com/mohaps/lrucache11/master/LRUCache11.hpp
}