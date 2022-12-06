//
// Created by leonhard on 12/5/22.
//

#include <S3Cache.h>
#include <algorithm>
#include <future>
#include "Logger.h"

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
            throw std::runtime_error("not yet implemented");
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
                          option<size_t> uri_size) {
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
            memcpy(buf, it->buf + offset, std::min(it->size() - offset, range_end - range_start));
        }

        // no chunk found, need to fetch and store if there's space - else kick out as much as possible.
        // could optimize with partial fetch etc.
        // but for now, simply fetch the concrete block demanded.

        auto entry = s3Read(uri, range_start, range_end);

        return true;
    }

    // simplify design maybe for Lambda. I.e., needs to be explicitly put called!
    // get doesn't automatically place an entry in the cache.
    // -> this would make it easier...


    S3FileCache::CacheEntry S3FileCache::s3Read(const URI &uri, size_t range_start, size_t range_end) {



        CacheEntry entry;
        return entry;
    }

    // cf. https://raw.githubusercontent.com/mohaps/lrucache11/master/LRUCache11.hpp
}