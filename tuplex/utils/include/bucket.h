//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "hashmap.h"
#include "int_hashmap.h"

// helper functions when dealing with buckets

namespace tuplex {
    // note: could specialize bucket structures A LOT for better performance...
    /*!
     * adds a row to a bucket, reallocs if necessary.
     * @param bucket pointer to bucket
     * @param buf serialized row
     * @param buf_size serialized row size
     * @return new bucket pointer
     */
    inline uint8_t* extend_bucket(uint8_t* bucket, uint8_t* buf, size_t buf_size) {

        // structure of bucket is as follows (varlen content)
        // 32 bit => num elements | 32 bit => size of bucket | int32_t row length | row content

        // empty bucket?
        // ==> alloc and init with defaults!

        // @TODO: maybe alloc one page?
        if(!bucket) {
            auto bucket_size = sizeof(int64_t) + sizeof(int32_t) + buf_size;
            bucket = (uint8_t*)realloc(nullptr, bucket_size);
            if(!bucket)
                return nullptr;

            uint64_t info = (0x1ul << 32ul) | bucket_size;

            *(uint64_t*)bucket = info;
            *(uint32_t*)(bucket + sizeof(int64_t)) = buf_size;
            if(buf)
                memcpy(bucket + sizeof(int64_t) + sizeof(int32_t), buf, buf_size);
        } else {

            uint64_t info = *(uint64_t*)bucket;
            auto bucket_size = info & 0xFFFFFFFF;
            auto num_elements = (info >> 32ul);

            // realloc and copy contents to end of bucket...
            bucket = (uint8_t*)realloc(bucket, bucket_size + sizeof(int32_t) + buf_size);

            auto new_size = bucket_size + sizeof(int32_t) + buf_size;
            info = ((num_elements + 1) << 32ul) | new_size;
            *(uint64_t*)bucket = info;

            *(uint32_t*)(bucket + bucket_size) = buf_size;
            if(buf)
                memcpy(bucket + bucket_size + sizeof(int32_t), buf, buf_size);
        }

        return bucket;
    }

    // what does this function do? where is it called??
    inline bool get_bucket_element_ptr(uint8_t *bucket, int idx, uint8_t** buf, int64_t *buf_size) {
        // needs to be a bucket
        if(!bucket) return false;

        // element needs to exist
        uint64_t info = *(uint64_t*)bucket;
        auto num_elements = (info >> 32ul);
        if(idx >= num_elements) return false;

        // get the element
        bucket += 8; // move forward to first element
        while(idx-- > 0) {
            auto cur_size = *(uint32_t*)bucket;
            bucket += 4;
            if(idx == 0) {
                *buf_size = cur_size;
                *buf = bucket;
                return true;
            } else bucket += cur_size;
        }

        // that correct??
        return true;
    }
}