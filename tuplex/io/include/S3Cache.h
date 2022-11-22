//
// Created by leonhard on 11/21/22.
//

#ifndef TUPLEX_S3CACHE_H
#define TUPLEX_S3CACHE_H
#ifdef BUILD_WITH_AWS

#include <thread>
#include <mutex>

namespace tuplex {

    /*!
     * this is a helper class to cache S3 requests. It can be prefilled with data as well using
     * separate threads to speed up overall query processing and minimize costs!
     */
    class S3FileCache {
    public:
        /*!
         * resets cache (by emptying everything). Should be used between queries to guarantee consistency.
         */
        void reset();
    private:
        std::mutex _mutex; // everything for this cache needs to be thread-safe.


    };
}


#endif
#endif //TUPLEX_S3CACHE_H
