//
// Created by Leonhard Spiegelberg on 2/22/22.
//

#ifndef HYPERFLIGHTS_AGG_H
#define HYPERFLIGHTS_AGG_H

#include <cstdint>
#include <string>
#include <memory>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cmath>
#include <chrono>
#include <iostream>
#include <tuple>

// hash support for tuple
namespace std {
    // Code from boost
    // Reciprocal of the golden ratio helps spread entropy
    //     and handles duplicates.
    // See Mike Seymour in magic-numbers-in-boosthash-combine:
    //     https://stackoverflow.com/questions/4948780

    template <class T>
    inline void hash_combine(std::size_t& seed, T const& v)
    {
        seed ^= hash<T>()(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
    }

    // Recursive template code derived from Matthieu M.
    template <class Tuple, size_t Index = std::tuple_size<Tuple>::value - 1>
    struct HashValueImpl
    {
        static void apply(size_t& seed, Tuple const& tuple)
        {
            HashValueImpl<Tuple, Index-1>::apply(seed, tuple);
            hash_combine(seed, get<Index>(tuple));
        }
    };

    template <class Tuple>
    struct HashValueImpl<Tuple,0>
    {
        static void apply(size_t& seed, Tuple const& tuple)
        {
            hash_combine(seed, get<0>(tuple));
        }
    };

    template <typename ... TT>
    struct hash<std::tuple<TT...>>
    {
        size_t
        operator()(std::tuple<TT...> const& tt) const
        {
            size_t seed = 0;
            HashValueImpl<std::tuple<TT...> >::apply(seed, tt);
            return seed;
        }

    };
}

// define interfaces to export
extern "C" int64_t init_aggregate(void* userData);
extern "C" int64_t process_cells(void *userData, char **cells, int64_t *cell_sizes);
extern "C" int64_t fetch_aggregate(void* userData, uint8_t** buf, size_t* buf_size);


class Timer {
private:
    std::chrono::high_resolution_clock::time_point _start;
public:

    Timer() {
        reset();
    }

    // nanoseconds since 1970
    static int64_t currentTimestamp();

    /*!
     * returns time since start of the timer in seconds
     * @return time in seconds
     */
    double time() {
        auto stop = std::chrono::high_resolution_clock::now();

        double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - _start).count() / 1000000000.0;
        return duration;
    }

    void reset() {
        _start = std::chrono::high_resolution_clock::now();
    }

};

#endif //HYPERFLIGHTS_AGG_H
