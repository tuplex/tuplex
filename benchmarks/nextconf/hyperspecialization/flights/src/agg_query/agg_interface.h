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
