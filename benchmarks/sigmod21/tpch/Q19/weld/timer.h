//
// Created by Leonhard Spiegelberg on 11/26/17.
//

#ifndef PYUDF_TIMER_H
#define PYUDF_TIMER_H

#include <chrono>

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

#endif //PYUDF_TIMER_H