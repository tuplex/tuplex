//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TIMER_H
#define TUPLEX_TIMER_H

#include "Base.h"
#include <chrono>

namespace tuplex {
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
}
#endif //TUPLEX_TIMER_H