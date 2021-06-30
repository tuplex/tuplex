//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "../include/Timer.h"

// use C++11 time libs
#include <chrono>


namespace tuplex {
    int64_t Timer::currentTimestamp() {
        std::chrono::high_resolution_clock clock;
        return std::chrono::duration_cast<std::chrono::nanoseconds>(clock.now().time_since_epoch()).count();
    }
}