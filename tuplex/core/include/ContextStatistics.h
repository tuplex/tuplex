//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CONTEXTSTATISTICS_H
#define TUPLEX_CONTEXTSTATISTICS_H

#include <Utils.h>
#include <StringUtils.h>

//@TODO: print here compile time etc. statistics out...
namespace tuplex {

    enum class ContextMetric {
        COMPILE_TIME=100,
    };
    /*!
     * helper class to collect/update statistics of the last job run.
     * is thread-safe
     */
    class CobtextStatistics {
    public:
        void reset();

    private:

    };
}

#endif //TUPLEX_CONTEXTSTATISTICS_H