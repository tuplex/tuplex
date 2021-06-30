//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TASKTYPES_H
#define TUPLEX_TASKTYPES_H

namespace tuplex {

    /*!
     * identifiers for task types...
     */
    enum class TaskType {
        UNKNOWN,
        UDFTRAFOTASK=10,
        RESOLVE=11,
        HASHPROBE=12,
        SIMPLEFILEWRITE=13
    };
}

#endif //TUPLEX_TASKTYPES_H