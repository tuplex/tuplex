//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PHYSICALTASK_H
#define TUPLEX_PHYSICALTASK_H

#include "PhysicalStage.h"

namespace tuplex {

    /*
     * can be either a narrow or wide task. Wide Tasks create shuffle sets and send them across the cluster.
     * Narrow tasks perform per node operations.
     */
    class PhysicalTask {
    private:
        PhysicalStage *_stage;
    };
}

#endif //TUPLEX_PHYSICALTASK_H