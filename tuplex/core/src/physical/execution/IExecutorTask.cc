//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/execution/IExecutorTask.h>

namespace tuplex {

    size_t IExecutorTask::getNumOutputRows() const {
        size_t num = 0;
        for(auto p : getOutputPartitions())
            num += p->getNumRows();
        return num;
    }

    void sortTasks(std::vector<IExecutorTask*>& tasks) {
        // sort tasks after their order
        // arg sort
        std::sort(std::begin(tasks), std::end(tasks), [](const IExecutorTask* a, const IExecutorTask* b){
            // check both are the same
            auto ordA = a->getOrder();
            auto ordB = b->getOrder();


            return compareOrders(ordA, ordB);
        });
    }
}