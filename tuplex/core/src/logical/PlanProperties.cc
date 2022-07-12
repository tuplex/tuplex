//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2022, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 4/5/2022                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#include <logical/PlanProperties.h>

namespace tuplex {
    PlanProperties PlanProperties::detect(const std::shared_ptr<LogicalOperator> &root) {
        PlanProperties pp;

        if(!root)
            return pp;

        // go through nodes
        std::queue<std::shared_ptr<LogicalOperator>> q; // BFS
        q.push(root);
        while(!q.empty()) {
            auto node = q.front(); q.pop();
            pp._types.insert(node->type());

            // add all parents to queue
            for(const auto &p : node->parents())
                q.push(p);
        }

        return pp;
    }
}