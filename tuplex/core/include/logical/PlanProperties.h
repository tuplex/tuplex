//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2022, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 4/5/2022                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PLANPROPERTIES_H
#define TUPLEX_PLANPROPERTIES_H

#include "Operators.h"

namespace tuplex {
    struct PlanProperties {
        inline bool hasJoin() const {
            return _types.find(LogicalOperatorType::JOIN) != _types.end();
        }

        static PlanProperties detect(const std::shared_ptr<LogicalOperator>& root);
    private:
        PlanProperties() {}
        std::set<LogicalOperatorType> _types;
    };
}

#endif //TUPLEX_PLANPROPERTIES_H
