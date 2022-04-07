//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TAKEOPERATOR_H
#define TUPLEX_TAKEOPERATOR_H


#include "LogicalOperator.h"

namespace tuplex {
    class TakeOperator : public LogicalOperator {
    private:
        size_t _topLimit;
        size_t _bottomLimit;
    public:
        LogicalOperator *clone() override;

    public:
        TakeOperator(LogicalOperator *parent, size_t topLimit, size_t bottomLimit);

        std::string name() override {
            if(_topLimit == std::numeric_limits<size_t>::max() || _bottomLimit == std::numeric_limits<size_t>::max())
                return "collect";
            return "take";
        }
        LogicalOperatorType type() const override { return LogicalOperatorType::TAKE; }

        bool isActionable() override { return true; }

        bool isDataSource() override { return false; }

        bool good() const override;

        size_t topLimit() const { return _topLimit; }

        size_t bottomLimit() const { return _bottomLimit; }

        std::vector<Row> getSample(const size_t num) const override;

        Schema getInputSchema() const override { return getOutputSchema(); }

        std::vector<std::string> columns() const override;
    };
}

#endif //TUPLEX_TAKEOPERATOR_H