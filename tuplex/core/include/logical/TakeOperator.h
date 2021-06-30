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
        int64_t _limit;
    public:
        LogicalOperator *clone() override;

    public:
        TakeOperator(LogicalOperator *parent, const int64_t numElements);

        std::string name() override {
            if(_limit < 0 || std::numeric_limits<int64_t>::max() == _limit)
                return "collect";
            return "take";
        }
        LogicalOperatorType type() const override { return LogicalOperatorType::TAKE; }

        bool isActionable() override { return true; }

        bool isDataSource() override { return false; }

        bool good() const override;

        int64_t limit() { return _limit; }


        std::vector<Row> getSample(const size_t num) const override;

        Schema getInputSchema() const override { return getOutputSchema(); }

        std::vector<std::string> columns() const override;
    };
}

#endif //TUPLEX_TAKEOPERATOR_H