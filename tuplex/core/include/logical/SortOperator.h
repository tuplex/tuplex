//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_SORTOPERATOR_H
#define TUPLEX_SORTOPERATOR_H

#include "LogicalOperator.h"
#include <SortBy.h>

namespace tuplex {

    class SortOperator : public LogicalOperator {
    private:
        /// the indices of the columns in order of how they should be sorted by
        std::vector<size_t> _colIndicesInOrderToSortBy;
        /// the enum corresponding to each
        std::vector<SortBy> _orderEnum;
        std::vector<std::string> _columns;
    public:
        SortOperator(LogicalOperator *parent, std::vector<size_t> colIndicesInOrderToSortBy, std::vector<SortBy> orderEnum);

        std::vector<SortBy> orderEnum() { return _orderEnum; }

        std::vector<size_t> colIndicesInOrderToSortBy() { return _colIndicesInOrderToSortBy; }

        virtual std::string name() override { return "sort"; }

        bool good() const override;

        std::vector<Row> getSample(size_t num) const override;

        bool isActionable() override;

        bool isDataSource() override;

        LogicalOperator *clone() override;

        Schema getInputSchema() const override {return getOutputSchema();}

        std::vector<std::string> columns() const override { return _columns; }

        LogicalOperatorType type() const override {return LogicalOperatorType::SORT;}


    };
}

#endif //TUPLEX_SORTOPERATOR_H