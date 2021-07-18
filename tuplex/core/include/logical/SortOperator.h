//
// Created by Colby Anderson on 5/27/21.
//

#ifndef TUPLEX_SORTOPERATOR_H
#define TUPLEX_SORTOPERATOR_H

#include "LogicalOperator.h"

namespace tuplex {
    class SortOperator : public LogicalOperator {
    private:
        std::vector<size_t> _order;
        std::vector<size_t> _orderEnum;
        std::vector<std::string> _columns;
    public:
        SortOperator(LogicalOperator *parent, std::vector<size_t> order, std::vector<size_t> orderEnum);

        std::vector<size_t> orderEnum() { return _orderEnum; }

        std::vector<size_t> order() { return _order; }

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