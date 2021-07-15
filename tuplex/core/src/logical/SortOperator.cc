//
// Created by Colby Anderson on 5/27/21.
//

#include <logical/SortOperator.h>

namespace tuplex {

    SortOperator::SortOperator(LogicalOperator *parent, std::vector<size_t> order, std::vector<size_t> orderEnum)  : LogicalOperator::LogicalOperator(parent), _order(order), _orderEnum(orderEnum) {
        // take schema from parent node
        setSchema(this->parent()->getOutputSchema());
    }

    bool SortOperator::good() const {
        return true;
    }

    std::vector<Row> SortOperator::getSample(size_t num) const {
        // TODO: COLBY ?
        std::vector<Row> v;
        return v;
    }

    bool SortOperator::isActionable() {
        return false;
    }

    bool SortOperator::isDataSource() {
        return false;
    }

    LogicalOperator *SortOperator::clone() {
        // create clone of this operator
        auto copy = new SortOperator(parent()->clone(), _order, _orderEnum);

        copy->setDataSet(getDataSet()); // weak ptr to old dataset...
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
    }
}