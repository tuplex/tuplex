//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/TakeOperator.h>
#include <cassert>

namespace tuplex {
    TakeOperator::TakeOperator(const std::shared_ptr<LogicalOperator>& parent, const int64_t numElements) : LogicalOperator::LogicalOperator(parent), _limit(numElements) {
        // take schema from parent node
        setSchema(this->parent()->getOutputSchema());
    }

    bool TakeOperator::good() const {
            return _limit >= -1;
    }

    std::vector<Row> TakeOperator::getSample(const size_t num) const {
        // take sample from parent
        return parent()->getSample(num);
    }

    std::vector<std::string> TakeOperator::columns() const {
        assert(parent());
        return parent()->columns();
    }

    std::shared_ptr<LogicalOperator> TakeOperator::clone() {
        // create clone of this operator
        auto copy = new TakeOperator(parent()->clone(), _limit);

        copy->setDataSet(getDataSet()); // weak ptr to old dataset...
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }
}