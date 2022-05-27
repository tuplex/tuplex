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
    TakeOperator::TakeOperator(LogicalOperator *parent, size_t topLimit, size_t bottomLimit) : LogicalOperator::LogicalOperator(parent), _topLimit(topLimit), _bottomLimit(bottomLimit) {
        // take schema from parent node
        setSchema(this->parent()->getOutputSchema());
    }

    bool TakeOperator::good() const {
            return _topLimit >= 0 && _bottomLimit >= 0;
    }

    std::vector<Row> TakeOperator::getSample(const size_t num) const {
        // take sample from parent
        return parent()->getSample(num);
    }

    std::vector<std::string> TakeOperator::columns() const {
        assert(parent());
        return parent()->columns();
    }

    LogicalOperator *TakeOperator::clone() {
        // create clone of this operator
        auto copy = new TakeOperator(parent()->clone(), _topLimit, _bottomLimit);

        copy->setDataSet(getDataSet()); // weak ptr to old dataset...
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
    }
}