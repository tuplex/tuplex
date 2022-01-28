//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/TakeLastOperator.h>
#include <cassert>

namespace tuplex {
    TakeLastOperator::TakeLastOperator(LogicalOperator *parent, const int64_t numElements) : LogicalOperator::LogicalOperator(parent), _limit(numElements) {
        // take schema from parent node
        setSchema(this->parent()->getOutputSchema());
    }

    bool TakeLastOperator::good() const {
            return _limit >= -1;
    }

    std::vector<Row> TakeLastOperator::getSample(const size_t num) const {
        // take sample from parent
        return parent()->getSample(num);
    }

    std::vector<std::string> TakeLastOperator::columns() const {
        assert(parent());
        return parent()->columns();
    }

    LogicalOperator *TakeLastOperator::clone() {
        // create clone of this operator
        auto copy = new TakeLastOperator(parent()->clone(), _limit);

        copy->setDataSet(getDataSet()); // weak ptr to old dataset...
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
    }
}