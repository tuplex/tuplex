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
        std::shared_ptr<LogicalOperator> clone(bool cloneParents) const override;

    public:

        // required by cereal
        TakeOperator() = default;

        TakeOperator(const std::shared_ptr<LogicalOperator>& parent, const int64_t numElements);

        std::string name() const override {
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

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<LogicalOperator>(this), _limit);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<LogicalOperator>(this), _limit);
        }
#endif
    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::TakeOperator);
#endif

#endif //TUPLEX_TAKEOPERATOR_H