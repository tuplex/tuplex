//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IGNOREOPERATOR_H
#define TUPLEX_IGNOREOPERATOR_H

#include "LogicalOperator.h"
#include "LogicalOperatorType.h"
#include "ExceptionOperator.h"

namespace tuplex {
    class IgnoreOperator : public LogicalOperator, public ExceptionOperator<IgnoreOperator> {
    public:
        IgnoreOperator() = default;
        virtual ~IgnoreOperator() override = default;

        IgnoreOperator(const std::shared_ptr<LogicalOperator>& parent, const ExceptionCode& ec) : LogicalOperator(parent) {
            setSchema(this->parent()->getOutputSchema());
            setCode(ec);
        }

        inline int64_t getIgnoreID() {
            // get first NON-ignore operator id
            auto parent = this->parent();
            while(parent->type() == LogicalOperatorType::IGNORE)
                parent = parent->parent();
            return parent->getID();
        }

        std::shared_ptr<LogicalOperator> clone(bool cloneParents) override {
            auto copy =  new IgnoreOperator(cloneParents ? parent()->clone() : nullptr, ecCode());
            copy->copyMembers(this);
            return std::shared_ptr<LogicalOperator>(copy);
        }

        std::string name() const override { return "ignore"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::IGNORE; }
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override { return true; }

        Schema getInputSchema() const override { return getOutputSchema(); }
        void updateSchema() { setSchema(parent()->getOutputSchema()); }
        bool retype(const RetypeConfiguration& conf) override {
            updateSchema();
            return true;
        }
        virtual std::vector<Row> getSample(const size_t num) const override { return parent()->getSample(num); }
        std::vector<std::string> columns() const override { return parent()->columns(); }

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<LogicalOperator>(this), ::cereal::base_class<ExceptionOperator<IgnoreOperator>>(this));
        }

        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<LogicalOperator>(this), ::cereal::base_class<ExceptionOperator<IgnoreOperator>>(this));
        }
#endif

    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::IgnoreOperator);
#endif

#endif //TUPLEX_IGNOREOPERATOR_Hs