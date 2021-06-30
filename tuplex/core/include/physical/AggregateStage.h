//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_AGGREGATESTAGE_H
#define TUPLEX_AGGREGATESTAGE_H

#include "PhysicalStage.h"
#include <logical/AggregateOperator.h>

namespace tuplex {

    class AggregateStage : public PhysicalStage {
    public:
        AggregateStage() = delete;

        AggregateStage(PhysicalPlan *plan,
                       IBackend *backend,
                       PhysicalStage *parent,
                       const AggregateType &at,
                       int64_t stage_number,
                       int64_t outputDataSetID);

        /*!
        * fetch data into resultset
        * @return resultset of this stage
        */
        std::shared_ptr<ResultSet> resultSet() const override { return _rs; }

        int64_t outputDataSetID() const { return _outputDataSetID; }

        void setResultSet(const std::shared_ptr<ResultSet> &rs) { _rs = rs; }

        std::string generateCode();

    private:
        int64_t _outputDataSetID;
        AggregateType _aggType;
        std::shared_ptr<ResultSet> _rs;
    };

}
#endif //TUPLEX_AGGREGATESTAGE_H