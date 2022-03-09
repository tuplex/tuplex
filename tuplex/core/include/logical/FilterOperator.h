//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FILTEROPERATOR_H
#define TUPLEX_FILTEROPERATOR_H

#include "LogicalOperator.h"
#include "UDFOperator.h"

namespace tuplex {
    class FilterOperator : public UDFOperator {
    public:

        // required by cereal
        FilterOperator() = default;

        FilterOperator(const std::shared_ptr<LogicalOperator>& parent, const UDF& udf, const std::vector<std::string>& columnNames);

        std::string name() override { return "filter"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::FILTER; }
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override;

        void setDataSet(DataSet* dsptr) override;

        std::vector<Row> getSample(const size_t num) const override;

        std::shared_ptr<LogicalOperator> clone() override;

        /*!
         * projection pushdown, update UDF and schema...
         * @param rewriteMap
         */
        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap) override;

        bool retype(const std::vector<python::Type>& rowTypes) override;

        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<UDFOperator>(this), _good);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<UDFOperator>(this), _good);
        }

    private:
        bool _good;
    };
}

CEREAL_REGISTER_TYPE(tuplex::FilterOperator);
#endif //TUPLEX_FILTEROPERATOR_H