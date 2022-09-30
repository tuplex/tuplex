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

        FilterOperator(const std::shared_ptr<LogicalOperator>& parent,
                       const UDF& udf,
                       const std::vector<std::string>& columnNames,
                       const std::unordered_map<size_t, size_t>& rewriteMap={});

        std::string name() override { return "filter"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::FILTER; }
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override;

        void setDataSet(DataSet* dsptr) override;

        std::vector<Row> getSample(const size_t num) const override;

        std::shared_ptr<LogicalOperator> clone(bool cloneParents) override;

        /*!
         * projection pushdown, update UDF and schema...
         * @param rewriteMap
         */
        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap) override;

        bool retype(const python::Type& input_row_type, bool is_projected_row_type) override;

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<UDFOperator>(this), _good);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<UDFOperator>(this), _good);
        }
#endif

    private:
        bool _good;
    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::FilterOperator);
#endif

#endif //TUPLEX_FILTEROPERATOR_H