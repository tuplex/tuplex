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

        std::string name() const override { return "filter"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::FILTER; }
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override;

        void setDataSet(DataSet* dsptr) override;

        std::vector<Row> getSample(const size_t num) const override;

        std::shared_ptr<LogicalOperator> clone(bool cloneParents) override;

        Schema getInputSchema() const override {
            if(parent())
                return parent()->getOutputSchema();

            return Schema::UNKNOWN;
        }

        /*!
         * projection pushdown, update UDF and schema...
         * @param rewriteMap
         */
        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap) override;

        bool retype(const RetypeConfiguration& conf) override;

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<UDFOperator>(this), _good);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<UDFOperator>(this), _good);
        }
#endif

        inline nlohmann::json to_json() const {
            // make it a super simple serialiation!
            // basically mimick clone
            nlohmann::json obj;
            obj["name"] = "filter";
            obj["columnNames"] = UDFOperator::columns();
            obj["outputColumns"] =columns();
            obj["schema"] = LogicalOperator::schema().getRowType().desc();
            obj["id"] = getID();

            // no closure env etc.
            nlohmann::json udf;
            udf["code"] = _udf.getCode();

            // this doesn't work, needs base64 encoding. skip for now HACK
            //udf["pickledCode"] = _udf.getPickledCode();

            obj["udf"] = udf;

            return obj;
        }

    private:
        bool _good;
    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::FilterOperator);
#endif

#endif //TUPLEX_FILTEROPERATOR_H