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

        /*!
         * construct filter op from UDF, no parent needed.
         * @param udf
         * @param row_type
         * @param input_column_names
         * @return filter operator or nullptr on failure.
         */
        static std::shared_ptr<FilterOperator> from_udf(const UDF& udf, const python::Type& row_type, const std::vector<std::string>& input_column_names);

        bool good() const override;

        void setDataSet(DataSet* dsptr) override;

        /*!
         * returns sample from parent "as-is"
         * @param num maximum number of samples to process
         * @return sample
         */
        std::vector<Row> getSample(const size_t num) const override;

        /*!
         * get sample but apply filter (i.e. remove rows that do not pass filter)
         * @param num  maximum number of samples to process (output)
         * @param applyFilter whether to apply filter or simply return sample from parent "as-is"
         * @return sample
         */
        std::vector<Row> getSample(const size_t num, bool applyFilter) const;

        std::shared_ptr<LogicalOperator> clone(bool cloneParents) const override;

        Schema getInputSchema() const override {
            if(parent())
                return parent()->getOutputSchema();

            return Schema::UNKNOWN;
        }

        std::vector<std::string> inputColumns() const override { return parent() ? parent()->columns() : std::vector<std::string>(); }

        // for filter, output columns do not change.
        std::vector<std::string> columns() const override { return inputColumns(); }

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
            obj["outputColumns"] = columns();
            obj["schema"] = getOutputSchema().getRowType().desc();
            obj["id"] = getID();

            // no closure env etc.
            nlohmann::json udf;
            udf["code"] = _udf.getCode();

            // this doesn't work, needs base64 encoding. skip for now HACK
            //udf["pickledCode"] = _udf.getPickledCode();

            obj["udf"] = udf;

            return obj;
        }

        inline static std::shared_ptr<FilterOperator> from_json(const std::shared_ptr<LogicalOperator>& parent, nlohmann::json json) {
            auto columnNames = json["columnNames"].get<std::vector<std::string>>();
            auto id = json["id"].get<int>();
            auto code = json["udf"]["code"].get<std::string>();
            // @TODO: avoid typing call?
            // i.e. this will draw a sample too?
            // or ok, because sample anyways need to get drawn??
            UDF udf(code);

            auto fop = new FilterOperator(parent, udf, columnNames);
            fop->setID(id);
            return std::shared_ptr<FilterOperator>(fop);
        }

    private:
        bool _good;
    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::FilterOperator);
#endif

#endif //TUPLEX_FILTEROPERATOR_H