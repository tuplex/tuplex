//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_WITHCOLUMNOPERATOR_H
#define TUPLEX_WITHCOLUMNOPERATOR_H

#include "LogicalOperator.h"
#include "UDFOperator.h"
#include <string>
#include <vector>

namespace tuplex {
    class WithColumnOperator : public UDFOperator {
    private:
        std::string _newColumn;
        int _columnToMapIndex;

        int calcColumnToMapIndex(const std::vector<std::string> &columnNames,
                             const std::string &columnName);

        Schema getOutputSchemaFromReturnAndInputRowType(const python::Type &retType, const python::Type &input_type) const;
    public:
        std::shared_ptr<LogicalOperator> clone(bool cloneParents) override;

    protected:
        Schema inferSchema(Schema parentSchema, bool is_projected_row_type) override;
    public:
        // required by cereal
        WithColumnOperator() = default;

        WithColumnOperator(const std::shared_ptr<LogicalOperator>& parent,
        const std::vector<std::string>& columnNames,
        const std::string& columnName,
        const UDF& udf,
        const std::unordered_map<size_t, size_t>& rewriteMap={});

        std::string name() const override { return "withColumn"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::WITHCOLUMN; }

        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override { return _columnToMapIndex >= 0 && UDFOperator::getOutputSchema() != Schema::UNKNOWN; }

        void setDataSet(DataSet* dsptr) override;

        std::vector<Row> getSample(const size_t num) const override;

        int getColumnIndex() const { assert(_columnToMapIndex >= 0); return _columnToMapIndex; }
        std::string columnToMap() const { return _newColumn; }

        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap) override;

        virtual std::vector<std::string> columns() const override;

        Schema getInputSchema() const override {

            // UDF input schema & parent output schema should match??
            if(parent())
                return parent()->getOutputSchema(); // overwrite here, because UDFOperator always returns the UDF's input schema. However, for mapColumn it's not a row but an element!
            else
                return Schema::UNKNOWN;
        }

        Schema getOutputSchema() const override;

        bool retype(const RetypeConfiguration& conf) override;

        /*!
         * check whether withColumn creates new column or overwrites existing one.
         * @return whether it creates new column(true) or overwrites existing one (false)
         */
        inline bool creates_new_column() const {
            auto input_schema = getInputSchema();
            auto output_schema = getOutputSchema();
            if(input_schema != Schema::UNKNOWN && output_schema != Schema::UNKNOWN) {
                return input_schema.getRowType().parameters().size() < output_schema.getRowType().parameters().size();
            }

            auto columnNames = UDFOperator::inputColumns();
            if(columnNames.empty()) {
                return true; // no column names given -> always create new column
            } else {
                auto columnToMapIndex = indexInVector(_newColumn, columnNames);
                if(columnToMapIndex < 0)
                    return true;
                else
                    return false;
            }
        }

#ifdef BUILD_WITH_CEREAL
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<UDFOperator>(this), _newColumn, _columnToMapIndex);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<UDFOperator>(this), _newColumn, _columnToMapIndex);
        }
#endif

        inline nlohmann::json to_json() const {
            // make it a super simple serialiation!
            // basically mimick clone
            nlohmann::json obj;
            obj["name"] = "withColumn";
            obj["columnNames"] = UDFOperator::columns();
            obj["columnName"] = _newColumn;
            obj["columnIndex"] = getColumnIndex();
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
    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::WithColumnOperator);
#endif

#endif //TUPLEX_WITHCOLUMNOPERATOR_H