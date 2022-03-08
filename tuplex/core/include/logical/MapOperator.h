//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_MAPOPERATOR_H
#define TUPLEX_MAPOPERATOR_H

#include <UDF.h>
#include "UDFOperator.h"

namespace tuplex {
    class MapOperator : public UDFOperator {
    private:
        std::vector<std::string> _outputColumns;
        std::string _name;
    public:
        std::shared_ptr<LogicalOperator> clone() override;

        MapOperator(const std::shared_ptr<LogicalOperator>& parent,
                    const UDF& udf,
                    const std::vector<std::string>& columnNames);
        // needs a parent

        inline nlohmann::json to_json() const {
            // make it a super simple serialiation!
            // basically mimick clone
            nlohmann::json obj;
            obj["name"] = "map";
            obj["columnNames"] = UDFOperator::columns();
            obj["outputColumns"] = _outputColumns;
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


        /*!
         * set name of operator. Because map Operator is an umbrella for select/rename, use this here to give it more precise meaning.
         */
        void setName(const std::string& name) { _name = name; }
        std::string name() override { return _name; }
        LogicalOperatorType type() const override { return LogicalOperatorType::MAP; }
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override;

        void setDataSet(DataSet* dsptr) override;

        std::vector<Row> getSample(const size_t num) const override;

        std::vector<std::string> inputColumns() const override { return UDFOperator::columns(); }
        std::vector<std::string> columns() const override { return _outputColumns; }

        /*!
         * assign names to the output columns (order matters here...)
         * @param columns
         */
        void setOutputColumns(const std::vector<std::string>& columns) {
            assert(_outputColumns.empty() || _outputColumns.size() == columns.size());
            _outputColumns = columns;
        }

        /*!
         * projection pushdown, update UDF and schema...
         * @param rewriteMap
         */
        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap) override;

        bool retype(const std::vector<python::Type>& rowTypes) override;

        // cereal serialization functions
        template<class Archive> void serialize(Archive &ar) {
            ar(::cereal::base_class<UDFOperator>(this), _outputColumns, _name);
        }
    };
}

#endif //TUPLEX_MAPOPERATOR_H