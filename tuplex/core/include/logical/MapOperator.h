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
        LogicalOperator *clone() override;

        MapOperator(LogicalOperator *parent,
                    const UDF& udf,
                    const std::vector<std::string>& columnNames);
        // needs a parent

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
    };
}
#endif //TUPLEX_MAPOPERATOR_H