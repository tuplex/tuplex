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
    public:
        LogicalOperator *clone() override;

    protected:
        Schema inferSchema(Schema parentSchema) override;
    public:
        WithColumnOperator(LogicalOperator *parent,
        const std::vector<std::string>& columnNames,
        const std::string& columnName,
        const UDF& udf);

        std::string name() override { return "withColumn"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::WITHCOLUMN; }

        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override { return _columnToMapIndex >= 0 && UDFOperator::schema() != Schema::UNKNOWN; }

        void setDataSet(DataSet* dsptr) override;

        std::vector<Row> getSample(const size_t num) const override;

        int getColumnIndex() const { assert(_columnToMapIndex >= 0); return _columnToMapIndex; }
        std::string columnToMap() const { return _newColumn; }

        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap) override;

        virtual std::vector<std::string> columns() const override;

        Schema getInputSchema() const override {

            // UDF input schema & parent output schema should match??

            return parent()->getOutputSchema(); // overwrite here, because UDFOperator always returns the UDF's input schema. However, for mapColumn it's not a row but an element!
        }

        bool retype(const std::vector<python::Type>& rowTypes=std::vector<python::Type>()) override;
    };
}

#endif //TUPLEX_WITHCOLUMNOPERATOR_H