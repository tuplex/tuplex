//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_MAPCOLUMNOPERATOR_H
#define TUPLEX_MAPCOLUMNOPERATOR_H

#include "LogicalOperator.h"
#include "UDFOperator.h"

namespace tuplex {

    class MapColumnOperator : public UDFOperator {
    private:
        std::string _columnToMap;
        int         _columnToMapIndex;
    public:
        LogicalOperator *clone() override;

    protected:
        Schema inferSchema(Schema parentSchema) override;
    public:

        MapColumnOperator(LogicalOperator *parent,
                          const std::string& columnName,
                          const std::vector<std::string>& columns,
                          const UDF& udf,
                          bool allowNumericTypeUnification);
        // needs a parent

        std::string name() override { return "mapColumn"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::MAPCOLUMN; }

        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override { return _columnToMapIndex >= 0; }

        void setDataSet(DataSet* dsptr) override;

        std::vector<Row> getSample(const size_t num) const override;

        int getColumnIndex() const { assert(_columnToMapIndex >= 0); return _columnToMapIndex; }
        std::string columnToMap() const { return _columnToMap; }

        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap) override;

        Schema getInputSchema() const override {
            return parent()->getOutputSchema(); // overwrite here, because UDFOperator always returns the UDF's input schema. However, for mapColumn it's not a row but an element!
        }

        bool retype(const std::vector<python::Type>& rowTypes) override;
    };

}

#endif //TUPLEX_MAPCOLUMNOPERATOR_H