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
        FilterOperator(LogicalOperator *parent, const UDF& udf, const std::vector<std::string>& columnNames);

        std::string name() override { return "filter"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::FILTER; }
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override;

        void setDataSet(DataSet* dsptr) override;

        std::vector<Row> getSample(const size_t num) const override;

        LogicalOperator *clone() override;

        /*!
         * projection pushdown, update UDF and schema...
         * @param rewriteMap
         */
        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap) override;

        bool retype(const std::vector<python::Type>& rowTypes) override;

    private:
        bool _good;
    };
}

#endif //TUPLEX_FILTEROPERATOR_H