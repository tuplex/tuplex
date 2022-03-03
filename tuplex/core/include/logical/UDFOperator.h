//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_UDFOPERATOR_H
#define TUPLEX_UDFOPERATOR_H

#include "LogicalOperator.h"
#include <unordered_map>
#include <UDF.h>
#include <vector>
#include <string>

namespace tuplex {

    /*!
     * abstract class for operators using a UDF
     */
    class UDFOperator : public LogicalOperator {
    protected:
        UDF _udf;

        /*!
         * detects schema of operator using sample if necessary.
         * @return
         */
        virtual Schema inferSchema(Schema parentSchema=Schema::UNKNOWN);

        /*!
         * update internal column names with a rewrite map from projection pushdown
         * @param rewriteMap
         */
        void projectColumns(const std::unordered_map<size_t, size_t>& rewriteMap);

    private:
        std::vector<std::string> _columnNames;
    public:
        UDFOperator() = delete;
        UDFOperator(const std::shared_ptr<LogicalOperator> &parent, const UDF& udf,
                const std::vector<std::string>& columnNames=std::vector<std::string>());

        UDF& getUDF() { return _udf; }
        const UDF& getUDF() const { return _udf; }

        virtual void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap);

        virtual Schema getInputSchema() const override {
            // @TODO: fix this
            // => should return row schema! not what the udf needs... (because of nesting...)
            assert(_udf.getInputSchema().getRowType() != python::Type::UNKNOWN); return _udf.getInputSchema();
        }

        virtual std::vector<std::string> columns() const override { return _columnNames; }

        void setColumns(const std::vector<std::string>& columns) { assert(_columnNames.empty() || _columnNames.size() == columns.size()); _columnNames = columns; }

        // cereal serialization functions
        template<class Archive> void serialize(Archive &ar) {
            ar(::cereal::base_class<LogicalOperator>(this), _udf, _columnNames);
        }
    };

    /*!
     * helper funtion to determine whether an operator has a UDF or not
     * @param op
     * @return whether op has a UDF or not.
     */
    extern bool hasUDF(const LogicalOperator *op);
}

CEREAL_REGISTER_TYPE(tuplex::UDFOperator);
#endif //TUPLEX_UDFOPERATOR_H