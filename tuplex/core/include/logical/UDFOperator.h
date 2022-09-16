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
         * @param parentSchema the (output) schema of the parent operator. If unknown, a sample will beused
         * @param is_projected_schema whether parentSchema is a projected schema or not. If it is, the projection map will be utilized.
         * @return the inferred output schema that the UDF would return.
         */
        virtual Schema inferSchema(Schema parentSchema=Schema::UNKNOWN, bool is_projected_schema=false);

        /*!
         * update internal column names with a rewrite map from projection pushdown
         * @param rewriteMap
         */
        void projectColumns(const std::unordered_map<size_t, size_t>& rewriteMap);

        bool performRetypeCheck(const python::Type& input_row_type, bool is_projceted_row_type);
    private:
        std::vector<std::string> _columnNames;
        // need to store rewrite map as well so when cloning AST can be properly rewritten
        std::unordered_map<size_t, size_t> _rewriteMap;
    public:
        UDFOperator() = default; // required by cereal
        UDFOperator(const std::shared_ptr<LogicalOperator> &parent, const UDF& udf,
                const std::vector<std::string>& columnNames=std::vector<std::string>(),
                        const std::unordered_map<size_t, size_t>& rewriteMap=std::unordered_map<size_t, size_t>());

        UDF& getUDF() { return _udf; }
        const UDF& getUDF() const { return _udf; }

        virtual void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap);

        virtual Schema getInputSchema() const override {
            // @TODO: fix this
            // => should return row schema! not what the udf needs... (because of nesting...)

            // // this gets triggered when retyping/reoptimization through clone...
            // assert(_udf.getInputSchema().getRowType() != python::Type::UNKNOWN);


            return _udf.getInputSchema();
        }

        virtual std::vector<std::string> columns() const override { return _columnNames; }

        virtual std::unordered_map<size_t, size_t> rewriteMap() const { return _rewriteMap; }

        /*!
         * indicates whether stored UDF has well defined types or not.
         * @return
         */
        bool hasWellDefinedTypes() const { return _udf.hasWellDefinedTypes(); }

        void setColumns(const std::vector<std::string>& columns) { assert(_columnNames.empty() || _columnNames.size() == columns.size()); _columnNames = columns; }

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<LogicalOperator>(this), _udf, _columnNames, _rewriteMap);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<LogicalOperator>(this), _udf, _columnNames, _rewriteMap);
        }
#endif
    };

    /*!
     * helper funtion to determine whether an operator has a UDF or not
     * @param op
     * @return whether op has a UDF or not.
     */
    extern bool hasUDF(const LogicalOperator *op);
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::UDFOperator);
#endif

#endif //TUPLEX_UDFOPERATOR_H