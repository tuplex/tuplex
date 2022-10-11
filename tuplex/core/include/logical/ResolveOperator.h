//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_RESOLVEOPERATOR_H
#define TUPLEX_RESOLVEOPERATOR_H

#include "UDFOperator.h"
#include "ExceptionOperator.h"

namespace tuplex {

    class ResolveOperator : public UDFOperator, public ExceptionOperator<ResolveOperator> {
    private:

        // do schemas of resolver udf and parent match?
        bool schemasMatch() const;

    public:
        LogicalOperator *clone() override;

    private:

        // resolve operator is special when it comes to inferring the schema
        // instead of taking the output of the parent, it takes its input!
        Schema inferSchema(Schema parentSchema) override;
    public:
        ResolveOperator(LogicalOperator *parent,
                const ExceptionCode& ecToResolve,
                const UDF& udf,
                const std::vector<std::string>& columnNames);

        std::string name() override { return "resolve"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::RESOLVE; }

        bool good() const override;
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        /*!
         * indicates whether the output schema of the resolve operator (after upcast etc.) is compatible with
         * the operator it is resolving exceptions for.
         * @return true -> compatible, false -> not compatible, i.e. need to put row onto fallback path.
         */
        bool isCompatibleWithThrowingOperator() const;

        Schema throwingOperatorSchema() const;
        Schema resolverSchema() const;

        std::vector<Row> getSample(const size_t num) const override;

        void rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) override;
    };
}

#endif //TUPLEX_RESOLVEOPERATOR_H