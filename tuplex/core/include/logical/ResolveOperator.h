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
        std::shared_ptr<LogicalOperator> clone() override;

    private:

        // resolve operator is special when it comes to inferring the schema
        // instead of taking the output of the parent, it takes its input!
        Schema inferSchema(Schema parentSchema) override;
    public:

        // required by cereal
        ResolveOperator() = default;

        ResolveOperator(const std::shared_ptr<LogicalOperator>& parent,
                const ExceptionCode& ecToResolve,
                const UDF& udf,
                const std::vector<std::string>& columnNames);

        std::string name() override { return "resolve"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::RESOLVE; }

        bool good() const override;
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        std::vector<Row> getSample(const size_t num) const override;

        void rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) override;

        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<UDFOperator>(this), ::cereal::base_class<ExceptionOperator<ResolveOperator>>(this));
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<UDFOperator>(this), ::cereal::base_class<ExceptionOperator<ResolveOperator>>(this));
        }
    };
}

CEREAL_REGISTER_TYPE(tuplex::ResolveOperator);
#endif //TUPLEX_RESOLVEOPERATOR_H