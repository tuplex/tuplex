//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 3/2/20                                                                   //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <utility>

//
// Created by Leonhard Spiegelberg on 3/2/20.
//

#ifndef TUPLEX_AGGREGATEOPERATOR_H
#define TUPLEX_AGGREGATEOPERATOR_H

namespace tuplex {

    enum class AggregateType {
        AGG_NONE=0,
        AGG_UNIQUE=10, // aggregate is unique(distinct) operation over whole schema.
        AGG_GENERAL=20, // the aggregate is simply one global object that gets updated via init, update, combine.
        AGG_BYKEY=30 // aggregate is keyed and then similar to general with init, update, combine.
    };

    class AggregateOperator : public LogicalOperator {
    public:
        virtual ~AggregateOperator() override = default;

        AggregateOperator(LogicalOperator* parent,
                          const AggregateType& at,
                          const UDF& combiner=UDF("",""),
                          const UDF& aggregator=UDF("",""),
                          Row initialValue=Row(),
                          std::vector<std::string> keys = {}) : LogicalOperator(parent), _aggType(at),
                                                                _combiner(combiner), _aggregator(aggregator),
                                                                _initialValue(std::move(initialValue)), _keys(std::move(keys)) {
            std::vector<python::Type> keyTypes;
            for(const auto &k : _keys) {
                auto val = indexInVector(k, parent->columns());
                if(val < 0) throw std::runtime_error("Column " + k + " not in columns (Aggregate)");
                _keyColsInParent.push_back(val);
                keyTypes.push_back(parent->getOutputSchema().getRowType().parameters()[val]);
            }
            _keyType = python::Type::makeTupleType(keyTypes);
            if(_keyType.parameters().size() == 1) _keyType = _keyType.parameters().front();

            if(!inferAndCheckTypes()) {
//#ifndef NDEBUG
                // use defensive programming here...
                // swap combiner & aggregator => easily to be mixed up.
                core::swap(_combiner, _aggregator);

                // reset type info in both
                _combiner.removeTypes();
                _aggregator.removeTypes();

                if(!inferAndCheckTypes()) {
                    core::swap(_combiner, _aggregator);
                    throw std::runtime_error("failed to type aggregate operator. Wrong order of parameters within UDFs?");
                }
                Logger::instance().defaultLogger().warn("wrong order of functions in aggregate, please fix in source code.");
//#else
//                throw std::runtime_error("failed to type aggregate operator. Wrong order of parameters or functions?");
//#endif

            }
        }


        std::string name() override {
            switch(_aggType) {
                case AggregateType::AGG_UNIQUE:
                    return "unique";
                case AggregateType::AGG_BYKEY:
                    return "aggregate_by_key";
                default:
                    return "aggregate";
            }
        }

        AggregateType aggType() const { return _aggType; }

        LogicalOperatorType type() const override { return LogicalOperatorType::AGGREGATE; }
        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override { return true; }

        Schema getInputSchema() const override { return parent()->getOutputSchema(); }

        virtual std::vector<Row> getSample(const size_t num) const override {
            return parent()->getSample(num);
        }

        std::vector<std::string> columns() const override {
            if(aggType() == AggregateType::AGG_UNIQUE)
                return parent()->columns();
            else if(aggType() == AggregateType::AGG_BYKEY) {
                // flatten output tuple
                auto num_agg_columns = _aggregateOutputType.isTupleType() ? _aggregateOutputType.parameters().size() : 1;
                std::vector<std::string> cols(_keys.size() + num_agg_columns, ""); // initialize to empty strings
                assert(cols.size() >= _keys.size());
                for(int pos = 0; pos < _keys.size(); pos++) cols[pos] = _keys[pos]; // set the known columns
                return cols;
            }

            // for general aggregate, no column names! They need to be introduced again.
            return std::vector<std::string>();
        }

        LogicalOperator* clone() override;

        const UDF& aggregatorUDF() const { return _aggregator; }
        const UDF& combinerUDF() const { return _combiner; }
        UDF& aggregatorUDF() { return _aggregator; }
        UDF& combinerUDF() { return _combiner; }
        Row initialValue() const { return _initialValue; }
        python::Type aggregateOutputType() const { return _aggregateOutputType; }

        /*!
         * Retrieve the keys of an aggregate by key operation -- only valid for operators of type AGG_BYKEY.
         * @return The names of the key columns
         */
        std::vector<std::string> keys() const { assert(aggType() == AggregateType::AGG_BYKEY); return _keys; }

        /*!
         * Retrieve the indices of the keys in the parent -- only valid for operators of type AGG_BYKEY.
         * @return The indices of key columns in parent
         */
        std::vector<size_t> keyColsInParent() const { assert(aggType() == AggregateType::AGG_BYKEY); return _keyColsInParent; }
        python::Type keyType() const { assert(aggType() == AggregateType::AGG_BYKEY); return _keyType; }
    private:
        AggregateType _aggType;

        python::Type _aggregateOutputType;
        UDF _combiner;
        UDF _aggregator;
        Row _initialValue;

        // these vectors represent the columns passed by the user to be used as keys in AggregateByKey
        std::vector<std::string> _keys; // string column names
        std::vector<size_t> _keyColsInParent; // indices of key columns
        python::Type _keyType; // the overall type of the key

        bool inferAndCheckTypes();
    };
}

#endif //TUPLEX_AGGREGATEOPERATOR_H