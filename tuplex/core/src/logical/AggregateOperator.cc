//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/execution/AggregateStage.h>
#include <logical/AggregateOperator.h>


namespace tuplex {

    AggregateStage::AggregateStage(tuplex::PhysicalPlan *plan, tuplex::IBackend *backend, tuplex::PhysicalStage *parent,
                                   const tuplex::AggregateType &at, int64_t stage_number, int64_t outputDataSetID)
            : PhysicalStage::PhysicalStage(plan,
                                           backend,
                                           stage_number,
                                           {parent}),
              _aggType(at),
              _outputDataSetID(outputDataSetID) {

    }

    std::shared_ptr<LogicalOperator> AggregateOperator::clone(bool cloneParents) {
        // copy manually everything else over
        auto copy = new AggregateOperator();
        if(cloneParents)
            copy->setParent(parent()->clone());
        // members
        copy->_aggType = aggType();
        copy->_combiner = _combiner;
        copy->_aggregator = _aggregator;
        copy->_initialValue = _initialValue;
        copy->_keys = _keys;
        copy->_keyColsInParent = keyColsInParent();
        copy->_keyType = keyType();
        copy->_aggregateOutputType = _aggregateOutputType;

//        // important to use here input column names, i.e. stored in base class UDFOperator!
//        auto copy = new AggregateOperator(cloneParents ? parent()->clone() : nullptr, aggType(),
//                                          _combiner, _aggregator, _initialValue, _keys);

        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }

    void hintTwoParamUDF(UDF& udf, const python::Type& a, const python::Type& b) {
        auto params = udf.getInputParameters();

        // there should be two params:
        if(params.size() != 2) {
            throw std::runtime_error("function " +
                                     (udf.isPythonLambda() ? udf.getCode() : udf.pythonFunctionName())
                                     + " has incompatible signature to be passed to aggregate function. "
                                       "Function has to have two parameters aggregate,row.");
        }

        // add type hints
        if(!udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({a, b})))) {
            // could not hint
            std::stringstream ss;
            ss << "Could not hint input schema " << std::get<0>(params[0]) << "=" << a.desc()
               << ", " <<
               std::get<0>(params[1]) << "=" << b.desc()
               << " to combiner UDF" << std::endl;
            throw std::runtime_error(ss.str());
        }
    }

    void retypeTwoParamUDF(UDF& udf, const python::Type& a, const python::Type& b) {
        auto params = udf.getInputParameters();

        // there should be two params:
        if(params.size() != 2) {
            throw std::runtime_error("function " +
                                     (udf.isPythonLambda() ? udf.getCode() : udf.pythonFunctionName())
                                     + " has incompatible signature to be passed to aggregate function. "
                                       "Function has to have two parameters aggregate,row.");
        }

        // retype using provided types
        auto new_row_type = python::Type::makeTupleType({a, b});
        udf.removeTypes(false);
        if(!udf.retype(new_row_type)) {
            // could not hint
            std::stringstream ss;
            ss << "Could not retype combiner UDF with " << std::get<0>(params[0]) << "=" << a.desc()
               << ", " <<
               std::get<0>(params[1]) << "=" << b.desc()
               << std::endl;
            throw std::runtime_error(ss.str());
        }
    }

    bool AggregateOperator::inferAndCheckTypes() {

        auto& logger = Logger::instance().defaultLogger();

        try {
            // aggregate type unique? -> simply take parent schema
            if(AggregateType::AGG_UNIQUE == aggType()) {
                setOutputSchema(parent()->getOutputSchema()); // inherit schema from parent
                return true;
            }

            // one empty? --> abort.
            if(_combiner.empty() || _aggregator.empty() || _initialValue == Row()) {
#ifndef NDEBUG
                if(AggregateType::AGG_UNIQUE != _aggType)
                    throw std::runtime_error("params invalid for aggregateType != unique");
#endif
                _combiner = UDF("", "");
                _aggregator = UDF("", "");
                _initialValue = Row();
                return true;
            }

            //  rewrite dict access in UDFs
            if(_aggregator.isCompiled()) {
                assert(_aggregator.getInputParameters().size() == 2);
                auto rowParam = std::get<0>(_aggregator.getInputParameters()[1]);

                // rewrite second param!
                _aggregator.rewriteDictAccessInAST(parent()->columns(), rowParam);
            }

            // check functions for compatibility:
            // i.e. do inference first, then deduce types.
            auto aggregateType = _initialValue.getRowType();

            // unpack one level if single param!
            if(aggregateType.parameters().size() == 1)
                aggregateType = aggregateType.parameters().front();

            _aggregateOutputType = aggregateType;

            // hint first the aggregator. It's output type an aggregateType need to be compatible.
            // if not - fail.
            // hint the aggregator function. It does have to have two input params too.
            auto rowtype = parent()->getOutputSchema().getRowType();
            if(rowtype.parameters().size() == 1) // unpack one level
                rowtype = rowtype.parameters().front();
            hintTwoParamUDF(_aggregator, aggregateType, rowtype);
            logger.debug("aggregator output-schema is: " + _aggregator.getOutputSchema().getRowType().desc());

            if(Schema::UNKNOWN == _aggregator.getOutputSchema()) {
                throw std::runtime_error("failed to type aggregator function.");
            }

            // are they compatible?
            auto t_policy = TypeUnificationPolicy::defaultPolicy();
            t_policy.allowAutoUpcastOfNumbers = true;
            t_policy.unifyMissingDictKeys = true;
            auto aggregator_output_type = _aggregator.getOutputSchema().getRowType();
            if(aggregator_output_type.parameters().size() == 1)
               aggregator_output_type = aggregator_output_type.parameters().front();
            auto uni_type = unifyTypes(aggregateOutputType(), aggregator_output_type, t_policy);
            if(python::Type::UNKNOWN == uni_type) {
                logger.error("type of initial aggregate " + aggregateOutputType().desc() +
                " and output type of aggregator udf " + aggregator_output_type.desc() + " incompatible.");
                return false;
            }

            // different? update!
            if(aggregateOutputType() != uni_type) {
                logger.debug("updating aggregate type from " + aggregateType.desc()
                + " to " + uni_type.desc() + " due to output of aggregator udf.");
                aggregateType = uni_type;
                _aggregateOutputType = uni_type;
            }

            // type combiner now (with potentially updated output type)
            hintTwoParamUDF(_combiner, aggregateType, aggregateType);
            logger.debug("combiner output-schema is: " + _combiner.getOutputSchema().getRowType().desc());

            // how


            // check whether everything is compatible.
            // i.e. find out what the super type of everything is
            auto ctype = _combiner.getOutputSchema().getRowType();
            auto atype = _aggregator.getOutputSchema().getRowType();
            auto itype = _initialValue.getRowType();

            // @TODO: upcasting checks from tplx197...

            auto final_type = unifyTypes(ctype, unifyTypes(atype, itype, t_policy), t_policy);
            if(final_type == python::Type::UNKNOWN)
                throw std::runtime_error("incompatible types in aggregate operator");

            // aggregate by key needs to keep the key columns
            if(AggregateType::AGG_BYKEY == aggType()) {
                auto parent_row_types = parent()->getOutputSchema().getRowType().parameters();
                std::vector<python::Type> final_row_type;
                for(const auto &idx : keyColsInParent()) final_row_type.push_back(parent_row_types[idx]);
                // TODO(rahuly): should this be a recursive flatten?
                for(const auto &t : final_type.parameters()) final_row_type.push_back(t); // flatten the aggregate type
                final_type = python::Type::makeTupleType(final_row_type);
            }

            logger.debug("aggregate operator yields: " + final_type.desc());
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, final_type));
            return true;
        } catch(std::exception& e) {
            logger.error("exception while inferring types in aggregate: " + std::string(e.what()));
            return false;
        }
    }

    bool AggregateOperator::retype(const RetypeConfiguration &conf) {

        auto& logger = Logger::instance().defaultLogger();

        logger.info("retyping aggregate operator with new input row type=" + conf.row_type.desc());

        // unique? no retype necessary, simply take over new row type.
        if(AggregateType::AGG_UNIQUE == aggType()) {
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, conf.row_type)); // inherit schema from parent
            return true;
        }

        // others require retyping of aggregator etc.
        // -> means extracting part of row type to feed in the case of bykey
        try {
            //  rewrite dict access in UDFs
            if(_aggregator.isCompiled()) {
                assert(_aggregator.getInputParameters().size() == 2);
                auto rowParam = std::get<0>(_aggregator.getInputParameters()[1]);

                // rewrite second param!
                _aggregator.rewriteDictAccessInAST(conf.columns, rowParam);
            }

            // check functions for compatibility:
            // i.e. do inference first, then deduce types.
            auto aggregateType = _initialValue.getRowType();

            // unpack one level if single param!
            if(aggregateType.parameters().size() == 1)
                aggregateType = aggregateType.parameters().front();

            _aggregateOutputType = aggregateType;

            // hint first the aggregator. It's output type an aggregateType need to be compatible.
            // if not - fail.
            // hint the aggregator function. It does have to have two input params too.
            auto rowtype = conf.row_type;
            if(rowtype.parameters().size() == 1) // unpack one level
                rowtype = rowtype.parameters().front();
            retypeTwoParamUDF(_aggregator, aggregateType, rowtype);
            logger.debug("aggregator output-schema is: " + _aggregator.getOutputSchema().getRowType().desc());

            if(Schema::UNKNOWN == _aggregator.getOutputSchema()) {
                throw std::runtime_error("failed to type aggregator function.");
            }

            // are they compatible?
            auto t_policy = TypeUnificationPolicy::defaultPolicy();
            t_policy.allowAutoUpcastOfNumbers = true;
            t_policy.unifyMissingDictKeys = true;
            auto aggregator_output_type = _aggregator.getOutputSchema().getRowType();
            if(aggregator_output_type.parameters().size() == 1)
                aggregator_output_type = aggregator_output_type.parameters().front();
            auto uni_type = unifyTypes(aggregateOutputType(), aggregator_output_type, t_policy);
            if(python::Type::UNKNOWN == uni_type) {
                logger.error("type of initial aggregate " + aggregateOutputType().desc() +
                             " and output type of aggregator udf " + aggregator_output_type.desc() + " incompatible.");
                return false;
            }

            // different? update!
            if(aggregateOutputType() != uni_type) {
                logger.debug("updating aggregate type from " + aggregateType.desc()
                             + " to " + uni_type.desc() + " due to output of aggregator udf.");
                aggregateType = uni_type;
                _aggregateOutputType = uni_type;
            }

            // type combiner now (with potentially updated output type)
            retypeTwoParamUDF(_combiner, aggregateType, aggregateType);
            logger.debug("combiner output-schema is: " + _combiner.getOutputSchema().getRowType().desc());

            // how


            // check whether everything is compatible.
            // i.e. find out what the super type of everything is
            auto ctype = _combiner.getOutputSchema().getRowType();
            auto atype = _aggregator.getOutputSchema().getRowType();
            auto itype = _initialValue.getRowType();

            // @TODO: upcasting checks from tplx197...

            auto final_type = unifyTypes(ctype, unifyTypes(atype, itype, t_policy), t_policy);
            if(final_type == python::Type::UNKNOWN)
                throw std::runtime_error("incompatible types in aggregate operator");

            // aggregate by key needs to keep the key columns
            if(AggregateType::AGG_BYKEY == aggType()) {
                auto parent_row_types = conf.row_type.parameters();
                std::vector<python::Type> final_row_type;
                for(const auto &idx : keyColsInParent()) final_row_type.push_back(parent_row_types[idx]);
                // TODO(rahuly): should this be a recursive flatten?
                for(const auto &t : final_type.parameters()) final_row_type.push_back(t); // flatten the aggregate type
                final_type = python::Type::makeTupleType(final_row_type);
            }

            logger.debug("aggregate operator yields: " + final_type.desc());
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, final_type));
            return true;
        } catch(std::exception& e) {
            logger.error("exception while retyping types in aggregate: " + std::string(e.what()));
            return false;
        }

        return false;
    }
}