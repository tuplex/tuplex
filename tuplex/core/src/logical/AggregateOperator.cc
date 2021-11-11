//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/AggregateStage.h>
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

    LogicalOperator *AggregateOperator::clone() {
        // important to use here input column names, i.e. stored in base class UDFOperator!
        auto copy = new AggregateOperator(parent()->clone(), aggType(),
                                          _combiner, _aggregator, _initialValue, _keys);

        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
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

    bool AggregateOperator::inferAndCheckTypes() {

        auto& logger = Logger::instance().defaultLogger();

        try {
            // aggregate type unique? -> simply take parent schema
            if(AggregateType::AGG_UNIQUE == aggType()) {
                setSchema(parent()->getOutputSchema()); // inherit schema from parent
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
            hintTwoParamUDF(_combiner, aggregateType, aggregateType);
            logger.info("combiner output-schema is: " + _combiner.getOutputSchema().getRowType().desc());

            // how hint the aggregator function. It too has to have two input params.
            auto rowtype = parent()->getOutputSchema().getRowType();
            if(rowtype.parameters().size() == 1) // unpack one level
                rowtype = rowtype.parameters().front();
            hintTwoParamUDF(_aggregator, aggregateType, rowtype);
            logger.info("aggregator output-schema is: " + _aggregator.getOutputSchema().getRowType().desc());


            // check whether everything is compatible.
            // i.e. find out what the super type of everything is
            auto ctype = _combiner.getOutputSchema().getRowType();
            auto atype = _aggregator.getOutputSchema().getRowType();
            auto itype = _initialValue.getRowType();

            // @TODO: upcasting checks from tplx197...

            auto final_type = python::Type::superType(ctype, python::Type::superType(atype, itype));
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

            logger.info("aggregate operator yields: " + final_type.desc());
            setSchema(Schema(Schema::MemoryLayout::ROW, final_type));
            return true;
        } catch(std::exception& e) {
            logger.error("exception while inferring types in aggregate: " + std::string(e.what()));
            return false;
        }
    }
}