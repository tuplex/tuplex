//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/FilterOperator.h>

namespace tuplex {

    // within constructor
    // check that return type of UDF is bool!
    FilterOperator::FilterOperator(LogicalOperator *parent,
            const UDF &udf,
            const std::vector<std::string>& columnNames) : UDFOperator::UDFOperator(parent, udf, columnNames), _good(true) {

        //// infer schema (may throw exception!) after applying UDF
        //setSchema(Schema(Schema::MemoryLayout::ROW, python::Type::UNKNOWN));

        // check if compiled, then hintschema
        if(_udf.isCompiled()) {
            // remove types from UDF --> retype using parent...
            _udf.removeTypes();

            // rewrite column access if given info
            if(!_udf.rewriteDictAccessInAST(parent->columns())) {
                _good = false;
                return;
            }

            // hint from parents
            _udf.hintInputSchema(this->parent()->getOutputSchema());
        } else {
            // set input schema from parent & set as output bool
            _udf.setInputSchema(this->parent()->getOutputSchema());
            _udf.setOutputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::BOOLEAN})));
        }

        // check whether UDF is compliant, if so take schema from parent
        if(good())
            setSchema(this->parent()->getOutputSchema());
    }

    bool FilterOperator::good() const {
        return _good && !_udf.getOutputSchema().getRowType().isIllDefined();
    }

    void FilterOperator::setDataSet(DataSet *dsptr) {
        // check whether schema is ok, if not set error dataset!
        if(schema().getRowType().isIllDefined())
            LogicalOperator::setDataSet(&dsptr->getContext()->makeError("schema could not be propagated successfully"));
        else
            LogicalOperator::setDataSet(dsptr);
    }

    std::vector<Row> FilterOperator::getSample(const size_t num) const {
        // this here is actually a bit tricky.
        // What sample shall be returned here?
        // tuples that satisfy the filter condition?
        // --> problem with this is there could be tuples that do not satisfy this
        // or in the worst case to find a single tuple that satisfies the condition
        // we would need to touch the full data.
        // Hence, the best is to restrict in the following way:
        // Applying a filter does not change the underlying type and the exception guarantees
        // I.e. the normal case after filtering is still the same!

        return parent()->getSample(num);
    }

    LogicalOperator *FilterOperator::clone() {
        auto copy = new FilterOperator(parent()->clone(), _udf,
                                       UDFOperator::columns());
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        copy->_good = _good;
        return copy;
    }

    void FilterOperator::rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) {
        UDFOperator::rewriteParametersInAST(rewriteMap);

        // update schema & co
        setSchema(parent()->getOutputSchema());

        // won't work b.c. of single param as tuple or not...
        // assert(_udf.getInputSchema() == parent()->getOutputSchema());
    }

    bool FilterOperator::retype(const std::vector<python::Type> &rowTypes) {
        assert(rowTypes.size() == 1);
        assert(rowTypes.front().isTupleType());

        auto schema = Schema(getOutputSchema().getMemoryLayout(), rowTypes.front());

        // is it an empty UDF? I.e. a rename operation?
        try {
            // update UDF
            _udf.removeTypes(false);
            _udf.hintInputSchema(schema);

            setSchema(schema);
            return true;
        } catch(...) {
            return false;
        }
    }
}