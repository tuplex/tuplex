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
    FilterOperator::FilterOperator(const std::shared_ptr<LogicalOperator>& parent,
            const UDF &udf,
            const std::vector<std::string>& columnNames,
            const std::unordered_map<size_t, size_t>& rewriteMap) : UDFOperator::UDFOperator(parent, udf, columnNames, rewriteMap), _good(true) {

        //// infer schema (may throw exception!) after applying UDF
        //setSchema(Schema(Schema::MemoryLayout::ROW, python::Type::UNKNOWN));

        // special case: parent is of exception type -> propagate
        if(parent && parent->getOutputSchema().getRowType().isExceptionType()) {
            _udf.setInputSchema(this->parent()->getOutputSchema());
            _udf.setOutputSchema(this->parent()->getOutputSchema());
        } else {
            // check if compiled, then hintschema
            if(_udf.isCompiled()) {
                // remove types from UDF --> retype using parent...
                _udf.removeTypes();

                if(parent) {
                    // rewrite column access if given info
                    if(!_udf.rewriteDictAccessInAST(parent->columns())) {
                        _good = false;
                        return;
                    }

                    // hint from parents
                    _udf.hintInputSchema(this->parent()->getOutputSchema());
                }
            } else {
                // set input schema from parent & set as output bool
                if(parent)
                    _udf.setInputSchema(this->parent()->getOutputSchema());
                else
                    _udf.setInputSchema(Schema::UNKNOWN);
                _udf.setOutputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::BOOLEAN})));
            }

        }

        // check whether UDF is compliant, if so take schema from parent
        if(good() && parent)
            setOutputSchema(this->parent()->getOutputSchema());
    }

    std::shared_ptr<FilterOperator> FilterOperator::from_udf(const UDF &udf, const python::Type &row_type,
                                                             const std::vector<std::string> &input_column_names) {
       if(!row_type.isRowType()) {
           assert(row_type.isTupleType());
           assert(!input_column_names.empty() && row_type.parameters().size() == input_column_names.size());
       }

        auto fop = std::shared_ptr<FilterOperator>(new FilterOperator(nullptr, udf, input_column_names));

        auto schema = Schema(Schema::MemoryLayout::ROW, row_type);

        // type
        if(fop->_udf.isCompiled()) {

            // rewrite dict access (for non-rowtype types)
            if(!input_column_names.empty() && !row_type.isRowType() && !fop->_udf.rewriteDictAccessInAST(input_column_names))
                return nullptr;

            if(!fop->_udf.hintInputSchema(schema))
                return nullptr;
        } else {
            fop->_udf.setInputSchema(schema);
            fop->_udf.setOutputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::BOOLEAN})));
        }

        return fop;
    }


    bool FilterOperator::good() const {
        return _good && !_udf.getOutputSchema().getRowType().isIllDefined();
    }

    void FilterOperator::setDataSet(DataSet *dsptr) {
        // check whether schema is ok, if not set error dataset!
        if(getOutputSchema().getRowType().isIllDefined()) {
            if(dsptr)
                LogicalOperator::setDataSet(&dsptr->getContext()->makeError("schema could not be propagated successfully"));
            else {
                Logger::instance().defaultLogger().error("output schema for filter operator is not well-defined, propagation error?");
                LogicalOperator::setDataSet(nullptr);
            }
        } else
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

    std::vector<Row> FilterOperator::getSample(const size_t num, bool applyFilter) const {
        if(!applyFilter)
            return getSample(num);

        // get sample from parent & apply filter!
        auto in_sample = parent()->getSample(num);
        std::vector<Row> sample;
        auto pickledCode = _udf.getPickledCode();
        python::lockGIL();

        auto func = python::deserializePickledFunction(python::getMainModule(), pickledCode.c_str(),
                                                       pickledCode.length());
        size_t numExceptions = 0;
        for (const auto& row : in_sample) {

            auto rowObj = python::rowToPython(row);
            // call python function
            ExceptionCode ec;
            // first try using dict mode
            // => should use python pipeline builder with its helper classes to process that...
            auto pcr = !inputColumns().empty() ? python::callFunctionWithDictEx(func, rowObj, inputColumns()) :
                       python::callFunctionEx(func, rowObj);
            // if fails, call again without dict mode
            if(pcr.exceptionCode != ExceptionCode::SUCCESS) {
                pcr = python::callFunctionEx(func, rowObj);
            }

            ec = pcr.exceptionCode;
            auto pyobj_res = pcr.res;

            // only append if success
            if (ec != ExceptionCode::SUCCESS)
                numExceptions++;
            else {

                bool filter_ret = false;
                // what type is it? tuple or not?
                if(PyTuple_Check(pyobj_res) && PyTuple_Size(pyobj_res) == 1) {
                    auto item = PyTuple_GET_ITEM(pyobj_res, 0);
                    filter_ret = PyObject_IsTrue(item);
                } else {
                    filter_ret = PyObject_IsTrue(pyobj_res);
                }

                if(filter_ret)
                    sample.push_back(row);
#ifndef NDEBUG
                // Py_XINCREF(pyobj_res);
                // auto res = python::pythonToRow(pyobj_res);
                // // check what res is...
                // std::cout<<res.toPythonString()<<std::endl;
#endif

                Py_XDECREF(pyobj_res);
            }
        }

        if (numExceptions != 0)
            Logger::instance().logger("physical planner").warn(
                    "sampling map operator lead to " + std::to_string(numExceptions) + " exceptions");
        python::unlockGIL();

        return sample;
    }


    std::shared_ptr<LogicalOperator> FilterOperator::clone(bool cloneParents) {
        auto copy = new FilterOperator(cloneParents ? parent()->clone() : nullptr, _udf,
                                       UDFOperator::columns(),
                                       UDFOperator::rewriteMap());
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        copy->_good = _good;
        assert(checkBasicEqualityOfOperators(*copy, *this));
        return std::shared_ptr<LogicalOperator>(copy);
    }

    void FilterOperator::rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) {
        UDFOperator::rewriteParametersInAST(rewriteMap);

        // update schema & co
        setOutputSchema(parent()->getOutputSchema());

        // won't work b.c. of single param as tuple or not...
        // assert(_udf.getInputSchema() == parent()->getOutputSchema());
    }

    bool FilterOperator::retype(const RetypeConfiguration& conf) {

        // UDFOperator retype
        auto ret = UDFOperator::retype(conf);
        if(!ret)
            return false;

        // update output schema (based on parent)
        if(parent())
            setOutputSchema(parent()->getOutputSchema());
        else {
            Schema schema(Schema::MemoryLayout::ROW, conf.row_type);
            setOutputSchema(schema);
        }
        return true;

//        auto input_row_type = conf.row_type;
//        auto is_projected_row_type = conf.is_projected;
//
//        assert(input_row_type.isTupleType());
//
//        performRetypeCheck(input_row_type, is_projected_row_type);
//
//        auto schema = Schema(getOutputSchema().getMemoryLayout(), input_row_type);
//
//        // is it an empty UDF? I.e. a rename operation?
//        try {
//            // update UDF
//            _udf.removeTypes(false);
//
//            // set columns & rewriteMap
//            _udf.rewriteDictAccessInAST(parent()->columns());
//            if(!_udf.retype(input_row_type))
//                return false;
//
//            // get new input/output schema
//            auto output_schema = _udf.getOutputSchema(); // should be boolean or so (else truth test!)
//
//            // filter preserves schema from parent operator.
//            setOutputSchema(parent()->getOutputSchema());
//
//            return true;
//        } catch(...) {
//            return false;
//        }
    }
}