//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/WithColumnOperator.h>

namespace tuplex {
    WithColumnOperator::WithColumnOperator(const std::shared_ptr<LogicalOperator>& parent, const std::vector <std::string>& columnNames,
                                           const std::string &columnName, const UDF &udf, const std::unordered_map<size_t, size_t>& rewriteMap): UDFOperator::UDFOperator(parent, udf, columnNames, rewriteMap), _newColumn(columnName) {

        // define index
        _columnToMapIndex = calcColumnToMapIndex(columnNames, columnName);

        // infer schema, this is slightly more involved...
        if(parent)
            setOutputSchema(inferSchema(parent->getOutputSchema(), false));
        else {
            // not set ...
        }
    }

    int WithColumnOperator::calcColumnToMapIndex(const std::vector<std::string> &columnNames,
                                             const std::string &columnName) {
        int columnToMapIndex = 0;

        // empty columns?
        if(columnNames.empty()) {
            // no column names given, two options: if inputschema of parent is tuple get num elements, else it is 1
            auto parentRowType = parent()->getOutputSchema().getRowType();
            if(parentRowType.isTupleType() && parentRowType != python::Type::EMPTYTUPLE) {
                columnToMapIndex = (int)parentRowType.parameters().size();
            } else
                columnToMapIndex = 1; // single element, append new column
        } else {
            columnToMapIndex = indexInVector(columnName, columnNames);
            if(columnToMapIndex < 0)
                columnToMapIndex = (int)columnNames.size(); // i.e. new column, append at end
        }

        return columnToMapIndex;
    }

    Schema WithColumnOperator::inferSchema(Schema parentSchema, bool is_projected_row_type) {

        // if(is_projected_row_type)
        //    throw std::runtime_error("nyimpl");

        if(parentSchema == Schema::UNKNOWN)
            parentSchema = getInputSchema();

        auto inputColumnNames = UDFOperator::inputColumns();

        // detect schema of UDF
        auto udfSchema = UDFOperator::inferSchema(parentSchema, is_projected_row_type);

        // now it's time to unpack or not
        auto udfRetRowType = udfSchema.getRowType();
        auto retType = udfRetRowType;

        // unknown or ill-defined?
        if(retType == python::Type::UNKNOWN || retType.isIllDefined()) {
            auto& logger = Logger::instance().logger("logical plan");
            logger.error("failed to type withColumn operator");
            return Schema::UNKNOWN;
        }

        // could be exception -> return immediately then.
        if(udfRetRowType.isExceptionType())
            return Schema(Schema::MemoryLayout::ROW, udfRetRowType);

        assert(udfRetRowType.isTupleType());
        if(udfRetRowType.parameters().size() == 1)
            retType = retType.parameters().front();

        // create new rowtype depending on output and where column is
        auto inParameters = parentSchema.getRowType().parameters();

        // tuple unpack?
        if(inParameters.size() == 1 && inParameters.front().isTupleType())
            inParameters = inParameters.front().parameters();

        if(!inputColumnNames.empty())
            assert(inputColumnNames.size() <= inParameters.size());

        if(_columnToMapIndex < inParameters.size())
            inParameters[_columnToMapIndex] = retType;
        else
            inParameters.emplace_back(retType);
        return Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType(inParameters));
    }

    void WithColumnOperator::setDataSet(tuplex::DataSet *dsptr) {
        // check whether schema is ok, if not set error dataset!
        if(getOutputSchema().getRowType().isIllDefined()) {
            if(dsptr)
                LogicalOperator::setDataSet(&dsptr->getContext()->makeError("schema could not be propagated successfully"));
            else {
                Logger::instance().defaultLogger().error("output schema for " + name() + " operator is not well-defined, propagation error?");
                LogicalOperator::setDataSet(nullptr);
            }
        } else
            LogicalOperator::setDataSet(dsptr);
    }

    std::vector<std::string> WithColumnOperator::columns() const {
        auto columnNames = UDFOperator::columns();

        if(_columnToMapIndex < columnNames.size())
            return columnNames;

        // else, append new column name
        std::vector<std::string> v(columnNames.begin(), columnNames.end());
        v.emplace_back(_newColumn);
        return v;
    }

    Schema WithColumnOperator::getOutputSchema() const {
        // get python type from UDF operator
        auto udf_ret_type = _udf.getAnnotatedAST().getReturnType();

        auto in_schema = getInputSchema();
        if(Schema::UNKNOWN == in_schema)
            return Schema::UNKNOWN;

        // special case: UDF produced only exceptions -> set ret type to exception!
        if(udf_ret_type.isExceptionType()) {
            return Schema(Schema::MemoryLayout::ROW, udf_ret_type);
        }

        // check what schema is supposed to be
        std::vector<python::Type> col_types = in_schema.getRowType().parameters();
        if(_columnToMapIndex < UDFOperator::columns().size()) {
            col_types[_columnToMapIndex] = udf_ret_type;
        } else {
            col_types.push_back(udf_ret_type);
        }
        auto row_type = python::Type::makeTupleType(col_types);

        return Schema(getInputSchema().getMemoryLayout(), row_type);
    }

    std::vector<Row> WithColumnOperator::getSample(const size_t num) const {
        // @TODO: refactor the whole sampling into SampleProcessor.
        using namespace std;

        auto vSamples = parent()->getSample(num);
        auto pickledCode = _udf.getPickledCode();

        assert(pickledCode.length() > 0);

        std::vector<Row> vRes;

        // get GIL
        python::lockGIL();

        auto pFunc = python::deserializePickledFunction(python::getMainModule(),
                                                       pickledCode.c_str(), pickledCode.length());

        size_t numExceptions = 0;
        for(auto row : vSamples) {

            auto rowObj = python::rowToPython(row);

            // object should be a tuple, get column
            assert(PyTuple_Check(rowObj));

            // call python function
            // issue: when pushdown occurred, then this fails!
            // => SampleProcessor is really, really required!
            ExceptionCode ec;

            // HACK: skip for pushdown.
            // this is bad, but let's get tplx208 done.
            if(!inputColumns().empty() && row.getNumColumns() != inputColumns().size())
                continue;

            auto pcr = !inputColumns().empty() ? python::callFunctionWithDictEx(pFunc, rowObj, inputColumns()) :
                       python::callFunctionEx(pFunc, rowObj);
            ec = pcr.exceptionCode;
            auto pyobj_res = pcr.res;

            // only append if success
            if(ec != ExceptionCode::SUCCESS)
                numExceptions++;
            else {
                assert(pyobj_res);

                // now perform adding or replacing column
                if(_columnToMapIndex < PyTuple_Size(rowObj))
                    PyTuple_SetItem(rowObj, _columnToMapIndex, pyobj_res);
                else {
                    // create a copy!
                    assert(_columnToMapIndex == PyTuple_Size(rowObj));

                    auto singleColObj = PyTuple_New(1);
                    PyTuple_SET_ITEM(singleColObj, 0, pyobj_res);
                    rowObj = PySequence_Concat(rowObj, singleColObj);
                }
                auto res = python::pythonToRow(rowObj);
                vRes.push_back(res);
            }

            Py_XDECREF(rowObj);
        }

        if(numExceptions != 0)
            Logger::instance().logger("physical planner").debug("sampling mapColumn operator lead to "
            + std::to_string(numExceptions) + " exceptions");

        python::unlockGIL();

        return vRes;
    }

    void WithColumnOperator::rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) {
       //  throw std::runtime_error("something is wrong here...");
        // get current input schema before rewrite
        auto input_schema = getInputSchema(); // this is the unrewritten one.

        // rewrite UDF & update schema
        UDFOperator::rewriteParametersInAST(rewriteMap);
        _columnToMapIndex = calcColumnToMapIndex(UDFOperator::columns(), _newColumn);
        setOutputSchema(inferSchema(input_schema, false)); // input schema is not rewritten, but parameters in AST are?
    }

    std::shared_ptr<LogicalOperator> WithColumnOperator::clone(bool cloneParents) {
        auto copy = new WithColumnOperator(cloneParents ? parent()->clone() : nullptr, UDFOperator::columns(),
                                           _newColumn, _udf, UDFOperator::rewriteMap());
        copy->setDataSet(getDataSet());
        // clone id
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }

    bool WithColumnOperator::retype(const RetypeConfiguration& conf) {

        auto input_row_type = conf.row_type;

        assert(good());

        // save old schema
        auto oldIn = getInputSchema();
        auto oldOut = getOutputSchema();

        assert(input_row_type.isTupleType());
        auto colTypes = input_row_type.parameters();

        // check that number of parameters are identical, else can't rewrite (need to project first!)
        size_t num_params_before_retype = oldIn.getRowType().parameters().size();
        size_t num_params_after_retype = colTypes.size();
        if(num_params_before_retype != num_params_after_retype) {
            throw std::runtime_error("attempting to retype " + name() + " operator, but number of parameters does not match.");
        }

        // update UDFOperator
        auto rc = UDFOperator::retype(conf);
        _columnToMapIndex = calcColumnToMapIndex(UDFOperator::columns(), _newColumn);
        if(rc) {

            // update schema with result of UDF operator.

            return true;
        } else {
            return false;
        }
    }
}