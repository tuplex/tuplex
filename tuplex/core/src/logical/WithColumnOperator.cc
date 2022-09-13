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
                                           const std::string &columnName, const UDF &udf): UDFOperator::UDFOperator(parent, udf, columnNames), _newColumn(columnName) {

        // define index
        _columnToMapIndex = calcColumnToMapIndex(columnNames, columnName);

        // infer schema, this is slightly more involved...
        setSchema(inferSchema(parent->getOutputSchema()));
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

    Schema WithColumnOperator::inferSchema(Schema parentSchema) {

        if(parentSchema == Schema::UNKNOWN)
            parentSchema = getInputSchema();

        auto inputColumnNames = UDFOperator::inputColumns();

        // detect schema of UDF
        auto udfSchema = UDFOperator::inferSchema(parentSchema);

        // now it's time to unpack or not
        auto udfRetRowType = udfSchema.getRowType();
        auto retType = udfRetRowType;

        // unknown or ill-defined?
        if(retType == python::Type::UNKNOWN || retType.isIllDefined()) {
            auto& logger = Logger::instance().logger("logical plan");
            logger.error("failed to type withColumn operator");
            return Schema::UNKNOWN;
        }


        assert(udfRetRowType.isTupleType());
        if(udfRetRowType.parameters().size() == 1)
            retType = retType.parameters().front();

        // create new rowtype depending on output and where column is
        auto inParameters = parentSchema.getRowType().parameters();

        // tuple unpack?
        if(inParameters.size() == 1 && inParameters.front().isTupleType())
            inParameters = inParameters.front().parameters();

        if(!inputColumnNames.empty())
            assert(inputColumnNames.size() == inParameters.size());

        if(_columnToMapIndex < inParameters.size())
            inParameters[_columnToMapIndex] = retType;
        else
            inParameters.emplace_back(retType);
        return Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType(inParameters));
    }

    void WithColumnOperator::setDataSet(tuplex::DataSet *dsptr) {
        // check whether schema is ok, if not set error dataset!
        if(schema().getRowType().isIllDefined())
            LogicalOperator::setDataSet(&dsptr->getContext()->makeError("schema could not be propagated successfully"));
        else
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
        // rewrite UDF & update schema
        UDFOperator::rewriteParametersInAST(rewriteMap);
        _columnToMapIndex = calcColumnToMapIndex(UDFOperator::columns(), _newColumn);
        setSchema(inferSchema(parent()->getOutputSchema()));
    }

    std::shared_ptr<LogicalOperator> WithColumnOperator::clone(bool cloneParents) {
        auto copy = new WithColumnOperator(cloneParents ? parent()->clone() : nullptr, UDFOperator::columns(),
                                           _newColumn, _udf);
        copy->setDataSet(getDataSet());
        // clone id
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }

    bool WithColumnOperator::retype(const std::vector<python::Type> &rowTypes) {

        assert(good());

        // save old schema
        auto oldIn = getInputSchema();
        auto oldOut = getOutputSchema();

        // infer new schema using one row type
        assert(rowTypes.size() == 1);
        assert(rowTypes[0].isTupleType());

        // reset udf
        _udf.removeTypes(false);

        // infer schema with given type
        auto schema = inferSchema(Schema(oldOut.getMemoryLayout(), rowTypes.front()));
        if(schema == Schema::UNKNOWN) {
            return false;
        } else {
            setSchema(schema);
            return true;
        }
    }
}