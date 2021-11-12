//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/MapOperator.h>
#include <cassert>
#include <UDF.h>
#include <PythonHelpers.h>
#include <ColumnReturnRewriteVisitor.h>

namespace tuplex {
    MapOperator::MapOperator(LogicalOperator *parent, const UDF &udf, const std::vector<std::string> &columnNames)
            : UDFOperator::UDFOperator(parent, udf, columnNames), _name("map") {

        assert(parent);

        // is it an empty UDF? I.e. a rename operation?
        if(udf.empty()) {
            // nothing todo, simply set schema as same. this is the same as supplying an identity function
            // lambda x: x!
            setSchema(parent->getOutputSchema());

            // also overwrite schema in udf b.c. this allows setters/getters to work
            _udf.setInputSchema(parent->getOutputSchema());
            _udf.setOutputSchema(parent->getOutputSchema());
            _outputColumns = columnNames; // set output columns to the given ones AND retrieve UDF Operator columns from parent
            UDFOperator::setColumns(parent->columns());
        } else {
            // rewrite output if it is a dictionary
            if (_udf.isCompiled()) {
                 // fetch column names if dictionary is returned...
                 auto root = _udf.getAnnotatedAST().getFunctionAST();
                 ColumnReturnRewriteVisitor rv;
                 root->accept(rv);
                 if (rv.foundColumnNames()) {
                     auto outputColumnNames = rv.columnNames;
                     // type annotator hasn't run yet , so we don't need to reset outputschema
                     _outputColumns = outputColumnNames;
                 }

                 // remove types
                 _udf.removeTypes();
            }

            // infer schema (may throw exception!) after applying UDF
            setSchema(this->inferSchema(parent->getOutputSchema()));
        }

#ifndef NDEBUG
        if(!_udf.empty())
            Logger::instance().defaultLogger().info(
                "detected output type for " + _name + " operator is " + schema().getRowType().desc());
#endif
    }

    void MapOperator::setDataSet(DataSet *dsptr) {
        // check whether schema is ok, if not set error dataset!
        if (schema().getRowType().isIllDefined())
            LogicalOperator::setDataSet(&dsptr->getContext()->makeError("schema could not be propagated successfully"));
        else
            LogicalOperator::setDataSet(dsptr);
    }

    bool MapOperator::good() const {
        if (schema().getRowType().isIllDefined()) {
            Logger::instance().defaultLogger().error("Could not infer schema for map operator.");
            return false;
        }
        return true;
    }


    std::vector<Row> MapOperator::getSample(const size_t num) const {
        // @TODO: refactor this using sample processor. It's not done in a smart way yet...
        // empty udf? take sample from parent!
        if(_udf.empty())
            return parent()->getSample(num);

        // first retrieve samples from parent
        // then, apply lambda (python version)
        // and retrieve result
        auto vSamples = parent()->getSample(num);

        auto pickledCode = _udf.getPickledCode();

        // execute pickled Code
        assert(pickledCode.length() > 0);

        std::vector<Row> vRes;
        // get GIL
        python::lockGIL();

        auto func = python::deserializePickledFunction(python::getMainModule(), pickledCode.c_str(),
                                                       pickledCode.length());

        size_t numExceptions = 0;
        for (const auto& row : vSamples) {

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
                auto res = python::pythonToRow(pyobj_res);
                vRes.push_back(res);
            }
        }

        if (numExceptions != 0)
            Logger::instance().logger("physical planner").warn(
                    "sampling map operator lead to " + std::to_string(numExceptions) + " exceptions");

        python::unlockGIL();

        return vRes;
    }

    LogicalOperator *MapOperator::clone() {
        // important to use here input column names, i.e. stored in base class UDFOperator!
        // @TODO: avoid here the costly retyping but making a faster, better clone.
        auto copy = new MapOperator(parent()->clone(),
                                    _udf,
                                    UDFOperator::columns());
        copy->setOutputColumns(_outputColumns); // account for the rewrite visitor
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        copy->setName(_name);
        assert(getID() == copy->getID());
        return copy;
    }

    void MapOperator::rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) {
        using namespace std;

        // update UDF, account for rename/empty udf case!
        UDFOperator::rewriteParametersInAST(rewriteMap);

        // update output schema
        setSchema(_udf.getOutputSchema());

        // special case, empty udf: need to update output column names
        // ==> for the others, output column names are deduced from map.
        if(_udf.empty()) {
            vector<string> cols_to_keep;
            for(int i = 0; i < _outputColumns.size(); ++i) {
                // in rewritemap? keep!
                if(rewriteMap.find(i) != rewriteMap.end())
                    cols_to_keep.emplace_back(_outputColumns[i]);
            }

            // check it matches output schema!
            assert(getOutputSchema().getRowType().isTupleType());
            assert(getOutputSchema().getRowType().parameters().size() == cols_to_keep.size());
            _outputColumns = cols_to_keep;
        }
    }

    bool MapOperator::retype(const std::vector<python::Type> &rowTypes) {

        // make sure no unknown in it!
#ifndef NDEBUG
    assert(!rowTypes.empty());
    assert(rowTypes.front().isTupleType());
    for(auto t : rowTypes.front().parameters())
        assert(t != python::Type::UNKNOWN);
#endif

        assert(good());
        assert(rowTypes.size() == 1);
        assert(rowTypes.front().isTupleType());
        auto schema = Schema(getOutputSchema().getMemoryLayout(), rowTypes.front());

        // is it an empty UDF? I.e. a rename operation?
        if(_udf.empty()) {
            // force schema
            setSchema(schema);

            // overwrite schema in udf b.c. this allows setters/getters to work
            _udf.setInputSchema(schema);
            _udf.setOutputSchema(schema);
            return true;
        } else {
            try {
                _udf.removeTypes(false);
                setSchema(this->inferSchema(schema));
                return true;
            } catch(...) {
                return false;
            }
        }
    }
}