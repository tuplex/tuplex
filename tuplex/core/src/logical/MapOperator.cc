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
#include <visitors/ColumnReturnRewriteVisitor.h>
#include "visitors/ApplyVisitor.h"
#include "visitors/ReplaceIndexToNameVisitor.h"

namespace tuplex {
    MapOperator::MapOperator(const std::shared_ptr<LogicalOperator>& parent,
                             const UDF &udf,
                             const std::vector<std::string>& outputColumnNames,
                             const std::unordered_map<size_t, size_t>& rewriteMap)
            : UDFOperator::UDFOperator(parent, udf, outputColumnNames, rewriteMap), _name("map") {

        // assert(parent);

        bool udf_well_defined = udf.hasWellDefinedTypes();
        bool typeUDF = !udf_well_defined || (parent && parent->getOutputSchema() != _udf.getInputSchema());

        if(typeUDF) {
            // is it an empty UDF? I.e. a rename operation?
            if(udf.empty()) {
                if(parent) {
                    // nothing todo, simply set schema as same. this is the same as supplying an identity function
                    // lambda x: x!
                    setOutputSchema(parent->getOutputSchema());

                    // also overwrite schema in udf b.c. this allows setters/getters to work
                    _udf.setInputSchema(parent->getOutputSchema());
                    _udf.setOutputSchema(parent->getOutputSchema());
                    UDFOperator::setColumns(parent->columns());

                }

                _outputColumns = outputColumnNames; // set output columns to the given ones AND retrieve UDF Operator columns from parent
            } else {
                // rewrite output if it is a dictionary
                if (_udf.isCompiled()) {
                    // fetch column names if dictionary is returned...
                    auto root = _udf.getAnnotatedAST().getFunctionAST();
                    ColumnReturnRewriteVisitor rv;
                    root->accept(rv);
                    if (rv.foundColumnNames()) {
                        Logger::instance().defaultLogger().warn("StructDict type will help make this easier.");
                        auto outputColumnNames = rv.columnNames;
                        // type annotator hasn't run yet , so we don't need to reset outputschema
                        _outputColumns = outputColumnNames;
                    }

                    // remove types
                    _udf.removeTypes();
                }

                if(parent) {
                    // infer schema (may throw exception!) after applying UDF
                    setOutputSchema(this->inferSchema(parent->getOutputSchema()));
                    //_udf.retype(parent->getOutputSchema().getRowType());
                    //assert(_udf.getOutputSchema() != Schema::UNKNOWN);
                    //setSchema(_udf.getOutputSchema());
                }
            }
        }

#ifndef NDEBUG
        if(!_udf.empty())
            Logger::instance().defaultLogger().info(
                "detected output type for " + _name + " operator is " + getOutputSchema().getRowType().desc());
#endif
    }

    void MapOperator::setDataSet(DataSet *dsptr) {
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

    bool MapOperator::good() const {
        if (getOutputSchema().getRowType().isIllDefined()) {
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

        auto output_columns = columns();

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
                // old: no dict unwrapping
                // auto res = python::pythonToRow(pyobj_res);
                auto res = python::pythonToRowWithDictUnwrap(pyobj_res, output_columns);
                vRes.push_back(res);
            }
        }

        if (numExceptions != 0)
            Logger::instance().logger("physical planner").warn(
                    "sampling map operator lead to " + std::to_string(numExceptions) + " exceptions");

        python::unlockGIL();

        return vRes;
    }

    std::shared_ptr<LogicalOperator> MapOperator::clone(bool cloneParents) const {
        // important to use here input column names, i.e. stored in base class UDFOperator!
        // @TODO: avoid here the costly retyping but making a faster, better clone.
        auto copy = new MapOperator(cloneParents ? parent()->clone() : nullptr,
                                    _udf,
                                    UDFOperator::columns(),
                                    UDFOperator::rewriteMap());
        copy->setOutputColumns(_outputColumns); // account for the rewrite visitor
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        copy->setName(_name);
        assert(checkBasicEqualityOfOperators(*copy, *this));
        return std::shared_ptr<LogicalOperator>(copy);
    }

    void MapOperator::rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) {
        using namespace std;

        // debug:
        {
            std::stringstream ss;
            ss<<"Operator "<<name()<<" got rewriteMap of "<<rewriteMap.size()<<" entries, calling rewriteParametersInAST now for following udf:\n";
            ss<<_udf.getCode()<<std::endl;


            Logger::instance().logger("logical").debug(ss.str());
        }

        // update UDF, account for rename/empty udf case!
        UDFOperator::rewriteParametersInAST(rewriteMap);

        // update output schema
        setOutputSchema(_udf.getOutputSchema());

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

    bool MapOperator::retype(const RetypeConfiguration& conf) {
        // assert(good());

        // there are two options:
        // 1.) it's a rename operator -> special case
        if(_udf.empty()) {
            throw std::runtime_error("rename not supported yet. Basically check if rename scheme works etc.");
        } else {

            // special case select:
            if("select" == name()) {
                // first rewrite integer literals into column names accessed
                auto columns_accessed = columns();
                if(!columns_accessed.empty()) {
                    // create new, simple UDF for this
                    auto new_udf_code = generate_python_code_for_select_columns_udf(columns_accessed);
                    auto new_udf = UDF(new_udf_code);
                    _udf = new_udf;
                    return UDFOperator::retype(conf);
                }

                throw std::runtime_error("not implemented yet, missing rewrite feature - ill-formed select?");
//                ReplaceIndexToNameVisitor rv(columns_accessed);
//                _udf.getAnnotatedAST().getFunctionAST()->accept(rv);
//
//                // rewrite UDF now
//                auto ret = UDFOperator::retype(conf);
//                return ret;
            }

            // 2.) regular
            bool ret = UDFOperator::retype(conf);

            // extract output columns / input columns from retype
            auto input_columns = UDFOperator::columns();
            //_outputColumns = _udf.extractOutputColumns();

            return ret;
        }


        throw std::runtime_error("wrong, use here the UDFOperator approach with the columns...");

        auto input_row_type = conf.row_type;
        auto is_projected_row_type = conf.is_projected;

        performRetypeCheck(input_row_type, is_projected_row_type);

        auto schema = Schema(getOutputSchema().getMemoryLayout(), input_row_type);

        // is it an empty UDF? I.e. a rename operation?
        if(_udf.empty()) {
            // force schema
            setOutputSchema(schema);

            // overwrite schema in udf b.c. this allows setters/getters to work
            _udf.setInputSchema(schema);
            _udf.setOutputSchema(schema);
            return true;
        } else {
            try {
                _udf.removeTypes(false);
                _udf.rewriteDictAccessInAST(inputColumns());
                setOutputSchema(this->inferSchema(schema, is_projected_row_type));
                return true;
            } catch(...) {
                return false;
            }
        }
    }
}