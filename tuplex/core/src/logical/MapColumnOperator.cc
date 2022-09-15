//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/MapColumnOperator.h>

namespace tuplex {
    MapColumnOperator::MapColumnOperator(const std::shared_ptr<LogicalOperator>& parent, const std::string &columnName, const std::vector<std::string>& columns,
                                         const tuplex::UDF &udf,
                                         const std::unordered_map<size_t, size_t>& rewriteMap) : UDFOperator::UDFOperator(parent, udf, columns, rewriteMap), _columnToMap(columnName) {

        _columnToMapIndex = indexInVector(columnName, columns);
        assert(_columnToMapIndex >= 0);

        setSchema(inferSchema(parent->getOutputSchema(), false));

//#ifndef NDEBUG
//        Logger::instance().defaultLogger().info("detected output type for " + name() + " operator is " + schema().getRowType().desc());
//#endif
    }


#warning "make sure that that mapColumn function is compatible!!!!"

    Schema MapColumnOperator::inferSchema(Schema parentSchema, bool is_projected_row_type) {
        // ATTENTION!!!
        // in map column operator NO column rewrite will be undertaken
        // in withColumn it will be though...

        // this is very similar to UDFOperator schema, however, there is a difference
        // map column takes a single parameter and maps to single type
        // schema is then combined!
        assert(_columnToMapIndex >= 0);

        assert(parentSchema.getRowType().isTupleType());

        auto colTypes = parentSchema.getRowType().parameters();
        assert(_columnToMapIndex < colTypes.size());

        // exchange at columnToMapIndex type

        if(_udf.isCompiled()) {
            // note: there is NO UDF rewrite here, because single element is used.
            // ==> wrong usage should be indicated.

            auto hintSchema = Schema(parentSchema.getMemoryLayout(), python::Type::propagateToTupleType(colTypes[_columnToMapIndex]));
            if(!_udf.hintInputSchema(hintSchema))
                throw std::runtime_error("could not hint input schema for mapColumn operator. Please check semantics!");

            // check what type udf returns
            auto udfResType = _udf.getOutputSchema().getRowType();

            assert(udfResType.isTupleType());
            // single element? or multiple?
            if(udfResType.parameters().size() == 1)
                colTypes[_columnToMapIndex] = udfResType.parameters().front();
            else
                colTypes[_columnToMapIndex] = udfResType;

        } else {
            // not compilable, i.e. use pickled version
            Logger::instance().defaultLogger().error("mapColumn for uncompilable funcs not supported yet");
            std::exit(1);
        }

        auto retType = python::Type::makeTupleType(colTypes);

        // Logger::instance().defaultLogger().info("detected type for " + name() + " operator is " + retType.desc());

        return Schema(parentSchema.getMemoryLayout(), retType);
    }

    void MapColumnOperator::setDataSet(tuplex::DataSet *dsptr) {
        // check whether schema is ok, if not set error dataset!
        if(schema().getRowType().isIllDefined())
            LogicalOperator::setDataSet(&dsptr->getContext()->makeError("schema could not be propagated successfully"));
        else
            LogicalOperator::setDataSet(dsptr);
    }

    std::vector<Row> MapColumnOperator::getSample(const size_t num) const {
        auto vSamples = parent()->getSample(num);
        auto pickledCode = _udf.getPickledCode();

        assert(pickledCode.length() > 0);

        std::vector<Row> vRes;

        // get GIL
        python::lockGIL();

        auto func = python::deserializePickledFunction(python::getMainModule(),
                                                       pickledCode.c_str(), pickledCode.length());

        size_t numExceptions = 0;
        for(const auto& row : vSamples) {

            auto rowObj = python::rowToPython(row);

            // object should be a tuple, get column
            assert(PyTuple_Check(rowObj));
            assert(_columnToMapIndex < PyTuple_Size(rowObj));

            auto colObj = PyTuple_GetItem(rowObj, _columnToMapIndex);
            Py_XINCREF(colObj);

            // call python function
            ExceptionCode ec;

            // put it in a single tuple
            auto arg = PyTuple_New(1); PyTuple_SET_ITEM(arg, 0, colObj);
            auto pyobj_res = python::callFunction(func, arg, ec);

            // only append if success
            if(ec != ExceptionCode::SUCCESS)
                numExceptions++;
            else {
                PyTuple_SetItem(rowObj, _columnToMapIndex, pyobj_res);
                auto res = python::pythonToRow(rowObj);
                vRes.push_back(res);
            }

            Py_XDECREF(rowObj);
        }

        if(numExceptions != 0)
            Logger::instance().logger("physical planner").warn("sampling mapColumn operator lead to " + std::to_string(numExceptions) + " exceptions");

        python::unlockGIL();

        return vRes;
    }


    void MapColumnOperator::rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) {
        // throw std::runtime_error("not sure what's going on here...");
        if(rewriteMap.find(_columnToMapIndex) != rewriteMap.end())
            _columnToMapIndex = rewriteMap.at(_columnToMapIndex);
        else
            throw std::runtime_error("something wrong here...");

        // update column names
        projectColumns(rewriteMap);

        // update schema
        setSchema(inferSchema(parent()->getOutputSchema(), false));
    }

    std::shared_ptr<LogicalOperator> MapColumnOperator::clone(bool cloneParents) {
        auto copy = new MapColumnOperator(cloneParents ? parent()->clone() : nullptr, _columnToMap,
                                          UDFOperator::columns(), _udf, UDFOperator::rewriteMap());
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }

    bool MapColumnOperator::retype(const python::Type& input_row_type, bool is_projected_row_type) {
        assert(good());

        // save old schema
        auto oldIn = getInputSchema();
        auto oldOut = getOutputSchema();

        // infer new schema using one row type
        assert(input_row_type.isTupleType());
        auto colTypes = input_row_type.parameters();

        // check that number of parameters are identical, else can't rewrite (need to project first!)
        auto old_input_type = oldIn.getRowType().parameters().at(_columnToMapIndex);
        size_t num_params_before_retype = oldIn.getRowType().parameters().size();
        size_t num_params_after_retype = colTypes.size();
        if(num_params_before_retype != num_params_after_retype) {
            throw std::runtime_error("attempting to retype " + name() + " operator, but number of parameters does not match.");
        }

        python::Type udfResType = python::Type::UNKNOWN;
        auto memLayout = oldOut.getMemoryLayout();
        _udf.removeTypes(false);
        if(_udf.isCompiled()) {
            // note: there is NO UDF rewrite here, because single element is used.
            // ==> wrong usage should be indicated.

            auto hintSchema = Schema(memLayout, python::Type::propagateToTupleType(colTypes[_columnToMapIndex]));
            if(!_udf.hintInputSchema(hintSchema))
                throw std::runtime_error("could not hint input schema for mapColumn operator. Please check semantics!");

            // check what type udf returns
            udfResType = _udf.getOutputSchema().getRowType();

            assert(udfResType.isTupleType());
            // single element? or multiple?
            if(udfResType.parameters().size() == 1)
                colTypes[_columnToMapIndex] = udfResType.parameters().front();
            else
                colTypes[_columnToMapIndex] = udfResType;

        } else {
            // not compilable, i.e. use pickled version
            Logger::instance().defaultLogger().error("mapColumn for uncompilable funcs not supported yet");
            std::exit(1);
        }

        // success?
        if(udfResType != python::Type::UNKNOWN) {
            // set schema
            setSchema(Schema(memLayout, python::Type::makeTupleType(colTypes)));
            return true;
        } else {
            setSchema(oldOut);
            return false;
        }

    }
}