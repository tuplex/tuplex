//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/ResolveOperator.h>
#include <logical/MapColumnOperator.h>

static const size_t typeDetectionSampleSize = 5; // use 5 as default for now

namespace tuplex {

    ResolveOperator::ResolveOperator(LogicalOperator *parent,
            const ExceptionCode &ecToResolve,
            const UDF &udf,
            const std::vector<std::string>& columnNames) : UDFOperator::UDFOperator(parent, udf, columnNames) {

        // infer schema. Make sure it fits parents schema!
        setSchema(inferSchema(parent->getOutputSchema()));
        setCode(ecToResolve);
    }

    bool ResolveOperator::schemasMatch() const {
        assert(_udf.getOutputSchema() != Schema::UNKNOWN);

        auto parent = getNormalParent();

        if(!parent) {
            Logger::instance().defaultLogger().error("found no parent to which to apply resolve function");
            return false;
        }

        if(hasUDF(parent)) {
            auto udfop = dynamic_cast<UDFOperator*>(parent);

            // check that udf schemas match (up to upcasting)
            // Note: better use unifyTypes here??
            return canUpcastToRowType(_udf.getOutputSchema().getRowType(), udfop->getUDF().getOutputSchema().getRowType());
        } else {
            Logger::instance().defaultLogger().error("unsupported resolve found");
            return false;
        }
    }

    bool ResolveOperator::good() const {

        if(!schemasMatch()) {
            Logger::instance().defaultLogger().error("schema of udf is: " + _udf.getOutputSchema().getRowType().desc()
            + " but expected schema is " + parent()->getOutputSchema().getRowType().desc());
        }

        return schemasMatch();
    }

    std::vector<Row> ResolveOperator::getSample(const size_t num) const {
        // just get sample from parent
        return parent()->getSample(num);
    }

    Schema ResolveOperator::inferSchema(Schema parentSchema) {
        assert(getNormalParent());

        auto inputSchema = getNormalParent()->getInputSchema();

        // check if MapColumn Operator b.c. this operator doesn't map the full row
        if(getNormalParent()->type() == LogicalOperatorType::MAPCOLUMN) {
            auto mcop = dynamic_cast<MapColumnOperator*>(getNormalParent()); assert(mcop);
            auto colTypes = inputSchema.getRowType().parameters();
            auto hintSchema = Schema(inputSchema.getMemoryLayout(), python::Type::propagateToTupleType(colTypes[mcop->getColumnIndex()]));
            inputSchema = hintSchema;
        }

        // check if _udf is compilable
        // else, use sample to determine type
        if(_udf.isCompiled()) {
            auto normalParent = getNormalParent();

            // rewrite column access if given info and
            // we have a wide operator
            if(normalParent->type() != LogicalOperatorType::MAPCOLUMN) {
                // rewrite dict access!!!
                // remove types from UDF --> retype using parent...
                _udf.removeTypes();
                _udf.rewriteDictAccessInAST(normalParent->columns());
            }

            _udf.hintInputSchema(inputSchema);
            Logger::instance().defaultLogger().info("detected type for " + name() + " operator is " + _udf.getOutputSchema().getRowType().desc());

            if(hasUDF(normalParent)) {
                auto udfop = dynamic_cast<UDFOperator*>(normalParent);

                // if schema of resolver udf does not match *normal* parent ones, need to upcast
                // need to upcast result. E.g., could be that resolver is lambda x: 0, but something more elaborate is wanted
                if(udfop->getUDF().getOutputSchema() != _udf.getOutputSchema()) {
                    // can upcast?
                    if(canUpcastType(_udf.getOutputSchema().getRowType(), udfop->getUDF().getOutputSchema().getRowType())) {
                        // set upcasted schema!
                        _udf.setOutputSchema(udfop->getUDF().getOutputSchema());
                        return normalParent->getOutputSchema();
                    } else {
                        throw std::runtime_error("incompatible upcasting in resolve operator!");
                    }
                }
            }

            // resolve operator has SAME schema as normal parent (perform additional checks to ensure this)
            return normalParent->getOutputSchema();
        }
        else {
            auto pickledCode = _udf.getPickledCode();

            // important to get GIL for this
            python::lockGIL();

            PyObject* pFunc = python::deserializePickledFunction(python::getMainModule(), pickledCode.c_str(), pickledCode.length());
            if(!pFunc)
                return Schema::UNKNOWN;
            auto detectedType = python::detectTypeWithSample(pFunc, getSample(typeDetectionSampleSize));

            // release GIL here
            python::unlockGIL();

            // set manually for codegen type
            Schema schema = Schema(Schema::MemoryLayout::ROW, detectedType);
            _udf.setOutputSchema(schema);
            _udf.setInputSchema(inputSchema);

            std::stringstream ss;
            ss<<"detected type for " + name() + " operator using "<<typeDetectionSampleSize<<" samples as "<<detectedType.desc();
            Logger::instance().defaultLogger().info(ss.str());
            if(hasUDF(parent())) {
                UDFOperator *udfop = dynamic_cast<UDFOperator*>(parent());
                assert(udfop->getUDF().getOutputSchema() == _udf.getOutputSchema());
            }

            // resolve operator has SAME schema as normal parent (perform additional checks to ensure this)
            return getNormalParent()->getOutputSchema();
        }
    }

    LogicalOperator *ResolveOperator::clone() {
        auto copy = new ResolveOperator(parent()->clone(), ecCode(), _udf,
                                        UDFOperator::columns());
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
    }

    void ResolveOperator::rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) {
        // if mapColumn, no rewrite necessary!
        auto np = getNormalParent(); assert(np);
        if(np->type() == LogicalOperatorType::MAPCOLUMN)
            return;

        // else, need to rewrite underlying UDFop
        UDFOperator::rewriteParametersInAST(rewriteMap);

        // update schema
        if(hasUDF(np)) {
            auto udfop = dynamic_cast<UDFOperator*>(np);

            // if schema of resolver udf does not match *normal* parent ones, need to upcast
            // need to upcast result. E.g., could be that resolver is lambda x: 0, but something more elaborate is wanted
            if(udfop->getUDF().getOutputSchema() != _udf.getOutputSchema()) {
                // can upcast?
                if(canUpcastType(_udf.getOutputSchema().getRowType(), udfop->getUDF().getOutputSchema().getRowType())) {
                    // set upcasted schema!
                    _udf.setOutputSchema(udfop->getUDF().getOutputSchema());
                    setSchema(np->getOutputSchema());
                } else {
                    throw std::runtime_error("incompatible upcasting in resolve operator/rewriteDictAccess encountered.");
                }
            } else {
                setSchema(np->getOutputSchema());
            }
        }
    }
}