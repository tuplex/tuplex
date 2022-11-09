//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/UDFOperator.h>

namespace tuplex {

    static const size_t typeDetectionSampleSize = 5; // use 5 as default for now

    inline bool containsConstantType(const python::Type& type) {
        if(type.isConstantValued())
            return true;
        if(type.isTupleType()) {
            for(auto param : type.parameters())
                if(containsConstantType(param))
                    return true;
            return false;
        } else {
            // unsupported types...
            // @TODO....
            return false;
        }
    }

    UDFOperator::UDFOperator(const std::shared_ptr<LogicalOperator>& parent, const UDF& udf,
    const std::vector<std::string>& columnNames,
    const std::unordered_map<size_t, size_t>& rewriteMap) : LogicalOperator::LogicalOperator(parent), _udf(udf), _columnNames(columnNames), _rewriteMap(rewriteMap) {
        // assert(parent);
    }

    bool UDFOperator::retype(const tuplex::RetypeConfiguration &conf) {
        // apply a new typing to the existing UDF.
        auto& logger = Logger::instance().logger("typer");

        // first: check whether column names are different, if so apply!
        if(!conf.columns.empty()) {
            // update internal column names & rewrite UDF accordingly
            _columnNames = conf.columns;
            _udf.rewriteDictAccessInAST(_columnNames);
        }

        // remove types & rewrite
        try {
            _udf.removeTypes(false);
            bool success = _udf.retype(conf.row_type);

            // check what the return type is. If it is of exception type, try to use a sample to get rid off branches that are off
            if(!success || _udf.getOutputSchema().getRowType().isExceptionType()) {
                if(success && _udf.getOutputSchema().getRowType().isExceptionType())
                    logger.debug("static retyping resulted in UDF producing always exceptions. Try hinting with sample...");
                Timer s_timer;
                auto rows_sample = parent()->getPythonicSample(MAX_TYPE_SAMPLING_ROWS);
                logger.debug("retrieving pythonic sample took: " + std::to_string(s_timer.time()) + "s");
                _udf.removeTypes(false);
                success = _udf.hintSchemaWithSample(rows_sample,
                                                    conf.row_type, true);
                if(success) {
                    if(_udf.getCompileErrors().empty() || _udf.getReturnError() != CompileError::COMPILE_ERROR_NONE) {
                        // @TODO: add the constant folding for the other scenarios as well!
                        if(containsConstantType(conf.row_type)) {
                            _udf.optimizeConstants();
                        }
                    }
                } else {
                    logger.debug("no success typing UDF - even with sample.");
                }
            }

            if(!success)
                return false;

            // update schemas accordingly
            setOutputSchema(_udf.getOutputSchema());
        } catch(const std::exception& e) {
            logger.debug(std::string("retype failed because of exception: ") + e.what());
            return false;
        } catch(...) {
            return false;
        }
        return true;
    }


    Schema UDFOperator::inferSchema(Schema parentSchema, bool is_projected_schema) {

        auto& logger = Logger::instance().defaultLogger();

        if(parentSchema == Schema::UNKNOWN)
            parentSchema = parent()->getOutputSchema();

        if(_udf.empty())
            return _udf.getOutputSchema();

        // check if _udf is compilable
        // else, use sample to determine type
        if(_udf.isCompiled()) {

            // schema already defined within udf? => use output schema!
            if(_udf.getInputSchema() != Schema::UNKNOWN && _udf.getOutputSchema() != Schema::UNKNOWN)
                return _udf.getOutputSchema();

            // // reset udf for rewrite (schema etc. may have changed)
            // _udf.resetAST();

            // if column names exist, attempt rewrite
            if(!_udf.rewriteDictAccessInAST(_columnNames))
                return Schema::UNKNOWN;

            // 3-stage typing
            // 1. try to type statically by simply annotating the AST
            logger.debug("performing static typing for UDF in operator " + name());
            bool success = _udf.hintInputSchema(parentSchema, false, false);

            // check what the return type is. If it is of exception type, try to use a sample to get rid off branches that are off
            if(success && _udf.getOutputSchema().getRowType().isExceptionType()) {
                logger.debug("static typing resulted in UDF producing always exceptions. Try hinting with sample...");
                Timer s_timer;
                auto rows_sample = parent()->getPythonicSample(MAX_TYPE_SAMPLING_ROWS);
                logger.info("retrieving pythonic sample took: " + std::to_string(s_timer.time()) + "s");
                _udf.removeTypes(false);
                success = _udf.hintSchemaWithSample(rows_sample,
                                                    parentSchema.getRowType(), true);
                if(success) {
                    if(_udf.getCompileErrors().empty() || _udf.getReturnError() != CompileError::COMPILE_ERROR_NONE) {
                        // @TODO: add the constant folding for the other scenarios as well!
                        if(containsConstantType(parentSchema.getRowType())) {
                            _udf.optimizeConstants();
                        }
                    }
                    // if unsupported types presented, use sample to determine type and use fallback mode (except for list return type error, only print error messages for now)
                    return _udf.getOutputSchema();
                }
                logger.debug("no success hinting, trying out different hint modes...");
                success = false;
            }

            if(!success) {
                // in debug, print compile errors.
                logger.debug("static typing failed because of:\n  -- " + _udf.getCompileErrorsAsStr());
                _udf.clearCompileErrors();
                // 2. try by annotating with if-blocks getting ignored statically...
                logger.debug("performing static typing with partially ignoring branches for UDF in operator " + name());
                _udf.removeTypes(false);
                success = _udf.hintInputSchema(parentSchema, true, false);
                if(!success && _udf.getCompileErrors().empty()) {
                    // 3. type by tracing a small sample from the parent!
                    // => only use rows which match parent type.
                    // => general case rows thus get transferred to interpreter...
                    logger.debug("performing traced typing for UDF in operator " + name());
                    auto rows_sample = parent()->getPythonicSample(MAX_TYPE_SAMPLING_ROWS);
                    _udf.removeTypes(false);
                    success = _udf.hintSchemaWithSample(rows_sample,
                                                        parentSchema.getRowType(), true);

                    // only exceptions?
                    // => report, abort compilation!
                    if(_udf.getCompileErrors().empty()) {
                        if(!success) {

                            // check if output type is exception type -> this means sample produced only errors!
                            if(_udf.getOutputSchema().getRowType().isExceptionType()) {
                                std::stringstream ss;
                                ss<<"Sample of " + pluralize(rows_sample.size(), "row") + " produced only exceptions of type " +_udf.getOutputSchema().getRowType().desc() + ". Either increase sample size so rows producing no exceptions are in the majority, or change user-defined code.\n";

                                // produce a sample traceback for user feedback... -> this should probably go to python API display??
                                // @TODO

                                logger.error(ss.str());
                                return Schema::UNKNOWN;
                            } else {
                                Logger::instance().defaultLogger().info("was not able to detect type for UDF, statically and via tracing."
                                                                        " Provided parent row type was " + parentSchema.getRowType().desc() + ". Marking UDF as uncompilable which will require interpreter for execution.");
                                // 4. mark as uncompilable UDF, but sample output row type, so at least subsequent operators
                                //    can get compiled!
                                _udf.markAsNonCompilable();

                                return Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::PYOBJECT));
                                // throw std::runtime_error("TODO: implement via sampling of Python UDF executed most likely type, so pipeline can still get compiled.");
                            }
                        } else {
                            // check what return type was given
                            auto output_type = _udf.getOutputSchema().getRowType();
                            if(output_type.isExceptionType()) {
                                _udf.markAsNonCompilable();
                                // exception type? => sample produced only exceptions! warn user.
                                Logger::instance().defaultLogger().error("Tracing via sample produced mostly"
                                                                         " exceptions, please increase sample size"
                                                                         " or alter UDF to not yield exceptions only.");
                                // @TODO: print nice traceback with sample...
                                // => should use beefed up sample processor class for this...
                                return Schema::UNKNOWN;
                            } else {
                                // all good, keep sampled type but mark as non compilable
                                // cannot statically type AST, but sampling yields common-case output type to propagate to subsequent stages
                            }
                        }
                    }
                }
            }

            if(_udf.getCompileErrors().empty() || _udf.getReturnError() != CompileError::COMPILE_ERROR_NONE) {
                // @TODO: add the constant folding for the other scenarios as well!
                if(containsConstantType(parentSchema.getRowType())) {
                    _udf.optimizeConstants();
                }

                // if unsupported types presented, use sample to determine type and use fallback mode (except for list return type error, only print error messages for now)
                return _udf.getOutputSchema();
            }

            // unsupported type exists, print warning
            _udf.markAsNonCompilable();
            for (const auto& err : _udf.getCompileErrors()) {
                Logger::instance().defaultLogger().error(_udf.compileErrorToStr(err));
            }
            Logger::instance().defaultLogger().warn("will use fallback mode");
        }

        // @Todo: support here dict syntax...
        // @TODO: this feels redundant, i.e. tracerecord visitor should have already dealt with this...

        auto pickledCode = _udf.getPickledCode();
        auto processed_sample = getSample(typeDetectionSampleSize);

        std::vector<python::Type> retrieved_types;
        for(auto r : processed_sample)
            retrieved_types.push_back(r.getRowType());

        // empty types? return unknown.
        if(retrieved_types.empty()) {
            Logger::instance().defaultLogger().debug("no types could be retrieved, using Schema unknown.");
            return Schema::UNKNOWN;
        }

        auto detectedType = mostFrequentItem(retrieved_types);

        // set manually for codegen type
        Schema schema = Schema(Schema::MemoryLayout::ROW, detectedType);
        _udf.setOutputSchema(schema);
        _udf.setInputSchema(parentSchema);

        std::stringstream ss;
        ss<<"detected type for " + name() + " operator using "<<typeDetectionSampleSize<<" samples as "<<detectedType.desc();
        Logger::instance().defaultLogger().info(ss.str());
        return schema;
    }

    bool hasUDF(const LogicalOperator *op) {
        if(!op)
            return false;

        switch(op->type()) {
            case LogicalOperatorType::MAP:
            case LogicalOperatorType::MAPCOLUMN:
            case LogicalOperatorType::WITHCOLUMN:
            case LogicalOperatorType::FILTER:
            case LogicalOperatorType::RESOLVE:
                // check type integrity
                assert(dynamic_cast<const UDFOperator*>(op));
                return true;
            default:
                // check type integrity
                assert(!dynamic_cast<const UDFOperator*>(op));
                return false;
        }
    }

    void UDFOperator::projectColumns(const std::unordered_map<size_t, size_t> &rewriteMap) {
        // need to update columns
        std::vector<std::string> columnsToRetain;
        std::vector<bool> columnsVisited;
        columnsToRetain.reserve(rewriteMap.size());
        for(int i = 0; i < rewriteMap.size(); ++i) {
            columnsToRetain.emplace_back("");
            columnsVisited.emplace_back(false);
        }

        // add columns
        for(auto keyVal : rewriteMap) {
            // index, but only those who fall in range
            if(keyVal.first < _columnNames.size()) {
                columnsToRetain[keyVal.second] = _columnNames[keyVal.first];
                columnsVisited[keyVal.second] = true;
            }
        }

        // find max visited column
        int max_idx = 0;
        while(max_idx < columnsVisited.size() && columnsVisited[max_idx])
            ++max_idx;

        // overwrite columns + mapping index
        _columnNames = std::vector<std::string>(columnsToRetain.begin(), columnsToRetain.begin() + max_idx);
    }

    void UDFOperator::rewriteParametersInAST(const std::unordered_map<size_t, size_t> &rewriteMap) {
       _rewriteMap = rewriteMap;
       projectColumns(rewriteMap);
        _udf.rewriteParametersInAST(rewriteMap);
    }

    bool UDFOperator::performRetypeCheck(const python::Type& input_row_type, bool is_projected_row_type) {
        assert(input_row_type.isTupleType());
        auto colTypes = input_row_type.parameters();

        // check that number of parameters are identical, else can't rewrite (need to project first!)
        auto input_t = UDFOperator::getInputSchema().getRowType().desc();

        // could be unknown => then retype always! If not, check correct type is given.
        if(UDFOperator::getInputSchema() != Schema::UNKNOWN) {
            size_t num_params_before_retype = rewriteMap().size(); //UDFOperator::getInputSchema().getRowType().parameters().size();
            size_t num_params_after_retype = colTypes.size();
            if(!rewriteMap().empty() && num_params_before_retype != num_params_after_retype) {
                throw std::runtime_error("attempting to retype " + name() + " operator, but number of parameters does not match.");
                return false;
            }
        } else {
            // check parent
            if(parent() && parent()->getOutputSchema() != Schema::UNKNOWN) {
                size_t num_params_before_retype = parent()->getOutputSchema().getRowType().parameters().size();
                size_t num_params_after_retype = colTypes.size();
                if(num_params_before_retype != num_params_after_retype) {
                    throw std::runtime_error("attempting to retype " + name() + " operator, but number of parameters does not match.");
                    return false;
                }
            }
        }
        return true;
    }
}