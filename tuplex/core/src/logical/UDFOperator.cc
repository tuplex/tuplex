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


    UDFOperator::UDFOperator(LogicalOperator* parent, const UDF& udf,
    const std::vector<std::string>& columnNames) : LogicalOperator::LogicalOperator(parent), _udf(udf), _columnNames(columnNames) {
        assert(parent);
    }


    Schema UDFOperator::inferSchema(Schema parentSchema) {

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

            // if column names exist, attempt rewrite
            if(!_udf.rewriteDictAccessInAST(_columnNames))
                return Schema::UNKNOWN;

            // 3-stage typing
            // 1. try to type statically by simply annotating the AST
            logger.info("performing static typing for UDF in operator " + name());
            bool success = _udf.hintInputSchema(parentSchema, false, false);
            if(!success) {

                _udf.clearCompileErrors();
                // 2. try by annotating with if-blocks getting ignored statically...
                logger.info("performing static typing with partially ignoring branches for UDF in operator " + name());
                success = _udf.hintInputSchema(parentSchema, true, false);
                if(!success) {
                    _udf.clearCompileErrors();
                    // 3. type by tracing a small sample from the parent!
                    // => only use rows which match parent type.
                    // => general case rows thus get transferred to interpreter...
                    logger.info("performing traced typing for UDF in operator " + name());
                    success = _udf.hintSchemaWithSample(parent()->getPythonicSample(MAX_TYPE_SAMPLING_ROWS),
                                                        parentSchema.getRowType(), true);

                    // only exceptions?
                    // => report, abort compilation!
                    if(_udf.getCompileErrors().empty()) {
                        if(!success) {
                            Logger::instance().defaultLogger().info("was not able to detect type for UDF, statically and via tracing."
                                                                    " Provided parent row type was " + parentSchema.getRowType().desc() );
                            // 4. mark as uncompilable UDF, but sample output row type, so at least subsequent operators
                            //    can get compiled!
                            _udf.markAsNonCompilable();
                            throw std::runtime_error("TODO: implement via sampling of Python UDF executed most likely type, so pipeline can still get compiled.");
                            return Schema::UNKNOWN;
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
                // if unsupported types presented, use sample to determine type and use fallback mode (except for list return type error, only print error messages for now)
                return _udf.getOutputSchema();
            }

            // unsupported type exists, print warning
            _udf.markAsNonCompilable();
            for (const auto& err : _udf.getCompileErrors()) {
                Logger::instance().defaultLogger().error(_udf.compileErrorToStr(err));
            }
            Logger::instance().defaultLogger().error("will use fallback mode");
        }

        // @Todo: support here dict syntax...
        // @TODO: this feels redundant, i.e. tracerecord visitor should have already dealt with this...

        auto pickledCode = _udf.getPickledCode();
        auto processed_sample = getSample(typeDetectionSampleSize);

        std::vector<python::Type> retrieved_types;
        for(auto r : processed_sample)
            retrieved_types.push_back(r.getRowType());
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

    bool hasUDF(const LogicalOperator* op) {
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

       projectColumns(rewriteMap);
        _udf.rewriteParametersInAST(rewriteMap);

        // update schema
        //_schema = inferSchema();
    }
}