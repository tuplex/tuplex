//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/codegen/StageBuilder.h>
#include <physical/codegen/PythonPipelineBuilder.h>
#include <physical/codegen/PipelineBuilder.h>
#include <physical/PhysicalPlan.h>
#include <physical/codegen/BlockBasedTaskBuilder.h>
#include <physical/codegen/CellSourceTaskBuilder.h>
#include <physical/codegen/JITCSVSourceTaskBuilder.h>
#include <physical/codegen/TuplexSourceTaskBuilder.h>
#include <physical/codegen/ExceptionSourceTaskBuilder.h>
#include <physical/codegen/AggregateFunctions.h>
#include <logical/CacheOperator.h>
#include <JSONUtils.h>
#include <CSVUtils.h>
#include <Utils.h>
#include <string.h>
#include <logical/AggregateOperator.h>

#include <limits.h>

// New: Stage Specialization, maybe rename?
#include <physical/codegen/StagePlanner.h>
#include <physical/codegen/CodeGenerationContext.h>

// @TODO: code gen needs to be lazily done
// i.e. codegen stages then execute
// => if normal case passes, then codegen the big pipeline!
// ==> makes it a bit more complicated, but hey JIT compilation for the rule!


#ifndef NDEBUG
// verbose output when building
#define VERBOSE_BUILD
#endif

namespace tuplex {
    namespace codegen {

        StageBuilder::StageBuilder(int64_t stage_number,
                                   bool rootStage,
                                   bool allowUndefinedBehavior,
                                   bool generateParser,
                                   double normalCaseThreshold,
                                   bool sharedObjectPropagation,
                                   bool nullValueOptimization,
                                   bool constantFoldingOptimization,
                                   bool updateInputExceptions,
                                   bool generateSpecializedNormalCaseCodePath)
                : _stageNumber(stage_number), _isRootStage(rootStage), _allowUndefinedBehavior(allowUndefinedBehavior),
                  _generateParser(generateParser), _normalCaseThreshold(normalCaseThreshold), _sharedObjectPropagation(sharedObjectPropagation),
                  _nullValueOptimization(nullValueOptimization),
                  _constantFoldingOptimization(constantFoldingOptimization),
                  _updateInputExceptions(updateInputExceptions),
                  _inputNode(nullptr),
                  _outputLimit(std::numeric_limits<size_t>::max()),
                  _generateNormalCaseCodePath(generateSpecializedNormalCaseCodePath) {
        }

        inline std::string next_hashmap_name() {
            static int counter = 0;
            return "hashmap" + fmt::format("{:02d}", counter++);
        }

        StageBuilder::PythonCodePath StageBuilder::generatePythonCode(const CodeGenerationContext& ctx,
                                                        int stageNo) {

            PythonCodePath path;

            // go over all operators and generate python-fallback pipeline code to be purely executed within the interpreter
            std::string funcName = "pipeline_stage_" + std::to_string(stageNo);
            path.pyPipelineName = funcName;
            PythonPipelineBuilder ppb(funcName);

            if(!ctx.slowPathContext.valid())
                throw std::runtime_error("python code is generated from slow-path context,"
                                         " make sure to have it set in ctx object.");

            // check what input type of Stage is
            if(ctx.inputMode == EndPointMode::FILE) {
                auto fop = std::dynamic_pointer_cast<FileInputOperator>(ctx.slowPathContext.inputNode); assert(fop);
                switch (ctx.inputFileFormat) {
                    case FileFormat::OUTFMT_CSV: {
                        ppb.cellInput(ctx.slowPathContext.inputNode->getID(),
                                      fop->inputColumns(), fop->null_values(),
                                      fop->typeHints(),
                                      fop->inputColumnCount(),
                                      fop->projectionMap());
                        break;
                    }
                    case FileFormat::OUTFMT_TEXT:
                    case FileFormat::OUTFMT_ORC: {
                        ppb.objInput(fop->getID(), fop->inputColumns());
                        break;
                    }
                    default:
                        throw std::runtime_error("file input format not yet supported!");
                }
            } else {
                ppb.objInput(ctx.slowPathContext.inputNode->getID(),
                             ctx.slowPathContext.inputNode->inputColumns());
            }

            for (const auto &op : ctx.slowPathContext.operators) {
                switch (op->type()) {
                    case LogicalOperatorType::PARALLELIZE: {
                        ppb.objInput(op->getID(), op->columns());
                        break;
                    }
                    case LogicalOperatorType::FILEINPUT: {
                        auto fileop = std::dynamic_pointer_cast<FileInputOperator>(op);
                        if (fileop->fileFormat() == FileFormat::OUTFMT_CSV) {
                            // use cells, b.c. parser already has string contents.
                            ppb.cellInput(op->getID(), op->columns(), fileop->null_values(), fileop->typeHints(),
                                          fileop->inputColumnCount(), fileop->projectionMap());
                        } else if (fileop->fileFormat() == FileFormat::OUTFMT_TEXT) {
                            // text pipeline is the same with forced string type!
                            ppb.cellInput(op->getID(), op->columns(), fileop->null_values(), {{0, python::Type::STRING}}, 1);
                            Logger::instance().defaultLogger().warn("accessing untested feature in Tuplex");
                        } else {
                            throw std::runtime_error("Unsupported file input type!");
                        }
                        break;
                    }
                    case LogicalOperatorType::MAP: {
                        auto udfop = dynamic_cast<UDFOperator *>(op.get()); assert(udfop);
                        ppb.mapOperation(op->getID(), udfop->getUDF(), udfop->columns());
                        break;
                    }
                    case LogicalOperatorType::FILTER: {
                        ppb.filterOperation(op->getID(), dynamic_cast<UDFOperator *>(op.get())->getUDF());
                        break;
                    }
                    case LogicalOperatorType::MAPCOLUMN: {
                        ppb.mapColumn(op->getID(), dynamic_cast<MapColumnOperator *>(op.get())->columnToMap(),
                                      dynamic_cast<UDFOperator *>(op.get())->getUDF());
                        break;
                    }
                    case LogicalOperatorType::WITHCOLUMN: {
                        ppb.withColumn(op->getID(), dynamic_cast<WithColumnOperator *>(op.get())->columnToMap(),
                                       dynamic_cast<UDFOperator *>(op.get())->getUDF());
                        break;
                    }
                    case LogicalOperatorType::RESOLVE: {
                        ppb.resolve(op->getID(), dynamic_cast<ResolveOperator *>(op.get())->ecCode(),
                                    dynamic_cast<UDFOperator *>(op.get())->getUDF());
                        break;
                    }
                    case LogicalOperatorType::IGNORE: {
                        ppb.ignore(op->getID(), dynamic_cast<IgnoreOperator *>(op.get())->ecCode());
                        break;
                    }
                    case LogicalOperatorType::FILEOUTPUT: {
                        auto fop = dynamic_cast<FileOutputOperator *>(op.get());
                        switch (fop->fileFormat()) {
                            case FileFormat::OUTFMT_CSV: {
                                ppb.csvOutput();
                                break;
                            }
                            default:
                                throw std::runtime_error(
                                        "unknown fileformat encountered. Can't generate pure python pipeline.");
                        }
                    }
                    case LogicalOperatorType::CACHE: {
                        // simply output the python objects
                        // ignore, output is done below...
                        // note: cache can be both FIRST and LAST operator in a stage...
                        break;
                    }
                    case LogicalOperatorType::AGGREGATE: {
                        assert(op == ctx.slowPathContext.operators.back()); // make sure it's the last one
                        // usually it's a hash aggregate, so python output.
                        ppb.pythonOutput();
                        break;
                    }
                    case LogicalOperatorType::TAKE: {
                        assert(op == ctx.slowPathContext.operators.back()); // make sure it's the last one
                        ppb.tuplexOutput(op->getID(), op->getOutputSchema().getRowType());
                        break;
                    }

                    case LogicalOperatorType::JOIN: {

                        // only inner & left join yet supported
                        auto jop = dynamic_cast<JoinOperator*>(op.get()); assert(jop);

                        // TODO test this out, seems rather quick yet
                        auto leftColumn = jop->buildRight() ? jop->leftColumn().value_or("") : jop->rightColumn().value_or("");
                        auto bucketColumns = jop->bucketColumns();
                        if(jop->joinType() == JoinType::INNER) {
                            ppb.innerJoinDict(jop->getID(), next_hashmap_name(),
                                              leftColumn, bucketColumns,
                                              jop->leftPrefix(), jop->leftSuffix(), jop->rightPrefix(), jop->rightSuffix());
                        } else if(jop->joinType() == JoinType::LEFT) {
                            ppb.leftJoinDict(jop->getID(), next_hashmap_name(), leftColumn, bucketColumns,
                                             jop->leftPrefix(), jop->leftSuffix(), jop->rightPrefix(), jop->rightSuffix());
                        } else {
                            throw std::runtime_error("right join not yet supported!");
                        }

                        break;
                    }

                    default: {
                        throw std::runtime_error(
                                "unknown operator '" + op->name() + "' in generatePythonCode() encountered");
                    }
                }
            }

            // output mode?
            if(ctx.outputMode == EndPointMode::FILE && ctx.outputFileFormat == FileFormat::OUTFMT_CSV) {
                // pip->buildWithCSVRowWriter(_funcMemoryWriteCallbackName, _outputNodeID, _fileOutputParameters["null_value"],
                //                                                       true, csvOutputDelimiter(), csvOutputQuotechar());
                ppb.csvOutput(ctx.csvOutputDelimiter(), ctx.csvOutputQuotechar());
            } else {
                // hashing& Co has to be done with the intermediate object.
                // no code injected here. Do it whenever the python codepath is called.
                ppb.pythonOutput();
            }

            path.pyCode = ppb.getCode();
            return path;
        }

        void StageBuilder::addFileInput(const std::shared_ptr<FileInputOperator> &csvop) {

            // add a csvoperator & fetch all info
            assert(_fileInputParameters.empty());

            // fetch file input columns from original schema (before projection pushdown!)
            _fileInputParameters["numInputColumns"] = std::to_string(
                    csvop->getInputSchema().getRowType().parameters().size()); // actual count of input columns, not the projected out count!
            _fileInputParameters["hasHeader"] = boolToString(csvop->hasHeader());
            _fileInputParameters["delimiter"] = char2str(csvop->delimiter());
            _fileInputParameters["quotechar"] = char2str(csvop->quotechar());
            _fileInputParameters["null_values"] = stringArrayToJSON(csvop->null_values());

            // store CSV header information...
            if (csvop->hasHeader()) {
                // extract header!
                auto headerLine = csvToHeader(csvop->inputColumns()) + "\n";
                _fileInputParameters["csvHeader"] = headerLine;
            }

            // projection: which columns to read.
            _columnsToRead = csvop->columnsToSerialize();
            _inputNodeID = csvop->getID();
            _readSchema = csvop->getInputSchema(); // schema before projection pushdown...
            _inputSchema = csvop->getOutputSchema(); // input schema for CSV yields original schema,
            _normalCaseInputSchema = csvop->getOptimizedOutputSchema();
            // but b.c. of projection pushdown output schema is here what the codegen
            // should use as input schema!
            _inputMode = EndPointMode::FILE;
            _inputColumns = csvop->columns(); // after projection pushdown, columns hold the result!
            _inputFileFormat = csvop->fileFormat();
            _inputNode = std::dynamic_pointer_cast<LogicalOperator>(csvop);
        }

        std::string StageBuilder::formatBadUDFNode(tuplex::UDFOperator *udfop) {
            assert(udfop);
            assert(hasUDF(udfop));

            std::stringstream ss;
            ss << "bad UDF node encountered, details:\n";
            ss << "name: " << udfop->name() << "(" << udfop->getID() << ")" << "\n";

            ss << "parents: ";
            for (auto p : udfop->parents())
                ss << p->name() << "(" << p->getID() << ") ";
            ss << "\n";

            ss << "\n";

            auto funcCode = udfop->getUDF().getCode();
            trim(funcCode);
            ss << core::withLineNumbers(funcCode);

            return ss.str();
        }

        std::string formatBadAggNode(tuplex::AggregateOperator* aggop) {
            assert(aggop);

            std::stringstream ss;
            ss << "bad Aggregate node encountered, details:\n";
            ss << "name: " << aggop->name() << "(" << aggop->getID() << ")" << "\n";

            ss << "parents: ";
            for (auto p : aggop->parents())
                ss << p->name() << "(" << p->getID() << ") ";
            ss << "\n";

            ss << "\n";

            if(!aggop->aggregatorUDF().empty()) {
                ss<<"Aggregate UDF:\n";
                auto funcCodeAgg = aggop->aggregatorUDF().getCode();
                trim(funcCodeAgg);
                ss << core::withLineNumbers(funcCodeAgg);
            }
            if(!aggop->combinerUDF().empty()) {
                ss<<"\nCombine UDF:\n";
                auto funcCodeComb = aggop->combinerUDF().getCode();
                trim(funcCodeComb);
                ss << core::withLineNumbers(funcCodeComb);
            }
            return ss.str();
        }



        void StageBuilder::fillInCallbackNames(const std::string& func_prefix, size_t stageNo, TransformStage::StageCodePath& cp) {
            using namespace std;
            // the two main functions
            cp.funcStageName = func_prefix + "Stage_" + to_string(stageNo);
            cp.funcProcessRowName = func_prefix + "processRow_Stage_" + to_string(stageNo);

            // callbacks (per row)
            cp.writeFileCallbackName = func_prefix + "writeOut_Stage_" + to_string(stageNo);
            cp.writeMemoryCallbackName = func_prefix + "memOut_Stage_" + to_string(stageNo);
            cp.writeHashCallbackName = func_prefix + "hashOut_Stage_" + to_string(stageNo);
            cp.writeExceptionCallbackName = func_prefix + "except_Stage_" + to_string(stageNo);
            cp.writeAggregateCallbackName = func_prefix + "aggregate_stage" + std::to_string(stageNo);

            // aggregate functions
            cp.aggregateInitFuncName = func_prefix + "init_aggregate_stage" + std::to_string(stageNo);
            cp.aggregateCombineFuncName = "combine_aggregate_stage" + std::to_string(stageNo);
            cp.aggregateAggregateFuncName = "aggregate_aggregate_stage" + std::to_string(stageNo);

            // init stage funcs
            cp.initStageFuncName = func_prefix + "init_Stage_" + to_string(stageNo);
            cp.releaseStageFuncName = func_prefix + "release_Stage_" + to_string(stageNo);

        }

        // helper function to list all functions within llvm::Module and set names to "", where no symbol is found
        void resetMissingSymbol(llvm::Module& m, std::string& sym) {
            using namespace llvm;
            for(const auto& func : m.functions()) {
                if(sym == func.getName().str()) {
                    return;
                }
            }
            // func not found, reset name
            sym = "";
        }

        void StageBuilder::removeMissingSymbols(llvm::Module& m, TransformStage::StageCodePath& cp) {
            resetMissingSymbol(m, cp.initStageFuncName);
            resetMissingSymbol(m, cp.releaseStageFuncName);
            resetMissingSymbol(m, cp.funcStageName);
            resetMissingSymbol(m, cp.funcProcessRowName);
            resetMissingSymbol(m, cp.writeFileCallbackName);
            resetMissingSymbol(m, cp.writeMemoryCallbackName);
            resetMissingSymbol(m, cp.writeHashCallbackName);
            resetMissingSymbol(m, cp.writeExceptionCallbackName);
            resetMissingSymbol(m, cp.writeAggregateCallbackName);
            resetMissingSymbol(m, cp.aggregateInitFuncName);
            resetMissingSymbol(m, cp.aggregateCombineFuncName);
            resetMissingSymbol(m, cp.aggregateAggregateFuncName);
        }

        TransformStage::StageCodePath StageBuilder::generateFastCodePath(const CodeGenerationContext& ctx,
                                                                         const CodeGenerationContext::CodePathContext& pathContext,
                                                                         const python::Type& generalCaseInputRowType,
                                                                         const std::vector<bool> &generalCaseColumnsToRead,
                                                                         const python::Type& generalCaseOutputRowType,
                                                                         const std::map<int, int>& normalToGeneralMapping,
                                                                         int stageNo,
                                                                         const std::string& env_name) {
            using namespace std;

            TransformStage::StageCodePath ret;
            fillInCallbackNames("fast_", stageNo, ret);
            ret.type = TransformStage::StageCodePath::Type::FAST_PATH;

            string func_prefix = "";
            // name for function processing a row (include stage number)
            string funcStageName = ret.funcStageName;//func_prefix + "Stage_" + to_string(number());
            string funcProcessRowName = ret.funcProcessRowName;//func_prefix + "processRow_Stage_" + to_string(number());
//            ret._funcFileWriteCallbackName = func_prefix + "writeOut_Stage_" + to_string(number());
//            ret._funcMemoryWriteCallbackName = func_prefix + "memOut_Stage_" + to_string(number());
//            ret._funcHashWriteCallbackName = func_prefix + "hashOut_Stage_" + to_string(number());
//            ret._funcExceptionCallback = func_prefix + "except_Stage_" + to_string(number());
//            ret._writerFuncName = _writerFuncName;

            auto &logger = Logger::instance().logger("codegen");
            auto env = make_shared<codegen::LLVMEnvironment>(env_name);

            Row intermediateInitialValue; // filled by aggregate operator, if needed.

#ifdef VERBOSE_BUILD
            {
                stringstream ss;
//                ss<<FLINESTR<<endl;
//                ss<<"Stage"<<stageNo<<" schemas:"<<endl;
//                ss<<"\tnormal case input: "<<_normalCaseInputSchema.getRowType().desc()<<endl;
//                ss<<"\tnormal case output: "<<_normalCaseOutputSchema.getRowType().desc()<<endl;
//                ss<<"\tgeneral case input: "<<_generalCaseInputSchema.getRowType().desc()<<endl;
//                ss<<"\tgeneral case output: "<<_generalCaseOutputSchema.getRowType().desc()<<endl;

                logger.debug(ss.str());
            }
#endif

#ifndef NDEBUG
            if(!pathContext.operators.empty()) {
                stringstream ss;
                ss<<"output type of specialized pipeline is: "<<pathContext.outputSchema.getRowType().desc()<<endl;
                ss<<"is this the most outer stage?: "<<ctx.isRootStage<<endl;
                if(!ctx.isRootStage)
                    ss<<"need to upgrade output type to "<<pathContext.operators.back()->getOutputSchema().getRowType().desc()<<endl;
                logger.debug(ss.str());
            }
#endif

            assert(pathContext.inputSchema.getRowType() != python::Type::UNKNOWN);
            assert(pathContext.outputSchema.getRowType() != python::Type::UNKNOWN);

            // special case: empty pipeline
            if (pathContext.outputSchema.getRowType().parameters().empty() && pathContext.inputSchema.getRowType().parameters().empty()) {
                logger.info("no pipeline code generated, empty pipeline");
                return ret;
            }

            // go through nodes & add operation
            logger.info("generating pipeline for " + pathContext.inputSchema.getRowType().desc() + " -> "
                        + pathContext.outputSchema.getRowType().desc() + " (" + pluralize(pathContext.operators.size(), "operator") + " pipelined)");

            // first node determines the data source

            // collect while going through ops what exceptions can be ignored in the normal case
            vector<tuple<int64_t, ExceptionCode>> ignoreCodes; // tuples of operatorID, code, to be ignored

            // create initstage/release stage functions (LLVM)
            using namespace llvm;
//            ret._fastPathInitStageFuncName = func_prefix + "fastPathInitStage" + to_string(number());
//            ret._fastPathReleaseStageFuncName = func_prefix + "fastPathReleaseStage" + to_string(number());
            auto fastPathInitStageFuncType = FunctionType::get(env->i64Type(),
                                                       {env->i64Type(), env->i8ptrType()->getPointerTo(),
                                                        env->i8ptrType()->getPointerTo()}, false);
            auto fastPathReleaseStageFuncType = FunctionType::get(env->i64Type(), false);

            // create functions + builders
            auto fastPathInitStageFunc = cast<Function>(
                    env->getModule()->getOrInsertFunction(ret.initStageFuncName, fastPathInitStageFuncType).getCallee());
            auto fastPathReleaseStageFunc = cast<Function>(
                    env->getModule()->getOrInsertFunction(ret.releaseStageFuncName, fastPathReleaseStageFuncType).getCallee());

            BasicBlock *bbISBody = BasicBlock::Create(env->getContext(), "", fastPathInitStageFunc);
            BasicBlock *bbRSBody = BasicBlock::Create(env->getContext(), "", fastPathReleaseStageFunc);
            IRBuilder<> isBuilder(bbISBody);
            IRBuilder<> rsBuilder(bbRSBody);
            auto isArgs = codegen::mapLLVMFunctionArgs(fastPathInitStageFunc, {"num_args", "hashmaps", "null_buckets"});

            // step 1. build pipeline, i.e. how to process data
            auto pip = std::make_shared<codegen::PipelineBuilder>(env, pathContext.inputSchema.getRowType(), intermediateType(pathContext.operators), funcProcessRowName);

            // Note: the pipeline function will return whether an exception occured.
            // if that happens, then call to handler in transform task builder
            // pip->addExceptionHandler(_funcExceptionCallback); // don't add a exception handler here.

            // sanity check: output of last op should match schema!
            if(!pathContext.operators.empty() && pathContext.outputSchema.getRowType() != pathContext.operators.back()->getOutputSchema().getRowType()) {
                cout<<"outSchema is different than last operator's schema:"<<endl;
                cout<<"outSchema: "<<pathContext.outputSchema.getRowType().desc()<<endl;
                cout<<"last Op: "<<pathContext.operators.back()->getOutputSchema().getRowType().desc()<<endl;
            }

            int global_var_cnt = 0;
            auto num_operators = pathContext.operators.size();
            for (int i = 0; i < num_operators; ++i) {
                auto node = pathContext.operators[i];
                assert(node);
                UDFOperator *udfop = dynamic_cast<UDFOperator *>(node.get());
                switch (node->type()) {
                    case LogicalOperatorType::MAP: {
                        if (!pip->mapOperation(node->getID(), udfop->getUDF(), ctx.normalCaseThreshold, ctx.allowUndefinedBehavior,
                                               ctx.sharedObjectPropagation)) {
                            logger.error(formatBadUDFNode(udfop));
                            return ret;
                        }
                        break;
                    }
                    case LogicalOperatorType::FILTER: {
                        if (!pip->filterOperation(node->getID(), udfop->getUDF(),
                                                  ctx.normalCaseThreshold,
                                                  ctx.allowUndefinedBehavior,
                                                  ctx.sharedObjectPropagation)) {
                            logger.error(formatBadUDFNode(udfop));
                            return ret;
                        }
                        break;
                    }
                    case LogicalOperatorType::MAPCOLUMN: {
                        auto mop = dynamic_cast<MapColumnOperator *>(node.get());
                        if (!pip->mapColumnOperation(node->getID(), mop->getColumnIndex(), udfop->getUDF(),
                                                     ctx.normalCaseThreshold,
                                                     ctx.allowUndefinedBehavior,
                                                     ctx.sharedObjectPropagation)) {
                            logger.error(formatBadUDFNode(udfop));
                            return ret;
                        }
                        break;
                    }
                    case LogicalOperatorType::WITHCOLUMN: {
                        auto wop = dynamic_cast<WithColumnOperator *>(node.get());
                        if (!pip->withColumnOperation(node->getID(), wop->getColumnIndex(), udfop->getUDF(),
                                                      ctx.normalCaseThreshold,
                                                      ctx.allowUndefinedBehavior,
                                                      ctx.sharedObjectPropagation)) {
                            logger.error(formatBadUDFNode(udfop));
                            return ret;
                        }
                        break;
                    }
                    case LogicalOperatorType::RESOLVE: {
                        // skip, will be dealt with in slow path!
                        break;
                    }
                    case LogicalOperatorType::IGNORE: {
                        auto iop = dynamic_cast<IgnoreOperator *>(node.get());

                        auto baseCode = iop->ecCode();
                        // encode types & Co
                        auto ecType = python::Type::byName(exceptionCodeToPythonClass(baseCode));
                        assert(ecType != python::Type::UNKNOWN);
                        auto ecTypes = ecType.derivedClasses();
                        vector<ExceptionCode> codes{baseCode};
                        for(const auto& t : ecTypes) {
                            codes.emplace_back(pythonClassToExceptionCode(t.desc()));
                        }

                        for(const auto& code : codes)
                             ignoreCodes.emplace_back(make_tuple(iop->getIgnoreID(), code));
                        break;
                    }
                    case LogicalOperatorType::JOIN: {
                        // generate here only the probe part, the build part should have been done separately
                        auto jop = dynamic_cast<JoinOperator *>(node.get());
                        assert(jop);

                        string hashmap_global_name =
                                func_prefix + "hash_map_" + to_string(global_var_cnt) + "_stage" + to_string(stageNo);
                        string null_bucket_global_name =
                                func_prefix + "null_bucket_" + to_string(global_var_cnt) + "_stage" + to_string(stageNo);

                        // add two new globals + init code to init/release func
                        auto hash_map_global = env->createNullInitializedGlobal(hashmap_global_name, env->i8ptrType());
                        auto null_bucket_global = env->createNullInitializedGlobal(null_bucket_global_name,
                                                                                   env->i8ptrType());

                        isBuilder.CreateStore(isBuilder.CreateLoad(
                                isBuilder.CreateGEP(isArgs["hashmaps"], env->i32Const(global_var_cnt))),
                                              hash_map_global);
                        isBuilder.CreateStore(isBuilder.CreateLoad(
                                isBuilder.CreateGEP(isArgs["null_buckets"], env->i32Const(global_var_cnt))),
                                              null_bucket_global);

                        rsBuilder.CreateStore(env->i8nullptr(), hash_map_global);
                        rsBuilder.CreateStore(env->i8nullptr(), null_bucket_global);


                        auto leftRowType = jop->left()->getOutputSchema().getRowType();
                        auto rightRowType = jop->right()->getOutputSchema().getRowType();

                        // if null-value optimization is used, might need to adjust the type for the normal path!
                        if(ctx.nullValueOptimization) {
                            // build right or left?
                            if(jop->buildRight()) {
                                // i.e.
                            } else {

                            }
                        }

                        global_var_cnt++;
                        if (!pip->addHashJoinProbe(jop->leftKeyIndex(), leftRowType,
                                                   jop->rightKeyIndex(),
                                                   rightRowType,
                                                   jop->joinType(),
                                                   jop->buildRight(),
                                                   hash_map_global,
                                                   null_bucket_global)) {
                            logger.error(formatBadUDFNode(udfop));
                            return ret;
                        }
                        break;
                    }
                    case LogicalOperatorType::AGGREGATE: {
                        auto aop = dynamic_cast<AggregateOperator*>(node.get()); assert(aop);
                        if(aop->aggType() == AggregateType::AGG_GENERAL || aop->aggType() == AggregateType::AGG_BYKEY) {

                            // right now aggregation is done using a global variable.
                            // this is because of the overall compilation design
                            // in the future we should prob. rewrite this to compile better plans...
                            // writing to a pointer seems like a bad idea...

                            // NOTE: these functions need to be generated only once for the general case type!
                            auto aggType = aop->aggregateOutputType();
//                            ret._aggregateInitFuncName = "init_aggregate_stage" + std::to_string(number());
//                            ret._aggregateCombineFuncName = "combine_aggregate_stage" + std::to_string(number());
//                            if(aop->aggType() == AggregateType::AGG_BYKEY)
//                                ret._aggregateAggregateFuncName = "aggregate_aggregate_stage" + std::to_string(number());
                            //ret._aggregateCallbackName = "aggregate_callback_stage" + std::to_string(number());
                            auto aggregateInitFunc = codegen::createAggregateInitFunction(env.get(),
                                                                                          ret.aggregateInitFuncName,
                                                                                          aop->initialValue(),
                                                                                          aggType); // use c-malloc!
                            auto combFunc = codegen::createAggregateCombineFunction(env.get(),
                                                                                    ret.aggregateCombineFuncName,
                                                                                    aop->combinerUDF(),
                                                                                    aggType,
                                                                                    malloc);
                            if(!aggregateInitFunc)
                                throw std::runtime_error("error compiling aggregate initialize function");
                            if(!combFunc)
                                throw std::runtime_error("error compiling combiner function for aggregate");
                            // update func names, to avoid duplicates
                            ret.aggregateInitFuncName = aggregateInitFunc->getName().str();
                            ret.aggregateCombineFuncName = combFunc->getName().str();

                            if(aop->aggType() == AggregateType::AGG_BYKEY) { // need to make the aggregate functor
                                auto aggregateFunc = codegen::createAggregateFunction(env.get(),
                                                                                      ret.aggregateAggregateFuncName,
                                                                                      aop->aggregatorUDF(), aggType,
                                                                                      aop->parent()->getOutputSchema().getRowType(),
                                                                                      malloc);
                                if(!aggregateFunc)
                                    throw std::runtime_error("error compiling aggregate function");
                                ret.aggregateAggregateFuncName = aggregateFunc->getName().str();
                            } else {
                                // init intermediate within Stage process function.
                                intermediateInitialValue = aop->initialValue();
                                if (!pip->addAggregate(aop->getID(), aop->aggregatorUDF(),
                                                       aop->getOutputSchema().getRowType(),
                                                       ctx.normalCaseThreshold,
                                                       ctx.allowUndefinedBehavior,
                                                       ctx.sharedObjectPropagation)) {
                                    logger.error(formatBadAggNode(aop));
                                    return ret;
                                }
                            }
                        } else if(aop->aggType() == AggregateType::AGG_UNIQUE) {
                            // nothing to do...
                            // => here aggregate is directly written to output table!
                        } else {
                            throw std::runtime_error("unsupported aggregate type");
                        }

                        break; // aggregate isn't codegen'd
                    }
                    case LogicalOperatorType::CACHE: {
                        // skip
                        break;
                    }
                    default: {
                        std::stringstream ss;
                        ss<<"found unknown operator " + node->name() +
                          " for which a pipeline could not be generated";
                        logger.error(ss.str());
                        throw std::runtime_error(ss.str());
                    }
                }
            }

            //// opt: i.e. for outer stage this is not required
            //// type upgrade because of nullvalue opt?
            //if (_nullValueOptimization && !_isRootStage
            //    && outSchema != operators.back()->getOutputSchema().getRowType()) {
            //
            //    if (!pip->addTypeUpgrade(outSchema))
            //        throw std::runtime_error(
            //                "type upgrade from " + operators.back()->getOutputSchema().getRowType().desc() + " to " +
            //                outSchema.desc() + "failed.");
            //}


            // only fast
            switch(ctx.outputMode) {
                case EndPointMode::FILE: {
                    // for file mode, can directly merge output rows
                    //pip->buildWithTuplexWriter(_funcMemoryWriteCallbackName, _outputNodeID); //output node id

                    switch (ctx.outputFileFormat) {
                        case FileFormat::OUTFMT_CSV: {
                            // i.e. write to memory writer!
                            pip->buildWithCSVRowWriter(ret.writeMemoryCallbackName,
                                                       ctx.outputNodeID,
                                                       ctx.hasOutputLimit(),
                                                       ctx.fileOutputParameters.at("null_value"),
                                                       true, ctx.csvOutputDelimiter(), ctx.csvOutputQuotechar());
                            break;
                        }
                        case FileFormat::OUTFMT_ORC: {
                            pip->buildWithTuplexWriter(ret.writeMemoryCallbackName,
                                                       ctx.outputNodeID,
                                                       ctx.hasOutputLimit());
                            break;
                        }
                        default:
                            throw std::runtime_error("unsupported output fmt encountered, can't codegen!");
                    }

                    // old way used to be to generate additional file writer code with csv conversion
                    // ==> speed it up by normal case specialization
                    //                // generate separate function to write from main memory to file
//                generateFileWriterCode(env, _funcFileWriteCallbackName, _funcExceptionCallback, _outputNodeID,
//                                       _outputFileFormat, _fileOutputParameters["null_value"], _allowUndefinedBehavior);
                    break;
                }
                case EndPointMode::HASHTABLE: {
                     // force output type to be always general case (=> so merging works easily!)
                     // only exception is if source is cache and no exceptions happened.

                     bool leaveNormalCase = false;

                     // special case: join is executed on top of a .cache()
                     // =>
                    if(ctx.nullValueOptimization) {
                        if(!leaveNormalCase) {
                            if (!pip->addTypeUpgrade(generalCaseOutputRowType))
                                throw std::runtime_error(
                                        "type upgrade from " + pathContext.outputSchema.getRowType().desc() + " to " +
                                                generalCaseOutputRowType.desc() + "failed.");
                            // set normal case output type to general case
                            logger.warn("using const cast here, it's a code smell. need to fix...");

                            // HACK: uncommented... need to fix.
                            //const_cast<StageBuilder*>(this)->_normalCaseOutputSchema = _generalCaseOutputSchema;
                        }
                    }
                    pip->buildWithHashmapWriter(ret.writeHashCallbackName,
                                                ctx.hashColKeys,
                                                hashtableKeyWidth(ctx.hashKeyType),
                                                ctx.hashSaveOthers,
                                                ctx.hashAggregate);
                    break;
                }
                case EndPointMode::MEMORY: {

                    // special case: writing intermediate output
                    if(intermediateType(pathContext.operators) == python::Type::UNKNOWN) {
                        // NOTE: forcing output to be general case is not necessary for cache operator!
                        // => i.e. may manually convert...

                        // is outputNode a cache operator? Then leave normal case as is...
                        // => always pass cache node!
                        bool leaveNormalCase = false;

                        if(!pathContext.operators.empty())
                            leaveNormalCase = pathContext.operators.back()->type() == LogicalOperatorType::CACHE;

                        // force output type to be always general case (=> so merging works easily!)
                        if(ctx.nullValueOptimization) {
                            if(!leaveNormalCase) {
                                if (!pip->addTypeUpgrade(generalCaseOutputRowType))
                                    throw std::runtime_error(
                                            "type upgrade from " + pathContext.outputSchema.getRowType().desc() + " to " +
                                            generalCaseOutputRowType.desc() + "failed.");
                                // set normal case output type to general case
                                // _normalCaseOutputSchema = _generalCaseOutputSchema;
                            }
                        }
                        pip->buildWithTuplexWriter(ret.writeMemoryCallbackName,
                                                   ctx.outputNodeID,
                                                   ctx.hasOutputLimit());
                    } else {
                        // build w/o writer
                        pip->build();
                    }

                    break;
                }
                default: {
                    throw std::runtime_error("unknown output mode encountered, could not finish pipeline!");
                    break;
                }
            }

            // step 2. build connector to data source, i.e. generated parser or simply iterating over stuff
            std::shared_ptr<codegen::BlockBasedTaskBuilder> tb;
            if (ctx.inputMode == EndPointMode::FILE) {

                // only CSV supported yet // throw std::runtime_error("found unknown data-source operator " + node->name() + " for which a pipeline could not be generated");

                // input schema holds for CSV node the original, unoptimized number of columns.
                // if pushdown is performed, outputschema holds whatever is left.
                char delimiter = ctx.fileInputParameters.at("delimiter")[0];
                char quotechar = ctx.fileInputParameters.at("quotechar")[0];

                // note: null_values may be empty!
                auto null_values = jsonToStringArray(ctx.fileInputParameters.at("null_values"));

                switch (ctx.inputFileFormat) {
                    case FileFormat::OUTFMT_CSV:
                    case FileFormat::OUTFMT_TEXT: {

                        // check that generalCaseInputRowType and readSchema are compatible
                        if(!python::canUpcastToRowType(pathContext.readSchema.getRowType(), generalCaseInputRowType)) {
                            throw std::runtime_error("incompatible normal and general case row type for parsing text data");
                        }

                        if (ctx.generateParser) {
                            //@TODO: optimization/hyperspecialization checks!
                            tb = make_shared<codegen::JITCSVSourceTaskBuilder>(env,
                                                                               pathContext.readSchema.getRowType(),
                                                                               generalCaseInputRowType,
                                                                               pathContext.columnsToRead,
                                                                               normalToGeneralMapping,
                                                                               funcStageName,
                                                                               ctx.inputNodeID,
                                                                               null_values,
                                                                               delimiter,
                                                                               quotechar);
                        } else {
                            tb = make_shared<codegen::CellSourceTaskBuilder>(env,
                                                                             pathContext.readSchema.getRowType(),
                                                                             pathContext.columnsToRead,
                                                                             generalCaseInputRowType,
                                                                             generalCaseColumnsToRead,
                                                                             normalToGeneralMapping,
                                                                             funcStageName,
                                                                             ctx.inputNodeID,
                                                                             null_values);
                        }
                        break;
                    }
                    case FileFormat::OUTFMT_ORC: {

                        // check that generalCaseInputRowType and readSchema are compatible
                        if(!python::canUpcastToRowType(pathContext.inputSchema.getRowType(), generalCaseInputRowType)) {
                            throw std::runtime_error("incompatible normal and general case row type for reading ORC data");
                        }

                        tb = make_shared<codegen::TuplexSourceTaskBuilder>(env,
                                                                           pathContext.inputSchema.getRowType(),
                                                                           generalCaseInputRowType,
                                                                           normalToGeneralMapping,
                                                                           funcStageName);
                        break;
                    }
                    default:
                        throw std::runtime_error("file input format not yet supported!");
                }
            } else {
                // HACK: fix this later...!
//                // tuplex (in-memory) reader
//               if (ctx.updateInputExceptions)
//                    tb = make_shared<codegen::ExceptionSourceTaskBuilder>(env, pathContext.inputSchema.getRowType(), funcStageName);
//                else


                // check that generalCaseInputRowType and readSchema are compatible
                if(!python::canUpcastToRowType(pathContext.inputSchema.getRowType(), generalCaseInputRowType)) {
                    throw std::runtime_error("incompatible normal and general case row type for reading ORC data");
                }

                tb = make_shared<codegen::TuplexSourceTaskBuilder>(env,
                                                                   pathContext.inputSchema.getRowType(),
                                                                   generalCaseInputRowType,
                                                                   normalToGeneralMapping,
                                                                   funcStageName);
            }

            // set pipeline and
            // add ignore codes & exception handler
            tb->setExceptionHandler(ret.writeExceptionCallbackName);
            tb->setIgnoreCodes(ignoreCodes);

            // #error "need to add here the optional checking for the input! --> i.e. smaller pipeline etc."
            logger.warn("hack, need to fix stuff here...");
            tb->setPipeline(pip);

            // special case: intermediate
            if(intermediateType(pathContext.operators) != python::Type::UNKNOWN) {
                tb->setIntermediateInitialValueByRow(intermediateType(pathContext.operators), intermediateInitialValue);
                tb->setIntermediateWriteCallback(ret.writeAggregateCallbackName);
            }

            // create code for "wrap-around" function
            auto func = tb->build(ctx.hasOutputLimit());
            if (!func)
                throw std::runtime_error("could not build codegen csv parser");

            assert(func);

            // close initStage/releaseStage functions
            // => call global init function of llvm env
            isBuilder.CreateRet(env->callGlobalsInit(isBuilder));
            rsBuilder.CreateRet(env->callGlobalsRelease(rsBuilder));

            // // print module for debug/dev purposes
            // auto code = codegen::moduleToString(*env->getModule());
            // std::cout<<core::withLineNumbers(code)<<std::endl;
            // LLVMContext test_ctx;
            // auto test_mod = codegen::stringToModule(test_ctx, code);

            // save into variables (allows to serialize stage etc.)
            // IR is generated. Save into stage.
            ret.funcStageName = func->getName();
            // remove non-existing symbols
            removeMissingSymbols(*env->getModule(), ret);
            ret.irBitCode = codegen::moduleToBitCodeString(*env->getModule()); // trafo stage takes ownership of module

            // @TODO: lazy & fast codegen of the different paths + lowering of them
            // generate interpreter fallback path (always) --> probably do that lazily or parallelize it...
            return ret;
        }

        TransformStage::StageCodePath StageBuilder::generateResolveCodePath(const CodeGenerationContext& ctx,
                                                                            const CodeGenerationContext::CodePathContext& pathContext,
                                                                            const python::Type& normalCaseType,
                                                                            const std::map<int, int>& normalToGeneralMapping) const {
            using namespace std;
            using namespace llvm;

            TransformStage::StageCodePath ret;

            // @TODO: the short-circuiting here kinda sounds off...!
            // --> i.e. when normal case path is specialized, require ALWAYS special resolve path???


            if(!pathContext.valid())
                throw std::runtime_error("invalid pathContext given. Need to specify at least some nodes in there!");

            // Compile if resolve function is present or if null-value optimization is present
            auto numResolveOperators = resolveOperatorCount();
            bool requireSlowPath = ctx.nullValueOptimization; // per default, slow path is always required when null-value opt is enabled.

            // special case: input source is cached and no exceptions happened => no resolve path necessary if there are no resolvers!
            if(pathContext.inputNode->type() == LogicalOperatorType::CACHE &&
               std::dynamic_pointer_cast<CacheOperator>(pathContext.inputNode)->cachedExceptions().empty())
                requireSlowPath = false;

            // nothing todo, return empty code-path - i.e., normal
            if (numResolveOperators == 0 && !requireSlowPath) {
                return ret;
            }

            // when there are no operators present, there is no way to generate a resolve path
            // => skip
            if(pathContext.operators.empty() &&
               !ctx.nullValueOptimization) // when null value optimization is done, need to always generate resolve path.
                return ret;

            // @TODO: one needs to add here somewhere an option where bad input rows/data get resolved when they do not fit the initial schema!
            //  r/n it's all squashed together in a pipeline.

            // @TODO: need to type here UDFs with commonCase/ExceptionCase type!
            // ==> i.e. the larger, unspecialized type!

            auto &logger = Logger::instance().logger("codegen");

            // fill in names
            fillInCallbackNames("slow_", number(), ret);
            ret.type = TransformStage::StageCodePath::Type::SLOW_PATH;

            // Create environment
            string env_name = "tuplex_slowCodePath";
            string func_prefix = "";

            auto readSchema = pathContext.readSchema.getRowType(); // what to read from files (before projection pushdown)
            auto inSchema = pathContext.inputSchema.getRowType(); // with what to start the pipeline (after projection pushdown)
            auto resolveInSchema = inSchema;
            auto outSchema = pathContext.outputSchema.getRowType(); // what to output from pipeline

            auto env = make_shared<codegen::LLVMEnvironment>(env_name);

//            ret._slowPathInitStageFuncName = func_prefix + "slowPathInitStage" + to_string(number());
//            ret._slowPathReleaseStageFuncName = func_prefix + "slowPathReleaseStage" + to_string(number());
            auto slowPathInitStageFuncType = FunctionType::get(env->i64Type(),
                                                       {env->i64Type(), env->i8ptrType()->getPointerTo(),
                                                        env->i8ptrType()->getPointerTo()}, false);
            auto slowPathReleaseStageFuncType = FunctionType::get(env->i64Type(), false);

            // create functions + builders
            auto slowPathInitStageFunc = cast<Function>(
                    env->getModule()->getOrInsertFunction(ret.initStageFuncName, slowPathInitStageFuncType).getCallee());
            auto slowPathReleaseStageFunc = cast<Function>(
                    env->getModule()->getOrInsertFunction(ret.releaseStageFuncName, slowPathReleaseStageFuncType).getCallee());

            BasicBlock *bbISBody = BasicBlock::Create(env->getContext(), "", slowPathInitStageFunc);
            BasicBlock *bbRSBody = BasicBlock::Create(env->getContext(), "", slowPathReleaseStageFunc);
            IRBuilder<> isBuilder(bbISBody);
            IRBuilder<> rsBuilder(bbRSBody);
            auto isArgs = codegen::mapLLVMFunctionArgs(slowPathInitStageFunc,
                                                                 {"num_args", "hashmaps", "null_buckets"});

            // Note: this here is quite confusing, because for map operator when tuples are needed, this will return not the row schema but the UDF input schema =? fix that
            // @TODO: fix getInputSchema for MapOperator!!!
            inSchema = _inputSchema.getRowType(); // old: _operators.front()->getInputSchema().getRowType();
//            string funcSlowPathName = "processViaSlowPath_Stage_" + to_string(number());
//            string funcResolveRowName = "resolveSingleRow_Stage_" + to_string(number());
//            string slowPathMemoryWriteCallback = "memOutViaSlowPath_Stage_" + to_string(number());
//            string slowPathHashWriteCallback = "hashOutViaSlowPath_Stage_" + to_string(number());
//            string slowPathExceptionCallback = "exceptionOutViaSlowPath_Stage_" + to_string(number());

            logger.debug("input schema for general case is: " + resolveInSchema.desc());
            logger.debug("intermediate type for general case is: " + intermediateType(pathContext.operators).desc());

            auto slowPip = std::make_shared<codegen::PipelineBuilder>(env, resolveInSchema, intermediateType(pathContext.operators), ret.funcStageName/*funcSlowPathName*/);
            int global_var_cnt = 0;
            auto num_operators = pathContext.operators.size();
            for (int i = 0; i < num_operators; ++i) {
                auto node = pathContext.operators[i];
                assert(node);
                UDFOperator *udfop = dynamic_cast<UDFOperator *>(node.get());
                switch (node->type()) {
                    case LogicalOperatorType::MAP: {
                        slowPip->mapOperation(node->getID(), udfop->getUDF(), _normalCaseThreshold, ctx.allowUndefinedBehavior,
                                              ctx.sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::FILTER: {
                        slowPip->filterOperation(node->getID(), udfop->getUDF(), _normalCaseThreshold, ctx.allowUndefinedBehavior,
                                                 ctx.sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::MAPCOLUMN: {
                        auto mop = dynamic_cast<MapColumnOperator *>(node.get());
                        slowPip->mapColumnOperation(node->getID(), mop->getColumnIndex(), udfop->getUDF(),
                                                    _normalCaseThreshold, ctx.allowUndefinedBehavior, ctx.sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::WITHCOLUMN: {
                        auto wop = dynamic_cast<WithColumnOperator *>(node.get());
                        slowPip->withColumnOperation(node->getID(), wop->getColumnIndex(), udfop->getUDF(),
                                                     _normalCaseThreshold, ctx.allowUndefinedBehavior, ctx.sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::CACHE:
                    case LogicalOperatorType::FILEOUTPUT: {
                        // skip, exception resolution is in memory
                        break;
                    }

                    case LogicalOperatorType::RESOLVE: {
                        // ==> this means slow code path needs to be generated as well!
                        auto rop = std::dynamic_pointer_cast<ResolveOperator>(node);
                        slowPip->addResolver(rop->ecCode(), rop->getID(), rop->getUDF(), _normalCaseThreshold, ctx.allowUndefinedBehavior,
                                             ctx.sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::IGNORE: {

                        // do not skip, if one of the ancestors is a resolver (skip ignores)
                        auto iop = std::dynamic_pointer_cast<IgnoreOperator>(node);
                        assert(iop);

                        // always add ignore on slowpath, because else special cases (i.e. exception resolved, throws again etc.)
                        // won't work.
                        slowPip->addIgnore(iop->ecCode(), iop->getID());
                        break;
                    }
                    case LogicalOperatorType::TAKE: {
                        // ignore, is collect operator...
                        break;
                    }

                    case LogicalOperatorType::JOIN: {
                        // take previous hashmaps
                        // generate here only the probe part, the build part should have been done separately
                        auto jop = dynamic_cast<JoinOperator *>(node.get());
                        assert(jop);

                        string hashmap_global_name =
                                func_prefix + "hash_map_" + to_string(global_var_cnt) + "_stage" + to_string(number());
                        string null_bucket_global_name =
                                func_prefix + "null_bucket_" + to_string(global_var_cnt) + "_stage" + to_string(number());

                        // add two new globals + init code to init/release func
                        auto hash_map_global = env->createNullInitializedGlobal(hashmap_global_name, env->i8ptrType());
                        auto null_bucket_global = env->createNullInitializedGlobal(null_bucket_global_name,
                                                                                   env->i8ptrType());

                        isBuilder.CreateStore(isBuilder.CreateLoad(
                                isBuilder.CreateGEP(isArgs["hashmaps"], env->i32Const(global_var_cnt))),
                                              hash_map_global);
                        isBuilder.CreateStore(isBuilder.CreateLoad(
                                isBuilder.CreateGEP(isArgs["null_buckets"], env->i32Const(global_var_cnt))),
                                              null_bucket_global);

                        rsBuilder.CreateStore(env->i8nullptr(), hash_map_global);
                        rsBuilder.CreateStore(env->i8nullptr(), null_bucket_global);

                        auto leftRowType = jop->left()->getOutputSchema().getRowType();
                        auto rightRowType = jop->right()->getOutputSchema().getRowType();

                        global_var_cnt++;

                        if (!slowPip->addHashJoinProbe(jop->leftKeyIndex(), jop->left()->getOutputSchema().getRowType(),
                                                   jop->rightKeyIndex(),
                                                   jop->right()->getOutputSchema().getRowType(),
                                                   jop->joinType(),
                                                   jop->buildRight(),
                                                   hash_map_global,
                                                   null_bucket_global)) {
                            logger.error(formatBadUDFNode(udfop));
                            return ret;
                        }
                        break;
                    }

                    case LogicalOperatorType::AGGREGATE: {
                        //  @TODO: needs more support for full aggregate fallback code
                        auto aop = std::dynamic_pointer_cast<AggregateOperator>(node); assert(aop);
                        if(aop->aggType() == AggregateType::AGG_GENERAL || aop->aggType() == AggregateType::AGG_BYKEY) {

                            // right now aggregation is done using a global variable.
                            // this is because of the overall compilation design
                            // in the future we should prob. rewrite this to compile better plans...
                            // writing to a pointer seems like a bad idea...

                            // NOTE: these functions need to be generated only once for the general case type!
                            auto aggType = aop->aggregateOutputType();
//                            ret._aggregateInitFuncName = "init_aggregate_stage" + std::to_string(number());
//                            ret._aggregateCombineFuncName = "combine_aggregate_stage" + std::to_string(number());
//                            if(aop->aggType() == AggregateType::AGG_BYKEY)
//                                ret._aggregateAggregateFuncName = "aggregate_aggregate_stage" + std::to_string(number());
//                            ret._aggregateCallbackName = "aggregate_callback_stage" + std::to_string(number());
                            auto aggregateInitFunc = codegen::createAggregateInitFunction(env.get(),
                                                                                          ret.aggregateInitFuncName/*ret._aggregateInitFuncName*/,
                                                                                          aop->initialValue(),
                                                                                          aggType); // use c-malloc!
                            auto combFunc = codegen::createAggregateCombineFunction(env.get(),
                                                                                    ret.aggregateCombineFuncName/*ret._aggregateCombineFuncName*/,
                                                                                    aop->combinerUDF(),
                                                                                    aggType,
                                                                                    malloc);
                            if(!aggregateInitFunc)
                                throw std::runtime_error("error compiling aggregate initialize function");
                            if(!combFunc)
                                throw std::runtime_error("error compiling combiner function for aggregate");
                            // update func names, to avoid duplicates
                            ret.aggregateInitFuncName = aggregateInitFunc->getName().str();
                            ret.aggregateCombineFuncName = combFunc->getName().str();

                            if(aop->aggType() == AggregateType::AGG_BYKEY) { // need to make the aggregate functor
                                auto aggregateFunc = codegen::createAggregateFunction(env.get(),
                                                                                      ret.aggregateAggregateFuncName,
                                                                                      aop->aggregatorUDF(), aggType,
                                                                                      aop->parent()->getOutputSchema().getRowType(),
                                                                                      malloc);
                                if(!aggregateFunc)
                                    throw std::runtime_error("error compiling aggregate function");
                                ret.aggregateAggregateFuncName = aggregateFunc->getName().str();
                            } else {
                                // init intermediate within Stage process function.
//                                intermediateInitialValue = aop->initialValue();
                                if (!slowPip->addAggregate(aop->getID(),
                                                           aop->aggregatorUDF(),
                                                       aop->getOutputSchema().getRowType(),
                                                       this->_normalCaseThreshold,
                                                       _allowUndefinedBehavior,
                                                       _sharedObjectPropagation)) {
                                    logger.error(formatBadAggNode(aop.get()));
                                    return ret;
                                }
                            }
                        } else if(aop->aggType() == AggregateType::AGG_UNIQUE) {
                            // nothing to do...
                        } else {
                            throw std::runtime_error("unsupported aggregate type");
                        }

                        break; // aggregate isn't codegen'd
                    }

                    default: {
                        throw std::runtime_error(
                                "found unknown operator " + node->name() +
                                " for which a pipeline could not be generated");
                    }
                }
            }

            // add exception callback (required when resolvers throw exceptions themselves!)
            slowPip->addExceptionHandler(ret.writeExceptionCallbackName/*slowPathExceptionCallback*/);

            // @TODO: when supporting text output, need to include check that no newline occurs within string!
            // => else, error!


            bool useRawOutput = ctx.outputMode == EndPointMode::FILE && ctx.outputFileFormat == FileFormat::OUTFMT_CSV;
            // build slow path with mem writer or to CSV
            llvm::Function* slowPathFunc = nullptr;
            if(useRawOutput) {
                slowPathFunc = slowPip->buildWithCSVRowWriter(ret.writeMemoryCallbackName/*slowPathMemoryWriteCallback*/, ctx.outputNodeID,
                                                              hasOutputLimit(),
                                                              ctx.fileOutputParameters.at("null_value"), true,
                                                              ctx.fileOutputParameters.at("delimiter")[0], ctx.fileOutputParameters.at("quotechar")[0]);
            } else {
                // @TODO: hashwriter if hash output desired
                if(ctx.outputMode == EndPointMode::HASHTABLE) {
                    slowPathFunc = slowPip->buildWithHashmapWriter(ret.writeHashCallbackName/*slowPathHashWriteCallback*/, ctx.hashColKeys, hashtableKeyWidth(ctx.hashKeyType), ctx.hashSaveOthers, ctx.hashAggregate);
                } else {
                    slowPathFunc = slowPip->buildWithTuplexWriter(ret.writeMemoryCallbackName/*slowPathMemoryWriteCallback*/, ctx.outputNodeID, hasOutputLimit());
                }
            }

            // create wrapper which decodes automatically normal-case rows with optimized types...
            auto null_values = ctx.inputMode == EndPointMode::FILE ? jsonToStringArray(ctx.fileInputParameters.at("null_values"))
                                                     : std::vector<std::string>{"None"};
            auto rowProcessFunc = codegen::createProcessExceptionRowWrapper(*slowPip, ret.funcStageName/*funcResolveRowName*/,
                                                                            normalCaseType, normalToGeneralMapping, null_values);

            ret.funcStageName = rowProcessFunc->getName();
//            ret._resolveRowFunctionName = rowProcessFunc->getName();
//            ret._resolveRowWriteCallbackName = slowPathMemoryWriteCallback;
//            ret._resolveRowExceptionCallbackName = slowPathExceptionCallback;
//            ret._resolveHashCallbackName = slowPathHashWriteCallback;

            // close initStage/releaseStage functions
            // => call global init function of llvm env
            isBuilder.CreateRet(env->callGlobalsInit(isBuilder));
            rsBuilder.CreateRet(env->callGlobalsRelease(rsBuilder));

            // remove unused symbols, i.e. a simple pass over the module should do.
            removeMissingSymbols(*env->getModule(), ret);
            ret.irBitCode = codegen::moduleToBitCodeString(*env->getModule()); // transform stage takes ownership of module

            return ret;
        }

        void StageBuilder::addFileOutput(const std::shared_ptr<FileOutputOperator>& fop) {
            _fileOutputParameters["splitSize"] = std::to_string(fop->splitSize());
            _fileOutputParameters["numParts"] = std::to_string(fop->numParts());
            _fileOutputParameters["udf"] = fop->udf().getCode();

            // add all keys from options
            for (auto keyval : fop->options()) {
                assert(_fileOutputParameters.find(keyval.first) == _fileOutputParameters.end());
                _fileOutputParameters[keyval.first] = keyval.second;
            }

            // make sure certain options exist
            if (_fileOutputParameters.find("null_value") == _fileOutputParameters.end())
                _fileOutputParameters["null_value"] = ""; // empty string for now // @TODO: make this an option in the python API

            switch (fop->fileFormat()) {
                case FileFormat::OUTFMT_CSV: {
                    // sanitize options: If write Header is true and neither csvHeader nor columns are given, set to false
                    if (fop->columns().empty() && _fileOutputParameters.find("csvHeader") == _fileOutputParameters.end()) {
                        _fileOutputParameters["header"] = "false";
                    }

                    bool writeHeader = stringToBool(get_or(_fileOutputParameters, "header", "false"));

                    if (writeHeader) {
                        assert(!fop->columns().empty());

                        if (_fileOutputParameters.find("csvHeader") == _fileOutputParameters.end()) {
                            auto headerLine = csvToHeader(fop->columns()) + "\n";
                            _fileOutputParameters["csvHeader"] = headerLine;
                        } else {
                            // check correct number of arguments
                            auto v = jsonToStringArray(_fileOutputParameters["csvHeader"]);
                            if (v.size() != fop->columns().size()) {
                                std::stringstream ss;
                                ss << "number of ouput column names given to tocsv operator (" << v.size()
                                   << ") does not equal number of elements from pipeline (" << fop->columns().size() << ")";
                                throw std::runtime_error(ss.str());
                            }
                        }
                    }
                    break;
                }
                case FileFormat::OUTFMT_ORC: {
                    _fileOutputParameters["columnNames"] = csvToHeader(fop->columns());
                    break;
                }
                default:
                    throw std::runtime_error("unsupported file output format!");
            }

            _outputFileFormat = fop->fileFormat();
            _outputNodeID = fop->getID();
            _outputDataSetID = fop->getDataSetID();
            _outputSchema = fop->getOutputSchema();
            _outputColumns = fop->columns();
            _normalCaseOutputSchema = fop->getOutputSchema();
            _outputURI = fop->uri();
            _outputMode = EndPointMode::FILE;
        }

        int64_t StageBuilder::outputDataSetID() const { return _outputDataSetID; }


        void StageBuilder::addMemoryInput(const Schema &schema, std::shared_ptr<LogicalOperator> node = nullptr) {
            // add reader
            _inputSchema = schema;
            _normalCaseInputSchema = schema;
            _readSchema = schema; // no projection pushdown yet for tuplex spurce @TODO to improve speeds!
            _inputMode = EndPointMode::MEMORY;
            _inputFileFormat = FileFormat::OUTFMT_TUPLEX;
            _inputNode = node;
        }

        void StageBuilder::addMemoryOutput(const Schema &schema, int64_t opID, int64_t dsID) {
            // add memory writer
            _outputMode = EndPointMode::MEMORY;
            _outputSchema = schema;
            _normalCaseOutputSchema = schema;

            _outputFileFormat = FileFormat::OUTFMT_TUPLEX;
            _outputNodeID = opID;
            _outputDataSetID = dsID;
        }

        void StageBuilder::addHashTableOutput(const Schema &schema,
                                              bool bucketizeOthers,
                                              bool aggregate,
                                              const std::vector<size_t> &colKeys,
                                              const python::Type& keyType,
                                              const python::Type& bucketType) {
            assert(!bucketizeOthers || (bucketizeOthers && !colKeys.empty())); // can't bucketize w/o colkey
            _outputMode = EndPointMode::HASHTABLE;
            _hashColKeys = colKeys;
            _hashSaveOthers = bucketizeOthers;
            _hashAggregate = aggregate;
            _outputSchema = schema;
            _outputFileFormat = FileFormat::OUTFMT_UNKNOWN;
            _outputNodeID = 0;
            _outputDataSetID = 0;

            // leave others b.c. it's an intermediate stage...

            // assert key type
            // TODO(rahuly): do something about this assertion - it's false for AggregateByKey because the output schema doesn't match the parent's
//            python::Type kt;
//            if(colKey.has_value()) {
//                kt = schema.getRowType().parameters().at(colKey.value());
//                std::cout << "kt: " << kt.desc() << std::endl;
//                assert(canUpcastType(kt, keyType));
//            }
            _hashKeyType = keyType;
            _hashBucketType = bucketType;
        }

        void StageBuilder::fillStageParameters(TransformStage* stage) {
            if(!stage)
                return;

            // fill in defaults
            stage->_inputColumns = _inputColumns;
            stage->_outputColumns = _outputColumns;

            stage->_generalCaseReadSchema = _readSchema;
            stage->_generalCaseInputSchema = _inputSchema;
            stage->_generalCaseOutputSchema = _outputSchema;
            stage->_normalCaseInputSchema = _normalCaseInputSchema;
            stage->_normalCaseOutputSchema = _normalCaseOutputSchema;
            stage->_outputDataSetID = outputDataSetID();
            stage->_inputNodeID = _inputNodeID;
            auto numColumns = stage->_generalCaseReadSchema.getRowType().parameters().size();
            if(_inputMode == EndPointMode::FILE && _inputNode) {
                stage->_inputColumnsToKeep = dynamic_cast<FileInputOperator*>(_inputNode.get())->columnsToSerialize();
                if(stage->_inputColumnsToKeep.empty())
                    stage->_inputColumnsToKeep = std::vector<bool>(numColumns, true);
                assert(stage->_inputColumnsToKeep.size() == numColumns);
            } else {
                stage->_inputColumnsToKeep = std::vector<bool>(numColumns, true);
            }

            stage->_outputURI = _outputURI;
            stage->_inputFormat = _inputFileFormat;
            stage->_outputFormat = _outputFileFormat;

            // output limit?
            // no limit operator yet...

            // get limit
            stage->_outputLimit = _outputLimit;

            // copy input/output configurations
            stage->_fileInputParameters = _fileInputParameters;
            stage->_fileOutputParameters = _fileOutputParameters;
            stage->_inputMode = _inputMode;
            stage->_outputMode = _outputMode;
            stage->_hashOutputKeyType = _hashKeyType;
            stage->_hashOutputBucketType = _hashBucketType;

            // copy code
            // llvm ir as string is super wasteful, use bitcode instead. Can be faster parsed.
            // => https://llvm.org/doxygen/BitcodeWriter_8cpp_source.html#l04457
            // stage->_irCode = _irCode;
            // stage->_irResolveCode = _irResolveCode;
            stage->_updateInputExceptions = _updateInputExceptions;

            // if last op is CacheOperator, check whether normal/exceptional case should get cached separately
            // or an upcasting step should be performed.
            stage->_persistSeparateCases = false;
            if(!_operators.empty() && _operators.back()->type() == LogicalOperatorType::CACHE)
                stage->_persistSeparateCases = ((CacheOperator*)_operators.back().get())->storeSpecialized();

            stage->_operatorIDsWithResolvers = getOperatorIDsAffectedByResolvers(_operators);
            stage->setInitData();
        }

        /*!
         * creates specialized (normal-case) version of pipeline
         */
        CodeGenerationContext::CodePathContext specializePipeline(const CodeGenerationContext::CodePathContext& general_path_ctx,
                                                                  std::map<int, int>& normalToGeneralMapping,
                                                                  bool enableNVO=true, bool enableCF=true) {

            using namespace std;
            auto& logger = Logger::instance().logger("physical planner");
            Timer timer;

            auto path_ctx = general_path_ctx;
            assert(path_ctx.valid());

            auto inputNode = path_ctx.inputNode;
            auto operators = path_ctx.operators;

            // sample using strategy?
            // // force resampling b.c. of thin layer
            // if(inputNode->type() == LogicalOperatorType::FILEINPUT) {
            //     auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode); assert(fop);
            //     fop->setInputFiles({uri}, {file_size}, true);
            // }

            // node need to find some smart way to QUICKLY detect whether the optimization can be applied or should be rather skipped...
            codegen::StagePlanner planner(inputNode, operators);
            planner.disableAll();
            if(enableNVO)
                planner.enableNullValueOptimization();
            if(enableCF)
                planner.enableConstantFoldingOptimization();
            planner.optimize();

            // use optimized or non-optimized schema
            Schema readSchema = Schema::UNKNOWN;
            if(enableNVO) {
                readSchema = std::dynamic_pointer_cast<FileInputOperator>(planner.input_node())->getOptimizedInputSchema(); // when null-value opt is used, then this is different! hence apply!
            } else {
                readSchema = planner.input_node()->getInputSchema();
            }
            logger.debug("read schema is: " + readSchema.getRowType().desc());
            path_ctx.inputNode = planner.input_node();
            path_ctx.operators = planner.optimized_operators();
            path_ctx.outputSchema = path_ctx.operators.back()->getOutputSchema();
            path_ctx.inputSchema = path_ctx.inputNode->getOutputSchema();
            path_ctx.readSchema = readSchema;
            path_ctx.columnsToRead = std::dynamic_pointer_cast<FileInputOperator>(path_ctx.inputNode)->columnsToSerialize();
            logger.info("specialized to input:  " + path_ctx.inputSchema.getRowType().desc());
            logger.info("specialized to output: " + path_ctx.outputSchema.getRowType().desc());
            size_t numToRead = 0;
            for(auto indicator : path_ctx.columnsToRead)
                numToRead += indicator;
            logger.info("specialized code reads: " + pluralize(numToRead, "column"));
            logger.info("Specialized stage in " + std::to_string(timer.time() * 1000.0) + "ms");

            normalToGeneralMapping.clear();
            normalToGeneralMapping = planner.normalToGeneralMapping();
            return path_ctx;
        }

        TransformStage *StageBuilder::build(PhysicalPlan *plan, IBackend *backend) {
            auto& logger = Logger::instance().logger("codegen");

            TransformStage *stage = new TransformStage(plan, backend, _stageNumber, _allowUndefinedBehavior);

            bool mem2mem = _inputMode == EndPointMode::MEMORY && _outputMode == EndPointMode::MEMORY;

            JobMetrics* metrics = stage->PhysicalStage::plan() ? &stage->PhysicalStage::plan()->getContext().metrics()
                                                               : nullptr;
            Timer timer;
            if (_operators.empty() && mem2mem) {
                stage->_pyCode = "";
                TransformStage::StageCodePath slow;
                TransformStage::StageCodePath fast;
                stage->_slowCodePath = slow;
                stage->_fastCodePath = fast;
            } else {
                // this here is the code-generation part

                // 1. fetch general code generation context
                auto codeGenerationContext = createCodeGenerationContext();

                logger.debug("TODO: Need to add specialization/optimization here...");

                // need to set codeGenerationContext.normalToGeneralMapping here as well!
                // 2. specialize fast path (if desired)
                codeGenerationContext.slowPathContext = getGeneralPathContext();
                if(_generateNormalCaseCodePath)
                    codeGenerationContext.fastPathContext = specializePipeline(codeGenerationContext.slowPathContext,
                                                                               codeGenerationContext.normalToGeneralMapping,
                                                                               _nullValueOptimization,
                                                                               _constantFoldingOptimization);
                else
                    codeGenerationContext.fastPathContext = getGeneralPathContext();

                // 3. fill in general case codepath context
                python::Type normalCaseInputRowType = codeGenerationContext.fastPathContext.inputSchema.getRowType(); // if NO normal-case is specialized, generated use this

                // kick off slow path generation
                std::shared_future<TransformStage::StageCodePath> slowCodePath_f = std::async(std::launch::async, [this,
                                                                                                                   &codeGenerationContext,
                                                                                                                   &normalCaseInputRowType]() {
                    return generateResolveCodePath(codeGenerationContext,
                                                   codeGenerationContext.slowPathContext,
                                                   normalCaseInputRowType,
                                                   codeGenerationContext.normalToGeneralMapping);
                });

                auto py_path = generatePythonCode(codeGenerationContext, number());
                stage->_pyCode = py_path.pyCode;
                stage->_pyPipelineName = py_path.pyPipelineName;

                // wait for threads to finish generating the two paths...!

                // general case input row type is the input schema, but for files the read schema.
                auto generalCaseInputRowType = codeGenerationContext.slowPathContext.inputSchema.getRowType();
                if(codeGenerationContext.slowPathContext.inputNode->type() == LogicalOperatorType::FILEINPUT)
                    generalCaseInputRowType = codeGenerationContext.slowPathContext.readSchema.getRowType();

                python::Type restrictedGeneralCaseInputRowType = generalCaseInputRowType;
                // print out restricted stuff
                std::stringstream ss;
                ss<<"general case input row type: "<<codeGenerationContext.slowPathContext.inputSchema.getRowType().desc()<<std::endl;
                ss<<"has #columns: "<<codeGenerationContext.slowPathContext.inputSchema.getRowType().parameters().size()<<std::endl;
                ss<<"general case input row type: "<<codeGenerationContext.fastPathContext.inputSchema.getRowType().desc()<<std::endl;
                ss<<"has #columns: "<<codeGenerationContext.fastPathContext.inputSchema.getRowType().parameters().size()<<std::endl;
                logger.debug(ss.str());

                stage->_fastCodePath = generateFastCodePath(codeGenerationContext,
                                                            codeGenerationContext.fastPathContext,
                                                            generalCaseInputRowType,
                                                            codeGenerationContext.slowPathContext.columnsToRead,
                                                            codeGenerationContext.slowPathContext.outputSchema.getRowType(),
                                                            codeGenerationContext.normalToGeneralMapping,
                                                            number());
                stage->_slowCodePath = slowCodePath_f.get();
            }

            // fill parameters from builder
            fillStageParameters(stage);

            // DEBUG, write out generated trafo code...
#ifndef NDEBUG
            stringToFile(URI("fastpath_transform_stage_" + std::to_string(_stageNumber) + ".txt"), stage->fastPathCode());
            stringToFile(URI("slowpath_transform_stage_" + std::to_string(_stageNumber) + ".txt"), stage->slowPathCode());
#endif
            if(metrics)
                metrics->setGenerateLLVMTime(timer.time());
            return stage;
        }

        python::Type intermediateType(const std::vector<std::shared_ptr<LogicalOperator>>& operators) {
            // output node aggregate? --> save intermediate schema!
            if(!operators.empty() && operators.back()) {
                auto output_node = operators.back();
                if(output_node->type() == LogicalOperatorType::AGGREGATE) {
                    auto aop = std::dynamic_pointer_cast<AggregateOperator>(output_node);
                    if(aop->aggType() == AggregateType::AGG_GENERAL) {
                        return aop->getOutputSchema().getRowType();
                    }
                }
            }
            return python::Type::UNKNOWN;
        }

        std::vector<int64_t>
        StageBuilder::getOperatorIDsAffectedByResolvers(const std::vector<std::shared_ptr<LogicalOperator>> &operators) {
            if(operators.empty())
                return std::vector<int64_t>();
            std::set<int64_t> unique_ids;
            for(const auto &op : operators) {
                assert(op);
                if(op->type() == LogicalOperatorType::RESOLVE) {
                    // get normal parent!
                    // => then search from there until resolve node is found.
                    auto np = ((ResolveOperator*)op.get())->getNormalParent();
                    std::queue<std::shared_ptr<LogicalOperator>> q; q.push(np);
                    while(!q.empty() && q.front() && q.front()->getID() != op->getID()) {
                        auto node = q.front(); q.pop();
                        if(node) {
                            if(node->getID() == op->getID())
                                break;
                            unique_ids.insert(node->getID());

                            for(auto c : node->children())
                                q.push(c);
                        }
                    }
                }
            }

            auto ids = std::vector<int64_t>(unique_ids.begin(), unique_ids.end());
            std::sort(ids.begin(), ids.end());
            return ids;
        }

        //@TODO: refactor this design AFTER paper deadline.
        CodeGenerationContext StageBuilder::createCodeGenerationContext() const {
            CodeGenerationContext ctx;
            // copy common attributes first
            ctx.allowUndefinedBehavior = _allowUndefinedBehavior;
            ctx.sharedObjectPropagation = _sharedObjectPropagation;
            ctx.nullValueOptimization = _nullValueOptimization;
            ctx.constantFoldingOptimization = _constantFoldingOptimization;
            ctx.isRootStage = _isRootStage;
            ctx.generateParser = _generateParser;
            ctx.normalCaseThreshold = _normalCaseThreshold;

            // output params
            ctx.outputMode = _outputMode;
            ctx.outputFileFormat = _outputFileFormat;
            ctx.outputNodeID = _outputNodeID;
            ctx.outputSchema = _outputSchema; //! final output schema of stage
            ctx.fileOutputParameters = _fileOutputParameters; // parameters specific for a file output format
            ctx.outputLimit = _outputLimit;

            // hash output parameters
            ctx.hashColKeys = _hashColKeys;
            ctx.hashKeyType = _hashKeyType;
            ctx.hashSaveOthers = _hashSaveOthers;
            ctx.hashAggregate = _hashAggregate;

            // input params
            ctx.inputMode = _inputMode;
            ctx.inputFileFormat = _inputFileFormat;
            ctx.inputNodeID = _inputNodeID;
            ctx.fileInputParameters = _fileInputParameters;

            // the others are the fast & slow path context objects => to be filled by pipelines!
            return ctx;
        }

        CodeGenerationContext::CodePathContext StageBuilder::getGeneralPathContext() const {
            CodeGenerationContext::CodePathContext cp;

            // simply use the schemas from the operators as given
            cp.readSchema = _readSchema;
            cp.inputSchema = _inputSchema;
            cp.outputSchema = _outputSchema;

            cp.inputNode = _inputNode;
            cp.operators = _operators;
            cp.columnsToRead = _columnsToRead;
            return cp;
        }

        TransformStage* StageBuilder::encodeForSpecialization(PhysicalPlan* plan, IBackend* backend,
                                                              bool gen_py_code,
                                                              bool gen_fast_code,
                                                              bool gen_slow_code) {
            // do not generate code-paths, rather store the info necessary to store stuff.
            // then send this off
            auto &logger = Logger::instance().logger("codegen");
            // only allow for single operators/end modes etc.
            logger.info("hyper specialization encoding");

            TransformStage *stage = new TransformStage(plan, backend, _stageNumber, _allowUndefinedBehavior);
            bool mem2mem = _inputMode == EndPointMode::MEMORY && _outputMode == EndPointMode::MEMORY;

            fillStageParameters(stage);

            if (_operators.empty() && mem2mem) {
                stage->_pyCode = "";
                TransformStage::StageCodePath slow;
                TransformStage::StageCodePath fast;
                stage->_slowCodePath = slow;
                stage->_fastCodePath = fast;
            } else {
                // normally code & specialization would happen here - yet in hyper-specializaiton mode this is postponed to the executor

                // basically just need to get general settings & general path and then encode that!
                auto ctx = createCodeGenerationContext();
                ctx.slowPathContext = getGeneralPathContext();

                // also important to encode into stage python code. Else, nowhere to be found!
                if(gen_py_code) {
                    auto py_path = generatePythonCode(ctx, number());
                    stage->_pyCode = py_path.pyCode;
                    stage->_pyPipelineName = py_path.pyPipelineName;
                }

                // use fast path context
                ctx.fastPathContext = ctx.slowPathContext;


                // general case input row type is the input schema, but for files the read schema.
                auto generalCaseInputRowType = ctx.slowPathContext.inputSchema.getRowType();
                if(ctx.slowPathContext.inputNode->type() == LogicalOperatorType::FILEINPUT)
                    generalCaseInputRowType = ctx.slowPathContext.readSchema.getRowType();
                python::Type normalCaseInputRowType = ctx.fastPathContext.inputSchema.getRowType(); // if no specific fastPath Context was created, simply use this

                if(gen_fast_code) {
                    stage->_fastCodePath = generateFastCodePath(ctx,
                                                                ctx.fastPathContext,
                                                                generalCaseInputRowType,
                                                                ctx.slowPathContext.columnsToRead,
                                                                ctx.slowPathContext.outputSchema.getRowType(),
                                                                ctx.normalToGeneralMapping,
                                                                number());
                }

                if(gen_slow_code) {
                    stage->_slowCodePath = generateResolveCodePath(ctx, ctx.slowPathContext, normalCaseInputRowType, ctx.normalToGeneralMapping);
                }

#ifndef NDEBUG
                stringToFile("python_code_" + stage->_pyPipelineName + ".py", stage->_pyCode);
#endif

#ifdef BUILD_WITH_CEREAL
                // use test-wise cereal to encode the context (i.e., the stage) to send
                // over to individual executors for specialization.
                std::ostringstream oss(std::stringstream::binary);
                {
                    cereal::BinaryOutputArchive ar(oss);
                    ar(ctx);
                    // ar going out of scope flushes everything
                }
                auto bytes_str = oss.str();
#else
                std::string bytes_str;

                // use custom written JSON serialization routine
                bytes_str = ctx.toJSON();
#endif
                logger.info("Serialized CodeGeneration Context to " + sizeToMemString(bytes_str.size()));
                // compress this now using zip or so...
                // https://gist.github.com/gomons/9d446024fbb7ccb6536ab984e29e154a
                auto compressed_cg_str = compress_string(bytes_str);
                logger.info("ZLIB compressed CodeGeneration Context is: " + sizeToMemString(compressed_cg_str.size()));
                // @TODO: remove the hacky stuff!

                #ifndef NDEBUG
                    // validate result
                    auto decompressed_str = decompress_string(compressed_cg_str);
                    if(decompressed_str != bytes_str)
                        logger.error("decompressed string doesn't match compressed one.");
                #endif

//                auto json_str = ctx.toJSON();
//                logger.info("serialized stage as JSON string (TODO: make this better, more efficient, ...");
                // stage->_encodedData = json_str; // hack
                stage->_encodedData = compressed_cg_str; // the codegen context
            }

            return stage;
        }
    }
}