//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/StageBuilder.h>
#include <physical/PythonPipelineBuilder.h>
#include <physical/PipelineBuilder.h>
#include <physical/BlockBasedTaskBuilder.h>
#include <physical/CellSourceTaskBuilder.h>
#include <physical/JITCSVSourceTaskBuilder.h>
#include <physical/TuplexSourceTaskBuilder.h>
#include <physical/ExceptionSourceTaskBuilder.h>
#include <physical/AggregateFunctions.h>
#include <logical/CacheOperator.h>
#include <JSONUtils.h>
#include <CSVUtils.h>
#include <Utils.h>
#include <logical/AggregateOperator.h>

#include <limits.h>

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
                                   bool updateInputExceptions)
                : _stageNumber(stage_number), _isRootStage(rootStage), _allowUndefinedBehavior(allowUndefinedBehavior),
                  _generateParser(generateParser), _normalCaseThreshold(normalCaseThreshold), _sharedObjectPropagation(sharedObjectPropagation),
                  _nullValueOptimization(nullValueOptimization), _updateInputExceptions(updateInputExceptions),
                  _inputNode(nullptr), _outputLimit(std::numeric_limits<size_t>::max()) {
        }

        void StageBuilder::generatePythonCode() {
            // go over all operators and generate python-fallback pipeline code to be purely executed within the interpreter
            std::string funcName = "pipeline_stage_" + std::to_string(this->number());
            _pyPipelineName = funcName;
            PythonPipelineBuilder ppb(funcName);

            // check what input type of Stage is
            if(_inputMode == EndPointMode::FILE) {
                auto fop = dynamic_cast<FileInputOperator*>(_inputNode); assert(fop);
                switch (_inputFileFormat) {
                    case FileFormat::OUTFMT_CSV: {
                        ppb.cellInput(_inputNode->getID(), fop->inputColumns(), fop->null_values(), fop->typeHints(),
                                      fop->inputColumnCount(), fop->projectionMap());
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
                ppb.objInput(_inputNode->getID(), _inputNode->inputColumns());
            }

            for (auto op : _operators) {
                switch (op->type()) {
                    case LogicalOperatorType::PARALLELIZE: {
                        ppb.objInput(op->getID(), op->columns());
                        break;
                    }
                    case LogicalOperatorType::FILEINPUT: {
                        auto fileop = dynamic_cast<FileInputOperator *>(op);
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
                        auto udfop = dynamic_cast<UDFOperator *>(op); assert(udfop);
                        ppb.mapOperation(op->getID(), udfop->getUDF(), udfop->columns());
                        break;
                    }
                    case LogicalOperatorType::FILTER: {
                        ppb.filterOperation(op->getID(), dynamic_cast<UDFOperator *>(op)->getUDF());
                        break;
                    }
                    case LogicalOperatorType::MAPCOLUMN: {
                        ppb.mapColumn(op->getID(), dynamic_cast<MapColumnOperator *>(op)->columnToMap(),
                                      dynamic_cast<UDFOperator *>(op)->getUDF());
                        break;
                    }
                    case LogicalOperatorType::WITHCOLUMN: {
                        ppb.withColumn(op->getID(), dynamic_cast<WithColumnOperator *>(op)->columnToMap(),
                                       dynamic_cast<UDFOperator *>(op)->getUDF());
                        break;
                    }
                    case LogicalOperatorType::RESOLVE: {
                        ppb.resolve(op->getID(), dynamic_cast<ResolveOperator *>(op)->ecCode(),
                                    dynamic_cast<UDFOperator *>(op)->getUDF());
                        break;
                    }
                    case LogicalOperatorType::IGNORE: {
                        ppb.ignore(op->getID(), dynamic_cast<IgnoreOperator *>(op)->ecCode());
                        break;
                    }
                    case LogicalOperatorType::FILEOUTPUT: {
                        auto fop = dynamic_cast<FileOutputOperator *>(op);
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
                        assert(op == _operators.back()); // make sure it's the last one
                        // usually it's a hash aggregate, so python output.
                        ppb.pythonOutput();
                        break;
                    }
                    case LogicalOperatorType::TAKE: {
                        assert(op == _operators.back()); // make sure it's the last one
                        ppb.tuplexOutput(op->getID(), op->getOutputSchema().getRowType());
                        break;
                    }

                    case LogicalOperatorType::JOIN: {

                        // only inner & left join yet supported
                        auto jop = dynamic_cast<JoinOperator*>(op); assert(jop);

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
            if(_outputMode == EndPointMode::FILE && _outputFileFormat == FileFormat::OUTFMT_CSV) {
                // pip->buildWithCSVRowWriter(_funcMemoryWriteCallbackName, _outputNodeID, _fileOutputParameters["null_value"],
                //                                                       true, csvOutputDelimiter(), csvOutputQuotechar());
                ppb.csvOutput(csvOutputDelimiter(), csvOutputQuotechar());
            } else {
                // hashing& Co has to be done with the intermediate object.
                // no code injected here. Do it whenever the python codepath is called.
                ppb.pythonOutput();
            }

            _pyCode = ppb.getCode();
        }

        void StageBuilder::addFileInput(FileInputOperator *csvop) {

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
            _inputNode = csvop;
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


        // check type.
        void checkRowType(const python::Type& rowType) {
            assert(rowType.isTupleType());
            for(auto t : rowType.parameters())
                assert(t != python::Type::UNKNOWN);
        }

        std::vector<LogicalOperator*> specializePipeline(bool nullValueOptimization, LogicalOperator* inputNode, const std::vector<LogicalOperator*>& operators) {

            using namespace std;
            auto& logger = Logger::instance().defaultLogger();

            // only null-value opt yet supported
            if(!nullValueOptimization)
                return operators;

            // special case: cache operator! might have exceptions or no exceptions => specialize depending on that!
            // i.e. an interesting case happens when join(... .cache(), ...) is used. Then, need to upcast result to general case
            // => for now, do not support...
            // => upcasting should be done in LocalBackend.


            // no input node or input node not FileInputOperator?
            // => can't specialize...
            if(!inputNode)
                return operators;
            if(inputNode->type() != LogicalOperatorType::FILEINPUT && inputNode->type() != LogicalOperatorType::CACHE)
                return operators;

            // fetch optimized schema from input operator
            Schema opt_input_schema;
            if(inputNode->type() == LogicalOperatorType::FILEINPUT)
                opt_input_schema = dynamic_cast<FileInputOperator*>(inputNode)->getOptimizedOutputSchema();
            else if(inputNode->type() == LogicalOperatorType::CACHE)
                opt_input_schema = dynamic_cast<CacheOperator*>(inputNode)->getOptimizedOutputSchema();
            else
                throw std::runtime_error("internal error in specializing for the normal case");
            auto opt_input_rowtype = opt_input_schema.getRowType();

#ifdef VERBOSE_BUILD
            {
                stringstream ss;
                ss<<FLINESTR<<endl;
                ss<<"specializing pipeline for normal case ("<<pluralize(operators.size(), "operator")<<")"<<endl;
                ss<<"input node: "<<inputNode->name()<<endl;
                ss<<"optimized schema of input node: "<<opt_input_rowtype.desc()<<endl;
                logger.debug(ss.str());
            }
#endif

            auto last_rowtype = opt_input_rowtype;
            checkRowType(last_rowtype);

            // go through ops & specialize (leave jop as is)
            vector<LogicalOperator*> opt_ops;
            LogicalOperator* lastNode = nullptr;
            for(auto node : operators) {
                auto lastParent = opt_ops.empty() ? inputNode : opt_ops.back();
                if(!lastNode)
                    lastNode = lastParent; // set lastNode to parent to make join on fileop work!

                switch(node->type()) {
                    case LogicalOperatorType::PARALLELIZE: {
                        opt_ops.push_back(node);
                        break;
                    }
                    case LogicalOperatorType::FILEINPUT: {
                        // create here copy using normalcase!
                        auto op = dynamic_cast<FileInputOperator*>(node->clone());
                        op->useNormalCase();
                        opt_ops.push_back(op);
                        break;
                    }
                        // construct clone with parents & retype
                    case LogicalOperatorType::FILTER:
                    case LogicalOperatorType::MAP:
                    case LogicalOperatorType::MAPCOLUMN:
                    case LogicalOperatorType::WITHCOLUMN:
                    case LogicalOperatorType::IGNORE: {
                        auto op = node->clone();
                        auto oldInputType = op->getInputSchema().getRowType();
                        auto oldOutputType = op->getInputSchema().getRowType();

                        if(node->type() == LogicalOperatorType::WITHCOLUMN) {
                            auto wop = (WithColumnOperator*)node;
                            if(wop->columnToMap() == "ActualElapsedTime") {
                                std::cout<<"start checking retyping here!!!"<<std::endl;
                            }
                        }

                        checkRowType(last_rowtype);
                        // set FIRST the parent. Why? because operators like ingore depend on parent schema
                        // therefore, this needs to get updated first.
                        op->setParent(lastParent);
                        if(!op->retype({last_rowtype}))
                            throw std::runtime_error("could not retype operator " + op->name());
                        opt_ops.push_back(op);
                        opt_ops.back()->setID(node->getID());
#ifdef VERBOSE_BUILD
                        {
                            stringstream ss;
                            ss<<FLINESTR<<endl;
                            ss<<"retyped "<<op->name()<<endl;
                            ss<<"\told input type: "<<oldInputType.desc()<<endl;
                            ss<<"\told output type: "<<oldOutputType.desc()<<endl;
                            ss<<"\tnew input type: "<<op->getInputSchema().getRowType().desc()<<endl;
                            ss<<"\tnew output type: "<<op->getOutputSchema().getRowType().desc()<<endl;

                            logger.debug(ss.str());
                        }
#endif

                        break;
                    }

                    case LogicalOperatorType::JOIN: {
                        auto jop = dynamic_cast<JoinOperator *>(node);
                        assert(lastNode);

//#error "bug is here: basically if lastParent is cache, then it hasn't been cloned and thus getOutputSchema gives the general case"
//                        "output schema, however, if the cache operator is the inputnode, then optimized schema should be used..."

                        // this here is a bit more involved.
                        // I.e., is it left side or right side?
                        vector<LogicalOperator*> parents;
                        if(lastNode == jop->left()) {
                            // left side is pipeline
                            //cout<<"pipeline is left side"<<endl;

                            // i.e. leave right side as is => do not take normal case there!
                            parents.push_back(lastParent); // --> normal case on left side
                            parents.push_back(jop->right());
                        } else {
                            // right side is pipeline
                            assert(lastNode == jop->right());
                            //cout<<"pipeline is right side"<<endl;

                            // i.e. leave left side as is => do not take normal case there!
                            parents.push_back(jop->left());
                            parents.push_back(lastParent); // --> normal case on right side
                        }

                        opt_ops.push_back(new JoinOperator(parents[0], parents[1], jop->leftColumn(), jop->rightColumn(),
                                                jop->joinType(), jop->leftPrefix(), jop->leftSuffix(), jop->rightPrefix(),
                                                jop->rightSuffix()));
                        opt_ops.back()->setID(node->getID()); // so lookup map works!

//#error "need a retype operator for the join operation..."
#ifdef VERBOSE_BUILD
                        {
                            jop = (JoinOperator*)opt_ops.back();
                            stringstream ss;
                            ss<<FLINESTR<<endl;
                            ss<<"retyped "<<node->name()<<endl;
                            ss<<"\tleft type: "<<jop->left()->getOutputSchema().getRowType().desc()<<endl;
                            ss<<"\tright type: "<<jop->right()->getOutputSchema().getRowType().desc()<<endl;

                            logger.debug(ss.str());
                        }
#endif

                        break;
                    }

                    case LogicalOperatorType::RESOLVE: {
                        // ignore, just return parent. This is the fast path!
                        break;
                    }
                    case LogicalOperatorType::TAKE: {
                        opt_ops.push_back(new TakeOperator(lastParent, dynamic_cast<TakeOperator*>(node)->limit()));
                        opt_ops.back()->setID(node->getID());
                        break;
                    }
                    case LogicalOperatorType::FILEOUTPUT: {
                        auto fop = dynamic_cast<FileOutputOperator *>(node);

                        opt_ops.push_back(new FileOutputOperator(lastParent, fop->uri(), fop->udf(), fop->name(),
                                                      fop->fileFormat(), fop->options(), fop->numParts(), fop->splitSize(),
                                                      fop->limit()));
                        break;
                    }
                    case LogicalOperatorType::CACHE: {
                        // two options here: Either cache is used as last node or as source!
                        // source?
                        auto cop = (CacheOperator*)node;
                        if(!cop->getChildren().empty()) {
                            // => cache is a source, i.e. fetch optimized schema from it!
                            last_rowtype = cop->getOptimizedOutputSchema().getRowType();
                            checkRowType(last_rowtype);
                            cout<<"cache is a source: optimized schema "<<last_rowtype.desc()<<endl;

                            // use normal case & clone WITHOUT parents
                            // clone, set normal case & push back
                            cop = dynamic_cast<CacheOperator*>(cop->cloneWithoutParents());
                            cop->setOptimizedOutputType(last_rowtype);
                            cop->useNormalCase();
                        } else {
                            // cache should not have any children
                            assert(cop->getChildren().empty());
                            // => i.e. first time cache is seen, it's processed as action!
                            cout<<"cache is action, optimized schema: "<<endl;
                            cout<<"cache normal case will be: "<<last_rowtype.desc()<<endl;
                            // => reuse optimized schema!
                            cop->setOptimizedOutputType(last_rowtype);
                            // simply push back, no cloning here necessary b.c. no data is altered
                        }

                        opt_ops.push_back(cop);
                        break;
                    }
                    case LogicalOperatorType::AGGREGATE: {
                        // aggregate is currently not part of codegen, i.e. the aggregation happens when writing out the output!
                        // ==> what about aggByKey though?
                        break;
                    }
                    default: {
                        std::stringstream ss;
                        ss<<"unknown operator " + node->name() + " encountered in fast path creation";
                        logger.error(ss.str());
                        throw std::runtime_error(ss.str());
                    }
                }

                if(!opt_ops.empty()) {
                    // cout<<"last opt_op name: "<<opt_ops.back()->name()<<endl;
                    // cout<<"last opt_op output type: "<<opt_ops.back()->getOutputSchema().getRowType().desc()<<endl;
                    if(opt_ops.back()->type() != LogicalOperatorType::CACHE)
                        last_rowtype = opt_ops.back()->getOutputSchema().getRowType();
                    checkRowType(last_rowtype);
                }

                lastNode = node;
            }


            // important to have cost available
            if(!opt_ops.empty())
                assert(opt_ops.back()->cost() > 0);

            return opt_ops;
        }

        bool StageBuilder::generateFastCodePath() {
            using namespace std;


            // special case: no operators and mem2mem
            // ==> no code needed, i.e. empty stage
            bool mem2mem = _inputMode == EndPointMode::MEMORY && _outputMode == EndPointMode::MEMORY;
            if (_operators.empty() && mem2mem) {
                // _irCode = "";
                // _irResolveCode = "";
                _irBitCode = "";
                _pyCode = "";
                return true;
            }

            // null-value optimization?
            // => specialize operators of THIS stage then.
            // => joins take the general type
            auto operators = specializePipeline(_nullValueOptimization, _inputNode, _operators);

            string env_name = "tuplex_fastCodePath";
            string func_prefix = "";
            // name for function processing a row (include stage number)
            string funcStageName = func_prefix + "Stage_" + to_string(number());
            string funcProcessRowName = func_prefix + "processRow_Stage_" + to_string(number());
            _funcFileWriteCallbackName = func_prefix + "writeOut_Stage_" + to_string(number());
            _funcMemoryWriteCallbackName = func_prefix + "memOut_Stage_" + to_string(number());
            _funcHashWriteCallbackName = func_prefix + "hashOut_Stage_" + to_string(number());
            _funcExceptionCallback = func_prefix + "except_Stage_" + to_string(number());

            auto &logger = Logger::instance().logger("codegen");
            auto readSchema = _readSchema.getRowType(); // what to read from files (before projection pushdown)
            auto inSchema = _inputSchema.getRowType(); // with what to start the pipeline (after projection pushdown)
            auto outSchema = _outputSchema.getRowType(); // what to output from pipeline
            auto env = make_shared<codegen::LLVMEnvironment>(env_name);

            // per default, set normalcase to output types!
            _normalCaseInputSchema = _inputSchema;
            _normalCaseOutputSchema = _outputSchema;

            Row intermediateInitialValue; // filled by aggregate operator, if needed.

            // null-value optimization? => use input schema from operators!
            if(_nullValueOptimization) {
                if(_inputMode == EndPointMode::FILE) {
                    readSchema = dynamic_cast<FileInputOperator*>(_inputNode)->getOptimizedInputSchema().getRowType();
                    _normalCaseInputSchema = dynamic_cast<FileInputOperator*>(_inputNode)->getOptimizedOutputSchema();
                    inSchema = _normalCaseInputSchema.getRowType();
                    _normalCaseOutputSchema = _normalCaseInputSchema;
                } else if(_inputMode == EndPointMode::MEMORY && _inputNode && _inputNode->type() == LogicalOperatorType::CACHE) {
                    _normalCaseInputSchema = dynamic_cast<CacheOperator*>(_inputNode)->getOptimizedOutputSchema();
                    inSchema = _normalCaseInputSchema.getRowType();
                } else {
                    _normalCaseOutputSchema = _outputSchema;
                }

                // output schema stays the same unless it's the most outer stage...
                // i.e. might need type upgrade in the middle for inner stages as last operator
                if(_isRootStage) {
                    if(!operators.empty()) {
                        _normalCaseOutputSchema = operators.back()->getOutputSchema();

                        // special case: CacheOperator => use optimized schema!
                        auto lastOp = operators.back();
                        if(lastOp->type() == LogicalOperatorType::CACHE)
                            _normalCaseOutputSchema = ((CacheOperator*)lastOp)->getOptimizedOutputSchema();
                    }

                    outSchema = _normalCaseOutputSchema.getRowType();
                }
            }

#ifdef VERBOSE_BUILD
            {
                stringstream ss;
                ss<<FLINESTR<<endl;
                ss<<"Stage"<<this->_stageNumber<<" schemas:"<<endl;
                ss<<"\tnormal case input: "<<_normalCaseInputSchema.getRowType().desc()<<endl;
                ss<<"\tnormal case output: "<<_normalCaseOutputSchema.getRowType().desc()<<endl;
                ss<<"\tgeneral case input: "<<_inputSchema.getRowType().desc()<<endl;
                ss<<"\tgeneral case output: "<<_outputSchema.getRowType().desc()<<endl;

                logger.debug(ss.str());
            }
#endif

#ifndef NDEBUG
            if(!operators.empty()) {
                stringstream ss;
                ss<<"output type of specialized pipeline is: "<<outSchema.desc()<<endl;
                ss<<"is this the most outer stage?: "<<_isRootStage<<endl;
                if(!_isRootStage)
                    ss<<"need to upgrade output type to "<<_operators.back()->getOutputSchema().getRowType().desc()<<endl;

                logger.debug(ss.str());
            }
#endif

            assert(inSchema != python::Type::UNKNOWN);
            assert(outSchema != python::Type::UNKNOWN);

            // special case: empty pipeline
            if (outSchema.parameters().empty() && inSchema.parameters().empty()) {
                logger.info("no pipeline code generated, empty pipeline");
                return true;
            }

            // go through nodes & add operation
            logger.info("generating pipeline for " + inSchema.desc() + " -> "
                        + outSchema.desc() + " (" + pluralize(_operators.size(), "operator") + " pipelined)");

            // first node determines the data source

            // collect while going through ops what exceptions can be ignored in the normal case
            vector<tuple<int64_t, ExceptionCode>> ignoreCodes; // tuples of operatorID, code, to be ignored

            // create initstage/release stage functions (LLVM)
            using namespace llvm;
            _initStageFuncName = func_prefix + "initStage" + to_string(number());
            _releaseStageFuncName = func_prefix + "releaseStage" + to_string(number());
            auto initStageFuncType = FunctionType::get(env->i64Type(),
                                                       {env->i64Type(), env->i8ptrType()->getPointerTo(),
                                                        env->i8ptrType()->getPointerTo()}, false);
            auto releaseStageFuncType = FunctionType::get(env->i64Type(), false);

            // create functions + builders
            auto initStageFunc = cast<Function>(
                    env->getModule()->getOrInsertFunction(_initStageFuncName, initStageFuncType).getCallee());
            auto releaseStageFunc = cast<Function>(
                    env->getModule()->getOrInsertFunction(_releaseStageFuncName, releaseStageFuncType).getCallee());

            BasicBlock *bbISBody = BasicBlock::Create(env->getContext(), "", initStageFunc);
            BasicBlock *bbRSBody = BasicBlock::Create(env->getContext(), "", releaseStageFunc);
            IRBuilder<> isBuilder(bbISBody);
            IRBuilder<> rsBuilder(bbRSBody);
            auto isArgs = codegen::mapLLVMFunctionArgs(initStageFunc, {"num_args", "hashmaps", "null_buckets"});

            // step 1. build pipeline, i.e. how to process data
            auto pip = std::make_shared<codegen::PipelineBuilder>(env, inSchema, intermediateType(), funcProcessRowName);

            // Note: the pipeline function will return whether an exception occured.
            // if that happens, then call to handler in transform task builder
            // pip->addExceptionHandler(_funcExceptionCallback); // don't add a exception handler here.

            // sanity check: output of last op should match schema!
            if(!operators.empty() && outSchema != operators.back()->getOutputSchema().getRowType()) {
                cout<<"outSchema is different than last operator's schema:"<<endl;
                cout<<"outSchema: "<<outSchema.desc()<<endl;
                cout<<"last Op: "<<operators.back()->getOutputSchema().getRowType().desc()<<endl;
            }

            int global_var_cnt = 0;
            auto num_operators = operators.size();
            for (int i = 0; i < num_operators; ++i) {
                auto node = operators[i];
                assert(node);
                UDFOperator *udfop = dynamic_cast<UDFOperator *>(node);
                switch (node->type()) {
                    case LogicalOperatorType::MAP: {
                        if (!pip->mapOperation(node->getID(), udfop->getUDF(), _normalCaseThreshold, _allowUndefinedBehavior,
                                               _sharedObjectPropagation)) {
                            logger.error(formatBadUDFNode(udfop));
                            return false;
                        }
                        break;
                    }
                    case LogicalOperatorType::FILTER: {
                        if (!pip->filterOperation(node->getID(), udfop->getUDF(), _normalCaseThreshold, _allowUndefinedBehavior,
                                                  _sharedObjectPropagation)) {
                            logger.error(formatBadUDFNode(udfop));
                            return false;
                        }
                        break;
                    }
                    case LogicalOperatorType::MAPCOLUMN: {
                        auto mop = dynamic_cast<MapColumnOperator *>(node);
                        if (!pip->mapColumnOperation(node->getID(), mop->getColumnIndex(), udfop->getUDF(),
                                                     _normalCaseThreshold, _allowUndefinedBehavior, _sharedObjectPropagation)) {
                            logger.error(formatBadUDFNode(udfop));
                            return false;
                        }
                        break;
                    }
                    case LogicalOperatorType::WITHCOLUMN: {
                        auto wop = dynamic_cast<WithColumnOperator *>(node);
                        if (!pip->withColumnOperation(node->getID(), wop->getColumnIndex(), udfop->getUDF(),
                                                      _normalCaseThreshold, _allowUndefinedBehavior, _sharedObjectPropagation)) {
                            logger.error(formatBadUDFNode(udfop));
                            return false;
                        }
                        break;
                    }
                    case LogicalOperatorType::RESOLVE: {
                        // skip, will be dealt with in slow path!
                        break;
                    }
                    case LogicalOperatorType::IGNORE: {
                        auto iop = dynamic_cast<IgnoreOperator *>(node);

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
                        auto jop = dynamic_cast<JoinOperator *>(node);
                        assert(jop);

                        string hashmap_global_name =
                                func_prefix + "hash_map_" + to_string(global_var_cnt) + "_stage" + to_string(number());
                        string null_bucket_global_name =
                                func_prefix + "null_bucket_" + to_string(global_var_cnt) + "_stage" + to_string(number());

                        // add two new globals + init code to init/release func
                        auto hash_map_global = env->createNullInitializedGlobal(hashmap_global_name, env->i8ptrType());
                        auto null_bucket_global = env->createNullInitializedGlobal(null_bucket_global_name,
                                                                                   env->i8ptrType());

                        // add to lookup map for slow case
                        _hashmap_vars[jop->getID()] = make_tuple(hash_map_global, null_bucket_global);

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
                        if(_nullValueOptimization) {
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
                            return false;
                        }
                        break;
                    }
                    case LogicalOperatorType::AGGREGATE: {
                        auto aop = dynamic_cast<AggregateOperator*>(node); assert(aop);
                        if(aop->aggType() == AggregateType::AGG_GENERAL || aop->aggType() == AggregateType::AGG_BYKEY) {

                            // right now aggregation is done using a global variable.
                            // this is because of the overall compilation design
                            // in the future we should prob. rewrite this to compile better plans...
                            // writing to a pointer seems like a bad idea...

                            // NOTE: these functions need to be generated only once for the general case type!
                            auto aggType = aop->aggregateOutputType();
                            _aggregateInitFuncName = "init_aggregate_stage" + std::to_string(number());
                            _aggregateCombineFuncName = "combine_aggregate_stage" + std::to_string(number());
                            if(aop->aggType() == AggregateType::AGG_BYKEY)
                                _aggregateAggregateFuncName = "aggregate_aggregate_stage" + std::to_string(number());
                            _aggregateCallbackName = "aggregate_callback_stage" + std::to_string(number());
                            auto aggregateInitFunc = codegen::createAggregateInitFunction(env.get(),
                                                                                          _aggregateInitFuncName,
                                                                                          aop->initialValue(),
                                                                                          aggType); // use c-malloc!
                            auto combFunc = codegen::createAggregateCombineFunction(env.get(),
                                                                                    _aggregateCombineFuncName,
                                                                                    aop->combinerUDF(),
                                                                                    aggType,
                                                                                    malloc);
                            if(!aggregateInitFunc)
                                throw std::runtime_error("error compiling aggregate initialize function");
                            if(!combFunc)
                                throw std::runtime_error("error compiling combiner function for aggregate");
                            // update func names, to avoid duplicates
                            _aggregateInitFuncName = aggregateInitFunc->getName().str();
                            _aggregateCombineFuncName = combFunc->getName().str();

                            if(aop->aggType() == AggregateType::AGG_BYKEY) { // need to make the aggregate functor
                                auto aggregateFunc = codegen::createAggregateFunction(env.get(),
                                                                                      _aggregateAggregateFuncName,
                                                                                      aop->aggregatorUDF(), aggType,
                                                                                      aop->parent()->getOutputSchema().getRowType(),
                                                                                      malloc);
                                if(!aggregateFunc)
                                    throw std::runtime_error("error compiling aggregate function");
                                _aggregateAggregateFuncName = aggregateFunc->getName().str();
                            } else {
                                // init intermediate within Stage process function.
                                intermediateInitialValue = aop->initialValue();
                                if (!pip->addAggregate(aop->getID(), aop->aggregatorUDF(),
                                                       aop->getOutputSchema().getRowType(),
                                                       _normalCaseThreshold,
                                                       _allowUndefinedBehavior,
                                                       _sharedObjectPropagation)) {
                                    logger.error(formatBadAggNode(aop));
                                    return false;
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
            switch(_outputMode) {
                case EndPointMode::FILE: {
                    // for file mode, can directly merge output rows
                    //pip->buildWithTuplexWriter(_funcMemoryWriteCallbackName, _outputNodeID); //output node id

                    switch (_outputFileFormat) {
                        case FileFormat::OUTFMT_CSV: {
                            // i.e. write to memory writer!
                            pip->buildWithCSVRowWriter(_funcMemoryWriteCallbackName,
                                                       _outputNodeID,
                                                                hasOutputLimit(),
                                                       _fileOutputParameters["null_value"],
                                                       true, csvOutputDelimiter(), csvOutputQuotechar());
                            break;
                        }
                        case FileFormat::OUTFMT_ORC: {
                            pip->buildWithTuplexWriter(_funcMemoryWriteCallbackName, _outputNodeID, hasOutputLimit());
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
                    if(_nullValueOptimization) {
                        if(!leaveNormalCase) {
                            if (!pip->addTypeUpgrade(_outputSchema.getRowType()))
                                throw std::runtime_error(
                                        "type upgrade from " + outSchema.desc() + " to " +
                                        _outputSchema.getRowType().desc() + "failed.");
                            // set normal case output type to general case
                            _normalCaseOutputSchema = _outputSchema;
                        }
                    }
                    pip->buildWithHashmapWriter(_funcHashWriteCallbackName, _hashColKeys, hashtableKeyWidth(_hashKeyType), _hashSaveOthers, _hashAggregate);
                    break;
                }
                case EndPointMode::MEMORY: {

                    // special case: writing intermediate output
                    if(intermediateType() == python::Type::UNKNOWN) {
                        // NOTE: forcing output to be general case is not necessary for cache operator!
                        // => i.e. may manually convert...

                        // is outputNode a cache operator? Then leave normal case as is...
                        // => always pass cache node!
                        bool leaveNormalCase = false;

                        if(!operators.empty())
                            leaveNormalCase = operators.back()->type() == LogicalOperatorType::CACHE;

                        // force output type to be always general case (=> so merging works easily!)
                        if(_nullValueOptimization) {
                            if(!leaveNormalCase) {
                                if (!pip->addTypeUpgrade(_outputSchema.getRowType()))
                                    throw std::runtime_error(
                                            "type upgrade from " + outSchema.desc() + " to " +
                                            _outputSchema.getRowType().desc() + "failed.");
                                // set normal case output type to general case
                                _normalCaseOutputSchema = _outputSchema;
                            }
                        }
                        pip->buildWithTuplexWriter(_funcMemoryWriteCallbackName, _outputNodeID, hasOutputLimit());
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
            if (_inputMode == EndPointMode::FILE) {

                // only CSV supported yet // throw std::runtime_error("found unknown data-source operator " + node->name() + " for which a pipeline could not be generated");

                // input schema holds for CSV node the original, unoptimized number of columns.
                // if pushdown is performed, outputschema holds whatever is left.
                char delimiter = _fileInputParameters.at("delimiter")[0];
                char quotechar = _fileInputParameters.at("quotechar")[0];

                // note: null_values may be empty!
                auto null_values = jsonToStringArray(_fileInputParameters.at("null_values"));

                switch (_inputFileFormat) {
                    case FileFormat::OUTFMT_CSV:
                    case FileFormat::OUTFMT_TEXT: {
                        if (_generateParser) {
                            tb = make_shared<codegen::JITCSVSourceTaskBuilder>(env,
                                                                               readSchema,
                                                                               _columnsToRead,
                                                                               funcStageName,
                                                                               _inputNodeID,
                                                                               null_values,
                                                                               delimiter,
                                                                               quotechar);
                        } else {
                            tb = make_shared<codegen::CellSourceTaskBuilder>(env, readSchema, _columnsToRead,
                                                                             funcStageName,
                                                                             _inputNodeID, null_values);
                        }
                        break;
                    }
                    case FileFormat::OUTFMT_ORC: {
                        tb = make_shared<codegen::TuplexSourceTaskBuilder>(env, inSchema, funcStageName);
                        break;
                    }
                    default:
                        throw std::runtime_error("file input format not yet supported!");
                }
            } else {
                // tuplex (in-memory) reader
                if (_updateInputExceptions)
                    tb = make_shared<codegen::ExceptionSourceTaskBuilder>(env, inSchema, funcStageName);
                else
                    tb = make_shared<codegen::TuplexSourceTaskBuilder>(env, inSchema, funcStageName);
            }

            // set pipeline and
            // add ignore codes & exception handler
            tb->setExceptionHandler(_funcExceptionCallback);
            tb->setIgnoreCodes(ignoreCodes);
            tb->setPipeline(pip);

            // special case: intermediate
            if(intermediateType() != python::Type::UNKNOWN) {
                tb->setIntermediateInitialValueByRow(intermediateType(), intermediateInitialValue);
                tb->setIntermediateWriteCallback(_aggregateCallbackName);
            }

            // create code for "wrap-around" function
            auto func = tb->build(hasOutputLimit());
            if (!func)
                throw std::runtime_error("could not build codegen csv parser");

            // compile on top of this pipeline resolve code path if several conditons are met
            // 1.) compile if resolve function is present
            // 2.) compile if null-value optimization is present (could be done lazily)
            auto numResolveOperators = resolveOperatorCount();
            // if resolvers are present, compile a slowPath.
            bool requireSlowPath = _nullValueOptimization; // per default, slow path is always required when null-value opt is enabled.

            // special case: input source is cached and no exceptions happened => no resolve path necessary if there are no resolvers!
            if(_inputNode->type() == LogicalOperatorType::CACHE && dynamic_cast<CacheOperator*>(_inputNode)->cachedExceptions().empty())
                requireSlowPath = false;

            if (numResolveOperators > 0 || requireSlowPath) {
                // generate in same env b.c. need to have access to globals...
                generateResolveCodePath(env);
            }

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
            _funcStageName = func->getName();
            _irBitCode = codegen::moduleToBitCodeString(*env->getModule()); // trafo stage takes ownership of module

            // @TODO: lazy & fast codegen of the different paths + lowering of them
            // generate interpreter fallback path (always) --> probably do that lazily or parallelize it...
            generatePythonCode();

            return true;
        }

        bool StageBuilder::generateResolveCodePath(const std::shared_ptr<codegen::LLVMEnvironment> &env) {
            using namespace std;
            using namespace llvm;


            // when there are no operators present, there is no way to generate a resolve path
            // => skip
            if(_operators.empty() && !_nullValueOptimization) // when null value optimization is done, need to always generate resolve path.
                return true;

            // @TODO: one needs to add here somewhere an option where bad input rows/data get resolved when they do not fit the initial schema!
            //  r/n it's all squashed together in a pipeline.

            // @TODO: need to type here UDFs with commonCase/ExceptionCase type!
            // ==> i.e. the larger, unspecialized type!

            auto &logger = Logger::instance().logger("codegen");

            // Note: this here is quite confusing, because for map operator when tuples are needed, this will return not the row schema but the UDF input schema =? fix that
            // @TODO: fix getInputSchema for MapOperator!!!
            auto inSchema = _inputSchema.getRowType(); // old: _operators.front()->getInputSchema().getRowType();
            string funcSlowPathName = "processViaSlowPath_Stage_" + to_string(number());
            string funcResolveRowName = "resolveSingleRow_Stage_" + to_string(number());
            string slowPathMemoryWriteCallback = "memOutViaSlowPath_Stage_" + to_string(number());
            string slowPathHashWriteCallback = "hashOutViaSlowPath_Stage_" + to_string(number());
            string slowPathExceptionCallback = "exceptionOutViaSlowPath_Stage_" + to_string(number());

            auto slowPip = std::make_shared<codegen::PipelineBuilder>(env, inSchema, intermediateType(), funcSlowPathName);
            for (auto it = _operators.begin(); it != _operators.end(); ++it) {
                auto node = *it;
                assert(node);
                UDFOperator *udfop = dynamic_cast<UDFOperator *>(node);
                switch (node->type()) {
                    case LogicalOperatorType::MAP: {
                        slowPip->mapOperation(node->getID(), udfop->getUDF(), _normalCaseThreshold, _allowUndefinedBehavior,
                                              _sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::FILTER: {
                        slowPip->filterOperation(node->getID(), udfop->getUDF(), _normalCaseThreshold, _allowUndefinedBehavior,
                                                 _sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::MAPCOLUMN: {
                        auto mop = dynamic_cast<MapColumnOperator *>(node);
                        slowPip->mapColumnOperation(node->getID(), mop->getColumnIndex(), udfop->getUDF(),
                                                    _normalCaseThreshold, _allowUndefinedBehavior, _sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::WITHCOLUMN: {
                        auto wop = dynamic_cast<WithColumnOperator *>(node);
                        slowPip->withColumnOperation(node->getID(), wop->getColumnIndex(), udfop->getUDF(),
                                                     _normalCaseThreshold, _allowUndefinedBehavior, _sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::CACHE:
                    case LogicalOperatorType::FILEOUTPUT: {
                        // skip, exception resolution is in memory
                        break;
                    }

                    case LogicalOperatorType::RESOLVE: {
                        // ==> this means slow code path needs to be generated as well!
                        auto rop = dynamic_cast<ResolveOperator *>(node);
                        slowPip->addResolver(rop->ecCode(), rop->getID(), rop->getUDF(), _normalCaseThreshold, _allowUndefinedBehavior,
                                             _sharedObjectPropagation);
                        break;
                    }
                    case LogicalOperatorType::IGNORE: {

                        // do not skip, if one of the ancestors is a resolver (skip ignores)
                        auto iop = dynamic_cast<IgnoreOperator*>(node);
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
                        auto jop = dynamic_cast<JoinOperator *>(node);
                        assert(jop);

                        auto entry = _hashmap_vars.at(jop->getID());

                        // add two new globals + init code to init/release func
                        auto hash_map_global = std::get<0>(entry);
                        auto null_bucket_global = std::get<1>(entry);


                        if (!slowPip->addHashJoinProbe(jop->leftKeyIndex(), jop->left()->getOutputSchema().getRowType(),
                                                   jop->rightKeyIndex(),
                                                   jop->right()->getOutputSchema().getRowType(),
                                                   jop->joinType(),
                                                   jop->buildRight(),
                                                   hash_map_global,
                                                   null_bucket_global)) {
                            logger.error(formatBadUDFNode(udfop));
                            return false;
                        }
                        break;
                    }

                    case LogicalOperatorType::AGGREGATE: {
//                        auto aop = dynamic_cast<AggregateOperator *>(node); assert(aop);
//                        auto entry = _hashmap_vars.at(aop->getID());
//
//                        if(aop->aggType() == AggregateType::AGG_UNIQUE) {
//                            auto hash_map_global = std::get<0>(entry);
//                            if (!slowPip->addHashUnique(aop->parent()->getOutputSchema().getRowType(), hash_map_global)) {
//                                logger.error(formatBadUDFNode(udfop));
//                                return false;
//                            }
//                        } else {
//                            throw runtime_error("Unsupported aggregate type!");
//                        }

                        // for byKey separate logic is required to key properly
                        auto aop = dynamic_cast<AggregateOperator *>(node); assert(aop);
                        switch(aop->aggType()) {
                            case AggregateType::AGG_UNIQUE: {
                                // this is supported => do not need to specify further!
                                break;
                            }
                            default:
                                // --> need to codegen aggregate for resolve??
                                // or should this be handled directly in ResolveTask incl. function to extract potentially the key
                                // from the output buffer?
                                throw std::runtime_error("aggregateResolve not yet supported, have to think about this...");
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
            slowPip->addExceptionHandler(slowPathExceptionCallback);

            // @TODO: when supporting text output, need to include check that no newline occurs within string!
            // => else, error!


            bool useRawOutput = _outputMode == EndPointMode::FILE && _outputFileFormat == FileFormat::OUTFMT_CSV;
            // build slow path with mem writer or to CSV
            llvm::Function* slowPathFunc = nullptr;
            if(useRawOutput) {
                slowPathFunc = slowPip->buildWithCSVRowWriter(slowPathMemoryWriteCallback, _outputNodeID,
                                                              hasOutputLimit(),
                                                              _fileOutputParameters["null_value"], true,
                                                              csvOutputDelimiter(), csvOutputQuotechar());
            } else {
                // @TODO: hashwriter if hash output desired
                if(_outputMode == EndPointMode::HASHTABLE) {
                    slowPathFunc = slowPip->buildWithHashmapWriter(slowPathHashWriteCallback,
                                                                   _hashColKeys,
                                                                   hashtableKeyWidth(_hashKeyType),
                                                                   _hashSaveOthers, _hashAggregate);
                } else {
                    slowPathFunc = slowPip->buildWithTuplexWriter(slowPathMemoryWriteCallback, _outputNodeID, hasOutputLimit());
                }
            }

            // create wrapper which decodes automatically normal-case rows with optimized types...
            auto normalCaseType = _normalCaseInputSchema.getRowType();
            auto null_values =
                    _inputMode == EndPointMode::FILE ? jsonToStringArray(_fileInputParameters.at("null_values"))
                                                     : std::vector<std::string>{"None"};
            auto rowProcessFunc = codegen::createProcessExceptionRowWrapper(*slowPip, funcResolveRowName,
                                                                            normalCaseType, null_values);

            _resolveRowFunctionName = rowProcessFunc->getName();
            _resolveRowWriteCallbackName = slowPathMemoryWriteCallback;
            _resolveRowExceptionCallbackName = slowPathExceptionCallback;
            _resolveHashCallbackName = slowPathHashWriteCallback;

            return true;
        }

        void StageBuilder::addFileOutput(FileOutputOperator *fop) {
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


        void StageBuilder::addMemoryInput(const Schema &schema, LogicalOperator *node = nullptr) {
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

        TransformStage *StageBuilder::build(PhysicalPlan *plan, IBackend *backend) {

            // generate fast code path
            // --> slow code path is generated within this function (i.e. resolve & Co)
            generateFastCodePath();

            TransformStage *stage = new TransformStage(plan, backend, _stageNumber, _allowUndefinedBehavior);

            stage->_inputColumns = _inputColumns;
            stage->_outputColumns = _outputColumns;

            stage->_readSchema = _readSchema;
            stage->_inputSchema = _inputSchema;
            stage->_outputSchema = _outputSchema;
            stage->_normalCaseInputSchema = _normalCaseInputSchema;
            stage->_normalCaseOutputSchema = _normalCaseOutputSchema;
            stage->_outputDataSetID = outputDataSetID();
            stage->_inputNodeID = _inputNodeID;
            auto numColumns = stage->_readSchema.getRowType().parameters().size();
            if(_inputMode == EndPointMode::FILE && _inputNode) {
                stage->_inputColumnsToKeep = dynamic_cast<FileInputOperator*>(_inputNode)->columnsToSerialize();
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
            stage->_irBitCode = _irBitCode;
            stage->_pyCode = _pyCode;
            stage->_pyPipelineName = _pyPipelineName;
            stage->_updateInputExceptions = _updateInputExceptions;

            // if last op is CacheOperator, check whether normal/exceptional case should get cached separately
            // or an upcasting step should be performed.
            stage->_persistSeparateCases = false;
            if(!_operators.empty() && _operators.back()->type() == LogicalOperatorType::CACHE)
                stage->_persistSeparateCases = ((CacheOperator*)_operators.back())->storeSpecialized();

            // DEBUG, write out generated trafo code...
#ifndef NDEBUG
            stringToFile(URI("transform_stage_" + std::to_string(_stageNumber) + ".txt"), stage->code());
#endif

            // code names/symbols
            stage->_funcStageName = _funcStageName;
            stage->_funcMemoryWriteCallbackName = _funcMemoryWriteCallbackName;
            stage->_funcExceptionCallback = _funcExceptionCallback;
            stage->_funcFileWriteCallbackName = _funcFileWriteCallbackName;
            stage->_funcHashWriteCallbackName = _funcHashWriteCallbackName;
            stage->_writerFuncName = _writerFuncName;

            stage->_initStageFuncName = _initStageFuncName;
            stage->_releaseStageFuncName = _releaseStageFuncName;
            stage->_resolveRowFunctionName = _resolveRowFunctionName;
            stage->_resolveRowWriteCallbackName = _resolveRowWriteCallbackName;
            stage->_resolveRowExceptionCallbackName = _resolveRowExceptionCallbackName;
            stage->_resolveHashCallbackName = _resolveHashCallbackName;

            stage->_aggregateInitFuncName = _aggregateInitFuncName;
            stage->_aggregateCombineFuncName = _aggregateCombineFuncName;
            stage->_aggregateCallbackName = _aggregateCallbackName;
            stage->_aggregateAggregateFuncName = _aggregateAggregateFuncName;

            stage->_operatorIDsWithResolvers = getOperatorIDsAffectedByResolvers(_operators);

            stage->setInitData();

            return stage;
        }

        python::Type StageBuilder::intermediateType() const {
            // output node aggregate? --> save intermediate schema!
            if(!_operators.empty() && _operators.back()) {
                auto output_node = _operators.back();
                if(output_node->type() == LogicalOperatorType::AGGREGATE) {
                    auto aop = dynamic_cast<AggregateOperator*>(output_node);
                    if(aop->aggType() == AggregateType::AGG_GENERAL) {
                        return aop->getOutputSchema().getRowType();
                    }
                }
            }

            return python::Type::UNKNOWN;
        }

        std::vector<int64_t>
        StageBuilder::getOperatorIDsAffectedByResolvers(const std::vector<LogicalOperator *> &operators) {
            if(operators.empty())
                return std::vector<int64_t>();
            std::set<int64_t> unique_ids;
            for(auto op : operators) {
                assert(op);
                if(op->type() == LogicalOperatorType::RESOLVE) {
                    // get normal parent!
                    // => then search from there until resolve node is found.
                    auto np = ((ResolveOperator*)op)->getNormalParent();
                    std::queue<LogicalOperator*> q; q.push(np);
                    while(!q.empty() && q.front() && q.front()->getID() != op->getID()) {
                        auto node = q.front(); q.pop();
                        if(node) {
                            if(node->getID() == op->getID())
                                break;
                            unique_ids.insert(node->getID());

                            for(auto c : node->getChildren())
                                q.push(c);
                        }
                    }
                }
            }

            auto ids = std::vector<int64_t>(unique_ids.begin(), unique_ids.end());
            std::sort(ids.begin(), ids.end());
            return ids;
        }
    }
}