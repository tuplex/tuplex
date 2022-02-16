//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/PhysicalPlan.h>
#include <physical/TransformStage.h>
#include <physical/HashJoinStage.h>
#include <physical/AggregateStage.h>
#include <physical/StageBuilder.h>
#include <logical/ParallelizeOperator.h>
#include <logical/FileInputOperator.h>
#include <logical/MapOperator.h>
#include <logical/FilterOperator.h>
#include <logical/TakeOperator.h>
#include <logical/ResolveOperator.h>
#include <logical/IgnoreOperator.h>
#include <logical/MapColumnOperator.h>
#include <logical/JoinOperator.h>
#include <logical/AggregateOperator.h>
#include <logical/CacheOperator.h>
#include <RuntimeInterface.h>
#include <Signals.h>

namespace tuplex {

    PhysicalPlan::PhysicalPlan(tuplex::LogicalPlan *optimizedPlan, tuplex::LogicalPlan *originalPlan, const Context& context)
            : _context(context), _num_stages(0) {

        assert(optimizedPlan && originalPlan);

        _lp = optimizedPlan;
        _lpOriginal = originalPlan;

        // split logical plan into physical stages
        // (@TODO: add here database like plan selection/optimization, for now naive generation)
        _stage = splitIntoAndPlanStages();

        // @TODO: push down limits
        // ==> add logic here!
    }

    // Notes: pushdown limits if possible
    // void TransformStage::pushDownOutputLimit() {
    //        // if there is NO filter operation in this stage, we can push down the output limit to the inputs
    //        bool filterOpFound = std::any_of(_operators.begin(), _operators.end(), [](LogicalOperator* op) { return op->type() == LogicalOperatorType::FILTER; });
    //
    //        if(!filterOpFound) {
    //            // limit input
    //            _inputLimit = _outputLimit;
    //        }
    //    }


    PhysicalStage* PhysicalPlan::createStage(LogicalOperator* root, LogicalOperator* endNode=nullptr,
                                             bool isRootStage=true,
                                             EndPointMode outMode=EndPointMode::UNKNOWN) {
        using namespace std;

        // Indicators for stage
        bool hasFilter = false;
        bool hasInputExceptions = false;
        // step 1: break up pipeline according to cost model
        // go through nodes
        queue<LogicalOperator*> q; q.push(root);
        deque<LogicalOperator*> ops;
        // add endNode if desired (necessary for aggregates)
        if(endNode)
            ops.emplace_back(endNode);
        vector<PhysicalStage*> dependents;
        while(!q.empty()) {
            auto node = q.front(); q.pop();

            // pass hashmaps & Co as global variables!
            // => makes life easier!
            if(node->parents().size() == 1) {
                // simple, add to ops, no dependent yet
                // special case cache operator, i.e. stop following, if there are already operators!
                // however, go dag up if cache is the first operator!
                if(node->type() == LogicalOperatorType::CACHE) {

                    // is the cache operator an action or a source?
                    // => simply check whether it's been cached
                    auto cop = (CacheOperator*)node;
                    if(cop->isCached()) {
#ifndef NDEBUG
                        // stop & get row count (this will refresh partitions as well)
                        _logger.info("reusing cached results (" + pluralize(cop->getTotalCachedRows(), "row") + ")");
#endif
                    } else {
                        // must be an action => execute!
                        assert(ops.empty() && isRootStage);
                        q.push(node->parent());
                    }

                    // do nothing
                } else if(node->type() == LogicalOperatorType::AGGREGATE) {
                    auto aop = dynamic_cast<AggregateOperator*>(node); assert(aop);
                    if(aop->aggType() == AggregateType::AGG_UNIQUE) {
                        dependents.emplace_back(createStage(aop->parent(), aop, false, EndPointMode::HASHTABLE)); // hashtable because we need to find unique rows
                        continue; // do not push this node to the stage!
                        // don't push to queue: we're done recursing backwards (the call above handles the remainder of the path)
                    } else if(aop->aggType() == AggregateType::AGG_GENERAL) {
                        // create new stage, but sure to add node first
                        dependents.emplace_back(createStage(aop->parent(), aop, false, EndPointMode::MEMORY)); // memory b.c. it's not a hashby aggregate...
                        continue; // do not push this node to the stage!
                    } else if(aop->aggType() == AggregateType::AGG_BYKEY) {
                        dependents.emplace_back(createStage(aop->parent(), aop, false, EndPointMode::HASHTABLE)); // hashtable because we need to group by key
                        continue; // do not push this node to the stage!
                    } else {
                        throw std::runtime_error("unsupported aggregate type found");
                    }
                } else { // follow tree
                    if (node->type() == LogicalOperatorType::FILTER)
                        hasFilter = true;
                    q.push(node->parent());
                }

                // do not add resolvers in fast path
                ops.push_front(node);
            } else if(node->parents().size() == 0) {
                ops.push_front(node);
                // done
            } else if(node->parents().size() == 2) {
                // more difficult, go down more costly path and spill off separate stage depending on operator!
                // todo
                assert(node->type() == LogicalOperatorType::JOIN); // only join yet supported here...

                // cost model necessary here (simple, just use #rows!)
                // there should be something like build left, build right
                JoinOperator* jop = dynamic_cast<JoinOperator*>(node); assert(jop);

                // HACK here:
                // right join is not yet implemented, however when build direction is switched for left join,
                // it's basically a right join.
                // => could implement by removing entries from hash-table and then writing with null-vals all entries out
                // which haven't been flagged.
                // => requires more complicated logic. Not done yet.

                if(jop->joinType() == JoinType::INNER) {
                    // build on whichever has the lower cost (prefer right to build for equal cost)
                    if(jop->left()->cost() >= jop->right()->cost()) {
                        // build right
                        dependents.emplace_back(createStage(jop->right(), nullptr, false, EndPointMode::HASHTABLE));

                        // continue pipeline on left
                        ops.push_front(jop);
                        q.push(jop->left());
                    } else {
                        // build left
                        dependents.emplace_back(createStage(jop->left(), nullptr, false, EndPointMode::HASHTABLE));

                        // continue pipeline on right
                        ops.push_front(jop);
                        q.push(jop->right());
                    }
                } else {
                    // always build right side for left join
                    assert(jop->joinType() == JoinType::LEFT);

#ifndef NDEBUG
                    if(jop->left()->cost() < jop->right()->cost()) {
                        Logger::instance().defaultLogger().info("Query optimizer bug: Left join build forced"
                                                                " to right side, though left would be better. Need to implement right join to resolve");
                    }
#endif
                    // build right
                    dependents.emplace_back(createStage(jop->right(), nullptr, false, EndPointMode::HASHTABLE));

                    // continue pipeline on left
                    ops.push_front(jop);
                    q.push(jop->left());
                }
            } else
                throw std::runtime_error("operator " + node->name() + " not yet supported...");
        }

        // step 2:
        // compile stage after split was performed!
        EndPointMode inputMode = EndPointMode::MEMORY; // stage input mode
        EndPointMode outputMode = EndPointMode::MEMORY; // stage output mode
        // the type of hash-grouped intermediate data (e.g. unique vs. hash aggregate
        //   --> indicates how the hashtable values should be converted to partitions)
        auto hashGroupedDataType = AggregateType::AGG_NONE;

        assert(!ops.empty());
        LogicalOperator* ioNode = nullptr; // needed to reconstruct when IO is separated from rest
        if(ops.front()->isDataSource()) {
            // stream data from source into memory
            inputMode = ops.front()->type() == LogicalOperatorType::FILEINPUT ? EndPointMode::FILE : EndPointMode::MEMORY;
            ioNode = ops.front();
            if(inputMode == EndPointMode::MEMORY) {
                // type should be parallelize or cache!
                auto t = ops.front()->type();
                assert(t == LogicalOperatorType::PARALLELIZE || t == LogicalOperatorType::CACHE);
                if (t == LogicalOperatorType::PARALLELIZE)
                    hasInputExceptions = !((ParallelizeOperator *)ops.front())->getPythonObjects().empty();
                if (t == LogicalOperatorType::CACHE)
                    hasInputExceptions = !((CacheOperator *)ops.front())->cachedExceptions().empty();
            }
        }

        if(ops.back()->isActionable()) {
            if(ops.back()->type() == LogicalOperatorType::FILEOUTPUT)
                outputMode = EndPointMode::FILE;
            else if(ops.back()->type() == LogicalOperatorType::TAKE ||
                    ops.back()->type() == LogicalOperatorType::CACHE) {
               // memory?
               outputMode = EndPointMode::MEMORY;
            } else
                throw std::runtime_error("unknown actionable operator " + ops.back()->name() + " found");
        } else {
            // it's an internal node. Thus, the question is what type of node comes after?
            // => decide between memory or hashtable as output format!
            // (i.e. union op would have memory, join hashtable)
            auto lastOp = ops.back();

            // make sure only one child!
            assert(lastOp->getChildren().size() == 1);
            auto child = lastOp->getChildren().front();

            // join op? => then hash table, else memory
            if(child->type() == LogicalOperatorType::JOIN || child->type() == LogicalOperatorType::AGGREGATE)
                outputMode = EndPointMode::HASHTABLE;
            else
                outputMode = EndPointMode::MEMORY;
        }

        // overwrite outputMode, if explicitly given
        if(outMode != EndPointMode::UNKNOWN) {
            outputMode = outMode;
        }

        // Need to update indices input exceptions in the case that input exceptions exist, the pipeline has filters, and the
        // user wants to merge exceptions in order.
        bool updateInputExceptions = hasFilter && hasInputExceptions && _context.getOptions().OPT_MERGE_EXCEPTIONS_INORDER();

        // create trafostage via builder pattern
        auto builder = codegen::StageBuilder(_num_stages++,
                                               isRootStage,
                                               _context.getOptions().UNDEFINED_BEHAVIOR_FOR_OPERATORS(),
                                               _context.getOptions().OPT_GENERATE_PARSER(),
                                               _context.getOptions().NORMALCASE_THRESHOLD(),
                                               _context.getOptions().OPT_SHARED_OBJECT_PROPAGATION(),
                                               _context.getOptions().OPT_NULLVALUE_OPTIMIZATION(),
                                               updateInputExceptions);
        // start code generation

        // first, add input
        assert(!ops.empty());
        auto inputNode = ops.front();
        switch(inputMode) {
            case EndPointMode::FILE: {
                assert(inputNode->type() == LogicalOperatorType::FILEINPUT);
                builder.addFileInput(dynamic_cast<FileInputOperator*>(inputNode));
                break;
            }
            case EndPointMode::MEMORY: {
                // note: can be parallelize or any other node if its internal!
                // => has to be output schema here of parent because of Map Operator!
                Schema inSchema;
                if(ioNode)
                    inSchema = ioNode->getOutputSchema();
                else
                    inSchema = inputNode->parents().empty() ? inputNode->getOutputSchema() : inputNode->parent()->getOutputSchema();
                builder.addMemoryInput(inSchema, inputNode);
                break;
            }
            default: {
                throw std::runtime_error("something wrong in setting inputMode, must be memory or file");
                break;
            }
        }

        // add operators
        for(auto op : ops) {
            switch(op->type()) {
                case LogicalOperatorType::FILEINPUT:
                case LogicalOperatorType::PARALLELIZE: {
                    // skip, handled above...
                    break;
                }
                case LogicalOperatorType::CACHE: {
                    // special case here: cache needs to be passed too, in order to generate certain stuff
                    builder.addOperator(op);
                    break;
                }
                case LogicalOperatorType::MAP:
                case LogicalOperatorType::FILTER:
                case LogicalOperatorType::MAPCOLUMN:
                case LogicalOperatorType::WITHCOLUMN:
                case LogicalOperatorType::JOIN:
                case LogicalOperatorType::AGGREGATE: {
                    builder.addOperator(op);
                    break;
                }

                // note: ignore/resolve should be part of slow code path only
                case LogicalOperatorType::RESOLVE:
                case LogicalOperatorType::IGNORE: {
                    builder.addOperator(op);
                    break;
                }

                default:
                    // skip
                    break;
            }
        }

        // add output writer (depending on op)
        assert(!ops.empty());
        auto outputNode = ops.back(); assert(outputNode);
        // what is the detected outputMode?
        switch(outputMode) {
            case EndPointMode::FILE: {
                builder.addFileOutput(dynamic_cast<FileOutputOperator*>(outputNode));
                break;
            }
            case EndPointMode::MEMORY: {
                auto outputDataSetID = outputNode->getDataSet() ? outputNode->getDataSet()->getID() : 0;
                    builder.addMemoryOutput(outputNode->getOutputSchema(), outputNode->getID(),
                                            outputDataSetID);

                // is lat node aggregate?
                if(outputNode->type() == LogicalOperatorType::AGGREGATE) {
                    auto aop = dynamic_cast<AggregateOperator*>(outputNode); assert(aop);

                    // save output mode (unique + bykey imply hashtable output)
                    if(aop->aggType() == AggregateType::AGG_GENERAL)
                        hashGroupedDataType = AggregateType::AGG_GENERAL;
                }

                break;
            }
            case EndPointMode::HASHTABLE: {
                // get child node
                assert(outputNode->getChildren().size() == 1);
                auto child = outputNode->getChildren().front(); assert(child);
                // two options:

                // 1.) Join (b.c. build!)
                // 2.) Aggregate => i.e. unique aggregate
                if(child->type() == LogicalOperatorType::JOIN) {
                    auto jop = dynamic_cast<JoinOperator*>(child); assert(jop);
                    // build left or right?
                    size_t keyCol = jop->buildRight() ? jop->rightKeyIndex() : jop->leftKeyIndex();
                    auto schema = jop->buildRight() ? jop->right()->getOutputSchema() : jop->left()->getOutputSchema();
                    builder.addHashTableOutput(schema, true, false, {keyCol}, jop->keyType(), jop->bucketType()); // using keycol
                }
                // is output node hashtable?
                else if(outputNode->type() == LogicalOperatorType::AGGREGATE) {
                    // hash unique?
                    // note: need to hash multiple key cols -> use tuple to string functionality for now, need to improve
                    // TODO: base64 encode maybe if its multiple columns so string can be reused...
                    auto aop = dynamic_cast<AggregateOperator*>(outputNode); assert(aop);
                    auto schema = aop->parent()->getOutputSchema();
                    if(aop->aggType() == AggregateType::AGG_UNIQUE) {
                        // @TODO: maybe do this via intermediates? And then combine everything?
                        builder.addHashTableOutput(schema, false, false, {}, schema.getRowType(),
                                                   python::Type::UNKNOWN);
                        hashGroupedDataType = AggregateType::AGG_UNIQUE; // need to process the output hashtable to rows
                    } else if(aop->aggType() == AggregateType::AGG_BYKEY) {
                        // Builds intermediate hashtable outputs with the aggregate function, and then merges them with the combiner
                        builder.addHashTableOutput(outputNode->getOutputSchema(), false, true, aop->keyColsInParent(), aop->keyType(), aop->aggregateOutputType());
                        hashGroupedDataType = AggregateType::AGG_BYKEY;
                    } else throw std::runtime_error("wrong aggregate type!");
                } else
                    throw std::runtime_error("output mode of operator " + outputNode->name() + " is hashtable, not supported");
                break;
            }
            default: {
                throw std::runtime_error("unknown endpoint encountered");
            }
        }

        // set limit if output node has a limit (currently only TakeOperator)
        if(outputNode->type() == LogicalOperatorType::TAKE) {
            auto top = static_cast<TakeOperator*>(outputNode);
            builder.setOutputLimit(top->limit());
        }

        // @TODO: add slowPip builder to this process...

        // generate code for stage and init vars
        auto stage = builder.build(this, backend());
        // converting deque of ops to vector of ops and set to stage. Note these are the optimized operators. (correct?)
        stage->setOperators(std::vector<LogicalOperator*>(ops.begin(), ops.end()));
        stage->setDataAggregationMode(hashGroupedDataType);
        // fill in physical plan data
        // b.c. the stages were constructed top-down, need to reverse the stages
        std::reverse(dependents.begin(), dependents.end());
        stage->dependOn(dependents);

        assert(!ops.empty());
        // this is the last stage. Check whether it is an input stage & set variables
        // fill in data to start processing from operators.
        if (inputNode->type() == LogicalOperatorType::PARALLELIZE) {
            auto pop = dynamic_cast<ParallelizeOperator *>(inputNode); assert(inputNode);
            stage->setInputPartitions(pop->getPartitions());
            stage->setInputExceptions(pop->getPythonObjects());
            stage->setPartitionToExceptionsMap(pop->getInputPartitionToPythonObjectsMap());
        } else if(inputNode->type() == LogicalOperatorType::CACHE) {
            auto cop = dynamic_cast<CacheOperator*>(inputNode);  assert(inputNode);
            stage->setInputPartitions(cop->cachedPartitions());
            stage->setInputExceptions(cop->cachedExceptions());
            stage->setPartitionToExceptionsMap(cop->partitionToExceptionsMap());
        } else if(inputNode->type() == LogicalOperatorType::FILEINPUT) {
            auto csvop = dynamic_cast<FileInputOperator*>(inputNode);
            stage->setInputFiles(csvop->getURIs(), csvop->getURISizes());
        } // else it must be an internal node! => need to set manually based on result

        return stage;
    }

    PhysicalStage *PhysicalPlan::splitIntoAndPlanStages() {

        using namespace std;

        // check that node is a leaf
        if (!_lp->getAction()->isLeaf())
            throw std::runtime_error("logical plan's action node is NOT leaf node. abort.");

#ifndef NDEBUG
#ifdef GENERATE_PDFS
        _lp->toPDF("logical_plan.pdf");
#else
        Logger::instance().defaultLogger().debug("saving logical plan to PDF skipped.");
#endif
#endif

        // now, go through nodes & split into stages!
        _num_stages = 0; // used as generator, set to 0!


        // use conservative approach, i.e. split each stage into normal/except case
        // => merging needs to reflect types therefore!
        return createStage(_lp->getAction(), nullptr, true);
    }

    PhysicalPlan::~PhysicalPlan() {
        if (_stage)
            delete _stage;
        _stage = nullptr;
    }

    std::string formatExceptions(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts, LogicalOperator* op, std::string indent) {
        using namespace std;

        std::stringstream ss; // hold exception text
        // find all exceptions for this operator
        vector<tuple<string, string>> rows;
        for(auto keyval : ecounts) {
            if(std::get<0>(keyval.first) == op->getID())
                rows.push_back(make_tuple(exceptionCodeToPythonClass(std::get<1>(keyval.first)), to_string(keyval.second)));
        }


#ifndef NDEBUG
        // // print out UDF from op if available
        // if(hasUDF(op) && !rows.empty()) {
        //     UDFOperator *udfop = (UDFOperator*)op;
        //     ss<<"code:\n"<<udfop->getUDF().getCode()<<"\n";
        // }
#endif

        // format
        if(!rows.empty()) {
            // get max-width for each col
            size_t mw_ec = 0, mw_cnt = 0;
            for(auto r : rows) {
                mw_ec = std::max(mw_ec, std::get<0>(r).length());
                mw_cnt = std::max(mw_cnt, std::get<1>(r).length());
            }

            // sort rows after exception class
            std::sort(rows.begin(), rows.end(), [](const std::tuple<std::string, std::string>& a,
                                                   const std::tuple<std::string, std::string>& b){
                auto sa = std::get<0>(a);
                auto sb = std::get<1>(b);
                return lexicographical_compare(sa.begin(), sa.end(), sb.begin(), sb.end());
            });


            // other, more compact format could be
            // +- parallelize
            //|- withColumn
            //   ~> index error : 1
            //   ~> value error : 10
            //|  +- parallelize
            //      ~> index error : 1
            //      ~> value error : 10
            //| /
            //|/ join
            //+- collect

            // print out nicely formatted table
            for(auto r : rows) {

                auto ec = std::get<0>(r);
                auto count = std::get<1>(r);
                ss<<indent; // indent
                // line 1
                ss<<"+";
                for(int i = 0; i < mw_ec + 2; ++i)ss<<"-";
                ss<<"+";
                for(int i = 0; i < mw_cnt + 2; ++i)ss<<"-";
                ss<<"+\n";
                // line 2

                // left justify string
                ss<<indent<<"| "<<ec;
                for(int i = 0; i < mw_ec - ec.length(); ++i)ss<<" ";

                // right justify count
                ss<<" | ";
                for(int i = 0; i < mw_cnt - count.length(); ++i)ss<<" ";
                ss<<count<<" |\n";

            }
            // final line
            ss<<indent<<"+";
            for(int i = 0; i < mw_ec + 2; ++i)ss<<"-";
            ss<<"+";
            for(int i = 0; i < mw_cnt + 2; ++i)ss<<"-";
            ss<<"+";
        }
        return ss.str().empty() ? "" : ss.str() + "\n"; // append newline at end
    }

    void printErrorTreeHelper(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> &ecounts,
                                         LogicalOperator *op, std::string indent, bool last,
                                         std::ostream& os) {
        using namespace std;
        if(!op)
            return;

        // try to produce a graph that looks something like
        // +- parallelize
        // | + parallelize
        // | | +- parallelize
        // | | |- withColumn
        // | | /
        // | |- join
        // | /
        // |- join
        // |- map
        // +- collect

        // how many parents?
        if(op->parents().empty()) {
            os<<indent<<"+- "<<op->name()<<std::endl;
            os<<formatExceptions(ecounts, op, indent + "   ");
        } else if(op->parents().size() == 1) {
            // simple |
            printErrorTreeHelper(ecounts, op->parent(), indent, last, os);
            auto prefix = (op->isActionable() ? "+- " : "|- ");
            os<<indent<<prefix<<op->name()<<std::endl;
            os<<formatExceptions(ecounts, op, indent + "   ");
        } else if(op->parents().size() == 2) {
            // call on both parents with indent
            assert(op->type() == LogicalOperatorType::JOIN);
            printErrorTreeHelper(ecounts, op->parents()[0], indent, last, os);
            printErrorTreeHelper(ecounts, op->parents()[1], "|  " + indent, last, os);
            // no exceptions here...
            os<<indent<<"| /"<<std::endl;
            os<<indent<<"|/ "<<op->name()<<std::endl;
        } else {
            throw std::runtime_error("node with more than 2 parents found, abort.");
        }
    }

    void PhysicalPlan::execute() {
        // safety check
        if (!_stage) {
            _logger.error("no stage found, stopping execution");
            throw std::runtime_error("no planned stage, aborting execution");
        }

        // execute using backend...
        _stage->execute(_context);


        // signal check & no print of exception stats in that case
        if(check_and_forward_signals()) {
            Logger::instance().defaultLogger().error("Job interrupted.");
            return;
        }

        // exceptions found? ==> print out here using unoptimized logical plan.
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> ecounts;
        std::queue<PhysicalStage*> q;
        q.push(_stage);
        while(!q.empty()) {
            auto stage = q.front(); assert(stage);
            q.pop();

            // add all exception counts to array
            auto stage_counts = stage->exceptionCounts();
            for(auto keyval : stage_counts) {
                auto it = ecounts.find(keyval.first);
                if(it == ecounts.end())
                    ecounts[keyval.first] = keyval.second;
                else {
                    it->second += keyval.second;
                }
            }
            for(auto pred : stage->predecessors())
                q.push(pred);
        }

        // count total exceptions
        size_t numTotalExceptionsFound = 0;
        for(auto keyval : ecounts)
            numTotalExceptionsFound += keyval.second;

        // update context job statistics
        _context.metrics().totalExceptionCount = numTotalExceptionsFound;
        _context.metrics().setExceptionCounts(ecounts);

        if(numTotalExceptionsFound > 0) {
            std::stringstream ss;
            ss<<"During execution "<<pluralize(numTotalExceptionsFound, "exception")<<" occurred:\n";
            printErrorTreeHelper(ecounts, _lpOriginal->getAction(), "", false, ss);
            auto message = ss.str();
            trim(message); // remove ending new lines...
            Logger::instance().defaultLogger().info(message);
        }

        // metrics
        // aggregate sampling time from all input operators (run sequentially)
        _context.metrics().setSamplingTime(aggregateSamplingTime());

        // @TODO: update history server? make sure things work?
    }

    double PhysicalPlan::aggregateSamplingTime() const {
        // go through operators, check for fileinput and aggregate
        using namespace std;
        queue<LogicalOperator*> q;
        q.push(_lpOriginal->getAction());
        double aggregate_time = 0.0;
        while(!q.empty()) {
            auto op = q.front();
            q.pop();
            for(auto p : op->parents())
                q.push(p);
            if(op->type() == LogicalOperatorType::FILEINPUT)
                aggregate_time += ((FileInputOperator*)op)->samplingTime();
        }
        return aggregate_time;
    }

    std::shared_ptr<ResultSet> PhysicalPlan::resultSet() {
        // fetch result set from stage
        // if stage failed, return empty result set
        if (!_stage)
            return std::make_shared<ResultSet>(Schema::UNKNOWN, std::vector<Partition *>());

        auto rs = _stage->resultSet();
        assert(rs);
        return rs;
    }

    void PhysicalPlan::foreachStage(std::function<void(const PhysicalStage *)> func) const {
        std::queue<PhysicalStage *> q;

        // BFS
        if (_stage) {
            q.push(_stage);
        }

        while (!q.empty()) {
            auto s = q.front();
            q.pop();

            // call function (pre)
            func(s);

            for (auto p : s->predecessors())
                if (p)
                    q.push(p);
        }
    }

    nlohmann::json PhysicalPlan::getStagedRepresentationAsJSON() const {
        using namespace std;
        using namespace nlohmann;

        vector<json> v;

        foreachStage([&](const PhysicalStage *stage) {
            v.emplace_back(stage->getJSON());
        });

        return json(v);
    }

    std::string PhysicalPlan::actionName() const {
        return logicalPlan()->getAction()->name();
    }
}