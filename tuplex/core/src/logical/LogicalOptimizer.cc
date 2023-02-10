//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2022, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 4/5/2022                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/LogicalOptimizer.h>
#include <logical/PlanProperties.h>

#include <visitors/ApplyVisitor.h>
#include <visitors/FilterBreakdownVisitor.h>


namespace tuplex {

    static bool isMapSelect(MapOperator* op) {
        // simple case: name is select, then ok.
        if(op->name() == "select")
            return true;

        // reorder case is also ok
        // I.e. under func root there is only a Tuple expression accessing columns...
        // @TODO: add here...

        return false;
    }

    std::vector<std::shared_ptr<LogicalOperator>>
    LogicalOptimizer::optimize(std::vector<std::shared_ptr<LogicalOperator>> &operators, bool inplace) {
        if(!inplace)
            throw std::runtime_error("not yet supported!");

        // make sure it's a stage, i.e. no join etc.!
        for(auto op : operators)
            if(op->parents().size() > 1) {
                logger().error("given operators do not form a stage, found operator " + op->name() +
                " with " + pluralize(op->parents().size(), "parent"));
                return {};
            }

        // optimize from the back
        auto last_op = operators.back();

        // filter breakup and reorder!
        // @TODO.

        if(_options.OPT_SELECTION_PUSHDOWN()) {
            // pushdown
            // note: set dropOperators to true to get rid off not computed columns!!!
            std::vector<size_t> cols;
            // start with requiring all columns from action node!
            // there's a subtle difference now b.c. output schema for csv was changed to str
            // --> use therefore input schema of the operator!
            auto num_cols = last_op->getInputSchema().getRowType().parameters().size();
            for(unsigned i = 0; i < num_cols; ++i)
                cols.emplace_back(i);
            projectionPushdown(last_op, nullptr, cols);
        }

        // reconstruct array
        std::vector<std::shared_ptr<LogicalOperator>> opt_ops;
        auto cur_op = last_op;
        while(cur_op) {
            opt_ops.push_back(cur_op);
            cur_op = cur_op->parent();
        }
        std::reverse(opt_ops.begin(), opt_ops.end());
        return opt_ops;
    }

    std::shared_ptr<LogicalOperator>
    LogicalOptimizer::optimize(const std::shared_ptr<LogicalOperator> &root, bool inplace) {
        using namespace std;

        // fiorst check validity
        if(!root) {
            logger().error("NULL root given, skip optimization.");
            return nullptr;
        }

        if(!root->isActionable()) {
            logger().error("given root is not actionable, skip optimization.");
            return root;
        }

        auto node = inplace ? root : shared_ptr<LogicalOperator>(root->clone());
        auto properties = PlanProperties::detect(node);

        if(_options.OPT_FILTER_PUSHDOWN()) {
            emitPartialFilters(node);
            optimizeFilters(node);
        }

        // prune tree? (note for lambda x: true / lambda x: false the optimization will only work if constant-fold is active)
        if(_options.OPT_CONSTANTFOLDING_OPTIMIZATION()) {
            pruneConstantFilters(node);
        }

        // @TODO: filter reordering! -> could be also done in a specializing way!

        // this opt makes only sense when joins (or flatmap later) are involved...
        if(properties.hasJoin() && _options.OPT_OPERATOR_REORDERING())
            reorderDataProcessingOperators(node);

        // projectionPushdown (to csv parser etc. if possible)
        // ==> i.e. only parse accessed fields!
        if(_options.OPT_SELECTION_PUSHDOWN()) {

            // note: set dropOperators to true to get rid off not computed columns!!!
            vector<size_t> cols;
            // start with requiring all columns from action node!
            // there's a subtle difference now b.c. output schema for csv was changed to str
            // --> use therefore input schema of the operator!
            auto num_cols = node->getInputSchema().getRowType().parameters().size();
            for(unsigned i = 0; i < num_cols; ++i)
                cols.emplace_back(i);
            projectionPushdown(node, nullptr, cols);

            // note: could remove identity functions...
            // i.e. lambda x: x or lambda x: (x[0], x[1], ..., x[len(x) - 1]) same for def...
        }

        return node;
 add    }

    void LogicalOptimizer::filterBreakup(const std::shared_ptr<LogicalOperator>& op) {
        if(!op) return;
        if(op->type() == LogicalOperatorType::FILTER) {
            auto fop = std::dynamic_pointer_cast<FilterOperator>(op);
            auto root = fop->getUDF().getAnnotatedAST().getFunctionAST();
            // @TODO: what about floats? etc?

            if(!fop->getInputSchema().getRowType().isTupleType()) return; // filter on scalar can't be broken
            auto params = fop->getInputSchema().getRowType().parameters();
            if(params.size() == 1 && !params[0].isTupleType()) return; // filter on scalar can't be broken
            auto cols = fop->columns();
            if(cols.empty()) return; // for now, can't deal with unlabeled columns (need to track through joins)

            // do the breakdown
            FilterBreakdownVisitor fbv;
            root->accept(fbv);

            auto tmp = fop->getUDF().getAccessedColumns();
            std::set<size_t> accessed_columns(tmp.begin(), tmp.end());
            if(fbv.succeeded() && (accessed_columns.size() > 1)) { // don't break the filter if only one column is accessed
                auto ranges = fbv.getRanges();
#ifdef TRACE_LOGICAL_OPTIMIZATION
                for(const auto &r : ranges) {
                    if(!r.second.intervals.empty()) {
                        std::string col_var = "x['" + cols[r.first] + "']";
                        std::cout << r.second.createLambdaString(col_var) << std::endl;
                    }
                }
#endif
                for (const auto &r : ranges) {
                    const auto &intervalCollection = r.second;
                    if (!intervalCollection.intervals.empty()) {
                        std::string condition = "lambda x: ";
                        std::string access = "x['" + cols[r.first] + "']";
                        condition += intervalCollection.createLambdaString(access);
#ifdef TRACE_LOGICAL_OPTIMIZATION
                        std::cout << condition << std::endl;
#endif

                        // add the condition
                        // children -> fop -> parent
                        // children -> fop -> new_filter -> parent
                        assert(fop->parents().size() == 1);
                        auto parent = fop->parent();
                        auto new_filter = std::shared_ptr<LogicalOperator>(new FilterOperator(parent, UDF(condition), parent->columns()));
                        parent->setChild(new_filter);
                        new_filter->setParent(parent);
                        fop->setParent(new_filter);
                        new_filter->setChild(fop);
                    }
                }
            }
        }
    }

    void LogicalOptimizer::emitPartialFilters(std::shared_ptr<LogicalOperator> &root) {

        // optimize: break up filters

        // first step: find all filter operators. Easy with ApplyVisitor!
        std::vector<std::shared_ptr<LogicalOperator>> v_filters;
        // @TODO: maybe define a logical tree visitor class because they're so convenient
        std::queue<std::shared_ptr<LogicalOperator>> q; // BFS
        q.push(root);
        while(!q.empty()) {
            auto node = q.front(); q.pop();
            if(node->type() == LogicalOperatorType::FILTER)
                v_filters.push_back(node);
            // add all parents to queue
            for(const auto &p : node->parents())
                q.push(p);
        }
        if(!v_filters.empty()) {
#ifdef TRACE_LOGICAL_OPTIMIZATION
            std::cout<<"found "<<v_filters.size()<<" filters to break"<<std::endl;
#endif
            // ==> could even lower some filters to parsers at some point!
            for(auto node : v_filters)
                filterBreakup(node);
        }

#ifndef NDEBUG
#ifdef GENERATE_PDFS
        // toPDF("logical_plan_after_filter_breakup.pdf");
#else
        Logger::instance().defaultLogger().debug("saving logical plan after filter breakup to PDF skipped.");
#endif
#endif
    }

    inline bool type_python_is_true(const python::Type& t) {
        if(python::Type::EMPTYTUPLE == t
           || python::Type::EMPTYLIST == t
           || python::Type::EMPTYDICT == t
           || python::Type::NULLVALUE == t)
            return false;

        // now only constants should work...
        assert(t.isConstantValued());

        auto ut = t.underlying();
        if(python::Type::BOOLEAN == ut) {
            if(t.constant() == "true") {
                return true;
            } else if(t.constant() == "false") {
                return false;
            } else {
                throw std::runtime_error("unknown bool const "+ t.constant() + " saved in constant type. Internal error?");
            }
        } else if(python::Type::I64 == ut) {
            if(stoi(t.constant()) == 0)
                return false;
            else
                return true;
        } else if(python::Type::STRING == ut) {
            auto const_value = str_value_from_python_raw_value(t.constant());
            return !const_value.empty();
        } else {
            throw std::runtime_error("unsupported type in type_python_is_true: " + t.desc());
        }
    }

    void LogicalOptimizer::pruneConstantFilters(const std::shared_ptr<LogicalOperator>& node) {
        // this optimization should be carried out after pushing down filters.

        // filters may evaluate to be a constant, i.e. either false/true
        // due to constant-folding

        // if this happens, there are two scenarios:
        // 1. filter evaluates to true -> keep elements, i.e. remove filter (it's always true)
        // 2. filter evaluates to false -> remove all subsequent elements, remove filter. -> pipeline only has to perform check

        // empty node? return
        if(!node)
            return;

        if(node->type() == LogicalOperatorType::FILTER) {
            std::cout<<"found filter"<<std::endl;
            auto fop = std::dynamic_pointer_cast<FilterOperator>(node);

            auto ret_type = fop->getUDF().getAnnotatedAST().getReturnType();
            if(ret_type.isExceptionType()) {
                return; // stop, no need to proceed further.
            }
            if(python::Type::UNKNOWN == ret_type) {
                throw std::runtime_error("filter operator has unknown return type, can't optimize nor prune beyond this point.");
            }

            // is it constant-valued?
            if(ret_type.isConstantValued() || python::Type::EMPTYTUPLE == ret_type
            || python::Type::EMPTYLIST == ret_type || python::Type::EMPTYDICT == ret_type
            || python::Type::NULLVALUE == ret_type) {
                // great! we found a special case!
                // what's the verdict? is it true or false?
                // (could use python here or directly decide)

                bool always_true = type_python_is_true(ret_type);

                if(always_true) {
                    // true? remove filter! -> keep rest
                    auto parent = fop->parent();
                    for(auto& child : fop->children()) {
                        child->setParent(parent);
                    }
                    // that's it, filter is removed.
                    // can continue optimizing pipeline.

                } else {
                    // false? keep filter, remove the rest (except the output/first pipeline breaker).
                    // i.e. remove everything after parents (except for
                    // go through tree and stop only at output nodes
                    std::queue<std::shared_ptr<LogicalOperator>> q;
                    for(auto c : fop->children())
                        q.push(c);
                    std::vector<std::shared_ptr<LogicalOperator>> end_nodes;
                    while(!q.empty()) {
                        auto cur_node = q.front();
                        q.pop();
                        if(!cur_node->children().empty()) {
                            for(auto c : fop->children())
                                q.push(c);
                        }
                        if(cur_node->type() == LogicalOperatorType::TAKE || cur_node->type() == LogicalOperatorType::FILEOUTPUT)
                            end_nodes.push_back(cur_node);
                    }
                    for(auto& end_node : end_nodes)
                        end_node->setParent(fop);

                    // no need to optimize further, pipeline has been removed already.
                    return;
                }
            } else {
                // regular, continue walking the tree...
            }
        }

        // regular walk
        for(auto child : node->parents()) {
            pruneConstantFilters(child);
        }
    }

    void LogicalOptimizer::optimizeFilters(std::shared_ptr<LogicalOperator>& root) {
        // optimize:
        // ==> i.e. reorder filter predicates to bottom if possible!
        // @TODO: push all filters down
        // Algorithm: while(filter depends not on previous operators result) move filter down.
        // Next step: Fuse filters together, i.e. combine conditions!

        // first step: find all filter operators. Easy with ApplyVisitor!
        std::vector<std::shared_ptr<LogicalOperator>> v_filters;
        // @TODO: maybe define a logical tree visitor class because they're so convenient
        std::queue<std::shared_ptr<LogicalOperator>> q; // BFS
        q.push(root);
        while(!q.empty()) {
            auto node = q.front(); q.pop();
            if(node->type() == LogicalOperatorType::FILTER)
                v_filters.push_back(node);
            // add all parents to queue
            for(const auto &p : node->parents())
                q.push(p);
        }
        if(!v_filters.empty()) {
#ifdef TRACE_LOGICAL_OPTIMIZATION
            std::cout<<"found "<<v_filters.size()<<" filters to push down"<<std::endl;
#endif
            // ==> could even lower some filters to parsers at some point!
            for(const auto &node : v_filters)
                filterPushdown(node);
        }

#ifndef NDEBUG
#ifdef GENERATE_PDFS
        //toPDF("logical_plan_after_filter_pushdown.pdf");
#else
        Logger::instance().defaultLogger().debug("saving logical plan after filter pushdown to PDF skipped.");
#endif
#endif
    }

    // return true if the push succeeded (e.g. they should try again)
    static bool pushParentThroughJoin(std::shared_ptr<JoinOperator> jop, bool left) {
        if(!jop) return false; // jop needs to exist
        if((left && !jop->left()) || (!left && !jop->right())) return false; // jop parent needs to exist

        // TODO: account for prefix/suffix in join operator -> need to rewrite UDF column strings
        if(!jop->leftSuffix().empty() || !jop->leftPrefix().empty() || !jop->rightSuffix().empty() || !jop->rightPrefix().empty()) return false;

        auto parent = left ? jop->left() : jop->right();
        // jop parent can only have one parent (to take its place)
        if(parent->numParents() != 1) return false;

        // jop parent can only have one child - we can get around this, but requires extra logic below when moving
        if(parent->numChildren() != 1) return false;

        switch(parent->type()) {
            case LogicalOperatorType::MAPCOLUMN: {
                // is index of mapCol is the join key, no; else yes
                auto mop = std::dynamic_pointer_cast<MapColumnOperator>(parent);
                auto mapIndex = mop->getColumnIndex();
                auto mapColumn = mop->columns()[mapIndex];
                auto mapColumnAfterJoin = left ? jop->leftPrefix() + mapColumn + jop->leftSuffix() : jop->rightPrefix() + mapColumn + jop->rightSuffix();
                auto joinOutputColumns = jop->columns();
                bool mapColumnIsKeyColumn = mapIndex == (left ? jop->leftKeyIndex() : jop->rightKeyIndex());

                // if the column being mapped is not the join key and the column is unique after the join
                if(!mapColumnIsKeyColumn && std::count(joinOutputColumns.begin(), joinOutputColumns.end(), mapColumnAfterJoin) == 1) {
                    // push the mapColumn up after the join

                    // child -> parent
                    // i.e.
                    // children -> join +--> mapCol -> leftP
                    //                   \-> right
                    // becomes
                    // children -> mapCol -> join +--> leftP
                    //                             \-> right

                    // make a new copy of the mapColumn UDF
                    auto code = mop->getUDF().getCode();
                    auto pickled_code = mop->getUDF().getPickledCode();
                    auto udf = UDF(code, pickled_code);

                    // rewrite UDF with the output of the join
                    if(!udf.rewriteDictAccessInAST(joinOutputColumns))
                        throw std::runtime_error("failed to rewrite UDF of mapColumn parent of join in join pushdown");

                    // create a new mapcolumn operator
                    auto new_mop = std::shared_ptr<LogicalOperator>(new MapColumnOperator(jop, mapColumnAfterJoin, joinOutputColumns, udf));
                    new_mop->setID(mop->getID());

                    // set up parent/child relationships
                    auto children = jop->children(); // children of join
                    children.erase(std::remove(children.begin(), children.end(), new_mop), children.end()); // need to remove new_mop because it was constructed with jop as parent
                    for(auto& c : children) {
                        c->setParent(new_mop);
                    }
                    new_mop->setChildren(children);
                    jop->setChild(new_mop);
                    new_mop->setParent(jop);

                    auto newParent = mop->parent();
                    jop->replaceParent(mop, newParent);
                    newParent->replaceChild(mop, jop);

                    // remove old map column operator
                    mop->setChildren({}); mop->setParents({}); // no dependencies
                    mop = nullptr;
                    return true;
                }
                return false;
            }
            case LogicalOperatorType::WITHCOLUMN: {
                // TODO: this seems much harder because join can increase #cols, so a UDF that has unpacked parameters has the wrong schema

                // // this is similar to MapColumn but a bit more complicated, i.e. need to check which columns withcolumn accesses!
                // auto wop = dynamic_cast<WithColumnOperator *>(parent);
                // assert(wop);
                // auto parentColsAccessed = wop->getUDF().getAccessedColumns();
                // auto idx = wop->getColumnIndex();
                // // check whether sets are disjoint and also index not used
                // parentColsAccessed.push_back(idx); // just add to set for check

                return false;
            }
            default:
                return false;
        }
    }

    void LogicalOptimizer::operatorPushup(const std::shared_ptr<LogicalOperator> &op) {
        if(!op)
            return;

        if(op->type() == LogicalOperatorType::JOIN) {
            // TODO: perform cardinality estimation and only proceed if it reduces
            {
#ifdef TRACE_LOGICAL_OPTIMIZATION
                std::cout<<"join found!"<<std::endl;
#endif
                auto jop = std::dynamic_pointer_cast<JoinOperator>(op);
                while(pushParentThroughJoin(jop, true));
                while(pushParentThroughJoin(jop, false));
            }
        } else {
            // only operate when passed a join
            return;
        }
    }

    void LogicalOptimizer::reorderDataProcessingOperators(std::shared_ptr<LogicalOperator>& root) {
        // optimize:
        // ==> i.e. reorder joins that reduce cardinality to bottom if possible!
        // Algorithm:
        // if join reduces cardinality:
        //     while(join doesn't depend on left parent's result) move left parent down.
        //     while(join doesn't depend on right parent's result) move right parent down.
        // @TODO: we can actually make a stronger statement I think: even if a given left parent doesn't move down, if it is in turn independent from its parent, then that parent can move down (e.g. skip over two operators)

        // first step: find all join operators
        std::vector<std::shared_ptr<LogicalOperator>> v_joins;
        std::queue<std::shared_ptr<LogicalOperator>> q; // BFS
        q.push(root);
        while(!q.empty()) {
            auto node = q.front(); q.pop();
            if(node->type() == LogicalOperatorType::JOIN)
                v_joins.push_back(node);
            // add all parents to queue
            for(const auto &p : node->parents())
                q.push(p);
        }
        if(!v_joins.empty()) {
#ifdef TRACE_LOGICAL_OPTIMIZATION
            std::cout<<"found "<<v_joins.size()<<" joins to push down"<<std::endl;
#endif
            // reverse so that we start from the bottom and move up, rather than other way around
            //  -> this way, a single operator can push up past multiple joins
            std::reverse(v_joins.begin(), v_joins.end());
            for(const auto &node : v_joins)
                operatorPushup(node);
        }

#ifndef NDEBUG
#ifdef GENERATE_PDFS
        // toPDF("logical_plan_after_join_pushdown.pdf");
#else
        Logger::instance().defaultLogger().debug("saving logical plan after join pushdown to PDF skipped.");
#endif
#endif
    }


    std::vector<size_t> LogicalOptimizer::projectionPushdown(const std::shared_ptr<LogicalOperator>& op,
                                           const std::shared_ptr<LogicalOperator>& child,
                                           std::vector<size_t> requiredCols,
                                           bool dropOperators) {
        using namespace std;

        if(!op)
            return requiredCols;

        // type to restrict columns of?
        assert(op);

        // get schemas
        auto inputRowType = op->parents().size() != 1 ? python::Type::UNKNOWN : op->getInputSchema().getRowType(); // could be also a tuple of one element!!!
        auto outputRowType = op->getOutputSchema().getRowType();

        vector<size_t> accCols; // indices of accessed columns from input row type!

        // udf operator? ==> selection possible!
        if(hasUDF(op.get())) {
            auto udfop = std::dynamic_pointer_cast<UDFOperator>(op);
            assert(udfop);

            switch(op->type()) {
                case LogicalOperatorType::MAP:
                case LogicalOperatorType::WITHCOLUMN:
                case LogicalOperatorType::FILTER: {
                    // UDF access of input...
                    accCols = udfop->getUDF().getAccessedColumns();
                    break;
                }
                case LogicalOperatorType::MAPCOLUMN: {
                    // special case: mapColumn! ==> because it takes single column as input arg!
                    accCols = vector<size_t>{static_cast<unsigned long>(std::dynamic_pointer_cast<MapColumnOperator>(op)->getColumnIndex())};
                    break;
                }
                case LogicalOperatorType::RESOLVE: {
                    // special case: resolve!
                    // if normalParent is mapColumn, not necessary to ask for cols (because index already in accCols!)
                    auto rop = std::dynamic_pointer_cast<ResolveOperator>(op); assert(rop);
                    auto np = rop->getNormalParent(); assert(np);

                    if(np->type() != LogicalOperatorType::MAPCOLUMN) {
                        accCols = udfop->getUDF().getAccessedColumns();
                    }

                    break;
                }
                case LogicalOperatorType::IGNORE: {
                    // skip, nothing to do...
                    break;
                }
                default:
                    throw std::runtime_error("unsupported UDFOperator in projection pushdown " + op->name());
            }

            // only a map operator selects a number of columns, i.e. the lowest map operator determines the pushdown!
            // ==> because of nested maps, subselect from current requiredCols if they are not empty!
            if(op->type() == LogicalOperatorType::MAP) {
                // some UDFs may only subselect columns but perform no operations on them...
                // i.e. could restrict further, use the following code to find
                if(!accCols.empty() && !udfop->getUDF().empty())  {
                    // don't update for rename, i.e. empty UDF!

                    // special case are resolve operators following, because they will change what columns are required. I.e.
                    // compute union with them.

                    set<size_t> cols(accCols.begin(), accCols.end());


                    // go over all resolvers following map and combine required columns with this map operator
                    if(op->children().size() == 1) {
                        auto cur_op = op->children().front();
                        while(cur_op->type() == LogicalOperatorType::RESOLVE) {
                            auto rop = std::dynamic_pointer_cast<ResolveOperator>(cur_op); assert(rop);
                            accCols = rop->getUDF().getAccessedColumns();
                            for(auto c : accCols)
                                cols.insert(c);

                            if(cur_op->children().size() != 1)
                                break;

                            cur_op = cur_op->children().front();
                        }
                    }

                    requiredCols = vector<size_t>(cols.begin(), cols.end());
                }
            }

            // filter operator also enforces a requirement, because records could be dropped!
            // ==> i.e. add required columns of filters coming BEFORE map operations!
            if(op->type() == LogicalOperatorType::FILTER) {
                set<size_t> cols(requiredCols.begin(), requiredCols.end());
                for(auto idx : accCols)
                    cols.insert(idx);
                requiredCols = vector<size_t>(cols.begin(), cols.end());
            }

            // if dropping is not allowed, then mapColumn/withColumn will be executed
            if (!dropOperators &&
                (op->type() == LogicalOperatorType::MAPCOLUMN || op->type() == LogicalOperatorType::WITHCOLUMN ||
                 op->type() == LogicalOperatorType::FILTER || op->type() == LogicalOperatorType::RESOLVE)) {
                set<size_t> cols(requiredCols.begin(), requiredCols.end());
                for (auto idx : accCols)
                    cols.insert(idx);
                requiredCols = vector<size_t>(cols.begin(), cols.end());
            }
        }

        if(op->type() == LogicalOperatorType::AGGREGATE) {
            auto aop = std::dynamic_pointer_cast<AggregateOperator>(op); assert(aop);

#warning "@TODO: implement proper analysis of the aggregate function to deduct which columns are required!"

            if(aop->aggType() == AggregateType::AGG_GENERAL || aop->aggType() == AggregateType::AGG_BYKEY) {
                // Note: this here is a quick hack:
                // simply require all columns.
                // However, we need a better solution for the aggregate function...
                // this will also involve rewriting...
                auto rowtype = aop->getInputSchema().getRowType();

                assert(rowtype.isTupleType());
                set<size_t> cols(requiredCols.begin(), requiredCols.end());
                for (int i = 0; i < rowtype.parameters().size(); ++i) {
                    cols.insert(i);
                }
                requiredCols = vector<size_t>(cols.begin(), cols.end());
            } else if(aop->aggType() == AggregateType::AGG_UNIQUE) {
                // unique makes all the columns required: add them all in
                auto rowtype = aop->getInputSchema().getRowType();

                assert(rowtype.isTupleType());
                set<size_t> cols(requiredCols.begin(), requiredCols.end());
                for (int i = 0; i < rowtype.parameters().size(); ++i) {
                    cols.insert(i);
                }
                requiredCols = vector<size_t>(cols.begin(), cols.end());
            } else {
                throw std::runtime_error("unknown aggregate type found in logical plan optimization!");
            }
        }

#ifdef TRACE_LOGICAL_OPTIMIZATION
        cout<<"projection pushdown on "<<op->name()<<endl;
#endif

        // traverse
        if(op->type() == LogicalOperatorType::JOIN) {

            auto jop = std::dynamic_pointer_cast<JoinOperator>(op); assert(jop);
            vector<size_t> leftRet;
            vector<size_t> rightRet;

            // fetch num cols of operators BEFORE correction
            auto numLeftColumnsBeforePushdown = jop->left()->columns().size();

            if(requiredCols.empty()) {
                leftRet = projectionPushdown(jop->left(), jop, requiredCols);
                rightRet = projectionPushdown(jop->right(), jop, requiredCols);
            } else {
                // need to split traversal up
                set<size_t> reqLeft;
                set<size_t> reqRight;


#ifdef TRACE_LOGICAL_OPTIMIZATION
                cout<<"join requires columns: "<<endl;
                for(auto idx : requiredCols) {
                    auto name = jop->columns()[idx];

                    // pos in left or right
                    auto leftColumns = jop->left()->columns();
                    auto rightColumns = jop->right()->columns();
                    auto lt = indexInVector(name, leftColumns);
                    auto rt = indexInVector(name, rightColumns);

                    cout<<name;
                    if(lt >= 0)
                        cout<<"    left pos: "<<lt;
                    if(rt >= 0)
                        cout<<"    right pos: "<<rt;
                    cout<<endl;
                }
#endif

                auto numLeftCols = jop->left()->getOutputSchema().getRowType().parameters().size();
                auto numRightCols = jop->right()->getOutputSchema().getRowType().parameters().size();
                for(auto idx : requiredCols) {
                    // required is key column + all that fall on left side for left
                    if(idx < numLeftCols)
                        reqLeft.insert(idx + (idx >= jop->leftKeyIndex())); // correct for join column drop
                }
                reqLeft.insert(jop->leftKeyIndex());

                for(auto idx : requiredCols) {
                    // need to correct for left number of cols (join is over one key)
                    if(idx >= numLeftCols) {
                        assert(idx < numRightCols + numLeftCols);
                        reqRight.insert(idx - numLeftCols + (idx - numLeftCols >= jop->rightKeyIndex())); // correct for join column drop
                    }
                }
                reqRight.insert(jop->rightKeyIndex());

                auto requiredLeftCols = vector<size_t>(reqLeft.begin(), reqLeft.end());
                auto requiredRightCols = vector<size_t>(reqRight.begin(), reqRight.end());

                leftRet = projectionPushdown(jop->left(), jop, requiredLeftCols);
                rightRet = projectionPushdown(jop->right(), jop, requiredRightCols);
            }


            // rewrite of join now necessary...
            vector<size_t> ret = leftRet; // @TODO: correct indices??

            for(auto idx : rightRet) {
                ret.push_back(idx + numLeftColumnsBeforePushdown); // maybe correct for key column?
            }

            //cout<<"need to rewrite join here with combined "<<ret<<endl;
            // update join (because columns have changed)
            assert(jop);

            auto oldLeftKeyIndex = jop->leftKeyIndex();
            auto oldRightKeyIndex = jop->rightKeyIndex();

            jop->projectionPushdown();
            // construct map


            // Note: the weird - (i >= ...) is because of the key column being rearranged
            // i.e. remember the result of a join is
            // |left non key cols | key col | right non key cols |
            vector<size_t> colsToKeep;
            for(int i = 0; i < leftRet.size(); ++i)
                if(i != jop->leftKeyIndex())
                    colsToKeep.push_back(leftRet[i] - (i >= jop->leftKeyIndex()));

            // keep the key column
            colsToKeep.push_back(numLeftColumnsBeforePushdown - 1);

            // fill in columns from right side to keep
            for(int i = 0; i < rightRet.size(); ++i) {
                if(i != jop->rightKeyIndex())
                    colsToKeep.push_back(numLeftColumnsBeforePushdown + rightRet[i] - (i >= jop->rightKeyIndex()));
            }

#ifdef TRACE_LOGICAL_OPTIMIZATION
            // dataset columns
            cout<<"Dataset columns: "<<jop->getDataSet()->columns()<<endl;
            cout<<"join column indices to keep: "<<colsToKeep<<endl;
            cout<<"names: ";
            for(auto idx : colsToKeep)
                cout<<jop->getDataSet()->columns()[idx]<<" ";
            cout<<endl;
            cout<<"left key column: "<<jop->left()->columns()[jop->leftKeyIndex()]<<endl;
            cout<<"right key column: "<<jop->right()->columns()[jop->rightKeyIndex()]<<endl;
#endif
            return colsToKeep;

        } else {
            // make sure only one parent
            assert(op->parents().size() <= 1);
            // special case CacheOperator, exec with parent nullptr if child is not null
            auto ret = op->type() == LogicalOperatorType::CACHE && child ?
                       projectionPushdown(nullptr, op, requiredCols) :
                       projectionPushdown(op->parent(), op, requiredCols);

#ifdef TRACE_LOGICAL_OPTIMIZATION
            cout<<"traverse done on "<<op->name();
            if(ret.empty()) {
                cout << " no rewrite here necessary." << endl;
            }
            else
                cout<<", time to rewrite!"<<endl;
#endif

            // CSV operator? do rewrite here!
            // ==> because it's a source node, use requiredCols!
            if(op->type() == LogicalOperatorType::FILEINPUT) {
                // rewrite csv here
                auto input_op = std::dynamic_pointer_cast<FileInputOperator>(op);
                assert(input_op);
                auto input_op_row_type = input_op->getInputSchema().getRowType();
                vector<size_t> colsToSerialize;
                for (auto idx : requiredCols) {
                    if (idx < input_op_row_type.parameters().size())
                        colsToSerialize.emplace_back(idx);
                }
                sort(colsToSerialize.begin(), colsToSerialize.end());

#ifdef TRACE_LOGICAL_OPTIMIZATION
                // info on columns + their types
                cout<<"rewrite csv here with "<<ret<<endl;
                cout<<"CSV columns before pushdown: "<<endl;
                cout <<"names: " << csvop->columns() << endl;
                cout <<"type: " << csvop->getOutputSchema().getRowType().desc() << endl;

                cout<<"CSV output type before pushdown: "<<csvop->getOutputSchema().getRowType().desc()<<endl;
#endif
                // actual projection pushdown into the parser...
                // --> use here relative indices!
                input_op->selectColumns(colsToSerialize, false);

#ifdef TRACE_LOGICAL_OPTIMIZATION
                cout<<"CSV output type after pushdown: "<<csvop->getOutputSchema().getRowType().desc()<<endl;
                cout<<"CSV projection pushdown: selected "<<ret.size()<<" columns from "<<inputRowType.parameters().size()<<endl;

                // info on columns + their types
                cout<<"CSV columns after pushdown: "<<endl;
                cout <<"names: " << csvop->columns() << endl;
                cout <<"type: " << csvop->getOutputSchema().getRowType().desc() << endl;
#endif
                // ok todo further rewrite, so return req Cols for building!
                return requiredCols;
            }

            // list other input operators here...
            // -> e.g. Parallelize, ... => could theoretically perform pushdown there as well
            if(op->type() == LogicalOperatorType::PARALLELIZE || op->type() == LogicalOperatorType::CACHE) {
                // this is a source operator
                // => no pushdown implemented here yet. Therefore, require all columns
                python::Type rowtype;
                if(op->type() == LogicalOperatorType::PARALLELIZE) {
                    auto pop = std::dynamic_pointer_cast<ParallelizeOperator>(op); assert(pop);
                    rowtype = pop->getOutputSchema().getRowType();
                } else {
                    auto cop = std::dynamic_pointer_cast<CacheOperator>(op); assert(cop);
                    rowtype = cop->getOutputSchema().getRowType();
                }

                vector<size_t> colsToSerialize;
                assert(rowtype.isTupleType());
                for(auto i = 0; i < rowtype.parameters().size(); ++i)
                    colsToSerialize.emplace_back(i);

                return colsToSerialize;
            }



            // make sure all source ops have been handled by above code!
            assert(!op->isDataSource());

            // b.c. of some special unrolling etc. could happen that ret is smaller than accCols!
            // -> make sure all requiredCols are within ret!
            std::set<size_t> col_set(ret.begin(), ret.end());
            for(auto col : requiredCols) {
                col_set.insert(col);
            }
            ret = vector<size_t>(col_set.begin(), col_set.end());


            // construct rewrite Map
            unordered_map<size_t, size_t> rewriteMap;
            if(!ret.empty()) {
                auto max_idx = *max_element(ret.begin(), ret.end()); // limit by max idx available
                unsigned counter = 0;
                for (unsigned i = 0; i <= max_idx; ++i) {
                    if (std::find(ret.begin(), ret.end(), i) != ret.end()) {
                        rewriteMap[i] = counter++;
                    }
                }
            }

            // map stops rewrite, so rewrite map and then do not return anything!
            if(op->type() == LogicalOperatorType::MAP) {
                // NOTE: rename ops continue rewrite mission!
                auto mop = std::dynamic_pointer_cast<MapOperator>(op); assert(mop);

                if(!mop->getUDF().empty()) {

#ifdef TRACE_LOGICAL_OPTIMIZATION
                    // type should NOT change...
                    cout<<"MAP type before projection pushdown: "<<mop->getOutputSchema().getRowType().desc()<<endl;
#endif

                    mop->rewriteParametersInAST(rewriteMap);

                    // rewrite all ResolveOperators following (skip ignore)
                    rewriteAllFollowingResolvers(op, rewriteMap);

#ifdef TRACE_LOGICAL_OPTIMIZATION
                    cout<<"MAP type after projection pushdown: "<<mop->getOutputSchema().getRowType().desc()<<endl;
                    cout<<"rewrite map here with"<<ret<<", stop rewriting."<<endl;
#endif
                    // non-empty UDF?
                    // simply return all indices, i.e. all columns are now to be kept!
                    // @TODO: can avoid rewrite if it's identity map!
                    auto numElements = mop->getOutputSchema().getRowType().parameters().size();
                    vector<size_t> colsToKeep;
                    for(int i = 0; i < numElements; ++i)
                        colsToKeep.emplace_back(i);
                    return colsToKeep;
                } else {

                    // empty UDF, i.e. need to update carried type...
                    // ==> create dummy rewriteMap to keep all indices!
#ifdef TRACE_LOGICAL_OPTIMIZATION
                    cout<<"MAP needs rewrite because empty but parent type is: "<<mop->parent()->getOutputSchema().getRowType().desc()<<endl;
                    cout<<"And MAP type is: "<<mop->getInputSchema().getRowType().desc()<<endl;
#endif

                    mop->rewriteParametersInAST(rewriteMap);

#ifdef TRACE_LOGICAL_OPTIMIZATION
                    cout<<"After rewrite: "<<mop->getInputSchema().getRowType().desc()<<endl;
#endif
                    // else, return continue rewrite with requiredCols
                    return ret;
                }
            }

                // UDF and NOT map?
            else if(hasUDF(op.get()) && op->type() != LogicalOperatorType::RESOLVE) {
                auto udfop = std::dynamic_pointer_cast<UDFOperator>(op); assert(udfop);


                // special case withColumn: I.e. a new column is added, need to append to rewrite Map and reqCols!
                if(op->type() == LogicalOperatorType::WITHCOLUMN) {
                    auto wop = std::dynamic_pointer_cast<WithColumnOperator>(op);
                    assert(wop);

                    size_t colIdx = wop->getColumnIndex();
                    // in rewrite map?
                    if(rewriteMap.find(colIdx) == rewriteMap.end()) {
                        // now always append. Because it doesn't matter anymore!
                        auto new_idx = ret.size();
                        rewriteMap[colIdx] = new_idx;
                        // also append to ret, because further functions might rely on this added column!
                        ret.push_back(colIdx);
                    }
                }

#ifdef TRACE_LOGICAL_OPTIMIZATION
                cout<<"REWRITE "<<op->name()<<" input type: "<<op->getInputSchema().getRowType().desc()<<endl;
#endif
                udfop->rewriteParametersInAST(rewriteMap);

                // rewrite all resolvers which follow
                rewriteAllFollowingResolvers(op, rewriteMap);
#ifdef TRACE_LOGICAL_OPTIMIZATION
                cout<<"AFTER R "<<op->name()<<" input type: "<<op->getInputSchema().getRowType().desc()<<endl;
#endif
                return ret;
            }

#ifdef TRACE_LOGICAL_OPTIMIZATION
            // other operators should not have a need to be updated, else order is wrong... -.-
            cout<<"unknown op "<<op->name()<<", continue rewrite"<<endl;
#endif
            return ret;
        }

        // not initialized, important for rename...
        return vector<size_t>();
    }

    void LogicalOptimizer::pushdownFilterInJoin(std::shared_ptr<FilterOperator> fop, const std::shared_ptr<JoinOperator> &jop) {
        using namespace std;

        assert(fop && jop);
        assert(fop->parent() == jop);

        auto idx = jop->outputKeyIndex();
        // fetch accessed columns
        auto filterAccessedCols = fop->getUDF().getAccessedColumns();

        // where to put filter for join?
        // if idx == filterAccessedCols ==> put on both sides
        // else, push down on one side
        if(filterAccessedCols.size() == 1 && idx == filterAccessedCols.front()) {
            // pushdown to both sides (i.e. create a copy!)

            // child -> parent
            // i.e. filter -> join +--> left
            //                      \-> right
            // becomes
            // join +--> filter -> left
            //       \-> filter -> right

            auto children = fop->children(); // children of filter
            auto left = jop->left();
            auto right = jop->right();


            // need to alias/rename column for left/right
            // @TODO: introduce better aliasing system...

            // easiest way here is to reparse the UDFs and then do all sorts of transformations...
            // ==> could be expensive though...
            auto code = fop->getUDF().getCode();
            auto pickled_code = fop->getUDF().getPickledCode();
            auto udf_left = UDF(code, pickled_code);
            auto udf_right = udf_left; // another copy

            auto outputKeyColumnName = jop->columns()[jop->outputKeyIndex()];
            auto leftColumns = left->columns();
            auto rightColumns = right->columns();
            // replace name of key index in these arrays
            leftColumns[jop->leftKeyIndex()] = outputKeyColumnName;
            rightColumns[jop->rightKeyIndex()] = outputKeyColumnName;

            // rewrite both udfs using updated column names,
            // this is important because of the Join combining col
            if(!udf_left.rewriteDictAccessInAST(leftColumns))
                throw std::runtime_error("failed to rewrite UDF of left subtree in filter pushdown for join");
            if(!udf_right.rewriteDictAccessInAST(rightColumns))
                throw std::runtime_error("failed to rewrite UDF of right subtree in filter pushdown for join");

            auto new_fop_left = std::shared_ptr<LogicalOperator>(new FilterOperator(left, udf_left,
                                                                                    left->columns()));
            auto new_fop_right = std::shared_ptr<LogicalOperator>(new FilterOperator(right, udf_right,
                                                                                     right->columns()));

            new_fop_left->setID(fop->getID());
            new_fop_right->setID(fop->getID());

            // set up parent/child relationship
            assert(left->numChildren() == 2); // only new fop, jop
            left->setChild(new_fop_left);
            assert(right->numChildren() == 2); // only new fop, jop
            right->setChild(new_fop_right);
            jop->setParents({new_fop_left, new_fop_right});
            new_fop_left->setChild(jop);
            new_fop_right->setChild(jop);

            // link children, i.e. remove filter
            for(auto& c : children) {
                c->replaceParent(fop, jop);
            }
            assert(jop->numChildren() == 1); // only filter before
            jop->setChildren(children);

            filterPushdown(new_fop_left);
            filterPushdown(new_fop_right);
        } else {
            bool allIndicesLessEqualKeyIndex = std::all_of(filterAccessedCols.begin(), filterAccessedCols.end(), [&](const size_t i) { return i <= idx; });
            bool allIndicesGreaterEqualKeyIndex = std::all_of(filterAccessedCols.begin(), filterAccessedCols.end(), [&](const size_t i) { return i >= idx; });
            if(allIndicesGreaterEqualKeyIndex == allIndicesLessEqualKeyIndex)
                throw std::runtime_error("fatal error, filter can't be pushed down!");

            std::shared_ptr<LogicalOperator> child = nullptr;
            // left pushdown
            if(allIndicesLessEqualKeyIndex) {
                // children -> filter -> join +--> left
                //                             \-> right
                // should become
                // children -> join +--> filter -> left
                //                   \-> right
                child = jop->left();
            } else {
                // right pushdown
                // children -> filter -> join +--> left
                //                             \-> right
                // should become
                // children -> join +--> left
                //                   \-> filter -> right
                child = jop->right();
            }

            // children -> filter -> join +--> ...
            //                             \-> right
            // should become
            // children -> join +--> filter -> child
            //                   \-> ...
            // create copy of filter (shallow)

            auto udf = UDF(fop->getUDF().getCode(), fop->getUDF().getPickledCode());
            auto outputKeyColumnName = jop->columns()[jop->outputKeyIndex()];
            auto cols = child == jop->left() ? jop->left()->columns() : jop->right()->columns();
            auto keyIdx = child == jop->left() ? jop->leftKeyIndex() : jop->rightKeyIndex();
            // replace name of key index in these arrays
            cols[keyIdx] = outputKeyColumnName;
            // rewrite udf using updated column names,
            // this is important because of the Join combining col
            udf.rewriteDictAccessInAST(cols);

            auto new_fop = std::shared_ptr<LogicalOperator>(new FilterOperator(child, udf, child->columns()));
            new_fop->setID(fop->getID());

            auto children = fop->children(); // children of filter
            auto left = jop->left();
            auto right = jop->right();
            auto jop_parents =
                    child == jop->left() ? std::vector<std::shared_ptr<LogicalOperator>>{new_fop, right}
                                         : std::vector<std::shared_ptr<LogicalOperator>>{left, new_fop};

            // link everything together
            // link children -> join (and vice versa)
            for(auto& c : children) {
                c->replaceParent(fop, jop);
            }
            jop->setChildren(children);

            // join -> filter
            jop->setParents(jop_parents); new_fop->setChild(jop); // (right should have jop as child)

            // link of jop_parents to jop
            assert(jop_parents.size() == 2);
            // TODO: do these do anything?
            jop_parents[0]->replaceChild(fop, jop);
            jop_parents[1]->replaceChild(fop, jop);

            // filter -> child
            new_fop->setParent(child);
            assert(child->numChildren() == 2); // new fop, jop
            child->setChild(new_fop);

            // assert(verifyLogicalPlan(jop));

            filterPushdown(new_fop);
        }

        // remove old filter
        fop->setChildren({}); fop->setParents({}); // no dependencies
        fop = nullptr;
    }

    void LogicalOptimizer::filterPushdown(const std::shared_ptr<LogicalOperator> &op) {
        if(!op)
            return;

        if(op->type() == LogicalOperatorType::FILTER) {
#ifdef TRACE_LOGICAL_OPTIMIZATION
            std::cout<<"filter found!"<<std::endl;
#endif
            if(!filterDependsOnParentOperator(dynamic_cast<FilterOperator*>(op.get()))) {
                assert(op->parents().size() == 1); // filter has exactly one parent!
#ifdef TRACE_LOGICAL_OPTIMIZATION
                std::cout<<"push down filter in front of "<<op->parent()->name()<<std::endl;
#endif
                // how many parents does parent have?
                if(op->parent()->parents().size() == 1) {
                    // simply move operator in tree

                    // children -> filter -> parent -> grandparent
                    // should become
                    // children -> parent -> filter -> grandparent
                    // & call on filter again!
                    auto children = op->children();
                    auto parent = op->parent(); assert(parent);
                    auto grandparent = parent->parent(); assert(grandparent);

#ifdef TRACE_LOGICAL_OPTIMIZATION
                    std::cout<<"parent output schema: "<<parent->getOutputSchema().getRowType().desc()<<std::endl;
                    std::cout<<"grandparent output schema: "<<grandparent->getOutputSchema().getRowType().desc()<<std::endl;
#endif

                    // @TODO: this here is rather slow because the whole compilation pipeline gets kicked off
                    // could optimize by remapping indices... => s
                    // create copy of filter ==> need to reparse UDF & Co because of column access!
                    auto code = std::dynamic_pointer_cast<FilterOperator>(op)->getUDF().getCode();
                    auto pickled_code = std::dynamic_pointer_cast<FilterOperator>(op)->getUDF().getPickledCode();
                    auto fop = std::shared_ptr<LogicalOperator>(new FilterOperator(grandparent, UDF(code, pickled_code), grandparent->columns()));
                    fop->setID(op->getID()); // clone with ID, important for exception tracking!
#ifdef TRACE_LOGICAL_OPTIMIZATION
                    // debug:
                    std::cout<<"new filter input schema: "<<fop->getUDF().getInputSchema().getRowType().desc()<<std::endl;
                    std::cout<<"new filter output schema: "<<fop->getUDF().getOutputSchema().getRowType().desc()<<std::endl;

                    std::cout<<"filter input schema: "<<fop->getInputSchema().getRowType().desc()<<std::endl;
                    std::cout<<"filter output schema: "<<fop->getOutputSchema().getRowType().desc()<<std::endl;
#endif

                    // error here.. this code is wrong when join operator is involved!
                    // link children -> parent (and vice versa)
                    for(auto& child : children) {
                        //child->setParent(parent);
                        bool found = child->replaceParent(op, parent);
                        assert(found);
                        found = parent->replaceChild(op, child);
                        // assert(found);
                    }

                    // parent -> filter (and vice versa)
                    parent->setParent(fop); fop->setChild(parent);

                    // filter->grandparent (and vice versa)
                    fop->setParent(grandparent); grandparent->setChild(fop);

                    // call filter pushdown on filter again

                    // remove old filter
                    op->setChildren({}); op->setParents({}); // no dependencies

                    // // DEBUG
                    // for(auto child : children)
                    //     verifyLogicalPlan(child);
                    // // END DEBUG

                    // continue pushdown
                    filterPushdown(fop);
                } else {
                    // parent has more than one parent? ==> i.e. add filter to whichever grandparent where if it makes sense!
                    if(op->parent()->type() == LogicalOperatorType::JOIN) {
                        auto jop = std::dynamic_pointer_cast<JoinOperator>(op->parent()); assert(jop);
                        auto fop = std::dynamic_pointer_cast<FilterOperator>(op); assert(fop);
                        pushdownFilterInJoin(fop, jop);
                    } else throw std::runtime_error("only operator for multiple grandparent supported yet is join!");

                    // go on with pushdown...

                }

            } else {
#ifdef TRACE_LOGICAL_OPTIMIZATION
                std::cout<<"no pushdown possible"<<std::endl;
#endif
                // stop.
            }
        } else {

            // traverse tree until filter is found...
            for(const auto &p : op->parents())
                filterPushdown(p);
        }
    }

    void LogicalOptimizer::rewriteAllFollowingResolvers(std::shared_ptr<LogicalOperator> op, const std::unordered_map<size_t, size_t>& rewriteMap) {
        // go over children (single!)
        if(!op)
            return;

        while(op && op->children().size() == 1) {
            auto cur_op = op->children().front();

            // ignore? => continue
            if(cur_op->type() == LogicalOperatorType::RESOLVE) {
                // rewrite!
                auto rop = std::dynamic_pointer_cast<ResolveOperator>(cur_op); assert(rop);
                rop->rewriteParametersInAST(rewriteMap);
            } else if(cur_op->type() == LogicalOperatorType::IGNORE) {
                auto iop = std::dynamic_pointer_cast<IgnoreOperator>(cur_op); assert(iop);
                iop->updateSchema();
                // nothing todo...
            } else {
                // quit loop & function
                return;
            }
            op = cur_op;
        }
    }



    bool filterDependsOnParentOperator(FilterOperator* op) {

        auto& logger = Logger::instance().logger("logical optimizer");

        if(!op || op->parents().empty())
            return false;

        // make sure at least one parent exists!
        assert(!op->parents().empty());

        if(op->parents().size() != 1) {
            throw std::runtime_error("filter should have only one parent");
        }

        // how many parents? ==> should be one here!
        auto parent = op->parent();
        auto parent_operator_type = op->parent()->type();

        // // can also access paths to push further down.
        // auto accessPaths = op->getUDF().getAccessedPaths();

        // get accessed columns in filter (important for checking with withColumn/mapColumn/join...)
        auto accessedColumns = op->getUDF().getAccessedColumns();

        switch(parent_operator_type) {
            case LogicalOperatorType::AGGREGATE: {
                auto aop = dynamic_cast<AggregateOperator*>(parent.get()); assert(aop);
                if(aop->aggType() == AggregateType::AGG_UNIQUE) {
                    return false;
                } else {
                    throw std::runtime_error("unsupported aggregation type");
                }
            }
            case LogicalOperatorType::MAP: {
                // empty? i.e. rename? switch ok
                auto mop = dynamic_cast<MapOperator*>(parent.get()); assert(mop);

                // special case:
                // select is ok, because it's a direct map, the same goes for some query which just reorders columns...
                // => after rewrite!
                if(isMapSelect(mop))
                    return false;

                if(mop->getUDF().empty())
                    return false;
                else
                    return true; // because Map transforms columns oddly!
            }
            case LogicalOperatorType::MAPCOLUMN: {
                // is index of mapCol contained in accessedColumns ==> depends, else no
                auto idx = dynamic_cast<MapColumnOperator*>(parent.get())->getColumnIndex();
                auto it = std::find(accessedColumns.begin(), accessedColumns.end(), idx);
                return it != accessedColumns.end(); // true if contained, else no
            }
            case LogicalOperatorType::WITHCOLUMN: {
                // this is similar to MapColumn but a bit more complicated, i.e. need to check which columns withcolumn accesses!
                auto wop = dynamic_cast<WithColumnOperator*>(parent.get()); assert(wop);
                auto parentColsAccessed = wop->getUDF().getAccessedColumns();
                auto idx = wop->getColumnIndex();

                // Note: this requires adjustment of getting rid of unused params in UDFs when
                //       multi-param syntax is used...
                // @TODO: fix this.
                // // new code:
                // // withcolumn adds values based on all other columns. Thus, it's safe to pushdown a filter
                // // if the newly added column is not part of the columns the filter requires
                // auto it = std::find(accessedColumns.begin(), accessedColumns.end(), idx);
                // return it != accessedColumns.end();

                // old code:
                // check whether sets are disjoint and also index not used
                parentColsAccessed.push_back(idx); // just add to set for check

                std::vector<size_t> commonCols;
                std::set_intersection(accessedColumns.begin(), accessedColumns.end(), parentColsAccessed.begin(),
                                      parentColsAccessed.end(), std::back_inserter(commonCols));

                // if intersection is empty, then no dependence. Else, dependence
                return !commonCols.empty();
            }

            case LogicalOperatorType::JOIN: {
                // a filter does not depend on Join if it does not access columns of both sides
                auto jop = dynamic_cast<JoinOperator*>(parent.get()); assert(jop);
                auto idx = jop->outputKeyIndex(); // special case, if filter only accesses key col, no dependence

                // 3 checks:
                // either all accessed keys are <= idx or >= idx
                bool allIndicesLessEqualKeyIndex = std::all_of(accessedColumns.begin(), accessedColumns.end(), [&](const size_t i) {
                    return i <= idx;
                });
                bool allIndicesGreaterEqualKeyIndex = std::all_of(accessedColumns.begin(), accessedColumns.end(), [&](const size_t i) {
                    return i >= idx;
                });

                // special case both true?
                if(allIndicesGreaterEqualKeyIndex && !allIndicesLessEqualKeyIndex) {
                    // if it's an inner join, then pushdown can be done
                    // for left join, it can't because here the UDF accesses columns which might become null in
                    // the join
                    if(jop->joinType() == JoinType::INNER)
                        return false;
                    if(jop->joinType() == JoinType::LEFT)
                        return true; // Note: could do an optimization by typing the UDF in two cases... => for NUll and non-null...
                }
                if(!allIndicesGreaterEqualKeyIndex && allIndicesLessEqualKeyIndex) {
                    // todo: right join, for left join all good.
                    return false;
                }
                if(allIndicesGreaterEqualKeyIndex && allIndicesLessEqualKeyIndex) {
                    // only key index? => works for both left/right join
                    return !(accessedColumns.size() == 1 && accessedColumns.front() == idx);
                }
                return true;
            }

            case LogicalOperatorType::FILTER: {
#ifdef TRACE_LOGICAL_OPTIMIZATION
                std::cout<<"@TODO: could combine filters..."<<std::endl;
#endif
                return false; // filters never depend on each other... --> they can be freely reordered.
            }

            case LogicalOperatorType::FILEINPUT:
            case LogicalOperatorType::PARALLELIZE: {
                return true; // always depend on data sources!
            }

            default:
                logger.debug("unknown operator " + parent->name() + " in filterDependsOnParentOperator encountered, assuming full dependence.");
                return true;
        }
        return true;
    }

}
