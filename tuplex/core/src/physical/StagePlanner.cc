//
// Created by Leonhard Spiegelberg on 2/27/22.
//

#include <physical/StagePlanner.h>
#include <physical/AggregateFunctions.h>
#include <logical/CacheOperator.h>
#include <logical/JoinOperator.h>
#include <logical/FileInputOperator.h>
#include <JSONUtils.h>
#include <CSVUtils.h>
#include <Utils.h>
#include <logical/AggregateOperator.h>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iostream>
#include <iterator>
#include <string_view>
#include <vector>
#include <physical/StageBuilder.h>

namespace tuplex {
    namespace codegen {

        // check type.
        void checkRowType(const python::Type& rowType) {
            assert(rowType.isTupleType());
            for(auto t : rowType.parameters())
                assert(t != python::Type::UNKNOWN);
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::optimize() {
            using namespace std;

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            if(_useNVO) {
                logger.info("performing NVO");
                _operators = nulLValueOptimization();
            }


            // step 1: retrieve sample from inputnode!
            std::vector<Row> sample = fetchInputSample();

            if(_useConstantFolding && sample.size() >= 100) { // should have at least 100 samples to determine this...
                // check which columns could be constants and if so propagate that information!
                logger.info("Performing constant folding optimization");

                DetectionStats ds;
                ds.detect(sample);

                // print info
                cout<<"Following columns detected to be constant: "<<ds.constant_column_indices()<<endl;
                // print out which rows are considered constant (and with which values!)
                for(auto idx : ds.constant_column_indices()) {
                    string column_name;
                    if(_inputNode && !_inputNode->inputColumns().empty())
                        column_name = _inputNode->inputColumns()[idx];
                    cout<<" - "<<column_name<<": "<<ds.constant_row.get(idx).desc()<<" : "<<ds.constant_row.get(idx).getType().desc()<<endl;
                }

                // for now simplify life (HACK!) just one operator
                if(_operators.size() == 1) {
                    // apply the constant folding operation!

                    // works now only for map operator
                    auto op = _operators.front();
                    if(op->type() == LogicalOperatorType::MAP) {
                        auto mop = std::dynamic_pointer_cast<MapOperator>(op);
                        assert(mop);

                        // do opt only if input cols are valid...!

                        // retype UDF
                       cout<<"input type before: "<<mop->getInputSchema().getRowType().desc()<<endl;
                       cout<<"output type before: "<<mop->getOutputSchema().getRowType().desc()<<endl;
                       cout<<"num input columns required: "<<mop->inputColumns().size()<<endl;
                       // retype
                       auto input_cols = mop->inputColumns(); // HACK! won't work if no input cols are specified.
                       auto input_type = mop->getInputSchema().getRowType();
                       if(input_cols.empty()) {
                            logger.debug("skipping, only for input cols now working...");
                            return _operators;
                       }
                       // for all constants detected, add type there & use that for folding!
                       // if(input_type.parameters().size() == 1 && input_type.parameters().front().isTupleType())
                       auto tuple_mode = input_type.parameters().size() == 1 && input_type.parameters().front().isTupleType();
                       if(!tuple_mode) {
                           logger.debug("only tuple/dict mode supported! skipping for now");
                           return _operators;
                       }

                       auto param_types = input_type.parameters()[0].parameters();
                       if(param_types.size() != input_cols.size()) {
                           logger.warn("Something wrong, numbers do not match up.");
                           return _operators;
                       }

                       // now update these vars with whatever is possible
                       std::unordered_map<std::string, python::Type> constant_types;
                       // HACK! do not change column names, else this will fail...!
                        for(auto idx : ds.constant_column_indices()) {
                            string column_name;
                            if(_inputNode && !_inputNode->inputColumns().empty()) {
                                column_name = _inputNode->inputColumns()[idx];
                                constant_types[column_name] = python::Type::makeConstantValuedType(ds.constant_row.get(idx).getType(), ds.constant_row.get(idx).desc()); // HACK
                            }
                        }
                        // lookup column names (NOTE: this should be done using integers & properly propagated through op graph)
                        for(unsigned i = 0; i < input_cols.size(); ++i) {
                            auto name = input_cols[i];
                            auto it = constant_types.find(name);
                            if(it != constant_types.end())
                                param_types[i] = it->second;
                        }

                        // now update specialized type with constant if possible!
                       auto specialized_type = tuple_mode ? python::Type::makeTupleType({python::Type::makeTupleType(param_types)}) : python::Type::makeTupleType(param_types);
                       if(specialized_type != input_type) {
                           cout<<"specialized type "<<input_type.desc()<<endl;
                           cout<<"  - to - "<<endl;
                           cout<<specialized_type.desc()<<endl;
                       } else {
                           cout<<"no specialization possible, same type";
                           // @TODO: can skip THIS optimization, continue with the next one!
                       }

                       auto accColsBeforeOpt = mop->getUDF().getAccessedColumns();

                       mop->retype({specialized_type});

                       // now check again what columns are required from input, if different count -> push down!
                       // @TODO: this could get difficult for general graphs...
                       auto accCols = mop->getUDF().getAccessedColumns();
                        // Note: this works ONLY for now, because no other op after this...


                       // check again
                        cout<<"input type after: "<<mop->getInputSchema().getRowType().desc()<<endl;
                        cout<<"output type after: "<<mop->getOutputSchema().getRowType().desc()<<endl;
                        cout<<"num input columns required after opt: "<<accCols.size()<<endl;

                        // which columns where eliminated?
                        //     const std::vector<int> v1 {1, 2, 5, 5, 5, 9};
                        //    const std::vector<int> v2 {2, 5, 7};
                        //    std::vector<int> diff; // { 1 2 5 5 5 9 } ∖ { 2 5 7 } = { 1 5 5 9 }
                        //
                        //    std::set_difference(v1.begin(), v1.end(), v2.begin(), v2.end(),
                        //                        std::inserter(diff, diff.begin()));
                        std::sort(accColsBeforeOpt.begin(), accColsBeforeOpt.end());
                        std::sort(accCols.begin(), accCols.end());
                        std::vector<size_t> diff;
                        std::set_difference(accColsBeforeOpt.begin(), accColsBeforeOpt.end(),
                                            accCols.begin(), accCols.end(), std::inserter(diff, diff.begin()));
                        cout<<"There were "<<pluralize(diff.size(), "column")<<" optimized away:"<<endl;
                        vector<string> opt_away_names;
                        for(auto idx : diff)
                            opt_away_names.push_back(mop->inputColumns()[idx]);
                        cout<<"-> "<<opt_away_names<<endl;

                        // rewrite which columns to access in input node
                        if(_inputNode->type() != LogicalOperatorType::FILEINPUT) {
                            logger.debug("stopping here, should get support for ops...");
                            return _operators;
                        }
                        auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode);
                        auto colsToSerialize = fop->columnsToSerialize();
                        vector<size_t> colsToSerializeIndices;
                        for(unsigned i = 0; i < colsToSerialize.size(); ++i)
                            if(colsToSerialize[i])
                                colsToSerializeIndices.push_back(i);
                        cout<<"reading columns: "<<colsToSerializeIndices<<endl;

                        cout<<"Column indices to read before opt: "<<accColsBeforeOpt<<endl;
                        cout<<"After opt only need to read: "<<accCols<<endl;

                        // TODO: need to also rewrite access in mop again
                        // mop->rewriteParametersInAST(rewriteMap);

                        // gets a bit more difficult now:
                        // num input columns required after opt: 13
                        //There were 4 columns optimized away:
                        //-> [YEAR, MONTH, CRS_DEP_TIME, CRS_ELAPSED_TIME]
                        //reading columns: [0, 2, 3, 6, 10, 11, 20, 29, 31, 42, 50, 54, 56, 57, 58, 59, 60]
                        //Column indices to read before opt: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
                        //After opt only need to read: [2, 3, 4, 5, 6, 8, 9, 11, 12, 13, 14, 15, 16]

                        // so accCols should be sorted, now map to columns. Then check the position in accColsBefore
                        // => look up what the original cols were!
                        // => then push that down to reader/input node!
                        unordered_map<size_t, size_t> rewriteMap;
                        vector<size_t> indices_to_read_from_previous_op;
                        vector<string> rewriteInfo; // for printing info!
                        vector<string> col_names_to_read_before;
                        vector<string> col_names_to_read_after;

                        _normalToGeneralMapping.clear();

                        for(unsigned i = 0; i < accCols.size(); ++i) {
                            rewriteMap[accCols[i]] = i;

                            rewriteInfo.push_back(to_string(accCols[i]) + " -> " + to_string(i));

                            // save normal -> general mapping
                            _normalToGeneralMapping[i] = accCols[i];

                            int j = 0;
                            while(j < accColsBeforeOpt.size() && accCols[i] != accColsBeforeOpt[j])
                                ++j;
                            indices_to_read_from_previous_op.push_back(colsToSerializeIndices[j]);
                        }

                        for(auto idx : colsToSerializeIndices)
                            col_names_to_read_before.push_back(fop->inputColumns()[idx]);
                        for(auto idx : indices_to_read_from_previous_op)
                            col_names_to_read_after.push_back(fop->inputColumns()[idx]);
                        cout<<"Rewriting indices: "<<rewriteInfo<<endl;

                        // this is quite hacky...
                        // ==> CLONE???
                        fop->selectColumns(indices_to_read_from_previous_op);
                        cout<<"file input now only reading: "<<indices_to_read_from_previous_op<<endl;
                        cout<<"I.e., read before: "<<col_names_to_read_before<<endl;
                        cout<<"now: "<<col_names_to_read_after<<endl;
                        fop->useNormalCase(); // !!! retype input op. mop already retyped above...

                        mop->rewriteParametersInAST(rewriteMap);
                        // retype!
                        cout<<"mop updated: \ninput type: "<<mop->getInputSchema().getRowType().desc()
                            <<"\noutput type: "<<mop->getOutputSchema().getRowType().desc()<<endl;

#ifdef GENERATE_PDFS
                        mop->getUDF().getAnnotatedAST().writeGraphToPDF("final_mop_udf.pdf");
#endif
                        // @TODO: should clone operators etc. here INCL. input oprator else issue.
                        // input operator needs additional check... => put this check into parser...

                    }

                }


            }

            // check accesses -> i.e. need to check for all funcs till first map or end of stage is reached.
            // why? b.c. map destroys structure. The other require analysis though...!
            // i.e. trace using sample... (this could get expensive!)




            return _operators;
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::nulLValueOptimization() {
            using namespace std;

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // only null-value opt yet supported
            if(!_useNVO)
                return _operators;

            // special case: cache operator! might have exceptions or no exceptions => specialize depending on that!
            // i.e. an interesting case happens when join(... .cache(), ...) is used. Then, need to upcast result to general case
            // => for now, do not support...
            // => upcasting should be done in LocalBackend.


            // no input node or input node not FileInputOperator?
            // => can't specialize...
            if(!_inputNode)
                return _operators;
            if(_inputNode->type() != LogicalOperatorType::FILEINPUT && _inputNode->type() != LogicalOperatorType::CACHE)
                return _operators;

            // fetch optimized schema from input operator
            Schema opt_input_schema;
            if(_inputNode->type() == LogicalOperatorType::FILEINPUT)
                opt_input_schema = std::dynamic_pointer_cast<FileInputOperator>(_inputNode)->getOptimizedOutputSchema();
            else if(_inputNode->type() == LogicalOperatorType::CACHE)
                opt_input_schema = std::dynamic_pointer_cast<CacheOperator>(_inputNode)->getOptimizedOutputSchema();
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
            vector<std::shared_ptr<LogicalOperator>> opt_ops;
            std::shared_ptr<LogicalOperator> lastNode = nullptr;
            for(auto node : _operators) {
                auto lastParent = opt_ops.empty() ? _inputNode : opt_ops.back();
                if(!lastNode)
                    lastNode = lastParent; // set lastNode to parent to make join on fileop work!

                switch(node->type()) {
                    case LogicalOperatorType::PARALLELIZE: {
                        opt_ops.push_back(node);
                        break;
                    }
                    case LogicalOperatorType::FILEINPUT: {
                        // create here copy using normalcase!
                        auto op = std::dynamic_pointer_cast<FileInputOperator>(node->clone());
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
                            auto wop = std::dynamic_pointer_cast<WithColumnOperator>(node);
                            if(wop->columnToMap() == "ActualElapsedTime") {
                                std::cout<<"start checking retyping here!!!"<<std::endl;
                            }
                        }

                        checkRowType(last_rowtype);
                        // set FIRST the parent. Why? because operators like ignore depend on parent schema
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
                        auto jop = std::dynamic_pointer_cast<JoinOperator>(node);
                        assert(lastNode);

//#error "bug is here: basically if lastParent is cache, then it hasn't been cloned and thus getOutputSchema gives the general case"
//                        "output schema, however, if the cache operator is the inputnode, then optimized schema should be used..."

                        // this here is a bit more involved.
                        // I.e., is it left side or right side?
                        vector<std::shared_ptr<LogicalOperator>> parents;
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

                        opt_ops.push_back(std::shared_ptr<LogicalOperator>(new JoinOperator(parents[0], parents[1], jop->leftColumn(), jop->rightColumn(),
                                                           jop->joinType(), jop->leftPrefix(), jop->leftSuffix(), jop->rightPrefix(),
                                                           jop->rightSuffix())));
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
                        opt_ops.push_back(std::shared_ptr<LogicalOperator>(new TakeOperator(lastParent, std::dynamic_pointer_cast<TakeOperator>(node)->limit())));
                        opt_ops.back()->setID(node->getID());
                        break;
                    }
                    case LogicalOperatorType::FILEOUTPUT: {
                        auto fop = std::dynamic_pointer_cast<FileOutputOperator>(node);

                        opt_ops.push_back(std::shared_ptr<LogicalOperator>(new FileOutputOperator(lastParent, fop->uri(), fop->udf(), fop->name(),
                                                                 fop->fileFormat(), fop->options(), fop->numParts(), fop->splitSize(),
                                                                 fop->limit())));
                        break;
                    }
                    case LogicalOperatorType::CACHE: {
                        // two options here: Either cache is used as last node or as source!
                        // source?
                        auto cop = std::dynamic_pointer_cast<CacheOperator>(node);
                        if(!cop->children().empty()) {
                            // => cache is a source, i.e. fetch optimized schema from it!
                            last_rowtype = cop->getOptimizedOutputSchema().getRowType();
                            checkRowType(last_rowtype);
                            cout<<"cache is a source: optimized schema "<<last_rowtype.desc()<<endl;

                            // use normal case & clone WITHOUT parents
                            // clone, set normal case & push back
                            cop = std::dynamic_pointer_cast<CacheOperator>(cop->cloneWithoutParents());
                            cop->setOptimizedOutputType(last_rowtype);
                            cop->useNormalCase();
                        } else {
                            // cache should not have any children
                            assert(cop->children().empty());
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

        std::vector<Row> StagePlanner::fetchInputSample() {
            if(!_inputNode)
                return {};
            return _inputNode->getSample(1000);
        }
    }



    // HACK: magical experiment function!!!
    // HACK!
    void hyperspecialize(TransformStage *stage, const URI& uri, size_t file_size) {
        auto& logger = Logger::instance().logger("hyper specializer");
        // run hyperspecialization using planner, yay!
        assert(stage);

        // need to decode CodeGenerationCOntext from stage
        if(stage->_encodedData.empty()) {
            logger.info("did not find encoded codegen context, skipping.");
            return;
        }

        logger.info("specializing code to file " + uri.toString());

        // deserialize using Cereal

        // fetch codeGenerationContext & restore logical operator tree!
        codegen::CodeGenerationContext ctx;
        Timer timer;
        {
            auto compressed_str = stage->_encodedData;
            auto decompressed_str = decompress_string(compressed_str);
            logger.info("Decompressed Code context from " + sizeToMemString(compressed_str.size()) + " to " + sizeToMemString(decompressed_str.size()));
            std::istringstream iss(decompressed_str);
            cereal::BinaryInputArchive ar(iss);
            ar(ctx);
        }
        logger.info("Decode took " + std::to_string(timer.time()) + "s");
        // old hacky version
        // auto ctx = codegen::CodeGenerationContext::fromJSON(stage->_encodedData);

        assert(ctx.slowPathContext.valid());
        // decoded, now specialize
        auto path_ctx = ctx.slowPathContext;
        //specializePipeline(fastPath, uri, file_size);
        auto inputNode = path_ctx.inputNode;
        auto operators = path_ctx.operators;

        // force resampling b.c. of thin layer
        if(inputNode->type() == LogicalOperatorType::FILEINPUT) {
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode); assert(fop);
            fop->setInputFiles({uri}, {file_size}, true);
        }

        // node need to find some smart way to QUICKLY detect whether the optimizaiton can be applied or should be rather skipped...

        codegen::StagePlanner planner(inputNode, operators);
        planner.enableAll();
        path_ctx.operators = planner.optimize();
        path_ctx.outputSchema = path_ctx.operators.back()->getOutputSchema();
        path_ctx.inputSchema = path_ctx.inputNode->getOutputSchema();
        path_ctx.readSchema = std::dynamic_pointer_cast<FileInputOperator>(path_ctx.inputNode)->getOptimizedInputSchema(); // when null-value opt is used, then this is different! hence apply!
        path_ctx.columnsToRead = std::dynamic_pointer_cast<FileInputOperator>(path_ctx.inputNode)->columnsToSerialize();
        logger.info("specialized to input:  " + path_ctx.inputSchema.getRowType().desc());
        logger.info("specialized to output: " + path_ctx.outputSchema.getRowType().desc());
        size_t numToRead = 0;
        for(auto indicator : path_ctx.columnsToRead)
            numToRead += indicator;
        logger.info("specialized code reads: " + pluralize(numToRead, "column"));
        ctx.fastPathContext = path_ctx;


        auto generalCaseInputRowType = ctx.slowPathContext.inputSchema.getRowType();
        if(ctx.slowPathContext.inputNode->type() == LogicalOperatorType::FILEINPUT)
            generalCaseInputRowType = ctx.slowPathContext.readSchema.getRowType();

        auto generalCaseOutputRowType = ctx.slowPathContext.outputSchema.getRowType();

        // generate code! Add to stage, can then compile this. Yay!
        timer.reset();
        stage->_fastCodePath = codegen::StageBuilder::generateFastCodePath(ctx, ctx.fastPathContext,
                                                                           generalCaseInputRowType,
                                                                           generalCaseOutputRowType,
                                                                           planner.normalToGeneralMapping(),
                                                                           stage->number());
        logger.info("generated code in " + std::to_string(timer.time()) + "s");
        // can then compile everything, hooray!
    }
}