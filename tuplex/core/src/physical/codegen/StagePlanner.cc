//
// Created by Leonhard Spiegelberg on 2/27/22.
//

#include <physical/codegen/StagePlanner.h>
#include <physical/codegen/AggregateFunctions.h>
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
#include <physical/codegen/StageBuilder.h>
#include <graphviz/GraphVizBuilder.h>
#include "logical/LogicalOptimizerTest.h"

#define VERBOSE_BUILD

namespace tuplex {
    namespace codegen {

        // check type.
        void checkRowType(const python::Type& rowType) {
            assert(rowType.isTupleType());
            for(auto t : rowType.parameters())
                assert(t != python::Type::UNKNOWN);
        }

        template<typename T> std::vector<T> vec_prepend(const T& t, const std::vector<T>& v) {
            using namespace std;
            vector<T> res; res.reserve(v.size() + 1);
            res.push_back(t);
            for(const auto& el : v)
                res.push_back(el);
            return res;
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::filterReordering(const std::vector<Row>& sample) {
            // todo

            return vec_prepend(_inputNode, _operators);
        }


        python::Type StagePlanner::get_specialized_row_type(const std::shared_ptr<LogicalOperator>& inputNode, const DetectionStats& ds) const {
            assert(inputNode);

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            logger.debug("output schema of input node is: " + inputNode->getOutputSchema().getRowType().desc());

            // check if inputNode is FileInput -> i.e. projection scenario!
            if(inputNode->type() == LogicalOperatorType::FILEINPUT) {
                auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode);

                auto pushed_down_output_row_type = fop->getOutputSchema().getRowType();
                std::vector<python::Type> col_types = pushed_down_output_row_type.parameters();
                auto cols_to_serialize = fop->columnsToSerialize();
                if(cols_to_serialize.size() != ds.constant_row.getNumColumns()) {
                    throw std::runtime_error("internal error in constant-folding optimization, original number of columns not matching detection columns");
                } else {

                    logger.debug("file input operator output type: " + fop->getOutputSchema().getRowType().desc());
                    col_types = std::vector<python::Type>(cols_to_serialize.size(), python::Type::NULLVALUE); // init as dummy nulls
                    auto input_col_types = fop->getOutputSchema().getRowType().parameters();
                    unsigned pos = 0;
                    for(unsigned i = 0; i < cols_to_serialize.size(); ++i) {
                        // to be serialized or not?
                        if(cols_to_serialize[i]) {
                            assert(pos < input_col_types.size());
                            col_types[i] = input_col_types[pos++];
                        }
                    }
                }
                auto reprojected_output_row_type = python::Type::makeTupleType(col_types);
                auto specialized_row_type = ds.specialize_row_type(reprojected_output_row_type);

                // create projected version
                col_types.clear();
                for(unsigned i = 0; i < cols_to_serialize.size(); ++i) {
                    // to be serialized or not?
                    if(cols_to_serialize[i]) {
                        col_types.push_back(specialized_row_type.parameters()[i]);
                    }
                }

                auto projected_specialized_row_type = python::Type::makeTupleType(col_types);
                return projected_specialized_row_type;
            } else {
                // simple, no reprojection done. Just specialize type.
                return ds.specialize_row_type(inputNode->getOutputSchema().getRowType());
            }
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::constantFoldingOptimization(const std::vector<Row>& sample) {
            using namespace std;
            vector<shared_ptr<LogicalOperator>> opt_ops;

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // at least 100 samples required to be somehow reliable
            static size_t MINIMUM_SAMPLES_REQUIRED = 100;

            if(!_useConstantFolding || sample.size() < MINIMUM_SAMPLES_REQUIRED) {
                if(sample.size() < MINIMUM_SAMPLES_REQUIRED)
                    logger.debug("not enough samples to reliably apply constant folding optimization, skipping.");
                return vec_prepend(_inputNode, _operators);
            }

            // should have at least 100 samples to determine this...
            // check which columns could be constants and if so propagate that information!
            logger.info("Performing constant folding optimization");

            // detect constants over sample.
            // note that this sample is WITHOUT any projection pushdown, i.e. full columns
            DetectionStats ds;
            ds.detect(sample);

            {
                assert(!sample.empty());
                std::stringstream ss;
                ss<<"sample has "<<pluralize(sample.size(), "row")<<" rows with "<<pluralize(sample.front().getNumColumns(), "column");
                logger.debug(ss.str());
            }

            // no column constants? skip optimization!
            if(ds.constant_column_indices().empty()) {
                logger.debug("skipping constant folding optimization, no constants detected.");
                return vec_prepend(_inputNode, _operators);
            }

            // clone input operator
            auto inputNode = _inputNode ? _inputNode->clone() : nullptr;
            if(inputNode)
                inputNode->setID(_inputNode->getID());

            {
                // print info
                std::stringstream ss;
                ss<<"Identified "<<pluralize(ds.constant_column_indices().size(), "column")<<" to be constant: "<<ds.constant_column_indices()<<endl;

                // print out which rows are considered constant (and with which values!)
                for(auto idx : ds.constant_column_indices()) {
                    string column_name;
                    if(inputNode && !inputNode->inputColumns().empty())
                        column_name = inputNode->inputColumns()[idx];
                    ss<<" - "<<column_name<<": "<<ds.constant_row.get(idx).desc()<<" : "<<ds.constant_row.get(idx).getType().desc()<<endl;
                }
                logger.debug(ss.str());
            }

            // generate checks for all the original column indices that are constant
            vector<size_t> checks_indices = ds.constant_column_indices();
            vector<NormalCaseCheck> checks;
            auto unprojected_row_type = unprojected_optimized_row_type();
            for(auto idx : checks_indices) {
                auto underlying_type = ds.constant_row.getType(idx);
                auto underlying_constant = ds.constant_row.get(idx).desc();
                auto constant_type = python::Type::makeConstantValuedType(underlying_type, underlying_constant);

                constant_type = simplifyConstantType(constant_type);

                // null-checks handled separately, do not add them
                // if null-value optimization has been already performed.
                // --> i.e., they're done using the schema (?)
                assert(idx < unprojected_row_type.parameters().size());
                auto opt_schema_col_type = unprojected_row_type.parameters()[idx];
                if(constant_type == python::Type::NULLVALUE && opt_schema_col_type == python::Type::NULLVALUE) {
                    // skip
                } else {
                    if(constant_type.isConstantValued())
                        checks.emplace_back(NormalCaseCheck::ConstantCheck(idx, constant_type));
                    else if(constant_type == python::Type::NULLVALUE) {
                        checks.emplace_back(NormalCaseCheck::NullCheck(idx));
                    } else {
                        logger.error("invalid constant type to check for: " + constant_type.desc());
                    }
                }
            }
            logger.debug("generated " + pluralize(checks.size(), "check") + " for stage");

            // folding is done now in two steps:
            // 1. propagate the new input type through the ASTs, i.e. make certain fields constant
            //    this will help to drop potentially columns!

            // first issue is, stage could have been already equipped with projection pushdown --> need to reflect this
            // => get mapping from original to pushed down for input types.
            // i.e. create dummy and fill in
            auto projected_specialized_row_type = get_specialized_row_type(inputNode, ds);

            logger.debug("specialized output-type of " + inputNode->name() + " from " +
                         inputNode->getOutputSchema().getRowType().desc() + " to " + projected_specialized_row_type.desc());

            // check which input columns are required and remove checks.
            // --> this requires pushdown to work before!
            auto acc_cols = get_accessed_columns({inputNode});
            std::vector<NormalCaseCheck> projected_checks;
            for(auto col_idx : acc_cols) {
               for(auto check : checks) {
                   if(check.colNo == col_idx)
                       projected_checks.push_back(checks[col_idx]);
               }
            }
            logger.debug("normal case detection requires "
                         + pluralize(checks.size(), "check")
                         + ", given current logical optimizations "
                         + pluralize(projected_checks.size(), "check")
                         + " are required to detect normal case.");

            // set input type for input node
            auto input_type_before = inputNode->getOutputSchema().getRowType();
            inputNode->retype({projected_specialized_row_type});
            auto lastParent = inputNode;
            opt_ops.push_back(inputNode);
            logger.debug("input (before): " + input_type_before.desc() +
            "\ninput (after): " + inputNode->getOutputSchema().getRowType().desc());

            // retype the other operators.
            for(const auto& op : _operators) {

                // clone operator & specialize!
                auto opt_op = op->clone();
                opt_op->setParent(lastParent);
                opt_op->setID(op->getID());

                // before row type
                std::stringstream ss;
                ss<<op->name()<<" (before): "<<op->getInputSchema().getRowType().desc()<<" -> "<<op->getOutputSchema().getRowType().desc()<<endl;
                // retype
                ss<<"retyping with parent's output schema: "<<lastParent->getOutputSchema().getRowType().desc()<<endl;
                opt_op->retype({lastParent->getOutputSchema().getRowType()});
                // after retype
                ss<<op->name()<<" (after): "<<op->getInputSchema().getRowType().desc()<<" -> "<<op->getOutputSchema().getRowType().desc();
                logger.debug(ss.str());
                opt_ops.push_back(opt_op);
                lastParent = opt_op;
            }

            {
                // print out new chain of types
                std::stringstream ss;
                ss<<"Pipeline (types/before logical opt):\n";

                for(auto op : opt_ops) {
                    ss<<op->name()<<":\n";
                    ss<<"  in: "<<op->getInputSchema().getRowType().desc()<<"\n";
                    ss<<" out: "<<op->getOutputSchema().getRowType().desc()<<"\n";
                    ss<<"  in columns: "<<op->inputColumns()<<"\n";
                    ss<<" out columns: "<<op->columns()<<"\n";
                    ss<<"----\n";
                }

                logger.debug(ss.str());
            }


            // 2. because some fields were replaced with constants, less columns might need to get accessed!
            //    --> perform projection pushdown and then eliminate as many checks as possible
            auto accessed_columns_before_opt = get_accessed_columns(opt_ops);

            // basically use just on this stage the logical optimization pipeline
            auto logical_opt = std::make_unique<LogicalOptimizer>(options());

            // logically optimize pipeline, this performs reordering/projection pushdown etc.
            opt_ops = logical_opt->optimize(opt_ops, true); // inplace optimization

            std::vector<size_t> accessed_columns = get_accessed_columns(opt_ops);

            // so accCols should be sorted, now map to columns. Then check the position in accColsBefore
            // => look up what the original cols were!
            // => then push that down to reader/input node!
            _normalToGeneralMapping = createNormalToGeneralMapping(accessed_columns, accessed_columns_before_opt);

            {
                // print out new chain of types
                std::stringstream ss;
                ss<<"Pipeline (types):\n";

                for(auto op : opt_ops) {
                    ss<<op->name()<<":\n";
                    ss<<"  in: "<<op->getInputSchema().getRowType().desc()<<"\n";
                    ss<<" out: "<<op->getOutputSchema().getRowType().desc()<<"\n";
                    ss<<"  in columns: "<<op->inputColumns()<<"\n";
                    ss<<" out columns: "<<op->columns()<<"\n";
                    ss<<"----\n";
                }

                logger.debug(ss.str());
            }


            {
                std::stringstream ss;
                ss<<"constant folded pipeline requires now only "<<pluralize(accessed_columns.size(), "column");
                ss<<" (general: "<<pluralize(accessed_columns_before_opt.size(), "column")<<")";
                ss<<", reduced checks from "<<checks.size()<<" to "<<projected_checks.size();
                logger.debug(ss.str());

                ss.str("normal col to general col mapping:\n");
                for(auto kv : _normalToGeneralMapping) {
                    ss<<kv.first<<" -> "<<kv.second<<"\n";
                }
                logger.debug(ss.str());
            }




//            // only keep in projected checks the ones that are needed
//            // ?? how ??
//
//            // go over operators and see what can be pushed down!
//            lastParent = inputNode;
//            opt_ops.push_back(inputNode);
//            for(const auto& op : _operators) {
//                // clone operator & specialize!
//                auto opt_op = op->clone();
//                opt_op->setParent(lastParent);
//                opt_op->setID(op->getID());
//
//                switch(opt_op->type()) {
//                    case LogicalOperatorType::MAP: {
//                        auto mop = std::dynamic_pointer_cast<MapOperator>(opt_op);
//                        assert(mop);
//
//                        // do opt only if input cols are valid...!
//
//                        // retype UDF
//                        cout<<"input type before: "<<mop->getInputSchema().getRowType().desc()<<endl;
//                        cout<<"output type before: "<<mop->getOutputSchema().getRowType().desc()<<endl;
//                        cout<<"num input columns required: "<<mop->inputColumns().size()<<endl;
//                        // retype
//                        auto input_cols = mop->inputColumns(); // HACK! won't work if no input cols are specified.
//                        auto input_type = mop->getInputSchema().getRowType();
//                        if(input_cols.empty()) {
//                            logger.debug("skipping, only for input cols now working...");
//                            return _operators;
//                        }
//                        // for all constants detected, add type there & use that for folding!
//                        // if(input_type.parameters().size() == 1 && input_type.parameters().front().isTupleType())
//                        auto tuple_mode = input_type.parameters().size() == 1 && input_type.parameters().front().isTupleType();
//                        if(!tuple_mode) {
//                            logger.debug("only tuple/dict mode supported! skipping for now");
//                            return _operators;
//                        }
//
//                        auto param_types = input_type.parameters()[0].parameters();
//                        if(param_types.size() != input_cols.size()) {
//                            logger.warn("Something wrong, numbers do not match up.");
//                            return _operators;
//                        }
//
//                        // now update these vars with whatever is possible
//                        std::unordered_map<std::string, python::Type> constant_types;
//                        // HACK! do not change column names, else this will fail...!
//                        for(auto idx : ds.constant_column_indices()) {
//                            string column_name;
//                            if(inputNode && !inputNode->inputColumns().empty()) {
//                                column_name = inputNode->inputColumns()[idx];
//                                constant_types[column_name] = python::Type::makeConstantValuedType(ds.constant_row.get(idx).getType(), ds.constant_row.get(idx).desc()); // HACK
//                            }
//                        }
//                        // lookup column names (NOTE: this should be done using integers & properly propagated through op graph)
//                        for(unsigned i = 0; i < input_cols.size(); ++i) {
//                            auto name = input_cols[i];
//                            auto it = constant_types.find(name);
//                            if(it != constant_types.end())
//                                param_types[i] = it->second;
//                        }
//
//                        // now update specialized type with constant if possible!
//                        auto specialized_type = tuple_mode ? python::Type::makeTupleType({python::Type::makeTupleType(param_types)}) : python::Type::makeTupleType(param_types);
//                        if(specialized_type != input_type) {
//                            cout<<"specialized type "<<input_type.desc()<<endl;
//                            cout<<"  - to - "<<endl;
//                            cout<<specialized_type.desc()<<endl;
//                        } else {
//                            cout<<"no specialization possible, same type";
//                            // @TODO: can skip THIS optimization, continue with the next one!
//                        }
//
//                        auto accColsBeforeOpt = mop->getUDF().getAccessedColumns();
//
//                        mop->retype({specialized_type});
//
//                        // now check again what columns are required from input, if different count -> push down!
//                        // @TODO: this could get difficult for general graphs...
//                        auto accCols = mop->getUDF().getAccessedColumns();
//                        // Note: this works ONLY for now, because no other op after this...
//
//
//                        // check again
//                        cout<<"input type after: "<<mop->getInputSchema().getRowType().desc()<<endl;
//                        cout<<"output type after: "<<mop->getOutputSchema().getRowType().desc()<<endl;
//                        cout<<"num input columns required after opt: "<<accCols.size()<<endl;
//
//                        // which columns where eliminated?
//                        //     const std::vector<int> v1 {1, 2, 5, 5, 5, 9};
//                        //    const std::vector<int> v2 {2, 5, 7};
//                        //    std::vector<int> diff; // { 1 2 5 5 5 9 } ∖ { 2 5 7 } = { 1 5 5 9 }
//                        //
//                        //    std::set_difference(v1.begin(), v1.end(), v2.begin(), v2.end(),
//                        //                        std::inserter(diff, diff.begin()));
//                        std::sort(accColsBeforeOpt.begin(), accColsBeforeOpt.end());
//                        std::sort(accCols.begin(), accCols.end());
//                        std::vector<size_t> diff;
//                        std::set_difference(accColsBeforeOpt.begin(), accColsBeforeOpt.end(),
//                                            accCols.begin(), accCols.end(), std::inserter(diff, diff.begin()));
//                        cout<<"There were "<<pluralize(diff.size(), "column")<<" optimized away:"<<endl;
//                        vector<string> opt_away_names;
//                        for(auto idx : diff)
//                            opt_away_names.push_back(mop->inputColumns()[idx]);
//                        cout<<"-> "<<opt_away_names<<endl;
//
//                        // rewrite which columns to access in input node
//                        if(inputNode->type() != LogicalOperatorType::FILEINPUT) {
//                            logger.error("stopping here, should get support for ops...");
//                            return opt_ops;
//                        }
//                        auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode);
//                        auto colsToSerialize = fop->columnsToSerialize();
//                        vector<size_t> colsToSerializeIndices;
//                        for(unsigned i = 0; i < colsToSerialize.size(); ++i)
//                            if(colsToSerialize[i])
//                                colsToSerializeIndices.push_back(i);
//                        cout<<"reading columns: "<<colsToSerializeIndices<<endl;
//
//                        cout<<"Column indices to read before opt: "<<accColsBeforeOpt<<endl;
//                        cout<<"After opt only need to read: "<<accCols<<endl;
//
//                        // TODO: need to also rewrite access in mop again
//                        // mop->rewriteParametersInAST(rewriteMap);
//
//                        // gets a bit more difficult now:
//                        // num input columns required after opt: 13
//                        //There were 4 columns optimized away:
//                        //-> [YEAR, MONTH, CRS_DEP_TIME, CRS_ELAPSED_TIME]
//                        //reading columns: [0, 2, 3, 6, 10, 11, 20, 29, 31, 42, 50, 54, 56, 57, 58, 59, 60]
//                        //Column indices to read before opt: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
//                        //After opt only need to read: [2, 3, 4, 5, 6, 8, 9, 11, 12, 13, 14, 15, 16]
//
//                        // so accCols should be sorted, now map to columns. Then check the position in accColsBefore
//                        // => look up what the original cols were!
//                        // => then push that down to reader/input node!
//                        unordered_map<size_t, size_t> rewriteMap;
//                        vector<size_t> indices_to_read_from_previous_op;
//                        vector<string> rewriteInfo; // for printing info!
//                        vector<string> col_names_to_read_before;
//                        vector<string> col_names_to_read_after;
//
//                        // redo normal to general mapping
//                        _normalToGeneralMapping.clear();
//                        for(unsigned i = 0; i < accCols.size(); ++i) {
//                            rewriteMap[accCols[i]] = i;
//
//                            rewriteInfo.push_back(to_string(accCols[i]) + " -> " + to_string(i));
//
//                            // save normal -> general mapping
//                            _normalToGeneralMapping[i] = accCols[i];
//
//                            int j = 0;
//                            while(j < accColsBeforeOpt.size() && accCols[i] != accColsBeforeOpt[j])
//                                ++j;
//                            indices_to_read_from_previous_op.push_back(colsToSerializeIndices[j]);
//                        }
//
//                        for(auto idx : colsToSerializeIndices)
//                            col_names_to_read_before.push_back(fop->inputColumns()[idx]);
//                        for(auto idx : indices_to_read_from_previous_op)
//                            col_names_to_read_after.push_back(fop->inputColumns()[idx]);
//                        cout<<"Rewriting indices: "<<rewriteInfo<<endl;
//
//                        // this is quite hacky...
//                        // ==> CLONE???
//                        fop->selectColumns(indices_to_read_from_previous_op);
//                        cout<<"file input now only reading: "<<indices_to_read_from_previous_op<<endl;
//                        cout<<"I.e., read before: "<<col_names_to_read_before<<endl;
//                        cout<<"now: "<<col_names_to_read_after<<endl;
//                        fop->useNormalCase(); // !!! retype input op. mop already retyped above...
//
//                        mop->rewriteParametersInAST(rewriteMap);
//                        // retype!
//                        cout<<"mop updated: \ninput type: "<<mop->getInputSchema().getRowType().desc()
//                            <<"\noutput type: "<<mop->getOutputSchema().getRowType().desc()<<endl;
//
//#ifdef GENERATE_PDFS
//                        mop->getUDF().getAnnotatedAST().writeGraphToPDF("final_mop_udf.pdf");
//                        // write typed version as well
//                        mop->getUDF().getAnnotatedAST().writeGraphToPDF("final_mop_udf_wtypes.pdf", true);
//#endif
//                        // @TODO: should clone operators etc. here INCL. input oprator else issue.
//                        // input operator needs additional check... => put this check into parser...
//
//
//                        break;
//                    }
//                    default:
//                        throw std::runtime_error("Unknown operator " + opt_op->name());
//                }
//                lastParent = opt_op;
//
//                opt_ops.push_back(opt_op);
//            }

            // check accesses -> i.e. need to check for all funcs till first map or end of stage is reached.
            // why? b.c. map destroys structure. The other require analysis though...!
            // i.e. trace using sample... (this could get expensive!)

            return opt_ops;
        }

        void StagePlanner::optimize() {
            using namespace std;

            vector<shared_ptr<LogicalOperator>> optimized_operators = vec_prepend(_inputNode, _operators);
            auto& logger = Logger::instance().logger("specializing stage optimizer");

            if(_useNVO) {
                logger.info("performing NVO");
                optimized_operators = nullValueOptimization();

                // overwrite internal operators to apply subsequent optimizations
                _inputNode = _inputNode ? optimized_operators.front() : nullptr;
                _operators = _inputNode ? vector<shared_ptr<LogicalOperator>>{optimized_operators.begin() + 1,
                                                                              optimized_operators.end()}
                                                                              : optimized_operators;
            }

            // step 1: retrieve sample from inputnode!
            std::vector<Row> sample = fetchInputSample();

            // perform sample based optimizations
            if(_useConstantFolding) {
                logger.info("performing Constant-Folding specialization");
                optimized_operators = constantFoldingOptimization(sample);

                // overwrite internal operators to apply subsequent optimizations
                _inputNode = _inputNode ? optimized_operators.front() : nullptr;
                _operators = _inputNode ? vector<shared_ptr<LogicalOperator>>{optimized_operators.begin() + 1,
                                                                              optimized_operators.end()}
                                                                              : optimized_operators;
            }

            // can filters get pushed down even further? => check! constant folding may remove code!
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::nullValueOptimization() {
            using namespace std;

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // only null-value opt yet supported
            if(!_useNVO)
                return vec_prepend(_inputNode, _operators);

            // special case: cache operator! might have exceptions or no exceptions => specialize depending on that!
            // i.e. an interesting case happens when join(... .cache(), ...) is used. Then, need to upcast result to general case
            // => for now, do not support...
            // => upcasting should be done in LocalBackend.


            // no input node or input node not FileInputOperator?
            // => can't specialize...
            if(!_inputNode)
                return _operators;

            if(_inputNode->type() != LogicalOperatorType::FILEINPUT && _inputNode->type() != LogicalOperatorType::CACHE) {
                logger.debug("Skipping null-value optimization for pipeline because input node is " + _inputNode->name());
                return vec_prepend(_inputNode, _operators);
            }

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
                ss<<"specializing pipeline for normal case ("<<pluralize(_operators.size(), "operator")<<")"<<endl;
                ss<<"input node: "<<_inputNode->name()<<endl;
                ss<<"optimized schema of input node: "<<opt_input_rowtype.desc()<<endl;
                logger.debug(ss.str());
            }
#endif
            {
                stringstream ss;
                auto original_input_rowtype = _inputNode->getOutputSchema().getRowType();
                if(opt_input_rowtype != original_input_rowtype)
                    ss<<"NVO can specialize input schema from\n"<<original_input_rowtype.desc()<<"\n- to - \n"<<opt_input_rowtype.desc();
                else
                    ss<<"no specialization using NVO possible";
                logger.debug(ss.str());
            }

            auto last_rowtype = opt_input_rowtype;
            checkRowType(last_rowtype);

            bool pipeline_breaker_present = false;

            // go through ops & specialize (leave jop as is)
            vector<std::shared_ptr<LogicalOperator>> opt_ops;
            std::shared_ptr<LogicalOperator> lastNode = nullptr;
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode->clone());
            fop->useNormalCase();
            opt_ops.push_back(fop);
            // go over the other ops from the stage...
            for(const auto& node : _operators) {
                auto lastParent = opt_ops.empty() ? _inputNode : opt_ops.back();
                if(!lastNode)
                    lastNode = lastParent; // set lastNode to parent to make join on fileop work!

                switch(node->type()) {
                    // handled above...
                    //  case LogicalOperatorType::PARALLELIZE: {
                    //      opt_ops.push_back(node);
                    //      break;
                    //  }
                    //  case LogicalOperatorType::FILEINPUT: {
                    //      // create here copy using normalcase!
                    //      auto op = std::dynamic_pointer_cast<FileInputOperator>(node->clone());
                    //      op->useNormalCase();
                    //      opt_ops.push_back(op);
                    //      break;
                    //  }
                        // construct clone with parents & retype
                    case LogicalOperatorType::FILTER:
                    case LogicalOperatorType::MAP:
                    case LogicalOperatorType::MAPCOLUMN:
                    case LogicalOperatorType::WITHCOLUMN:
                    case LogicalOperatorType::IGNORE: {
                        auto op = node->clone();
                        auto oldInputType = op->getInputSchema().getRowType();
                        auto oldOutputType = op->getInputSchema().getRowType();

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
                        pipeline_breaker_present = true;
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
                            jop = std::dynamic_pointer_cast<JoinOperator>(opt_ops.back());
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
                        pipeline_breaker_present = true;
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
            if(!opt_ops.empty() && pipeline_breaker_present)
                assert(opt_ops.back()->cost() > 0);

            return opt_ops;
        }

        std::vector<Row> StagePlanner::fetchInputSample() {
            if(!_inputNode)
                return {};
            return _inputNode->getSample(2000);
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
#ifdef BUILD_WITH_CEREAL
        {
            auto compressed_str = stage->_encodedData;
            auto decompressed_str = decompress_string(compressed_str);
            logger.info("Decompressed Code context from " + sizeToMemString(compressed_str.size()) + " to " + sizeToMemString(decompressed_str.size()));
            std::istringstream iss(decompressed_str);
            cereal::BinaryInputArchive ar(iss);
            ar(ctx);
        }
        logger.info("Decode took " + std::to_string(timer.time()) + "s");
#else
        // use custom JSON encoding
        auto compressed_str = stage->_encodedData;
        auto decompressed_str = decompress_string(compressed_str);
        logger.info("Decompressed Code context from " + sizeToMemString(compressed_str.size()) + " to " + sizeToMemString(decompressed_str.size()));
        ctx = codegen::CodeGenerationContext::fromJSON(decompressed_str);
#endif
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
        planner.optimize();
        path_ctx.inputNode = planner.input_node();
        path_ctx.operators = planner.optimized_operators();
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
        stage->_fastCodePath = codegen::StageBuilder::generateFastCodePath(ctx,
                                                                           ctx.fastPathContext,
                                                                           generalCaseInputRowType,
                                                                           ctx.slowPathContext.columnsToRead,
                                                                           generalCaseOutputRowType,
                                                                           planner.normalToGeneralMapping(),
                                                                           stage->number());
        logger.info("generated code in " + std::to_string(timer.time()) + "s");
        // can then compile everything, hooray!
    }

    std::vector<size_t>
    codegen::StagePlanner::get_accessed_columns(const std::vector<std::shared_ptr<LogicalOperator>> &ops) {
        std::vector<size_t> col_idxs;

        if(ops.empty())
            return col_idxs;
        if(ops.front()->type() == LogicalOperatorType::FILEINPUT) {
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(ops.front());
            auto cols = fop->columnsToSerialize();
            auto num_columns = cols.size();
            for(unsigned i = 0; i < num_columns; ++i) {
                if(cols[i])
                    col_idxs.emplace_back(i);
            }
        } else {
            // check from type
            auto num_columns = ops.front()->getOutputSchema().getRowType().parameters().size();
            for(unsigned i = 0; i < num_columns; ++i)
                col_idxs.emplace_back(i);
        }

        return col_idxs;
    }

    std::map<int, int> codegen::StagePlanner::createNormalToGeneralMapping(const std::vector<size_t>& normalAccessedOriginalIndices,
                                               const std::vector<size_t>& generalAccessedOriginalIndices) {
        std::map<int, int> m;

        // normal case should be less than general
        assert(normalAccessedOriginalIndices.size() <= generalAccessedOriginalIndices.size());

        // create two lookup maps (incl. original columns!)
        std::unordered_map<size_t, size_t> originalToGeneral;
        for(unsigned i = 0; i < generalAccessedOriginalIndices.size(); ++i) {
            originalToGeneral[generalAccessedOriginalIndices[i]] = i;
        }

        // now lookup from normal
        for(unsigned i = 0; i < normalAccessedOriginalIndices.size(); ++i) {
            // this should NOT error
            m[i] = originalToGeneral[normalAccessedOriginalIndices[i]];
        }

        // debug check
#ifndef NDEBUG
        for(auto kv : m) {
            // original index should correspond!
            assert(normalAccessedOriginalIndices[kv.first] == generalAccessedOriginalIndices[kv.second]);
        }
#endif

        return m;
    }
}