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
#include "logical/LogicalOptimizer.h"
#include <unordered_map>

#define VERBOSE_BUILD

namespace tuplex {
    namespace codegen {

        // check type.
        void checkRowType(const python::Type& rowType) {
            assert(rowType.isExceptionType() || rowType.isTupleType());
            if(rowType.isTupleType())
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
                return ds.specialize_row_type(inputNode->getOutputSchema().getRowType());

//                auto pushed_down_output_row_type = fop->getOutputSchema().getRowType();
//                std::vector<python::Type> col_types = pushed_down_output_row_type.parameters();
//                auto num_cols_to_serialize = fop->outputColumnCount();
//                if(num_cols_to_serialize != ds.constant_row.getNumColumns()) {
//                    throw std::runtime_error("internal error in constant-folding optimization, original number of columns not matching detection columns");
//                } else {
//
//                    logger.debug("file input operator output type: " + fop->getOutputSchema().getRowType().desc());
//                    col_types = std::vector<python::Type>(num_cols_to_serialize, python::Type::NULLVALUE); // init as dummy nulls
//                    auto input_col_types = fop->getOutputSchema().getRowType().parameters();
//                    unsigned pos = 0;
//                    for(unsigned i = 0; i < cols_to_serialize.size(); ++i) {
//                        // to be serialized or not?
//                        if(cols_to_serialize[i]) {
//                            assert(pos < input_col_types.size());
//                            col_types[i] = input_col_types[pos++];
//                        }
//                    }
//                }
//                auto reprojected_output_row_type = python::Type::makeTupleType(col_types);
//                auto specialized_row_type = ds.specialize_row_type(reprojected_output_row_type);
//
//                // create projected version
//                col_types.clear();
//                for(unsigned i = 0; i < cols_to_serialize.size(); ++i) {
//                    // to be serialized or not?
//                    if(cols_to_serialize[i]) {
//                        col_types.push_back(specialized_row_type.parameters()[i]);
//                    }
//                }
//
//                auto projected_specialized_row_type = python::Type::makeTupleType(col_types);
//                return projected_specialized_row_type;
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
                    logger.warn("not enough samples to reliably apply constant folding optimization, consider increasing sample size.");
                // return vec_prepend(_inputNode, _operators);
            }

            // should have at least 100 samples to determine this...
            // check which columns could be constants and if so propagate that information!
            logger.info("Performing constant folding optimization (sample size=" + std::to_string(sample.size()) + ")");

            // first, need to detect which columns are required. This is important because of the checks.
            auto acc_cols_before_opt = this->get_accessed_columns();
            {
                std::string col_str = "";
                for(auto idx : acc_cols_before_opt) {
                    col_str += std::to_string(idx) + " ";
                }
                logger.info("accessed columns before folding: " + col_str);
            }


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
            if(inputNode) {
                inputNode->setID(_inputNode->getID());
                if(inputNode->type() == LogicalOperatorType::FILEINPUT)
                    std::dynamic_pointer_cast<FileInputOperator>(inputNode)->cloneCaches(*((FileInputOperator*)_inputNode.get()));
            }


            {
                // print info
                std::stringstream ss;
                ss<<"Identified "<<pluralize(ds.constant_column_indices().size(), "column")<<" to be constant: "<<ds.constant_column_indices()<<endl;

                // print out which rows are considered constant (and with which values!)
                // --> note that these checks are done POST initial pushdown.
                // i.e. use output columns. (previously: input columns)
                for(auto idx : ds.constant_column_indices()) {
                    string column_name;
                    if(inputNode && !inputNode->columns().empty())
                        column_name = inputNode->columns()[idx];
                    ss<<" - "<<column_name<<": "<<ds.constant_row.get(idx).desc()<<" : "<<ds.constant_row.get(idx).getType().desc()<<endl;
                }
                logger.debug(ss.str());
            }

            // generate checks for all the original column indices that are constant
            vector<size_t> checks_indices = ds.constant_column_indices();
            vector<NormalCaseCheck> checks;
            auto unprojected_row_type = unprojected_optimized_row_type();
            if(inputNode) {
                for(auto idx : checks_indices) {
                    auto underlying_type = ds.constant_row.getType(idx);
                    auto underlying_constant = ds.constant_row.get(idx).desc();
                    auto constant_type = python::Type::makeConstantValuedType(underlying_type, underlying_constant);

                    constant_type = simplifyConstantType(constant_type);

                    // original index is from ALL available rows in input node
                    size_t original_idx = 0;
                    if(inputNode->type() == LogicalOperatorType::FILEINPUT)
                        original_idx = std::dynamic_pointer_cast<FileInputOperator>(inputNode)->reverseProjectToReadIndex(
                                idx);
                    else {
                        // no project on other operators.
                        original_idx = idx;
                        auto out_column_count = inputNode->getOutputSchema().getRowType().parameters().size();
                        assert(original_idx <= out_column_count);
                    }

                    // null-checks handled separately, do not add them
                    // if null-value optimization has been already performed.
                    // --> i.e., they're done using the schema (?)
                    assert(original_idx < extract_columns_from_type(unprojected_row_type));
                    auto opt_schema_col_type = unprojected_row_type.isRowType() ? unprojected_row_type.get_column_type(original_idx) : unprojected_row_type.parameters()[original_idx];
                    if(constant_type == python::Type::NULLVALUE && opt_schema_col_type == python::Type::NULLVALUE) {
                        // skip
                    } else {
                        if(constant_type.isConstantValued())
                            checks.emplace_back(NormalCaseCheck::ConstantCheck(original_idx, constant_type));
                        else if(constant_type == python::Type::NULLVALUE) {
                            checks.emplace_back(NormalCaseCheck::NullCheck(original_idx));
                        } else {
                            logger.error("invalid constant type to check for: " + constant_type.desc());
                        }
                    }
                }
                logger.debug("generated " + pluralize(checks.size(), "check") + " for stage");
            } else {
                logger.debug("skipped check generation, because input node is empty.");
            }


            // folding is done now in two steps:
            // 1. propagate the new input type through the ASTs, i.e. make certain fields constant
            //    this will help to drop potentially columns!

            // first issue is, stage could have been already equipped with projection pushdown --> need to reflect this
            // => get mapping from original to pushed down for input types.
            // i.e. create dummy and fill in
            auto projected_specialized_row_type = get_specialized_row_type(inputNode, ds);

            logger.debug("specialized output-type of " + inputNode->name() + " from " +
                         inputNode->getOutputSchema().getRowType().desc() + " to " + projected_specialized_row_type.desc());

            // check which input columns are required and remove checks. --> this requires to use ORIGINAL indices.
            // --> this requires pushdown to work before!
            auto acc_cols = acc_cols_before_opt;
            std::vector<NormalCaseCheck> projected_checks;
            for(const auto& col_idx : acc_cols) {
                // changed acc cols to retrieve original indices, below is commented old code:
                //  auto original_col_idx =  inputNode->type() == LogicalOperatorType::FILEINPUT ?
                //          std::dynamic_pointer_cast<FileInputOperator>(inputNode)->reverseProjectToReadIndex(col_idx)
                //          : col_idx;
                // new
                auto original_col_idx = col_idx;
               for(const auto& check : checks) {
                   if(check.isSingleColCheck() && check.colNo() == original_col_idx) {
                       projected_checks.emplace_back(check); // need to adjust internal colNo? => no, keep for now.
                   } else if(!check.isSingleColCheck()) {
                       throw std::runtime_error("multi-col check not supported yet");
                   }
               }
            }
            logger.debug("normal case detection requires "
                         + pluralize(checks.size(), "check")
                         + ", given current logical optimizations "
                         + pluralize(projected_checks.size(), "check")
                         + " are required to detect normal case.");

            // set input type for input node
            auto input_type_before = inputNode->getOutputSchema().getRowType();
            logger.debug("retyping stage with type " + projected_specialized_row_type.desc());
            inputNode->retype(projected_specialized_row_type, true);
            if(inputNode->type() == LogicalOperatorType::FILEINPUT)
                ((FileInputOperator*)inputNode.get())->useNormalCase();
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
                opt_op->retype(lastParent->getOutputSchema().getRowType(), true);
                // after retype
                ss<<opt_op->name()<<" (after): "<<opt_op->getInputSchema().getRowType().desc()<<" -> "<<opt_op->getOutputSchema().getRowType().desc();
                logger.debug(ss.str());
                opt_ops.push_back(opt_op);
                lastParent = opt_op;
            }

            {
                // print out new chain of types
                std::stringstream ss;
                ss<<"Pipeline (types/before logical opt):\n";

                for(const auto& op : opt_ops) {
                    ss<<op->name()<<":\n";
                    ss<<"  in: "<<op->getInputSchema().getRowType().desc()<<"\n";
                    ss<<" out: "<<op->getOutputSchema().getRowType().desc()<<"\n";
                    ss<<"  in columns: "<<op->inputColumns()<<"\n";
                    ss<<" out columns: "<<op->columns()<<"\n";
                    ss<<"----\n";
                }

                logger.debug(ss.str());
            }


            // 2. because some fields were replaced with constants, fewer columns might need to get accessed!
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
            std::vector<size_t> out_accessed_columns;
            std::vector<size_t> out_accessed_columns_before_opt;
            // project each when input op present
            if(inputNode && inputNode->type() == LogicalOperatorType::FILEINPUT) {
                auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode);
                for(const auto& idx : accessed_columns)
                    out_accessed_columns.emplace_back(fop->projectReadIndex(idx));
                for(const auto& idx : accessed_columns_before_opt)
                    out_accessed_columns_before_opt.emplace_back(fop->projectReadIndex(idx));
            } else {
                out_accessed_columns = accessed_columns;
                out_accessed_columns_before_opt = accessed_columns_before_opt;
            }
            _normalToGeneralMapping = createNormalToGeneralMapping(out_accessed_columns, out_accessed_columns_before_opt);

            {
                // print out new chain of types
                std::stringstream ss;
                ss<<"Pipeline (types):\n";

                for(const auto& op : opt_ops) {
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

            // add all projected checks
            for(const auto& check : projected_checks)
                _checks.push_back(check);


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

            // clear checks
            //_checks.clear();

            vector<shared_ptr<LogicalOperator>> optimized_operators = vec_prepend(_inputNode, _operators);
            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // run validation on initial pipeline
            bool validation_rc = validatePipeline();
            logger.debug(std::string("initial pipeline validation: ") + (validation_rc ? "ok" : "failed"));

            // step 1: retrieve sample from inputnode!
            std::vector<Row> sample = fetchInputSample();
            std::vector<std::string> sample_columns = _inputNode ? _inputNode->columns() : std::vector<std::string>();

            // retype using sample, need to do this initially.
            retypeOperators(sample, sample_columns);

            // check now whether filters can be promoted or not
            if(_useFilterPromo) {

                auto input_columns_before = _inputNode->columns();

                promoteFilters();

                // want to redo typing if checks changed!
                if(_checks.end() != std::find_if(_checks.begin(),
                                                 _checks.end(),
                                                 [](const NormalCaseCheck& check) {
                    return check.type == CheckType::CHECK_FILTER;
                })) {
                    logger.info("retyping b.c. of promoted filter");
                    auto input_type_before_promo = _inputNode->getOutputSchema().getRowType();
                    sample = fetchInputSample();
                    sample_columns = _inputNode ? _inputNode->columns() : std::vector<std::string>();
                    retypeOperators(sample, sample_columns);

                    auto input_type_after_promo = _inputNode->getOutputSchema().getRowType();
                    if(input_type_after_promo == input_type_before_promo) {
                        logger.debug("input row type didn't change due to promoted filter.");
                    } else {
                        logger.debug("input row type changed:\nwas before: " + input_type_before_promo.desc() + "\nis now: " + input_type_after_promo.desc());
                    }

                    // did column order change?
                    if(!vec_equal(sample_columns, input_columns_before)) {
                        std::stringstream ss;
                        ss<<"input columns changed:\nbefore: "<<input_columns_before<<"\nafter: "<<sample_columns;
                        logger.debug(ss.str());
                    }

                    logger.debug("filter promo done.");
                }
            }


            // perform sample based optimizations
            if(_useConstantFolding) {

                // perform only when input op is present (could do later, but requires sample!)
                if(_inputNode && _inputNode->type() == LogicalOperatorType::FILEINPUT) {
                    logger.info("Performing constant folding optimization (sample size=" + std::to_string(sample.size()) + ")");
                    optimized_operators = constantFoldingOptimization(sample);

                    // overwrite internal operators to apply subsequent optimizations
                    _inputNode = _inputNode ? optimized_operators.front() : nullptr;
                    _operators = _inputNode ? vector<shared_ptr<LogicalOperator>>{optimized_operators.begin() + 1,
                                                                                  optimized_operators.end()}
                                            : optimized_operators;

                    // run validation after applying constant folding
                    validation_rc = validatePipeline();
                    logger.debug(std::string("post-constant-folding pipeline validation: ") + (validation_rc ? "ok" : "failed"));
                } else {
                    logger.debug("skipping constant-folding optimization for stage");
                }
            }

            // can filters get pushed down even further? => check! constant folding may remove code!
        }

        bool StagePlanner::validatePipeline() {

            auto& logger = Logger::instance().logger("specializing stage optimizer");

            python::Type lastRowType;
            if(_inputNode) {
                if(_inputNode->type() == LogicalOperatorType::FILEINPUT) {
                    auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode);
                    lastRowType = fop->getOutputSchema().getRowType();
                } else {
                    lastRowType = _inputNode->getOutputSchema().getRowType();
                }
            } else {
                if(_operators.empty())
                    return true;
                lastRowType = _operators.front()->getInputSchema().getRowType();
            }

            bool validation_ok = true;

            // go through ops and check input/output is compatible (flattened!)
            for(const auto& op : _operators) {
                // note: chain of resolvers works differently, i.e. update lastRowType ONLY for non-resolve ops
                // (ignore is fine)
                if(op->type() == LogicalOperatorType::RESOLVE) {
                    auto rop = std::dynamic_pointer_cast<ResolveOperator>(op);
                    assert(rop);
                    lastRowType = rop->getNormalParent()->getInputSchema().getRowType();
                }

                if(flattenedType(lastRowType) != flattenedType(op->getInputSchema().getRowType())) {
                    logger.error("(" + op->name() + "): input schema "
                                     + op->getInputSchema().getRowType().desc()
                                     + " incompatible with previous operator's output schema "
                                     + lastRowType.desc());
                    validation_ok = false;
                }

                lastRowType = op->getOutputSchema().getRowType();
            }

            return validation_ok;
        }

        std::vector<std::shared_ptr<LogicalOperator>> StagePlanner::retypeUsingOptimizedInputSchema(const python::Type& input_row_type,
                                                                                                    const std::vector<std::string>& input_column_names) {
            using namespace std;

            auto& logger = Logger::instance().logger("specializing stage optimizer");

//            // only null-value opt yet supported
//            if(!_useNVO)
//                return vec_prepend(_inputNode, _operators);

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
                // @TODO: maybe this should also be done using retyping?
                return vec_prepend(_inputNode, _operators);
            }

            // fetch optimized schema from input operator
            Schema opt_input_schema;
            if(_inputNode->type() == LogicalOperatorType::FILEINPUT)
                opt_input_schema = std::dynamic_pointer_cast<FileInputOperator>(_inputNode)->getOptimizedOutputSchema();
            else if(_inputNode->type() == LogicalOperatorType::CACHE) {
                throw std::runtime_error("need to fix here case when columns differ...");
                opt_input_schema = std::dynamic_pointer_cast<CacheOperator>(_inputNode)->getOptimizedOutputSchema();
            } else
                throw std::runtime_error("internal error in specializing for the normal case");
            auto opt_input_rowtype = opt_input_schema.getRowType();

            logger.info("Row type before retype: " + opt_input_rowtype.desc());
            opt_input_rowtype = input_row_type;
            logger.info("Row type after retype: " + opt_input_rowtype.desc());

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
                    ss<<"no specialization using NVO, because types are identical.";
                logger.debug(ss.str());
            }

            auto last_rowtype = opt_input_rowtype;
            checkRowType(last_rowtype);

            bool pipeline_breaker_present = false;

            // go through ops & specialize (leave jop as is)
            vector<std::shared_ptr<LogicalOperator>> opt_ops;
            std::shared_ptr<LogicalOperator> lastNode = nullptr;
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(_inputNode->clone());
            fop->cloneCaches(*((FileInputOperator*)_inputNode.get())); // copy samples!

            // need to restrict potentially?
            RetypeConfiguration r_conf;
            r_conf.is_projected = true;
            r_conf.row_type = input_row_type;
            r_conf.columns = input_column_names;
            r_conf.remove_existing_annotations = true; // remove all annotations (except the column restore ones?)

            // perform quick sanity check
            if(!r_conf.columns.empty())
                assert(r_conf.row_type.parameters().size() == r_conf.columns.size());

            if(!fop->retype(r_conf, true))
                throw std::runtime_error("failed to retype " + fop->name() + " operator."); // for input operator, ignore Option[str] compatibility which is set per default
            fop->useNormalCase(); // this forces output schema to be normalcase (i.e. overwrite internally output schema to be normal case schema)
            opt_ops.push_back(fop);

            last_rowtype = fop->getOutputSchema().getRowType();
            auto last_columns = fop->columns(); // the columns delivered by this particular operator.

            // go over the other ops from the stage...
            for(const auto& node : _operators) {
                auto lastParent = opt_ops.empty() ? _inputNode : opt_ops.back();
                if(!lastNode)
                    lastNode = lastParent; // set lastNode to parent to make join on fileop work!

                // update RetypeConfiguration
                r_conf.is_projected = true;
                r_conf.row_type = last_rowtype;
                r_conf.columns = last_columns;
                r_conf.remove_existing_annotations = true; // remove all annotations (except the column restore ones?)

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
                        assert(node->getInputSchema() != Schema::UNKNOWN);

                        auto op = node->clone(false); // no need to clone with parents, b.c. assigned below.
                        auto oldInputType = op->getInputSchema().getRowType();
                        auto oldOutputType = op->getInputSchema().getRowType();
                        auto columns_before = op->columns();
                        auto in_columns_before = op->inputColumns();

                        checkRowType(last_rowtype.isRowType() ? last_rowtype.get_columns_as_tuple_type() : last_rowtype);
                        // set FIRST the parent. Why? because operators like ignore depend on parent schema
                        // therefore, this needs to get updated first.
                        op->setParent(lastParent); // need to call this before retype, so that columns etc. can be utilized.
                        if(!op->retype(r_conf))
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


                            // columns: before/after
                            ss<<"input columns before:  "<<in_columns_before<<endl;
                            ss<<"output columns before: "<<columns_before<<endl;
                            ss<<"---"<<endl;
                            ss<<"input columns after: "<<op->inputColumns()<<endl;
                            ss<<"output columns after:  "<<op->columns()<<endl;

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
                            last_columns = cop->columns();
                            checkRowType(last_rowtype);
                            // cout<<"cache is a source: optimized schema "<<last_rowtype.desc()<<endl;

                            // use normal case & clone WITHOUT parents
                            // clone, set normal case & push back
                            cop = std::dynamic_pointer_cast<CacheOperator>(cop->cloneWithoutParents());
                            cop->setOptimizedOutputType(last_rowtype);
                            cop->useNormalCase();
                        } else {
                            // cache should not have any children
                            assert(cop->children().empty());
                            // => i.e. first time cache is seen, it's processed as action!
                            // cout<<"cache is action, optimized schema: "<<endl;
                            // cout<<"cache normal case will be: "<<last_rowtype.desc()<<endl;
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

                        auto aop = std::dynamic_pointer_cast<AggregateOperator>(node);
                        // need to retype aggregateByKey operator with new, specialized input type!
                        assert(node->getInputSchema() != Schema::UNKNOWN);

                        auto op = node->clone(false); // no need to clone with parents, b.c. assigned below.
                        auto oldInputType = node->getInputSchema().getRowType();
                        auto oldOutputType = node->getInputSchema().getRowType();
                        auto columns_before = node->columns();

                        {
                            std::stringstream ss;
                            ss<<"retyping "<<aop->name()<<std::endl;
                            logger.debug(ss.str());
                        }


                        checkRowType(last_rowtype);
                        // set FIRST the parent. Why? because operators like ignore depend on parent schema
                        // therefore, this needs to get updated first.
                        op->setParent(lastParent); // need to call this before retype, so that columns etc. can be utilized.
                        if(!op->retype(r_conf))
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


                            // columns: before/after
                            ss<<"output columns before: "<<columns_before<<endl;
                            ss<<"output columns after: "<<op->columns()<<endl;

                            logger.debug(ss.str());
                        }
#endif
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
                    if(opt_ops.back()->type() != LogicalOperatorType::CACHE) {
                        last_rowtype = opt_ops.back()->getOutputSchema().getRowType();
                        last_columns = opt_ops.back()->columns();
                    }

                    checkRowType(last_rowtype.isRowType() ? last_rowtype.get_columns_as_tuple_type() : last_rowtype);
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
            return _inputNode->getSample(10000); // 10k rows. -> should be stratified sample?
        }
    }

    // HACK: magical experiment function!!!
    // HACK!
    // bool hyperspecialize(TransformStage *stage, const URI& uri, size_t file_size,
    //                         double nc_threshold, size_t sample_limit, bool enable_cf)
    bool hyperspecialize(TransformStage *stage,
                         const URI& uri,
                         size_t file_size,
                         size_t sample_limit,
                         size_t strata_size=1,
                         size_t samples_per_strata=1,
                         const codegen::StageBuilderConfiguration& conf=codegen::StageBuilderConfiguration()) {
        auto& logger = Logger::instance().logger("hyper specializer");
        // run hyperspecialization using planner, yay!
        assert(stage);

        // need to decode CodeGenerationCOntext from stage
        if(stage->_encodedData.empty()) {
            logger.info("did not find encoded codegen context, skipping.");
            return false;
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
            Timer deserializeTimer;
            std::istringstream iss(decompressed_str);
            cereal::BinaryInputArchive ar(iss);
            ar(ctx);
            logger.info("Deserialization of Code context took " + std::to_string(deserializeTimer.time()) + "s");
        }
        logger.info("Total Stage Decode took " + std::to_string(timer.time()) + "s");
#else
        // use custom JSON encoding
        auto compressed_str = stage->_encodedData;
        auto decompressed_str = decompress_string(compressed_str);
        logger.info("Decompressed Code context from " + sizeToMemString(compressed_str.size()) + " to " + sizeToMemString(decompressed_str.size()));
        Timer deserializeTimer;
        ctx = codegen::CodeGenerationContext::fromJSON(decompressed_str);
        logger.info("Deserialization of Code context took " + std::to_string(deserializeTimer.time()) + "s");
        logger.info("Total Stage Decode took " + std::to_string(timer.time()) + "s");
#endif
        // old hacky version
        // auto ctx = codegen::CodeGenerationContext::fromJSON(stage->_encodedData);

        assert(ctx.slowPathContext.valid());
        // decoded, now specialize
        auto path_ctx = ctx.slowPathContext;
        //specializePipeline(fastPath, uri, file_size);
        auto inputNode = path_ctx.inputNode;
        auto operators = path_ctx.operators;

        auto input_row_type = inputNode->getOutputSchema().getRowType(); if(input_row_type.isRowType())input_row_type = input_row_type.get_columns_as_tuple_type();
        size_t num_input_before_hyper = input_row_type.parameters().size();
        logger.info("number of input columns before hyperspecialization: " + std::to_string(num_input_before_hyper));

        // force resampling b.c. of thin layer
        Timer samplingTimer;
        bool enable_cf = conf.constantFoldingOptimization;
        if(inputNode->type() == LogicalOperatorType::FILEINPUT) {
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(inputNode); assert(fop);

            // // debug: overwrite values!
            // auto temp_uri = URI("/home/leonhards/projects/tuplex-public/tuplex/cmake-build-debug-w-cereal/dist/bin/bad_sample.ndjson");
            // strata_size = 1;
            // samples_per_strata = 1;
            // VirtualFileSystem::fromURI(temp_uri).file_size(temp_uri, file_size);
            // fop->setInputFiles({temp_uri}, {file_size}, true, sample_limit, true, strata_size, samples_per_strata);

            // use sampling size provided
            if(0 != conf.sampling_size)
                fop->setSamplingSize(conf.sampling_size);

            // resample
            fop->setInputFiles({uri}, {file_size}, true, sample_limit, true, strata_size, samples_per_strata);

            if(fop->fileFormat() == FileFormat::OUTFMT_JSON) {
                enable_cf = false;
                logger.warn("Disabled constant-folding for now (not supported for JSON yet).");
            }
        }

        {
            std::stringstream ss;
            ss<<"sampling (setInputFiles) took " + std::to_string(samplingTimer.time()) + "s";
            if(sample_limit < std::numeric_limits<size_t>::max())
                ss << " (limit=" << sample_limit << ")";
            logger.info(ss.str());
        }

        // node need to find some smart way to QUICKLY detect whether the optimization can be applied or should be rather skipped...
        codegen::StagePlanner planner(inputNode, operators, conf.policy.normalCaseThreshold);


        planner.enableAll();
        planner.disableAll();
        if(conf.nullValueOptimization)
            planner.enableNullValueOptimization();
        if(conf.filterPromotion)
            planner.enableFilterPromoOptimization();
        planner.enableDelayedParsingOptimization();
        if(enable_cf)
            planner.enableConstantFoldingOptimization();
        planner.optimize();
        path_ctx.inputNode = planner.input_node();
        path_ctx.operators = planner.optimized_operators();
        path_ctx.checks = planner.checks();

        assert(inputNode->getID() == path_ctx.inputNode->getID());

        // output schema: the schema this stage yields ultimately after processing
        // input schema: the (optimized/projected, normal case) input schema this stage reads from (CSV, Tuplex, ...)
        // read schema: the schema to read from (specialized) but unprojected.
        if(path_ctx.inputNode->type() == LogicalOperatorType::FILEINPUT) {
            auto fop = std::dynamic_pointer_cast<FileInputOperator>(path_ctx.inputNode);
            path_ctx.inputSchema = fop->getOptimizedOutputSchema();
            path_ctx.readSchema = fop->getOptimizedInputSchema(); // when null-value opt is used, then this is different! hence apply!
            path_ctx.columnsToRead = fop->columnsToSerialize();
             // print out columns & types!
            auto col_types = path_ctx.inputSchema.getRowType().isRowType() ? path_ctx.inputSchema.getRowType().get_column_types() : path_ctx.inputSchema.getRowType().parameters();
            assert(fop->columns().size() == col_types.size());
            for(unsigned i = 0; i < fop->columns().size(); ++i) {
                std::cout<<"col "<<i<<" (" + fop->columns()[i] + ")"<<": "<<col_types[i].desc()<<std::endl;
            }

        } else {
            path_ctx.inputSchema = path_ctx.inputNode->getOutputSchema();
            path_ctx.readSchema = Schema::UNKNOWN; // not set, b.c. not a reader...
            path_ctx.columnsToRead = {};
        }

        path_ctx.outputSchema = path_ctx.operators.back()->getOutputSchema();
        logger.info("specialized to input:  " + path_ctx.inputSchema.getRowType().desc());
        logger.info("specialized to output: " + path_ctx.outputSchema.getRowType().desc());

        // print out:
        // std::cout<<"input schema ("<<pluralize(path_ctx.inputSchema.getRowType().parameters().size(), "column")<<"): "<<path_ctx.inputSchema.getRowType().desc()<<std::endl;
        // std::cout<<"read schema ("<<pluralize(path_ctx.readSchema.getRowType().parameters().size(), "column")<<"): "<<path_ctx.readSchema.getRowType().desc()<<std::endl;
        // std::cout<<"output schema ("<<pluralize(path_ctx.outputSchema.getRowType().parameters().size(), "column")<<"): "<<path_ctx.outputSchema.getRowType().desc()<<std::endl;

        size_t numToRead = 0;
        for(auto indicator : path_ctx.columnsToRead)
            numToRead += indicator;
        logger.info("specialized code reads: " + pluralize(numToRead, "column"));

        {
            std::stringstream ss;
            // print out normal-case columns and types
            unsigned pos = 0;
            auto col_types = path_ctx.inputSchema.getRowType().isRowType() ? path_ctx.inputSchema.getRowType().get_column_types() : path_ctx.inputSchema.getRowType().parameters();
            for(unsigned i = 0; i < path_ctx.columnsToRead.size(); ++i) {
                if(path_ctx.columnsToRead[i]) {
                    ss<<"column "<<i<<" '"<<path_ctx.columns()[pos]<<"': "<<col_types[pos].desc()<<"\n";
                    pos++;
                }
            }

            logger.info(ss.str());
        }



        ctx.fastPathContext = path_ctx;

        auto generalCaseInputRowType = ctx.slowPathContext.inputSchema.getRowType();
        if(ctx.slowPathContext.inputNode->type() == LogicalOperatorType::FILEINPUT)
            generalCaseInputRowType = ctx.slowPathContext.readSchema.getRowType();

        auto generalCaseOutputRowType = ctx.slowPathContext.outputSchema.getRowType();

        // did specialized stage only yield exceptions or unknown? => then specialization failed.
        if(path_ctx.inputSchema.getRowType().isExceptionType()
            || path_ctx.inputSchema == Schema::UNKNOWN) {
            logger.warn("could not hyperspecialize stage (input type: "
                        + path_ctx.inputSchema.getRowType().desc()
                        + "), skipping code generation.");
            return false;
        }
        if(path_ctx.outputSchema.getRowType().isExceptionType()
           || path_ctx.outputSchema == Schema::UNKNOWN) {
            logger.warn("could not hyperspecialize stage (output type: "
                        + path_ctx.outputSchema.getRowType().desc()
                        + "), skipping code generation.");
            return false;
        }

        // assignment to stage should happen below...

        // generate code! Add to stage, can then compile this. Yay!
        timer.reset();
        TransformStage::StageCodePath fast_code_path;
        try {
            fast_code_path = codegen::StageBuilder::generateFastCodePath(ctx,
                                                                         ctx.fastPathContext,
                                                                         generalCaseInputRowType,
                                                                         ctx.slowPathContext.columnsToRead,
                                                                         generalCaseOutputRowType,
                                                                         ctx.slowPathContext.columns(),
                                                                         planner.normalToGeneralMapping(),
                                                                         stage->number(),
                                                                         conf.exceptionSerializationMode);
        } catch(const std::exception& e) {
            std::stringstream ss;
            ss<<"Exception occurred during fast code generation in hyperspecialization, details: ";
            ss<<e.what();
            logger.error(ss.str());
            return false;
        } catch(...) {
            std::stringstream ss;
            ss<<"Unknown Exception occurred during fast code generation in hyperspecialization.";
            logger.error(ss.str());
            return false;
        }

        // update schemas!
        stage->_fastCodePath = fast_code_path;
        stage->_normalCaseInputSchema = Schema(stage->_normalCaseInputSchema.getMemoryLayout(), path_ctx.inputSchema.getRowType());

        // the output schema (str in tocsv) case is not finalized yet...
        // stage->_normalCaseOutputSchema = Schema(stage->_normalCaseOutputSchema.getMemoryLayout(), path_ctx.outputSchema.getRowType());
        auto after_hyper_output_row_type = ctx.fastPathContext.inputNode->getOutputSchema().getRowType();
        size_t num_input_after_hyper = after_hyper_output_row_type.isRowType() ? after_hyper_output_row_type.get_column_count() : after_hyper_output_row_type.parameters().size();
        logger.info("number of input columns after hyperspecialization: " + std::to_string(num_input_after_hyper));

        logger.info("generated code in " + std::to_string(timer.time()) + "s");
        // can then compile everything, hooray!
        return true;
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

    namespace codegen {

        // recursive helper function
        std::vector<size_t> acc_helper(const std::shared_ptr<LogicalOperator>& op,
                                       const std::shared_ptr<LogicalOperator>& child,
                                       std::vector<size_t> requiredCols,
                                       bool dropOperators) {

            using namespace std;
            auto& logger = Logger::instance().logger("logical");

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

#ifndef NDEBUG
                {
                    std::stringstream ss;
                    ss<<"operator "<<op->name()<<" accesses column indices: "<<accCols<<"\n";
                    if(!op->inputColumns().empty()) {
                        ss<<"\tequal to columns: ";
                        for(unsigned idx : accCols) {
                            if(idx >= op->inputColumns().size())
                                ss<<"INVALID INDEX ";
                            else
                                ss<<op->inputColumns()[idx]<<" ";
                        }
                    }
                    logger.debug(ss.str());
                }
#endif

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

                // special case withColumn operator:
                // if column to create is not overwriting an existing column, can drop it from required cols!
                if(op->type() == LogicalOperatorType::WITHCOLUMN) {
                    auto wop = std::dynamic_pointer_cast<WithColumnOperator>(op);
                    if(wop->creates_new_column()) {
                        requiredCols.erase(std::find(requiredCols.begin(), requiredCols.end(), wop->getColumnIndex()));
                    }
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

            // traverse
            if(op->type() == LogicalOperatorType::JOIN) {

                auto jop = std::dynamic_pointer_cast<JoinOperator>(op); assert(jop);
                vector<size_t> leftRet;
                vector<size_t> rightRet;

                // fetch num cols of operators BEFORE correction
                auto numLeftColumnsBeforePushdown = jop->left()->columns().size();

                if(requiredCols.empty()) {
                    leftRet = acc_helper(jop->left(), jop, requiredCols, dropOperators);
                    rightRet = acc_helper(jop->right(), jop, requiredCols, dropOperators);
                } else {
                    // need to split traversal up
                    set<size_t> reqLeft;
                    set<size_t> reqRight;

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

                    leftRet = acc_helper(jop->left(), jop, requiredLeftCols, dropOperators);
                    rightRet = acc_helper(jop->right(), jop, requiredRightCols, dropOperators);
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

                return colsToKeep;

            }

#ifndef NDEBUG
            {
                std::stringstream ss;
                ss<<"operator "<<op->name()<<" required column indices: "<<requiredCols<<"\n";
                if(!op->inputColumns().empty()) {
                    ss<<"\tequal to columns: ";
                    for(unsigned idx : requiredCols) {
                        if(idx >= op->inputColumns().size())
                            ss<<"INVALID INDEX ";
                        else
                            ss<<op->inputColumns()[idx]<<" ";
                    }
                }
                logger.debug(ss.str());
            }

            logger.debug("----\n time to recurse!\n----");
#endif


            // make sure only one parent
            assert(op->parents().size() <= 1);
            // special case CacheOperator, exec with parent nullptr if child is not null
            auto ret = op->type() == LogicalOperatorType::CACHE && child ?
                       acc_helper(nullptr, op, requiredCols, dropOperators) :
                       acc_helper(op->parent(), op, requiredCols, dropOperators);

#ifndef NDEBUG
            logger.debug("recursion done");
            {
                std::stringstream ss;
                ss<<"operator "<<op->name()<<" returned required column indices: "<<ret<<"\n";
                if(!op->inputColumns().empty()) {
                    ss<<"\tequal to columns: ";
                    for(unsigned idx : ret) {
                        if(idx >= op->inputColumns().size())
                            ss<<"INVALID INDEX ";
                        else
                            ss<<op->inputColumns()[idx]<<" ";
                    }
                }
                logger.debug(ss.str());
            }
#endif

            // CSV operator? do rewrite here!
            // ==> because it's a source node, use requiredCols!
            if(op->type() == LogicalOperatorType::FILEINPUT) {
                // rewrite csv here
                auto csvop = std::dynamic_pointer_cast<FileInputOperator>(op);

#ifndef NDEBUG
                logger.debug("input operator::");
                {
                    std::stringstream ss;
                    ss<<"operator "<<op->name()<<" required output column indices: "<<ret<<"\n";
                    if(!op->columns().empty()) {
                        ss<<"\tequal to columns: ";
                        for(unsigned idx : ret) {
                            if(idx >= op->columns().size())
                                ss<<"INVALID INDEX ";
                            else
                                ss<<op->columns()[idx]<<" ";
                        }
                    }
                    logger.debug(ss.str());
                }
#endif

                assert(csvop);
                auto inputRowType = csvop->getInputSchema().getRowType();
                vector<size_t> colsToSerialize;
                for (auto idx : ret) {
                    //if (idx < inputRowType.parameters().size())
                    //    colsToSerialize.emplace_back(idx);
                    // reverse project
                    auto read_index = csvop->reverseProjectToReadIndex(idx); // output idx -> read index1
                    colsToSerialize.emplace_back(read_index);
                }
                sort(colsToSerialize.begin(), colsToSerialize.end());

                return colsToSerialize;
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

            return ret;
//            // b.c. of some special unrolling etc. could happen that ret is smaller than accCols!
//            // -> make sure all requiredCols are within ret!
//            std::set<size_t> col_set(ret.begin(), ret.end());
//            for(auto col : requiredCols) {
//                col_set.insert(col);
//            }
//            ret = vector<size_t>(col_set.begin(), col_set.end());
//
//            // construct rewrite Map (??) apply ??
//            unordered_map<size_t, size_t> rewriteMap;
//            if(!ret.empty()) {
//                auto max_idx = *max_element(ret.begin(), ret.end()); // limit by max idx available
//                unsigned counter = 0;
//                for (unsigned i = 0; i <= max_idx; ++i) {
//                    if (std::find(ret.begin(), ret.end(), i) != ret.end()) {
//                        rewriteMap[i] = counter++;
//                    }
//                }
//            }
//
//            // following would rewrite, yet this func only delivers the columns required
//            std::sort(ret.begin(), ret.end());
//            return ret;
        }


        std::vector<size_t> StagePlanner::get_accessed_columns() const {

            // start with last operator
            std::vector<std::shared_ptr<LogicalOperator>> ops;
            if(_inputNode)
                ops.push_back(_inputNode);
            for(auto op : _operators)
                ops.push_back(op);
            // reverse!
            std::reverse(ops.begin(), ops.end());
//            // i.e. child -> parent -> grandparent -> ... -> input node
//            for(auto op : ops) {
//                if(hasUDF(op.get())) {
//
//                }
//            }

            auto node = ops.front();
            std::vector<size_t> cols;
            // start with requiring all columns from action node!
            // there's a subtle difference now b.c. output schema for csv was changed to str
            // --> use therefore input schema of the operator!
            auto num_cols = node->getInputSchema().getRowType().parameters().size();
            for(unsigned i = 0; i < num_cols; ++i)
                cols.emplace_back(i);
            return acc_helper(node, nullptr, cols, false); // dropOperators should be true??
        }

        static bool canPromoteFilterToCheck(const std::shared_ptr<FilterOperator>& fop) {
            assert(fop->type() == LogicalOperatorType::FILTER);

            // compiled?
            if(!fop->getUDF().isCompiled())
                return false;

            // can promote if parent is input operator!
            if(fop->parents().empty())
                throw std::runtime_error("filter has no parent, can't check");

            if(fop->parent()->isDataSource())
                return true;

            // can promote if all parents are filters!
            std::shared_ptr<LogicalOperator> node = fop;
            while(node->parent() && !node->parent()->isDataSource()) {
                if(node->type() != LogicalOperatorType::FILTER)
                    return false;
                node = node->parent();
            }
            return true;
        }

        static codegen::NormalCaseCheck filterToCheck(const std::shared_ptr<FilterOperator>& fop) {
            // serialize
            std::string serialized_filter;
#ifdef BUILD_WITH_CEREAL
            std::ostringstream oss(std::stringstream::binary);
            {
                cereal::BinaryOutputArchive ar(oss);
                ar(fop);
                // ar going out of scope flushes everything
            }
            auto bytes_str = oss.str();
            serialized_filter = bytes_str;
#else
            serialized_filter = fop->to_json();
#endif
            auto acc_cols = fop->getUDF().getAccessedColumns();
            return NormalCaseCheck::FilterCheck(acc_cols, serialized_filter);
        }

        void StagePlanner::promoteFilters() {
            auto& logger = Logger::instance().logger("optimizer");

            logger.debug("carrying out potential filter promotion");

            if(!_inputNode) {
                logger.warn("no input node, skip optimization.");
                return;
            }

            size_t original_sample_size = 0;
            if(_inputNode->type() == LogicalOperatorType::FILEINPUT) {
                original_sample_size = std::dynamic_pointer_cast<FileInputOperator>(_inputNode)->storedSampleRowCount();
            }

            if(0 == original_sample_size) {
                logger.debug("skip because empty original sample");
                return;
            }

            std::vector<std::shared_ptr<LogicalOperator>> operators_post_op;

            // check if there is at least one filter operator!
            // -> carry then repeated filters out!
            auto node = _inputNode;
            while(node && !node->children().empty()) {
                assert(node->children().size() == 1); // only single child yet supported...

                logger.debug(node->name() + " has child: " + node->children().front()->name());

                auto next_node = node->children().front();

                // is it a filter? can the filter be promoted?
                if(node->type() == LogicalOperatorType::FILTER) {
                    auto filter_node = std::dynamic_pointer_cast<FilterOperator>(node);
                    // get sample! is it non-empty and smaller than the original sample size?
                    auto samples_post_filter = filter_node->getSample(original_sample_size, true);
                    logger.debug("sample size post-filter: " + pluralize(samples_post_filter.size(), "row"));

                    // @TODD: There's two possibilities here:
                    // [1] if sample is empty, replace pipeline with simple filter node throwing normal-case if filter condition is not met.
                    // [2] sample is empty, ignore filter promot and continue with other majority case. Above is more aggressive, the other one less.

                    if(!samples_post_filter.empty() && samples_post_filter.size() < original_sample_size) {
                        logger.info("filter is candidate for promotion, reduced sample size from " + std::to_string(original_sample_size) + " -> " + std::to_string(samples_post_filter.size()));

                        // check that filter is only dependent on input operator, and not other operator (not yet supported)
                        // if so, then add a new check for the filter. -> if the check passes (and filter=true), stay in normal case
                        // can also remove filter from pipeline, because check is identical with filter, i.e. when check is true also filter will be true!
                        // if check doesn't pass, no problem. Row anyway not processed, skip. no problem.
                        // but set to
                        if(canPromoteFilterToCheck(filter_node) && _inputNode && _inputNode->type() == LogicalOperatorType::FILEINPUT) {

                            // promote ONLY if there's a significant schema change with filter promotion.
                            // else, there's no benefit.
                            // @TODO:
                            logger.debug("add here logic so no accidental promo happens for flights query...");

                            // get additional information about filter:
                            // i.e., rowtype and which columns it accesses
                            std::stringstream ss;
                            ss<<"promoted filter operator, detailed info::\n";
                            ss<<"filter input columns: "<<filter_node->inputColumns()<<"\n";
                            ss<<"filter output columns: "<<filter_node->columns()<<"\n";
                            ss<<"filter input schema: "<<filter_node->getInputSchema().getRowType().desc()<<"\n";

                            // retype filter operator
                            std::vector<std::string> acc_column_names;
                            std::vector<python::Type> acc_col_types;
                            auto acc_cols = filter_node->getUDF().getAccessedColumns(false);
                            auto col_types = filter_node->getInputSchema().getRowType().isRowType() ? filter_node->getInputSchema().getRowType().get_column_types() : filter_node->getInputSchema().getRowType().parameters();
                            for(auto idx : acc_cols) {
                                acc_column_names.push_back(filter_node->inputColumns()[idx]);

                                if(filter_node->getInputSchema().getRowType().isRowType()) {
                                    assert(filter_node->inputColumns()[idx] == filter_node->getInputSchema().getRowType().get_column_names()[idx]);
                                }
                                acc_col_types.push_back(col_types[idx]);
                            }

                            // however, these here should show correct columns/types.
                            ss<<"filter accessed input columns: "<<acc_column_names<<"\n";
                            ss<<"filter accessed, condensed input schema: "<<python::Type::makeTupleType(acc_col_types).desc()<<"\n";
                            logger.debug(ss.str());

                            // remove filter and add check!
                            filter_node->remove();

                            // retype now with input & make smaller
                            RetypeConfiguration conf;
                            conf.columns = _inputNode->columns();
                            conf.row_type = _inputNode->getOutputSchema().getRowType();
                            conf.is_projected = true;
                            auto ret = filter_node->retype(conf);
                            if(!ret) {
                                throw std::runtime_error("did not succeed retyping filter supposed to be promoted to check");
                            }

                            // no need for parents etc.
                            _checks.push_back(filterToCheck(filter_node));

                            // manipulate input node sample!
                            std::dynamic_pointer_cast<FileInputOperator>(_inputNode)->setRowsSample(samples_post_filter);
                            logger.debug("replaced samples in input operator with " + pluralize(_inputNode->getSample(original_sample_size).size(), "filtered sample"));
                            logger.debug("promoted filter to check: \n" + core::withLineNumbers(filter_node->getUDF().getCode()));

                            node = nullptr;
                        }
                    }
                }
                if(node && node->getID() != _inputNode->getID())
                    operators_post_op.push_back(node);

                // go on...
                node = next_node;
            }
            if(node)
                operators_post_op.push_back(node);
            _operators = operators_post_op;
        }

        bool StagePlanner::retypeOperators(const std::vector<Row>& sample,
                                           const std::vector<std::string>& sample_columns) {
            auto& logger = Logger::instance().logger("specializing stage optimizer");

            // @TODO: stats on types for sample. Use this to retype!
            // --> important first step!
            std::unordered_map<std::string, size_t> counts;
            std::unordered_map<python::Type, size_t> t_counts;
            for(const auto& row : sample) {
                counts[row.getRowType().desc()]++;
                t_counts[row.getRowType()]++;
            }
            // for(const auto& keyval : counts) {
            //     std::cout<<keyval.second<<": "<<keyval.first<<std::endl;
            // }

            if(sample.empty()) {
                logger.info("got empty sample, skipping optimization.");
                return true;
            }

            // detect majority type
            // detectMajorityRowType(const std::vector<Row>& rows, double threshold, bool independent_columns)
            auto majType = detectMajorityRowType(sample, _nc_threshold, true, _useNVO);
            python::Type projectedMajType = majType;

            assert(majType.isTupleType());
            assert(projectedMajType.isTupleType());
            size_t num_columns_before_pushdown = majType.parameters().size();
            size_t num_columns_after_pushdown = projectedMajType.parameters().size();

            auto projectedColumns = _inputNode->columns();

#ifndef NDEBUG
            // check how many rows adhere to the normal-case type.
            // (can upcast to normal-case type)
            size_t num_passing = 0;
            std::vector<Row> majRows;
            for(auto row: sample) {
                if(python::canUpcastToRowType(deoptimizedType(row.getRowType()), deoptimizedType(majType))) {
                    num_passing++;
                    majRows.push_back(row);
                }
            }
            std::stringstream sample_stream;
            for(const auto& row: sample) {
                auto row_as_str = row.toJsonString(sample_columns); //row.toPythonString();
                sample_stream<<row_as_str<<"\n";
            }

            std::string sample_dbg_save_path = "extracted_sample.ndjson";
            stringToFile(sample_dbg_save_path, sample_stream.str());

            logger.debug("saved obtained sample to " + sample_dbg_save_path + " as ndjson");
            logger.debug("Of " + pluralize(sample.size(), "sample row") + ", " + std::to_string(num_passing) + " adhere to detected majority type.");
            for(unsigned i = 0; i < std::min(majRows.size(), 5ul); ++i)
                std::cout<<majRows[i].toPythonString()<<std::endl;

            // check if any forkevents are found in the sample
            std::vector<std::string> rows_as_python_strings;
            size_t fork_events_found = 0;
            for(auto row : sample) {
                rows_as_python_strings.push_back(row.toPythonString());
                if(rows_as_python_strings.back().find("ForkEvent") != std::string::npos)
                    fork_events_found++;
            }
            std::cout<<"Found forkevents: "<<fork_events_found<<"x"<<std::endl;
#endif


            // the detected majority type here is BEFORE projection pushdown.
            // --> therefore restrict it to the type of the input operator.
            // std::cout<<"Majority detected row type is: "<<projectedMajType.desc()<<std::endl;

            // if majType of sample is different from input node type input sample -> retype!
            // also need to restrict type first!
            logger.debug("performing Retyping");
            auto optimized_operators = retypeUsingOptimizedInputSchema(projectedMajType, projectedColumns);

            // overwrite internal operators to apply subsequent optimizations
            _inputNode = _inputNode ? optimized_operators.front() : nullptr;
            _operators = _inputNode ? std::vector<std::shared_ptr<LogicalOperator>>{optimized_operators.begin() + 1,
                                                                          optimized_operators.end()}
                                    : optimized_operators;


            // run validation after forcing majority sample based type
            auto validation_rc = validatePipeline();
            logger.debug(std::string("post-specialization pipeline validation: ") + (validation_rc ? "ok" : "failed"));
            assert(validation_rc);
            return validation_rc;
        }
    }
}