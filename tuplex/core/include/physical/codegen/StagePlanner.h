//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_STAGEPLANNER_H
#define TUPLEX_STAGEPLANNER_H

#include <physical/execution/TransformStage.h>
#include <logical/LogicalOperator.h>

// this here is the class to create a specialized version of a stage.
namespace tuplex {
    namespace codegen {

        struct DetectionStats {
            size_t num_rows;
            size_t num_columns_min;
            size_t num_columns_max;
            std::vector<bool> is_column_constant;
            Row constant_row;

            DetectionStats() : num_rows(0),
                               num_columns_min(std::numeric_limits<size_t>::max()),
                               num_columns_max(std::numeric_limits<size_t>::min()) {}
            std::vector<size_t> constant_column_indices() const {
                std::vector<size_t> v;
                for(unsigned i = 0; i < is_column_constant.size(); ++i) {
                    if(is_column_constant[i])
                        v.push_back(i);
                }
                return v;
            }

            inline python::Type specialize_row_type(const python::Type& row_type) const {
                assert(row_type.isTupleType());
                assert(row_type.parameters().size() == constant_row.getNumColumns());

                std::vector<python::Type> colTypes = row_type.parameters();

                // fill in constant valued types
                for(auto idx : constant_column_indices()) {
                    auto underlying_type = constant_row.getType(idx);
                    auto underlying_constant = constant_row.get(idx).desc();
                    auto constant_type = python::Type::makeConstantValuedType(underlying_type, underlying_constant);
                    colTypes[idx] = constant_type;
                }
                return python::Type::makeTupleType(colTypes);
            }

            void detect(const std::vector<Row>& rows) {
                if(rows.empty())
                    return;

                // init?
                if(0 == num_rows) {
                    constant_row = rows.front();
                    // mark everything as constant!
                    is_column_constant = std::vector<bool>(constant_row.getNumColumns(), true);
                }
                size_t row_number = 0;
                for(const auto& row : rows) {

                    // ignore small rows
                    if(row.getNumColumns() < constant_row.getNumColumns())
                        continue;

                    // compare current row with constant row.
                    for(unsigned i = 0; i < std::min(constant_row.getNumColumns(), row.getNumColumns()); ++i) {
                        // field comparisons might be expensive, so compare only if not marked yet as false...
                        // field different? replace!
                        if(is_column_constant[i] && constant_row.get(i).withoutOption() != row.get(i).withoutOption()) {
                            // allow option types to be constant!
                            if(constant_row.get(i).isNull() && !row.get(i).isNull()) {
                                // saved row value is null -> replace with constant!
                                constant_row.set(i, row.get(i).makeOptional());
                            } else if(row.get(i).isNull()) {
                                // update to make optional to indicate null
                                if(!constant_row.get(i).getType().isOptionType()) {
                                    constant_row.set(i, constant_row.get(i).makeOptional());
                                }
                            } else  {
                                is_column_constant[i] = false;
                            }
                        }
                    }
                    row_number++;

                    // cur row larger? replace!

                    num_columns_min = std::min(num_columns_min, row.getNumColumns());
                    num_columns_max = std::max(num_columns_max, row.getNumColumns());
                }

                num_rows += rows.size();
            }
        };

        /*!
         * this class creates a specialized version of a stage
         */
        class StagePlanner {
        public:

            /*!
             * constructor, taking nodes of a stage in
             * @param inputNode the input node of the stage (parent of first operator)
             * @param operators operators following the input node.
             */
            StagePlanner(const std::shared_ptr<LogicalOperator>& inputNode,
                         const std::vector<std::shared_ptr<LogicalOperator>>& operators) : _inputNode(inputNode),
                         _operators(operators), _useNVO(false), _useConstantFolding(false), _useDelayedParsing(false) {
                assert(inputNode);
                for(auto op : operators)
                    assert(op);
                enableAll();
            }

            /*!
             * create optimized, specialized pipeline, i.e. first operator returned is the input operator.
             * The others are (possibly rearranged) operators. Operators are clones/copies of original operators. I.e.
             * planner is non-destructive.
             */
            void optimize();

            std::vector<std::shared_ptr<LogicalOperator>> optimized_operators() const {
                return _operators;
            }

            std::shared_ptr<LogicalOperator> input_node() const {
                return _inputNode;
            }

            /*!
             * shortcut to enable all optimizations
             */
            void enableAll() {
                enableNullValueOptimization();
                enableConstantFoldingOptimization();
                enableDelayedParsingOptimization();
            }

            void enableNullValueOptimization() { _useNVO = true; }
            void enableConstantFoldingOptimization() { _useConstantFolding = true; }
            void enableDelayedParsingOptimization() { _useDelayedParsing = true; }

            std::map<int, int> normalToGeneralMapping() const { return _normalToGeneralMapping; }

        private:
            std::shared_ptr<LogicalOperator> _inputNode;
            std::vector<std::shared_ptr<LogicalOperator>> _operators;

            bool _useNVO;
            bool _useConstantFolding;
            bool _useDelayedParsing;

            // helper when normal-case is specialized to yield less rows than general case
            std::map<int, int> _normalToGeneralMapping;

            // helper functions
            std::vector<Row> fetchInputSample();

            /*!
             * perform null-value optimization & return full pipeline. First op is inputnode
             * @return vector of operators
             */
            std::vector<std::shared_ptr<LogicalOperator>> nullValueOptimization();

            /*!
             * perform constant folding optimization using sample
             */
            std::vector<std::shared_ptr<LogicalOperator>> constantFoldingOptimization(const std::vector<Row>& sample);


            python::Type get_specialized_row_type(const std::shared_ptr<LogicalOperator>& inputNode, const DetectionStats& ds) const;

            /*!
             * perform filter-reordering using sample selectivity
             */
            std::vector<std::shared_ptr<LogicalOperator>> filterReordering(const std::vector<Row>& sample);
        };
    }

    // HACK!
    extern void hyperspecialize(TransformStage *stage, const URI& uri, size_t file_size);
}

#endif