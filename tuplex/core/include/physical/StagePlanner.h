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

#include <physical/TransformStage.h>
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

        class StagePlanner {
        public:
            StagePlanner(LogicalOperator* inputNode, const std::vector<LogicalOperator*>& operators) : _inputNode(inputNode), _operators(operators), _useNVO(false), _useConstantFolding(false), _useDelayedParsing(false) {
                assert(inputNode);
                for(auto op : operators)
                    assert(op);
                enableAll();
            }

            /*!
             * create optimized, specialized pipeline
             */
            std::vector<LogicalOperator*> optimize();

            void enableAll() {
                enableNullValueOptimization();
                enableConstantFoldingOptimization();
                enableDelayedParsingOptimization();
            }

            void enableNullValueOptimization() { _useNVO = true; }
            void enableConstantFoldingOptimization() { _useConstantFolding = true; }
            void enableDelayedParsingOptimization() { _useDelayedParsing = true; }

        private:
            LogicalOperator* _inputNode;
            std::vector<LogicalOperator*> _operators;

            bool _useNVO;
            bool _useConstantFolding;
            bool _useDelayedParsing;

            // helper functions
            std::vector<Row> fetchInputSample();

            std::vector<LogicalOperator*> nulLValueOptimization();
        };
    }
}

#endif