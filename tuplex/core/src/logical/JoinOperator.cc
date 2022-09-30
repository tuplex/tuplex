//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/JoinOperator.h>
#include <algorithm>
#include <CSVUtils.h>

namespace tuplex {

    JoinOperator::JoinOperator(const std::shared_ptr<LogicalOperator>& left, const std::shared_ptr<LogicalOperator>& right,
                               tuplex::option<std::string> leftColumn, tuplex::option<std::string> rightColumn,
                               const tuplex::JoinType &jt, const std::string &leftPrefix, const std::string &leftSuffix,
                               const std::string &rightPrefix, const std::string &rightSuffix) : LogicalOperator(
            {left, right}),
                                                                                                 _leftColumn(
                                                                                                         leftColumn),
                                                                                                 _rightColumn(
                                                                                                         rightColumn),
                                                                                                 _joinType(jt),
                                                                                                 _leftPrefix(
                                                                                                         leftPrefix),
                                                                                                 _leftSuffix(
                                                                                                         leftSuffix),
                                                                                                 _rightPrefix(
                                                                                                         rightPrefix),
                                                                                                 _rightSuffix(
                                                                                                         rightSuffix) {

        // inner join:
        // schema is to be combined using columns etc.
        inferSchema();
    }

    int64_t JoinOperator::leftKeyIndex() const {
        // column based?
        if (columnBasedJoin()) {
            // search left column in columns. -1 if invalid!
            assert(left());
            return indexInVector(_leftColumn.value(), left()->columns());
        } else {
            // join is based on (K, V), (K, W) so just give back zero
            return 0;
        }
    }

    int64_t JoinOperator::rightKeyIndex() const {
        // column based?
        if (columnBasedJoin()) {
            // search left column in columns. -1 if invalid!
            assert(right());
            return indexInVector(_rightColumn.value(), right()->columns());
        } else {
            // join is based on (K, V), (K, W) so just give back zero
            return 0;
        }
    }

    int64_t JoinOperator::outputKeyIndex() const {
        // easy, simply last idx of left columns
        if(columnBasedJoin()) {
            assert(left() && right());
            return left()->columns().size() - 1; // -1 for index
        } else throw std::runtime_error("not yet supported!");
    }

    void JoinOperator::inferSchema() {
        using namespace std;

        // two modes:
        // either column based OR (K, V), (K, W) based
        if ((_leftColumn.has_value() && !_rightColumn.has_value()) ||
            (!_leftColumn.has_value() && _rightColumn.has_value()))
            throw std::runtime_error("join mode is either column name based or tuple based with types (K, V), (K, W).");

        if (columnBasedJoin()) {
            // column based => get key type of column!

            // important to get here the columns of the result dataset! // ==> no, projection pushdown???
            //            auto leftColumns = left()->getDataSet()->columns();
            //            auto rightColumns = right()->getDataSet()->columns();

            auto leftColumns = left()->columns();
            auto rightColumns = right()->columns();


            auto leftIndex = indexInVector(_leftColumn.value(), leftColumns);
            auto rightIndex = indexInVector(_rightColumn.value(), rightColumns);

            if (leftIndex < 0)
                throw std::runtime_error("column '" + _leftColumn.value() + "' not found in left dataset for join.");
            if (rightIndex < 0)
                throw std::runtime_error("column '" + _rightColumn.value() + "' not found in right dataset for join.");

            // indexing asserts...
            assert(leftIndex >= 0 && rightIndex >= 0);
            assert(leftIndex < left()->getOutputSchema().getRowType().parameters().size());
            assert(rightIndex < right()->getOutputSchema().getRowType().parameters().size());
            assert(leftColumns.size() == left()->getOutputSchema().getRowType().parameters().size());
            assert(rightColumns.size() == right()->getOutputSchema().getRowType().parameters().size());

            auto leftType = left()->getOutputSchema().getRowType().parameters()[leftIndex];
            auto rightType = right()->getOutputSchema().getRowType().parameters()[rightIndex];

            // make sure key types are the same, else abort
            // @TODO: could replace logically with empty result set b.c. python objects
            if (!((leftType == rightType) ||
                  (leftType.isOptionType() && leftType.getReturnType() == rightType) ||
                  (rightType.isOptionType() && rightType.getReturnType() == leftType) ||
                  (leftType.isOptionType() && rightType == python::Type::NULLVALUE) ||
                  (rightType.isOptionType() && leftType == python::Type::NULLVALUE))) {
                throw std::runtime_error(
                        "can't perform join, left column '" + _leftColumn.value() + "'type " + leftType.desc()
                        + " is not the same as right column '" + _rightColumn.value() + "'type " + rightType.desc());
            }

            // same types, hence extract values
            // what order of columns?
            // ==> first all the left columns EXCEPT the join column, then the join column,
            //     then the right columns EXCEPT the join column?
            //     what about the column name?
            // @TODO: different options here...


            // Check whether there are conflicting column names. If so, issue warning!
            // sanity check, see whether column names exist multiple times!
            if(leftPrefix().empty() && rightPrefix().empty() && leftSuffix().empty() && rightSuffix().empty()) {
                vector<string> overlappingColumns;
                set_intersection(leftColumns.begin(), leftColumns.end(), rightColumns.begin(), rightColumns.end(),
                                 std::back_inserter(overlappingColumns));
                sort(overlappingColumns.begin(), overlappingColumns.end());

                if ((overlappingColumns.size() == 1 &&
                     (_leftColumn.value() != _rightColumn.value() || overlappingColumns.front() != _leftColumn.value())) ||
                    overlappingColumns.size() > 1) {
                    stringstream ss;
                    ss << "Found columns " << csvToHeader(overlappingColumns)
                       << " in both left and right dataset for join operation, consider prefixing or suffixing them, because when indexing with names the first matching column name will be used.";
                    Logger::instance().defaultLogger().warn(ss.str());
                }
            }


            // @TODO: probably need at some point a rename function...
            // ==> this can be a simple map...
            // Note: need to update this in WebUI! => i.e. selectColumns, rename etc. can be all realized using a simple map.
            // why reinvent the wheel?


            // @TODO: use here combinedTypes

            auto leftTypes = left()->getOutputSchema().getRowType().parameters();
            auto rightTypes = right()->getOutputSchema().getRowType().parameters();

            // Note: if a left or right join is involved, propagate types to Nullables!
            // construct columns
            // general type of join is
            // | ... all cols from left side... | keycol | ... all cols from right side ... |
            vector<string> columns;
            int joinColIdx = 0;
            for (int i = 0; i < leftColumns.size(); ++i) {
                if (_leftColumn.value() != leftColumns[i]) {
                    columns.push_back(_leftPrefix + leftColumns[i] + _leftSuffix);
                } else
                    joinColIdx = i;
            }

            // the join column (reuse name from left!)
            // ==> it never gets nulled!
            // @TODO: add alias...
            columns.push_back(_leftPrefix + leftColumns[joinColIdx] + _leftSuffix);

            for (int i = 0; i < rightColumns.size(); ++i) {
                if (_rightColumn.value() != rightColumns[i]) {
                    columns.push_back(_rightPrefix + rightColumns[i] + _rightSuffix);
                }
            }

            // combine types
            auto combinedRowType = combinedJoinType(left()->getOutputSchema().getRowType(),
                    leftKeyIndex(),
                    right()->getOutputSchema().getRowType(),
                    rightKeyIndex(),
                    joinType());

            // create schema
            setSchema(Schema(Schema::MemoryLayout::ROW, combinedRowType));
            _columns = columns;

        } else {
            // tuple based
            // easier: nothing to worry about.
            throw std::runtime_error("not implemented yet");
        }
    }

    LogicalOperatorType JoinOperator::type() const {
        return LogicalOperatorType::JOIN;
    }

    bool JoinOperator::good() const {
        return true;
    }

    std::vector<Row> JoinOperator::getSample(size_t num) const {
        // @TODO: fix this later!!!
        Logger::instance().defaultLogger().warn("getSample for join not yet implemented, returning empty vector");

        // @TODO: better handling of C++ exceptions with C-extension.
        //throw std::runtime_error("getSample for join not yet implemented");

        // for now, empty sample...
        return std::vector<Row>();
    }

    bool JoinOperator::isActionable() {
        return false;
    }

    bool JoinOperator::isDataSource() {
        return false;
    }

    Schema JoinOperator::getInputSchema() const {
        throw std::runtime_error("input schema makes no sense for join operator, because there are two input schemas!");
        return Schema();
    }

    void JoinOperator::projectionPushdown() {

        // need to rewrite keys etc. here...
        inferSchema();
    }

    std::shared_ptr<LogicalOperator> JoinOperator::clone(bool cloneParents) {
        auto copy = new JoinOperator(cloneParents ? left()->clone() : nullptr, cloneParents ? right()->clone() : nullptr,
                _leftColumn, _rightColumn, _joinType, _leftPrefix, _leftSuffix, _rightPrefix, _rightSuffix);
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }
}