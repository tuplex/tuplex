//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_JOINOPERATOR_H
#define TUPLEX_JOINOPERATOR_H

#include "LogicalOperator.h"
#include "LogicalOperatorType.h"
#include "ExceptionOperator.h"

namespace tuplex {


    enum class JoinType {
        INNER,
        LEFT,
        RIGHT
    };

    class JoinOperator : public LogicalOperator {
    public:
        // required by cereal
        JoinOperator() = default;

        JoinOperator(const std::shared_ptr<LogicalOperator> &left,
                           const std::shared_ptr<LogicalOperator> &right,
                           option<std::string> leftColumn,
                           option<std::string> rightColumn, const JoinType& jt,
                           const std::string& leftPrefix, const std::string& leftSuffix,
                           const std::string& rightPrefix, const std::string& rightSuffix);

        virtual ~JoinOperator() {}

        virtual std::string name() const override { return "join"; }

        bool columnBasedJoin() const {
            assert(!((_leftColumn.has_value() && !_rightColumn.has_value()) ||
            (!_leftColumn.has_value() && _rightColumn.has_value())));

            return _leftColumn.has_value() && _rightColumn.has_value();
        }

        bool good() const override;

        std::vector<Row> getSample(size_t num) const override;

        bool isActionable() override;

        bool isDataSource() override;

        std::shared_ptr<LogicalOperator> clone(bool cloneParents) const override;

        Schema getInputSchema() const override;

        std::vector<std::string> columns() const override { return _columns; }

        // whether to build left or right (build on smaller relation)
        bool buildRight() const {

#warning "Query optimizer bug here: force build on right side for left join"
            if(joinType() == JoinType::LEFT)
                return true;

            return left()->cost() >= right()->cost();
        }

        // overwrite cost (should be estimated better, for now simply multiply)

    private:
        option<std::string> _leftColumn;  // column within left dataset
        option<std::string> _rightColumn;
        JoinType _joinType;

        std::string _leftPrefix;
        std::string _leftSuffix;
        std::string _rightPrefix;
        std::string _rightSuffix;
    public:
        LogicalOperatorType type() const override;

        std::shared_ptr<LogicalOperator> left() const  { assert(parents().size() == 2); return parents().front(); }
        std::shared_ptr<LogicalOperator> right() const { assert(parents().size() == 2); return parents()[1]; }

        int64_t leftKeyIndex() const;
        int64_t rightKeyIndex() const;
        /*!
         * where is the key stored in the final output?
         * @return index of the join key in the final schema
         */
        int64_t outputKeyIndex() const;

        option<std::string> leftColumn() const { return _leftColumn; }  // column within left dataset
        option<std::string> rightColumn() const { return _rightColumn; }
        JoinType joinType() const { return _joinType; }
        std::string leftPrefix () const { return _leftPrefix; }
        std::string leftSuffix() const { return _leftSuffix; }
        std::string rightPrefix() const { return _rightPrefix; }
        std::string rightSuffix() const { return _rightSuffix; }


        /*!
         * return columns in the bucket if a hash join is used.
         * @return vector of columns within bucket (key col excluded!)
         */
        std::vector<std::string> bucketColumns() const {
            std::vector<std::string> cols;
            if(buildRight()) {
                for(int i = 0; i < right()->columns().size(); ++i) {
                    if(i != rightKeyIndex())
                        cols.emplace_back(right()->columns()[i]);
                }
            } else {
                for(int i = 0; i < left()->columns().size(); ++i) {
                    if(i != leftKeyIndex())
                        cols.emplace_back(left()->columns()[i]);
                }
            }
            return cols;
        }

        /*!
         * return python Type for operator
         * @return
         */
        python::Type keyType() const {
           auto rk = right()->getOutputSchema().getRowType().parameters().at(rightKeyIndex());
           auto lk = left()->getOutputSchema().getRowType().parameters().at(leftKeyIndex());
           if(rk == lk)
              return rk;
           if(canUpcastType(rk, lk))
               return lk;
           if(canUpcastType(lk, rk))
               return rk;
           throw std::runtime_error("incomaptible key types " + rk.desc() +
           " [right] and " + lk.desc() + " [left] found.");

        }

        /*!
         * return bucket Type (depending where join is built) if hash was used
         * @return
         */
        python::Type bucketType() const {
            // fetch columns from schema
            std::vector<python::Type> types;
            auto rt = right()->getOutputSchema().getRowType().parameters();
            auto lt = left()->getOutputSchema().getRowType().parameters();

            if(buildRight()) {
                for(int i = 0; i < rt.size(); ++i) {
                    if(i != rightKeyIndex())
                        types.emplace_back(rt[i]);
                }
            } else {
                for(int i = 0; i < lt.size(); ++i) {
                    if(i != leftKeyIndex())
                        types.emplace_back(lt[i]);
                }
            }
            return python::Type::makeTupleType(types);
        }


        /*!
         * restrict join on columns, i.e. use a rewrite map for that
         * @param rewriteMap
         */
        virtual void projectionPushdown();

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<LogicalOperator>(this), _leftColumn, _rightColumn, _joinType, _leftPrefix, _leftSuffix, _rightPrefix, _rightSuffix);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<LogicalOperator>(this), _leftColumn, _rightColumn, _joinType, _leftPrefix, _leftSuffix, _rightPrefix, _rightSuffix);
        }
#endif

    private:
        // column within right dataset

        // join mode is inner join for now only

        std::vector<std::string> _columns;

        void inferSchema();
    };


    inline python::Type combinedJoinType(const python::Type& leftType,
                                         int leftKeyIndex,
                                         const python::Type& rightType,
                                         int rightKeyIndex,
                                         JoinType joinType)  {

        // combined schema from row type
        std::vector<python::Type> combinedTypes;
        for(int i = 0; i < leftType.parameters().size(); ++i) {
            auto t = leftType.parameters()[i];
            if(i != leftKeyIndex)
                combinedTypes.push_back(t);
        }

        // fetch more restrictive type b.c. it's an inner join...
        auto leftKeyType = leftType.parameters()[leftKeyIndex];
        auto rightKeyType = rightType.parameters()[rightKeyIndex];

        // if one is option type and the other is not, take
        switch(joinType) {
            case JoinType::LEFT: {
                // always the left result
                combinedTypes.push_back(leftKeyType);
                break;
            }
            case JoinType::RIGHT: {
                // always the right result
                combinedTypes.push_back(rightKeyType);
                break;
            }
            case JoinType::INNER: {
                // more interesting case:
                // same type => doesn't matter
                if(leftKeyType == rightKeyType)
                    combinedTypes.push_back(rightKeyType);
                else {
                    // there are a couple cases. Some should be handled separately, i.e. those resulting in
                    // empty datasets!
                    if(leftKeyType == python::Type::NULLVALUE && !rightKeyType.isOptionType())
                        throw std::runtime_error("empty datset, should be handled somewhere up the chain!");
                    else if(!leftKeyType.isOptionType() && rightKeyType == python::Type::NULLVALUE)
                        throw std::runtime_error("empty datset, should be handled somewhere up the chain!");
                    else if(leftKeyType == python::Type::NULLVALUE && rightKeyType.isOptionType())
                        combinedTypes.push_back(python::Type::NULLVALUE);
                    else if(leftKeyType.isOptionType() && rightKeyType == python::Type::NULLVALUE)
                        combinedTypes.push_back(python::Type::NULLVALUE);
                    else if(leftKeyType.isOptionType() && !rightKeyType.isOptionType())
                        combinedTypes.push_back(rightKeyType);
                    else if(!leftKeyType.isOptionType() && rightKeyType.isOptionType())
                        combinedTypes.push_back(leftKeyType);
                    else throw std::runtime_error("unknown combination encountered");
                }
                break;
            }
        }



        for(int i = 0; i < rightType.parameters().size(); ++i) {
            auto t = rightType.parameters()[i];
            if(i != rightKeyIndex) {
                // important to make option type (nullable in left join)
                switch(joinType) {
                    case JoinType::LEFT: {
                        combinedTypes.push_back(python::Type::makeOptionType(t));
                        break;
                    }
                    case JoinType::INNER: {
                        combinedTypes.push_back(t);
                        break;
                    }
                    default: {
                        throw std::runtime_error("join type not implemented");
                    }
                }
            }
        }
        return python::Type::makeTupleType(combinedTypes);
    }
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::JoinOperator);
#endif

#endif //TUPLEX_JOINOPERATOR_H