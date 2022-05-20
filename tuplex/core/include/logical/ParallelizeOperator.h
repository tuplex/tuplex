//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PARALLELIZEOPERATOR_H
#define TUPLEX_PARALLELIZEOPERATOR_H


#include "LogicalOperator.h"

namespace tuplex {
    class ParallelizeOperator : public LogicalOperator {

        std::vector<Partition*> _normalPartitions; // data, conforming to majority type
        std::vector<Partition*> _fallbackPartitions; // schema violations stored for interpreter processing as python objects
        std::vector<PartitionGroup> _partitionGroups; // maps normal partitions to their corresponding fallback partitions
        std::vector<std::string> _columnNames;

        std::vector<Row> _sample; // sample, not necessary conforming to one type

        void initSample(); // helper to fill sample with a certain amount of rows.
    public:
        LogicalOperator *clone() override;

        // this a root node
        ParallelizeOperator(const Schema& schema,
                            const std::vector<Partition*>& normalPartitions,
                            const std::vector<std::string>& columns);

        std::string name() override { return "parallelize"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::PARALLELIZE; }

        bool isActionable() override { return false; }

        bool isDataSource() override { return true; }

        std::vector<Row> getSample(const size_t num) const override;

        /*!
         * get the partitions where the parallelized data is stored.
         * @return vector of partitions.
         */
        std::vector<tuplex::Partition*> getNormalPartitions();

        void setFallbackPartitions(const std::vector<Partition*> &fallbackPartitions) { _fallbackPartitions = fallbackPartitions; }
        std::vector<Partition *> getFallbackPartitions() { return _fallbackPartitions; }

        void setPartitionGroups(const std::vector<PartitionGroup>& partitionGroups) { _partitionGroups = partitionGroups; }
        std::vector<PartitionGroup> getPartitionGroups() { return _partitionGroups; }

        Schema getInputSchema() const override { return getOutputSchema(); }

        bool good() const override;

        std::vector<std::string> columns() const override { return _columnNames; }

        std::vector<std::string> inputColumns() const override { return _columnNames; }

        int64_t cost() const override;
    };
}
#endif //TUPLEX_PARALLELIZEOPERATOR_H