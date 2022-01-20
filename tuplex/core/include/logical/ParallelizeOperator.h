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

        std::vector<Partition*> _partitions; // data, conforming to majority type
        std::vector<Partition*> _pythonObjects; // schema violations stored for interpreter processing as python objects
        // maps partitions to their corresponding python objects
        std::unordered_map<std::string, ExceptionInfo> _inputPartitionToPythonObjectsMap;
        std::vector<std::string> _columnNames;

        std::vector<Row> _sample; // sample, not necessary conforming to one type

        void initSample(); // helper to fill sample with a certain amount of rows.
    public:
        LogicalOperator *clone() override;

        // this a root node
        ParallelizeOperator(const Schema& schema,
                            std::vector<Partition*> partitions,
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
        std::vector<tuplex::Partition*> getPartitions();

        void setPythonObjects(const std::vector<Partition *>& pythonObjects) { _pythonObjects = pythonObjects; }
        std::vector<Partition *> getPythonObjects() { return _pythonObjects; }

        void setInputPartitionToPythonObjectsMap(const std::unordered_map<std::string, ExceptionInfo>& inputPartitionToPythonObjectsMap) { _inputPartitionToPythonObjectsMap = inputPartitionToPythonObjectsMap; }
        std::unordered_map<std::string, ExceptionInfo> getInputPartitionToPythonObjectsMap() { return _inputPartitionToPythonObjectsMap; }

        Schema getInputSchema() const override { return getOutputSchema(); }

        bool good() const override;

        std::vector<std::string> columns() const override { return _columnNames; }

        std::vector<std::string> inputColumns() const override { return _columnNames; }

        int64_t cost() const override;
    };
}
#endif //TUPLEX_PARALLELIZEOPERATOR_H