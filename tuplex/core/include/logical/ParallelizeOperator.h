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

        // TODO: how to do partitions?
        std::vector<Partition*> _partitions; // data, conforming to majority type
        std::vector<Partition*> _pythonObjects; // schema violations stored for interpreter processing as python objects
        // maps partitions to their corresponding python objects
        std::unordered_map<std::string, ExceptionInfo> _inputPartitionToPythonObjectsMap;
        std::vector<std::string> _columnNames;

        std::vector<Row> _sample; // sample, not necessary conforming to one type

        void initSample(); // helper to fill sample with a certain amount of rows.
    public:
        std::shared_ptr<LogicalOperator> clone() override;

        // required by cereal
        ParallelizeOperator() = default;

        // this a root node
        ParallelizeOperator(const Schema& schema,
                            const std::vector<Partition*>& partitions,
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

        void setPythonObjects(const std::vector<Partition*> &pythonObjects) { _pythonObjects = pythonObjects; }
        std::vector<Partition *> getPythonObjects() { return _pythonObjects; }

        void setInputPartitionToPythonObjectsMap(const std::unordered_map<std::string, ExceptionInfo>& pythonObjectsMap) { _inputPartitionToPythonObjectsMap = pythonObjectsMap; }
        std::unordered_map<std::string, ExceptionInfo> getInputPartitionToPythonObjectsMap() { return _inputPartitionToPythonObjectsMap; }

        Schema getInputSchema() const override { return getOutputSchema(); }

        bool good() const override;

        std::vector<std::string> columns() const override { return _columnNames; }

        std::vector<std::string> inputColumns() const override { return _columnNames; }

        int64_t cost() const override;

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            // DO NOT INCLUDE sample here.
            ar(::cereal::base_class<LogicalOperator>(this), _columnNames);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<LogicalOperator>(this), _columnNames);
        }
#endif
    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::ParallelizeOperator);
#endif

#endif //TUPLEX_PARALLELIZEOPERATOR_H