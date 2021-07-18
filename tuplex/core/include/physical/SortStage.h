//
// Created by Nathaneal Pitt on 6/2/21.
//

#ifndef TUPLEX_SORTSTAGE_H
#define TUPLEX_SORTSTAGE_H

#include "PhysicalStage.h"

namespace tuplex {
    // forward declaration of friend class
    namespace codegen {
        class StageBuilder;
    }

    /*!
     * describes a stage which sorts data.
     * execute as narrow stage with per-node/per-thread parallelism.
     */
    class SortStage : public PhysicalStage {
    public:
        /*!
 * creates a new SortStage with generated code
 * @param plan to which this stage belongs to
 * @param backend backend where to execute code
 * @param number number of the stage
 * @param allowUndefinedBehavior whether to allow unsafe operations for speedups, e.g. division by zero
 */
        SortStage(PhysicalPlan* plan,
                  IBackend* backend,
                  int64_t number,
                  bool allowUndefinedBehavior, std::vector<size_t> order, std::vector<size_t> orderEnum);

        ~SortStage() override = default;

        friend class ::tuplex::codegen::StageBuilder;

        void execute(const Context& context) override;

        void setResultSet(const std::shared_ptr<ResultSet> &rs) { _rs = rs; }

        std::vector<Partition*> inputPartitions() const { return _inputPartitions; }

        virtual EndPointMode outputMode() const override { return EndPointMode::MEMORY; }
        virtual EndPointMode inputMode() const override { return EndPointMode::MEMORY; }

        /*!
         * fetch data into resultset
         * @return resultset of this stage
         */
        std::shared_ptr<ResultSet> resultSet() const override { return _rs;}
        std::vector<size_t> order() { return _order;};
        std::vector<size_t> orderEnum() { return _orderEnum;};
    private:

        std::vector<size_t> _order;
        std::vector<size_t> _orderEnum;
        std::vector<Partition*> _inputPartitions; //! memory input partitions for this task.
        std::shared_ptr<ResultSet> _rs; //! result set
    };
}
#endif //TUPLEX_SORTSTAGE_H