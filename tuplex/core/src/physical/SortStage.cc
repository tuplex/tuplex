//
// Created by Colby Anderson on 6/2/21.
//

#include <physical/SortStage.h>

namespace tuplex {

    SortStage::SortStage(PhysicalPlan *plan, IBackend *backend,
                         int64_t number,
                         bool allowUndefinedBehavior,
                         std::vector<size_t> order, std::vector<size_t> orderEnum) : PhysicalStage::PhysicalStage(plan, backend, number),
                                                                                     _order(order), _orderEnum(orderEnum) {}

    void SortStage::execute(const Context &context) {
        using namespace std;

        // log
        stringstream ss;
        ss<<"Stage"<<this->number()<<" depends on: ";
        for(auto stage: predecessors())
            ss<<"Stage"<<stage->number()<<" ";
        Logger::instance().defaultLogger().info(ss.str());

        // execute all predecessors (can be at most one!)
        // @TODO: this should be parallelized for tiny stages!
        for (auto stage : predecessors())
            stage->execute(context);

        //        auto stage = predecessors()[0];
        //        vector<Partition*> p = stage->resultSet()->partitions();
        auto x = this;
        // execute stage via backend
        backend()->execute(x);
    }
}
