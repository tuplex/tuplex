//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LOGICALPLAN_H
#define TUPLEX_LOGICALPLAN_H

#include "../physical/PhysicalPlan.h"
#include "LogicalOperator.h"

// uncomment to trace logical plan optimization
// #define TRACE_LOGICAL_OPTIMIZATION

namespace tuplex {

    class LogicalPlan;
    class PhysicalPlan;
    class PhysicalStage;

    class LogicalPlan {
    private:
        // the action which is called
        LogicalOperator* _action;


        void optimizeFilters();
        void emitPartialFilters();
        void reorderDataProcessingOperators();
        void incrementalResolution(const Context &context);
        void updateIDs(LogicalOperator *previous);

    public:

        LogicalPlan() = delete;

        /*!
         * creates a logical plan from an action. I.e. traces path back to already cached resources,
         * performs optimization, ... Internally, this creates a deep copy of the operator tree.
         * @param action an operation to execute itself and all operations
         * before in the DAG that form a critical path.
         */
        LogicalPlan(LogicalOperator *action);

        /*!
         * destroys internal copy of operator tree
         */
        ~LogicalPlan();

        /*!
         * creates a deep copy of the logical plan
         * @return
         */
        LogicalPlan* clone();

         /*!
          * Optimize physical plan, i.e. reorder operators & perform selection push-down.
          * @param Context for which to optimize, i.e. use options of this context
          * @param inPlace if true, then optimizations are performed in place. Note: This means UDFs & Co might change!
          * @return optimized logical plan.
          */
         LogicalPlan* optimize(const Context& context, bool inPlace=true);

        /*!
         * creates a physical plan which is then used for executing multiple Stages via a TaskQueue.
         * Performs optimization of LogicalPlan
         * to create an optimized physical plan (for the cluster)
         * Compiles python UDFs for code generation
         * @param context Context for which to create a physical plan
         * @return pointer of newly allocated PhysicalPlan object. Caller of this function needs to manage its memory
         */
        PhysicalPlan* createPhysicalPlan(const Context& context);

        LogicalOperator* getAction() const { return _action; }

        /*!
         * output logical plan as PDF (graphviz)
         * @param path
         */
        void toPDF(const std::string& path) const;
    };
}



#endif //TUPLEX_LOGICALPLAN_H