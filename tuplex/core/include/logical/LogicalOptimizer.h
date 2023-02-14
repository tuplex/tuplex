//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2022, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 4/5/2022                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifndef TUPLEX_LOGICALOPTIMIZER_H
#define TUPLEX_LOGICALOPTIMIZER_H

#include "Operators.h"
#include <Context.h>

namespace tuplex {
    class LogicalOptimizer {
    public:
        // @TODO: refactor from LogicalPlan!
        // --> i.e. all the opt logic should go here...

        // need a version that can apply optimizations to full tree or solely a stage.
        LogicalOptimizer(const ContextOptions& opt_opts) : _options(opt_opts) {}

        /*!
         * optimize a lineage of operators -> i.e. a single stage
         * @param operators the vector of operators, if inplace is true new operators may be added or operators removed
         * @param inplace whether to modify original operator or return clones of the operators and leave original op untouched.
         * @return optimized lineage of operators
         */
        std::vector<std::shared_ptr<LogicalOperator>> optimize(std::vector<std::shared_ptr<LogicalOperator>>& operators,
                                                               bool inplace=true);

        /*!
         * optimize a full operator graph, i.e. only works for a root node
         * @param root
         * @param inplace
         * @return new op graph or same modified (when inplace=true)
         */
        std::shared_ptr<LogicalOperator> optimize(const std::shared_ptr<LogicalOperator>& root, bool inplace=true);

    private:
        ContextOptions _options;
        MessageHandler& logger() const { return Logger::instance().logger("logical optimizer"); }

        //helper functions
        static void operatorPushup(const std::shared_ptr<LogicalOperator> &op);

        // filter breakup & pushdown optimization
        static void emitPartialFilters(std::shared_ptr<LogicalOperator>& root, bool ignoreConstantTypedColumns);
        static void filterBreakup(const std::shared_ptr<LogicalOperator>& op, bool ignoreConstantTypedColumns);
        static void optimizeFilters(std::shared_ptr<LogicalOperator>& root, bool ignoreConstantTypedColumns);
        static void pushdownFilterInJoin(std::shared_ptr<FilterOperator> fop,
                                         const std::shared_ptr<JoinOperator> &jop,
                                         bool ignoreConstantTypedColumns);
        static void filterPushdown(const std::shared_ptr<LogicalOperator> &op, bool ignoreConstantTypedColumns);

        // optimize constant filters
        static void pruneConstantFilters(const std::shared_ptr<LogicalOperator>& root, bool projectionPushdown);

        // operator reordering (i.e. pullup/pulldown re join cardinality)
        static void reorderDataProcessingOperators(std::shared_ptr<LogicalOperator>& root);

        // @TODO: filter reordering optimization (sampling?)

        // projection pushdown
        /*!
         * this function performs on projection pushdown on operator tree, i.e. restricting which input
         * columns to parse. This will change internal UDFs, types etc. but not add any new nodes.
         * If dropOperators is specified though, map/WithColumn operators that do not need execution
         * anymore due to pushdown are removed from the tree.
         * @param op the operator
         * @param child
         * @param requiredCols
         * @param dropOperators if true, will drop operators?
         * @return which columns are required.
         */
        static std::vector<size_t> projectionPushdown(const std::shared_ptr<LogicalOperator>& op,
                                               const std::shared_ptr<LogicalOperator>& child = nullptr,
                                               std::vector<size_t> requiredCols=std::vector<size_t>(),
                                               bool dropOperators=false,
                                               bool ignoreConstantTypedColumns=false);
        static void rewriteAllFollowingResolvers(std::shared_ptr<LogicalOperator> op, const std::unordered_map<size_t, size_t>& rewriteMap);

    };


    /*!
     * check whether the UDF used in the filter depends on any columns produced by the parent operator.
     * @param filter FilterOperator
     * @return true if it depends, false else. Also returns false if filter has no parent or is nullptr
     */
    extern bool filterDependsOnParentOperator(FilterOperator* filter, bool ignoreConstantTypedColumns);

    /*!
     * check whether the UDF used in the filter depends on any columns produced by the parent operator.
     * @param filter FilterOperator
     * @return true if it depends, false else. Also returns false if filter has no parent or is nullptr
     */
    inline bool filterDependsOnParentOperator(const std::shared_ptr<FilterOperator>& filter, bool ignoreConstantTypedColumns) {
        return filterDependsOnParentOperator(filter.get(), ignoreConstantTypedColumns);
    }
}

#endif //TUPLEX_LOGICALOPTIMIZER_H
