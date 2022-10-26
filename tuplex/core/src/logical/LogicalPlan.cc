//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/LogicalPlan.h>
#include <logical/LogicalOptimizer.h>
#include <logical/UDFOperator.h>
#include <logical/ParallelizeOperator.h>
#include <logical/FileInputOperator.h>
#include <logical/MapOperator.h>
#include <logical/FilterOperator.h>
#include <logical/MapColumnOperator.h>
#include <logical/WithColumnOperator.h>
#include <logical/ResolveOperator.h>
#include <logical/IgnoreOperator.h>
#include <logical/JoinOperator.h>
#include <logical/CacheOperator.h>
#include <set>
#include <algorithm>
#include <visitors/ApplyVisitor.h>
#include <logical/AggregateOperator.h>

namespace tuplex {
    LogicalPlan::LogicalPlan(LogicalOperator *action) {
        assert(action->isActionable());

        _action = action->clone();
    }

    LogicalPlan::~LogicalPlan() {
        // simply call free on operator
        _action->freeParents();
        _action = nullptr;
    }

    PhysicalPlan* LogicalPlan::createPhysicalPlan(const Context& context) {

        Timer timer;

        // first step is to separate out the stages. As of now, only filter/map operations are supported.
        // Hence, there is a single stage.
        // Also, need to separate between narrow & wide stages (i.e. those with and without shuffling)

        // options which will change UDFs or the tree require a copy of the plan to operate.
        bool copy_required = context.getOptions().OPT_NULLVALUE_OPTIMIZATION() ||
                context.getOptions().OPT_SELECTION_PUSHDOWN() ||
                             context.getOptions().OPT_FILTER_PUSHDOWN();

        // optimize first if desired (context options object)
        // ==> optimize creates a copy if required

        auto optimized_plan = optimize(context, !copy_required); // overwrite

        double logical_optimization_time = timer.time();
        context.metrics().setLogicalOptimizationTime(logical_optimization_time);
        Logger::instance().logger("logical planner").info("logical optimization took "
        + std::to_string(logical_optimization_time) + "ms");

        Timer pp_timer;
        auto plan = new PhysicalPlan(optimized_plan, this, context);
        Logger::instance().logger("physical planner").info("Creating physical plan took " + std::to_string(pp_timer.time()) + "s");
        return plan;
    }

    bool verifyLogicalPlan(const std::shared_ptr<LogicalOperator>& root) {
        using namespace std;

        stringstream ss;
        if(!root)
            return false;

        std::queue<std::shared_ptr<LogicalOperator>> q; // BFS
        q.push(root);
        bool success = true;
        while(!q.empty()) {
            auto node = q.front(); q.pop();

            // check whether children and parents are set up properly for this node
            auto children = node->children();
            auto parents = node->parents();

            auto node_name = node->name() + "(" + std::to_string(node->getID()) + ")";

            // check that node is parent of children
            for(auto child : children) {
                auto cp = child->parents();
                auto it = std::find_if(cp.begin(), cp.end(), [&](const std::shared_ptr<LogicalOperator> &c) { return c == node; });
                if(it == cp.end()) {
                    success = false;
                    ss<<node_name<<": not in "<<child->name()<<"(" + std::to_string(child->getID()) + ")'s parents\n";
                }
            }

            // check that node is child of all parents
            for(const auto &p : parents) {
                auto pc = p->children();
                auto it = std::find(pc.begin(), pc.end(), node);
                if(it == pc.end()) {
                    success = false;
                    ss<<node_name<<": not in "<<p->name()<<"(" + std::to_string(p->getID()) + ")'s children\n";
                }
            }

            // add all parents to queue
            for(const auto &p : node->parents())
                q.push(p);
        }

        if(!success)
            Logger::instance().defaultLogger().error("validation of logical plan failed. Details:\n" + ss.str());
        return success;
    }

    template<typename T> std::string toStrWithInf(const T & t) {
        switch(t) {
            case std::numeric_limits<T>::min():
                return "-inf";
            case std::numeric_limits<T>::max():
                return "inf";
        }
        return std::to_string(t);
    }

    LogicalPlan* LogicalPlan::optimize(const Context& context, bool inPlace) {

        using namespace std;

        // make copy if requested
        if(!inPlace)
            return clone()->optimize(context, true);

        // new: using separate logical optimizer class
        auto opt = std::make_unique<LogicalOptimizer>(context.getOptions());

#ifndef NDEBUG
#ifdef GENERATE_PDFS
        toPDF("logical_plan_before_opt.pdf");
#else
        Logger::instance().defaultLogger().debug("saving logical plan before optimizations to PDF skipped.");
#endif
#endif

#ifndef NDEBUG
        assert(verifyLogicalPlan(_action));
#endif

        _action = opt->optimize(_action, true);

#ifndef NDEBUG
#ifdef GENERATE_PDFS
        toPDF("logical_plan_after_opt.pdf");
#else
        Logger::instance().defaultLogger().debug("saving logical plan before optimizations to PDF skipped.");
#endif
#endif

#ifndef NDEBUG
        assert(verifyLogicalPlan(_action));
#endif
        return this;
    }

    void recursiveLPBuilder(GraphVizBuilder &b, LogicalOperator *root, int idx = -1) {


        std::string color_string = "lightblue";

        // change color of node depending on type (source, inner, sink)
        // check https://www.w3schools.com/colors/colors_names.asp
        if(root->isDataSource())
            color_string = "lightseagreen";
        if(root->isActionable())
            color_string = "gainsboro";


        auto outRowType = root->getOutputSchema().getRowType();
        std::string rows_label =
                !outRowType.isTupleType() ? "?? columns" : std::to_string(outRowType.parameters().size()) +
                                                                     " columns";

        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"" + color_string + "\">";
        html += root->name() + ":" + std::to_string(root->getID());
        html += "</TD></TR><TR><TD>" + rows_label + " </TD></TR></TABLE>";

        int new_idx = b.addHTMLNode(html);
        if (idx != -1) {
            b.addEdge(new_idx, idx);
        }
        for (const auto &p : root->parents())
            recursiveLPBuilder(b, p.get(), new_idx);
    }

    void LogicalPlan::toPDF(const std::string &path) const {
        GraphVizBuilder b;
        recursiveLPBuilder(b, _action.get());
        b.saveToPDF(path);
    }

    LogicalPlan* LogicalPlan::clone() {
        if(!_action)
            return new LogicalPlan(nullptr);

        // perform a deep copy of the plan...
        return new LogicalPlan(_action.get());
    }
}