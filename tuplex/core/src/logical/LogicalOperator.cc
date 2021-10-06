//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <graphviz/GraphVizBuilder.h>
#include <logical/LogicalOperator.h>
#include <cassert>
#include <logical/LogicalPlan.h>
#include "cereal/types/memory.hpp"
#include "cereal/types/vector.hpp"

namespace tuplex {

    // init static class members
    int64_t LogicalOperator::logicalOperatorIDGenerator = 100000; // start at 100,000

    int LogicalOperator::buildGraph(GraphVizBuilder &builder) {
        int id = builder.addHTMLNode(name());

        // add all children & connection
        for(auto c : _children) {
            assert(c);
            int cid = c->buildGraph(builder);
            builder.addEdge(id, cid);
        }
        return id;
    }


    LogicalOperator::~LogicalOperator() {
#warning "memory management of op graph??"
    }


    std::shared_ptr<ResultSet> LogicalOperator::compute(const Context& context) {

        Timer planningTimer;
        // create LogicalPlan from this node
        LogicalPlan* lp = new LogicalPlan(this);
        assert(lp);
        PhysicalPlan* pp = lp->createPhysicalPlan(context);
        assert(pp);
        double planningTime = planningTimer.time();

        Timer executionTimer;
        pp->execute();
        auto rs = pp->resultSet();
        double executionTime = executionTimer.time();

        // output timing stats to logger (only if PP is good)
        if(pp->good()) {
            std::stringstream ss;
            ss<<"Query Execution took "<<executionTime + planningTime<<"s. (planning: "<<planningTime<<"s, execution: "<<executionTime<<"s)";
            Logger::instance().defaultLogger().info(ss.str());
        }

        // free plan memory
        delete lp;
        delete pp;

        return rs;
    }

    void LogicalOperator::copyMembers(const tuplex::LogicalOperator *other) {
        if(other) {
            _dataSet = other->_dataSet;
            _id = other->_id;
            // children and parents left, because special case...
            _schema = other->_schema;
        }
    }

    void LogicalOperator::freeParents() {
        // recurse
        for(const auto &parent : parents()) {
            parent->freeParents();
        }
        _parents.clear();
    }

    void LogicalOperator::setParents(const std::vector<std::shared_ptr<LogicalOperator>> &parents) {
        _parents.clear();
        _parents = parents;
    }

    void LogicalOperator::setChildren(const std::vector<LogicalOperator *> &children) {
        _children.clear();
        _children = children;
    }

    std::vector<PyObject*> LogicalOperator::getPythonicSample(size_t num) {
        std::vector<PyObject*> v;
        auto rows = getSample(num);
        python::lockGIL();
        v.reserve(rows.size());
        for(const auto& r : rows)
            v.push_back(python::rowToPython(r, true));
        python::unlockGIL();
        return v;
    }

    // cereal serialization functions
    template<class Archive>
    void LogicalOperator::serialize(Archive &archive) {
        archive(_id, _parents, _schema, _dataSet);
        // addThisToParents() should be called by default constructor
    }
//    template<class Archive>
//    void LogicalOperator::save(Archive &archive) const {
//        archive(_id, _parents, _schema, _dataSet);
//    }
}