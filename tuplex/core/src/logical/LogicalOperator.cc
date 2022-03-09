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

    int64_t LogicalOperator::getDataSetID(int64_t default_id_value) {
        if(getDataSet())
            return getDataSet()->getID();
        return default_id_value;
    }


    LogicalOperator::~LogicalOperator() {
        // remove this from parents
        // b.c. need to maintain invariance manually, i.e. remove from pointers when being deconstructed!
        for(auto& p:  _parents) {
            auto it = std::find(p->_children.begin(), p->_children.end(), this);
            if(it != p->_children.end())
                p->_children.erase(it);
        }
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
        // TODO: this function is redundant with smart pointers...
        // recurse
        for(const auto &parent : parents()) {
            parent->freeParents();
        }
        _parents.clear();
    }

    void LogicalOperator::setParents(const std::vector<std::shared_ptr<LogicalOperator>> &parents) {
        _parents.clear(); // this removes all the smart pointers owned by this tree.
        _parents = parents; // now own all the parents!

        // each parent has this as child
        for(auto& parent : _parents) {
            parent->_children = {this};
        }
    }

    std::vector<std::shared_ptr<LogicalOperator>> LogicalOperator::children() const {
        // trick, use here the shared ptrs which represent the children!
        // i.e. this is the parent of each child.
        std::vector<std::shared_ptr<LogicalOperator>> v;
        for(auto child : _children)
            v.emplace_back(child->shared_from_this());
        return v;
    }

    void LogicalOperator::setChildren(const std::vector<std::shared_ptr<LogicalOperator>> &children) {
        // each child gets owned by this

        // remove this from children parents
        for(auto child : _children) {
            child->_parents.erase(std::find(child->_parents.begin(), child->_parents.end(), shared_from_this()));
        }
        _children.clear();

        for(auto child : children) {
            child->setParent(shared_from_this()); // this becomes parent, thus child becomes owner of this
        }
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
}