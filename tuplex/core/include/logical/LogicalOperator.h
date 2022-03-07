//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LOGICALOPERATOR_H
#define TUPLEX_LOGICALOPERATOR_H

#include <logical/LogicalOperatorType.h>
#include <vector>
#include <utility>
#include <functional>
#include <memory>
#include <physical/ResultSet.h>
#include <DataSet.h>
#include "graphviz/GraphVizBuilder.h"
#include "Schema.h"
// to avoid conflicts with Python3.7
#include "../Context.h"

#include "cereal/access.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/map.hpp"
#include "cereal/types/unordered_map.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/common.hpp"

#include "cereal/archives/binary.hpp"


static const size_t MAX_TYPE_SAMPLING_ROWS=100; // make this configurable? i.e. defines how much to trace...

namespace tuplex {

    // forward declaration of derived classes to allow cousin access
    class DataSet;
    class Context;

    class LogicalOperator : public std::enable_shared_from_this<LogicalOperator> {
    private:
        int buildGraph(GraphVizBuilder& builder);
        int64_t _id;
        // tree structure here is a bit different then what is usually being done.
        // I.e., because an action represents a leaf node it has to own its parents
        // since we construct the object in the c'tor, we can't use weak ptrs to establish
        // the two way connection but need to use raw pointers.
        // the destructor thereby maintains the invariance.
        std::vector<LogicalOperator*> _children;
        std::vector<std::shared_ptr<LogicalOperator>> _parents;
        Schema _schema;
        DataSet *_dataSet; // TODO: figure out dataset serialization!

        void addThisToParents() {
            for(const auto &parent : _parents) {
                if(!parent)
                    continue;

                if(parent.get() == this)
                    throw std::runtime_error("cycle encountered! invalid for operator graph.");
                parent->_children.push_back(this);
            }
        }

        // for assigning continuously running IDs
        static int64_t logicalOperatorIDGenerator; // start at 100,000
    protected:
        void setSchema(const Schema& schema) { _schema = schema; }
        virtual void copyMembers(const LogicalOperator* other);
    public:
        explicit LogicalOperator(const std::vector<std::shared_ptr<LogicalOperator>>& parents) : _id(logicalOperatorIDGenerator++), _parents(parents.begin(), parents.end()), _dataSet(nullptr) { addThisToParents(); }
        explicit LogicalOperator(const std::shared_ptr<LogicalOperator>& parent) : _id(logicalOperatorIDGenerator++),
        _parents({parent}), _dataSet(nullptr) {
            if(!parent)
                throw std::runtime_error(name() + " can't have nullptr as parent");
            addThisToParents();
        }
        LogicalOperator() : _id(logicalOperatorIDGenerator++), _dataSet(nullptr) { addThisToParents(); }

        virtual ~LogicalOperator();

        virtual std::string name() { return "logical operator"; }
        virtual LogicalOperatorType type() const = 0;

        bool isLeaf() { return 0 == _children.size(); }
        virtual bool isRoot() { return 0 == _parents.size(); }

        Schema schema() const { return _schema; }

        /*!
         * checks whether logical operator is valid.
         * @return
         */
        virtual bool good() const = 0;

        std::vector<LogicalOperator*> children() const { return _children; }
        std::shared_ptr<LogicalOperator> parent() const { if(_parents.empty())return nullptr; assert(_parents.size() == 1); return _parents.front(); }
        std::vector<std::shared_ptr<LogicalOperator>> parents() const { return _parents; }
        size_t numParents() const { return _parents.size(); }
        size_t numChildren() const { return _children.size(); }


        // manipulating functions for the tree (i.e. used in logical optimizer)
        // Note: this functions DO NOT free memory.
        void setParents(const std::vector<std::shared_ptr<LogicalOperator>>& parents);
        void setChildren(const std::vector<std::shared_ptr<LogicalOperator>>& children);

        void setParent(const std::shared_ptr<LogicalOperator>& parent) { setParents({parent}); }
        void setChild(const std::shared_ptr<LogicalOperator>& child) { setChildren({child}); }

        /*!
         * swap pointers out
         * @param oldParent
         * @param newParent
         * @return true if oldParent found, false else
         */
        inline bool replaceParent(const std::shared_ptr<LogicalOperator>& oldParent,
                                  const std::shared_ptr<LogicalOperator>& newParent) {
            auto it = std::find(_parents.begin(), _parents.end(), oldParent);
            if(it == _parents.end())
                return false;
            *it = newParent;

            // need to maintain invariance...!
            // newParent has this as child
            newParent->_children = {this};
            return true;
        }

        /*!
         * swap pointers out
         * @param oldChild
         * @param newChild
         * @return true if oldChild found, false else
         */
        inline bool replaceChild(const std::shared_ptr<LogicalOperator>& oldChild,
                                 const std::shared_ptr<LogicalOperator>& newChild) {
            // this is more tricky, need to maintain invariance in tree.

            auto it = std::find_if(_children.begin(), _children.end(), [&oldChild](LogicalOperator* child) {
                return child == oldChild;
            });
            if(it == _children.end())
                return false;

            // now swap pointers
            // -> it is the pointer to the child, add smart ptr to parents etc.
            *it = newChild.get(); // raw pointer
            newChild->setParent(shared_from_this()); // make newChild's parent this (i.e. this now owns the child)
            return true;
        }

        /*!
         * retrieves a sample for type inference.
         * @param num number of samples to return. Note that these are restricted by parent operators.
         * @return collection of rows. These do not have to have the same type necessarily.
         */
        virtual std::vector<Row> getSample(const size_t num=1) const = 0;

        virtual void setDataSet(DataSet *ds) { _dataSet = ds;}

        /*!
         * get ID of attached dataset. If no dataset is found, return default value
         * @param default_id_value default ID value to return
         * @return dataSetID or defaultID
         */
        int64_t getDataSetID(int64_t default_id_value=-1);

        /*!
         * returns whether the given operator is a "final" operator in the way it is
         * an action that needs to be executed
         * @return true if it is an action or false if not
         */
        virtual bool isActionable() = 0;

        /*!
         * the first operator to be executed must read or provide data. This function allows to check for this property
         * @return true if node is a data provider
         */
        virtual bool isDataSource() = 0;

        virtual Schema getOutputSchema() const { assert(_schema.getRowType() != python::Type::UNKNOWN); return _schema; };

        virtual Schema getInputSchema() const = 0;

        /*!
         * creates a logical plan for this node, uses the logical plan to create a physical plan,
         * executes the physical plan and returns a ResultSet to collect the result of the computation
         * @param Context context on which to execute this plan
         * @return pointer to result set. Caller owns memory management of this pointer. nullptr if computation failed.
         */
        std::shared_ptr<ResultSet> compute(const Context& context);

        DataSet* getDataSet() const { return _dataSet; }

        int64_t getID() const { return _id; }

        /*!
         * retype the operator by providing an optional rowType
         * @param rowTypes vector of row types to use to retype the operator
         * @return true if retyping was successful.
         */
        virtual bool retype(const std::vector<python::Type>& rowTypes=std::vector<python::Type>()) { return false; }

        /*!
         * overwrite internal ID. Should be only used in LogicalOptimizer
         */
        void setID(int64_t id) { _id = id; }

        /*!
         * returns columns associated with the dataset belonging to this operator.
         * I.e. these are the OUTPUT columns after applying the operator.
         */
        virtual std::vector<std::string> columns() const = 0;

        /*!
         * returns columns fed into this operator, i.e. these are the output columns of the parent.
         * @return
         */
        virtual std::vector<std::string> inputColumns() const {
            if(_parents.size() == 1)
                return parent()->columns();
            else
                throw std::runtime_error("default input columns not working, consider overwriting this function in parents");
        }

        /*!
         * make a deep copy of the operator tree (needed because optimizers may rewrite UDFs.)
         * @return copy of this operator.
         */
        virtual std::shared_ptr<LogicalOperator> clone() = 0;

        /*!
         * used for cost based analysis (very dumb so far)
         * @return
         */
        virtual int64_t cost() const {
            // sum of parents
            int64_t sum = 0;
            for(auto p : parents())
                if(p)
                    sum += p->cost();
            return sum;
        }

        /*!
         * delete all parents
         */
        virtual void freeParents();

        /*!
        * retrieve sample as python objects.
        * @param num
        * @return python objects, acquires GIL and releases GIL
        */
        virtual std::vector<PyObject*> getPythonicSample(size_t num);

        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(_id, _parents, _schema);
        }
        template<class Archive> void load(Archive &ar) {
            ar(_id, _parents, _schema);
            addThisToParents(); // build children of parents
        }
    };
}
#endif //TUPLEX_LOGICALOPERATOR_H