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
#include <physical/ResultSet.h>
#include <DataSet.h>
#include "graphviz/GraphVizBuilder.h"
#include "Schema.h"
// to avoid conflicts with Python3.7
#include "../Context.h"

static const size_t MAX_TYPE_SAMPLING_ROWS=100; // make this configurable? i.e. defines how much to trace...

namespace tuplex {

    // forward declaration of derived classes to allow cousin access
    class DataSet;
    class Context;

    class LogicalOperator {
    private:
        int buildGraph(GraphVizBuilder& builder);
        int64_t _id;
        std::vector<LogicalOperator*> _children;
        std::vector<LogicalOperator*> _parents;
        Schema _schema;
        DataSet *_dataSet;
        void addThisToParents() {
            for(auto parent : _parents) {
                if(!parent)
                    continue;

                if(parent == this)
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
        explicit LogicalOperator(const std::vector<LogicalOperator*>& parents) : _id(logicalOperatorIDGenerator++), _parents(parents), _dataSet(nullptr) { addThisToParents(); }
        explicit LogicalOperator(LogicalOperator* parent) : _id(logicalOperatorIDGenerator++), _parents({parent}), _dataSet(nullptr) {
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

        std::vector<LogicalOperator*> getChildren() const { return _children; }
        LogicalOperator* parent() const { if(_parents.empty())return nullptr; assert(_parents.size() == 1); return _parents.front(); }
        std::vector<LogicalOperator*> parents() const { return _parents; }
        size_t numParents() const { return _parents.size(); }
        size_t numChildren() const { return _children.size(); }


        // manipulating functions for the tree (i.e. used in logical optimizer)
        // Note: this functions DO NOT free memory.
        void setParents(const std::vector<LogicalOperator*>& parents);
        void setChildren(const std::vector<LogicalOperator*>& children);

        void setParent(LogicalOperator* parent) { setParents({parent}); }
        void setChild(LogicalOperator* child) { setChildren({child}); }

        /*!
         * swap pointers out
         * @param oldParent
         * @param newParent
         * @return true if oldParent found, false else
         */
        bool replaceParent(LogicalOperator* oldParent, LogicalOperator* newParent) {
            auto it = std::find(_parents.begin(), _parents.end(), oldParent);
            if(it == _parents.end())
                return false;
            *it = newParent;
            return true;
        }

        /*!
         * swap pointers out
         * @param oldChild
         * @param newChild
         * @return true if oldChild found, false else
         */
        bool replaceChild(LogicalOperator* oldChild, LogicalOperator* newChild) {
            auto it = std::find(_children.begin(), _children.end(), oldChild);
            if(it == _children.end())
                return false;
            *it = newChild;
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
        virtual LogicalOperator* clone() = 0;

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
    };
}
#endif //TUPLEX_LOGICALOPERATOR_H