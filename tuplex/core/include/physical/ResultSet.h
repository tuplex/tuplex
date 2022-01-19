//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_RESULTSET_H
#define TUPLEX_RESULTSET_H

#include <Row.h>
#include <Partition.h>
#include <ExceptionInfo.h>
#include <deque>
#include <limits>
#include <ExceptionCodes.h>
#include <Python.h>

namespace tuplex {

    class Partition;

    class ResultSet {
    private:
        std::list<Partition*> _partitions;
        std::vector<Partition*> _exceptions; // unresolved exceptions
        std::unordered_map<std::string, ExceptionInfo> _partitionToExceptionsMap;
        // @TODO: use here rows instead? would make it potentially cleaner...
        std::deque<std::tuple<size_t, PyObject*>> _pyobjects; // python objects remaining whose type
        // did not confirm to the one of partitions. Maybe use Row here instead?
        size_t _curRowCounter; //! row counter for the current partition
        size_t _byteCounter;   //! byte offset for the current partition
        size_t _rowsRetrieved;
        size_t _totalRowCounter; // used for merging in rows!
        size_t _maxRows;
        Schema _schema;

        void removeFirstPartition();
    public:
        ResultSet() : _curRowCounter(0), _byteCounter(0), _rowsRetrieved(0),
        _totalRowCounter(0), _maxRows(0), _schema(Schema::UNKNOWN)  {}
        ~ResultSet() = default;

        // Non copyable
        ResultSet(ResultSet&&) = delete;
        ResultSet(ResultSet&) = delete;
        ResultSet(const ResultSet&&) = delete;
        ResultSet(const ResultSet&) = delete;
        ResultSet& operator = (const ResultSet&) = delete;

        ResultSet(const Schema& _schema,
                  const std::vector<Partition*>& partitions,
                  const std::vector<Partition*>& exceptions=std::vector<Partition*>{},
                  const std::unordered_map<std::string, ExceptionInfo>& exceptionsMap=std::unordered_map<std::string, ExceptionInfo>(),
                  const std::vector<std::tuple<size_t, PyObject*>> &pyobjects=std::vector<std::tuple<size_t, PyObject*>>{},
                  int64_t maxRows=std::numeric_limits<int64_t>::max());

        /*!
         * check whether result contains one more row
         */
        bool hasNextRow();

        /*!
         * get next row. If exhausted, return dummy empty row.
         */
        Row getNextRow();

        /*!
         * get up to limit rows
         * @param limit how many rows to retrieve at most
         * @return vector of rows with up to limit rows
         */
        std::vector<Row> getRows(size_t limit);

        bool hasNextPartition() const;

        /*! user needs to invalidate then!
         *
         * @return
         */
        Partition* getNextPartition();
        size_t rowCount() const;

        Schema schema() const { return _schema; }

        /*!
         * removes and invalidates all partitions!
         */
        void clear();

        /*!
         * retrieve all good rows in bulk, removes them from this result set.
         * @return
         */
        std::vector<Partition*> partitions() {
            std::vector<Partition*> p;
            while(hasNextPartition())
                p.push_back(getNextPartition());
            return p;
        }

        /*!
         * retrieve all unresolved rows (should be only called internally). DOES NOT REMOVE THEM FROM result set.
         * @return
         */
        std::vector<Partition*> exceptions() const { return _exceptions; }

        std::unordered_map<std::string, ExceptionInfo> partitionToExceptionsMap() const { return _partitionToExceptionsMap; }

        /*!
         * returns/removes all objects
         * @return
         */
        std::deque<std::tuple<size_t, PyObject*>> pyobjects() {
            return std::move(_pyobjects);
        }

        size_t pyobject_count() const { return _pyobjects.size(); }

        size_t numPartitions() const { return _partitions.size(); }
    };
}
#endif //TUPLEX_RESULTSET_H