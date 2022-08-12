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
#include <PartitionGroup.h>
#include <deque>
#include <limits>
#include <ExceptionCodes.h>
#include <Python.h>

namespace tuplex {

    class Partition;

    class ResultSet {
    private:
        std::list<Partition*> _currentNormalPartitions; //! normal partitions in current group
        std::list<Partition*> _currentGeneralPartitions; //! general partitions in current group
        std::list<Partition*> _currentFallbackPartitions; //! fallback partitions in current group
        std::list<Partition*> _remainingNormalPartitions; //! remaining normal partitions in other groups
        std::list<Partition*> _remainingGeneralPartitions; //! remaining general partitions in other groups
        std::list<Partition*> _remainingFallbackPartitions; //! remaining fallback partitions in other groups
        std::list<PartitionGroup> _partitionGroups; //! groups together normal, general, and fallback partitions for merging

        size_t _totalRowCounter; //! total rows emitted across all groups
        size_t _maxRows; //! max number of rows to emit
        Schema _schema; //! normal case schema

        size_t _curNormalRowCounter;
        size_t _curNormalByteCounter;
        size_t _curGeneralRowCounter;
        size_t _curGeneralByteCounter;
        size_t _curFallbackRowCounter;
        size_t _curFallbackByteCounter;
        size_t _normalRowCounter;
        size_t _generalRowCounter;
        size_t _fallbackRowCounter;

        int64_t currentGeneralRowInd();
        int64_t currentFallbackRowInd();

        Row getNextNormalRow();
        bool hasNextNormalRow();
        Row getNextFallbackRow();
        bool hasNextFallbackRow();
        Row getNextGeneralRow();
        bool hasNextGeneralRow();

        void removeFirstGeneralPartition();
        void removeFirstFallbackPartition();
        void removeFirstNormalPartition();
    public:
        /*!
         * Create new result set with normal, general, and fallback rows
         * @param schema normal case schema
         * @param normalPartitions normal case rows
         * @param generalPartitions general case rows
         * @param fallbackPartitions fallback case rows
         * @param partitionGroups information to merge row numbers correctly
         * @param maxRows limit on rows to emit
         */
        ResultSet(const Schema& schema,
                  const std::vector<Partition*>& normalPartitions,
                  const std::vector<Partition*>& generalPartitions=std::vector<Partition*>{},
                  const std::vector<Partition*>& fallbackPartitions=std::vector<Partition*>{},
                  const std::vector<PartitionGroup>& partitionGroups=std::vector<PartitionGroup>{},
                  int64_t maxRows=std::numeric_limits<int64_t>::max());

        ResultSet() : _curNormalRowCounter(0), _curNormalByteCounter(0), _curGeneralRowCounter(0), _curGeneralByteCounter(0),
                      _curFallbackRowCounter(0), _curFallbackByteCounter(0), _totalRowCounter(0), _maxRows(0), _schema(Schema::UNKNOWN),
                      _normalRowCounter(0), _generalRowCounter(0), _fallbackRowCounter(0) {}
        ~ResultSet() = default;

        // Non copyable
        ResultSet(ResultSet&&) = delete;
        ResultSet(ResultSet&) = delete;
        ResultSet(const ResultSet&&) = delete;
        ResultSet(const ResultSet&) = delete;
        ResultSet& operator = (const ResultSet&) = delete;

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

        /*!
         * check whether general partitions remain
         * @return
         */
        bool hasNextGeneralPartition() const;

        /*!
         * get next general partition but does not invalidate it
         * @return
         */
        Partition* getNextGeneralPartition();

        /*!
         * check whether fallback partitions remain
         * @return
         */
        bool hasNextFallbackPartition() const;

        /*!
         * get next fallback partition but does not invalidate it
         * @return
         */
        Partition* getNextFallbackPartition();

        /*!
         * check whether normal partitions remain
         * @return
         */
        bool hasNextNormalPartition() const;

        /*! user needs to invalidate then!
         * @return
         */
        Partition* getNextNormalPartition();

        /*!
         * number of rows across all cases of partitions
         * @return
         */
        size_t rowCount() const;

        /*!
         * normal case schema
         * @return
         */
        Schema schema() const { return _schema; }

        /*!
         * removes and invalidates all normalPartitions!
         */
        void clear();

        /*!
         * number of rows in fallback partitions
         * @return
         */
        size_t fallbackRowCount() const;

        /*!
         * retrieve all good rows in bulk, removes them from this result set.
         * @return vector of partitions
         */
        std::vector<Partition*> normalPartitions() {
            std::vector<Partition*> p;
            while(hasNextNormalPartition())
                p.push_back(getNextNormalPartition());
            return p;
        }

        /*!
         * returns all general partitions, removes them from result set.
         * @return vector of partitions
         */
        std::vector<Partition*> generalPartitions() {
            std::vector<Partition*> p;
            while(hasNextGeneralPartition())
                p.push_back(getNextGeneralPartition());
            return p;
        }

        /*!
         * returns all fallback partitions, removes them from result set.
         * @return vector partitions
         */
        std::vector<Partition*> fallbackPartitions() {
            std::vector<Partition*> p;
            while(hasNextFallbackPartition())
                p.push_back(getNextFallbackPartition());
            return p;
        }

        /*!
         * returns all partition groups
         * @return vector of partitiongroup objects
         */
        std::vector<PartitionGroup> partitionGroups() const {
            return std::vector<PartitionGroup>(_partitionGroups.begin(), _partitionGroups.end());
        }
    };
}
#endif //TUPLEX_RESULTSET_H