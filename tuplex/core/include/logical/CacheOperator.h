//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CACHEOPERATOR_H
#define TUPLEX_CACHEOPERATOR_H

#include "LogicalOperator.h"

namespace tuplex {

    /*!
     * caches (materializes) rows in main-memory. Can be used as artifical pipeline breaker,
     * or to speed up queries. Partitions live forever.
     */
    class CacheOperator : public LogicalOperator {
    public:
        virtual ~CacheOperator() override = default;

        CacheOperator(LogicalOperator* parent, bool storeSpecialized,
                      const Schema::MemoryLayout& memoryLayout=Schema::MemoryLayout::ROW) : LogicalOperator(parent), _storeSpecialized(storeSpecialized), _memoryLayout(memoryLayout), _cached(false),
        _columns(parent->columns()), _normalRowCount(0), _fallbackRowCount(0), _generalRowCount(0) {
            setSchema(this->parent()->getOutputSchema()); // inherit schema from parent
            _optimizedSchema = getOutputSchema();
            if(memoryLayout != Schema::MemoryLayout::ROW)
                throw std::runtime_error("only row based memory layout yet supported");

            // store sample
            _sample = parent->getSample(MAX_TYPE_SAMPLING_ROWS);
        }

        std::string name() override {
           return "cache";
        }

        LogicalOperatorType type() const override { return LogicalOperatorType::CACHE; }
        bool isActionable() override { return true; }
        bool isDataSource() override { return true; }

        bool good() const override { return true; }

        Schema getInputSchema() const override { return getOutputSchema(); }
        Schema getOptimizedOutputSchema() const { return _optimizedSchema; }

        // force optimized schema
        void setOptimizedOutputType(const python::Type& rowType) {
            _optimizedSchema = Schema(_memoryLayout, rowType);
        }
        void useNormalCase() {
            // optimized schema becomes normal schema
            setSchema(_optimizedSchema);
        }

        virtual std::vector<Row> getSample(const size_t num) const override {
            if(num > _sample.size()) {
                Logger::instance().defaultLogger().warn("requested " + std::to_string(num)
                                                        + " rows for sampling, but only "
                                                        + std::to_string(_sample.size())
                                                        + " cached. Consider decreasing sample size.");
            }

            // retrieve as many rows as necessary from the first file
            return std::vector<Row>(_sample.begin(), _sample.begin() + std::min(_sample.size(), num));
        }
        std::vector<std::string> columns() const override { return _columns; }

        void setResult(const std::shared_ptr<ResultSet>& rs);
        LogicalOperator* clone() override;
        CacheOperator* cloneWithoutParents() const;

        /*!
         * whether this operator holds an in-memory result or not. If not, then
         * @return
         */
        bool isCached() const { return _cached; }
        std::vector<Partition*> cachedNormalPartitions() const { return _normalPartitions; }
        std::vector<Partition*> cachedGeneralPartitions() const { return _generalPartitions; }
        std::vector<Partition*> cachedFallbackPartitions() const { return _fallbackPartitions; }
        std::vector<PartitionGroup> partitionGroups() const { return _partitionGroups; }

        size_t getTotalCachedRows() const;

        int64_t cost() const override;

        /*!
         * whether to store partitions split into normal and general case,
         * or store one version.
         * @return
         */
        bool storeSpecialized() const { return _storeSpecialized; }
    protected:
        void copyMembers(const LogicalOperator* other) override;
    private:

        CacheOperator() : _memoryLayout(Schema::MemoryLayout::ROW), _cached(false) {}

        Schema::MemoryLayout _memoryLayout;
        Schema _optimizedSchema;

        // partitions to be stored in memory. For optimization reasons,
        // cache operator may store partitions split into normal case and general case
        // or merge them.
        bool _cached;
        bool _storeSpecialized;
        std::vector<Partition*> _normalPartitions;    //! holds all data conforming to the normal case schema
        std::vector<Partition*> _generalPartitions;   //! holds all data which is considered to be a normal-case violation,
        std::vector<Partition*> _fallbackPartitions;  //! holds all data which is output as a python object from interpreter processing
        std::vector<PartitionGroup> _partitionGroups; //! groups together partitions for correct row ordering
        std::vector<std::string> _columns;

        // internal sample of normal case rows, used for tracing & Co.
        std::vector<Row> _sample;

        // number of rows need to be stored for cost estimates
        size_t _normalRowCount;
        size_t _generalRowCount;
        size_t _fallbackRowCount;
    };
}

#endif //TUPLEX_CACHEOPERATOR_H