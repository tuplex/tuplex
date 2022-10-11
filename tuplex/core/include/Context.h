//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CONTEXT_H
#define TUPLEX_CONTEXT_H

#include <logical/LogicalOperatorType.h>
#include <logical/LogicalOperator.h>
#include <DataSet.h>
#include "Partition.h"
#include <PartitionGroup.h>
#include "Row.h"
#include "HistoryServerClasses.h"
#include <initializer_list>
#include <iostream>
#include <vector>
#include <iterator>
#include <ContextOptions.h>
#include <JITCompiler.h>
#include <Utils.h>
#include <graphviz/GraphVizBuilder.h>
#include <ee/IBackend.h>
#include "JobMetrics.h"


namespace tuplex {

    class DataSet;
    class Context;
    class LogicalOperator;
    class Executor;
    class Partition;
    class IBackend;
    class PartitionGroup;

    class Context {
    private:
        int _datasetIDGenerator;
        uniqueid_t _uuid;
        int _id;

        static int _contextIDGenerator;

        // stores all datasets belonging to this context
        // right now, this class handles memory for this
        // later maybe transfer ownership of memory to MemoryManager (handles Partitions only)
        std::vector<DataSet*> _datasets;

        // a context is associated with a number of logical operators
        // the context object does the memory management of these operators.
        // a dataset is the result of applying the DAG of operations.
        std::vector<LogicalOperator*> _operators;

        // needed because of C++ template issues
        void addPartition(DataSet* ds, Partition *partition);

        /*!
         * Add parallelize logical operator to dataset
         * @param ds dataset
         * @param fallbackPartitions fallback partitions from python parallelize
         * @param partitionGroups partition mapping information
         */
        void addParallelizeNode(DataSet *ds, const std::vector<Partition*>& fallbackPartitions=std::vector<Partition*>{}, const std::vector<PartitionGroup>& partitionGroups=std::vector<PartitionGroup>{}); //! adds a paralellize node to the computation graph

        Partition* requestNewPartition(const Schema& schema, const int dataSetID, size_t minBytesRequired);
        uint8_t* partitionLockRaw(Partition *partition);
        void partitionUnlock(Partition *partition);
        size_t partitionCapacity(Partition *partition);
        void setColumnNames(DataSet* ds, const std::vector<std::string>& names);

        ContextOptions _options; //! global config for a context object. Holds defaults for memory/code generation/...

        std::unique_ptr<IBackend> _ee; //! execution backend of this context

        // context properties
        std::string _name;

        std::shared_ptr<JobMetrics> _lastJobMetrics;

        codegen::CompilePolicy _compilePolicy;
        codegen::CompilePolicy compilePolicyFromOptions(const ContextOptions& options);

        inline int getNextContextID() { return _contextIDGenerator++; }

    protected:
        inline int getNextDataSetID() { return _datasetIDGenerator++; };

        /*!
         * copies data from the iterators to a dataset. Data must have a fixed size per row (i.e. no variable size fields like strings, dicts, lists,...)
         * further: currently, no nested tuple types etc. are supported.
        */
        template<class Iterator> DataSet& parallelizeFixedSizeData(Iterator first, Iterator last, const Schema& schema, const std::vector<std::string>& columnNames=std::vector<std::string>());

    public:
        Context(const ContextOptions& options=ContextOptions::load());
        virtual ~Context();

        Executor* getDriver() const;

        // disable copying
        Context(const Context& other) = delete;

        int id() const { return _id; }

        // create from array
        DataSet& parallelize(std::initializer_list<int> L, const std::vector<std::string>& columnNames=std::vector<std::string>()) {
            if(!columnNames.empty())
                if(columnNames.size() != 1)
                    return makeError("more than one column name given");
            return parallelizeFixedSizeData(L.begin(),
                                            L.end(),
                                            Schema(Schema::MemoryLayout::ROW,
                                                   python::Type::makeTupleType({python::Type::I64})),
                                            columnNames);
        }

        DataSet& parallelize(const std::vector<Row>& L,
                             const std::vector<std::string>& columnNames=std::vector<std::string>());

        DataSet& parallelize(std::initializer_list<double> L, const std::vector<std::string>& columnNames=std::vector<std::string>()) {
            if(!columnNames.empty())
                if(columnNames.size() != 1)
                    return makeError("more than one column name given");
            return parallelizeFixedSizeData(L.begin(),
                                            L.end(),
                                            Schema(Schema::MemoryLayout::ROW,
                                                   python::Type::makeTupleType({python::Type::F64})),
                                            columnNames);
        }

        DataSet& parallelize(std::initializer_list<int64_t > L, const std::vector<std::string>& columnNames=std::vector<std::string>()) {
            if(!columnNames.empty())
                if(columnNames.size() != 1)
                    return makeError("more than one column name given");
            return parallelizeFixedSizeData(L.begin(),
                                            L.end(),
                                            Schema(Schema::MemoryLayout::ROW,
                                                   python::Type::makeTupleType({python::Type::I64})),
                                            columnNames);
        }

        DataSet& parallelize(std::initializer_list<bool> L, const std::vector<std::string>& columnNames=std::vector<std::string>()) {
            if(!columnNames.empty())
                if(columnNames.size() != 1)
                    return makeError("more than one column name given");
            return parallelizeFixedSizeData(L.begin(),
                                            L.end(),
                                            Schema(Schema::MemoryLayout::ROW,
                                                   python::Type::makeTupleType({python::Type::BOOLEAN})),
                                            columnNames);
        }

        template<typename Iterator> DataSet& parallelize(Iterator begin, Iterator end, const std::vector<std::string>& columnNames=std::vector<std::string>()) {
            return parallelizeFixedSizeData(begin, end, Schema(Schema::MemoryLayout::ROW,
                                                               python::Type::makeTupleType({deductType(*begin)})));
        }



        // Note: ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘n/a’, ‘nan’, ‘null’
        // is the array of values pandas treats as null! ==> should include that in parsing...
        /*!
         * reads csv files with autodetection into memory.
         * Analysis is done when function is called but actual loading is deferred (lazy)
         * @param pattern file pattern to search for
         * @param null_values optional array of strings to identify as null values.
         * @param columns optional columns/header preset. Also makes sense for headerless files
         * @param hasHeader optional info whether CSV files have a header or not
         * @param delimiter optional specification of delimiter
         * @param quotechar quote char
         * @param null_values list of string to interpret as null values.
         * @param index_based_type_hints map of column indices to force type of column to certain value.
         * @param column_based_type_hints map of column names to force type of column to certain value.
         * @return Dataset
         */
        DataSet& csv(const std::string &pattern,
                     const std::vector<std::string>& columns=std::vector<std::string>(),
                     option<bool> hasHeader=option<bool>::none,
                     option<char> delimiter=option<char>::none,
                     char quotechar='"',
                     const std::vector<std::string>& null_values=std::vector<std::string>{""},
                     const std::unordered_map<size_t, python::Type>& index_based_type_hints=std::unordered_map<size_t, python::Type>(),
                     const std::unordered_map<std::string, python::Type>& column_based_type_hints=std::unordered_map<std::string, python::Type>());

        /*!
         * read data from newline-delimited json file
         * @param pattern file pattern to search for
         * @return Dataset
         */
        DataSet& json(const std::string& pattern,
                      bool unwrap_first_level=true,
                      bool treat_heterogenous_lists_as_tuples=true);

        /*!
         * reads text files with into memory. Type will be always string or Option[string]. Parsing is done using line delimiters \n and \r
         * Analysis is done when function is called but actual loading is deferred (lazy)
         * @param pattern file pattern to search for
         * @param null_values which string to interpret as null values
         * @return Dataset
         */
        DataSet& text(const std::string &pattern, const std::vector<std::string>& null_values=std::vector<std::string>{});

        /*!
         * reads orc files with into memory.
         * @param pattern file pattern to search for
         * @param columns optional columns/header preset. Also makes sense for headerless files
         * @return Dataset
         */
        DataSet& orc(const std::string &pattern,
                     const std::vector<std::string>& columns=std::vector<std::string>());

        /*!
         * creates an error dataset
         * @param error
         * @return
         */
        DataSet& makeError(const std::string& error);

        DataSet& makeEmpty();

        IBackend* backend() const { assert(_ee); return _ee.get(); }

        LogicalOperator* addOperator(LogicalOperator* op);

        void visualizeOperationGraph(GraphVizBuilder& builder);

        DataSet* createDataSet(const Schema& schema);

        ContextOptions getOptions() const { return _options; }

        std::string uuid() const { return uuidToString(_uuid); }

        std::string name() const { return _name; }
        void setName(const std::string& name) { _name = name; }

        /*!
         * get the compile policy associated with this context
         * @return
         */
        const codegen::CompilePolicy& compilePolicy() const { return _compilePolicy; }

        /*!
         * gets a JobMetrics object
         * @return address to JobMetrics object
         */
        JobMetrics& metrics() const {
            if (!_lastJobMetrics)
                const_cast<Context *>(this)->_lastJobMetrics.reset(new JobMetrics());
            return *_lastJobMetrics.get();
        }

        /*!
         * gets a JobMetrics object
         * @return ptr to JobMetrics object
         */
        std::shared_ptr<JobMetrics> getMetrics() {
            return _lastJobMetrics;
        }

        /*!
         * construct a dataset using customly allocated partitions.
         * @param schema schema of the data within this dataset.
         * @param partitions partitions to assign to dataset. These should NOT be reused later.
         *        Also, partitions need to hold data in supplied schema. If empty vector is given,
         *        empty dataset will be created.
         * @param fallbackPartitions fallback partitions to assign to dataset
         * @param partitionGroups mapping of partitions to fallback partitions
         * @param columns optional column names
         * @return reference to newly created dataset.
         */
        DataSet& fromPartitions(const Schema& schema, const std::vector<Partition*>& partitions, const std::vector<Partition*>& fallbackPartitions, const std::vector<PartitionGroup>& partitionGroups, const std::vector<std::string>& columns);
    };
    // needed for template mechanism to work
#include <DataSet.h>

    template<class Iterator> DataSet& Context::parallelizeFixedSizeData(Iterator first, Iterator last,
            const Schema& schema, const std::vector<std::string>& columnNames) {
        using value_type = typename std::iterator_traits<Iterator>::value_type;

        // create new Dataset
        int dataSetID = getNextDataSetID();
        DataSet *dsptr = createDataSet(schema);

        int numRows = last - first;

        // transfer data in as many partitions as necessary
        // this here is row based

        assert(schema.getMemoryLayout() == Schema::MemoryLayout::ROW); // Columnar to follow, for now only row based layout is supported
        assert(schema.hasFixedSize()); // only fixed size rows for now

        // because size is fixed, can infer minRequired directly!
        auto flat_tuple_type = flattenedType(schema.getRowType());
        assert(flat_tuple_type.isTupleType());
        size_t minBytesRequired = flat_tuple_type.parameters().size() * sizeof(int64_t);

        int numPartitions = 0;
        Partition *partition = requestNewPartition(schema, dataSetID, minBytesRequired);
        numPartitions++;

        if(!partition)
            return makeError("no memory left to hold data");

        uint8_t* base_ptr = partitionLockRaw(partition);//(uint8_t*)partition->lock();
        int64_t* num_rows_ptr = (int64_t*)base_ptr;
        *num_rows_ptr = 0;
        base_ptr += sizeof(int64_t);

        int bytesPerPartitionTransferred = 0;
        int64_t totalBytesTransferred = 0;
        Iterator it = first;

        // debug assert, make sure schema match!
        assert(serializationSchema(*(it)).getRowType() == schema.getRowType());

        Serializer serializer;
        int numTransferredRows = 0;
        while(it != last) {

            // check if there is enough space to write one element to memory
            value_type value = *(it);
            serializer.reset().append(value);
            auto serializedBytes = serializer.serialize(base_ptr,
                                                       partitionCapacity(partition) - bytesPerPartitionTransferred);

            if(serializedBytes > 0) {
                bytesPerPartitionTransferred += serializedBytes;
                base_ptr += serializedBytes;
                numTransferredRows++;
                *num_rows_ptr = *num_rows_ptr + 1;
                ++it;
            } else {
                // check that at least one element can be transferred
                if(bytesPerPartitionTransferred == 0) {
                    Logger::instance().logger("core").error("partition size is too small. Can't transfer a single element");
                    //partition->unlock();
                    partitionUnlock(partition);
                    EXCEPTION("too low partition size");
                }

                // create new partition...
                //partition->unlock();
                partitionUnlock(partition);
                addPartition(dsptr, partition);

                partition = requestNewPartition(schema, dataSetID, minBytesRequired);
                numPartitions++;
                totalBytesTransferred += bytesPerPartitionTransferred;
                bytesPerPartitionTransferred = 0;
                if(!partition)
                    return makeError("no memory left to hold data (after having transferred "
                                     + std::to_string(numTransferredRows) + " elements)");
                base_ptr = partitionLockRaw(partition);//(uint8_t*)partition->lock();
                num_rows_ptr = (int64_t*)base_ptr;
                *num_rows_ptr = 0;
                base_ptr += sizeof(int64_t);
            }
        }
        totalBytesTransferred += bytesPerPartitionTransferred;
        //partition->unlock();
        partitionUnlock(partition);
        addPartition(dsptr, partition);

        Logger::instance()
                .logger("core")
                .info("materialized " + sizeToMemString(totalBytesTransferred) + " to "
                      + std::to_string(numPartitions) + " partitions");


        // add parallelize node to operation graph
        setColumnNames(dsptr, columnNames);
        addParallelizeNode(dsptr);
        return *dsptr;
    }
}


#endif //TUPLEX_CONTEXT_H