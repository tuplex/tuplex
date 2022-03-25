//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_DATASET_H
#define TUPLEX_DATASET_H

#include <memory>
#include "Schema.h"
#include "UDF.h"
#include "Partition.h"
#include "Row.h"
#include "Context.h"
#include <ExceptionCodes.h>
#include "Defs.h"
#include <limits>

namespace tuplex {

    class DataSet;

    class Context;

    class LogicalOperator;

    class ResultSet;

    class Partition;

    class CacheOperator;

    inline std::unordered_map<std::string, std::string> defaultCSVOutputOptions() {
        std::unordered_map<std::string, std::string> m;
        m["header"] = "true"; // write header...
        m["null_value"] = ""; // empty string
        m["delimiter"] = ",";
        m["quotechar"] = "\"";
        return m;
    }

    /*!
     * default output options for Orc file format
     * @return Key-value string map of options
     */
    inline std::unordered_map<std::string, std::string> defaultORCOutputOptions() {
        std::unordered_map<std::string, std::string> m;
        return m;
    }

    // maybe CRTP (Curiously recurring template pattern may be used here)
    // but likely it is going to be difficult
    // since the structure is already quite complicated
    class DataSet {
        friend class Context;
        friend class CacheOperator; // is allowed to change the schema
    protected:
        int _id;

        Schema _schema;
        Context *_context;
        LogicalOperator *_operator;

        // one or more (materialized) partitions belong to a dataset
        // they are used to store the data
        // this vector orders the partitions
        // some of them may also be error partitions (later feature, right now simple & straight processing)
        // if a DataSet has zero partitions, that simply means it has not been yet materialized or executed.
        std::vector<Partition *> _partitions;

        bool _cached; // indicates whether all partitions are in main memory

        std::vector<std::string> _columnNames;

        void setSchema(const Schema& schema) { _schema = schema; }
    public:

        DataSet() : _id(-1),
                    _schema(Schema::UNKNOWN),
                    _context(nullptr),
                    _operator(nullptr) {}

        DataSet(Context &context) : _id(-1),
                                    _schema(Schema::UNKNOWN),
                                    _context(&context),
                                    _operator(nullptr) {}

        virtual ~DataSet();

        // NOTE: When defining new functions here, make sure to override them in ErrorDataSet!
        /*!
         * add a map operation T -> S to the logical graph
         * @param udf UDF to apply which yields the trafo T -> S ultimately
         * @return DataSet after the map operation
         */
        virtual DataSet &map(const UDF &udf);

        /*!
         * add a filter operation with a UDF T -> bool to the logical graph.
         * Tuples for which the UDF returns true are kept, the others discarded.
         * @param udf UDF which returns bool
         * @return DataSet after filter operation
         */
        virtual DataSet &filter(const UDF &udf);

        /*!
         * add a resolve operation and apply it to all tuples with exception code ec.
         * @param ec Apply UDF to tuples which resulted in an exception code of type ec
         * @param udf UDF to apply. Type needs to be input of parent and output of parent logical node.
         * @return DataSet after error resolution.
         */
        virtual DataSet &resolve(const ExceptionCode &ec, const UDF &udf);

        /*!
         * ignore the following exception from the operator before
         * @param ec
         * @return
         */
        virtual DataSet &ignore(const ExceptionCode &ec);

        /*!
         * action that displays tuples as nicely formatted table
         * @param numRows how many rows to print, i.e. top numRows are printed.xs
         * @param os ostream where to print table to
         */
        virtual void show(const int64_t numRows = -1, std::ostream &os = std::cout);

        // named dataset management functions
        /*!
         * map Column using a UDF
         * @param columnName column name to map
         * @param udf UDF to execute
         * @return Dataset
         */
        virtual DataSet &mapColumn(const std::string &columnName, const UDF &udf);

        /*!
         * selects a subset of columns from dataset
         * @param columnNames
         * @return Dataset
         */
        virtual DataSet &selectColumns(const std::vector<std::string> &columnNames);

        /*!
         * selects a subset of columns from dataset using integer indices.
         * @param columnIndices
         * @return Dataset or Errordataset
         */
        virtual DataSet &selectColumns(const std::vector<size_t> &columnIndices);


        /*!
         * rename column in dataframe, string based. throws error if oldColumnName doesn't exist.
         * @param oldColumnName
         * @param newColumnName
         * @return Dataset or Errordataset
         */
        virtual DataSet &renameColumn(const std::string &oldColumnName, const std::string &newColumnName);

        /*!
         * rename column based on position in dataframe. throws error if invalid index is supplied.
         * @param index position, 0 <= index < #columns
         * @param newColumnName new column name
         * @return Dataset or Errordataset
         */
        virtual DataSet &renameColumn(int index, const std::string& newColumnName);

        /*!
         * add a new column to dataset, whose result is defined through the given udf
         * @param columnName
         * @param udf
         * @return
         */
        virtual DataSet &withColumn(const std::string &columnName, const UDF &udf);

        /*!
         * performs unique aggregate (i.e. can be also used for duplicate removal)
         * @return Dataset
         */
        virtual DataSet& unique();

        /*!
         * aggregate function
         * @param aggCombine function has signature lambda a, b: ... and needs to yield aggInitial type
         * @param aggUDF function has signature lambda a, x: ... where a is the aggregate type and x is a row
         * @param aggInitial initial value of the aggregate and with what to initialize it.
         * @return DataSet
         */
        virtual DataSet& aggregate(const UDF& aggCombine, const UDF& aggUDF, const Row& aggInitial);

        /*!
         * aggregate by key function
         * @param aggCombine function has signature lambda a, b: ... and needs to yield aggInitial type
         * @param aggUDF function has signature lambda a, x: ... where a is the aggregate type and x is a row
         * @param aggInitial initial value of the aggregate and with what to initialize it.
         * @param keyColumns set of columns to group by when aggregating
         * @return DataSet
         */
        virtual DataSet& aggregateByKey(const UDF& aggCombine, const UDF& aggUDF, const Row& aggInitial, const std::vector<std::string> &keyColumns);

        /*!
         * return column names of dataset
         * @return
         */
        std::vector<std::string> columns() const { return _columnNames; }

        /*!
         * get the normal case outputschema of the underlying operator
         */
        Schema schema() const;

        /*!
         * How many columns dataset has (at least 1)
         * @return number of columns
         */
        size_t numColumns() const;

        /*!
         * join dataset with other dataset, either based on (K, V), (K, W) layout or via column names(equijoin)
         * @param other
         * @param leftColumn
         * @param rightColumn
         * @return DataSet
         */
        virtual DataSet &join(const DataSet &other, option<std::string> leftColumn, option<std::string> rightColumn,
                              option<std::string> leftPrefix = std::string(),
                              option<std::string> leftSuffix = std::string(),
                              option<std::string> rightPrefix = std::string(),
                              option<std::string> rightSuffix = std::string());

        /*!
        * join dataset with other dataset, either based on (K, V), (K, W) layout or via column names(left outer join,
         * i.e. all rows of the left dataset will be in the final result. NULL values will be filled in if there is no match for the right column)
        * @param other
        * @param leftColumn
        * @param rightColumn
        * @return DataSet
        */
        virtual DataSet &leftJoin(const DataSet &other, option<std::string> leftColumn, option<std::string> rightColumn,
                                  option<std::string> leftPrefix = std::string(),
                                  option<std::string> leftSuffix = std::string(),
                                  option<std::string> rightPrefix = std::string(),
                                  option<std::string> rightSuffix = std::string());

        /*!
         * materializes stage in main-memory. Can be used to reuse partitions e.g.
         * @param memoryLayout
         * @return
         */
        virtual DataSet& cache(const Schema::MemoryLayout& memoryLayout, bool storeSpecialized);
        DataSet& cache(bool storeSpecialized=true) { return cache(Schema::MemoryLayout::ROW, storeSpecialized); }

        /*!
         * helper setter without checks, to update internal column names.
         */
        void setColumns(const std::vector<std::string> &columnNames) { _columnNames = columnNames; }

        // these are actions that cause execution
        virtual std::shared_ptr<ResultSet> collect(std::ostream &os = std::cout);

        virtual std::shared_ptr<ResultSet> take(size_t topLimit, size_t bottomLimit, std::ostream &os = std::cout);

        virtual std::vector<Row> collectAsVector(std::ostream &os = std::cout);

        virtual std::vector<Row> takeAsVector(size_t numElements, std::ostream &os = std::cout);

        /*!
         * saves dataset to file. There are multiple options to control the behavior
         * ==> 1.) files can be split across multiple ones. Specify number of files to split rows to
         * ==> 2.) files can be split to max size each (sharding), specify shard size
         * ==> 3.) URI can be one uri and tuplex auto creates numbering scheme, users may specify a naming function.
         * @param fmt Output file format of files
         * @param uri URI of the file (if tuplex should save to multiple files, then this will create a folder, where tuplex places part files.
         * @param udf A udf to name the parts, i.e. will be called with integer for part number and should return string on where to store the file. If empty, this is ignored.
         * @param fileCount number of files to split to. If 0, this is deactivated
         * @param shardSize shardSize in bytes, if set to 0 not active and Tuplex defaults to splitting files after tasks.
         * @param limit max number of rows to output.
         * @param os
         */
        virtual void tofile(FileFormat fmt,
                            const URI &uri,
                            const UDF &udf,
                            size_t fileCount,
                            size_t shardSize,
                            const std::unordered_map<std::string, std::string> &outputOptions,
                            size_t limit = std::numeric_limits<size_t>::max(),
                            std::ostream &os = std::cout);

        /*!
         * saves dataset as a csv file.
         * @param uri URI of the file (if tuplex should save to multiple files, then this will create a folder, where tuplex places part files.
         * @param outputOptions Options for writing the csv file.
         * @param os
         */
        void tocsv(const URI &uri,
                   const std::unordered_map<std::string, std::string> &outputOptions = defaultCSVOutputOptions(),
                   std::ostream &os = std::cout) {
            // empty udf...
            tofile(FileFormat::OUTFMT_CSV, uri, UDF(""), 0, 0, outputOptions, std::numeric_limits<size_t>::max(),
                   os);
        }

        /*!
         * saves dataset as an orc file.
         * supported options:
         * - "columnNames" -> column names as csv string.
         * @param uri URI of the file (if tuplex should save to multiple files, then this will create a folder, where tuplex places part files.
         * @param outputOptions Options for writing the orc file.
         * @param os
         */
        void toorc(const URI &uri,
                   const std::unordered_map<std::string, std::string> &outputOptions = defaultORCOutputOptions(),
                   std::ostream &os = std::cout) {
#ifndef BUILD_WITH_ORC
            throw std::runtime_error(MISSING_ORC_MESSAGE);
#endif

            tofile(FileFormat::OUTFMT_ORC, uri, UDF(""), 0, 0, outputOptions, std::numeric_limits<size_t>::max(),
                   os);
        }

        // some handy functions to complete the API:
        // --> input/output types
        // --> exceptions: I.e. somehow it should be possible to retrieve the exception rows + types?


        bool cached() const { return _cached; }

        virtual int getID() const { return _id; }

        std::vector<Partition *> &getPartitions() { return _partitions; }

        Context *getContext() const { return _context; }

        LogicalOperator* getOperator() const { return _operator; }

        virtual bool isError() const { return false; }
        virtual bool isEmpty() const;
    };
}

#endif //TUPLEX_DATASET_H