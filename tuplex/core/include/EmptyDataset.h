//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_EMPTYDATASET_H
#define TUPLEX_EMPTYDATASET_H


#include <DataSet.h>

namespace tuplex {

    class EmptyDataSet;
    class DataSet;
    class OperationGraph;
    class Context;

    /*!
     * this is a dummy class to represent an empty dataset. I.e. the result will be always empty
     * unless for joins etc.
     */
    class EmptyDataset : public DataSet {
    public:
        EmptyDataset() = default;

        virtual ~EmptyDataset() {}

        virtual bool isEmpty() const override { return true; }

        // need to overwrite all the functions of DataSet!
        virtual DataSet& map(const UDF& udf) override { return *this; }
        virtual DataSet& filter(const UDF& udf) override {return *this; }

        virtual DataSet& resolve(const ExceptionCode& ec, const UDF& udf) override { return *this; }
        virtual DataSet& ignore(const ExceptionCode& ec) override { return *this; }
        virtual DataSet& mapColumn(const std::string& columnName, const UDF& udf) override { return *this; }
        virtual DataSet& selectColumns(const std::vector<std::string>& columnNames) override { return *this; }
        virtual DataSet& selectColumns(const std::vector<size_t>& columnIndices) override { return *this; }
        virtual DataSet& renameColumn(const std::string& oldColumnName, const std::string& newColumnName) override { return *this; }
        virtual DataSet& withColumn(const std::string& columnName, const UDF& udf) override { return *this; }
        virtual void tofile(FileFormat fmt,
                            const URI& uri,
                            const UDF& udf,
                            size_t fileCount,
                            size_t shardSize,
                            const std::unordered_map<std::string, std::string>& outputOptions,
                            size_t limit,
                            std::ostream& os) override;

        virtual DataSet& join(const DataSet& other, option<std::string> leftColumn, option<std::string> rightColumn,
                              option<std::string> leftPrefix, option<std::string> leftSuffix,
                              option<std::string> rightPrefix, option<std::string> rightSuffix) override  { return *this; }

        virtual DataSet &leftJoin(const DataSet &other, option<std::string> leftColumn, option<std::string> rightColumn,
                                  option<std::string> leftPrefix, option<std::string> leftSuffix,
                                  option<std::string> rightPrefix, option<std::string> rightSuffix) override {return *this; }

        virtual DataSet& unique() override { return *this; }
        virtual DataSet& aggregate(const UDF& aggCombine, const UDF& aggUDF, const Row& aggInitial) override { return *this; }
        virtual DataSet& aggregateByKey(const UDF& aggCombine, const UDF& aggUDF, const Row& aggInitial, const std::vector<std::string> &keyColumns) override { return *this; }

        //virtual void show(const int64_t numRows=-1, std::ostream& os=std::cout) override;
        virtual std::shared_ptr<ResultSet> collect(std::ostream& os) override;

        // take / collect will print out the error only
        virtual std::shared_ptr<ResultSet> take(int64_t numElements, std::ostream& os) override;

        //virtual void show(const int64_t numRows=-1, std::ostream& os=std::cout) override;
        virtual std::vector<Row> collectAsVector(std::ostream& os) override;

        // take / collect will print out the error only
        virtual std::vector<Row> takeAsVector(int64_t numElements, std::ostream& os) override;

        DataSet& cache(const Schema::MemoryLayout& memoryLayout, bool storeSpecialized) override {
            return *this;
        }
    };
}

#endif //TUPLEX_EMPTYDATASET_H