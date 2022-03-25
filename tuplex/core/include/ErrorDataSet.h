//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ERRORDATASET_H
#define TUPLEX_ERRORDATASET_H


#include <DataSet.h>

#include <utility>

namespace tuplex {

    class ErrorDataSet;
    class DataSet;
    class OperationGraph;
    class Context;

    /*!
     * if an error occurs in the DAG of operations, an error data set can be returned.
     * It is a dummy instance that holds an error message merely.
     */
    class ErrorDataSet : public DataSet {
    private:
        std::string _error;
    public:
        ErrorDataSet(std::string error) : DataSet(), _error(std::move(error)) {
        }

        virtual ~ErrorDataSet() {}

        bool isError() const override { return true; }
        bool isEmpty() const override { return false; }

        std::string getError() const { return _error; }

        // need to overwrite all the functions of DataSet!
        DataSet& map(const UDF& udf) override { return *this; }
        DataSet& filter(const UDF& udf) override {return *this; }

        DataSet& resolve(const ExceptionCode& ec, const UDF& udf) override { return *this; }
        DataSet& ignore(const ExceptionCode& ec) override { return *this; }
        DataSet& mapColumn(const std::string& columnName, const UDF& udf) override { return *this; }
        DataSet& selectColumns(const std::vector<std::string>& columnNames) override { return *this; }
        DataSet& selectColumns(const std::vector<size_t>& columnIndices) override { return *this; }
        DataSet& renameColumn(const std::string& oldColumnName, const std::string& newColumnName) override { return *this; }
        DataSet& renameColumn(int, const std::string& newColumnName) override { return *this; }
        DataSet& withColumn(const std::string& columnName, const UDF& udf) override { return *this; }
        void tofile(FileFormat fmt,
                            const URI& uri,
                            const UDF& udf,
                            size_t fileCount,
                            size_t shardSize,
                            const std::unordered_map<std::string, std::string>& outputOptions,
                            size_t limit,
                            std::ostream& os) override;

        DataSet& join(const DataSet& other, option<std::string> leftColumn, option<std::string> rightColumn,
                      option<std::string> leftPrefix, option<std::string> leftSuffix,
                      option<std::string> rightPrefix, option<std::string> rightSuffix) override  {
            // always empty
            return *this;
        }

        DataSet& aggregate(const UDF& aggCombine, const UDF& aggUDF, const Row& aggInitial) override { return *this; }
        DataSet& aggregateByKey(const UDF& aggCombine, const UDF& aggUDF, const Row& aggInitial, const std::vector<std::string> &keyColumns) override { return *this; }

        DataSet &leftJoin(const DataSet &other, option<std::string> leftColumn, option<std::string> rightColumn,
                                  option<std::string> leftPrefix, option<std::string> leftSuffix,
                                  option<std::string> rightPrefix, option<std::string> rightSuffix) override {
            return *this;
        }

        DataSet& cache(const Schema::MemoryLayout& memoryLayout, bool storeSpecialized) override {
            return *this;
        }



        DataSet& unique() override { return *this; }

        //virtual void show(const int64_t numRows=-1, std::ostream& os=std::cout) override;
        std::shared_ptr<ResultSet> collect(std::ostream& os) override;

        // take / collect will print out the error only
        std::shared_ptr<ResultSet> take(size_t topLimit, size_t bottomLimit, std::ostream& os) override;

        //virtual void show(const int64_t numRows=-1, std::ostream& os=std::cout) override;
        std::vector<Row> collectAsVector(std::ostream& os) override;

        // take / collect will print out the error only
        std::vector<Row> takeAsVector(size_t numElements, std::ostream& os) override;
    };
}


#endif //TUPLEX_ERRORDATASET_H