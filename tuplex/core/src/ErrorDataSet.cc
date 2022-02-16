//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ErrorDataSet.h>


namespace tuplex {
    std::vector<Row> ErrorDataSet::takeAsVector(int64_t numElements, std::ostream &os) {
        // return empty vector and print err message
        Logger::instance().logger("core").error(this->_error);

        return std::vector<Row>();
    }

    std::vector<Row> ErrorDataSet::collectAsVector(std::ostream &os) {
        return takeAsVector(0, os);
    }

    std::shared_ptr<ResultSet> ErrorDataSet::take(int64_t numTop, int64_t numBottom, std::ostream &os) {
        // return empty vector and print err message
        Logger::instance().logger("core").error(this->_error);

        return std::shared_ptr<ResultSet>(new ResultSet(Schema::UNKNOWN, std::vector<Partition *>()));
    }

    std::shared_ptr<ResultSet> ErrorDataSet::collect(std::ostream &os) {
        return take(0, false, os);
    }

    void
    ErrorDataSet::tofile(enum tuplex::FileFormat fmt, const class tuplex::URI &uri, const class tuplex::UDF &udf,
                         size_t fileCount, size_t shardSize,
                         const std::unordered_map<std::string, std::string> &outputOptions, size_t limit,
                         std::ostream &os) {
        // return empty vector and print err message
        Logger::instance().logger("core").error(this->_error);
    }
}