//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <EmptyDataset.h>

namespace tuplex {
    std::shared_ptr<ResultSet> EmptyDataset::take(size_t topLimit, size_t bottomLimit, std::ostream &os) {
        return std::make_shared<ResultSet>();
    }

    std::vector<Row> EmptyDataset::takeAsVector(size_t numElements, std::ostream &os) {
        return std::vector<Row>{};
    }

    std::shared_ptr<ResultSet> EmptyDataset::collect(std::ostream &os) {
        return take(0, 0, os);
    }

    std::vector<Row> EmptyDataset::collectAsVector(std::ostream &os) {
        return takeAsVector(0, os);
    }

    void EmptyDataset::tofile(FileFormat fmt, const URI &uri, const UDF &udf, size_t fileCount, size_t shardSize, const std::unordered_map<std::string, std::string> &outputOptions, size_t limit, std::ostream &os) {
        // nothing todo.
    }
}