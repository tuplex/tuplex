//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/FileOutputOperator.h>

namespace tuplex {
    FileOutputOperator::FileOutputOperator(tuplex::LogicalOperator *parent, const tuplex::URI &uri,
                                           const tuplex::UDF &udf, const std::string &name,
                                           const tuplex::FileFormat &fmt,
                                           const std::unordered_map<std::string, std::string> &options, size_t numParts,
                                           size_t splitSize, size_t limit)  : LogicalOperator::LogicalOperator(parent),
                                                                              _uri(uri),
                                                                              _outputPathUDF(udf),
                                                                              _name(name),
                                                                              _fmt(fmt),
                                                                              _options(options),
                                                                              _numParts(numParts),
                                                                              _splitSize(splitSize),
                                                                              _limit(limit) {
        // take schema from parent node
        setSchema(this->parent()->getOutputSchema());
    }

    LogicalOperator *FileOutputOperator::clone() {
        auto copy = new FileOutputOperator(parent()->clone(), _uri, _outputPathUDF,
                _name, _fmt, _options, _numParts, _splitSize, _limit);
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
    }
}