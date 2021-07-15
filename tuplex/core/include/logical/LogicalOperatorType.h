//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LOGICALOPERATORTYPE_H
#define TUPLEX_LOGICALOPERATORTYPE_H

namespace tuplex {
    enum class LogicalOperatorType {
        UNKNOWN,
        MAP,
        FILTER,
        SORT,
        TAKE, // i.e. output to python / in memory
        PARALLELIZE, // i.e. input from python
        FILEINPUT,
        RESOLVE,
        IGNORE,
        MAPCOLUMN,
        WITHCOLUMN,
        FILEOUTPUT, // output to file
        JOIN,
        AGGREGATE,
        CACHE // i.e. to materialize intermediates & reuse partitions
    };

    inline bool isExceptionOperator(LogicalOperatorType lot) {
        if(lot == LogicalOperatorType::RESOLVE)
            return true;
        if(lot == LogicalOperatorType::IGNORE)
            return true;
        return false;
    }
}

#endif //TUPLEX_LOGICALOPERATORTYPE_H