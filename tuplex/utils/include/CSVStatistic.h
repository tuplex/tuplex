//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CSVSTATISTIC_H
#define TUPLEX_CSVSTATISTIC_H

#include "CSVUtils.h"
#include <vector>
#include "TypeSystem.h"

// a maximum of 7 chars (decoded) should be used for maximum cells.
#define SMALL_CELL_MAX_SIZE 7

namespace tuplex {
    class CSVStatistic {
    private:
        // from config inherited defaults
        std::vector<char> _separators;
        std::vector<char> _comments;
        char _quotechar;
        char _escapechar;
        size_t _maxDetectionMemory;
        size_t _maxDetectionRows;

        // user specified info
        double _threshold;

        size_t _rowCount; // how many detected rows.
        size_t _bufSize; // size of the buffer over which estimation was done
        // (good for interpolation #rows over bigger files)

        // estimated data
        char _delimiter;
        bool _header;
        python::Type _rowType;
        python::Type _rowSuperType;

        std::vector<std::string> _columnNames;
        std::vector<std::string> _null_values;
        std::vector<size_t> _smallCellIndices;

        python::Type mapCSVTypeToPythonType(const CSVType& csvtype);
    public:
        CSVStatistic() = delete;

        CSVStatistic(const std::vector<char>& separators,
                const std::vector<char>& comments,
                const char quotechar,
                size_t maxDetectionMemory,
                size_t maxDetectionRows,
                double threshold,
                const std::vector<std::string>& null_values=std::vector<std::string>{""}) : _separators(separators),
                                                  _comments(comments), _quotechar(quotechar),
                                                  _escapechar(0), _maxDetectionMemory(maxDetectionMemory),
                                                  _maxDetectionRows(maxDetectionRows), _threshold(threshold),
                                                  _delimiter(','),
                                                  _header(false),
                                                  _rowType(python::Type::UNKNOWN), _rowCount(0), _bufSize(0), _null_values(null_values) {
            assert(_separators.size() > 0);
            assert(_comments.size() > 0);
            assert(_maxDetectionMemory > _maxDetectionRows);
        }

        /*!
         * estimate types based on a buffer of raw ascii
         * @param start pointer to buffer
         * @param size size of buffer
         * @param hasUTF8 whether UTF8 chars are present or not
         * @param separator optionally predefine a spearator (else it will be inferred)
         * @param disableNullColumns whether to allow columns to be inferred as NULL or not.
         */
        void estimate(const char *start,
                      const size_t size,
                      tuplex::option<bool> hasUTF8=tuplex::option<bool>::none,
                      tuplex::option<char> separator=tuplex::option<char>::none,
                      bool disableNullColumns=false);

        python::Type type() { return _rowType; } // normal case type
        python::Type superType() { return _rowSuperType; } // non specialized type

        char quotechar() const { return _quotechar; }
        char delimiter() const { return _delimiter; }

        bool hasHeader() const { return _header; }

        std::vector<std::string> columns() const { return _columnNames; }

        size_t rowCount() const { return _rowCount; }
        size_t estimationBufferSize() const { return _bufSize; }

        /*!
         * return list of column indices where small cells have been detected.
         * @return vector of indices (ascending) or empty vector
         */
        std::vector<size_t> smallCellCandidates() const { return _smallCellIndices; }
    };
}

#endif //TUPLEX_CSVSTATISTIC_H