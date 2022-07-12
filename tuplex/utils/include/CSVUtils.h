//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CSVUTILS_H
#define TUPLEX_CSVUTILS_H

#include <string>
#include <vector>
#include <ExceptionCodes.h>
#include <optional.h>
#include <sstream>
#include <limits>
#include <Row.h>
#include "Utils.h"

namespace tuplex {

    // define after order (narrower higher)
    enum class CSVType {
        NULLVALUE=100,
        STRING=0,         // the regular case
        JSONSTRING=1,     // contains json
        BOOLEAN=1000,        // i.e. 0,1, True, False, true, false
        INTEGER=200,        // i.e. -100, 42, 0, 98
        FLOAT=10,          // i.e. -0.9887, 8375.
        POSITIVEINTEGER=250,// same as integer but without -
        POSITIVEFLOAT=15   // same as float but without -
        //@TODO include DATETIME formats...
        //@TODO floats without scientific notation?
    };

    extern std::string csvtypeToString(const CSVType &csvt);

    // simple csv function used for inference
    extern ExceptionCode parseRow(const char *start,
                                  const char *end,
                                  std::vector<std::string> &results,
                                  std::size_t &numParsedBytes,
                                  char delimiter = ',',
                                  char quotechar = '"',
                                  bool includeQuotes = false);

    /*!
     * parse a buffer into rows. Only complete lines are used. Type matching is performed according to type rules.
     * @param start
     * @param end
     * @param null_values which strings to interpret as None
     * @param delimiter
     * @param quotechar
     * @return vector of rows, potentially with different types!
     */
    extern std::vector<tuplex::Row> parseRows(const char *start, const char *end, const std::vector<std::string>& null_values, char delimiter=',', char quotechar='"');

    /*!
     * convert a vector of string cells to tuplex Row format, by simply performing type matching for each cell.
     * @param cells
     * @param null_values which strings to interpret as None
     * @return tuplex Row
     */
    extern tuplex::Row cellsToRow(const std::vector<std::string>& cells, const std::vector<std::string>& null_values);

    // @Todo: add support for comments, i.e. list of symbols [#, ~]
    // for ignoring lines starting with that
    // infer separator from sample
    extern char inferSeparator(const char *start,
                               const char *end,
                               std::vector<char> candidates = {',', ';', '\t', '|', ' '},
                               char quotechar = '"',
                               const size_t maxDetecionRows = std::numeric_limits<size_t>::max());

    extern size_t detectColumnCount(const char *start,
                                    const char *end,
                                    char delimiter,
                                    char quotechar = '"');

    extern std::vector<CSVType> detectTypes(const char *start,
                                            const char *end,
                                            char delimiter,
                                            size_t columnCount,
                                            char quotechar = '"',
                                            const size_t maxDetecionRows = std::numeric_limits<size_t>::max(),
                                            bool skipHeader = true,
                                            const std::vector<std::string>& null_values=std::vector<std::string>{""});


    extern bool detectHeader(const char *start,
                             const char *end,
                             char delimiter,
                             size_t columnCount,
                             char quotechar = '"');

    struct CSVColumnStat {
        double null_fraction; // [0, 1] fraction of null values seen in this particular column
        option<bool> quoted; // quoted?
        option<bool> startWhitespace; // whitespace in front of content?
        option<bool> endWhitespace; // whitespace at end?
        option <size_t> length; // typical length (0 if not determinable)
        size_t maxDequotedCellSize; // max dequoted cell size in bytes (incl. null terminator!)
        size_t rowCount; // how many rows.
        size_t bufSize; // size of buffer over which estimation occurred

        double dmin;
        double dmax;
        std::vector<double> dfrequentValues;

        int64_t imin;
        int64_t imax;
        std::vector<int64_t> ifrequentValues;

        std::vector<std::string> sfrequentValues;

        // --> quoted/unquoted?
        // --> whitespace after separator, after value?
        // --> length?
        // --> value range
        // --> min/max
        // -->

        std::string toString() {
            std::stringstream ss;
            ss << "nullable: " << null_fraction * 100 << "%\n";
            ss << "quoted: " << quoted.value_or(false) << "\n";
            ss << "whitespace at start: " << startWhitespace.value_or(false) << "\n";
            ss << "whitespace at end: " << endWhitespace.value_or(false) << "\n";
            ss << "length: " << length.value_or(-1) << "\n";
            return ss.str();
        }

    };

    extern std::vector<CSVColumnStat>
    computeColumnStats(const char *start, const char *end, char delimiter, const std::vector<CSVType> &types,
                       bool ignoreHeader, double threshold, char quotechar,
                       const std::vector<std::string> &null_values);


    enum class CSVState {
        UNKNOWN,
        RECORD_START,
        UNQUOTED_FIELD,
        QUOTED_FIELD
    };

    struct CSVChunkInfo {
        CSVState firstFieldState;
        size_t offset;
        bool valid;
    };


    // offset is from search start offset, i.e. the next row starts at searchStartOffset + offset!
    extern CSVChunkInfo findLineStart(const char *buffer, size_t buffer_size, size_t searchStartOffset, size_t expectedColumnCount, char delimiter=',', char quotechar='"');

    /*!
     * finds start of csv line in buffer, returns -1 if no such start was found
     */
    extern int
    csvFindLineStart(const char *buffer, size_t buffer_size, size_t expectedColumnCount, char delimiter = ',',
                     char quotechar = '"');

    /*!
     * number of bytes till NEWLINE start
     * @param buffer
     * @param buffer_size
     * @return offset in bytes till new line start
     */
    extern int csvOffsetToNextLine(const char *buffer, size_t buffer_size, char delimiter = ',', char quotechar = '"');

    /*!
     * creates string (without newline delimiter!) of header
     * @param columns
     * @param separator
     * @return string separated with separator
     */
    extern std::string csvToHeader(const std::vector<std::string> &columns, char separator = ',');

    inline std::string quote(const std::string& str, char delimiter = ',', char quotechar='"') {
        auto q = char2str(quotechar);
        std::string res;
        bool needs_quoting = false;
        for(auto c : str) {
            res += char2str(c);
            if(c == quotechar)
                res += q;
            if(c == delimiter || c == '\n' || c == '\r' || c == quotechar)
                needs_quoting = true;
        }
        return needs_quoting ? q + res + q : str;
    }
}

#endif //TUPLEX_CSVUTILS_H