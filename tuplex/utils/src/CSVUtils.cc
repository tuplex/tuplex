//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <CSVUtils.h>
#include <StringUtils.h>
#include <StatUtils.h>
#include <Logger.h>
#include <map>
#include <MessageHandler.h>
#include <unordered_map>
#include <Utils.h>

#include <boost/algorithm/string.hpp>

namespace tuplex {

    struct StringSpannerFallback
    {
        uint8_t charset_[256];

        StringSpannerFallback(char c1=0, char c2=0, char c3=0, char c4=0)
        {
            ::memset(charset_, 0, sizeof charset_);
            charset_[(unsigned) c1] = 1;
            charset_[(unsigned) c2] = 1;
            charset_[(unsigned) c3] = 1;
            charset_[(unsigned) c4] = 1;
            charset_[0] = 0;
        }

        size_t
        operator()(const char *s)
        //__attribute__((__always_inline__))
        {
//            CSM_DEBUG("bitfield[32] = %d", charset_[32]);
//            CSM_DEBUG("span[0] = {%d,%d,%d,%d,%d,%d,%d,%d}",
//                      s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]);
//            CSM_DEBUG("span[1] = {%d,%d,%d,%d,%d,%d,%d,%d}",
//                      s[8], s[9], s[10], s[11], s[12], s[13], s[14], s[15]);

            auto p = (const unsigned char *)s;
            auto e = p + 16;

            do {
                if(charset_[p[0]]) {
                    break;
                }
                if(charset_[p[1]]) {
                    p++;
                    break;
                }
                if(charset_[p[2]]) {
                    p += 2;
                    break;
                }
                if(charset_[p[3]]) {
                    p += 3;
                    break;
                }
                p += 4;
            } while(p < e);

            return p - (const unsigned char *)s;
        }
    };

    using StringSpanner=StringSpannerFallback;

    ExceptionCode parseRow(const char *start,
                           const char *end,
                           std::vector<std::string> &results,
                           std::size_t &numParsedBytes,
                           const char delimiter,
                           const char quote_char,
                           bool includeQuotes) {
        // updated function (without SSE & thread safe...)
        // note: there is no check for eof '\0' in here!!

        using namespace std;
        // start in unquoted field
        // or start in quoted field -> should be configurable

        // after state transitions from http://delivery.acm.org/10.1145/3320000/3319898/p883-ge.pdf?ip=128.148.231.11&id=3319898&acc=OPEN&key=7777116298C9657D%2EDD490426617BA903%2E4D4702B0C3E38B35%2E6D218144511F3437&__acm__=1563227977_05a123459d06ee7f0c17d52b6c3c9121
        //                     quote   comma   newline     other
        // R (Record start)    Q       F       R           U
        // F (Field start)     Q       F       R           U
        // U (Unquoted field)  -       F       R           U
        // Q (Quoted field)    E       Q       Q           Q
        // E (quoted End)      Q       F       R           -

        const char *p = start;
        const char *lastStart = p;
        const char *cellStart = nullptr;


        // special case: start == end
        if(start == end) {
            numParsedBytes = 0;
            return ExceptionCode::SUCCESS;
        }

        size_t columnCount = 0;


        // State 1: R (Record start)    Q       F       R           U
record_start:
        columnCount = 0;
        if(p >= end)
            goto parse_done;
        lastStart = p;

        if (*p == quote_char) {
            columnCount++;
            ++p;
            goto quoted_field;
        } else if (*p == delimiter) {
            columnCount++;
            results.push_back(fromCharPointers(lastStart, p));
            ++p;
            goto field_start;
        } else if (*p == '\n' || *p == '\r') {
            // no field serialization, instead greedy skip of empty lines at start!
            ++p;
            goto record_start; // only here go back to record start
        } else {
            columnCount++;
            ++p;
            goto unquoted_field;
        }

// State 2: F (Field start)     Q       F       R           U
field_start:
        if(p >= end) {
            // do not serialize here! i.e. , or \n dont get serialized as field!
            goto parse_done;
        }
        lastStart = p;
        if (*p == quote_char) {
            ++p;
            goto quoted_field;
        } else if (*p == delimiter) {
            results.push_back(fromCharPointers(lastStart, p));
            ++p;
            goto field_start;
        } else if (*p == '\n' || *p == '\r') {
            results.push_back(fromCharPointers(lastStart, p));
            ++p;
            // goto record_start;
            // new record starts, here means parse done!
            goto parse_done;
        } else {
            ++p;
            goto unquoted_field;
        }

// State 3: U (Unquoted field)  -       F       R           U
unquoted_field:
        if(p >= end) {
            results.push_back(fromCharPointers(lastStart, p));
            goto parse_done;
        }
        if (*p == quote_char) {
            ++p;
            goto parse_error;
        } else if (*p == delimiter) {
            columnCount++;
            results.push_back(fromCharPointers(lastStart, p));
            ++p;
            goto field_start;
        } else if (*p == '\n' || *p == '\r') {
            results.push_back(fromCharPointers(lastStart, p));
            ++p;
            // goto record_start;
            // new record starts, here means parse done!
            goto parse_done;
        } else {
            ++p;
            goto unquoted_field;
        }

// State 4: Q (Quoted field)    E       Q       Q           Q
        quoted_field:
        if(p >= end) {
            auto cell = fromCharPointers(lastStart, p);
            results.push_back(includeQuotes ? cell : dequote(cell.substr(1, cell.length() - 2), quote_char));
            goto parse_done;
        }
        if (*p == quote_char) {
            ++p;
            goto quoted_end;
        } else if (*p == delimiter) {
            ++p;
            goto quoted_field;
        } else if (*p == '\n' || *p == '\r') {
            ++p;
            goto quoted_field;
        } else {
            ++p;
            goto quoted_field;
        }

// State 5: E (quoted End)      Q       F       R           -
        quoted_end:
        if(p >= end) {
            auto cell = fromCharPointers(lastStart, p);
            results.push_back(includeQuotes ? cell : dequote(cell.substr(1, cell.length() - 2), quote_char));
            goto parse_done;
        }
        if (*p == quote_char) {
            ++p;
            goto quoted_field;
        } else if (*p == delimiter) {
            columnCount++;
            auto cell = fromCharPointers(lastStart, p);
            results.push_back(includeQuotes ? cell : dequote(cell.substr(1, cell.length() - 2), quote_char));
            ++p;
            goto field_start;
        } else if (*p == '\n' || *p == '\r') {
            auto cell = fromCharPointers(lastStart, p);
            results.push_back(includeQuotes ? cell : dequote(cell.substr(1, cell.length() - 2), quote_char));
            ++p;
            // goto record_start;
            // new record starts, here means parse done!
            goto parse_done;
        } else {
            ++p;
            goto parse_error;
        }

        parse_done:
        numParsedBytes = p - start;
        return ExceptionCode::SUCCESS;

        parse_error:
        numParsedBytes = p - start;
        return ExceptionCode::UNKNOWN; // -1 to signal no compatible run was found
    }



//    // simple csv function used for inference
//    __attribute__((no_sanitize_address)) ExceptionCode parseRow(const char *start,
//                           const char *end,
//                           std::vector<std::string> &results,
//                           std::size_t& numParsedBytes,
//                           char delimiter,
//                           char quotechar,
//                           bool includeQuotes) {
//        char escapechar = 0;
//
//        // empty?
//        if (start == end) {
//            numParsedBytes = 0;
//            return ExceptionCode::SUCCESS;
//        }
//
//        // must be 16 bytes aligned...
//        // check that last bytes are 0 such that all of it 16 byte aligned.
//        auto numalign = (16 - (end - start) % 16) % 16;
//        auto last = end + (16 - (end - start) % 16) % 16;
//        assert(end <= last);
////        // check that between end and last everything is 0
////        for(auto p = end; p != last; ++p) {
////            assert(*p == '\0');
////        }
//
//
//        const char *p = start;
//        const char *cell_start;
//        const char *cell_ptr = 0;
//        int cell_size = 0;
//        size_t rc;
//        bool in_newline_skip = true;
//        bool cell_escaped = false;
//
//        StringSpanner quoted_cell_spanner(quotechar, escapechar);
//        StringSpanner unquoted_cell_spanner(delimiter, '\r', '\n', escapechar);
//
//        newline_skip:
//        in_newline_skip = true;
//        if (p >= end) {
//            numParsedBytes = end - start;
//            return ExceptionCode::CSV_UNDERRUN;
//        }
//
//        if (*p == '\r' || *p == '\n') {
//            ++p;
//            goto newline_skip;
//        }
//
//        cell_begin:
//        in_newline_skip = false;
//        if (p >= end) {
//            numParsedBytes = end - start;
//            return ExceptionCode::CSV_UNDERRUN;
//        }
//        cell_escaped = false;
//        if (*p == '\r' || *p == '\n') {
//            // ???
//            cell_ptr = 0;
//            cell_size = 0;
//
//            // save cell
//            results.push_back(fromCharPointers(cell_start, cell_start));
//            p = p + 1;
//            // new line encountered
//            numParsedBytes = p - start;
//            return ExceptionCode::SUCCESS;
//        } else if (*p == quotechar) {
//            cell_start = ++p;
//            goto in_quoted_cell;
//        } else {
//            cell_start = p;
//            goto in_unquoted_cell;
//        }
//
//        in_quoted_cell:
////        if (p >= end) {
////
////            // check whether line was correctly ended with delimiter...
////            assert(cell_start < end - 1);
////            assert(*(end-1) == quotechar);
////            results.push_back(fromCharPointers(cell_start, end - 1));
////            return p;
////        }
//        if (p >= end) {
//            numParsedBytes = end - start;
//            return ExceptionCode::CSV_UNDERRUN;
//        }
//        rc = quoted_cell_spanner(p);
//        switch (rc) {
//            case 16:
//                p += 16;
//                goto in_quoted_cell;
//            default:
//                p += rc + 1;
//                goto in_escape_or_end_of_quoted_cell;
//        }
//
//        in_escape_or_end_of_quoted_cell:
//        // if p == end is met, cell found!
////        if (p >= end) {
////            // add last cell
////            results.push_back(fromCharPointers(cell_start, p - 1));
////            return p;
////        }
//        if (p >= end) {
//            numParsedBytes = end - start;
//            return ExceptionCode::CSV_UNDERRUN;
//        }
//        if (*p == delimiter) {
//            cell_ptr = cell_start;
//
//            // save cell
//            if(includeQuotes)
//                results.push_back(fromCharPointers(cell_start - 1, p));
//            else
//                results.push_back(fromCharPointers(cell_start, p - 1));
//
//            // materialize cell & goto next one!
//            ++p;
//            goto cell_begin;
//        } else if (*p == '\r' || *p == '\n') {
//            cell_ptr = cell_start;
//
//            // save cell
//            if(includeQuotes)
//                results.push_back(fromCharPointers(cell_start - 1, p));
//            else
//                results.push_back(fromCharPointers(cell_start, p - 1));
//            p = p + 1;
//            numParsedBytes = p - start;
//            return ExceptionCode::SUCCESS;
//        } else {
//            cell_escaped = true;
//            ++p;
//            goto in_quoted_cell;
//        }
//
//
//        in_unquoted_cell:
//        if (p >= end) {
//            numParsedBytes = end - start;
//            return ExceptionCode::CSV_UNDERRUN;
//        }
//        rc = unquoted_cell_spanner(p);
//        switch (rc) {
//            case 16:
//                p += 16;
//                goto in_unquoted_cell;
//            default:
//                p += rc;
//                goto in_escape_or_end_of_unquoted_cell;
//        }
//
//        in_escape_or_end_of_unquoted_cell:
//        if (p >= end) {
//            numParsedBytes = end - start;
//            return ExceptionCode::CSV_UNDERRUN;
//        }
//        if (*p == delimiter) {
//            cell_ptr = cell_start;
//            // save cell
//            results.push_back(fromCharPointers(cell_start, p));
//            ++p;
//            goto cell_begin;
//        } else if (*p == 'r' || *p == '\n') {
//            cell_ptr = cell_start;
//
//            // save cell
//            results.push_back(fromCharPointers(cell_start, p));
//            p = p + 1;
//            numParsedBytes = p - start;
//            return ExceptionCode::SUCCESS;
//        } else {
//            cell_escaped = true;
//            ++p;
//            goto in_unquoted_cell;
//        }
//    }


    char inferSeparator(const char *start,
                        const char *end,
                        std::vector<char> candidates,
                        char quotechar,
                        const size_t maxDetectionRows) {
        using namespace std;

        assert(start <= end);
        assert(candidates.size() > 0);

        auto& logger = Logger::instance().logger("csv inference");

        if(start == end) {
            logger.warn("empty file given for inference. Defaulting to first candidate");
            return candidates[0];
        }

        // check which separator is actually present in stream
        map<char, int> separatorPresence;
        for(auto delimiter : candidates) {
            const char* p = start;
            separatorPresence[delimiter] = 0;
            while(p != end) {
                if(delimiter == *p)
                    separatorPresence[delimiter]++;

                ++p;
            }
        }

        // @Todo: make this faster, by only allowing detection of up to 1,000 rows (should be sufficient to decide)

        // for each separator count how many columns it has
        // be aware of special case 1 rows!
        map<char, RollingStats<int64_t>> separatorStats;
        map<char, int> separatorErrs;
        for(auto delimiter : candidates) {
            auto p = start;
            vector<vector<string>> rows;
            int numErrors = 0;
            // break if max-detection rows are reached
            while(p < end && rows.size() < maxDetectionRows) {
                vector<string> row;
                size_t numBytes = 0;
                auto ec = parseRow(p, end, row, numBytes, delimiter, quotechar);
                if(ec != ExceptionCode::SUCCESS) {
                    numErrors++;
                } else {
                    // only add non-error rows
                    rows.push_back(row);
                }

                if(0 == numBytes)
                    break;
                p += numBytes;


            }

            // compute stats
            RollingStats<int64_t> rs;
            for(auto row: rows) {
                rs.update(row.size());
            }
            separatorStats[delimiter] = rs;
            separatorErrs[delimiter] = numErrors;

//            cout<<"rows detected: "<<rows.size()<<endl;
        }

        // there are several indicators for a good separator
        // each challenge casts one vote for a separator. The one with the most votes wins.
        map<char, uint64_t> votes;
        for(auto sepCan : candidates)
            votes[sepCan] = 0;


        double minStd = numeric_limits<double>::max();
        int64_t minRange = numeric_limits<int>::max();
        int minErrors = numeric_limits<int>::max();
        for(auto delimiter : candidates) {
            // challenge 1: separator with lowest std
            minStd = std::min(minStd, separatorStats[delimiter].std());

            // challenge 2: separator with lowest minmax range of number of columns
            minRange = std::min(minRange, separatorStats[delimiter].max() - separatorStats[delimiter].min());

            // challenge 3: separator with lowest errors
            minErrors = std::min(minErrors, separatorErrs[delimiter]);

            // challenge 4: number of rows > 1

            // challenge 5: mean > 1
        }

        // compute votes
        for(auto sepCan : candidates) {

            // if sep is not present, give 0 votes.
//            if(separatorPresence[sepCan] > 0)
//                votes[sepCan] += 2; // weight presence twice.
            if(separatorPresence[sepCan] == 0) {
                votes[sepCan] = 0;
                continue;
            }

            if(minStd == separatorStats[sepCan].std())
                votes[sepCan]++;
            if(minRange == separatorStats[sepCan].max() - separatorStats[sepCan].min())
                votes[sepCan]++;
            if(minErrors == separatorErrs[sepCan])
                votes[sepCan]++;
            if(separatorStats[sepCan].count() > 1)
                votes[sepCan]++;
            if(separatorStats[sepCan].mean() > 1)
                votes[sepCan]++;
        }

        // select seperator with most votes!
        // if there are multiple ones, default to order given...
        auto selectedSep = candidates[0];
        auto selectedVotes = 0ul;
        for(auto sepCan : candidates) {
            if(votes[sepCan] > selectedVotes) {
                selectedSep = sepCan;
                selectedVotes = votes[sepCan];
            }
        }

        return selectedSep;
    }


    size_t detectColumnCount(const char *start,
                             const char *end,
                             char delimiter,
                             char quotechar) {
        using namespace std;
        // parse all rows.
        // for each detect how many columns there are

        auto p = start;
        vector<vector<string>> rows;
        int numErrors = 0;
        while(p < end) {
            vector<string> row;
            size_t numBytes = 0;
            auto ec = parseRow(p, end, row, numBytes, delimiter, quotechar);
            if(ec != ExceptionCode::SUCCESS) {
                numErrors++;
            } else {
                // only add non-error rows
                // @Todo: ignore comment lines...
                rows.push_back(row);
            }

            if(0 == numBytes)
                break;
            p += numBytes;
        }

        // empty rows?
        if(rows.empty())
            return 0;

        // majority vote
        map<uint32_t, uint32_t> colCountVotes;
        for(auto row : rows) {
            auto colCount = row.size();
            if(colCountVotes.find(colCount) == colCountVotes.end())
                colCountVotes[colCount] = 0;
            colCountVotes[colCount]++;
        }

        // find max, simple majority win!
        // set as default the first row, i.e. header row if present
        uint32_t numCols = rows.front().size();
        uint32_t numVotes = 1;
        for(auto keyval : colCountVotes) {
            if(keyval.second > numVotes) {
                numCols = keyval.first;
                numVotes = keyval.second;
            }
        }

        return numCols;
    }


    std::string csvtypeToString(const CSVType& csvt) {
        switch(csvt) {
            case CSVType::BOOLEAN:
                return "boolean";
            case CSVType ::FLOAT:
                return "float";
            case CSVType ::INTEGER:
                return "integer";
            case CSVType ::JSONSTRING:
                return "json formatted string";
            case CSVType ::STRING:
                return "string";
            case CSVType ::POSITIVEFLOAT:
                return "positive float";
            case CSVType::POSITIVEINTEGER:
                return "positive integer";
            case CSVType::NULLVALUE:
                return "null";
            default:
                return "unknown";
        }
    }

    std::vector<CSVType> allCSVTypes() {
        return {CSVType::NULLVALUE,
            CSVType::BOOLEAN,
                CSVType::FLOAT,
                CSVType::INTEGER,
                CSVType::JSONSTRING,
                CSVType::STRING,
                CSVType::POSITIVEFLOAT,
                CSVType::POSITIVEINTEGER};
    }


    void updateColStat(const std::string& str, std::map<CSVType, std::size_t>& stats, const std::vector<std::string>& null_values) {

        // always strip whitespace first
        bool match_found = false;

        // boolean check should be done via cardinality.
        // i.e. check how many confirm to two corresponding encodings!
        // check if boolean
        if(!str.empty() && isBoolString(str.c_str())) {
            stats[CSVType::BOOLEAN]++;
            match_found = true;
        }

        // check for integer
        if(!str.empty() && isIntegerString(str.c_str())) {
            try {
                auto i = std::stoll(str, nullptr);
                if (i >= 0)
                    stats[CSVType::POSITIVEINTEGER]++;
                stats[CSVType::INTEGER]++;
                match_found = true;
            } catch (...) {}
        }

        // cast to float
        if(!str.empty() && isFloatString(str.c_str())) {

            stats[CSVType::FLOAT]++;
            match_found = true;
            // ASAN error with this code, ignore
            // try {
            //     auto d = std::stod(str, nullptr);
            //     if(d >= 0)
            //         stats[CSVType::POSITIVEFLOAT]++;
            //     stats[CSVType::FLOAT]++;
            //     match_found = true;
            // } catch (...) {}
        }


        // JSON String (starts & ends with {} or [])
        if(str.length() >= 2) {
            std::string stripped = str; // trim whitespace away...
            trim(stripped);
            if((stripped.front() == '{' && stripped.back() == '}') ||
                    (stripped.front() == '[' && stripped.back() == ']')) {
                stats[CSVType::JSONSTRING]++;
                match_found = true;
            }
        }

        // compare null values
        for(auto nv : null_values) {
            if(nv == str) {
                // return, skip NULL values here
                // why? so option type can be enforced for rare values...
                return;

                stats[CSVType::NULLVALUE]++;
                match_found = true;
            }
        }

        // if match found, do not count as string. Else count as string
        if(!match_found)
            stats[CSVType::STRING]++;
    }


    bool detectHeader(const char *start,
                      const char *end,
                      char delimiter,
                      size_t columnCount,
                      char quotechar) {

        // parse first row
        using namespace std;
        assert(columnCount > 0);
        assert(start <= end);

        vector<string> row;
        size_t numBytes = 0;
        auto ec = parseRow(start, end, row, numBytes, delimiter, quotechar);
        if(ExceptionCode::SUCCESS != ec)
            return false;

        // row number matches?
        if(row.size() != columnCount)
            return false;

        // all columns should have type string, then it is a header
        // always strip whitespace first
        for(auto str : row) {
            // check if boolean
            if(str.compare("0") == 0 || str.compare("1") == 0 ||
               boost::algorithm::to_lower_copy(str).compare("true") == 0 ||
               boost::algorithm::to_lower_copy(str).compare("false") == 0)
                return false;

            // check for integer
            if(isIntegerString(str.c_str()) && str.length() > 0)
                return false;

            // cast to float
            if(isFloatString(str.c_str()) && str.length() > 0)
                return false;


            // JSON String (starts & ends with {} or [])
            if(str.length() >= 2) {
                std::string stripped = str; // trim whitespace away...
                trim(stripped);
                if((stripped.front() == '{' && stripped.back() == '}') ||
                   (stripped.front() == '[' && stripped.back() == ']'))
                    return false;
            }
        }

        return true;
    }

    // @Todo: this here is the bottleneck
    // Note: for things like bool <= int <= float
    // it is also an interesting question what the normalcase is
    // i.e. when there are only a few floats but majority is int,
    // shouldn't we rely on integer encoding?
    std::vector<CSVType> detectTypes(const char *start,
                                     const char *end,
                                     char delimiter,
                                     size_t columnCount,
                                     char quotechar,
                                     const size_t maxDetectionRows,
                                     bool skipHeader,
                                     const std::vector<std::string>& null_values) {
        using namespace std;
        assert(columnCount > 0);
        assert(start <= end);

        // clamp to string length (avoid '0' chars)
        end = start + std::min(strlen(start),  (unsigned long)(end - start));

        // init stats
        vector<map<CSVType, size_t>> stats;
        for(int i = 0; i < columnCount; ++i) {
            map<CSVType, size_t> colStat;
            for (auto type : allCSVTypes()) {
                colStat[type] = 0;
            }
            stats.push_back(colStat);
        }

        // parse rows. For each row, determine type & add to stats
        auto p = start;
        size_t numRows = 0;

        // special case: for booleans, type using pairs. Else, values get thrown off.
        vector<unordered_map<string, size_t>> cell_unique_counts; // case insensitive lower value strings...
        while(*p != '\0' && p < end && numRows < maxDetectionRows) {
            vector<string> row;
            size_t numBytes = 0;
            auto ec = parseRow(p, end, row, numBytes, delimiter, quotechar);

            // add unique value counts!
            if(cell_unique_counts.size() < row.size()) {
                // add zero arrays!
                for(int i = cell_unique_counts.size(); i < row.size(); ++i)
                    cell_unique_counts.push_back(unordered_map<string, size_t>());
            }
            for(int i = 0; i < row.size(); ++i) {
                auto str = boost::algorithm::to_lower_copy(row[i]);
                auto it = cell_unique_counts[i].find(str);
                if(it == cell_unique_counts[i].end())
                    cell_unique_counts[i][str] = 0;
                cell_unique_counts[i][str]++;
            }

            // skip header if configured
            if(numRows == 0 && skipHeader) {
                numRows++;
                p += numBytes;
                continue;
            }

            // skip rows that don't match or produced an error
            if(ec == ExceptionCode::SUCCESS) {

                // special case: single str, could be a comment
                // @TODO: better support for comments in csv files...

                // this code is for handling comemnt case...
                size_t oldFirstColStringStat = 0;
                if(row.size() == 1) {
                    oldFirstColStringStat = stats[0][CSVType::STRING];
                }

                // analyze each column, include incomplete rows...
                for(int i = 0; i < std::min(row.size(), columnCount); ++i) {
                    updateColStat(row[i], stats[i], null_values);
                }

                // this code is for handling comment case
                if(row.size() == 1) {
                    // do not count string
                    if(stats[0][CSVType::STRING] == oldFirstColStringStat + 1)
                        stats[0][CSVType::STRING]--;
                }
            }

            if(0 == numBytes) {
                cout<<"error...."<<endl;
                break;
            }
            p += numBytes;
            numRows++;
        }

        // detect boolean separate, because it can easily become majority else!
        assert(booleanTrueStrings().size() == booleanFalseStrings().size());
        for(int i = 0; i < std::min(stats.size(), cell_unique_counts.size()); ++i) {
            size_t bool_pair_max_count = 0;
            for(int k = 0; k < booleanFalseStrings().size(); ++k) {
                // check if corresponding pair is in there!
                auto true_str = boost::algorithm::to_lower_copy(booleanTrueStrings()[k]);
                auto false_str = boost::algorithm::to_lower_copy(booleanFalseStrings()[k]);
                // both in there (case insensitive?)
                auto t_it = cell_unique_counts[i].find(true_str);
                auto f_it = cell_unique_counts[i].find(false_str);
                if(t_it != cell_unique_counts[i].end() && f_it != cell_unique_counts[i].end()) {
                    bool_pair_max_count = std::max(bool_pair_max_count, t_it->second + f_it->second);
                }
            }
            stats[i][CSVType::BOOLEAN] = bool_pair_max_count;
        }


//        // debug: print out stats
//        cout<<"total size: "<<sizeToMemString(end-start)<<endl;
//        cout<<"total: "<<numRows<<" rows"<<endl<<endl;
//        for(int i = 0; i < columnCount; ++i) {
//            cout<<"column "<<(i+1)<<endl;
//            for(auto& t : allCSVTypes())
//                cout<<csvtypeToString(t)<<": "<<stats[i][t]<<" votes"<<endl;
//            cout<<endl;
//        }

        // majority vote
        std::vector<CSVType> types;
        for(int i = 0; i < columnCount; ++i) {
            auto maxVotes = 0;
            auto type = CSVType::STRING;

            auto stat =  mapToVector(stats[i]);
            std::stable_sort(stat.begin(), stat.end(), [](const std::pair<CSVType, size_t>& a, const std::pair<CSVType, size_t>& b) {
                // same counts? adjust! => prefer narrower type because counts are contained within each other

                if(a.second == b.second) {
                    return (int)a.first >= (int)b.first;
                } else
                  return a.second > b.second;
            });

            // take first as type
            assert(!stat.empty());
            if(stat.front().second != 0)
                type = stat.front().first;

            // no single voting => NULL value column
            // ==> treat as CSV String value...
            types.push_back(type);
        }

        return types;
    }

    template<typename T> std::map<T, size_t> histogram(const std::vector<T>& data) {
        using namespace std;
        map<T, size_t> m;
        for(auto const el : data) {
            if(m.find(el) == m.end())
                m[el] = 0;
            m[el]++;
        }
        return m;
    }

    bool bhist_maj(const std::map<bool, size_t>& m) {
        if(m.size() == 1)
            return m.find(true) != m.end();
        else
            return m.at(true) > m.at(false) ? true : false;
    }

    template<typename T> T hist_maj(const std::map<T, size_t>& m, double minSupport=0.0) {
        T maxel = -1;

        size_t total = 0;
        for(auto el : m)
            total += el.second;

        size_t support = 0;
        for(auto el : m) {
            if(el.second > support
               && el.second > ceil(minSupport * total)) {
                maxel = el.first;
                support = el.second;
            }
        }
        return maxel;
    }

    class CountStat {
    private:
        size_t _rowCount;
        size_t _columnCount;
        size_t* _counts;
        bool* _flagged;
    public:
        CountStat(size_t rowCount, size_t columnCount) : _rowCount(rowCount),
                                                         _columnCount(columnCount) {
            _counts = new size_t[_rowCount * _columnCount];
            memset(_counts, 0, sizeof(size_t) * _rowCount * _columnCount);
            _flagged = new bool[_rowCount * _columnCount];
            memset(_flagged, 0, sizeof(bool) * _rowCount * _columnCount);
        }

        ~CountStat() {
            if(_counts)
                delete [] _counts;
            _counts = nullptr;
            if(_flagged)
                delete [] _flagged;
            _flagged = nullptr;
        }

        inline void inc(size_t row, size_t col) {
            _flagged[row + col * _rowCount] = true;
            _counts[row + col * _rowCount]++;
        }
        inline void set(size_t row, size_t col, size_t cnt) {
            _flagged[row + col * _rowCount] = true;
            _counts[row + col * _rowCount] = cnt;
        }

        inline size_t get(size_t row, size_t col) const {
            return _counts[row + col * _rowCount];
        }

        /*!
         * returns the parameter that is the most likely
         * @param col for which column
         * @param fraction optional to put out the support fraction
         * @return majority vote winner
         */
        size_t rowMajority(size_t col, double* fraction=nullptr) {
            using namespace std;
            unordered_map<size_t, size_t> hash;
            auto istart = col * _rowCount;
            auto iend = (col + 1) * _rowCount;
            for(auto i = istart; i < iend; ++i) {
                auto el = _counts[i];
                if(hash.find(el) == hash.end())
                    hash[el] = 0;
                hash[el]++;
            }

            // max
            size_t max_support = 0;
            size_t res = 0;
            for(auto it : hash) {
                if(it.second > max_support) {
                    max_support = it.second;
                    res = it.first;
                }
            }

            if(fraction) {
                *fraction = max_support / (1.0 * _rowCount);
            }
            return res;
        }

        /*!
         * retrieve maximum value across all rows
         * @param col for which column to retrieve the value
         * @return maximum value or 0 for empty stat
         */
        size_t rowMaximum(size_t col) {
            size_t m = 0;
            for(size_t i = 0; i < _rowCount; ++i) {
                auto val = get(i, col);
                if(val > m)
                    m = val;
            }
            return m;
        }


        double support(size_t col) {
            size_t implicitCounts = 0;
            auto istart = col * _rowCount;
            auto iend = (col + 1) * _rowCount;
            for(auto i = istart; i < iend; ++i)
                implicitCounts += _flagged[i];
            return implicitCounts / (1.0 * _rowCount);
        }
    };

    std::vector<CSVColumnStat>
    computeColumnStats(const char *start, const char *end, char delimiter, const std::vector<CSVType> &types,
                       bool ignoreHeader, double threshold, char quotechar, const std::vector<std::string>& null_values) {
        using namespace std;

        auto columnCount = types.size();
        auto rowCount = 0;

        std::vector<std::string> nv = null_values;
        // if(nv.empty())
        //    nv.emplace_back("");

        // speed up things by preallocating,
        // store pointers to cells later...
        auto p = start;
        while(p < end) {
            vector<string> row;
            size_t numBytes = 0;
            auto ec = parseRow(p, end, row, numBytes, delimiter, quotechar);
            if(0 == numBytes)
                break;
            p += numBytes;
            rowCount++;
        }

        // for each row add, for each depending on type infer stats
        p = start;
        unsigned int j = 0;

        // skip first row
        if(ignoreHeader) {
            vector<string> row;
            size_t numBytes = 0;
            auto ec = parseRow(p, end, row, numBytes, delimiter, quotechar, true);
            p += numBytes;

            rowCount--;
        }

        // @Todo: deal with special case here...
        assert(rowCount > 0 && columnCount > 0);

        CountStat nullable_stats(1, columnCount);
        CountStat quoted_stats(rowCount, columnCount);
        CountStat startws_stats(rowCount, columnCount);
        CountStat endws_stats(rowCount, columnCount);
        CountStat length_stats(rowCount, columnCount);

        while(p < end) {
            vector<string> row;
            size_t numBytes = 0;
            auto ec = parseRow(p, end, row, numBytes, delimiter, quotechar, true);

            // skip rows that don't match or produced an error
            if(ec == ExceptionCode::SUCCESS && row.size() == columnCount) {
                // analyze each column
                for(unsigned int i = 0; i < columnCount; ++i) {
                    auto type = types[i];

                    string rtrimmed = row[i];
                    string ltrimmed = row[i];
                    rtrim(rtrimmed);
                    ltrim(ltrimmed);

                    bool isquoted = false;

                    // update general stats only if there is not a null value
                    if(row[i].length() > 0) {
                        length_stats.set(j, i, row[i].length());
                        startws_stats.set(j, i, static_cast<size_t>(rtrimmed.length() < row[i].length()));
                        endws_stats.set(j, i, static_cast<size_t>(ltrimmed.length() < row[i].length()));
                        if(ltrimmed.length() > 0 && rtrimmed.length() > 0)
                            isquoted = ltrimmed.front() == quotechar && rtrimmed.back() == quotechar;

                        quoted_stats.set(j, i, static_cast<size_t>(isquoted));
                    }
                    // general stats
                    // compare to all given null values
                    // ==> dequote string for that!
                    string dequoted = isquoted ? dequote(row[i].substr(1, row[i].length() - 2), quotechar) : row[i];
                    auto it = std::find(nv.begin(), nv.end(), dequoted);
                    if(it != nv.end())
                        nullable_stats.inc(0, i);

                    // type stats are yet missing
                    // @Todo
                    switch(type) {
                        case CSVType::BOOLEAN: {
                            // not necessary...
                            break;
                        }
                        case CSVType::STRING: {
                            break;
                        }
                        case CSVType::POSITIVEFLOAT:
                        case CSVType::FLOAT: {
                            // cast to float & compute stats

                            break;
                        }
                        case CSVType::POSITIVEINTEGER:
                        case CSVType::INTEGER: {
                                    break;
                        }
                        case CSVType::JSONSTRING: {
                                    break;
                        }
                        case CSVType::NULLVALUE: {
                            break;
                        }
                        default:
                            assert(false);
                    }
                }
            }

            if(0 == numBytes)
                break;
            p += numBytes;
            j++;
        }

        size_t estimationBufferSize = p - start;

        vector<CSVColumnStat> stats;
        for(unsigned i = 0; i < columnCount; ++i) {
//            stringstream ss;
//            ss<<"Column "<<(i+1)<<"\n";
//
//            double support = 0.0;
//            ss<<"nullable: "<<nullable_stats.rowMajority(i, &support)<<" support: "<<support<<"\n";
//            ss<<"quoted: "<<quoted_stats.rowMajority(i, &support)<<" support: "<<support<<" implicit: "<<quoted_stats.support(i)<<"\n";
//            ss<<"whitespace start: "<<startws_stats.rowMajority(i, &support)<<" support: "<<support<<"\n";
//            ss<<"whitespace end: "<<endws_stats.rowMajority(i, &support)<<" support: "<<support<<"\n";
//            ss<<"length: "<<length_stats.rowMajority(i, &support)<<" support: "<<support<<"\n";
//
//            cout<<ss.str()<<endl;


            // set option only iff threshold is met
            double support = 0.0;
            CSVColumnStat stat;
            stat.null_fraction =  nullable_stats.get(0, i) / (1.0 * rowCount); // fraction of NULL values

            bool quoted_res = static_cast<bool>(quoted_stats.rowMajority(i, &support));
            support /= std::max(quoted_stats.support(i), 1.0);
            stat.quoted = support >= threshold ? option<bool>(quoted_res) : option<bool>::none;

            size_t length_res = length_stats.rowMajority(i, &support);
            support /= std::max(length_stats.support(i), 1.0);
            stat.length = support >= threshold ? option<size_t>(length_res) : option<size_t>::none;

            stat.rowCount = rowCount;
            stat.bufSize = estimationBufferSize;
            stat.maxDequotedCellSize = length_stats.rowMaximum(i);
            stats.push_back(stat);
        }

        return stats;
    }

    int csvOffsetToNextLine(const char *buffer, size_t buffer_size, char delimiter, char quotechar) {
        // note: there is no check for eof '\0' in here!!

        using namespace std;
        // start in unquoted field
        // or start in quoted field -> should be configurable

        // after state transitions from http://delivery.acm.org/10.1145/3320000/3319898/p883-ge.pdf?ip=128.148.231.11&id=3319898&acc=OPEN&key=7777116298C9657D%2EDD490426617BA903%2E4D4702B0C3E38B35%2E6D218144511F3437&__acm__=1563227977_05a123459d06ee7f0c17d52b6c3c9121
        //                     quote   comma   newline     other
        // R (Record start)    Q       F       R           U
        // F (Field start)     Q       F       R           U
        // U (Unquoted field)  -       F       R           U
        // Q (Quoted field)    E       Q       Q           Q
        // E (quoted End)      Q       F       R           -

        const char *p = buffer;
        const char *lastStart = p;
        const char *end = buffer + buffer_size;


        // State 1: R (Record start)    Q       F       R           U
record_start:
        if(p >= end)
            goto parse_done;
        lastStart = p;
        if (*p == quotechar) {
            ++p;
            goto quoted_field;
        } else if (*p == delimiter) {
            ++p;
            goto field_start;
        } else if (*p == '\n' || *p == '\r') {
            ++p;
            return p - lastStart;
        } else {
            ++p;
            goto unquoted_field;
        }

// State 2: F (Field start)     Q       F       R           U
        field_start:
        if(p >= end)
            goto parse_done;
        if (*p == quotechar) {
            ++p;
            goto quoted_field;
        } else if (*p == delimiter) {
            ++p;
            goto field_start;
        } else if (*p == '\n' || *p == '\r') {
            ++p;
            return p - lastStart;
        } else {
            ++p;
            goto unquoted_field;
        }

// State 3: U (Unquoted field)  -       F       R           U
        unquoted_field:
        if(p >= end)
            goto parse_done;
        if (*p == quotechar) {
                ++p; goto parse_error;
        } else if (*p == delimiter) {
                ++p; goto field_start;
        } else if (*p == '\n' || *p == '\r') {
                ++p; return p - lastStart;
        } else {
                ++p; goto unquoted_field;
            }

// State 4: Q (Quoted field)    E       Q       Q           Q
        quoted_field:
        if(p >= end)
            goto parse_done;
        if (*p == quotechar) {
                ++p; goto quoted_end;
        } else if (*p == delimiter) {
                ++p; goto quoted_field;
        } else if (*p == '\n' || *p == '\r') {
                ++p; goto quoted_field;
        } else {
                ++p; goto quoted_field;
            }

// State 5: E (quoted End)      Q       F       R           -
        quoted_end:
        if(p >= end)
            goto parse_done;
        if (*p == quotechar) {
                ++p; goto quoted_field;
        } else if (*p == delimiter) {
                ++p; goto field_start;
        } else if (*p == '\n' || *p == '\r') {
                ++p; return p - lastStart;
        } else {
                ++p; goto parse_error;
            }

        parse_done:
        // check if expected number was reached
        // compare whether current column count matches expected one, if so return offset!
        return p - lastStart;

        parse_error:
        return -1;
    }




    int csvFindStartOffset(const char *buffer, size_t buffer_size, size_t expectedColumnCount, CSVState state=CSVState::RECORD_START, size_t *bytesRead=nullptr, const char delimiter=',', const char quote_char='"') {

        // note: there is no check for eof '\0' in here!!

        assert(expectedColumnCount > 0);

        if(bytesRead)
            *bytesRead = 0;

        using namespace std;
        // start in unquoted field
        // or start in quoted field -> should be configurable

        // after state transitions from http://delivery.acm.org/10.1145/3320000/3319898/p883-ge.pdf?ip=128.148.231.11&id=3319898&acc=OPEN&key=7777116298C9657D%2EDD490426617BA903%2E4D4702B0C3E38B35%2E6D218144511F3437&__acm__=1563227977_05a123459d06ee7f0c17d52b6c3c9121
        //                     quote   comma   newline     other
        // R (Record start)    Q       F       R           U
        // F (Field start)     Q       F       R           U
        // U (Unquoted field)  -       F       R           U
        // Q (Quoted field)    E       Q       Q           Q
        // E (quoted End)      Q       F       R           -

        const char *p = buffer;
        const char *lastStart = p;
        const char *end = buffer + buffer_size;
        bool good_end = false;

        size_t columnCount = 0;

        // jump into specific state if desired
        switch(state) {
            case CSVState::RECORD_START:
                goto record_start;
            case CSVState::QUOTED_FIELD: {
                columnCount++;
                goto quoted_field;
            }
            case CSVState::UNQUOTED_FIELD: {
                columnCount++;
                goto unquoted_field;
            }
            default: {
                throw std::runtime_error("unknown state in csvFindStartOffset found");
            }
        }

        // State 1: R (Record start)    Q       F       R           U
        record_start:
//            std::cout<<"R";
        // compare whether current column count matches expected one, if so return offset!
        if(columnCount == expectedColumnCount) {
            if(bytesRead)
                *bytesRead = (int)(p - buffer);
            return (int)(lastStart - buffer);
        }

        if(p >= end)
            goto parse_done;
        columnCount = 0; // reset column count
        lastStart = p;
        if(*p == quote_char) {
            columnCount++; // new field, just special casef record start
            ++p; goto quoted_field;
        } else if(*p == delimiter) {
            columnCount++; // new field, just special casef record start
            ++p; goto field_start;
        } else if(*p == '\n' || *p == '\r') {
            ++p; goto record_start; // ignore empty rows.
        } else  {
            columnCount++; // new field, just special casef record start
            ++p; goto unquoted_field;
        }

// State 2: F (Field start)     Q       F       R           U
        field_start:
//        std::cout<<"F";
        if(p >= end)
            goto parse_done;
        // only field start counts columns!
        columnCount++;
        if (*p == quote_char) {
            ++p;
            goto quoted_field;
        } else if (*p == delimiter) {
            ++p;
            goto field_start;
        } else if (*p == '\n' || *p == '\r') {
            ++p;
            goto record_start;
        } else {
            ++p;
            goto unquoted_field;
        }

// State 3: U (Unquoted field)  -       F       R           U
        unquoted_field:
//        std::cout<<"U";
        if (p >= end) {
            if (*p == '\0')
                good_end = true;
            goto parse_done;
        }
        if (*p == quote_char) {
            ++p;
            goto parse_error;
        } else if (*p == delimiter) {
            ++p;
            goto field_start;
        } else if (*p == '\n' || *p == '\r') {
            ++p;
            goto record_start;
        } else {
            ++p;
            goto unquoted_field;
        }
// State 4: Q (Quoted field)    E       Q       Q           Q
        quoted_field:
//        std::cout<<"Q";
        if (p >= end)
            goto parse_done;
        if (*p == quote_char) {
            ++p;
            goto quoted_end;
        } else if (*p == delimiter) {
            // no update for columncount here because in quoted field!
            ++p;
            goto quoted_field;
        } else if (*p == '\n' || *p == '\r') {
            ++p;
            goto quoted_field;
        } else {
            ++p;
            goto quoted_field;
        }
// State 5: E (quoted End)      Q       F       R           -
        quoted_end:
//        std::cout<<"E";
        if (p >= end) {
            if (*p == '\0')
                good_end = true;
            goto parse_done;
        }
        if (*p == quote_char) {
            ++p;
            goto quoted_field;
        } else if (*p == delimiter) {
            ++p;
            goto field_start;
        } else if (*p == '\n' || *p == '\r') {
            ++p;
            goto record_start;
        } else {
            ++p;
            goto parse_error;
        }

parse_done:
        if(bytesRead)
            *bytesRead = (int)(p - buffer);

        // check if expected number was reached
        // compare whether current column count matches expected one, if so return offset!
        if(columnCount == expectedColumnCount)
            return (int)(lastStart - buffer);

        // special case: If *p == '\0', i.e. EOF, then if last state quoted end or end for unquoted field, add 1 to column count
        if(good_end && columnCount + 1 == expectedColumnCount)
            return (int)(lastStart - buffer);

        // maybe next line is it?
        // @TODO: fix this..., it's off...

parse_error:
        if(bytesRead)
            *bytesRead = 0;
        return -1; // -1 to signal no compatible run was found
    }

    CSVChunkInfo findLineStart(const char *buffer,
            size_t buffer_size,
            size_t searchStartOffset,
            size_t expectedColumnCount,
            char delimiter,
            char quotechar) {
        using namespace std;
        assert(searchStartOffset > 1); // need at least two chars to look back to determine whether search was successful
        assert(searchStartOffset < buffer_size);
        assert(buffer);

        CSVChunkInfo info;

        // check first whether start in unquoted field leads to a successful run
        size_t bytesRead = 0;
        int unquotedRun = csvFindStartOffset(buffer + searchStartOffset, buffer_size - searchStartOffset, expectedColumnCount, CSVState::UNQUOTED_FIELD, &bytesRead, delimiter, quotechar);
        // if 0, check whether preceding char is newline => then valid start, else within field, i.e. search to next line
        if(0 == unquotedRun) {
            char lastChar = *(buffer + searchStartOffset - 1);
            if(lastChar == '\n' || lastChar == '\r') {
                // ok, start at 0
            } else {
                // forward to the next line (i.e. how many bytes were read)
                unquotedRun = bytesRead;
            }
        }

        char beforeChar = '\0';

        // unquoted ok? i.e. char before should be newline
        if(unquotedRun >= 0) {
            beforeChar = *(buffer + searchStartOffset + unquotedRun - 1);
            if(beforeChar == '\n' || beforeChar == '\r') {
                info.firstFieldState = CSVState::RECORD_START;
                info.offset = unquotedRun;
                info.valid = true;
                return info;
            }
        }

        // now check with assumed start in quoted field
        int quotedRun = csvFindStartOffset(buffer + searchStartOffset, buffer_size - searchStartOffset, expectedColumnCount, CSVState::QUOTED_FIELD, &bytesRead, delimiter, quotechar);
        // if 0, check whether preceding char is newline => then valid start, else within field, i.e. search to next line
        if(0 == quotedRun) {
            char curChar = *(buffer + searchStartOffset);
            char lastChar = *(buffer + searchStartOffset - 1);
            if(curChar == quotechar && (lastChar == '\r' || lastChar == '\n')) {
                // ok, start at quotedRun
            } else {
                // forward to the next line (i.e. how many bytes were read)
                quotedRun = bytesRead;
            }
        }

        if(quotedRun >= 0) {
            beforeChar = *(buffer + searchStartOffset + quotedRun - 1);
            // two options 1.) either first new field is unquoted or 2.) first new field is quoted
            if(beforeChar == '\n' || beforeChar == '\r') {
                info.firstFieldState = CSVState::RECORD_START;
                info.offset = quotedRun; //
                info.valid = true;
                return info;
            }
        }

        // last, we might be lucky and just chunk at a record start, try this last
        int recordRun = csvFindStartOffset(buffer + searchStartOffset, buffer_size - searchStartOffset, expectedColumnCount, CSVState::RECORD_START, &bytesRead, delimiter, quotechar);

        // if it's 0 it's 0, nothing to change.
        if(recordRun >= 0) {
            beforeChar = *(buffer + searchStartOffset + recordRun - 1);
            // two options 1.) either first new field is unquoted or 2.) first new field is quoted
            if(beforeChar == '\n' || beforeChar == '\r') {
                info.firstFieldState = CSVState::RECORD_START;
                info.offset = recordRun;
                info.valid = true;
                return info;
            }
        }

        info.firstFieldState = CSVState::UNKNOWN;
        info.offset = 0;
        info.valid = false;
        return info;
    }

    int csvFindLineStart(const char *buffer, size_t buffer_size, size_t expectedColumnCount, char delimiter, char quotechar) {
        using namespace std;

        // check first, whether a start with quoted field was found
        // then try using unquoted field
        int unquotedRun = csvFindStartOffset(buffer, buffer_size, expectedColumnCount, CSVState::UNQUOTED_FIELD, nullptr, delimiter, quotechar);
        int quotedRun = csvFindStartOffset(buffer, buffer_size, expectedColumnCount, CSVState::QUOTED_FIELD, nullptr, delimiter, quotechar);

        // check whether one succeeded
        if(quotedRun >= 0 && unquotedRun >= 0) {
            // take the one with the lower start!
            return quotedRun < unquotedRun ? quotedRun : unquotedRun;
        }

        if(quotedRun >= 0)
            return quotedRun;
        if(unquotedRun >= 0)
            return unquotedRun;

        // last, attempt to parse csv file as default
        return csvFindStartOffset(buffer, buffer_size, expectedColumnCount, CSVState::RECORD_START, nullptr, delimiter, quotechar);
    }


    std::string csvToHeader(const std::vector<std::string>& columns, const char separator) {
        if(columns.empty())
            return "";

        std::stringstream ss;
        const char sep[2] = {separator, '\0'};
        ss<<columns.front();
        for(int i = 1; i < columns.size(); ++i)
            ss<<sep<<columns[i];
        return ss.str();
    }

    tuplex::Field cellToField(const std::string& cell, const std::vector<std::string>& null_values) {

        // i.e., mimick the python code from PythonPipelineBuilder.cc
        //   "def parse(s):\n"
        //                         "    assert isinstance(s, str)\n"
        //                         "    # try to parse s as different types\n";
        //                        // try parse via na_values if they exist
        //                        if(!na_values.empty()) {
        //                            ss<<"    if s in "<<vecToList(na_values)<<":\n";
        //                            ss<<"        return None\n";
        //                        }
        //        ss<<"    try:\n"
        //                         "        return to_bool(s)\n"
        //                         "    except:\n"
        //                         "        pass\n"
        //                         "    try:\n"
        //                         "        return int(s.strip())\n"
        //                         "    except:\n"
        //                         "        pass\n"
        //                         "    try:\n"
        //                         "        return float(s.strip())\n"
        //                         "    except:\n"
        //                         "        pass\n"
        //                         "    try:\n"
        //                         "        return json.loads(s.strip())\n"
        //                         "    except:\n"
        //                         "        pass\n"
        //                         "    # return as string, final option remaining...\n"
        //                         "    return s";

        // strip whitespace?
        std::string str = cell;
        trim(str);

        // check if null-value
        for(const auto& nv : null_values) {
            if(str == nv)
                return Field::null();
        }

        // check for bool
        if(!str.empty() && isBoolString(str)) {
            return Field(parseBoolString(str));
        }

        // check for integer
        if(!str.empty() && isIntegerString(str.c_str())) {

            // do not use stoll, stod here because exceptions will mess up boost python
            // instead, try to parse using standard ascii
            // simply use tuplex functions
            int64_t i = 0;
            if(ecToI32(ExceptionCode::SUCCESS) == fast_atoi64(str.c_str(), str.c_str() + str.length(), &i)) {
                return Field(i);
            }
        }

        // check for float
        if(!str.empty() && isFloatString(str.c_str())) {
            double d = 0.0;
            if(ecToI32(ExceptionCode::SUCCESS) == fast_atod(str.c_str(), str.c_str() + str.length(), &d)) {
                return Field(d);
            }
        }

        // JSON String (starts & ends with {} or [])
        if(str.length() >= 2) {
            std::string stripped = str; // trim whitespace away...
            trim(stripped);
            if((stripped.front() == '{' && stripped.back() == '}') ||
               (stripped.front() == '[' && stripped.back() == ']') ||
                    (stripped.front() == '(' && stripped.back() == ')')) {
                // warn, just interpret as string for now...
                // throw std::runtime_error("tuple, dict and list parsing not yet supported!");
                // there is an issue for this: https://github.com/LeonhardFS/Tuplex/issues/196
#warning "tuple, dict and list parsing not yet supported!"
            }
        }

        // leave as string, i.e. trimmed.
        return Field(str);
    }

    tuplex::Row cellsToRow(const std::vector<std::string>& cells, const std::vector<std::string>& null_values) {
        using namespace std;
        vector<Field> fields;
        for(auto cell : cells)
            fields.emplace_back(cellToField(cell, null_values));
        return Row::from_vector(fields);
    }

    std::vector<tuplex::Row> parseRows(const char *start, const char *end, const std::vector<std::string>& null_values, char delimiter, char quotechar) {
        using namespace std;
        using namespace tuplex;
        vector<Row> rows;

        ExceptionCode ec;
        vector<string> cells;
        size_t num_bytes = 0;
        const char* p = start;
        while(p < end && (ec = parseRow(p, end, cells, num_bytes, delimiter, quotechar, false)) == ExceptionCode::SUCCESS) {
            // convert cells to row
            rows.emplace_back(cellsToRow(cells, null_values));
            cells.clear();
            p += num_bytes;
        }

        return rows;
    }

}