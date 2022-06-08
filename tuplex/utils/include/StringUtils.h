//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_STRINGUTILS_H
#define TUPLEX_STRINGUTILS_H

#include <cstdint>
#include <cassert>
#include <string>
#include <functional>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <boost/algorithm/string/join.hpp>

#include "Base.h"

namespace tuplex {
    // helper functions for data conversion (optimized for correct input)
    // this function does not remove trailing whitespace!
    // --> use more sophisticated function for conversion else.
    extern int32_t fast_atoi64(const char *start, const char *end, int64_t* out);

    // check for naive implementation
    // under https://tinodidriksen.com/uploads/code/cpp/speed-string-to-double.cpp
    // also a nice one is http://www.leapsecond.com/tools/fast_atof.c
    // https://pastebin.com/dHP1pgQ4
    // based on
    // http://www.leapsecond.com/tools/fast_atof.c
    extern int32_t fast_atod(const char *start, const char *end, double* out);

    // fast conversion of string to bool
    // following are converted to true/false
    // t/T, f/F, true/True, false/False
    extern int32_t fast_atob(const char *start, const char *end, bool *out);


    // runtime allocates string and dequotes if necessary
    typedef void*(*malloc_f)(size_t);

    /*!
     * dequotes a string
     * @param start start ptr
     * @param end  end ptr (incl.)
     * @param quotechar the char to dequote
     * @param out nullptr if no quotes were found, else with mf alloced memory holding a string of length length
     * @param olength the result strlen(*out) would return
     * @param mf a malloc function, i.e. returns some memory.
     * @return ExceptionCode as int32. I.e. SUCCESS when everything went well. May return Doublequoteerror.
     */
    extern int32_t fast_dequote(const char *start, const char *end, const char quotechar, char **out, int64_t *olength, malloc_f mf);

    // from https://stackoverflow.com/questions/9277906/stdvector-to-string-with-custom-delimiter in order
    // to convert quickly any container to a delimiter concatenated list.

    template<typename T> std::string mkString(const T& container, const std::string& sep = ", ",
                                              const std::string& open = "",
                                              const std::string& close = "") {
        return open + boost::join(container, sep) + close;
    }

    template<class InItr>
    std::string mkString(InItr first,
                         InItr last,
                         const std::string& sep = ", ",
                         const std::string& open = "",
                         const std::string& close = "") {

        std::stringstream ss;
        ss << open;

        if (first != last) {
            ss << *first;
            ++first;
        }

        for (; first != last; ++first)
            ss << sep << *first;
        ss << close;

        return ss.str();
    }


    /*!
     * converts chars from start till end (excl.) to string
     * @param start
     * @param end
     * @return std::string
     */
    extern std::string fromCharPointers(const char *start, const char *end);

    /*!
     * finds the longest common prefix over all strings
     * @param strs
     * @return longest common prefix across all given strings
     */
    inline std::string longestCommonPrefix(std::vector<std::string> strs) {
        auto N = strs.size();

        if(0 == N)
            return "";

        if(1 == N)
            return strs.front();

        std::sort(strs.begin(), strs.end());
        auto end = std::min(strs.front().length(), strs.back().length());
        unsigned i = 0;
        while(i < end && strs.front()[i] == strs.back()[i])
            i++;

        return strs.front().substr(0, i);
    }

    /*!
     * returns a fixed point version (default 2)
     * @tparam T
     * @param t
     * @param precision
     * @return
     */
    template<typename T> std::string fixedPoint(const T t, size_t precision=2) {
        std::stringstream ss;
        ss<<std::fixed<<std::setprecision(precision)<<t;
        return ss.str();
    }

    // from https://stackoverflow.com/questions/11521183/return-fixed-length-stdstring-from-integer-value
    inline std::string fixedLength(const int i, const int length) {
        std::ostringstream ostr;

        if (i < 0)
            ostr << '-';

        ostr << std::setfill('0') << std::setw(length) << (i < 0 ? -i : i);

        return ostr.str();
    }

    inline std::string formatTime(double seconds) {
        std::stringstream ss;
        if(seconds < 1.0) {
            ss<<fixedPoint(seconds * 1000.0)<<" ms";
        } else {
            ss<<fixedPoint(seconds)<<" s";
        }
        return ss.str();
    }

    inline std::string dequote(const std::string& str, char quote_char = '"') {
        using namespace tuplex;
        //(const char *start, const char *end, const char quotechar, char **out, int64_t *osize, malloc_f mf)
        char *out =nullptr;
        int64_t size= 0;
        tuplex::fast_dequote(str.c_str(), str.c_str() + str.length(), quote_char, &out, &size, malloc);

        if(!out)
            return str;

        std::string res(fromCharPointers(out, out+size));
        if(out)
            free(out);
        return res;
    }

    inline std::string charToString(const char c) {
        std::string s;
        s= "'x'";
        s[1] = c;
        return s;
    }

    inline std::string boolToString(const bool b) {
        return b ? "true" : "false";
    }

    /*!
     * converts(without tabs) JSON to text representation
     * @param item cJSON object (tree)
     * @return empty string if item is nullptr, else json serialized as string
     */
    inline std::string cJSON_to_string(const cJSON* item) {
        if(!item)
            return "";
        char* ptr = cJSON_PrintUnformatted(item);
        std::string res(ptr); // create copy
        cJSON_free(ptr);
        return res;
    }

    // define here what counts all boolean true/false strings!
    // => adjust fast_atob function regarding them!!!
    // note that booleanTrueStrings[i] should correspond to booleanFalseStrings[i]! I.e., they build an encoding pair!
    // => DO NOT DEFINE 1/0 here unless you want to have clashes with integers.
    inline std::vector<std::string> booleanTrueStrings() {
        // case insensitive!
        return std::vector<std::string>{"true", "t", "yes", "y"};
    }
    inline std::vector<std::string> booleanFalseStrings() {
        return std::vector<std::string>{"false", "f", "no", "n"};
    }

    /*!
     * checks whether given string is boolean according to the list of boolean strings defined in StringUtils.h
     * @param str
     * @return true/false
     */
    extern bool isBoolString(const std::string& str);

    /*!
     * parse string, throws exception if not boolean string. @TODO: can we optimize this? I.e. via prefix tree?
     * @param str
     * @return the value of the boolean str
     */
    extern bool parseBoolString(const std::string& str);

    /*!
     * parses using tuplex functions
     */
    extern int64_t parseI64String(const std::string& str);

    /*!
     * parses using tuplex functions
     */
    extern double parseF64String(const std::string& str);

    extern bool isIntegerString(const char* s, bool ignoreWhitespace=true);
    extern bool isFloatString(const char* s, bool ignoreWhitespace=true);

    // trim from start (in place)
    inline void ltrim(std::string &s) {
        s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) {
            return !std::isspace(ch);
        }));
    }

// trim from end (in place)
    inline void rtrim(std::string &s) {
        s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) {
            return !std::isspace(ch);
        }).base(), s.end());
    }

// trim from both ends (in place)
    inline void trim(std::string &s) {
        ltrim(s);
        rtrim(s);
    }

    inline std::string trim(const std::string& s) {
        std::string copy(s.c_str());
        ltrim(copy);
        rtrim(copy);
        return copy;
    }

    inline size_t leading_line_count(const std::string& s) {
        size_t count = 0;
        for(auto c : s) {
            if(c == '\n' || c == '\r')
                count++;
            if(!isspace(c))
                break;
        }
        return count;
    }

//    // from https://en.cppreference.com/w/cpp/algorithm/lexicographical_compare
//    // C++17 feature...
//    template<class InputIt1, class InputIt2>
//    bool lexicographical_compare(InputIt1 first1, InputIt1 last1,
//                                 InputIt2 first2, InputIt2 last2)
//    {
//        for ( ; (first1 != last1) && (first2 != last2); ++first1, (void) ++first2 ) {
//            if (*first1 < *first2) return true;
//            if (*first2 < *first1) return false;
//        }
//        return (first1 == last1) && (first2 != last2);
//    }


    /*!
     * takes a string, and calls function over all substrings separated by delimiter
     * @param str string to parse
     * @param delimiter delimiter
     * @param func function to call on each substring
     */
    extern void splitString(const std::string &str, char delimiter, std::function<void(std::string)> func);


    inline std::vector<std::string> splitToArray(const std::string& str, char delimiter) {
        std::vector<std::string> v;

        splitString(str, delimiter, [&](std::string s) { v.emplace_back(s); });
        return v;
    }

    /*!
     * checks whether memory region from start to end contains any UTF8 characters
     * @param start where to start looking for UTF8 characters
     * @param size number of bytes to scan
     * @return true if any UTF8 character was found
     */
    extern bool containsUTF8(const char *start, const size_t size);

    // check whether string is valid utf8 string.
    extern bool utf8_check_is_valid(const std::string& string);

    // from https://stackoverflow.com/questions/4059775/convert-iso-8859-1-strings-to-utf-8-in-c-c
    inline std::string iso_8859_1_to_utf8(std::string &str) {
        std::string strOut;
        for (std::string::iterator it = str.begin(); it != str.end(); ++it) {
            uint8_t ch = *it;
            if (ch < 0x80) {
                strOut.push_back(ch);
            }
            else {
                strOut.push_back(0xc0 | ch >> 6);
                strOut.push_back(0x80 | (ch & 0x3f));
            }
        }
        return strOut;
    }

    /*!
     * returns maximum linewidth of a '\n' delimited string
     * @param s
     * @return maximum line break width
     */
    extern size_t getMaxLineLength(const std::string& s);

    /*!
     * replaces line breaks in strings
     * @param s
     * @return string with line breaks repalced by \n
     */
    extern std::string replaceLineBreaks(const std::string& s);

//    /*!
//     * produces a string of the roman number of the given natural number
//     * @param n must be a natural number
//     * @return string
//     */
//    extern std::string toRomanLiteral(const size_t n);

    /*!
     * 64 bit hex addr printed
     * @tparam T will be converted to uint64_t and then as hex number
     * @param t
     * @return string containing hex presentation, i.e. 0x12AB
     */
    template<typename T> std::string hexAddr(T t) {
        std::stringstream ss;
        ss << "0x" << std::uppercase << std::setfill('0') << std::setw(4) << std::hex << (uint64_t)t;
        return ss.str();
    }

    /*!
     * returns a correctly pluralized version of a word and its count
     * @tparam T should be integer
     * @param t count, i.e. 1, 2, 3
     * @param base_word wird , i.e. row
     * @return pluralized version. E.g. for inputs 1, row this will return "1 row"
     */
    template<typename T> std::string pluralize(const T t, const std::string& base_word) {
        return 1 == t ? std::to_string(t) + " " + base_word : std::to_string(t) + " " + base_word + "s";
    }

    /*!
     * returns current time as ISO8601 formatted utc string
     * @return  iso8601 utc iso time
     */
    extern std::string currentISO8601TimeUTC();

    /*!
     * split a string into lines
     * @param s
     * @return
     */
    inline std::vector<std::string> splitToLines(const std::string& s) {
        using namespace std;
        vector<string> v;
        istringstream stream(s);
        string line;

        while(getline(stream, line))
            v.push_back(line);

        return v;
    }


    inline std::string quoteForCSV(const std::string& str, char separator, char quotechar) {
        auto size = str.size();

        if(str.empty())
            return "";

        // check if there are chars in it that need to be quoted
        size_t num_quotes = 0;
        bool need_to_quote = false;

        auto len = size - 1;
        for(unsigned i = 0; i < len; ++i) {
            if(str[i] == quotechar)
                num_quotes++;
            if(str[i] == separator || str[i] == '\n' || str[i] == '\r')
                need_to_quote = true;
        }

        if(num_quotes > 0 || need_to_quote) {
            auto resSize = len + 1 + 2 + num_quotes;
            // alloc new string
            std::string res('\0', resSize);
            size_t pos = 0;

            res[pos] = quotechar; ++pos;
            for(unsigned i = 0; i < len; ++i) {
                if(str[i] == quotechar) {
                    res[pos] = quotechar; ++pos;
                    res[pos] = quotechar; ++pos;
                } else {
                    res[pos] = str[i]; ++pos;
                }
            }
            res[pos] = quotechar; ++pos;
            res[pos] = '\0';

            return res;
        } else {
           return str;
        }
    }
}


#endif //TUPLEX_STRINGUTILS_H