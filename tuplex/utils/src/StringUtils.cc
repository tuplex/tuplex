//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ExceptionCodes.h>
#include <functional>
#include <regex>
#include "../include/StringUtils.h"
#include <boost/algorithm/string.hpp>
#include <cmath>

namespace tuplex {
    // helper functions for data conversion (optimized for correct input)
    // this function does not remove trailing whitespace!
    // --> use more sophisticated function for conversion else.
    int32_t fast_atoi64(const char *start, const char *end, int64_t* out) {
        if(start == end)
            return static_cast<int32_t>(ExceptionCode::NULLERROR);


        assert(out);
        assert(start < end); // note: usually only such inputs should be used. the if below is to complete the function.

        // from https://tinodidriksen.com/uploads/code/cpp/speed-string-to-int.cpp
        // adopted for exception handling
        int64_t x = 0;
        const char *p = start;
        bool neg = false;
        if (*p == '-') {
            neg = true;
            ++p;
        }
        while (*p >= '0' && *p <= '9') {
            x = (x * 10) + (*p - '0');
            ++p;
        }

        // invalid?

        // use p < end for more generous criterion.
        // however, this captures parsing errors directly.
        if(p != end)
            return static_cast<int32_t>(ExceptionCode::I64PARSE_ERROR);

        // debug to get details on parsed string
        // if(p != end) {
        //
        //     printf("length of string is: %ld", (end - start) - 1);
        //     return static_cast<int32_t>(ExceptionCode::I64PARSE_ERROR);
        // }

        if (neg) {
            x = -x;
        }
        *out = x;
        return static_cast<int32_t>(ExceptionCode::SUCCESS);
    }

// check for naive implementation
// under https://tinodidriksen.com/uploads/code/cpp/speed-string-to-double.cpp
// also a nice one is http://www.leapsecond.com/tools/fast_atof.c
// https://pastebin.com/dHP1pgQ4
    // based on
    // http://www.leapsecond.com/tools/fast_atof.c
    int32_t fast_atod(const char *start, const char *end, double* out) {

        assert(out);
        assert(start <= end);

        if(start == end)
            return static_cast<int32_t>(ExceptionCode::NULLERROR);

        int32_t frac;
        double sign, value, scale;

        // skip leading whitespace is in parser
        const char* p = start;

        // retrieve sign
        sign = 1.0;
        if('-' == *p) {
            sign = -1.0;
            ++p;
        } else if('+' == *p)
            ++p;

        // get digits before decimal point (here .)
        for(value = 0.0; *p >= '0' && *p <= '9'; p++) {
            value = 10.0 * value + (*p - '0');
        }

        // get digits after decimal point
        if(*p == '.') {
            double pow10 = 10.0;
            ++p;

            while(*p >= '0' && *p <= '9') {
                value += (*p - '0') / pow10;
                pow10 *= 10.0;
                ++p;
            }
        }

        // exponent if there
        frac = 0;
        scale = 1.0;
        if(('e' == *p) || ('E' == *p)) {
            unsigned int exponent;

            // sign of exponent
            ++p;
            if('-' == *p) {
                frac = 1;
                ++p;
            } else if('+' == *p)
                ++p;

            // get digits of exponent
            for(exponent = 0; *p >= '0' && *p <= '9'; p++) {
                exponent = exponent * 10 + (*p - '0');
            }

            if(exponent > 308)
                exponent = 308;

            // compute scaling factor
            while (exponent >= 50) { scale *= 1E50; exponent -= 50; }
            while (exponent >=  8) { scale *= 1E8;  exponent -=  8; }
            while (exponent >   0) { scale *= 10.0; exponent -=  1; }
        }

        // handle nan, inf/infinity: https://docs.python.org/3/library/functions.html#float
        // check for nan
        const char *nanstr = "nan";
        int nanmatch = 0;
        if(p == start) {
            while(((*p == nanstr[nanmatch]) || (*p == toupper(nanstr[nanmatch]))) && nanmatch < 3) { p++; nanmatch++; }
        }

        // check for infinity
        const char *infstr = "infinity";
        int infmatch = 0;
        if(p == start) {
            while(((*p == infstr[infmatch]) || (*p == toupper(infstr[infmatch]))) && infmatch < 8) { p++; infmatch++; }
        }

        // check if parsing was successful
        if(p != end)
            return static_cast<int32_t>(ExceptionCode::F64PARSE_ERROR);

        // result
        if(nanmatch == 3) {
            *out = NAN;
        } else if(infmatch == 3 || infmatch == 8) {
            *out = INFINITY;
        } else {
            *out = sign * (frac ? (value / scale) : (value * scale));
        }

        return static_cast<int32_t>(ExceptionCode::SUCCESS);
    }


    // bool parsing allows the following input formats
    // all letters are case insensitive
    // T, F, Y, N, True, False, 0, 1, Yes, No
    // inline std::vector<std::string> booleanTrueStrings() {
    //        // case insensitive!
    //        return std::vector<std::string>{"true", "t", "yes", "y", "1"};
    //    }
    //    inline std::vector<std::string> booleanFalseStrings() {
    //        return std::vector<std::string>{"false", "f", "no", "n", "0"};
    //    }
    int32_t fast_atob(const char *start, const char *end, bool *out) {

        assert(out);
        assert(start < end);

        if(start == end)
            return static_cast<int32_t>(ExceptionCode::NULLERROR);

        auto length = end - start;
        char buffer[8];
        switch (length) {
            case 1:
                // using 0 or 1 leads to conflicts with int
                // if ('Y' == *start || 'y' == *start || 'T' == *start || 't' == *start || '1' == *start) {
                if ('Y' == *start || 'y' == *start || 'T' == *start || 't' == *start) {
                    *out = true;
                    return static_cast<int32_t>(ExceptionCode::SUCCESS);
                }
                // if ('N' == *start || 'n' == *start || 'F' == *start || 'f' == *start || '0' == *start) {
                if ('N' == *start || 'n' == *start || 'F' == *start || 'f' == *start) {
                    *out = false;
                    return static_cast<int32_t>(ExceptionCode::SUCCESS);
                }
                return static_cast<int32_t>(ExceptionCode::BOOLPARSE_ERROR);
            case 2:
                // convert char
                buffer[0] = (char) std::tolower(*start);
                buffer[1] = (char) std::tolower(*(start + 1));
                buffer[2] = 0;

                if (strcmp(buffer, "no") == 0) {
                    *out = false;
                    return static_cast<int32_t>(ExceptionCode::SUCCESS);
                }
                return static_cast<int32_t>(ExceptionCode::BOOLPARSE_ERROR);
            case 3:
                // convert char
                buffer[0] = (char) std::tolower(*start);
                buffer[1] = (char) std::tolower(*(start + 1));
                buffer[2] = (char) std::tolower(*(start + 2));
                buffer[3] = 0;

                if (strcmp(buffer, "yes") == 0) {
                    *out = true;
                    return static_cast<int32_t>(ExceptionCode::SUCCESS);
                }
                return static_cast<int32_t>(ExceptionCode::BOOLPARSE_ERROR);
            case 4:
                // convert char
                buffer[0] = (char) std::tolower(*start);
                buffer[1] = (char) std::tolower(*(start + 1));
                buffer[2] = (char) std::tolower(*(start + 2));
                buffer[3] = (char) std::tolower(*(start + 3));
                buffer[4] = 0;

                if (strcmp(buffer, "true") == 0) {
                    *out = true;
                    return static_cast<int32_t>(ExceptionCode::SUCCESS);
                }
                return static_cast<int32_t>(ExceptionCode::BOOLPARSE_ERROR);
            case 5:
                // convert char
                buffer[0] = (char) std::tolower(*start);
                buffer[1] = (char) std::tolower(*(start + 1));
                buffer[2] = (char) std::tolower(*(start + 2));
                buffer[3] = (char) std::tolower(*(start + 3));
                buffer[4] = (char) std::tolower(*(start + 4));
                buffer[5] = 0;

                if (strcmp(buffer, "false") == 0) {
                    *out = false;
                    return static_cast<int32_t>(ExceptionCode::SUCCESS);
                }
                return static_cast<int32_t>(ExceptionCode::BOOLPARSE_ERROR);
            default:
                return static_cast<int32_t>(ExceptionCode::BOOLPARSE_ERROR);
        }
    }

    std::string fromCharPointers(const char *start, const char *end) {
        assert(start <= end);
        std::string s;
        //s.reserve(static_cast<size_t>(end - start) + 1);
        s.assign(start, static_cast<size_t>(end - start));
        return s;
    }

    bool isBoolString(const std::string& str) {
        // @TODO: can we optimize this?

        // true list
        for(const auto& t : booleanTrueStrings()) {
            if(boost::algorithm::to_lower_copy(str).compare(t) == 0)
                return true;
        }

        // false list
        for(const auto& f : booleanFalseStrings()) {
            if(boost::algorithm::to_lower_copy(str).compare(f) == 0)
                return true;
        }

        // not a boolean string
        return false;
    }

    bool parseBoolString(const std::string& str) {
        // true list
        for(const auto& t : booleanTrueStrings()) {
            if(boost::algorithm::to_lower_copy(str).compare(t) == 0)
                return true;
        }

        // false list
        for(const auto& f : booleanFalseStrings()) {
            if(boost::algorithm::to_lower_copy(str).compare(f) == 0)
                return false;
        }

        throw std::runtime_error("parse exception, " + str + " is not boolean");
    }

    /*!
    * parses using tuplex functions
    */
    int64_t parseI64String(const std::string& str) {
        int64_t ret = 0;
        auto ec = fast_atoi64(str.c_str(), str.c_str() + str.size(), &ret);
        if(ecToI32(ExceptionCode::SUCCESS) != ec)
            throw std::runtime_error("failed to parse i64 from '" + str + "'");
        return ret;
    }

    /*!
     * parses using tuplex functions
     */
    double parseF64String(const std::string& str) {
        double ret = 0;
        auto ec = fast_atod(str.c_str(), str.c_str() + str.size(), &ret);
        if(ecToI32(ExceptionCode::SUCCESS) != ec)
            throw std::runtime_error("failed to parse f64 from '" + str + "'");
        return ret;
    }



    bool isIntegerString(const char* s, bool ignoreWhitespace) {
        auto p = s;

        bool minusSeen = false;
        while(*p != '\0') {
            // whitespace ignored
            if(ignoreWhitespace && (*p == ' ' || *p == '\t'))
                ++p;
            else if(*p == '-') {
                // more than one minus?
                // --> not a valid integer
                if(minusSeen)
                    return false;
                minusSeen = true;
                ++p;
            } else if(isdigit(*p)) {
                ++p;
                minusSeen = true; // as soon as a digit is seen, no more minus allowed.
            } else {
                return false;
            }
        }

        // check parsed length
        assert(*p == '\0');
        return true;
    }

    bool isFloatString(const char* s, bool ignoreWhitespace) {
        auto p = s;
        int numMinusSeen = 0;
        bool eSeen = false;
        bool dotSeen = false;
        bool digitSeen = false;
        while(*p != '\0') {
            // whitespace ignored
            if(ignoreWhitespace && (*p == ' ' || *p == '\t'))
                ++p;
            else if(*p == '-') {
                // more than one minus?
                // only one more allowed after e/E!
                if(numMinusSeen > 2)
                    return false;
                // 2 -- before e
                if(numMinusSeen == 1 && !eSeen)
                    return false;
                numMinusSeen++;
                ++p;
            } else if(digitSeen && (*p == 'e' || *p == 'E')) {
                // more than one e character?
                if(eSeen)
                    return false;
                eSeen = true;
                ++p;
            } else if(*p == '.') {
                if(dotSeen)
                    return false;
                dotSeen = true;
                ++p;
            } else if(isdigit(*p)) {
                    digitSeen = true;
                    ++p;
                    numMinusSeen = std::max(1, numMinusSeen);
            } else {
                return false;
            }
        }

        // disallow strings ending in e or E. scientific notation is only there, if numbers follow and there has been at
        // least one number before the e/E
        if(eSeen && (*(p - 1) == 'e' || *(p - 1) == 'E'))
            return false;

        // special case: only a dot is not a floating point number
        if(dotSeen && s[0] == '.' && s[1] == '\0')
            return false;

        return true;
    }

    // from https://stackoverflow.com/questions/1894886/parsing-a-comma-delimited-stdstring
    void splitString(const std::string &str, char delimiter, std::function<void(std::string)> func) {
        std::size_t from = 0;
        for (std::size_t i = 0; i < str.size(); ++i) {
            if (str[i] == delimiter) {
                func(str.substr(from, i - from));
                from = i + 1;
            }
        }
        if (from <= str.size())
            func(str.substr(from, str.size() - from));
    }

    bool containsUTF8(const char *start, const size_t size) {
        auto end = start + size;

        const char *p = start;
        while(p < end) {
            if(*p > 127)
                return true;
            ++p;
        }
        return false;
    }

    // from http://www.zedwood.com/article/cpp-is-valid-utf8-string-function
    bool utf8_check_is_valid(const std::string& string) {
        int c,i,ix,n,j;
        for (i=0, ix=string.length(); i < ix; i++) {
            c = (unsigned char) string[i];
            //if (c==0x09 || c==0x0a || c==0x0d || (0x20 <= c && c <= 0x7e) ) n = 0; // is_printable_ascii
            if (0x00 <= c && c <= 0x7f) n=0; // 0bbbbbbb
            else if ((c & 0xE0) == 0xC0) n=1; // 110bbbbb
            else if ( c==0xed && i<(ix-1) && ((unsigned char)string[i+1] & 0xa0)==0xa0) return false; //U+d800 to U+dfff
            else if ((c & 0xF0) == 0xE0) n=2; // 1110bbbb
            else if ((c & 0xF8) == 0xF0) n=3; // 11110bbb
                //else if (($c & 0xFC) == 0xF8) n=4; // 111110bb //byte 5, unnecessary in 4 byte UTF-8
                //else if (($c & 0xFE) == 0xFC) n=5; // 1111110b //byte 6, unnecessary in 4 byte UTF-8
            else return false;
            for (j=0; j<n && i<ix; j++) { // n bytes matching 10bbbbbb follow ?
                if ((++i == ix) || (( (unsigned char)string[i] & 0xC0) != 0x80))
                    return false;
            }
        }
        return true;
    }

    size_t getMaxLineLength(const std::string& s) {
        size_t maxLength = 0;
        size_t curLength = 0;
        for(int i = 0; i < s.length(); ++i) {
            if(s[i] == '\n') {
                maxLength = std::max(maxLength, curLength);
                curLength = 0;
            } else
                curLength++;
        }
        return std::max(maxLength, curLength);
    }

    std::string replaceLineBreaks(const std::string& s) {
        return std::regex_replace(s, std::regex("\n"), "\\n");
    }

    int32_t fast_dequote(const char *start, const char *end, const char quotechar, char **out, int64_t *olength, malloc_f mf) {
       size_t cnt = 0;

       // 1. count occurrences
       const char* p = start;
       while(p < end) {
           if(*p == quotechar && *(p+1) == quotechar) {
               cnt++;
               p += 2;
           } else {
               p++;
           }
       }


       // 2. replace if necessary
       p = start;

       if(cnt != 0) {
           size_t s = end - start - cnt;
           char *res = (char*)mf(s);
           res[s - 1] = '\0';

           *out = res;
           *olength = s;

           p = start;
           while(p < end) {
                //  // this would leave single quotes as is
                //  if(*p == quotechar && *(p+1) == quotechar) {
                //      *res = *p;
                //      p += 2;
                //      res++;
                //  } else {
                //      *res = *p;
                //      res++;
                //      p++;
                //  }
               if(*p == quotechar && *(p+1) == quotechar) {
                   *res = *p;
                   p += 2;
                   res++;
               } else if(*p != quotechar){
                   *res = *p;
                   res++;
                   p++;
               } else {
                   // single quote error
                   return ecToI32(ExceptionCode::DOUBLEQUOTEERROR);
               }
           }

           assert(res - *out == s);

           return ecToI32(ExceptionCode::SUCCESS);
       } else {
           *out = nullptr;
           return ecToI32(ExceptionCode::SUCCESS);
       }
    }

    // from https://techoverflow.net/2018/03/30/iso8601-utc-time-as-stdstring-using-c11-chrono/
    std::string currentISO8601TimeUTC() {
        auto now = std::chrono::system_clock::now();
        auto itt = std::chrono::system_clock::to_time_t(now);
        std::ostringstream ss;
        ss << std::put_time(gmtime(&itt), "%FT%TZ");
        return ss.str();
    }
}