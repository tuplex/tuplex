//
// Created by Leonhard Spiegelberg on 11/18/21.
//

#ifndef TUPLEX_FILEUTILS_H
#define TUPLEX_FILEUTILS_H

#include <string>

namespace tuplex {
    /*!
    * when performing unix wildcard matching, this finds the longest prefix where no matching character is used
    * @param s path
    * @param escapechar characters escaped with this char will be ignored for the prefix search
    * @return longest prefix
    */
    inline std::string findLongestPrefix(const std::string &s, const char escapechar = '\\') {
        // list from http://tldp.org/LDP/GNU-Linux-Tools-Summary/html/x11655.htm
        // http://man7.org/linux/man-pages/man7/glob.7.html

        // basically need to search for following chars
        char reserved_chars[] = "*?[]";
        auto num_reserved_chars = strlen(reserved_chars);
        char lastChar = 0;

        const char *ptr = s.c_str();
        while (*ptr != '\0') {
            auto curChar = *ptr;

            // check if curChar is any of the reserved ones
            for (unsigned i = 0; i < num_reserved_chars; ++i)
                if (curChar == reserved_chars[i]) {
                    // check if last char is escapechar
                    if (lastChar != escapechar)
                        goto done;
                }
            lastChar = *ptr;
            ptr++;
        }
        done:
        auto idx = ptr - s.c_str();
        return s.substr(0, idx);
    }

    inline std::string eliminateSeparatorRuns(const std::string& path, size_t numStartCharsToIgnore = 0, const char separator='/') {
        char *temp_buf = new char[path.size() + 1];
        memset(temp_buf, 0, path.size() + 1);
        memcpy(temp_buf, path.c_str(), numStartCharsToIgnore * sizeof(char));
        auto ptr = temp_buf + numStartCharsToIgnore;
        for(int i = numStartCharsToIgnore; i < path.size(); ++i) {
            *ptr = path[i];

            if(*ptr == separator) {
                // skip subsequent separators
                while(i + 1 < path.size() && path[i + 1] == separator)
                    ++i;
            }
            ptr++;
        }

        std::string res(temp_buf);
        delete [] temp_buf;
        return res;
    }
}

#endif //TUPLEX_FILEUTILS_H
