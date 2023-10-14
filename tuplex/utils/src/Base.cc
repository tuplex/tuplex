//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <iostream>
#include <iostream>
#include <iomanip>
#include <cmath>
#include <Utils.h>

void debug_error_message(const char* message, const char* file, const int line) {
    std::cerr<<"Error at "<<file<<"+"<<line<<" : "<<message<<std::endl;
}


namespace core {
    void hexdump(std::ostream& out, const void *ptr, const size_t numBytes, bool format) {

        // save old formats
        std::ios old_state(nullptr);
        old_state.copyfmt(out);

        out<<std::setfill('0');
        auto *p = reinterpret_cast<const unsigned char*>(ptr);
        for(int i = 0; i < numBytes; ++i, p++) {
            out<<std::hex<<std::setw(2)<<static_cast<unsigned>(*p);
            if(format)
                out<<(((i + 1) % 16 == 0) ? "\n" : " ");
        }
        out<<std::endl;

        // restore formats
        out.copyfmt(old_state);
    }

    void asciidump(std::ostream& out, const void *ptr, const size_t numBytes, bool format) {

        // save old formats
        std::ios old_state(nullptr);
        old_state.copyfmt(out);

        out<<std::setfill('0');
        auto *p = reinterpret_cast<const unsigned char*>(ptr);
        for(int i = 0; i < numBytes; ++i, p++) {
            out<<static_cast<char>(*p);
            if(format)
                out<<(((i + 1) % 16 == 0) ? "\n" : " ");
        }
        out<<std::endl;

        // restore formats
        out.copyfmt(old_state);
    }

    // taken from https://stackoverflow.com/questions/14265581/parse-split-a-string-in-c-using-string-delimiter-standard-c/14266139
    std::vector<std::string> splitLines(const std::string& s, const std::string& delimiter) {
        using namespace std;
        size_t pos_start = 0, pos_end, delim_len = delimiter.length();
        string token;
        vector<string> res;
        while ((pos_end = s.find(delimiter, pos_start)) != string::npos) {
            token = s.substr(pos_start, pos_end - pos_start);
            pos_start = pos_end + delim_len;
            res.push_back(token);
        }
        res.push_back(s.substr(pos_start));
        return res;
    }

    std::string withLineNumbers(const std::string& s) {
        using namespace std;
        stringstream ss;

        auto lines = splitLines(s, "\n");

        int numLines = lines.size();
        int numDigits = (int)ceil(log10(numLines));

        for(int i = 0; i < numLines; ++i){
            ss<<setw(numDigits)<<setfill('0')<<(i+1)<<": "<<lines[i]<<'\n';
        }
        return ss.str();
    }
}

std::string str_value_from_python_raw_value(const std::string& raw_string) {
    using namespace tuplex;

    // this routine is mostly modeled after https://github.com/python/cpython/blob/a5634c406767ef694df49b624adf9cfa6c0d9064/Parser/string_parser.c
    assert(raw_string.size() >= 2); // must be at least two chars!

    const char* s = raw_string.c_str();
    auto len = raw_string.size() - 1;

    bool is_raw = false;

    if(raw_string.front() == 'r' || raw_string.front() == 'R') {
        // it's a raw string!
        is_raw = true;
        s++; // skip r/R char!
        len--;
    }

    if(raw_string.front() == 'b' || raw_string.front() == 'B')
        throw std::runtime_error("bytes strings not yet supported");
    if(raw_string.front() == 'u' || raw_string.front() == 'U')
        throw std::runtime_error("unicode strings not yet supported");
    if(raw_string.front() == 'f' || raw_string.front() == 'F')
        throw std::runtime_error("formatted strings not yet supported");

    auto quote = *s++;
    auto lastQuote = s[--len];
    // check that last char matches detected quote!
    if(lastQuote != quote)
        throw std::runtime_error("ending quote char " + char2str(lastQuote) + " does not match starting quote char " + char2str(quote));

    // triple string? i.e. ''' .... ''' or """ ... """?
    if(len >= 4 && s[0] == quote && s[1] == quote) {
        if(s[--len] != quote || s[--len] != quote)
            throw std::runtime_error("quotes at start do not match quotes at end");
    }

    // use python's hack to avoid decoding escapes if possible
    is_raw = is_raw || strchr(s, '\\') == nullptr;
    if(is_raw) {
        return fromCharPointers(s, s + len);
    } else {
        char *buf = new char[len * 6]; memset(buf, 0, sizeof(char) * len * 6); // * 6 for unicode!
        char* p = buf;
        // iterate over str
        auto end = s + len;
        while(s < end) {
            if(*s == '\\') {
                s++;
                *p++ = *s++;
            }
            if(*s & 0x80) {
                // UTF-8 decode
                // todo
                throw std::runtime_error("UTF-8 not yet supported!");
            } else {
                *p++ = *s++;
            }
        }


        std::string decoded_str(buf);
        delete [] buf;
        return decoded_str;
    }

    return "UNDEFINED";
}