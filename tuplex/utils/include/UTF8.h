//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_UTF8_H
#define TUPLEX_UTF8_H

#include <string>
#include <cstdint>
#include "Base.h"
#include <cassert>

// provides some basic functions for handling utf8
// later use ICU 59.1 for this
// right now there are issues with cmake, so custom methods for now...

inline bool isNonASCII(const unsigned char c) {
    return c >= 128;
}

// for utf8 encoded int
inline bool isNonASCII(const int32_t i) {
    return (static_cast<unsigned char>(i) >= 128);
}

inline int utf8ByteCount(unsigned char c) {
    if(c < 128)
        return 1;

    // [0b1100 0000 - 0b1110 0000 )
    if(c >= 0b11000000 && c < 0b11100000)
        return 2;
    // [0b1110 0000 - 0b1111 0000 )
    if(c >= 0b11100000 && c < 0b11110000)
        return 3;
    // [0b1111 0000 - 0b1111 0111 ]
    if(c >= 0b11110000 && c <= 0b11110111)
        return 4;

    // there are also some invalid bytes!
    return -1;
}

// converts the first found utf8 char of the array to an int
inline int32_t utf8toi(const char* utf8) {

    // use little endian layout for storage
    assert(core::isLittleEndian());

    // UTF8 is encoded as follows (cf. https://en.wikipedia.org/wiki/UTF-8)
    //                              Byte 1	    Byte 2	    Byte 3	    Byte 4
    // 1	7	U+0000	U+007F	    0xxxxxxx
    // 2	11	U+0080	U+07FF	    110xxxxx	10xxxxxx
    // 3	16	U+0800	U+FFFF	    1110xxxx	10xxxxxx	10xxxxxx
    // 4	21	U+10000	U+10FFFF	11110xxx	10xxxxxx	10xxxxxx	10xxxxxx

    if(!utf8)
        return -1;

    int32_t i = 0;
    i = utf8[0] & 0xFF;



    // check if non ascii character is present
    if(utf8[0] != '\0' && isNonASCII(utf8[0])) {

        // determine how many utf8 bytes are present (= j)
        int j = utf8ByteCount(utf8[0] & 0xFF);

        // Note: This can be rewritten into a loop,
        // however later a better encoding might be desired.
        // hence, leave things simple!
        switch(j) {
            case 2:
                assert(isNonASCII(utf8[0]));
                assert(isNonASCII(utf8[1]));
                assert(utf8[0] & 0xC0);
                assert(utf8[1] & 0x80);

                return (static_cast<int32_t>(utf8[0] & 0xFF)  << 8 ) |
                       static_cast<int32_t>(utf8[1] & 0xFF);
            case 3:
                assert(isNonASCII(utf8[0]));
                assert(isNonASCII(utf8[1]));
                assert(isNonASCII(utf8[2]));
                assert(utf8[0] & 0xE0);
                assert(utf8[1] & 0x80);
                assert(utf8[2] & 0x80);
                return (static_cast<int32_t>(utf8[0] & 0xFF) << 16 ) |
                       (static_cast<int32_t>(utf8[1] & 0xFF) << 8 ) |
                       (static_cast<int32_t >(utf8[2] & 0xFF));
            case 4:
                assert(isNonASCII(utf8[0]));
                assert(isNonASCII(utf8[1]));
                assert(isNonASCII(utf8[2]));
                assert(utf8[0] & 0xF0);
                assert(utf8[1] & 0x80);
                assert(utf8[2] & 0x80);
                assert(utf8[3] & 0x80);
                return (static_cast<int32_t>(utf8[0] & 0xFF) << 24 ) |
                        (static_cast<int32_t>(utf8[1] & 0xFF) << 16 ) |
                        (static_cast<int32_t >(utf8[2] & 0xFF) << 8 ) |
                        (static_cast<int32_t>(utf8[3] & 0xFF));
            default:
                DEBUG_ERROR("invalid number of bytes detected!");
                return -1;
        }

    }

    return i;
}

inline std::string utf8itostr(const int32_t i) {
    // little endian layout
    assert(core::isLittleEndian());

    std::string str = "";
    str.reserve(4);

    // determine how many bytes are used (from byte 0!)
    int j = 1;
    if(i & 0xFF00)j++;
    if(i & 0xFF0000)j++;
    if(i & 0xFF000000)j++;

    switch(j) {
        case 1:
            str += static_cast<char>(i & 0xFF);
            break;
        case 2:
            str += static_cast<char>((i >> 8) & 0xFF);
            str += static_cast<char>(i & 0xFF);
            break;
        case 3:
            str += static_cast<char>((i >> 16) & 0xFF);
            str += static_cast<char>((i >> 8) & 0xFF);
            str += static_cast<char>(i & 0xFF);
            break;
        case 4:
            str += static_cast<char>((i >> 24) & 0xFF);
            str += static_cast<char>((i >> 16) & 0xFF);
            str += static_cast<char>((i >> 8) & 0xFF);
            str += static_cast<char>(i & 0xFF);
            break;
        default:
            DEBUG_ERROR("invalid number of bytes detected!");
            return "";
    }

    return str;
}

#endif //TUPLEX_UTF8_H