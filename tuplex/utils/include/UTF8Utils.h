//
// Created by Leonhard Spiegelberg on 11/15/22.
//

#ifndef TUPLEX_UTF8UTILS_H
#define TUPLEX_UTF8UTILS_H

#include "Base.h"

namespace tuplex {
    // from https://gist.github.com/Inndy/10484418

// Reference: http://en.wikipedia.org/wiki/UTF-8
#define UTF8_MASK_C 0x080 // 0b10000000

#define UTF8_MASK_2 0x0C0 // 0b11000000
#define UTF8_MASK_3 0x0E0 // 0b11100000
#define UTF8_MASK_4 0x0F0 // 0b11110000
#define UTF8_MASK_5 0x0F8 // 0b11111000
#define UTF8_MASK_6 0x0FC // 0b11111100
#define UTF8_MKMASK(X) ((X) | (X) >> 1)
// Test leading bytes
#define UTF8_TEST_L(X, M) (((X) & UTF8_MKMASK(M)) == (M))
// Test body bytes
#define UTF8_TEST_B(X) (((X) & UTF8_MASK_C) == UTF8_MASK_C)

// check body bytes
    inline int utf8_check_bytes(unsigned char * data, int l) {
        while (l--)
            if (! UTF8_TEST_B(*data++))
                return 0;
        return 1;
    }

// get character size
    inline int utf8_get_char_size(unsigned char data) {
        if (data) {
            if (UTF8_TEST_L(data, UTF8_MASK_6)) {
                return 6;
            } else if (UTF8_TEST_L(data, UTF8_MASK_5)) {
                return 5;
            } else if (UTF8_TEST_L(data, UTF8_MASK_4)) {
                return 4;
            } else if (UTF8_TEST_L(data, UTF8_MASK_3)) {
                return 3;
            } else if (UTF8_TEST_L(data, UTF8_MASK_2)) {
                return 2;
            } else if ((data & UTF8_MASK_C) == 0) {
                return 1;
            }
        } else {
            return 0;
        }
        return -1;
    }

// this function returns a clamped buf_size n so the buffer represented by bytes
// 0, ..., n - 1 (i.e. the first n bytes) is valid UTF8.
    inline size_t utf8clamp(const char* buf, size_t buf_size) {
        if(0 == buf_size)
            return 0;

        // start (up to) 16 bytes ago and check for first valid sequence
        size_t max_pos = 0;
        for(unsigned i = std::min(16ul, buf_size); i > 0; i--) {
            char c = buf[buf_size - i];
            int rc = utf8_get_char_size(c);
            if(rc != -1) {
                // valid found.
                auto off = (size_t)(buf_size - i + rc);
                if(off < buf_size)
                    max_pos = std::max(max_pos, off);
            }
        }
        return max_pos + 1;
    }
}

#endif //TUPLEX_UTF8UTILS_H
