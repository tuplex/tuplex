//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Runtime.h>
#include <cctype>
#include <cassert>
#ifdef __x86_64__
#include <immintrin.h>
#endif
#include <stdexcept>
#include <cassert>
#include <algorithm>
#include <cctype>
#include <string>

// could do some of them in LLVM IR, but sometimes it is just easier to write them in plain C
// use e.g. https://github.com/JuliaStrings/utf8proc for UTF8 processing
// for now, only ASCII supported
// if memory is used, use rtmalloc!!!


static const char empty_string[1]{'\0'};

int8_t strIsDecimal(const char* str) {
    for (; *str; str++) {
        // check if the character is a valid ascii decimal (i.e. [0-9])
        if (*str < '0' || *str > '9')
            return 0;
    }
    return 1;
}

int8_t strIsDigit(const char* str) {
    for (; *str; str++) {
        if (!(isdigit(*str)))
            return 0;
    }
    return 1;
}

int8_t strIsAlpha(const char* str) {
    for (; *str; str++) {
        if (!(isalpha(*str)))
            return 0;
    }
    return 1;
}

int8_t strIsAlNum(const char* str) {
    for (; *str; str++) {
        if (!(isalnum(*str)))
            return 0;
    }
    return 1;
}

int64_t strCount(const char*str, const char *sub, int64_t strSize, int64_t subSize) {
    if (--subSize == 0 || --strSize < subSize)
        return 0;

    int count = 0;
    while ((str = strstr(str, sub))) {
        str += subSize;
        count++;
    }
    return count;
}

char* strLower(const char* s, int64_t size) {
    char* res = (char*)rtmalloc(size * sizeof(char));
    assert(res);
    assert(size > 0);

    for(int i = 0; i < size - 1; ++i) {

        // if(0 == s[i])
        //    printf("zero str encountered: %s", s);

        assert(s[i] != 0);
        assert(s[i] <= 127); // no unicode, only 7bit ASCII!!!

        res[i] = tolower(s[i]);
    }

    res[size - 1] = 0;

    return res;
}

char* strUpper(const char* s, int64_t size) {
    char* res = (char*)rtmalloc(size * sizeof(char));
    assert(res);
    assert(size > 0);

    for(int i = 0; i < size - 1; ++i) {
        assert(s[i] != 0);
        assert(s[i] <= 127); // no unicode, only 7bit ASCII!!!

        res[i] = toupper(s[i]);
    }

    res[size - 1] = 0;

    return res;
}

char* strSwapcase(const char* s, int64_t size) {
    char* res = (char*)rtmalloc(size * sizeof(char));
    // size of memory allocated is multiples of 16
    assert(res);
    assert(size > 0);

    // default: use base case
    // if possible, use __m512i if len(str) >= 64, use __m128i if len(rest of str) >= 16, use base case for the rest
    int use_512 = 0, use_128 = 0, use_base = size - 1;
    #if defined(__AVX512F__) && defined(__AVX512BW__)
        use_512 = use_base / 64;
        use_base = use_base % 64;
        if(use_512 > 0) {
            __m512i ua_mask = _mm512_set1_epi8('A');
            __m512i uz_mask = _mm512_set1_epi8('Z');
            __m512i la_mask = _mm512_set1_epi8('a');
            __m512i lz_mask = _mm512_set1_epi8('z');
            __m512i* dest = reinterpret_cast<__m512i*>(res);
            for(int i = 0; i < use_512; i++) {
                __m512i str_buf = _mm512_loadu_si512(s + i * 64);
                // check if each char is between A - Z or a - z, only set bits corresponding to letters
                __mmask64 cmp_ua = _mm512_cmpge_epu8_mask(str_buf, ua_mask);
                __mmask64 is_upper = _mm512_mask_cmple_epu8_mask(cmp_ua, str_buf, uz_mask);
                __mmask64 cmp_la = _mm512_cmpge_epu8_mask(str_buf, la_mask);
                __mmask64 is_lower = _mm512_mask_cmple_epu8_mask(cmp_la, str_buf, lz_mask);
                __mmask64 is_letter = _kor_mask64(is_upper, is_lower);
                // XOR with 0x20 swaps the case for each letter
                __m512i swap_mask = _mm512_maskz_set1_epi8(is_letter, 0x20);
                __m512i new_str = _mm512_xor_si512(str_buf, swap_mask);
                _mm512_storeu_si512(dest, new_str);
                dest++;
            }
        }
    #endif

    #ifdef __SSE2__
        use_128 = use_base / 16;
        use_base = use_base % 16;
        if(use_128 > 0) {
            __m128i ua_mask = _mm_set1_epi8('A');
            __m128i uz_mask = _mm_set1_epi8('Z');
            __m128i la_mask = _mm_set1_epi8('a');
            __m128i lz_mask = _mm_set1_epi8('z');
            __m128i swap_mask = _mm_set1_epi8(0x20);
            __m128i* dest = reinterpret_cast<__m128i*>(res + use_512 * 64);
            for(int i = 0; i < use_128; i++) {
                __m128i str_buf = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s + use_512 * 64 + i * 16));
                // check if each char is between A - Z or a - z, only set bits corresponding to letters
                __m128i cmp_ua = _mm_cmpeq_epi8(str_buf, _mm_max_epu8(str_buf, ua_mask));
                __m128i cmp_uz = _mm_cmpeq_epi8(str_buf, _mm_min_epu8(str_buf, uz_mask));
                __m128i cmp_la = _mm_cmpeq_epi8(str_buf, _mm_max_epu8(str_buf, la_mask));
                __m128i cmp_lz = _mm_cmpeq_epi8(str_buf, _mm_min_epu8(str_buf, lz_mask));
                __m128i is_letter = _mm_or_si128(_mm_and_si128(cmp_ua, cmp_uz), _mm_and_si128(cmp_la, cmp_lz));
                // XOR with 0x20 swaps the case for each letter
                __m128i new_str = _mm_xor_si128(str_buf, _mm_and_si128(is_letter, swap_mask));
                _mm_storeu_si128(dest, new_str);
                dest++;
            }
        }
    #endif

    if(use_base > 0) {
        for(int i = 0; i < use_base; i++) {
            int curr = use_512 * 64 + use_128 * 16 + i;
            if(s[curr] >= 65 && s[curr] <= 90) {
                res[curr] = s[curr] + 32;
            } else if(s[curr] >= 97 && s[curr] <= 122) {
                res[curr] = s[curr] - 32;
            } else {
                res[curr] = s[curr];
            }
        }
    }

    res[size - 1] = 0;
    return res;
}

// Returns a string represented as a character pointer. The input string “s” 
// of size input integer “s_len” is appended to a string of length “left” of 
// “fillchar” characters. A string of length “right” of “fillchar” characters 
// is then appended.
char* pad(const char* s, int64_t s_len, int64_t left, int64_t right, const char fillchar) {
    if (left < 0)
        left = 0;
    if (right < 0)
        right = 0;

    if (left == 0 && right == 0) {
        char* res = (char*)rtmalloc(s_len * sizeof(char));
        memcpy(res, s, s_len);
        return res;
    }

    char* res = (char*)rtmalloc((left + s_len + right) * sizeof(char));

    if (res) {
        if (left)
            memset(res, fillchar, left);

        memcpy(res + left, s, s_len);

        if (right)
            memset(res + left + s_len, fillchar, right);
    }

    res[left + s_len + right] = '\0';
    return res;
}

char* strCenter(const char* s, int64_t s_size, int64_t width, int64_t* res_size, const char fillchar) {
    int64_t s_len = s_size - 1;

    int64_t marg, left;

    if (s_len >= width) {
        *res_size = s_size;
        return strdup(s);
    }
    *res_size = width + 1;

    marg = width - s_len;
    left = marg / 2 + (marg & width & 1);

    return pad(s, s_len, left, marg - left, fillchar);
}

// Taken from: https://github.com/python/cpython/blob/master/Objects/unicodeobject.c (line 286)
const unsigned char ascii_whitespace[] = {
    0, 0, 0, 0, 0, 0, 0, 0,
/*     case 0x0009: * CHARACTER TABULATION */
/*     case 0x000A: * LINE FEED */
/*     case 0x000B: * LINE TABULATION */
/*     case 0x000C: * FORM FEED */
/*     case 0x000D: * CARRIAGE RETURN */
    0, 1, 1, 1, 1, 1, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
/*     case 0x001C: * FILE SEPARATOR */
/*     case 0x001D: * GROUP SEPARATOR */
/*     case 0x001E: * RECORD SEPARATOR */
/*     case 0x001F: * UNIT SEPARATOR */
    0, 0, 0, 0, 1, 1, 1, 1,
/*     case 0x0020: * SPACE */
    1, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,

    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0
};

// TODO: fix the 1 argument case for non-ascii characters
extern char* strRStrip(const char* str, const char* chars, int64_t* res_size) {
    int size = strlen(str);
    const char* end = str + size - 1;
    if(chars == nullptr) {
        while(end >= str && ascii_whitespace[*end]) { end--; size--; }
    } else {
        int numchars = strlen(chars);
        while(end >= str) {
            bool strinchars = false;
            for(int i=0; i<numchars && !strinchars; i++) {
                strinchars = (*end == chars[i]);
            }
            if(!strinchars) break;
            end--; size--;
        }
    }
    char* res = (char*)rtmalloc((size+1) * sizeof(char));
    for(int i=0; i<size; i++) res[i] = str[i];
    res[size] = '\0';
    *res_size = size;
    return res;
}

extern char* strLStrip(const char* str, const char* chars, int64_t* res_size) {
    int size = strlen(str);
    if(chars == nullptr) {
        while(ascii_whitespace[*str]) { str++; size--; }
    } else {
        int numchars = strlen(chars);
        while(size > 0) {
            bool strinchars = false;
            for(int i=0; i<numchars && !strinchars; i++) {
                strinchars = (*str == chars[i]);
            }
            if(!strinchars) break;
            str++; size--;
        }
    }
    char* res = (char*)rtmalloc((size+1) * sizeof(char));
    for(int i=0; i<size; i++) res[i] = str[i];
    res[size] = '\0';
    *res_size = size;
    return res;
}

extern char* strStrip(const char* str, const char* chars, int64_t *res_size) {
    int size = strlen(str);
    const char* end = str + size - 1;

    if(chars == nullptr) {
        while(ascii_whitespace[*str]) { str++; size--; }
        while(end >= str && isspace(*end)) { end--; size--; }
    } else {
        int numchars = strlen(chars);
        while(size > 0) {
            bool strinchars = false;
            for(int i=0; i<numchars && !strinchars; i++) {
                strinchars = (*str == chars[i]);
            }
            if(!strinchars) break;
            str++; size--;
        }
        while(end >= str) {
            bool strinchars = false;
            for(int i=0; i<numchars && !strinchars; i++) {
                strinchars = (*end == chars[i]);
            }
            if(!strinchars) break;
            end--; size--;
        }
    }

    char* res = (char*)rtmalloc((size+1) * sizeof(char));
    for(int i=0; i<size; i++) res[i] = str[i];
    res[size] = '\0';
    *res_size = size;

    return res;
}

inline bool ispywhitespace(char c) {
    return c == ' ' || c == '\t' ||
           c == '\n' || c == '\r' || c == '\x0b' || c == '\x0c';
}

// @TODO implement stringCapwords with optional separator!
// string.capwords (ignore optional separator)
// ==> i.e. this here is the None/absent case!
extern char* stringCapwords(const char* str, int64_t size, int64_t *res_size) {

    assert(res_size);
    assert(strlen(str) + 1 == size);

    // go through string and capitalize each word
    // NOTE: the help description is
    // capwords(s, sep=None)
    //    capwords(s [,sep]) -> string
    //
    //    Split the argument into words using split, capitalize each
    //    word using capitalize, and join the capitalized words using
    //    join.  If the optional second argument sep is absent or None,
    //    runs of whitespace characters are replaced by a single space
    //    and leading and trailing whitespace are removed, otherwise
    //    sep is used to split and join the words.

    // remove leading and trailing whitespace
    // string whitespace is ' \t\n\r\x0b\x0c' (import string; string.whitespace
    auto start = str;
    auto end = str + size - 1;
    while(start < end && (*start == ' ' || *start == '\t' ||
                          *start == '\n' || *start == '\r' || *start == '\x0b' || *start == '\x0c'))
        start++;
    end--; // ignore trailing char
    while(end > start && (*end == ' ' || *end == '\t' ||
                          *end == '\n' || *end == '\r' || *end == '\x0b' || *end == '\x0c'))
        end--;
    end++; // add last char back
    // white space --

    auto out_size = end - start;

    // runtime alloc string of size (this overallocates but it's fine b.c. usually tiny strings)
    char *out = (char*)rtmalloc((out_size + 1) * sizeof(char));
    out[out_size - 1] = '\0';

    // ignore whitespace runs...

    size_t lastWordStart = 0;
    size_t pos = 0;
    for(int i = 0; i < out_size; ++i) {
        char c = *(start + i);

        // whitespace? do only add if last output was NOT whitespace
        if(ispywhitespace(c)) {
            if(i > 0 && !ispywhitespace(*(start + i - 1))) {
                out[pos++] = ' ';
                // capitalize lastWordStart
                out[lastWordStart] = toupper(out[lastWordStart]);

                // lastWordStart comes AFTER whitespace
                lastWordStart = pos;
            }
        } else {
            // lower
            // perform the word normalization
            out[pos++] = tolower(c);
        }
    }
    if(lastWordStart < pos)
        out[lastWordStart] = toupper(out[lastWordStart]);

    // zero terminate string, i.e. set last char to 0
    out[pos] = '\0';

    if(res_size)
        *res_size = pos + 1; // actual reported size after removing whitespace.

    assert(*res_size <= out_size + 1);

    return out;
}

// https://stackoverflow.com/questions/28939652/how-to-detect-sse-sse2-avx-avx2-avx-512-avx-128-fma-kcvi-availability-at-compile
// fast, SIMD backed lower function
// for general case UTF-8 strings, http://site.icu-project.org/ should be used
// --> speculate on UTF-8 strings!
const char * strLowerSIMD(const char *s, int64_t size) {
    // empty string, early branch out
    if(size <= 1)
        return empty_string;

#ifndef NDEBUG
    auto original_size = size;
#endif

    // alloc new string
    char* res = (char*)rtmalloc(size);

//    // assume 7bit ASCII and en locale
//    // basic version
//    for(int i = 0; i < size; ++i) {
//        auto c = s[i];
//        if('A' <= c && c <= 'Z')
//            c += 'a' - 'A'; // lower case letters come after upper case letters
//        res[i] = c;
//    }

    // start with the highest available vector width (AVX-512 or AVX-2)
    int pos = 0;
#if defined(__AVX512F__) && defined(__AVX512BW__)
    // 64 byte width
#endif
#ifdef __AVX__
    // 32 byte width
    while(size >= 32) {

    }
#endif

#ifdef __SSE2__
    // 16 byte width
    __m128i ua_mask = _mm_set1_epi8('A');
    __m128i uz_mask = _mm_set1_epi8('Z');
    __m128i swap_mask = _mm_set1_epi8(0x20);
    while(size >= 16) {
        // create SSE2 masks
        __m128i* dest = reinterpret_cast<__m128i*>(res + pos);
        __m128i str_buf = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s + pos));
        // check if each char is between A - Z or a - z, only set bits corresponding to letters
        __m128i cmp_ua = _mm_cmpeq_epi8(str_buf, _mm_max_epu8(str_buf, ua_mask));
        __m128i cmp_uz = _mm_cmpeq_epi8(str_buf, _mm_min_epu8(str_buf, uz_mask));
        auto is_upper_letter = _mm_and_si128(cmp_ua, cmp_uz);

        // XOR with 0x20 swaps the case for each letter
        __m128i new_str = _mm_xor_si128(str_buf, _mm_and_si128(is_upper_letter, swap_mask));
        _mm_storeu_si128(dest, new_str);

        pos += 16;
        size -= 16;
    }
#endif

    // do rest of string with regular instructions
    while(size > 0) {
        assert(pos < original_size);
        auto c = s[pos];
        if('A' <= c && c <= 'Z')
            c += 'a' - 'A'; // lower case letters come after upper case letters
        res[pos] = c;
        pos++;
        size--;
    }

    return res;
}

// @TODO: str.title