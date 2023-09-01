//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_RUNTIME_H
#define TUPLEX_RUNTIME_H

// this file defines external C functions accesible from within the Python/UDF Compiler. Functions should be prefixed
// with rt (no namespaces in C :/ )

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

/*!
 * controls how much memory the compiled codepath should use for malloc/free
 * @param size if 0, dynamic autogrowth is assumed
 */
extern void    setRunTimeMemory(const size_t size, size_t blockSize) noexcept;
extern size_t  getRunTimeMemorySize() noexcept;

/*!
 * needs to be called in order to free all memory as used by UDFs.
 */
extern void    freeRunTimeMemory() noexcept;

/*!
 * delete heap.
 */
extern void  releaseRunTimeMemory() noexcept;

/*!
 * returns address for memory block with given size
 * @param size
 * @return
 */
extern void*   rtmalloc(const size_t size) noexcept; // !!! do not change name without changing LLVMEnvironment.h malloc

/*!
 * frees memory block
 * @param ptr
 */
extern void     rtfree(void* ptr) noexcept;

/*!
 * frees all memory allocated by malloc at this point, i.e. garbage collection.
 * However, the C memory management is not invoked. (this is faster than always calling malloc/free)
 */
extern void    rtfree_all() noexcept; // !!! do not change without changing LLVMEnvironment.h freeAll

/***********
 * fast conversion functions
 * @Todo: Maybe later add llvm versions of them, i.e. by linking the module to further optimize the code
 */
extern int32_t fast_atoi64(const char *start, const char *end, int64_t* out);
extern int32_t fast_atod(const char *start, const char *end, double* out);
extern int32_t fast_atob(const char *start, const char *end, unsigned char *out);
extern int32_t fast_dequote(const char *start, const char *end, char **out, int64_t* size);

/*!
 * if necessary, return runtime allocated CSV quoted string, if not return string itself
 * @param str
 * @param size
 * @return
 */
extern char* quoteForCSV(const char *str, int64_t size, int64_t* new_size, char separator, char quotechar);

extern char* csvNormalize(const char quotechar, const char* start, const char* end, int64_t* ret_size);

// python3 compatible float to str function
// i.e. 0.0 is outputted to 0.0 instead of 0
// --> bug or feature in python3??
extern char* floatToStr(const double d, int64_t* res_size);

/******
 * String functions
 */
extern char* strCenter(const char* s, int64_t s_size, int64_t width, int64_t* res_size, const char fillchar);
extern char* strLower(const char* s, int64_t size);
extern const char* strLowerSIMD(const char *s, int64_t size);
extern char* strUpper(const char* s, int64_t size);
extern char* strSwapcase(const char* s, int64_t size);
extern char* strFormat(const char* fmt, int64_t* res_size, const char* argtypes, ...);
extern int64_t strRfind(const char* s, const char* needle);
extern char* strReplace(const char* str, const char* from, const char* to, int64_t* res_size);

extern char* strRStrip(const char* str, const char* chars, int64_t* res_size);
extern char* strLStrip(const char* str, const char* chars, int64_t* res_size);
extern char* strStrip(const char* str, const char* chars, int64_t* res_size);
extern int64_t strCount(const char* str, const char* sub, int64_t strSize, int64_t subSize);
extern int8_t strIsDecimal(const char* str);
extern int8_t strIsDigit(const char* str);
extern int8_t strIsAlpha(const char* str);
extern int8_t strIsAlNum(const char* str);

extern char* strJoin(const char *base_str, int64_t base_str_size, int64_t num_words, const char** str_array, const int64_t* len_array, int64_t* res_size);
extern int64_t strSplit(const char *base_str, int64_t base_str_length, const char *delim, int64_t delim_length, char*** res_str_array, int64_t** res_len_array, int64_t *res_list_size);

// string.capwords
extern char* stringCapwords(const char* str, int64_t size, int64_t *res_size);

// @TODO: str.title

/******
 * PCRE2 wrappers
 */
#include <pcre2.h>

// pcre2 match object wrappers
// Do not change without changing LLVMEnvironment::getMatchObjectPtrType()!!
struct matchObject {
    PCRE2_SIZE *ovector;
    char *subject;
    size_t subject_len;
};
extern matchObject* wrapPCRE2MatchObject(pcre2_match_data *match_data, char* subject, size_t subject_len);

// expose functions
extern pcre2_general_context* pcre2GetLocalGeneralContext();
extern void* pcre2GetGlobalGeneralContext();
extern void* pcre2GetGlobalMatchContext();
extern void* pcre2GetGlobalCompileContext();

// could get rid of these functions, it's a direct free call...
extern void pcre2ReleaseGlobalGeneralContext(void* gcontext);
extern void pcre2ReleaseGlobalMatchContext(void* mcontext);
extern void pcre2ReleaseGlobalCompileContext(void* ccontext);

// return a uniformly random integer on [start, end)
extern int64_t uniform_int(int64_t start, int64_t end);

// what about overflow?
extern int64_t pow_i64(int64_t base, int64_t exp);
extern double  pow_f64(double base, int64_t exp);

// python compatible python func for float
extern double rt_py_pow(double base, double exponent, int64_t* ecCode);

// this is a fix b.c. LLVM9 has a bug when it comes to storing double values into an array...
extern void llvm9_store_double(double* ptr, double value, int64_t idx);

#ifdef __cplusplus
}
#endif

#endif //TUPLEX_RUNTIME_H