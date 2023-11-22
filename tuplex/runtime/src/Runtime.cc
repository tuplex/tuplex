//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <cstdint> // required for gcc
#include <cassert>
#include <cmath>
#include <Runtime.h>
#include <mt/hashmap.h>
#include <string>
#include <thread>
#include <StringUtils.h> // <-- implemented in StringUtils
#include <ExceptionCodes.h>
#include <fmt/format.h>
#include <fmt/args.h>
#include <pcre2.h>
#include <Logger.h>
#include <random>
#include <cstdio>
#include <cstdlib> // for aligned_malloc

#define RUNTIME_DEFAULT_BLOCK_SIZE (4 * 1024 * 1024)

// there's a bug on apple platforms. not aligned_malloc in libstdc++, hence define using posix_memalign
#if defined(__APPLE__)
// void *aligned_alloc( size_t alignment, size_t size );
void* aligned_alloc(size_t alignment, size_t size) {
    void * pointer;
    posix_memalign(&pointer, alignment, size); // free with regular free. Note: On Mac OS X returned memory is per default 16 bytes aligned.
    return pointer;
}
// for other platforms,
// aligned malloc is part of stdlib
#endif



//#ifdef __cplusplus
//extern "C" {
//#endif

struct MemoryBlock {
    uint8_t *mem;
    size_t size;
    size_t offset;
    struct MemoryBlock *next;
};


#include <unordered_map>
#include <thread>

//! wrapper structure for one memory heap
class MemoryHeap {
public:
    MemoryBlock *memory;            //! first block
    MemoryBlock *lastBlock;         //! last block
    std::size_t defaultBlockSize;   //! block size to use to allocate a new block
    std::size_t runTimeMemorySize;  //! size of memory allocated in this heap

    MemoryHeap() : memory(nullptr), lastBlock(nullptr), defaultBlockSize(RUNTIME_DEFAULT_BLOCK_SIZE), runTimeMemorySize(0ul)   {}

    // non copyable
    MemoryHeap(MemoryHeap const& other) = delete;
    MemoryHeap& operator = (MemoryHeap const& other) = delete;
};

//static tuplex::hashmap<std::thread::id, std::shared_ptr<MemoryHeap>> heaps;



// confer https://akkadia.org/drepper/tls.pdf
// and https://fuchsia.dev/fuchsia-src/development/threads/tls

// --> i.e. when using static here, can be lowered to local dynamic compared to global dynamic (expensive tls_get_addr call!)
// use thread local to index heap
static thread_local MemoryHeap* heap = nullptr;

static const size_t HEAP_ALIGNMENT = 16ul;
static inline size_t alignHeapSize(size_t size) {
    return (((size) + ((HEAP_ALIGNMENT) - 1)) & ~((HEAP_ALIGNMENT) - 1));
}

/*!
 * helper function to init a memory block
 * @param size
 * @return
 */
static struct MemoryBlock *initMemoryBlock(const size_t size) {

    auto *node = (struct MemoryBlock *) malloc(sizeof(struct MemoryBlock));

    if (!node) {
        printf("Runtime error: Failed to allocate new memoryblock!");
        exit(1);
        return nullptr;
    }

    node->size = size;
    node->offset = 0;
    node->next = nullptr;
    node->mem = (uint8_t*)aligned_alloc(HEAP_ALIGNMENT, size);

    if (!node->mem) {
        printf("Runtime error: Failed to allocate new memoryblock!");
        exit(1);
        return nullptr;
    }
    return node;
}


/*!
 * helper that makes sure at least one block is initialized correctly
 * @param heap
 */
static void lazyGuard(MemoryHeap *heap) {
    // make sure heap is initialized, if not init a first block.
    if(!heap->memory) {
        heap->memory = initMemoryBlock(heap->defaultBlockSize);
        heap->lastBlock = heap->memory;
    }
}

/*!
 * externally called runtime function to (re)set the runtime memory that can be used from LLVM code
 * @param size
 */
extern "C" void setRunTimeMemory(const size_t size, size_t blockSize) noexcept {
    if(!heap)
        heap = new MemoryHeap();

    freeRunTimeMemory();
    if (blockSize == 0)
        blockSize = RUNTIME_DEFAULT_BLOCK_SIZE;

    heap->defaultBlockSize = blockSize;
    heap->runTimeMemorySize = size;

    heap->memory = nullptr;
    heap->lastBlock = nullptr;

    lazyGuard(heap);
}

extern "C" void freeRunTimeMemory() noexcept {
    if(!heap)
        heap = new MemoryHeap();

    // go through chain & delete memory

    auto cur = heap->memory;
    while(cur) {
        auto block = cur;
        if(block->mem) {
            free(block->mem);
            block->mem = nullptr;
        }
        block->size = 0;
        block->offset = 0;
        cur = cur->next;

        free(block);
        block = nullptr;
    }

    heap->memory = nullptr;
    heap->lastBlock = nullptr;
}

extern "C" void releaseRunTimeMemory() noexcept {
   if(heap) {
       freeRunTimeMemory();

       delete heap;
   }
   heap = nullptr;
}

// grow simply
extern "C" void *rtmalloc(const size_t requested_size) noexcept {
#ifndef NDEBUG
    // check size of memory, if larger than 128MB abort
    auto rtsize = getRunTimeMemorySize();
    if(rtsize > 1024 * 1024 * 256) {
        printf("runtime memory size exceeded 256MB");
        exit(1);
        return nullptr;
    }
#endif

    if(!heap)
        heap = new MemoryHeap();

    size_t size = alignHeapSize(requested_size); // align to nearest multiple of heap alignment

    assert(size >= requested_size);

    // make sure size does not exceed default block size!
    if (size > heap->defaultBlockSize) {

        if (0 == heap->defaultBlockSize) {
            printf("fatal error: forgot to call setRunTimeMemory()\n");
        }
        // @Todo: maybe increase block size?
        // @Todo: Request super large block?
        printf("fatal error: Requested object size %lu, is larger than default block size %lu! Can't handle memory request!\n", size, heap->defaultBlockSize);
        exit(-1);
        return NULL;
    }

    lazyGuard(heap);

    // check if current block enough space is left
    // if not, add a new block!
    if (heap->lastBlock->offset + size > heap->lastBlock->size) {

        // check if lastBlock has successor. If yes, try to find a memory location there!
        if (heap->lastBlock->next) {
            heap->lastBlock = heap->lastBlock->next;
            return rtmalloc(size);
        } else {
            if (0 == heap->runTimeMemorySize) {
                // just add a block
                heap->lastBlock->next = initMemoryBlock(heap->defaultBlockSize);
                heap->lastBlock = heap->lastBlock->next;
                return rtmalloc(size);
            } else {
                // get total allocated size if in non-dynamic mode
                // add with remaining memory budget
                struct MemoryBlock *block = heap->memory;
                assert(block);
                size_t totalAllocated = 0;
                while (block->next) {
                    totalAllocated += block->size;
                    block = block->next;
                }

                // still memory in budget left?
                if (totalAllocated < heap->runTimeMemorySize) {
                    // allocate new block with whatever is smaller:
                    // defaultBlocksize or leftover memory
                    size_t availableMemory = heap->runTimeMemorySize - totalAllocated < heap->defaultBlockSize ?
                                             heap->runTimeMemorySize - totalAllocated : heap->defaultBlockSize;

                    heap->lastBlock->next = initMemoryBlock(availableMemory);
                    heap->lastBlock = heap->lastBlock->next;
                    return rtmalloc(size);

                } else {
                    printf("exceeded runtime memory budget. OUT OF MEMORY. quitting execution.");
                    exit(-1);
                    return NULL;
                }
            }
        }
    } else {
        assert(heap->lastBlock);
        uint8_t *memaddr = heap->lastBlock->mem + heap->lastBlock->offset;

        // inc offset
        heap->lastBlock->offset += size;
        return memaddr;
    }
}

// no complicated memory management needed. Simply free (i.e. reset memory regions)
// after a row is executed (saves allocation cost)
extern "C" void rtfree(void *ptr) noexcept {
    // do nothing...
}

extern "C" void rtfree_all() noexcept {
    if(!heap)
        heap = new MemoryHeap();

#ifdef TRACE_MEMORY
    auto before_clean = getRunTimeMemorySize();
#endif

    // simply set lastblock to start and clear all offsets!
    heap->lastBlock = heap->memory;
    struct MemoryBlock *block = heap->memory;

    // if allocated, clear offsets...
    if(block) {
       while (block->next) {
           block->offset = 0;
           block = block->next;
       }
    }

#ifdef TRACE_MEMORY
    auto after_clean = getRunTimeMemorySize();
    if(before_clean != after_clean)
        printf("freed bytes %d -> %d\n", (int)before_clean, (int)after_clean);
#endif
}

extern "C" size_t getRunTimeMemorySize() noexcept {
    if(!heap)
        heap = new MemoryHeap();

    struct MemoryBlock *node = heap->memory;
    size_t size = 0;
    while(node) {
        size += node->size;
        node = node->next;
    }
    return size;
}

// convert functions
extern "C" int32_t fast_atoi64(const char *start, const char *end, int64_t* out) {
    using namespace tuplex;


    // whitespace --
    // remove trailing whitespace
    // string whitespace is ' \t\n\r\x0b\x0c' (import string; string.whitespace
    while(start < end && (*start == ' ' || *start == '\t' ||
          *start == '\n' || *start == '\r' || *start == '\x0b' || *start == '\x0c'))
        start++;
    end--; // ignore trailing char
    while(end > start && (*end == ' ' || *end == '\t' ||
                          *end == '\n' || *end == '\r' || *end == '\x0b' || *end == '\x0c'))
        end--;
    end++; // add last char back
    // white space --


    auto code = tuplex::fast_atoi64(start, end, out);

    // translate parse err to python ValueError
    return i32ToEC(code) != ExceptionCode::SUCCESS ? ecToI32(ExceptionCode::VALUEERROR) : ecToI32(ExceptionCode::SUCCESS);
}

extern "C" int32_t fast_atod(const char *start, const char *end, double* out) {
    using namespace tuplex;

    // whitespace --
    // remove trailing whitespace
    // string whitespace is ' \t\n\r\x0b\x0c' (import string; string.whitespace
    while(start < end && (*start == ' ' || *start == '\t' ||
                          *start == '\n' || *start == '\r' || *start == '\x0b' || *start == '\x0c'))
        start++;
    end--; // ignore trailing char
    while(end > start && (*end == ' ' || *end == '\t' ||
                          *end == '\n' || *end == '\r' || *end == '\x0b' || *end == '\x0c'))
        end--;
    end++; // add last char back

    // white space --

    auto code = tuplex::fast_atod(start, end, out);

    // translate parse err to python ValueError
    return i32ToEC(code) != ExceptionCode::SUCCESS ? ecToI32(ExceptionCode::VALUEERROR) : ecToI32(ExceptionCode::SUCCESS);
}

// note the unsigned char here. necessary because bool might be different across platforms...
extern "C" int32_t fast_atob(const char *start, const char *end, unsigned char *out) {
    using namespace tuplex;
    static_assert(sizeof(unsigned char) == 1, "char must be 1 byte");


    // Note: there are some weird conversion rules for bool

    // casting fun
    bool outtmp = false;
    auto code = tuplex::fast_atob(start, end, &outtmp);
    *out = (unsigned char)outtmp;

    // translate parse err to python ValueError
    return i32ToEC(code) != ExceptionCode::SUCCESS ? ecToI32(ExceptionCode::VALUEERROR) : ecToI32(ExceptionCode::SUCCESS);
}

extern "C" int32_t fast_dequote(const char *start, const char *end, char **out, int64_t* size) {
    return tuplex::fast_dequote(start, end, '\"', out, size, rtmalloc);
}

extern "C" int64_t strRfind(const char* s, const char* needle) {
    // use C++ implementation for rfind (uses anyways string view internally)
    std::string sv(s);
    auto idx = sv.rfind(needle);

    if(idx == std::string::npos)
        return -1;
    else
        return (int64_t)idx;
}


// source of this is https://creativeandcritical.net/str-replace-c
// with slightly modified memory management
extern "C" char* strReplace(const char* str, const char* from, const char* to, int64_t* res_size) {

    // special cases: empty string

    if(str[0] == '\0') {
        *res_size = 1;
        return (char*)str;
    }

    // if both strings are empty, return original string
    if(from[0] == '\0' && to[0] == '\0') {
        *res_size = (int64_t)strlen(str);
        return (char*)str;
    }

    // 'test'.replace('', ',')
    // yields
    //',t,e,s,t,'
    if(from[0] == '\0') {
        // special case alloc
        auto len = strlen(str);

        // alloc mem
        auto tolen = strlen(to);
        auto retlen = (tolen + 1) * (1 + len); //@TODO: check this, seems wrong.

        *res_size = retlen + 1;

        char* res = (char*)rtmalloc(retlen + 1);
        // copy to and one char from original str
        int pos = 0;
        for(int i = 0; i < len; ++i) {
            for(int j = 0; j < tolen; ++j)
                res[pos++] = to[j];
            res[pos++] = str[i];
        }
        for(int j = 0; j < tolen; ++j)
            res[pos++] = to[j];
        res[pos] = '\0';

        // bug here when it comes to length.
        *res_size = strlen(res) + 1; // todo, optimize this in the future. this here is a quick fix.

        return res;
    }

    // both from/to are non-zero
    /* Adjust each of the below values to suit your needs. */

    /* Increment positions cache size initially by this number. */
    size_t cache_sz_inc = 16;
    /* Thereafter, each time capacity needs to be increased,
     * multiply the increment by this factor. */
    const size_t cache_sz_inc_factor = 3;
    /* But never increment capacity by more than this number. */
    const size_t cache_sz_inc_max = 1048576;

    char *pret = nullptr, *ret = nullptr;
    const char *pstr2, *pstr = str;
    size_t i = 0, count = 0;
#if (__STDC_VERSION__ >= 199901L)
    uintptr_t *pos_cache_tmp, *pos_cache = NULL;
#else
    ptrdiff_t *pos_cache_tmp = nullptr, *pos_cache = nullptr;
#endif
    size_t cache_sz = 0;
    size_t cpylen = 0, orglen = 0, retlen = 0, tolen = 0, fromlen = strlen(from);

    /* Find all matches and cache their positions. */
    while ((pstr2 = strstr(pstr, from)) != nullptr) {
        count++;

        /* Increase the cache size when necessary. */
        if (cache_sz < count) {
            cache_sz += cache_sz_inc;
            pos_cache_tmp = (ptrdiff_t *)realloc(pos_cache, sizeof(*pos_cache) * cache_sz);
            if (pos_cache_tmp == nullptr) {
                goto end_repl_str;
            } else pos_cache = pos_cache_tmp;
            cache_sz_inc *= cache_sz_inc_factor;
            if (cache_sz_inc > cache_sz_inc_max) {
                cache_sz_inc = cache_sz_inc_max;
            }
        }

        pos_cache[count-1] = pstr2 - str;
        pstr = pstr2 + fromlen;
    }

    orglen = pstr - str + strlen(pstr);

    /* Allocate memory for the post-replacement string. */
    if (count > 0) {
        tolen = strlen(to);
        retlen = orglen + (tolen - fromlen) * count;
    } else	retlen = orglen;
    ret = (char*)rtmalloc(retlen + 1);
    if (ret == nullptr) {
        goto end_repl_str;
    }

    if (count == 0) {
        /* If no matches, then just duplicate the string. */
        strcpy(ret, str);
    } else {
        /* Otherwise, duplicate the string whilst performing
         * the replacements using the position cache. */
        pret = ret;
        memcpy(pret, str, pos_cache[0]);
        pret += pos_cache[0];
        for (i = 0; i < count; i++) {
            memcpy(pret, to, tolen);
            pret += tolen;
            pstr = str + pos_cache[i] + fromlen;
            cpylen = (i == count-1 ? orglen : pos_cache[i+1]) - pos_cache[i] - fromlen;
            memcpy(pret, pstr, cpylen);
            pret += cpylen;
        }
        ret[retlen] = '\0';
    }

end_repl_str:
    /* Free the cache and return the post-replacement string,
     * which will be NULL in the event of an error. */
    free(pos_cache);
    pos_cache = nullptr;

    // put out size
    assert(res_size);
    *res_size = retlen + 1;

    return ret;
}

// helper function to replace undefined floating point formats with correct ones
std::string replace_with_float_default_format(const std::string& fmt, const std::string& argtypes) {

    auto default_float_fmt = "{:#g}";

    unsigned pos = 0;
    std::string new_fmt;
    unsigned argpos = 0;
    unsigned startpos = 0;
    while(pos < fmt.size()) {
        auto curchar = fmt[pos];
        auto nextchar = pos + 1 < fmt.size() ? fmt[pos + 1] : 0;

        if(curchar == '{' && nextchar == '{') {
            new_fmt += "{{";
            pos += 2;
        } else if(curchar == '}' && nextchar == '}') {
            new_fmt += "}}";
            pos += 2;
        } else if(curchar == '{') {
            startpos = pos;

            // special case: {} and arg is float
            if(argpos < argtypes.size() && 'f' == argtypes[argpos] && nextchar == '}') {
                new_fmt += default_float_fmt;
                pos += 2;
            } else {
                new_fmt.push_back(curchar);
                pos++;
            }
        } else if(curchar == '}') {
            argpos++;
            new_fmt.push_back(curchar);
            pos++;
        } else {
            new_fmt.push_back(curchar);
            pos++;
        }
    }
    return new_fmt;
}

/*!
 * strFormat function with variable number of arguments. Supports formatting for bool, int, float, str.
 * No support for tuples or other objects yet.
 * @param str
 * @param res_size
 * @param argtypes helper string to parse type info during runtime
 * @param ...
 * @return
 */
extern "C" char* strFormat(const char *str, int64_t *res_size, const char* argtypes, ...) {
    using namespace std;

    // Note: define at some point custom memory allocator on top of rtmalloc for fmtlib
    //       cf. https://fmt.dev/latest/api.html

    string res;
    fmt::dynamic_format_arg_store<fmt::format_context> store;

    char *ret = nullptr;

    // empty fmt string leads to empty result
    if(str[0] == '\0') {
        *res_size = 1;
        return (char*)str;
    }

    // retrieve the arguments
    va_list argp;
    va_start(argp, argtypes);
    bool found_float = false;
    auto original_argtypes = argtypes;
    int num_args = (int)strlen(argtypes);
    for(int i=0; i<num_args; ++i) {
        char type = *argtypes++;
        if(type == 's') {
            const char *val = va_arg(argp, char*);
            store.push_back(val);
        } else if(type == 'f') {
            double val = va_arg(argp, double);
            store.push_back(val);
            found_float = true;
        } else if(type == 'b') { // boolean
            static_assert(sizeof(long long) == 8, "long long must be 8 bytes");
            int64_t val = va_arg(argp, long long);

            // convert to python strings, because fmt::fmtlib uses true and false instead of True and False
            if(val)
                store.push_back("True");
            else
                store.push_back("False");
        } else if(type == 'd') {
            static_assert(sizeof(long long) == 8, "long long must be 8 bytes");
            int64_t val = va_arg(argp, long long);
            store.push_back(val);
        } else {

            char ut[2] = {type, '\0'};
            Logger::instance().defaultLogger().error("formatting error, found unknown type '" + std::string(ut) + "' in fmt string. Returning empty string.");

            // return empty string
            ret = (char*)rtmalloc(1);
            *ret = '\0';
            va_end(argp);
            return ret;
        }
    }
    va_end(argp);

    // to be compatible with default floating point format (here g), convert non-defined floating point formats
    auto fmt_str = str;
    std::string new_fmt;
    if(found_float) {
        // make the formatting call
        new_fmt = replace_with_float_default_format(str, original_argtypes);
        fmt_str = new_fmt.c_str();
    }

    // make the formatting call
    res = fmt::vformat(fmt_str, store);
    *res_size = res.length() + 1;
    ret = (char*)rtmalloc((size_t)*res_size);
    memcpy(ret, res.c_str(), (size_t)*res_size);

    return ret;
}

char* strJoin(const char *base_str, int64_t base_str_size, int64_t num_words, const char** str_array, const int64_t* len_array, int64_t* res_size) {
    if(num_words == 0) { // empty list case
        char *ret = (char*)rtmalloc(1);
        ret[0] = 0;
        *res_size = 1;
        return ret;
    }

    // calculate total size
    int64_t total_size = (num_words - 1) * (base_str_size - 1); // joins in between words
    for(int64_t i=0; i<num_words; i++) { // add on word lengths
        total_size += len_array[i] - 1;
    }
    total_size += 1; // null terminator

    // allocate space
    char *ret = (char*)rtmalloc(total_size);

    // build result
    int64_t copied_so_far = 0;
    for(int64_t i=0; i<num_words; i++) {
        memcpy(ret + copied_so_far, str_array[i], (size_t)len_array[i] - 1); // copy in word
        copied_so_far += len_array[i] - 1;
        if(i < num_words - 1) { // if we're not at the last word
            memcpy(ret + copied_so_far, base_str, base_str_size - 1);
            copied_so_far += base_str_size - 1;
        }
    }
    ret[total_size-1] = 0; // null terminate

    // return
    *res_size = total_size;
    return ret;
}

int64_t strSplit(const char *base_str, int64_t base_str_length, const char *delim, int64_t delim_length, char*** res_str_array, int64_t** res_len_array, int64_t* res_list_size) {
    delim_length--; base_str_length--; // remove null terminator from length

    int64_t serialized_size = sizeof(int64_t); // number of elements

    // @TODO: spend ~1h to optimize this.
    // build result
    // use std::tuple<>
    std::vector<std::pair<int, int>> result; // start, length
    const char *next;
    const char *prev = base_str;
    while((next = strstr(prev, delim)) != nullptr) {
        result.emplace_back(prev - base_str, next-prev);
        prev = next + delim_length;
    }
    if(prev < base_str + base_str_length) { // there is a string left over at the end
        result.emplace_back(prev - base_str, (base_str + base_str_length) - prev);
    }

    // allocate space in runtime
    assert(sizeof(char**) == sizeof(int64_t));
    char** rtstr_array = (char**)rtmalloc(result.size() * sizeof(char**));
    int64_t* rtlen_array = (int64_t*)rtmalloc(result.size() * sizeof(int64_t));
    for(size_t i = 0; i < result.size(); i++) {
        rtlen_array[i] = result[i].second + 1;
        rtstr_array[i] = (char*)rtmalloc((size_t)rtlen_array[i]);
        memcpy(rtstr_array[i], base_str + result[i].first, (size_t)result[i].second);
        rtstr_array[i][result[i].second] = 0;
        serialized_size += sizeof(int64_t) + rtlen_array[i]; // each string stores its size and the string
    }

    // return
    *res_str_array = rtstr_array;
    *res_len_array = rtlen_array;
    *res_list_size = result.size();
    return serialized_size;
}

extern "C" char* quoteForCSV(const char *str, int64_t size, int64_t* new_size, char separator, char quotechar) {

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
        char *res = (char*)rtmalloc(resSize);
        char *p = res;

        *p = quotechar; ++p;
        for(unsigned i = 0; i < len; ++i) {
            if(str[i] == quotechar) {
                *p = quotechar; ++p;
                *p = quotechar; ++p;
            } else {
                *p = str[i]; ++p;
            }
        }
        *p = quotechar; ++p;
        *p = '\0';

        if(new_size)
            *new_size = resSize;

        return res;

    } else {
        // special case: null quoted string??
        if(str[size - 1] != '\0') {
            // return string with trailing 0
            auto resSize = size;
            char *res = (char*)rtmalloc(resSize);
            memcpy(res, str, size);
            res[size - 1] = '\0';

            if(new_size)
                *new_size = size;
            return res;
        }


        if(new_size)
            *new_size = size;
        return (char*)str;
    }
}

extern "C" char* floatToStr(const double d, int64_t* res_size) {
    // Todo: For faster implementation adapt https://github.com/ulfjack/ryu!

    // use g
    // use 40 bytes goodwill buf
    const size_t BUFFER_SIZE = 40;
    char* buf = (char*)rtmalloc(BUFFER_SIZE + 2); // security +2 to adjust for .0!
    auto bytes_written = snprintf(buf, BUFFER_SIZE, "%g", d);
    if(bytes_written > BUFFER_SIZE) {
        buf = (char*)rtmalloc(bytes_written + 1 + 2); // security +2 to adjust for .0 special case
        bytes_written = snprintf(buf, BUFFER_SIZE, "%g", d);
    }

    // adjust for weird python behavior
    bool dotFound = false;
    const char* p = buf;

    // ignore special vals inf/nan/...
    while(*p != '\0') {
        if(*p == '.') {
            dotFound = true;
            break;
        }
       p++;
    }

    if(!dotFound) {
        // add .0 chars if not special val
        if(bytes_written > 0)
            if(*buf == '-' || (*buf >= '0' && *buf <= '9')) {
                if(*(buf + 1) == '\0' || (*(buf + 1) >= '0' && *(buf + 1) <= '9')) {
                    buf[bytes_written] = '.';
                    buf[bytes_written + 1] = '0';
                    buf[bytes_written + 2] = '\0';
                    bytes_written += 2;
                }
            }
    }

    assert(res_size);
    *res_size = bytes_written + 1;

    return buf;
}

// helper function for CSV parser
//  std::string as_str()
//        {
//            auto s = std::string(ptr, size);
//            if(escaped) {
//                int o = 0;
//                for(size_t i = 0; i < s.size();) {
//                    char c = s[i];
//                    if((escapechar && c == escapechar) || (c == quotechar)) {
//                        i++;
//                    }
//                    s[o++] = s[i++];
//                }
//                s.resize(o);
//            }
//            return s;
//        }

char* csvNormalize(const char quotechar, const char* start, const char* end, int64_t* ret_size) {
    // copy over
    auto len = end - start;
    auto size = len + 1;

    char* res = (char*)rtmalloc(size);
    // memset(res, 0, size);

    // copy over unless quote char!
    const char* ptr = start;
    int i = 0;
    while(ptr < end) {
        if(*ptr == quotechar)
            ptr++;
        res[i++] = *ptr;
        ptr++;
    }

    // important, set last to 0 (if not 0)
    if('\0' != res[i])
        res[i++] = '\0';

    // adjust length (find first non-'\0' char)
    while(i > 0 && res[i - 1] == '\0')
        --i;

    if(ret_size)
        *ret_size = i + 1;

    return res;
}

// pcre2 wrappers
matchObject* wrapPCRE2MatchObject(pcre2_match_data *match_data, char* subject, size_t subject_len) {
    auto ret = (matchObject*)rtmalloc(sizeof(matchObject));
    ret->ovector = pcre2_get_ovector_pointer(match_data);
    ret->subject = subject;
    ret->subject_len = subject_len; // includes the +1 from null terminator
    return ret;
}

// wrap rtmalloc as memory allocator for pcre2
// data is ignored user data arg
static void *pcre2_malloc(PCRE2_SIZE size, void *user_data) {
    return rtmalloc((size_t)size);
}

static void pcre2_free(void *ptr, void *user_data) {
    rtfree(ptr);
}

// pcre2 context wrappers (redone to cast)
struct tplx_pcre2_memctl {
    void *    (*malloc)(size_t, void *);
    void      (*free)(void *, void *);
    void      *memory_data;
};

struct tplx_pcre2_general_context {
    tplx_pcre2_memctl memctl;
};

struct tplx_pcre2_compile_context {
    tplx_pcre2_memctl memctl;
    int (*stack_guard)(uint32_t, void *);
    void *stack_guard_data;
    const uint8_t *tables;
    PCRE2_SIZE max_pattern_length;
    uint16_t bsr_convention;
    uint16_t newline_convention;
    uint32_t parens_nest_limit;
    uint32_t extra_options;
};

struct tplx_pcre2_match_context {
    tplx_pcre2_memctl memctl;
    pcre2_jit_callback jit_callback;
    void *jit_callback_data;
    int    (*callout)(pcre2_callout_block *, void *);
    void    *callout_data;
    int    (*substitute_callout)(pcre2_substitute_callout_block *, void *);
    void    *substitute_callout_data;
    PCRE2_SIZE offset_limit;
    uint32_t heap_limit;
    uint32_t match_limit;
    uint32_t depth_limit;
};

pcre2_general_context *pcre2GetLocalGeneralContext() {
    return pcre2_general_context_create(pcre2_malloc, pcre2_free, nullptr);
}

void *pcre2GetGlobalGeneralContext() {
    tplx_pcre2_general_context *gcontext = reinterpret_cast<tplx_pcre2_general_context *>(pcre2_general_context_create(
            nullptr, nullptr, nullptr));
    if (gcontext == nullptr) return nullptr;

    // reassign with our struct definition
    gcontext->memctl.malloc = pcre2_malloc;
    gcontext->memctl.free = pcre2_free;
    gcontext->memctl.memory_data = nullptr;
    return gcontext;
}

void *pcre2GetGlobalMatchContext() {
    tplx_pcre2_match_context *mcontext =
            reinterpret_cast<tplx_pcre2_match_context *>(pcre2_match_context_create(nullptr));
    if(mcontext == nullptr) return nullptr;

    // reassign with our struct definition
    mcontext->memctl.malloc = pcre2_malloc;
    mcontext->memctl.free = pcre2_free;
    mcontext->memctl.memory_data = nullptr;
    return mcontext;
}

void *pcre2GetGlobalCompileContext() {
    tplx_pcre2_compile_context *ccontext =
            reinterpret_cast<tplx_pcre2_compile_context *>(pcre2_compile_context_create(nullptr));
    if (ccontext == nullptr) return nullptr;

    // reassign with our struct definitions
    ccontext->memctl.malloc = pcre2_malloc;
    ccontext->memctl.free = pcre2_free;
    ccontext->memctl.memory_data = nullptr;
    return ccontext;
}

// @TODO: could get rid off these, no merit in keeping...
void pcre2ReleaseGlobalGeneralContext(void* gcontext) {
    if(gcontext)
        free(gcontext);
    gcontext = nullptr;
}
void pcre2ReleaseGlobalMatchContext(void* mcontext) {
    if(mcontext)
        free(mcontext);
    mcontext = nullptr;
}
void pcre2ReleaseGlobalCompileContext(void* ccontext) {
    if(ccontext)
        free(ccontext);
    ccontext = nullptr;
}


#warning "should fix build process to include the required functions"
void temporary_linker_hack() {
    int x;
    size_t y;
    pcre2_code *tmp = pcre2_compile(reinterpret_cast<PCRE2_SPTR>("x"), 1, 0, &x, &y, nullptr);
    pcre2_match_data* match_data = pcre2_match_data_create_from_pattern(tmp, nullptr);
    pcre2_match(tmp, reinterpret_cast<PCRE2_SPTR8>("x"), 1, 0, 0, match_data, nullptr);
    char buf[20];
    size_t buf_size = 20;
    pcre2_substitute(tmp, reinterpret_cast<PCRE2_SPTR>("x"), 1, 0, 0, match_data, nullptr, reinterpret_cast<PCRE2_SPTR>("s"), 1,
                     reinterpret_cast<PCRE2_UCHAR8 *>(buf), &buf_size);
    pcre2_jit_compile(tmp, 1);
    pcre2_code_free(tmp);
}

int64_t uniform_int(int64_t start, int64_t end) {

    // notes: maybe use http://www.digicortex.net/node/22?
    // or https://lemire.me/blog/2019/03/19/the-fastest-conventional-random-number-generator-that-can-pass-big-crush/?
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int64_t> dist(start, end-1);
    return dist(generator);
}

int64_t pow_i64(int64_t base, int64_t exp) {
    assert(exp >= 0);
    // check for overflow??

    if(0 == exp)
        return 1;
    int64_t temp = pow_i64(base, exp / 2);
    if(exp & 0x1)
        return temp * temp * base;
    else
        return temp * temp;
}

double pow_f64(double base, int64_t exp) {
    assert(exp >= 0);
    if(0 == exp)
        return 1.0;
    double temp = pow_f64(base, exp / 2);
    if(exp & 0x1)
        return temp * temp * base;
    else
        return temp * temp;
}

inline int64_t DOUBLE_IS_ODD_INTEGER(double x) {
    return (fmod(fabs(x), 2.0) == 1.0);
}

double rt_py_pow(double base, double exponent, int64_t* ecCode) {
    using namespace tuplex;
    // use epsilon for float comparison? NO, because CPython does not either -.-
    if(ecCode)
        *ecCode = ecToI64(ExceptionCode::SUCCESS);

    // special cases, code adapted from https://github.com/python/cpython/blob/442ad74fc2928b095760eb89aba93c28eab17f9b/Objects/floatobject.c#L707
    if(0 == exponent) { // base ** 0 is 1, even 0 ** 0
        return 1.0;
    }
    if(NAN == base) { // nan ** exponent = nan, unless exponent = 0 (handled above)
        return NAN;
    }
    if(NAN == exponent) { // base ** nan = nan, unless base = 1, then 1 ** nan == 1
        return base == 1.0 ? 1.0 : NAN;
    }

    if(std::isinf(exponent)) {
        // base ** inf is: 0.0 if abs(base) < 1, 1.0 if abs(base) == 1, inf if abs(base) > 1
       auto abs_base = fabs(base);
       if(abs_base == 1.0)
           return 1.0;
       if((exponent > 0.0) == (abs_base > 1.0))
           return fabs(exponent);
       return 0.0;
    }

    if(std::isinf(base)) {
        // (+-inf) ** exponent is: inf for exponent positive, 0 for exponent negative.
        // in both cases, need to add appropriate sign if exponent is odd integer.
        int64_t exponent_is_odd = DOUBLE_IS_ODD_INTEGER(exponent);
        if(exponent > 0.0)
            return exponent_is_odd ? base : fabs(base);
        else
            return exponent_is_odd ? copysign(0.0, base) : 0.0;
    }

    if(base == 0.0) {
        // 0 ** exponent is: 0 for exponent positive, 1 for exponent zero (above handled) and error if exponent is negative
        int64_t exponent_is_odd = DOUBLE_IS_ODD_INTEGER(exponent);
        if(exponent < 0.0) {
            // handle via ecCode
            assert(ecCode);
            *ecCode = ecToI64(ExceptionCode::ZERODIVISIONERROR);
            return NAN;
        }
        // handle +/- 0
        return exponent_is_odd ? base : 0.0;
    }

    int negate_result = false;

    if(base < 0.0) {
        // Quote from python:
        //  /* Whether this is an error is a mess, and bumps into libm
        //         * bugs so we have to figure it out ourselves.
        //         */
        if(exponent != floor(exponent)) {
            // negative numbers raised to non-integers become complex
            // => normalcase violation b.c. we dont support complex type yet
            assert(ecCode);
            *ecCode = ecToI64(ExceptionCode::NORMALCASEVIOLATION);
            return NAN;
        }
        base = -base;
        negate_result = DOUBLE_IS_ODD_INTEGER(exponent);
    }

    if(base == 1.0) {
        // 1 ** exponent is 1, even 1 ** inf and 1 ** nan.
        // (-1) ** ... also ends up here
        return negate_result ? -1.0 : 1.0;
    }

    // regular C-pow function now
    errno = 0;
    auto res = pow(base, exponent);
    // ignore python overflow/underflow issues!
    // Py_ADJUST_ERANGE1 -> they say anyways this is not reliable...
    if(negate_result)
        res = -res;

    if(errno != 0) {
        // Quote from python:
        // /* We don't expect any errno value other than ERANGE, but
        //         * the range of libm bugs appears unbounded.
        //         */
        assert(ecCode);
        *ecCode = errno == ERANGE ? ecToI64(ExceptionCode::OVERFLOWERROR) : ecToI64(ExceptionCode::VALUEERROR);
        return NAN;
    }

    return res;
}

int fallback_spanner(const char* ptr, const char c1, const char c2, const char c3, const char c4) {
    if(!ptr)
        return 16;

    char charset[256];
    memset(charset, 0, 256);
    charset[c1] = 1;
    charset[c2] = 1;
    charset[c3] = 1;
    charset[c4] = 1;

    // manual implementation
    auto p = (const unsigned char *)ptr;
    auto e = p + 16;

    do {
        if(charset[p[0]]) {
            break;
        }
        if(charset[p[1]]) {
            p++;
            break;
        }
        if(charset[p[2]]) {
            p += 2;
            break;
        }
        if(charset[p[3]]) {
            p += 3;
            break;
        }
        p += 4;
    } while(p < e);

    if(! *p) {
        return 16; // PCMPISTRI reports NUL encountered as no match.
    }

    auto ret =  p - (const unsigned char *)ptr;
    return ret;
}


//#ifdef __cplusplus
//}
//#endif