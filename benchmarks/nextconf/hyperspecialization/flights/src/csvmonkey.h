// from https://github.com/dw/csvmonkey
// commit https://github.com/dw/csvmonkey/commit/110e6046615205b711c590e9fec2e5eade32a714
// modifies by adding ptr()


#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#if defined(__SSE4_2__) && !defined(CSM_IGNORE_SSE42)
#define CSM_USE_SSE42
#include <emmintrin.h>
#include <smmintrin.h>
#endif // __SSE4_2__

#ifdef USE_SPIRIT
#include "boost/spirit/include/qi.hpp"
#endif


#ifdef CSVMONKEY_DEBUG
#   define CSM_DEBUG(x, ...) fprintf(stderr, "csvmonkey: " x "\n", ##__VA_ARGS__);
#else
#   define CSM_DEBUG(x...) {}
#endif


namespace csvmonkey {


    class StreamCursor;


    template<class StreamCursorType=StreamCursor>
    class CsvReader;


    class Error : public std::exception
    {
        std::string s_;

    public:
        Error(const char *category, const std::string &s)
                : s_(category)
        {
            s_.append(": ");
            s_.append(s);
        }

        virtual const char *
        what() const throw()
        {
            return s_.c_str();
        }
    };


    class StreamCursor
    {
    public:
        /**
         * Current stream position. Must guarantee access to buf()[0..size()+15],
         * with 31 trailing NULs to allow safely running PCMPSTRI on the final data
         * byte.
         */
        virtual const char *buf() = 0;
        virtual size_t size() = 0;
        virtual void consume(size_t n) = 0;
        virtual bool fill() = 0;
    };


    class MappedFileCursor
            : public StreamCursor
    {
        char *startp_;
        char *endp_;
        char *p_;
        char *guardp_;

        size_t get_page_size()
        {
            return (size_t) sysconf(_SC_PAGESIZE);
        }

    public:
        MappedFileCursor()
                : startp_(0)
                , endp_(0)
                , p_(0)
                , guardp_(0)
        {
        }

        ~MappedFileCursor()
        {
            if(startp_) {
                ::munmap(startp_, endp_ - startp_);
            }
            if(guardp_) {
                ::munmap(guardp_, get_page_size());
            }
        }

        const char *buf()
        {
            return p_;
        }

        size_t size()
        {
            return endp_ - p_;
        }

        void consume(size_t n)
        {
            p_ += std::min(n, (size_t) (endp_ - p_));
            CSM_DEBUG("consume(%lu); new size: %lu", n, size())
        }

        bool fill()
        {
            return false;
        }

        void open(const char *filename)
        {
            int fd = ::open(filename, O_RDONLY);
            if(fd == -1) {
                throw Error(filename, strerror(errno));
            }

            struct stat st;
            if(fstat(fd, &st) == -1) {
                ::close(fd);
                throw Error("fstat", strerror(errno));
            }

            // UNIX sucks. We can't use MAP_FIXED to ensure a guard page appears
            // after the file data because it'll silently overwrite mappings for
            // unrelated stuff in RAM (causing bizarro unrelated errors and
            // segfaults). We can't rely on the kernel's map placement behaviour
            // because it varies depending on the size of the mapping (guard page
            // ends up sandwiched between .so mappings, data file ends up at bottom
            // of range with no space left before first .so). We can't parse
            // /proc/self/maps because that sucks and is nonportable and racy. We
            // could use random addresses pumped into posix_mem_offset() but that
            // is insane and likely slow and non-portable and racy.
            //
            // So that leaves us with: make a MAP_ANON mapping the size of the
            // datafile + the guard page, leaving the kernel to pick addresses,
            // then use MAP_FIXED to overwrite it. We can't avoid the MAP_FIXED
            // since there would otherwise be a race between the time we
            // mmap/munmap to find a usable address range, and another thread
            // performing the same operation. So here we exploit crap UNIX
            // semantics to avoid a race.

            unsigned long page_size = get_page_size();
            unsigned long page_mask = page_size - 1;
            size_t rounded = (st.st_size & page_mask)
                             ? ((st.st_size & ~page_mask) + page_size)
                             : st.st_size;

            auto startp = (char *) mmap(0, rounded+page_size, PROT_READ,
                                        MAP_ANON|MAP_PRIVATE, 0, 0);
            if(! startp) {
                ::close(fd);
                throw Error("mmap", "could not allocate guard page");
            }

            guardp_ = startp + rounded;
            startp_ = (char *) mmap(startp, st.st_size, PROT_READ,
                                    MAP_SHARED|MAP_FIXED, fd, 0);
            ::close(fd);

            if(startp_ != startp) {
                CSM_DEBUG("could not place data below guard page (%p) at %p, got %p.",
                          guardp_, startp, startp_);
                throw Error("mmap", "could not place data below guard page");
            }

            ::madvise(startp_, st.st_size, MADV_SEQUENTIAL);
            ::madvise(startp_, st.st_size, MADV_WILLNEED);
            endp_ = startp_ + st.st_size;
            p_ = startp_;
        }
    };


    class BufferedStreamCursor
            : public StreamCursor
    {
    protected:
        std::vector<char> vec_;
        size_t read_pos_;
        size_t write_pos_;

        virtual ssize_t readmore() = 0;

#ifndef NDEBUG
        BufferedStreamCursor()
                : vec_(2048), read_pos_(0), write_pos_(0) { // small buffer for testing
        }
#else
        BufferedStreamCursor()
                : vec_(131072), read_pos_(0), write_pos_(0) {
        }
#endif
    protected:
        void ensure(size_t capacity)
        {
            size_t available = vec_.size() - write_pos_;
            if(available < capacity) {
                CSM_DEBUG("resizing vec_ %lu", (size_t)(vec_.size() + capacity));
                vec_.resize(32 + (vec_.size() + capacity));
            }
        }

    public:

        void reset() {
            for(int i = 0; i < vec_.size(); ++i)
                vec_[i] = 0;
            read_pos_ = 0;
            write_pos_ = 0;
        }

        const char *buf()
        {
            return &vec_[0] + read_pos_;
        }

        size_t size()
        {
            return write_pos_ - read_pos_;
        }

        void consume(size_t n)
        {
            read_pos_ += std::min(n, write_pos_ - read_pos_);
            CSM_DEBUG("consume(%lu); new size: %lu", n, size())
        }

        virtual bool fill()
        {
            if(read_pos_) {
                size_t n = write_pos_ - read_pos_;
                CSM_DEBUG("read_pos_ needs adjust, it is %lu / n = %lu", read_pos_, n);
                memcpy(&vec_[0], &vec_[read_pos_], n);
                CSM_DEBUG("fill() adjust old write_pos = %lu", write_pos_);
                write_pos_ -= read_pos_;
                read_pos_ = 0;
                CSM_DEBUG("fill() adjust new write_pos = %lu", write_pos_);
            }

            if(write_pos_ >= vec_.size()) {
                ensure(vec_.size() / 2);
            }

            ssize_t rc = readmore();
            if(rc == -1) {
                CSM_DEBUG("readmore() failed");
                return false;
            }

            CSM_DEBUG("readmore() succeeded")
            CSM_DEBUG("fill() old write_pos = %lu", write_pos_);
            write_pos_ += rc;
            CSM_DEBUG("fill() new write_pos = %lu", write_pos_);
            return write_pos_ > 0;
        }
    };


    class FdStreamCursor
            : public BufferedStreamCursor
    {
        int fd_;

    public:
        FdStreamCursor(int fd)
                : BufferedStreamCursor()
                , fd_(fd)
        {
        }

        virtual ssize_t readmore()
        {
            return ::read(fd_, &vec_[write_pos_], vec_.size() - write_pos_);
        }
    };


    struct CsvCell
    {
        const char *ptr;
        size_t size;

        char escapechar;
        char quotechar;
        bool escaped;

        std::string as_str()
        {
            auto s = std::string(ptr, size);
            if(escaped) {
                int o = 0;
                for(size_t i = 0; i < s.size();) {
                    char c = s[i];
                    if((escapechar && c == escapechar) || (c == quotechar)) {
                        i++;
                    }
                    s[o++] = s[i++];
                }
                s.resize(o);
            }
            return s;
        }

        bool startswith(const char *str) const
        {
            return std::string(ptr, std::min(size, strlen(str))) == str;
        }

        bool equals(const char *str) const
        {
            auto p = ptr;
            for(auto len = size; len--;) {
                if((! *p) || *str++ != *p++) {
                    return false;
                }
            }
            return true;
        }

        double as_double()
        {
#ifdef USE_SPIRIT
            namespace qi = boost::spirit::qi;
        using qi::double_;
        double n;
        qi::parse(ptr, ptr+size, double_, n);
        return n;
#else
            return strtod(ptr, NULL);
#endif
        }

        unsigned long as_ulong() {
          return strtoul(ptr, NULL, 10);
        }

        uint8_t as_uint8() {
          return (uint8_t)as_ulong();
        }
        uint16_t as_uint16() {
          return (uint16_t)as_ulong();
        }
    };


    struct FieldPair
    {
        const char *name;
        CsvCell **cell;
    };


#ifndef CSM_USE_SSE42
    #warning Using non-SSE4.2 fallback implementation.
/**
 * Callable that matches a set of up to 5 bytes (including NUL) in a 16 byte
 * string. The index 0..15 of the first occurrence is returned, otherwise 16 is
 * returned if no match is found or NUL is encountered.
 */
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
        charset_[0] = 1;
    }

    size_t
    operator()(const char *s)
        __attribute__((__always_inline__))
    {
        CSM_DEBUG("bitfield[32] = %d", charset_[32]);
        CSM_DEBUG("span[0] = {%d,%d,%d,%d,%d,%d,%d,%d}",
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]);
        CSM_DEBUG("span[1] = {%d,%d,%d,%d,%d,%d,%d,%d}",
            s[8], s[9], s[10], s[11], s[12], s[13], s[14], s[15]);

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

        if(! *p) {
            return 16; // PCMPISTRI reports NUL encountered as no match.
        }

        return p - (const unsigned char *)s;
    }
};

using StringSpanner = StringSpannerFallback;
#   define CSM_ATTR_SSE42
#endif // !CSM_USE_SSE42


#ifdef CSM_USE_SSE42
    struct alignas(16) StringSpannerSse42
    {
        __m128i v_;

        StringSpannerSse42(char c1=0, char c2=0, char c3=0, char c4=0)
        {
            assert(! ((reinterpret_cast<intptr_t>(&v_) & 15)));
            __v16qi vq = {c1, c2, c3, c4};
            v_ = (__m128i) vq;
        }

        size_t __attribute__((__always_inline__, target("sse4.2")))
        operator()(const char *buf)
        {
            return _mm_cmpistri(
                    v_,
                    _mm_loadu_si128((__m128i *) buf),
                    0
            );
        }
    };

    using StringSpanner = StringSpannerSse42;
#   define CSM_ATTR_SSE42 __attribute__((target("sse4.2")))
#endif // CSM_USE_SSE42


    class CsvCursor
    {
    public:
        std::vector<CsvCell> cells;
        size_t count;

        CsvCursor()
                : cells()
                , count(0)
        {
        }

        bool
        by_value(const std::string &value, CsvCell *&cell)
        {
            for(size_t i = 0; i < count; i++) {
                if(value == cells[i].as_str()) {
                    cell = &cells[i];
                    return true;
                }
            }
            return false;
        }
    };


    template<class StreamCursorType>
    class alignas(16) CsvReader
    {
        const char *endp_;
        const char *p_;
        char delimiter_;
        char quotechar_;
        char escapechar_;
        bool yield_incomplete_row_;

    public:
        bool in_newline_skip;

        const char *ptr() { return p_;}

    private:
        StreamCursorType &stream_;
        StringSpanner quoted_cell_spanner_;
        StringSpanner unquoted_cell_spanner_;
        CsvCursor row_;

        enum CsmTryParseReturnType {
            kCsmTryParseOkay,
            kCsmTryParseOverflow,
            kCsmTryParseUnderrun
        };

        CsmTryParseReturnType
        try_parse()
        CSM_ATTR_SSE42
        {
            const char *p = p_;
            const char *cell_start;
            int rc, rc2;

            CsvCell *cell = &row_.cells[0];
            row_.count = 0;

#define PREAMBLE() \
            if(p >= endp_) {\
                CSM_DEBUG("pos exceeds size"); \
                return kCsmTryParseUnderrun; \
            } \
            CSM_DEBUG("p = %#p; remain = %ld; next char is: %d", p, endp_-p, (int)*p) \
            CSM_DEBUG("%d: distance to next newline: %d", __LINE__, strchr(p, '\n') - p);

#define NEXT_CELL() \
            ++cell; \
            if(row_.count == row_.cells.size()) { \
                CSM_DEBUG("cell array overflow"); \
                return kCsmTryParseOverflow; \
            }

            CSM_DEBUG("remain = %lu", endp_ - p);
            CSM_DEBUG("ch = %d %c", (int) *p, *p);

            newline_skip:
            /*
             * Skip newlines appearing at the start of the line, which may be a
             * result of DOS/MAC-formatted input. Or a double-spaced CSV file.
             */
            in_newline_skip = true;
            PREAMBLE()
            if(*p == '\r' || *p == '\n') {
                ++p;
                goto newline_skip;
            }

            cell_start:
            in_newline_skip = false;
            PREAMBLE()
            cell->escaped = false;
            if(*p == '\r' || *p == '\n') {
                /*
                 * A newline appearing after at least one cell has been read
                 * indicates the presence of a single comma demarcating an unquoted
                 * unquoted unquoted unquoted empty final field.
                 */
                cell->ptr = 0;
                cell->size = 0;
                ++row_.count;
                p_ = p + 1;
                return kCsmTryParseOkay;
            } else if(*p == quotechar_) {
                cell_start = ++p;
                goto in_quoted_cell;
            } else {
                cell_start = p;
                goto in_unquoted_cell;
            }

            in_quoted_cell:
        PREAMBLE()
            rc = quoted_cell_spanner_(p);
            rc2 = quoted_cell_spanner_(p+16);
            if(rc != 16) {
                p += rc + 1;
                goto in_escape_or_end_of_quoted_cell;
            }

            switch(rc2) {
                case 16:
                    p += 32;
                    goto in_quoted_cell;
                default:
                    p += rc2 + 1 + 16;
                    goto in_escape_or_end_of_quoted_cell;
            }

            in_escape_or_end_of_quoted_cell:
        PREAMBLE()
            if(*p == delimiter_) {
                cell->ptr = cell_start;
                cell->size = p - cell_start - 1;
                ++row_.count;
                NEXT_CELL();
                ++p;
                goto cell_start;
            } else if(*p == '\r' || *p == '\n') {
                cell->ptr = cell_start;
                cell->size = p - cell_start - 1;
                ++row_.count;
                p_ = p + 1;
                return kCsmTryParseOkay;
            } else {
                cell->escaped = true;
                ++p;
                goto in_quoted_cell;
            }

            in_unquoted_cell:
            CSM_DEBUG("\n\nin_unquoted_cell")
            PREAMBLE()
            rc = unquoted_cell_spanner_(p);
            CSM_DEBUG("unquoted span: %d; p[3]=%d p[..17]='%.17s'", rc, p[3], p);
            rc2 = unquoted_cell_spanner_(p+16);
            if(rc != 16) {
                p += rc;
                goto in_escape_or_end_of_unquoted_cell;
            }

            switch(rc2) {
                case 16:
                    p += 32;
                    goto in_unquoted_cell;
                default:
                    p += rc2 + 16;
                    goto in_escape_or_end_of_unquoted_cell;
            }

            in_escape_or_end_of_unquoted_cell:
        PREAMBLE()
            if(*p == delimiter_) {
                cell->ptr = cell_start;
                cell->size = p - cell_start;
                ++row_.count;
                CSM_DEBUG("in_escape_or_end_of_unquoted_cell(DELIMITER)")
                CSM_DEBUG("p[..17] = '%.17s'", p)
                CSM_DEBUG("done cell: '%.*s'", (int)cell->size, cell->ptr)
                NEXT_CELL();
                ++p;
                goto cell_start;
            } else if(*p == '\r' || *p == '\n') {
                CSM_DEBUG("in_escape_or_end_of_unquoted_cell(NEWLINE)")
                cell->ptr = cell_start;
                cell->size = p - cell_start;
                ++row_.count;
                p_ = p + 1;
                return kCsmTryParseOkay;
            } else {
                cell->escaped = true;
                ++p;
                goto in_unquoted_cell;
            }

            CSM_DEBUG("error out");
            return kCsmTryParseUnderrun;
        }

#undef PREAMBLE
#undef NEXT_CELL

    public:

        /**
         * Extract CsvCell pointers to fields with a particular value. Used as a
         * convenience for parsing the header row into a list of desired columns.
         * Throws csvmonkey::Error if a desired column is not found in the row.
         *
         * @example
         *      CsvCell *resource_id;
         *      CsvCell *item_description;
         *
         *      if(! reader.read_row()) {
         *          throw Error("cannot parse header row");
         *      }
         *
         *      reader.extract_fields({
         *          {"ResourceId", &resource_id},
         *          {"ItemDescription", &item_description},
         *      });
         */
        void
        extract_fields(const std::vector<FieldPair> &pairs)
        {
            for(const auto &pair : pairs) {
                if(! row_.by_value(pair.name, *pair.cell)) {
                    std::string e("Could not find required header: ");
                    e.append(pair.name);
                    throw Error("extract_fields", e);
                }
            }
        }

        void
        _resize()
        {
            auto &cells = row_.cells;
            auto size = cells.size() * 2;
            if(! size) {
                size = 32;
            }

            cells.resize(size);
            // For as_str()
            for(size_t i = 0; i < size; i++) {
                CsvCell &cell = cells[i];
                cell.quotechar = quotechar_;
                cell.escapechar = escapechar_;
            }
        }

        bool
        read_row()
        {
            const char *p;
            CSM_DEBUG("")

            do {
                p = stream_.buf();
                p_ = p;
                endp_ = p + stream_.size();
                switch(try_parse()) {
                    case kCsmTryParseOkay:
                        stream_.consume(p_ - p);
                        return true;
                    case kCsmTryParseOverflow:
                        _resize();
                        return read_row();
                    case kCsmTryParseUnderrun:
                        ;
                }
                CSM_DEBUG("attempting fill!")
            } while(stream_.fill());

            if(row_.count && yield_incomplete_row_) {
                CSM_DEBUG("stream fill failed, but partial row exists")
                stream_.consume(endp_ - p);
                return true;
            }

            CSM_DEBUG("stream fill failed")
            return false;
        }

        CsvCursor &
        row()
        {
            return row_;
        }

        CsvReader(StreamCursorType &stream,
                  char delimiter=',',
                  char quotechar='"',
                  char escapechar=0,
                  bool yield_incomplete_row=false)
                : endp_(stream.buf() + stream.size())
                , p_(stream.buf())
                , delimiter_(delimiter)
                , quotechar_(quotechar)
                , escapechar_(escapechar)
                , yield_incomplete_row_(yield_incomplete_row)
                , stream_(stream)
                , quoted_cell_spanner_(quotechar, escapechar)
                , unquoted_cell_spanner_(delimiter, '\r', '\n', escapechar)
        {
            _resize();
        }
    };


} // namespace csvmonkey
