//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_BASE_H
#define TUPLEX_BASE_H

#include <string>
#include <cstring>
#include <unordered_set>
#include <algorithm>

// gcc fix
#include <cstring>
#include <cstdlib>

#include <sstream>
#include <set>
#include <vector>


// to detect platform, use here boost predef
#include <boost/predef.h>
#if BOOST_OS_WINDOWS
#define WINDOWS
#elif BOOST_OS_LINUX
#define LINUX
#elif BOOST_OS_MACOS
#define MACOS
#endif

// assert we are only targeting Mac OS X and Linux for now
#if !(defined(LINUX) || defined(MACOS))
#error "Tuplex only targeted at MacOS X and Linux for now");
#endif

// framework is designed for 64Bit platforms. Hence, here the macros to ensure
// it is compiled against 64bit
#if _WIN32 || _WIN64
#if _WIN64
     #define ENV64BIT
  #else
    #define ENV32BIT
  #endif
#endif
#if __GNUC__
#if __x86_64__ || __ppc64__
#define ENV64BIT
#else
#define ENV32BIT
#endif
#endif


// GCC typedefs for atomic types
#if defined(__GNUC__) && (__GNUC___ < 7)

#include <atomic>
#include <cstdint>
#include <thread>
namespace std {
    /// atomic_int8_t
    typedef atomic<int8_t>		atomic_int8_t;

    /// atomic_uint8_t
    typedef atomic<uint8_t>		atomic_uint8_t;

    /// atomic_int16_t
    typedef atomic<int16_t>		atomic_int16_t;

    /// atomic_uint16_t
    typedef atomic<uint16_t>	atomic_uint16_t;

    /// atomic_int32_t
    typedef atomic<int32_t>		atomic_int32_t;

    /// atomic_uint32_t
    typedef atomic<uint32_t>	atomic_uint32_t;

    /// atomic_int64_t
    typedef atomic<int64_t>		atomic_int64_t;

    /// atomic_uint64_t
    typedef atomic<uint64_t>	atomic_uint64_t;
}
#endif

#ifdef ENV64BIT
static_assert(sizeof(void *) == 8, "framework arch must be 64bit compliant");

// define 64bit ptr type which can be used for ptr arithmetic
static_assert(sizeof(int64_t) == 8, "int64 not matching expected size");

typedef int64_t* ptr_t;

#endif

#ifdef ENV32BIT
#error "no 32bit support yet"
static_assert(sizeof(void *) == 4, "framework arch must be 32bit compliant");

// define 32bit ptr type which can be used for ptr arithmetic
static_assert(sizeof(int32_t) == 4, "int32 not matching expected size");

typedef int32_t* ptr_t;

#endif


#define EXCEPTION(message) throw Exception((message), __FILE__, __LINE__);

// define __FILE_NAME__ and __FILENAME__ macros

#ifndef __FILE_NAME__
#define __FILE_NAME__ \
  (strchr(__FILE__, '\\') \
  ? ((strrchr(__FILE__, '\\') ? strrchr(__FILE__, '\\') + 1 : __FILE__)) \
  : ((strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)))
#endif

#ifndef __FILENAME__
#define __FILENAME __FILE_NAME__
#endif


// some basic helper to throw for possible bad code in debug mode an error (should never occur in release mode!)
void debug_error_message(const char* message, const char* file, const int ine);
#define DEBUG_ERROR(message) debug_error_message((message), __FILE__, __LINE__);

class Exception {
private:
    std::string _message;
    std::string _file;
    int _lineNumber;
public:

    Exception(const std::string& message,
              const std::string& file,
              const int lineNumber) : _message(message), _file(file), _lineNumber(lineNumber) {}

    std::string getMessage() const {
        std::stringstream ss;
        ss <<_file<<"+"<<_lineNumber<<": "<<_message;
        return ss.str();
    }
};

// need to namespace everything due to GCC's internal problems
namespace core {


    // some basic helper functions
    enum class Endianness {
        LITTLE,
        BIG
    };

    inline Endianness determineEndianness() {
        int num = 1;
        if(*(char *)&num == 1)
            return Endianness::LITTLE;
        else
            return Endianness::BIG;
    }

    inline bool isLittleEndian() {
        return determineEndianness() == Endianness::LITTLE;
    }

    inline int32_t switchEndianness(const int32_t i) {
        int32_t byte0, byte1, byte2, byte3;

        byte0 = (i & 0x000000FF) >> 0;
        byte1 = (i & 0x0000FF00) >> 8;
        byte2 = (i & 0x00FF0000) >> 16;
        byte3 = (i & 0xFF000000) >> 24;

        return((byte0 << 24) | (byte1 << 16) | (byte2 << 8) | (byte3 << 0));
    }

    template<typename T> inline void make_set(std::vector<T>& vec) {
        std::set<T> s( vec.begin(), vec.end() );
        vec.assign( s.begin(), s.end() );
    }

    // mimicking python's floor function
    inline int64_t floori(const int64_t num, const int64_t den) {
        if (0 < (num^den))
            return num/den;
        else
        {
            ldiv_t res = ldiv(num,den);
            return (res.rem) ? res.quot - 1
                             : res.quot;
        }
    }

    template<typename T> inline T floorToMultiple(const T& x, const T& base) {
        T k = x / base;
        if(k * base > x)
            return (k - 1) * base;
        else
            return k * base;
    }

    template<typename T> inline T ceilToMultiple(const T& x, const T& base) {
        T k = x / base;
        if(k * base >= x)
            return k * base;
        else
            return (k + 1) * base;
    }

    // Note: GCC demands template<...> tokens to come first
    template<typename T> inline void swap(T& a, T& b) {
        T h = a;
        a = b;
        b = h;
    }

    /*!
     * dumps memory after ptr for numBytes bytes out to stream as hex
     * @param out output stream
     * @param ptr start of memory region to print out
     * @param numBytes specifies how many bytes should be printed
     * @param format will auto break lines after 16 bytes and insert space between bytes
     */
    extern void hexdump(std::ostream& out, const void *ptr, const size_t numBytes, bool format = true);

    extern void asciidump(std::ostream& out, const void *ptr, const size_t numBytes, bool format = true);

    /*!
     * splits strings using a delimiter
     * @param s
     * @return
     */
    extern std::vector<std::string> splitLines(const std::string& s, const std::string& delimiter);

    /*!
     * appends line numbers to string.
     * @param s
     * @return string with line numbers.
     */
    extern std::string withLineNumbers(const std::string& s);


    /*!
     * remove duplicates in a std::vector
     * from https://www.techiedelight.com/remove-duplicates-vector-cpp/
     * @tparam T type
     * @param v vector of type T
     */
    template<typename T> void removeDuplicates(std::vector<T>& v) {
        std::unordered_set<T> s;
        auto end = std::remove_if(v.begin(), v.end(),
                                  [&s](T const &t) {
                                      return !s.insert(t).second;
                                  });

        v.erase(end, v.end());
    }
}

namespace tuplex {
    inline std::string char2str(const char c) {
        return std::string(1, c);
    }
}

/*!
 * desugars various python strings, won't work for formatted strings.
 * @param raw_string
 * @return the actual string value
 */
extern std::string str_value_from_python_raw_value(const std::string& raw_string);

/*!
 * takes a C++ string and converts into an escaped python string, i.e. contents are embedded within single
 * tick quote ' ... '. If single tick quotes are contained within the string, they are escaped using \
 * @param s string, value
 * @return python escaped string, i.e. one that can be assigned to raw_value in NString AST class.
 */
inline std::string escape_to_python_str(const std::string& s) {
    if(s.find('\'') == std::string::npos)
        return "'" + s + "'";
    std::string res;
    for(auto c : s) {
        if(c == '\'')
            res += tuplex::char2str('\\');
        res += tuplex::char2str(c);
    }
    return res;
}

#endif //TUPLEX_BASE_H