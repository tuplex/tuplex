//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_UTILS_H
#define TUPLEX_UTILS_H

#include "Base.h"
#include "StringUtils.h"
#include "StatUtils.h"
#include "optional.h"

#include <Logger.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>
#include <cerrno>
#include <cstring>
#include <unordered_map>

#include <utility>
#include <cstdint>

#if __cplusplus < 201402L
namespace std {
    // Herb Sutters make_unique, part of C++14 but not C++11
    template<typename T, typename ...Args>
    std::unique_ptr<T> make_unique(Args &&...args) {
        return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
    }
}
#endif


// helper code to allow tuples in maps.
#include <boost/functional/hash.hpp>
#include <tuple>
#include <cstdint>

static_assert(__cplusplus >= 201402L, "need at least C++ 14 to compile this file");
// check https://blog.galowicz.de/2016/02/20/short_file_macro/
// for another cool macro
constexpr const char* base_file_name(const char* path) {
    const char* file = path;
    while (*path) {
        if (*path++ == '/') {
            file = path;
        }
    }
    return file;
}

// macros to print out filename + line
#define FLINESTR (std::string(base_file_name(__FILE__)) + "+" + std::to_string(__LINE__))


enum class Endianess {
    ENDIAN_UNKNOWN,
    ENDIAN_BIG,
    ENDIAN_LITTLE,
    ENDIAN_BIG_WORD,   /* Middle-endian, Honeywell 316 style */
    ENDIAN_LITTLE_WORD /* Middle-endian, PDP-11 style */
};
 // from https://sourceforge.net/p/predef/wiki/Endianness/
inline Endianess endianness(void) {
    union {
        uint32_t value;
        uint8_t data[sizeof(uint32_t)];
    } number;

    number.data[0] = 0x00;
    number.data[1] = 0x01;
    number.data[2] = 0x02;
    number.data[3] = 0x03;

    switch (number.value) {
        case UINT32_C(0x00010203):
            return Endianess::ENDIAN_BIG;
        case UINT32_C(0x03020100):
            return Endianess::ENDIAN_LITTLE;
        case UINT32_C(0x02030001):
            return Endianess::ENDIAN_BIG_WORD;
        case UINT32_C(0x01000302):
            return Endianess::ENDIAN_LITTLE_WORD;
        default:
            return Endianess::ENDIAN_UNKNOWN;
    }
}

namespace std
{

//    template<typename... T>
//    struct hash<tuple<T...>>
//    {
//        size_t operator()(tuple<T...> const& arg) const noexcept
//        {
//            return boost::hash_value(arg);
//        }
//    };

    // Code from boost
    // Reciprocal of the golden ratio helps spread entropy
    //     and handles duplicates.
    // See Mike Seymour in magic-numbers-in-boosthash-combine:
    //     https://stackoverflow.com/questions/4948780

    template <class T>
    inline void hash_combine(std::size_t& seed, T const& v)
    {
        seed ^= hash<T>()(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
    }

    // Recursive template code derived from Matthieu M.
    template <class Tuple, size_t Index = std::tuple_size<Tuple>::value - 1>
    struct HashValueImpl
    {
        static void apply(size_t& seed, Tuple const& tuple)
        {
            HashValueImpl<Tuple, Index-1>::apply(seed, tuple);
            hash_combine(seed, get<Index>(tuple));
        }
    };

    template <class Tuple>
    struct HashValueImpl<Tuple,0>
    {
        static void apply(size_t& seed, Tuple const& tuple)
        {
            hash_combine(seed, get<0>(tuple));
        }
    };

template <typename ... TT>
struct hash<std::tuple<TT...>>
{
    size_t
    operator()(std::tuple<TT...> const& tt) const
    {
        size_t seed = 0;
        HashValueImpl<std::tuple<TT...> >::apply(seed, tt);
        return seed;
    }

};


}

// llvm has competing debug macro -.-
// use this here to activate additional checks & easier debugging.
// #define TUPLEX_DEBUG

namespace tuplex {

    // from https://stackoverflow.com/questions/1068849/how-do-i-determine-the-number-of-digits-of-an-integer-in-c
    // and http://graphics.stanford.edu/~seander/bithacks.html#IntegerLog10Obvious
    inline int ilog10c(unsigned int v) {
        return (v >= 1000000000) ? 9 : (v >= 100000000) ? 8 : (v >= 10000000) ? 7 :
                                                              (v >= 1000000) ? 6 : (v >= 100000) ? 5 : (v >= 10000) ? 4
                                                                                                                    :
                                                                                                       (v >= 1000) ? 3
                                                                                                                   : (v >=
                                                                                                                      100)
                                                                                                                     ? 2
                                                                                                                     : (v >=
                                                                                                                        10)
                                                                                                                       ? 1
                                                                                                                       : 0;
    }

    // reuse boost uuid
    using uniqueid_t = boost::uuids::uuid;

    /*!
     * retrieves a UUID (usable for storing files, identifying objects)
     * @return uuid
     */
    extern uniqueid_t getUniqueID();

    inline std::string uuidToString(const uniqueid_t& uuid) {
        std::stringstream ss;
        ss<<uuid;
        return ss.str();
    }

    // C++14 tuple iteration
    // from https://stackoverflow.com/questions/26902633/how-to-iterate-over-a-stdtuple-in-c-11/26908596
    template<class F, class...Ts, std::size_t...Is>
    void for_each_in_tuple(const std::tuple<Ts...> & tuple, F func, std::index_sequence<Is...>){
        using expander = int[];
        (void)expander { 0, ((void)func(std::get<Is>(tuple)), 0)... };
    }

    template<class F, class...Ts>
    void for_each_in_tuple(const std::tuple<Ts...> & tuple, F func){
        for_each_in_tuple(tuple, func, std::make_index_sequence<sizeof...(Ts)>());
    }

    // from https://stackoverflow.com/questions/2333728/stdmap-default-value/26958878#26958878
    template<typename MAP> const typename MAP::mapped_type& get_or(const MAP& m,
                                                      const typename MAP::key_type& key,
                                                      const typename MAP::mapped_type& defval) {
        typename MAP::const_iterator it = m.find(key);
        if (it == m.end())
            return defval;

        return it->second;
    }

    template<typename K, typename V>
    std::vector<std::pair<K,V>> mapToVector(const std::unordered_map<K,V> &map) {
        return std::vector<std::pair<K,V>>(map.begin(), map.end());
    }

    template<typename K, typename V>
    std::vector<std::pair<K,V>> mapToVector(const std::map<K,V> &map) {
        return std::vector<std::pair<K,V>>(map.begin(), map.end());
    }

    /*!
     * takes a string which holds a memory size with suffixes and converts to bytes.
     * @param str
     * @return size_t which could be extracted from str. returns 0 and logs an error if memString could not be converted
     */
    extern size_t memStringToSize(const std::string& str);

    /*!
     * converts a size to a memory string using SI-suffixes. M
     * emory is expressed as floating point number of the largest available suffix
     * @param size
     * @return memory String
     */
    extern std::string sizeToMemString(const size_t size);


    /*!
     * converts string (accepting lower/uppercase versions) of True/False to boolean
     * @param s string, if it doesn't follow format, false is returned and the logger receives an error
     * @return boolean value according to string value
     */
    extern bool stringToBool(const std::string& s);

    /*!
     * check whether local file exsists or not.
     * @param Filename
     * @return
     */
    inline bool fileExists(const std::string &local_path) {
        // from https://stackoverflow.com/questions/12774207/fastest-way-to-check-if-a-file-exist-using-standard-c-c11-c
        return access( local_path.c_str(), 0 ) == 0;
    }

    /*!
     * throws a runtime_error with formatted strerror message
     */
    inline void handle_file_error(const std::string& msg = "") {
        using namespace std;
        stringstream ss;
        if(!msg.empty())
            ss<<msg<<endl;
        ss<<"Value of errno: "<<errno<<endl;
        ss<<"Details: "<<strerror(errno)<<endl;
        throw std::runtime_error(ss.str());
    }


    inline bool isDigit(const char c) {
        return c >= '0' && c <= '9';
    }

    /*!
     * returns the ordinal name of the number. I.e. for 1 1st is returned,...
     * @param i
     * @return ordinal name in English
     */
    inline std::string ordinal(const int i) {
        if(i % 10 == 1) {
            return std::to_string(i) + "st";
        }
        if(i % 10 == 2) {
            return std::to_string(i) + "nd";
        }
        if(i % 10 == 3) {
            return std::to_string(i) + "rd";
        }
        return std::to_string(i) + "th";
    }

    template<typename T> std::ostream &operator <<(std::ostream &os, const std::vector<T> &v) {
        using namespace std;

        os<<"[";
        if(!v.empty()) {
            os<<v[0];
            for(int i = 1; i < v.size(); ++i) {
                os<<", "<<v[i];
            }
        }
        os<<"]";
        return os;
    }

    template<typename T> std::ostream &operator <<(std::ostream &os, const std::vector<std::vector<T>> &v) {
        using namespace std;

        os<<"[";
        if(!v.empty()) {
            os<<v[0];
            for(int i = 1; i < v.size(); ++i) {
                os<<", "<<v[i];
            }
        }
        os<<"]";
        return os;
    }

    template<typename T> void reverseVector(std::vector<T>& v) {
        for(int i = 0; i < v.size() / 2; ++i) {
            core::swap(v[i], v[v.size() - i - 1]);
        }
    }

    namespace helper {
        void printSeparatingLine(std::ostream& os, const std::vector<int>& columnWidths);
        void printRow(std::ostream& os, const std::vector<int>& columnWidths,
                      const std::vector<std::string>& columnStrs);
    }

    /*!
     * helper function to check whether given string starts with prefix
     * @param s string to check for
     * @param prefix
     * @return true when string s has prefix
     */
    inline bool startsWith(const std::string& s, const std::string& prefix) {
        if(prefix.size() > s.size())
            return false;
        return std::equal(prefix.cbegin(), prefix.cend(), s.cbegin());
    }

    /*!
     * gets environment variable, returns empty string iff not found.
     * @param envName
     * @return value of env variable or empty string iff not found
     */
    inline std::string getEnv(const std::string& envName) {
        char *ptr = std::getenv(envName.c_str());
        if(ptr)
            return std::string(ptr);
        else
            return "";
    }

    // hashing functions
    // based on https://stackoverflow.com/questions/34597260/stdhash-value-on-char-value-and-not-on-memory-address
    // and http://www.isthe.com/chongo/tech/comp/fnv/index.html
    template <typename ResultT, ResultT OffsetBasis, ResultT Prime> class basic_fnv1a final {
        static_assert(std::is_unsigned<ResultT>::value, "hash result must be unsigned integer");

    public:
        using result_type = ResultT;
    private:
        result_type _state;
    public:
        basic_fnv1a() noexcept : _state{OffsetBasis} {}

        void update(const void *const data, const std::size_t size) noexcept {
            const auto cdata = static_cast<const unsigned char *>(data);
            auto acc = this->_state;
            for (auto i = std::size_t {}; i < size; ++i)
            {
                const auto next = std::size_t {cdata[i]};
                acc = (acc ^ next) * Prime;
            }
            this->_state = acc;
        }

        result_type digest() const noexcept {
            return this->_state;
        }

    };

    using fnv1a_32 = basic_fnv1a<std::uint32_t,
            UINT32_C(2166136261),
            UINT32_C(16777619)>;

    using fnv1a_64 = basic_fnv1a<std::uint64_t,
            UINT64_C(14695981039346656037),
            UINT64_C(1099511628211)>;

    inline std::uint64_t hash64_fnv(const void *const data, const std::size_t size) noexcept {
        assert(data);
        fnv1a_64 hashfn;
        hashfn.update(data, size);
        return hashfn.digest();
    }


    inline std::string hexaddr(const void *ptr) {
        std::stringstream ss;
        ss<<ptr;
        return ss.str();
    }


    // helper functions for debug purposes
    inline void TRACE_LOCK(const std::string& msg="") {
#ifdef TUPLEX_DEBUG

        std::stringstream ss;
        ss<<std::this_thread::get_id();
        auto strThread = ss.str();

        if(msg.length() == 0 ) {
            Logger::instance().defaultLogger().info("LOCK (" + strThread + ")");
        } else {
            Logger::instance().defaultLogger().info("LOCK (" + strThread + ") : " + msg);
        }
#endif
    }

    inline void TRACE_UNLOCK(const std::string& msg="") {
#ifdef TUPLEX_DEBUG

        std::stringstream ss;
        ss<<std::this_thread::get_id();
        auto strThread = ss.str();

        if(msg.length() == 0 ) {
            Logger::instance().defaultLogger().info("UNLOCK (" + strThread + ")");
        } else {
            Logger::instance().defaultLogger().info("UNLOCK (" + strThread + ") : " + msg);
        }
#endif
    }


    // from https://stackoverflow.com/questions/874134/find-out-if-string-ends-with-another-string-in-c
    // C++20 will provide this finally...
    inline bool strEndsWith(const std::string& s, const std::string& suffix) {
        return s.size() >= suffix.size() && s.rfind(suffix) == (s.size()-suffix.size());
    }

    inline bool strStartsWith(const std::string& s, const std::string& prefix) {
        return s.substr(0, prefix.length()) == prefix;
    }

    /*!
     * returns the most frequent item in the set.
     * @tparam T
     * @param v needs to contain at least one element
     * @return
     */
    template<typename T, class Hash = std::hash<T>> T mostFrequentItem(const std::vector<T>& v) {
        assert(v.size() > 0);

        std::unordered_map<T, size_t> votes;
        size_t maxcount = 0;
        T commonest;
        for(const auto& el : v) {
            if(votes.find(el) == votes.end())
                votes[el] = 0;
            votes[el]++;

            if(votes[el] > maxcount) {
                commonest = el;
                maxcount = votes[el];
            }
        }

        return commonest;
    }

    /*!
     * find element in vector and return its index
     * @tparam T
     * @param t
     * @param v
     * @return -1 if not found, else index in vector
     */
    template<typename T> int indexInVector(const T& t, const std::vector<T>& v) {
        int i = 0;
        for(const auto& e : v) {
            if(e == t)
                return i;
            i++;
        }
        return -1;
    }
}

#endif //TUPLEX_UTILS_H
