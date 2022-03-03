//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_OPTIONAL_H
#define TUPLEX_OPTIONAL_H

#include <stdexcept>

#include "cereal/access.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/map.hpp"
#include "cereal/types/unordered_map.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/common.hpp"

#include "cereal/archives/binary.hpp"

namespace tuplex {
    //! class mimicking optional of C++17.
    //! \tparam T
    template<typename T> class option {
    private:
        T _data;
        bool _isNone;
    public:
        option():_isNone(true)  {}
        option(const T& value) : _data(value), _isNone(false)   {}
        option(T&& value) : _data(std::move(value)), _isNone(false) {}
        option(const option& other) : _data(other._data), _isNone(other._isNone)    {}
        option& operator = (const option& other) { _data = other._data; _isNone = other._isNone; return *this; }
        option& operator = (const T& value) {_data = value; _isNone = false; return *this;}

        bool has_value() const { return !_isNone; }
        T value() const {
            if(_isNone)
                throw std::runtime_error("accessing empty option");
            return _data;
        }

        // data access without checks. Note: required for option type deduction, can be uninitialized.
        T data() const {
            return _data;
        }

        T value_or(const T& alternative) const { return _isNone ? alternative : _data; }

        static const option none;

        // compare to stored data element
        bool operator == (const T& other) {
            if(_isNone)
                return false;
            return _data == other;
        }

        bool operator != (const T& other) {
            if(_isNone)
                return true;
            return _data != other;
        }

        bool operator == (const option<T>& other) {
            if(_isNone && !other._isNone)
                return false;
            if(!_isNone && other._isNone)
                return false;
            if(_isNone && other._isNone)
                return true;
            return _data == other._data;
        }

        bool operator != (const option<T>& other) {
            if(_isNone && !other._isNone)
                return true;
            if(!_isNone && other._isNone)
                return true;
            if(_isNone && other._isNone)
                return false;
            return _data != other._data;
        }

        template<typename S> friend bool operator == (const S& lhs, const option<S>& rhs);
        template<typename S> friend bool operator != (const S& lhs, const option<S>& rhs);

        // cereal serialization functions
        template<class Archive> void serialize(Archive &ar) {
            ar(_data, _isNone);
        }
    };

    template<typename T> const option<T> option<T>::none=option();

    template<typename T> inline bool operator == (const T& lhs, const option<T>& rhs) {
        return rhs == lhs;
    }

    template<typename T> inline bool operator != (const T& lhs, const option<T>& rhs) {
        return rhs == lhs;
    }
}
#endif //TUPLEX_OPTIONAL_H