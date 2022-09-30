//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FIELD_H
#define TUPLEX_FIELD_H

#include <tuple>
#include <TypeSystem.h>
#include <Tuple.h>
#include <List.h>
#ifdef BUILD_WITH_AWS
#include <aws/core/external/cjson/cJSON.h>
#else
#include <cJSON.h>
#endif
#include <optional.h>
#include <type_traits>
#include <Utils.h>

#ifdef BUILD_WITH_CEREAL
#include "cereal/access.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/common.hpp"
#include "cereal/archives/binary.hpp"
#endif

namespace tuplex {

    class Field;
    class Tuple;
    class List;

    /*!
     * wrapper class to hold a single element with its value
     */
    class Field {
    private:

        union {
            int64_t  _iValue;
            double   _dValue;
            uint8_t* _ptrValue;
        };

        python::Type _type;
        int64_t _size;
        bool _isNull; // for the option type

        void releaseMemory();

        inline bool hasPtrData() const {

            // option type may have data
            auto type = _type;
            if(type.isOptionType())
                type = type.getReturnType();

            return python::Type::STRING == type ||
            type.isTupleType() || type.isDictionaryType() ||
            python::Type::GENERICDICT == type || type.isListType() || type == python::Type::PYOBJECT;
        }

        std::string extractDesc(const python::Type& type) const; /// helper function to extract data

        // helper function to initialize field as tuple field from vector of elements
        void tuple_from_vector(const std::vector<Field>& elements);

        void deep_copy_from_other(const Field& other);
    public:

        Field(): _ptrValue(nullptr), _type(python::Type::UNKNOWN), _size(0), _isNull(false) {}

        // copy and move constructor
        Field(const Field& other) : _type(other._type), _size(other._size), _isNull(other._isNull) {
            // deep copy...
            _ptrValue = nullptr;
            deep_copy_from_other(other);
        }

        Field(Field&& other) : _iValue(other._iValue), _type(other._type), _size(other._size), _isNull(other._isNull) {
            other._ptrValue = nullptr; // !!! important !!!
            other._type = python::Type::UNKNOWN;
            other._size = 0;
        }

        ~Field();
        Field& operator = (const Field& other);

        explicit Field(const bool b);
        explicit Field(const int64_t i);
        explicit Field(const double d);
        explicit Field(const std::string& s);
        explicit Field(const char* str) : Field(std::string(str))   {}

        explicit Field(const Tuple& t);
        explicit Field(const List& l);

        // using std::tuple to pass in values instead of the helper Tuple class...
        template <class... Types> Field(const std::tuple<Types...>& tuple) {
            std::vector<Field> v;
            for_each_in_tuple(tuple, [&](const auto& el) { v.push_back(Field(el)); });

            tuple_from_vector(v);
        }

        /*!
         * construct a field by explicitly assigning the data stored as string and setting a respective type. Useful for dictionaries.
         * @param data data as string, all contents incl. '\0' will be copied
         * @param type set explicit type
         * @return Field
         */
        static Field from_str_data(const std::string& data, const python::Type& type);

        static Field from_str_data(const option<std::string>& data, const python::Type& type);


        static Field from_pickled_memory(const uint8_t* buf, size_t buf_size);

        /*!
         * returns field for NULLVALUE type
         * @return
         */
        static Field null() {
            Field f;
            f._isNull = true;
            f._type = python::Type::NULLVALUE;
            f._iValue = 0;
            return f;
        }

        static Field empty_dict() {
           return Field::from_str_data("{}", python::Type::EMPTYDICT);
        }

        static Field empty_tuple();

        static Field empty_list();

        static Field null(const python::Type &type) {
            assert(type.isOptionType());
            Field f;
            f._isNull = true;
            f._type = type;
            f._iValue = 0;
            return f;
        }

        // hack via "bad" constructor chaining
        // i.e. init with the dummy data for the option::none case
        // ==> helpful also for the tuple case!
        template<typename T> explicit Field(const option<T>& opt) : Field(opt.data()) {
            _isNull = !opt.has_value();

            if(!opt.has_value())
               _size = 0;

           // adjust type
           _type = python::Type::makeOptionType(_type); // convert to option type!
        }

        // integer ambiguity resolve
        explicit Field(const option<int>& opt) : Field((int64_t)opt.data()) {
            _isNull = !opt.has_value();
            if(!opt.has_value())
                _size = 0;
            _type = python::Type::makeOptionType(python::Type::I64);
        }

        /*!
         * convert field to wider representation (no error checking!)
         * @param f
         * @param targetType
         * @return
         */
        static Field upcastTo_unsafe(const Field& f, const python::Type& targetType);

        /*!
         * prints formatted field values
         * @return
         */
        std::string desc() const;

        std::string toPythonString() const {
            return desc();
        }

        bool isNull() const { return _isNull; }

        python::Type getType() const { return _type; }

        /*!
         * enforces internal representation to be of option type,
         * sets null indicator
         */
        inline Field& makeOptional() {
            if(_type == python::Type::PYOBJECT)
                return *this; // do not change type

            if(_type.isOptionType())
                return *this;
            _type = python::Type::makeOptionType(_type);
            _isNull = false;

            return *this;
        }

        void* getPtr() const { return _ptrValue; }
        size_t getPtrSize() const { return _size; }
        int64_t getInt() const { return _iValue; }
        double getDouble() const { return _dValue; }

        Field withoutOption() const {
           if(isNull())
               return Field::null();
           else {
               Field f(*this);
               f._isNull = false;
               // only get rid off top-level option.
               f._type = f._type.isOptionType() ? f._type.getReturnType() : f._type;
               return f;
           }
        }

        friend bool operator == (const Field& lhs, const Field& rhs);

#ifdef BUILD_WITH_CEREAL
        template<class Archive> void serialize(Archive &ar) {
            ar(_iValue, _type, _size, _isNull);
        }
#endif
    };

    extern bool operator == (const Field& lhs, const Field& rhs);
    inline bool operator != (const Field& lhs, const Field& rhs) {
        return !(lhs == rhs);
    }


    // basic case
    inline void vec_build(std::vector<Field>& v) {} // do nothing

    // specialize to resolve integer ambiguity
    template<typename... Targs> inline void vec_build(std::vector<Field>& v, int value, Targs... Fargs) {
        v.push_back(Field((int64_t)value));
        vec_build(v, Fargs...);
    }

    template<typename... Targs> inline void vec_build(std::vector<Field>& v, long long int value, Targs... Fargs) {
        v.push_back(Field((int64_t)value));
        vec_build(v, Fargs...);
    }

    template<typename... Targs> inline void vec_build(std::vector<Field>& v, const option<int> value, Targs... Fargs) {
        if(!value.has_value())
            v.push_back(Field(option<int64_t>::none));
        else
            v.push_back(Field(option<int64_t>((int64_t)value.value())));
        vec_build(v, Fargs...);
    }

    /*!
     * helper function to collect variadic Fields into a vector
     * @tparam T
     * @tparam Targs
     * @param v
     * @param value
     * @param Fargs
     */
    template<typename T, typename... Targs> inline void vec_build(std::vector<Field>& v, T value, Targs... Fargs) {
        // recursive call
        v.push_back(Field(value));
        vec_build(v, Fargs...);
    }

    /*!
     * decode constant valued type to a field
     * @param type
     * @return field with value obtained from type
     */
    extern Field constantTypeToField(const python::Type& type);

}
#endif //TUPLEX_FIELD_H