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

#include <boost/any.hpp>

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
            return python::Type::STRING == _type ||
            _type.isTupleType() || _type.isDictionaryType() ||
            python::Type::GENERICDICT == _type || _type.isListType() || _type == python::Type::PYOBJECT;
        }

        std::string extractDesc(const python::Type& type) const; /// helper function to extract data

        // helper function to initialize field as tuple field from vector of elements
        void tuple_from_vector(const std::vector<Field>& elements);

        // parses JSON and converts it into python syntax (together with type)
        std::string internalJSONToPythonString() const;
    public:

        Field(): _ptrValue(nullptr), _type(python::Type::UNKNOWN), _size(0), _isNull(false) {}
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

        ~Field();

        Field(const Field& other);

        Field& operator = (const Field& other);

        /*!
         * prints formatted field values
         * @return
         */
        std::string desc() const;

        std::string toPythonString() const {
            // special case: struct type and dict -> they're stored as JSON string/dictionaries
            if(getType().isDictionaryType())
                return internalJSONToPythonString();
            return desc();
        }

        bool isNull() const { return _isNull; }

        python::Type getType() const { return _type; }

        /*!
         * enforces internal representation to be of option type,
         * sets null indicator
         */
        inline void makeOptional() {
            if(_type == python::Type::PYOBJECT)
                return; // do not change type

            if(_type.isOptionType())
                return;
            _type = python::Type::makeOptionType(_type);
            _isNull = false;
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
               f._type = f._type.getReturnType();
               return f;
           }
        }

        friend bool operator == (const Field& lhs, const Field& rhs);
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


    inline Field any_to_field(const boost::any& value, const Field& default_value = Field::null()) {
        auto f = default_value;

        // cast from a couple of basic C++ types
        if(value.type() == typeid(std::string)) {
            f = tuplex::Field(boost::any_cast<std::string>(value));
        } else if(value.type() == typeid(const char*)) {
            f = tuplex::Field(std::string(boost::any_cast<const char*>(value)));
        } else if(value.type() == typeid(float)) {
#ifndef NDEBUG
            std::cerr<<"WARNING: number supplied as float (f32) but Tuplex internally only uses double"
                     <<"precision floating point numbers. Consider calling with the correct double type (f64)."
                     <<std::endl;
#endif
            f = tuplex::Field(static_cast<double>(boost::any_cast<float>(value)));
        }else if(value.type() == typeid(double)) {
            f = tuplex::Field(boost::any_cast<double>(value));
        } else if(value.type() == typeid(int64_t)) {
            f = tuplex::Field(boost::any_cast<int64_t>(value));
        } else if(value.type() == typeid(int8_t)) {
            f = tuplex::Field(static_cast<int64_t>(boost::any_cast<int8_t>(value)));
        } else if(value.type() == typeid(int16_t)) {
            f = tuplex::Field(static_cast<int64_t>(boost::any_cast<int16_t>(value)));
        } else if(value.type() == typeid(int32_t)) {
            f = tuplex::Field(static_cast<int64_t>(boost::any_cast<int32_t>(value)));
        } else {
            try {
                f = boost::any_cast<tuplex::Field>(value);
            } catch (const boost::bad_any_cast& b) {
#ifndef NDEBUG
                std::cerr<<"bad cast, expecting Field here but got instead "<<value.type().name()<<std::endl;
                assert(false);
#else
                std::cerr<<"returning default value"<<std::endl;
#endif
            }
        }
        return f;
    }

}
#endif //TUPLEX_FIELD_H