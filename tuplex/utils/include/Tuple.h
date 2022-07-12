//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TUPLE_H
#define TUPLEX_TUPLE_H

#include <Field.h>
#include <vector>

namespace tuplex {

    class Field;
    class Tuple;

    /*!
     * helper class to handle fixed size collection of fields ala tuple of fields
     */
    class Tuple {
    private:
        Field* _elements;
        size_t _numElements;

        void init_from_vector(const std::vector<tuplex::Field>& elements);
    public:
        Tuple() : _elements(nullptr), _numElements(0)    {}
        ~Tuple();

//        Tuple(const Tuple& other) : _numElements(other._numElements), _elements(nullptr) {
//            _elements = new Field[_numElements];
//            for(unsigned i = 0; i < _numElements; ++i)
//                _elements[i] = other._elements[i];
//        }

        Tuple(Tuple&& other) : _numElements(other._numElements), _elements(other._elements) {
            other._numElements = 0;
            other._elements = nullptr;
        }

        // new variadic template param ctor
         template<typename... Targs> explicit Tuple(Targs... Fargs) {
            std::vector<Field> elements;
            vec_build(elements, Fargs...);
            init_from_vector(elements);
        }

        Tuple(const Tuple& other);

        Tuple& operator = (const Tuple& other);

        std::string desc() const;

        python::Type getType() const;

        size_t numElements() const { return _numElements; }
        Field getField(const int i) const;

        friend bool operator == (const Tuple& rhs, const Tuple& lhs);

        static Tuple from_vector(const std::vector<tuplex::Field>& elements) {
            Tuple t;
            t.init_from_vector(elements);
            return t;
        }

        Tuple* allocate_deep_copy() const;
    };

    extern bool operator == (const Tuple& rhs, const Tuple& lhs);
}

#endif //TUPLEX_TUPLE_H