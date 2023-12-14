//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LIST_H
#define TUPLEX_LIST_H

#include <Field.h>
#include <vector>

namespace tuplex {

    class Field;
    class List;

    /*!
     * helper class to handle lists of fields
     */
    class List {
    private:

        Field* _elements;
        size_t _numElements;
        python::Type _listType;

        void init_from_vector(const std::vector<tuplex::Field>& elements);

    public:
        List() : _elements(nullptr), _numElements(0), _listType(python::Type::EMPTYLIST)   {}
        List(List&& other) : _numElements(other._numElements), _elements(other._elements), _listType(other._listType) {
            other._numElements = 0;
            other._elements = nullptr;
        }

        List(const python::Type& elementType) : _elements(nullptr), _numElements(0), _listType(python::Type::makeListType(elementType)) {}

        ~List();

        // new variadic template param ctor
         template<typename... Targs> explicit List(Targs... Fargs) {
            std::vector<Field> elements;
            vec_build(elements, Fargs...);
            init_from_vector(elements);
        }

        List(const List& other);

        List& operator = (const List& other);

        std::string desc() const;

        python::Type getType() const;

        size_t numElements() const { return _numElements; }
        Field getField(const int i) const;

        size_t numNonNullElements() const;

        friend bool operator == (const List& rhs, const List& lhs);

        static List from_vector(const std::vector<tuplex::Field>& elements) {
            List l;
            l.init_from_vector(elements);
            return l;
        }

        List* allocate_deep_copy() const;

        size_t serialized_length() const;
        size_t serialize_to(uint8_t* ptr) const;
    };


    extern bool operator == (const List& rhs, const List& lhs);
}

#endif //TUPLEX_LIST_H