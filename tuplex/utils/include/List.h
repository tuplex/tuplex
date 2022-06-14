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

        void init_from_vector(const std::vector<tuplex::Field>& elements);

    public:
        List() : _elements(nullptr), _numElements(0)    {}
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
    };


    extern bool operator == (const List& rhs, const List& lhs);
}

#endif //TUPLEX_LIST_H