//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TTUPLE_H
#define TUPLEX_TTUPLE_H

#include <initializer_list>
#include <cassert>

template<typename T> class TTuple {
private:
    T *_elements;
    int _numElements;
public:
    TTuple() :_elements(nullptr), _numElements(0)    {}

    TTuple(std::initializer_list<T> L) {
        _elements = new T[L.size()];
        int i = 0;
        for(auto l : L) {
            _elements[i++] = l;
        }
        _numElements = L.size();
    }

    TTuple(const std::vector<T>& L) {
        _elements = new T[L.size()];
        int i = 0;
        for(auto l : L) {
            _elements[i++] = l;
        }
        _numElements = L.size();
    }

    TTuple(const TTuple& other) {
        _numElements = other._numElements;
        _elements = nullptr;
        if(other._elements) {
            // copy
            _elements = new T[_numElements];
            for(int i = 0; i < _numElements; ++i)
                _elements[i] = other._elements[i];
        }
    }

     TTuple& operator = (const TTuple& other) {
         if(_elements)
             delete [] _elements;

         _numElements = other._numElements;
         _elements = nullptr;
         if(other._elements) {
             // copy
             _elements = new T[_numElements];
             for(int i = 0; i < _numElements; ++i)
                 _elements[i] = other._elements[i];
         }

         return *this;
     }

    ~TTuple() {
        if(_elements)
            delete [] _elements;
        _numElements = 0;
        _elements = nullptr;
    }

    T& operator[](const size_t i) const {
        assert(0 <= i && i < _numElements);
        return _elements[i];
    }

    int length() const {
        return _numElements;
    }

};

template<typename T> bool operator == (const TTuple<T>& lhs, const TTuple<T>& rhs) {
    if(lhs.length() != rhs.length())
        return false;

    // compare element wise
    for(int i = 0; i < lhs.length(); ++i)
        if(lhs[i] != rhs[i])
            return false;
    return true;
}

#endif //TUPLEX_TTUPLE_H