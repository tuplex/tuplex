//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TSET_H
#define TUPLEX_TSET_H

#include <vector>
#include <cassert>
#include <Base.h>
#include <TTuple.h>
#include <Logger.h>

/*!
 * a simple implementation of a set. Elements must implement the == operator
 * @tparam T
 */
template<typename T> class TSet {
private:
    T *_elements;
    int _numElements;
    int _capacity;

    static const int growthAddend = 5; // grow by 5 elements

    void resize() {
        // check whether up- or downsizing is necessary
        if(_capacity - _numElements <= 0) {

            // upsize
            T *oldptr = _elements;
            _capacity += growthAddend;
            _elements = new T[_capacity];

            // copy old contents & delete
            if(oldptr) {
                for(int i = 0; i < _numElements; ++i)
                    _elements[i] = oldptr[i];
                delete [] oldptr;
            }

            //memset(&_elements[_limit], 0, sizeof(T) * (_capacity - _limit));

        } else if(_capacity - _numElements > growthAddend) {
            // downsize
            T *oldptr = _elements;
            assert(oldptr);

            _capacity -= growthAddend;
            _elements = new T[_capacity];

            // copy old contents & delete
            if(oldptr) {
                for(int i = 0; i < _numElements; ++i)
                    _elements[i] = oldptr[i];
                delete [] oldptr;
            }
        }
    }
public:

    TSet() : _elements(nullptr), _numElements(0), _capacity(0)  {}
    ~TSet() {
        if(_elements) {
            delete [] _elements;
        }
        _elements = nullptr;
        _numElements = 0;
        _capacity = 0;
    }

    TSet(std::initializer_list<T> L) {
        _elements = nullptr;
        _numElements = 0;
        _capacity = 0;
        for(T el : L) {
            add(el);
        }
    }

    TSet(const std::vector<T>& L) {
        _elements = nullptr;
        _numElements = 0;
        _capacity = 0;
        for(T el : L) {
            add(el);
        }
    }

    TSet(const TSet<T>& other) {
        _elements = nullptr;
        _numElements = other._numElements;
        _capacity = other._numElements;

        if(other._elements) {
            _elements = new T[_numElements];
            for(int i = 0; i < _numElements; i++)
                _elements[i] = other._elements[i];
        }
    }

    TSet& operator = (const TSet<T>& other) {
        _numElements = other._numElements;
        _capacity = other._numElements;

        if(_elements)
            delete [] _elements;
        _elements = new T[_numElements];

        for(int i = 0; i < _numElements; i++)
            _elements[i] = other._elements[i];
        return *this;
    }

    TSet& add(const T& val) {
        // check if contained
        if(!contains(val)) {
            resize();
            _elements[_numElements++] = val;
        }

        return *this;
    }

    bool contains(const T& val) const {
        if(!_elements)
            return false;

        for(int i = 0; i < _numElements; ++i) {
            if(_elements[i] == val)
                return true;
        }
        return false;
    }

    /*!
     * removes element from set. returns true if an element was actually removed.
     * @param val value to be removed
     * @return true if actually an element was removed
     */
    bool remove(const T& val) {
        if(!_elements)
            return false;

        //get position
        int i = 0;
        for(; i < _numElements; ++i) {
            if(_elements[i] == val)
                break;
        }

        // element found? (i != _limit)
        if(i == _numElements)
            return false;

        // swap last element and decrease, then resize
        assert(_numElements >= 1);
        core::swap(_elements[i], _elements[_numElements - 1]);
        _numElements--;
        resize();
        return true;
    }

    int size() const { return _numElements; }

    T& operator[](const size_t i) const {
        assert(0 <= i && i < _numElements);
        return _elements[i];
    }
};

template<typename T> TSet<T> intersect(const TSet<T>& A, const TSet<T>& B) {
    TSet<T> res;
    for(int i = 0; i < A.size(); i++) {
        if(B.contains(A[i]))
            res.add(A[i]);
    }
    return res;
}

template<typename T> bool operator==(const TSet<T>& lhs, const TSet<T>& rhs) {
    if(lhs.size() != rhs.size())
        return false;

    for(int i = 0; i < lhs.size(); i++) {
        if(!rhs.contains(lhs[i]))
            return false;
    }
    return true;
}

// helper function to construct a cartesian product out of sets
template<typename T> TSet<TTuple<T>> cartesian(const std::vector<TSet<T>>& sets) {
    // first compute output size
    int size = 1;
    for(int i = 0; i < sets.size(); ++i) {
        size *= sets[i].size();
    }

    // issue a warning when the size is larger than 2*10
    if(size > (0x1 << 10))
        Logger::instance().logger("memory").warn("computing a large cartesian product with " + std::to_string(size) + " elements");

    TSet< TTuple<T> > res = TSet<TTuple<T>>({});

    assert(res.size() == 0);


    // this function is flawed...
    // the ordering is missing... -.-
    for(int i = 0; i < size; i++) {
        std::vector<T> combi;
        for(int j = 0; j < sets.size(); j++) {
            int curIndex = i % sets[j].size();
            combi.push_back(sets[j][curIndex]);
        }

        assert(sets.size() == combi.size());

        res.add(TTuple<T>(combi));
    }

    assert(res.size() == size);

    for(int i = 0; i < size; i++) {
        assert(res[i].length() == sets.size());
    }

    return res;
}


template<typename T> std::vector<std::vector<T> > cart_product (const std::vector<std::vector<T>>& v) {
    std::vector<std::vector<T>> s = {{}};
    for (auto& u : v) {
        std::vector<std::vector<T>> r;
        for (auto& x : s) {
            for (auto y : u) {
                r.push_back(x);
                r.back().push_back(y);
            }
        }
        s.swap(r);
    }
    return s;
}

template<typename T> std::vector<T> toVector(const TSet<T>& S) {
    std::vector<T> v;
    for(int i = 0; i < S.size(); ++i) {
        v.push_back(S[i]);
    }
    return v;
}
#endif //TUPLEX_TSET_H