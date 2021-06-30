//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <List.h>
#include <sstream>
#include <string>

namespace tuplex {

    void List::init_from_vector(const std::vector<tuplex::Field>& elements) {
        if(elements.empty()) {
            _numElements = 0;
            _elements = nullptr;
        } else {
            _numElements = elements.size();
            _elements = new Field[_numElements];
            for(int i = 0; i < _numElements; ++i) {
                if(elements[i].getType() != elements[0].getType()) throw std::runtime_error("List::init_from_vector called with elements of nonuniform type.");
                _elements[i] = elements[i];
            }
        }
    }

    List::List(const List &other) {
        // deep copy needed
        _numElements = other._numElements;

        if(_numElements > 0) {
            _elements = new Field[_numElements];

            for(int i = 0; i < _numElements; ++i)
                _elements[i] = other._elements[i];
        }
        else
            _elements = nullptr;
    }

    List& List::operator=(const List &other) {
        // release mem
        if(_elements)
            delete [] _elements;
        _elements = nullptr;

        // deep copy needed
        _numElements = other._numElements;

        if(_numElements > 0) {
            _elements = new Field[_numElements];
            for(int i = 0; i < _numElements; ++i)
                _elements[i] = other._elements[i];
        }

        return *this;
    }

    List::~List() {
        if(_elements) {
            assert(_numElements > 0);
            delete [] _elements;
            _elements = nullptr;
            _numElements = 0;
        }
    }


    std::string List::desc() const {
        std::stringstream ss;

        ss<<"[";

        if(_numElements > 0)
            ss<<_elements[0].desc();

        for(int i = 1; i < _numElements; ++i) {
            ss<<","<<_elements[i].desc();
        }
        ss<<"]";
        return ss.str();
    }


    python::Type List::getType() const {
        if(_numElements > 0)
            return python::Type::makeListType(_elements[0].getType());
        else
            return python::Type::EMPTYLIST;
    }

    Field List::getField(const int i) const {
        assert(_elements);
        assert(i >= 0 && i < numElements());
        return _elements[i];
    }

    bool operator == (const List& rhs, const List& lhs) {
        if(rhs._numElements != lhs._numElements)
            return false;

        // elementwise comparison
        for(unsigned i = 0; i < rhs._numElements; ++i) {
            Field a = rhs.getField(i);
            Field b = rhs.getField(i);
            if(a != b)
                return false;
        }
        return true;
    }
}