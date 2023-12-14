//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Tuple.h>
#include <sstream>
#include <string>
#include <Serializer.h>

namespace tuplex {

    void Tuple::init_from_vector(const std::vector<tuplex::Field>& elements) {
        if(elements.empty()) {
            _numElements = 0;
            _elements = nullptr;
        } else {
            _numElements = elements.size();
            _elements = new Field[_numElements];
            for(int i = 0; i < _numElements; ++i)
                _elements[i] = elements[i];
        }
    }

    Tuple::Tuple(const Tuple &other) {
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

    Tuple& Tuple::operator=(const Tuple &other) {
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

    Tuple::~Tuple() {
        if(_elements) {
            assert(_numElements > 0);
            delete [] _elements;
        }
        _elements = nullptr;
        _numElements = 0;
    }


    std::string Tuple::desc() const {
        std::stringstream ss;

        ss<<"(";

        if(_numElements > 0)
            ss<<_elements[0].desc();

        // special case: 1 element. Add , to make clear it is a tuple (python syntax)
        if(_numElements == 1)
            ss<<",";

        for(int i = 1; i < _numElements; ++i) {
            ss<<","<<_elements[i].desc();
        }
        ss<<")";
        return ss.str();
    }


    python::Type Tuple::getType() const {
        std::vector<python::Type> types;
        for(int i = 0; i < _numElements; ++i)
            types.push_back(_elements[i].getType());
        return python::Type::makeTupleType(types);
    }

    Field Tuple::getField(const int i) const {
        assert(_elements);
        assert(i >= 0 && i < numElements());
        return _elements[i];
    }

    bool operator == (const Tuple& rhs, const Tuple& lhs) {
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

    Tuple* Tuple::allocate_deep_copy() const {
        Tuple *t = new Tuple();
        assert(t->_elements == nullptr);
        t->_numElements = _numElements;
        t->_elements = new Field[t->_numElements];
        for(unsigned i = 0; i < _numElements; ++i) {
            t->_elements[i] = _elements[i];
        }
        return t;
    }

    size_t Tuple::serialized_length() const {
        if(_numElements == 0)
            return 0;

        // use Serializer to check length
        Serializer s;
        for(unsigned i = 0; i < _numElements; ++i)
            s.appendField(_elements[i]);
        return s.length();
    }

    size_t Tuple::serialize_to(uint8_t* ptr) const {
        if(_numElements == 0)
            return 0;

        // use Serializer to check length
        Serializer s;
        for(unsigned i = 0; i < _numElements; ++i)
            s.appendField(_elements[i]);
        auto length = s.length();
        return s.serialize(ptr, length);
    }
}

