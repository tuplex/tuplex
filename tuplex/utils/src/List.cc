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
#include <Serializer.h>

namespace tuplex {

    void List::init_from_vector(const std::vector<tuplex::Field>& elements) {
        if(elements.empty()) {
            _numElements = 0;
            _elements = nullptr;
            _listType = python::Type::EMPTYLIST;
        } else {
            _numElements = elements.size();
            _elements = new Field[_numElements];

            // two-way approach: First, check if homogenous
            assert(!elements.empty());
            auto el_type = elements[0].getType();
            auto uni_type = el_type;
            bool is_homogeneous = true;
            for(unsigned i = 1; i < elements.size(); ++i) {
                uni_type = unifyTypes(uni_type, elements[i].getType());
                if(elements[i].getType() != el_type)
                    is_homogeneous = false;
            }

            if(is_homogeneous) {
                for(int i = 0; i < _numElements; ++i) {
                    if(elements[i].getType() != elements[0].getType())
                        throw std::runtime_error("List::init_from_vector called with elements"
                                                 " of nonuniform type, tried to set list element with field of type "
                                                 + elements[i].getType().desc() + " but list has assumed type of "
                                                 + elements[0].getType().desc());
                    _elements[i] = elements[i];
                }
                _listType = python::Type::makeListType(uni_type);
            } else if(python::Type::UNKNOWN != uni_type) {
                _listType = python::Type::makeListType(uni_type);
                // cast each element up
                for(unsigned i = 0; i < _numElements; ++i)
                    _elements[i] = Field::upcastTo_unsafe(elements[i], uni_type);
            } else {
                // heterogeneous list...
                _listType = python::Type::makeListType(python::Type::PYOBJECT);
                for(unsigned i = 0; i < _numElements; ++i)
                    _elements[i] = elements[i];
            }
        }
        assert(_numElements != 0 && _listType != python::Type::EMPTYLIST);
        assert(!_listType.isIllDefined());
    }

    List::List(const List &other) {
        // deep copy needed
        _numElements = other._numElements;
        _listType = other._listType;

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
        _listType = other._listType;

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
            return _listType;
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

    size_t List::numNonNullElements() const {
        size_t num = 0;
        for(size_t i = 0; i < _numElements; i++) {
            num += !getField(i).isNull();
        }
        return num;
    }

    List* List::allocate_deep_copy() const {
        List *L = new List();
        assert(L->_elements == nullptr);
        L->_numElements = _numElements;
        L->_elements = new Field[L->_numElements];
        L->_listType = _listType;
        for(unsigned i = 0; i < _numElements; ++i) {
            L->_elements[i] = _elements[i];
        }
        return L;
    }

    size_t List::serialized_length() const {
       return serialized_list_size(*this);
    }

    size_t List::serialize_to(uint8_t *ptr) const {
        auto len = serialized_list_size(*this);
        return serialize_list_to_ptr(*this, ptr, len);
    }
}