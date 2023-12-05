//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Field.h>
#include <sstream>
#include <iomanip>

// gcc fixes, needed for memcpy. Clang does not need those includes
#ifdef __GNUC__
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <string>
#include <iostream>
#include <Logger.h>

#endif

namespace tuplex {
    Field::Field(const double d) {
        _size = sizeof(double);
        _type = python::Type::F64;
        _dValue = d;
        _isNull = false;
    }

    Field::Field(const int64_t i) {
        _size = sizeof(int64_t);
        _type = python::Type::I64;
        _iValue = i;
        _isNull = false;
    }

    Field::Field(const bool b) {
        _size = sizeof(int64_t);
        _type = python::Type::BOOLEAN;
        _iValue = b;
        _isNull = false;
    }

    Field::Field(const std::string &s) {
        _size = s.length() + 1;
        _type = python::Type::STRING;
        _ptrValue = reinterpret_cast<uint8_t*>(new char[_size]);
        _isNull = false;

        // safe memory checks
        if(!_ptrValue)
            _size = 0;
        else
            std::memcpy(_ptrValue, s.c_str(), _size);
    }

    Field Field::from_str_data(const std::string &data, const python::Type &type) {
        Field f;
        f._size = data.length() + 1;
        f._type = type;
        f._ptrValue = reinterpret_cast<uint8_t*>(new char[f._size]);
        f._isNull = false;

        // safe memory checks
        if(!f._ptrValue)
            f._size = 0;
        else
            std::memcpy(f._ptrValue, data.c_str(), f._size);
        return f;
    }

    Field Field::from_str_data(const option<std::string> &data, const python::Type &type) {

        Field f;
        f._size = 0;
        f._ptrValue = nullptr;
        if(data.has_value())
            f = from_str_data(data.value(), type);
        f._type = python::Type::makeOptionType(type);
        f._isNull = !data.has_value();

        return f;
    }

    Field::Field(const Tuple &t) {
        // allocate size and then transfer tuple to ptr
        _size = sizeof(Tuple);
        _type = t.getType();
        _isNull = false;

        _ptrValue = reinterpret_cast<uint8_t*>(new Tuple(t));
    }

    Field::Field(const List &l) {
        // allocate size and then transfer tuple to ptr
        _size = sizeof(List);
        _type = l.getType();
        _isNull = false;

        _ptrValue = reinterpret_cast<uint8_t*>(new List(l));
    }

    void Field::tuple_from_vector(const std::vector<Field> &elements) {
        auto t = Tuple::from_vector(elements);

        // call here Tuple constructor
        _size = sizeof(Tuple);
        _type = t.getType();
        _isNull = false;

        _ptrValue = reinterpret_cast<uint8_t*>(new Tuple(t));
    }

    void Field::deep_copy_from_other(const Field &other) {
        if(other.hasPtrData()) {
            assert(_ptrValue == nullptr);

            // special data structs have to perform individual deep copies
            if(other._type.isTupleType()) {
                auto tuple_ptr = reinterpret_cast<Tuple*>(other._ptrValue);
                _ptrValue = reinterpret_cast<uint8_t*>(tuple_ptr->allocate_deep_copy());
                _size = sizeof(Tuple);
            } else if(other._type.isListType()) {
                auto list_ptr = reinterpret_cast<List*>(other._ptrValue);
                _ptrValue = reinterpret_cast<uint8_t*>(list_ptr->allocate_deep_copy());
                _size = sizeof(List);
            } else {
                // dict is currently stored as string...

                // memcpy --> is this correct for Tuple e.g.?
                _size = other._size;

                // special case option type
                if(_size != 0) {
                    _ptrValue = new uint8_t[_size];
                    assert(other._ptrValue);
                    std::memcpy(_ptrValue, other._ptrValue, _size);
                } else {
                    _ptrValue = nullptr;
                }
            }
        } else {
            _iValue = other._iValue;
        }
    }

    Field& Field::operator = (const Field &other) {

        _size = other._size;
        _isNull = other._isNull;

        // special handling:
        // ptr type?
        if(other.hasPtrData()) {
            releaseMemory();
            _ptrValue = nullptr;

            // only invoke deepcopy if size != 0
            if(other._size != 0) {
                assert(other._ptrValue);
                deep_copy_from_other(other);
            }
        } else {
            // primitive val copy (doesn't matter which)
            _iValue = other._iValue;
        }

        _type = other._type;
        return *this;
    }

    void Field::releaseMemory() {
        if(hasPtrData()) {
            if(_ptrValue) {
                // select correct deletion method!
                if(_type.withoutOptions().isListType() || _type.withoutOptions().isTupleType())
                    delete _ptrValue;
                else
                    delete [] _ptrValue;
            }
        }
        _ptrValue = nullptr;
        _size = 0;
    }

    Field::~Field() {
        // check for memory related var fields. If so, delete ptr!
        releaseMemory();
    }

    std::string StringFromCJSONKey(const char* keyString, const char type) {
        assert(keyString);
        switch(type) {
            case 's':
                return "'" + std::string(keyString) + "'";
            case 'b':
                return std::string(keyString);
            case 'i':
                return std::string(keyString);
            case 'f':
                return std::string(keyString);
            default:
                return "badtype";
        }
    }

    std::string StringFromCJSONVal(const cJSON* obj, const char type) {
        switch(type) {
            case 's':
                return "'" + std::string(obj->valuestring) + "'";
            case 'b':
                return cJSON_IsTrue(obj) ? "True" : "False";
            case 'i':
                return std::to_string((int64_t)(obj->valuedouble));
            case 'f': {
                std::ostringstream oss;
                // use up to 5 digits for precision
                // and a non trailing zero format
                oss << std::setprecision(5) << std::noshowpoint << obj->valuedouble;
                return oss.str();
            }
            default:
                return "badtype";
        }
    }

    std::string PrintCJSONDict(cJSON* dict) {
        assert(dict);
        std::string ret = "{";
        cJSON *cur_item = dict->child;
        bool first = true;
        while(cur_item) {
            // add the correct comma
            if(first) first = false;
            else ret += ",";

            char *key = cur_item->string;
            auto keyStr = StringFromCJSONKey(key + 2, key[0]);
            auto valStr = StringFromCJSONVal(cur_item, key[1]);
            ret += keyStr + ":" + valStr;
            cur_item = cur_item->next;
        }
        ret += "}";
        return ret;
    }

    std::string Field::desc() const {
        if(_isNull) // also holds for NULLVALUE type field.
            return "None";

        if(_type == python::Type::PYOBJECT) return "object";

        if(_type.isOptionType())
            return extractDesc(_type.getReturnType());

       return extractDesc(_type);
    }

    std::string Field::extractDesc(const python::Type& type) const {
        if(python::Type::BOOLEAN == type) {
            if(this->_iValue > 0)
                return "True";
            else
                return "False";
        } else if(python::Type::I64 == type) {
            return std::to_string(_iValue);
        } else if(python::Type::F64 == type) {
            std::ostringstream oss;
            // use up to 5 digits for precision
            // and a non trailing zero format
            oss << std::setprecision(5) << std::fixed << _dValue;
            return oss.str();
            return std::to_string(this->_dValue);
        } else if(python::Type::STRING == type) {
            std::string s;
            s = std::string(reinterpret_cast<char*>(_ptrValue));
            return "'" + s + "'";
        } else if(type.isTupleType()) {
            Tuple *t = (Tuple*) this->_ptrValue;
            return t->desc();
        } else if(type.isDictionaryType() || type == python::Type::GENERICDICT) {
            char *dstr = reinterpret_cast<char*>(_ptrValue);
            return PrintCJSONDict(cJSON_Parse(dstr));
        } else if(type.isListType()) {
            List *l = (List*)this->_ptrValue;
            return l->desc();
        } else {
            return "badtype";
        }
    }

    bool operator == (const Field& lhs, const Field& rhs) {
        // check if types match
        if(lhs._type != rhs._type)
            return false;

        // check if has ptr data
        assert(lhs.hasPtrData() == rhs.hasPtrData());

        if(lhs.hasPtrData()) {
            if(lhs._size != rhs._size)
                return false;

            // type dependent check
            if(lhs._type == python::Type::STRING) {
                // perform string comparison
                return strcmp((char*)lhs.getPtr(), (char*)rhs.getPtr()) == 0;
            } else if(lhs._type == python::Type::EMPTYTUPLE ||
                    lhs._type == python::Type::EMPTYLIST ||
                    lhs._type == python::Type::EMPTYDICT) {
                return true;
            } else if(lhs._type.isTupleType()) {
                Tuple *tr= (Tuple*)lhs.getPtr();
                Tuple *tl = (Tuple*)rhs.getPtr();

                return *tr == *tl;

            } else if(lhs._type.isListType()) {
                List *ll = (List*)lhs.getPtr();
                List *lr = (List*)rhs.getPtr();

                return *ll == *lr;
            } else {
                Logger::instance().defaultLogger().error("trying to compare for Field equality of "
                                                         "Field with type " + lhs._type.desc()
                                                         +". Not yet implemented");
                exit(1);
            }
        } else {
            return lhs._iValue == rhs._iValue;
        }
    }

    // needs to be declared here b.c. of incomplete Tuple Type...
    Field Field::empty_tuple() {
        return Field(Tuple());
    }

    Field Field::empty_list() {
        return Field(List());
    }


    Field Field::upcastTo_unsafe(const Field &f, const python::Type &targetType) {
        auto t = f.getType();

        if(f._type == targetType)
            return f;

        // null upcast to any
        if(f._type == python::Type::NULLVALUE && targetType.isOptionType()) {
            Field r;
            r._type = targetType;
            r._isNull = true;
            r._size = 0;
            r._ptrValue = nullptr;
            return r;
        }

        // emptylist to any list
        if(f._type == python::Type::EMPTYLIST && targetType.isListType()) {
            // upcast to list
            throw std::runtime_error("not yet implemented, pls add");
        }

        // emptydict to any dict
        if(f._type == python::Type::EMPTYDICT && targetType.isDictionaryType()) {
            // upcast to any dict
            throw std::runtime_error("not yet implemented, pls add");
        }

        // tuple type, recursive action

        // is f.type not option and target Type is option?
        if(!f._type.isOptionType() && targetType.isOptionType()) {
            Field c = upcastTo_unsafe(f, targetType.elementType());
            c._type = targetType;
            c._isNull = false; // f is not an option type, therefore can't be 0!
            return c;
        }

        if(f._type.isOptionType() && targetType.isOptionType()) {
            auto tmp = f;
            tmp._type = f._type.getReturnType();
            Field c = upcastTo_unsafe(tmp, targetType.elementType());
            c._type = targetType;
            c._isNull = f._isNull;
        }

        if(t == python::Type::BOOLEAN) {
            if(targetType == python::Type::I64)
                return Field((int64_t)f._iValue);
            if(targetType == python::Type::F64)
                return Field((double)f._iValue);
        }

        if(t == python::Type::I64 && targetType == python::Type::F64) {
            return Field((double)f._iValue);
        }

#ifndef NDEBUG
        throw std::runtime_error("bad field in upcast");
#endif
        // @TODO: construct dummy based on target type
        return Field::null();
    }

    Field Field::from_pickled_memory(const uint8_t *buf, size_t buf_size) {
        assert(buf);

        Field f;
        f._isNull = false;
        f._type = python::Type::PYOBJECT;
        f._size = buf_size;
        f._ptrValue =  new uint8_t[buf_size];
        memcpy(f._ptrValue, buf, buf_size);

        return f;
    }
}