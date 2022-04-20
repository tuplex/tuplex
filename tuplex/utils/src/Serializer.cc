//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Serializer.h>
#include <stack>
#include <TupleTree.h>
#include <Utils.h>

static_assert(sizeof(double) == 8, "double must be 64bit");
static_assert(sizeof(int64_t) == 8, "int64 must be 64bit");


// a note about the serialization format:
// if the schema consists of N fields,
// N x 8 bytes are written
// after that, variable length fields are written
// corresponding fields contain the offset from the current position to the start of the variable length argument

namespace tuplex {

    // compute size of bitmap at beginning. Multiple of 8 bytes!
    size_t calcBitmapSize(const std::vector<bool> &bitmap) {
        int num_nullable_fields = 0;
        for (auto b : bitmap)
            num_nullable_fields += b;

        // compute how many bytes are required to store the bitmap!
        size_t num = 0;
        while (num_nullable_fields > 0) {
            num++;
            num_nullable_fields -= 64;
        }
        return num * sizeof(int64_t); // multiple of 64bit because of alignment
    }


    void Buffer::provideSpace(const size_t numBytes) {

        // is buffer initialized?
        if (!_buffer) {
            _bufferCapacity += std::max((int) _growthConstant, (int) numBytes);
            _bufferSize = 0;
            _buffer = malloc(_bufferCapacity);
            if (!_buffer) {
                Logger::instance().logger("memory").error("could not allocate serialization buffer");
                _bufferCapacity = 0;
            }
        } else {
            // check if numBytes can be accommodated, if not realloc!
            if (_bufferSize + numBytes > _bufferCapacity) {
                _bufferCapacity += std::max((int) _growthConstant, (int) numBytes);
                _buffer = realloc(_buffer, _bufferCapacity);
            }
        }
    }

    Serializer &Serializer::reset() {
        // reset size & position to beginning of temp buffer
        _fixedLenFields.reset();
        _varLenFields.reset();
        _varLenFieldOffsets.clear();
        _isVarField.clear();
        _requiresBitmap.clear();
        _isNull.clear();
        fixSchema();
        _col = 0;
        return *this;
    }

    void Serializer::fixSchema() {
        if (_autoSchema && _types.size() > 0) {
            // create schema from types, and set auto Inference to false
            _autoSchema = false;
            _schema = Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType(_types));
            _types.clear();
        }
    }

    Serializer &Serializer::append(const int64_t i) {
        if (_autoSchema) {
            _types.push_back(python::Type::I64);
        } else {
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == python::Type::I64);
        }
        return appendWithoutInference(i);
    }

    Serializer &Serializer::appendWithoutInference(const int64_t i) {
        _isVarField.push_back(false);
        _fixedLenFields.provideSpace(sizeof(int64_t));
        _isNull.push_back(false);
        _requiresBitmap.push_back(false);

        // write to buffer
        *((int64_t *) _fixedLenFields.ptr()) = i;
        _fixedLenFields.movePtr(sizeof(int64_t));
        return *this;
    }

    Serializer &Serializer::append(const bool b) {
        if (_autoSchema) {
            _types.push_back(python::Type::BOOLEAN);
        } else {
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == python::Type::BOOLEAN);
        }
        return appendWithoutInference(b);
    }

    Serializer &Serializer::appendWithoutInference(const bool b) {
        _isVarField.push_back(false);
        _requiresBitmap.push_back(false);
        _isNull.push_back(false);
        _fixedLenFields.provideSpace(sizeof(int64_t));

        // write to buffer
        *((int64_t *) _fixedLenFields.ptr()) = static_cast<int64_t>(b);
        _fixedLenFields.movePtr(sizeof(int64_t));
        return *this;
    }

    Serializer &Serializer::append(const option<bool> &b) {
        if (_autoSchema)
            _types.push_back(python::Type::makeOptionType(python::Type::BOOLEAN));
        else
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == python::Type::makeOptionType(python::Type::BOOLEAN));

        return appendWithoutInference(b);
    }

    Serializer &Serializer::appendWithoutInference(const tuplex::option<bool> &b) {
        _isVarField.push_back(false);
        _requiresBitmap.push_back(true);
        _isNull.push_back(!b.has_value());
        _fixedLenFields.provideSpace(sizeof(int64_t));

        // write to buffer
        *((int64_t *) _fixedLenFields.ptr()) = static_cast<int64_t>(b.value_or(false));
        _fixedLenFields.movePtr(sizeof(int64_t));

        return *this;
    }

    Serializer &Serializer::append(const option<int64_t> &i) {
        if (_autoSchema)
            _types.push_back(python::Type::makeOptionType(python::Type::I64));
        else
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == python::Type::makeOptionType(python::Type::I64));
        return appendWithoutInference(i);
    }

    Serializer &Serializer::appendWithoutInference(const tuplex::option<int64_t> &i) {
        _isVarField.push_back(false);
        _fixedLenFields.provideSpace(sizeof(int64_t));
        _isNull.push_back(!i.has_value());
        _requiresBitmap.push_back(true);

        // write to buffer
        *((int64_t *) _fixedLenFields.ptr()) = i.value_or(0);
        _fixedLenFields.movePtr(sizeof(int64_t));
        return *this;
    }

    Serializer &Serializer::append(const option<double> &d) {
        if (_autoSchema)
            _types.push_back(python::Type::makeOptionType(python::Type::F64));
        else
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == python::Type::makeOptionType(python::Type::F64));

        return appendWithoutInference(d);
    }

    Serializer &Serializer::appendWithoutInference(const tuplex::option<double> &d) {
        _isVarField.push_back(false);
        _requiresBitmap.push_back(true);
        _isNull.push_back(!d.has_value());
        _fixedLenFields.provideSpace(sizeof(double));

        // write to buffer
        *((double *) _fixedLenFields.ptr()) = d.value_or(0.0);
        _fixedLenFields.movePtr(sizeof(double));
        return *this;
    }

    Serializer &Serializer::append(const double d) {
        if (_autoSchema) {
            _types.push_back(python::Type::F64);
        } else {
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == python::Type::F64);
        }
        return appendWithoutInference(d);
    }

    Serializer &Serializer::appendWithoutInference(const double d) {
        _isVarField.push_back(false);
        _requiresBitmap.push_back(false);
        _isNull.push_back(false);
        _fixedLenFields.provideSpace(sizeof(double));

        // write to buffer
        *((double *) _fixedLenFields.ptr()) = d;
        _fixedLenFields.movePtr(sizeof(double));
        return *this;
    }

    Serializer &Serializer::append(const std::string &str) {

        // this is a variable length field
        // add info std::vector
        // later: only in autoSchema mode!
        if (_autoSchema) {
            _types.push_back(python::Type::STRING);
        } else {
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == python::Type::STRING);
        }

        return appendWithoutInference(str);
    }

    Serializer& Serializer::appendObject(const uint8_t* buf, size_t bufSize) {
        if(_autoSchema)
            _types.push_back(python::Type::PYOBJECT);
        else
            assert(_schema.getRowType().parameters()[_col++] == python::Type::PYOBJECT);
        return appendWithoutInference(buf, bufSize);
    }

    Serializer & Serializer::appendWithoutInference(const uint8_t *buf, size_t bufSize) {
        _isVarField.push_back(true);
        _isNull.push_back(false);
        _requiresBitmap.push_back(false);

        // add a 8 byte offset
        _fixedLenFields.provideSpace(sizeof(int64_t));

        // write to buffer
        *((int64_t *) _fixedLenFields.ptr()) = 0L;
        _fixedLenFields.movePtr(sizeof(int64_t));

        // store current size
        _varLenFieldOffsets.push_back(_varLenFields.size());

        // copy out
        _varLenFields.provideSpace(bufSize);

        std::memcpy(_varLenFields.ptr(), buf, bufSize);
        _varLenFields.movePtr(bufSize);

        return *this;
    }

    Serializer &Serializer::appendWithoutInference(const std::string &str) {
        _isVarField.push_back(true);
        _isNull.push_back(false);
        _requiresBitmap.push_back(false);

        // add a 8 byte offset
        _fixedLenFields.provideSpace(sizeof(int64_t));

        // write to buffer
        *((int64_t *) _fixedLenFields.ptr()) = 0L;
        _fixedLenFields.movePtr(sizeof(int64_t));

        // as chars (later UTF8 support here!!!)
        _varLenFieldOffsets.push_back(_varLenFields.size());

        const char *cstr = str.c_str();
        auto slen = strlen(cstr);
        assert(slen == str.length());

        // copy out, +1 for '\0' char
        _varLenFields.provideSpace(slen + 1);

        std::memcpy(_varLenFields.ptr(), cstr, slen);
        *((uint8_t *) _varLenFields.ptr() + slen) = 0;
        _varLenFields.movePtr(slen + 1);

        return *this;
    }

    Serializer &Serializer::append(const option<std::string> &str) {
        if (_autoSchema)
            _types.push_back(python::Type::makeOptionType(python::Type::STRING));
        else
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == python::Type::makeOptionType(python::Type::STRING));

        return appendWithoutInference(str);
    }

    Serializer & Serializer::appendNull() {
        if(_autoSchema)
            _types.push_back(python::Type::NULLVALUE);

        // append null value
        // nothing to do, nulls don't get serialized
        return *this;
    }

    Serializer &Serializer::appendWithoutInference(const tuplex::option<std::string> &str) {
        _isVarField.push_back(true);
        _isNull.push_back(!str.has_value());
        _requiresBitmap.push_back(true);

        // add a 8 byte offset
        _fixedLenFields.provideSpace(sizeof(int64_t));

        // write to buffer
        *((int64_t *) _fixedLenFields.ptr()) = 0L;
        _fixedLenFields.movePtr(sizeof(int64_t));

        // as chars (later UTF8 support here!!!)
        _varLenFieldOffsets.push_back(_varLenFields.size());

        // copy if value
        if (str.has_value()) {
            auto slen = str.value().length();
            // copy out, +1 for '\0' char
            _varLenFields.provideSpace(slen + 1);

            std::memcpy(_varLenFields.ptr(), str.value().c_str(), slen);
            *((uint8_t *) _varLenFields.ptr() + slen) = 0;
            _varLenFields.movePtr(slen + 1);
        }

        return *this;
    }

    Serializer &Serializer::append(const option<Tuple> &t, const python::Type &tupleType) {
        if(_autoSchema) {
            _types.push_back(python::Type::makeOptionType(tupleType));
        } else {
            assert(_schema.getRowType().parameters()[_col++] == python::Type::makeOptionType(tupleType));
        }

        if(tupleType == python::Type::EMPTYTUPLE || (tupleType.isOptionType() && tupleType.getReturnType() == python::Type::EMPTYTUPLE)) {
            _isNull.push_back(!t.has_value());
            _requiresBitmap.push_back(true);
            _isVarField.push_back(false);
            return *this;
        }

        return appendWithoutInference(t, tupleType.isOptionType() ? tupleType.getReturnType() : tupleType);
    }

    Serializer &Serializer::appendWithoutInference(const option<Tuple> &tuple, const python::Type &tupleType) {
        assert(!tupleType.isOptionType() && tupleType != python::Type::EMPTYTUPLE);
        _isVarField.push_back(true);
        _isNull.push_back(!tuple.has_value());
        _requiresBitmap.push_back(true);

        _fixedLenFields.provideSpace(sizeof(uint64_t));
        *((uint64_t *)_fixedLenFields.ptr()) = 0;
        _fixedLenFields.movePtr(sizeof(uint64_t));

        _varLenFieldOffsets.push_back(_varLenFields.size());

        if(tuple.has_value()) {
            Tuple tupleData = tuple.data();
            appendWithoutInferenceHelper(tupleData);
        }

        return *this;
    }

    Serializer &Serializer::appendWithoutInference(const option<List> &list, const python::Type &listType) {
        assert(!listType.isOptionType() && listType != python::Type::EMPTYLIST);
        bool isVar = !(listType.elementType().isSingleValued());
        _isVarField.push_back(isVar);
        _isNull.push_back(!list.has_value());
        _requiresBitmap.push_back(true);

        // add a 8 byte offset
        _fixedLenFields.provideSpace(sizeof(int64_t));

        // write to buffer - if it's not variable and not null, just put the number of elements directly in the fixed len field
        *((int64_t *) _fixedLenFields.ptr()) = (isVar || !list.has_value()) ? 0L : list.data().numElements();
        _fixedLenFields.movePtr(sizeof(int64_t));

        _varLenFieldOffsets.push_back(_varLenFields.size());

        if(isVar && list.has_value()) {
            // get list data
            auto l = list.data();
            appendWithoutInferenceHelper(l);
        }
        return *this;
    }

    Serializer &Serializer::append(const std::string &dict, const python::Type &dictType) {
        // this is a variable length field
        // add info std::vector
        // later: only in autoSchema mode!
        if (_autoSchema) {
            _types.push_back(dictType);
        } else {
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == dictType);
        }

        if(dictType == python::Type::EMPTYDICT)
            return *this;

        return appendWithoutInference(Field::from_str_data(dict, dictType));
    }

    Serializer &Serializer::append(const option<std::string> &dict, const python::Type &dictType) {
        if (_autoSchema)
            _types.push_back(python::Type::makeOptionType(dictType));
        else
            assert(_schema.getRowType().parameters()[_col++] == python::Type::makeOptionType(dictType));

        if(dictType == python::Type::EMPTYDICT || (dictType.isOptionType() && dictType.getReturnType() == python::Type::EMPTYDICT)) {
            _isNull.push_back(!dict.has_value());
            _requiresBitmap.push_back(true);
            _isVarField.push_back(false);
            return *this;
        }

        return appendWithoutInference(!dict.has_value() ? option<std::string>::none :
                                      option<std::string>(std::string((char *) dict.value().c_str())));
    }

    Serializer &Serializer::append(const option<List>& list, const python::Type &listType) {
        // variable length field
        if(_autoSchema) {
            _types.push_back(python::Type::makeOptionType(listType));
        } else {
            assert(_schema.getRowType().parameters()[_col++] == python::Type::makeOptionType(listType));
        }

        if(listType == python::Type::EMPTYLIST || (listType.isOptionType() && listType.getReturnType() == python::Type::EMPTYLIST)) {
            _isNull.push_back(!list.has_value());
            _requiresBitmap.push_back(true);
            _isVarField.push_back(false);
            return *this;
        }

        return appendWithoutInference(list, listType.isOptionType() ? listType.getReturnType() : listType);
    }

    Serializer &Serializer::append(const NullType &n) {
        return appendNull();
    }

    Serializer &Serializer::appendEmptyTupleOption(const Field &f) {
        assert(f.getType().isOptionType() && f.getType().getReturnType() == python::Type::EMPTYTUPLE);
        if (_autoSchema)
            _types.push_back(python::Type::makeOptionType(python::Type::EMPTYTUPLE));
        else
            assert(_schema.getRowType().parameters()[_col++] == python::Type::makeOptionType(python::Type::EMPTYTUPLE));
        return appendWithoutInference(f);
    }

    Serializer &Serializer::appendWithoutInference(const Field f) {
        if (python::Type::BOOLEAN == f.getType())
            return appendWithoutInference(static_cast<bool>(f.getInt()));
        else if (python::Type::I64 == f.getType())
            return appendWithoutInference(f.getInt());
        else if (python::Type::F64 == f.getType())
            return appendWithoutInference(f.getDouble());
        else if (python::Type::STRING == f.getType())
            return appendWithoutInference(std::string((char *) f.getPtr()));
        else if (python::Type::EMPTYDICT == f.getType()) {
            // nothing
        } else if (python::Type::GENERICDICT == f.getType() || f.getType().isDictionaryType()) {
            return appendWithoutInference(std::string((char *) f.getPtr()));
        } else if (python::Type::EMPTYTUPLE == f.getType()) {
            // nothing
        } else if(python::Type::NULLVALUE == f.getType()) {
            // nothing
        } else if(python::Type::EMPTYLIST == f.getType()) {
            // nothing
        } else if(f.getType().isListType()) {
            return appendWithoutInference(*(List*)f.getPtr());
        }
        else if (f.getType().isOptionType()) {

            // get underlying type
            auto t = f.getType().getReturnType();

            if (python::Type::BOOLEAN == t)
                return appendWithoutInference(
                        f.isNull() ? option<bool>::none : option<bool>(static_cast<bool>(f.getInt())));
            else if (python::Type::I64 == t)
                return appendWithoutInference(f.isNull() ? option<int64_t>::none : option<int64_t>(f.getInt()));
            else if (python::Type::F64 == t)
                return appendWithoutInference(f.isNull() ? option<double>::none : option<double>(f.getDouble()));
            else if (python::Type::STRING == t)
                return appendWithoutInference(
                        f.isNull() ? option<std::string>::none : option<std::string>(std::string((char *) f.getPtr())));
            else if (python::Type::EMPTYDICT == t) {
                // optional empty dict
                _isVarField.push_back(false);
                _isNull.push_back(f.isNull());
                _requiresBitmap.push_back(true);
            }
            else if (python::Type::GENERICDICT == t || t.isDictionaryType()) {
                return appendWithoutInference(
                        f.isNull() ? option<std::string>::none : option<std::string>(std::string((char *) f.getPtr())));
            } else if (python::Type::EMPTYTUPLE == t) {
                // optional empty tuple
                _isVarField.push_back(false);
                _isNull.push_back(f.isNull());
                _requiresBitmap.push_back(true);
            } else if (t.isListType()) {
                return appendWithoutInference(f.isNull() ? option<List>::none : option<List>(*(List*)f.getPtr()), t);
            } else if (t.isTupleType()) {
                return appendWithoutInference(f.isNull() ? option<Tuple>::none : option<Tuple>(*(Tuple*)f.getPtr()), t);
            } else
                // throw std::runtime_error("unknown type " + f.getType().desc() + " found, can't serialize.");
                Logger::instance().logger("serializer").error(
                        "unknown field type " + f.getType().desc() + " encountered, can't serialize.");
        } else {
            Logger::instance().logger("serializer").error(
                    "unknown field type " + f.getType().desc() + " encountered, can't serialize.");
        }

        return *this;
    }

    Serializer &Serializer::append(const Tuple &t) {
        if (_autoSchema) {
            _types.push_back(t.getType());
        } else {
            // type check in debug mode
            assert(_schema.getRowType().parameters()[_col++] == t.getType());
        }

        // special case: empty tuple
        if (t.numElements() == 0) {
            assert(t.getType() == python::Type::EMPTYTUPLE);
            return *this; // don't append anything at all
        }

        // because of the way flattened tuples are constructed (DFS), they can be easily serialized
        auto tree = tupleToTree(t);

        // add flattened fields
        for (auto index : tree.getMultiIndices()) {

            Field f = tree.get(index);

            // always need to set varfield

            // serialize field
            assert(!f.getType().isTupleType() || f.getType() == python::Type::EMPTYTUPLE);
            appendWithoutInference(f);
        }

        return *this;
    }

    Serializer &Serializer::append(const List &l) {
        if(_autoSchema) {
            _types.push_back(l.getType());
        } else {
            assert(_schema.getRowType().parameters()[_col++] == l.getType());
        }

        if(l.getType() == python::Type::EMPTYLIST)
            return *this;

        return appendWithoutInference(l);
    }

    Serializer &Serializer::appendWithoutInference(const List &l) {
        assert(l.getType() != python::Type::EMPTYLIST);
        bool isVar = !(l.getType().elementType().isSingleValued());
        _isVarField.push_back(isVar);
        _isNull.push_back(false);
        _requiresBitmap.push_back(false);

        // add a 8 byte offset
        _fixedLenFields.provideSpace(sizeof(int64_t));

        // write to buffer - if it's not variable, just put the number of elements directly in the fixed len field
        *((int64_t *) _fixedLenFields.ptr()) = isVar ? 0L : l.numElements();
        _fixedLenFields.movePtr(sizeof(int64_t));

        if(isVar) {
            // as chars (later UTF8 support here!!!)
            _varLenFieldOffsets.push_back(_varLenFields.size());

            appendWithoutInferenceHelper(l);
        }
        return *this;
    }

    Serializer &Serializer::appendWithoutInferenceHelper(const Tuple &t) {
        auto tree = tupleToTree(t);
        tree.tupleType();

        // need bitmap field for elements?
        std::vector<bool> bitmapV;
        void *bitmapAddr = nullptr;
        size_t bitmapSize = 0;
        size_t numOptionalFields = python::numOptionalFields(t.getType());
        if(numOptionalFields) {
            auto numBitmapFields = core::ceilToMultiple(numOptionalFields, 64ul)/64;
            bitmapSize = numBitmapFields * sizeof(uint64_t);
            _varLenFields.provideSpace(bitmapSize);
            bitmapAddr = _varLenFields.ptr();
            _varLenFields.movePtr(bitmapSize);
        }

        // std::vector<int>: index for a varLen type in the tuple
        // void *: address for storing offset to actual data for the varlen type
        std::vector<std::pair<std::vector<int>, void *>> varLenOffsetAddrStack;
        for(const auto &index : tree.getMultiIndices()) {
            auto currField = tree.get(index);
            auto currFieldType = currField.getType();
            if(currFieldType.isSingleValued()) {
                continue;
            }
            if(currFieldType.isOptionType()) {
                // only write to bitmap for optional field in tuple
                if(currField.isNull()) {
                    bitmapV.push_back(true);
                } else {
                    // need actual type
                    currFieldType = currFieldType.getReturnType();
                    bitmapV.push_back(false);
                }
            }
            if (!currField.isNull()) {
                // assume 8 bytes for each field
                _varLenFields.provideSpace(sizeof(uint64_t));
                // either fill in actual data for fixed length field
                // or record current address to varLenOffsetAddr for filling in offset to actual data later
                if(currFieldType == python::Type::I64 || currFieldType == python::Type::BOOLEAN) {
                    *((uint64_t *)_varLenFields.ptr()) = currField.getInt();
                } else if(currFieldType == python::Type::F64) {
                    // double is 8 bytes
                    *((double *)_varLenFields.ptr()) = currField.getDouble();
                } else if(currFieldType == python::Type::STRING || currFieldType.isListType()) {
                    // skip 8 bytes as placeholder to fill in offset later
                    varLenOffsetAddrStack.emplace_back(index, _varLenFields.ptr());
                }
                _varLenFields.movePtr(sizeof(uint64_t));
            }
        }
        // append varLen fields
        for(const auto &varLenOffsetAddr : varLenOffsetAddrStack) {
            auto currVarLenField = tree.get(varLenOffsetAddr.first);
            auto currVarLenFieldType = currVarLenField.getType();
            if(currVarLenFieldType.isOptionType()) {
                currVarLenFieldType = currVarLenFieldType.getReturnType();
            }
            void *currVarLenOffsetAddr = varLenOffsetAddr.second;
            uint64_t currOffset = (uintptr_t)_varLenFields.ptr() - (uintptr_t)currVarLenOffsetAddr;
            // write offset to placeholder then append varLen field
            if(currVarLenFieldType == python::Type::STRING) {
                // write size | offset to placeholder
                uint64_t strSize = strlen((char *)currVarLenField.getPtr()) + 1;
                uint64_t sizeOffset = currOffset | (strSize << 32);
                *((uint64_t *)currVarLenOffsetAddr) = sizeOffset;
                appendWithoutInferenceHelper(std::string((char *) currVarLenField.getPtr()));
            } else if(currVarLenFieldType.isListType()) {
                // write offset to placeholder
                *((uint64_t *)currVarLenOffsetAddr) = currOffset;
                appendWithoutInferenceHelper(*(List *) currVarLenField.getPtr());
            } else if(currVarLenFieldType.isTupleType()) {
                // has to be an optional tuple otherwise it would have been flattened away
                assert(currVarLenField.getType().isOptionType());
                appendWithoutInferenceHelper(*(Tuple *) currVarLenField.getPtr());
            } else {
                // throw std::runtime_error("element type not support in tuple: " + currVarLenField.getType().desc());
                Logger::instance().logger("serializer").error(
                        "invalid element type in tuple: " + currVarLenField.getType().desc() + " encountered, can't serialize.");
            }
        }

        // write bitmap if exists
        if (bitmapSize) {
            uint64_t bitmap[bitmapSize / sizeof(int64_t)];
            std::memset(bitmap, 0, bitmapSize);

            int opt_counter = 0;
            assert(bitmapV.size() == numOptionalFields);
            for (int i = 0; i < numOptionalFields; i++) {
                // set bit
                if (bitmapV[i]) {
                    bitmap[i / 64] |= (1UL << (i % 64));
                }
            }
            // write to ptr
            std::memcpy(bitmapAddr, bitmap, bitmapSize);
        }
        return *this;
    }

    Serializer &Serializer::appendWithoutInferenceHelper(const std::string &str) {
        // append the string to of _varLenFields
        const char *cStr = str.c_str();
        auto strSize = strlen(cStr) + 1;
        _varLenFields.provideSpace(strSize);
        std::memcpy(_varLenFields.ptr(), cStr ,strSize);
        _varLenFields.movePtr(strSize);
        return *this;
    }

    Serializer &Serializer::appendWithoutInferenceHelper(const List &l) {

        // add number of elements
        _varLenFields.provideSpace(sizeof(uint64_t));
        *((uint64_t *)_varLenFields.ptr()) = l.numElements();
        _varLenFields.movePtr(sizeof(uint64_t));

        auto elementType = l.getType().elementType();
        if(elementType.isSingleValued()) {
            // done. List can be retrieved from numElements and listType
            return *this;
        }

        // need bitmap field for elements?
        std::vector<bool> bitmapV;
        void *bitmapAddr = nullptr;
        size_t bitmapSize = 0;
        if(elementType.isOptionType()) {
            auto numBitmapFields = core::ceilToMultiple(l.numElements(), 64ul)/64;
            bitmapSize = numBitmapFields * sizeof(uint64_t);
            _varLenFields.provideSpace(bitmapSize);
            bitmapAddr = _varLenFields.ptr();
            _varLenFields.movePtr(bitmapSize);
        }

        if(elementType == python::Type::STRING) { // strings are serialized differently
            // offset numbers
            size_t current_offset = sizeof(uint64_t) * l.numElements();
            for (size_t i = 0; i < l.numElements(); i++) {
                _varLenFields.provideSpace(sizeof(uint64_t));
                *((uint64_t *) _varLenFields.ptr()) = current_offset;
                _varLenFields.movePtr(sizeof(uint64_t));
                // update for next field: move forward one uint64_t, then add on the string
                current_offset -= sizeof(uint64_t);
                current_offset += strlen((char *) l.getField(i).getPtr()) + 1;
            }
            // string data
            for (size_t i = 0; i < l.numElements(); i++) {
                size_t slen = strlen((char*)l.getField(i).getPtr());
                _varLenFields.provideSpace(slen + 1);
                std::memcpy(_varLenFields.ptr(), l.getField(i).getPtr(), slen);
                *((uint8_t *) _varLenFields.ptr() + slen) = 0;
                _varLenFields.movePtr(slen + 1);
            }
        } else if(elementType.isTupleType()) {
            void *varLenOffsetAddr = _varLenFields.ptr();
            // skip #elements * 8 bytes as placeholder for offsets
            auto offsetBytes = l.numElements() * sizeof(uint64_t);
            _varLenFields.provideSpace(offsetBytes);
            _varLenFields.movePtr(offsetBytes);
            for (size_t listIndex = 0; listIndex < l.numElements(); ++listIndex) {
                // write offset to placeholder
                uint64_t currOffset = (uintptr_t)_varLenFields.ptr() - (uintptr_t)varLenOffsetAddr;
                *(uint64_t *)varLenOffsetAddr = currOffset;
                // increment varLenOffsetAddr by 8
                varLenOffsetAddr = (void *)((uint64_t *)varLenOffsetAddr + 1);
                // append tuple
                auto currTuple = *(Tuple *)(l.getField(listIndex).getPtr());
                appendWithoutInferenceHelper(currTuple);
            }
        }  else if (elementType.isListType()) {
            void *varLenOffsetAddr = _varLenFields.ptr();
            // skip #elements * 8 bytes as placeholder for offsets
            auto offsetBytes = l.numElements() * sizeof(uint64_t);
            _varLenFields.provideSpace(offsetBytes);
            _varLenFields.movePtr(offsetBytes);
            for (size_t listIndex = 0; listIndex < l.numElements(); ++listIndex) {
                // write offset to placeholder
                uint64_t currOffset = (uintptr_t)_varLenFields.ptr() - (uintptr_t)varLenOffsetAddr;
                *(uint64_t *)varLenOffsetAddr = currOffset;
                // increment varLenOffsetAddr by 8
                varLenOffsetAddr = (void *)((uint64_t *)varLenOffsetAddr + 1);
                // append list
                auto currList = *(List *)(l.getField(listIndex).getPtr());
                appendWithoutInferenceHelper(currList);
            }
        } else if(elementType == python::Type::I64 || elementType == python::Type::BOOLEAN) {
            for(size_t i = 0; i < l.numElements(); i++) {
                _varLenFields.provideSpace(sizeof(uint64_t));
                *((uint64_t*)_varLenFields.ptr()) = l.getField(i).getInt();
                _varLenFields.movePtr(sizeof(uint64_t));
            }
        } else if(elementType == python::Type::F64) {
            for(size_t i = 0; i < l.numElements(); i++) {
                _varLenFields.provideSpace(sizeof(uint64_t));
                *((double*)_varLenFields.ptr()) = l.getField(i).getDouble();
                _varLenFields.movePtr(sizeof(uint64_t));
            }
        } else if(elementType.isOptionType()) {
            auto underlyingElementType = elementType.getReturnType();
            size_t numNonNullElements = l.numNonNullElements();
            if(underlyingElementType == python::Type::STRING) {
                // offset numbers
                size_t currentOffset = sizeof(uint64_t) * numNonNullElements;
                for(size_t i = 0; i < l.numElements(); i++) {
                    if(l.getField(i).isNull()) {
                        bitmapV.push_back(true);
                    } else {
                        bitmapV.push_back(false);
                        // write offset
                        _varLenFields.provideSpace(sizeof(uint64_t));
                        *((uint64_t *) _varLenFields.ptr()) = currentOffset;
                        _varLenFields.movePtr(sizeof(uint64_t));
                        // update for next field: move forward one uint64_t, then add on the string
                        currentOffset -= sizeof(uint64_t);
                        currentOffset += strlen((char *) l.getField(i).getPtr()) + 1;
                    }
                }
                // string data
                for (size_t i = 0; i < l.numElements(); i++) {
                    if(!l.getField(i).isNull()) {
                        size_t slen = strlen((char*)l.getField(i).getPtr());
                        _varLenFields.provideSpace(slen + 1);
                        std::memcpy(_varLenFields.ptr(), l.getField(i).getPtr(), slen);
                        *((uint8_t *) _varLenFields.ptr() + slen) = 0;
                        _varLenFields.movePtr(slen + 1);
                    }
                }
            } else if(underlyingElementType.isTupleType()) {
                void *varLenOffsetAddr = _varLenFields.ptr();
                // skip #elements * 8 bytes as placeholder for offsets
                auto offsetBytes = numNonNullElements * sizeof(uint64_t);
                _varLenFields.provideSpace(offsetBytes);
                _varLenFields.movePtr(offsetBytes);
                for (size_t listIndex = 0; listIndex < l.numElements(); ++listIndex) {
                    if(l.getField(listIndex).isNull()) {
                        bitmapV.push_back(true);
                    } else {
                        bitmapV.push_back(false);
                        // write offset to placeholder
                        uint64_t currOffset = (uintptr_t)_varLenFields.ptr() - (uintptr_t)varLenOffsetAddr;
                        *(uint64_t *)varLenOffsetAddr = currOffset;
                        // increment varLenOffsetAddr by 8
                        varLenOffsetAddr = (void *)((uint64_t *)varLenOffsetAddr + 1);
                        // append tuple
                        auto currTuple = *(Tuple *)(l.getField(listIndex).getPtr());
                        appendWithoutInferenceHelper(currTuple);
                    }
                }
            } else if(underlyingElementType.isListType()) {
                void *varLenOffsetAddr = _varLenFields.ptr();
                // skip #elements * 8 bytes as placeholder for offsets
                auto offsetBytes = l.numNonNullElements() * sizeof(uint64_t);
                _varLenFields.provideSpace(offsetBytes);
                _varLenFields.movePtr(offsetBytes);
                for (size_t listIndex = 0; listIndex < l.numElements(); ++listIndex) {
                    if(l.getField(listIndex).isNull()) {
                        bitmapV.push_back(true);
                    } else {
                        bitmapV.push_back(false);
                        // write offset to placeholder
                        uint64_t currOffset = (uintptr_t)_varLenFields.ptr() - (uintptr_t)varLenOffsetAddr;
                        *(uint64_t *)varLenOffsetAddr = currOffset;
                        // increment varLenOffsetAddr by 8
                        varLenOffsetAddr = (void *)((uint64_t *)varLenOffsetAddr + 1);
                        // append list
                        auto currList = *(List *)(l.getField(listIndex).getPtr());
                        appendWithoutInferenceHelper(currList);
                    }
                }
            } else if(underlyingElementType == python::Type::I64 || underlyingElementType == python::Type::BOOLEAN) {
                for(size_t i = 0; i < l.numElements(); i++) {
                    if(l.getField(i).isNull()) {
                        bitmapV.push_back(true);
                    } else {
                        bitmapV.push_back(false);
                        _varLenFields.provideSpace(sizeof(uint64_t));
                        *((uint64_t*)_varLenFields.ptr()) = l.getField(i).getInt();
                        _varLenFields.movePtr(sizeof(uint64_t));
                    }
                }
            } else if(underlyingElementType == python::Type::F64) {
                for(size_t i = 0; i < l.numElements(); i++) {
                    if(l.getField(i).isNull()) {
                        bitmapV.push_back(true);
                    } else {
                        bitmapV.push_back(false);
                        _varLenFields.provideSpace(sizeof(uint64_t));
                        *((double*)_varLenFields.ptr()) = l.getField(i).getDouble();
                        _varLenFields.movePtr(sizeof(uint64_t));
                    }
                }
            } else {
                // throw std::runtime_error("serializing invalid list type!: " + l.getType().desc());
                Logger::instance().logger("serializer").error(
                        "invalid list type: " + l.getType().desc() + " encountered, can't serialize.");
            }
        } else {
            // throw std::runtime_error("serializing invalid list type!: " + l.getType().desc());
            Logger::instance().logger("serializer").error(
                    "invalid list type: " + l.getType().desc() + " encountered, can't serialize.");
        }

        // write bitmap if exists
        if (bitmapSize) {
            uint64_t bitmap[bitmapSize / sizeof(int64_t)];
            std::memset(bitmap, 0, bitmapSize);

            int opt_counter = 0;
            for (int i = 0; i < bitmapV.size(); i++) {
                // set bit
                if (bitmapV[i]) {
                    bitmap[i / 64] |= (1UL << (i % 64));
                }
            }
            // write to ptr
            std::memcpy(bitmapAddr, bitmap, bitmapSize);
        }

        return *this;
    }

    size_t Serializer::serialize(void *ptr, const size_t capacityLeft) {

        // if not done, fix schema
        fixSchema();

        // ensure invariants
        assert(_requiresBitmap.size() == _isVarField.size());
        assert(_isNull.size() == _isVarField.size());


        // first compute if enough space is available
        // fixed len fields + var len fields + additional field for var len size
        size_t size = _fixedLenFields.size();
        if (hasSchemaVarLenFields()) // important to use check on _isVarField here because option might be null
            size += _varLenFields.size() + sizeof(int64_t);

        // compute bitmap size
        // TODO:
        auto bitmapSize = calcBitmapSize(_requiresBitmap);
        size += bitmapSize;

        if (size > capacityLeft)
            return 0;
        else {
            // serialize by copying both buffers to ptr
            // fixedlen should be always valid
            assert(_fixedLenFields.size() >= 0); // can be 0 when e.g. NULLs are present...

            // write bitmap if it exists
            if (bitmapSize) {
                int64_t bitmap[bitmapSize / sizeof(int64_t)];
                std::memset(bitmap, 0, bitmapSize);

                int opt_counter = 0;
                for (int i = 0; i < _isNull.size(); ++i) {
                    // set bit
                    if (_isNull[i] && _requiresBitmap[i]) {
                        bitmap[opt_counter / 64] |= (1UL << (opt_counter % 64));
                    }
                    if(_requiresBitmap[i])
                        opt_counter++;
                }

                // write to ptr
                std::memcpy(ptr, bitmap, bitmapSize);
            }

            std::memcpy((uint8_t *) ptr + bitmapSize, _fixedLenFields.buffer(), _fixedLenFields.size());

            // always write this addr if varlen fields are present
            if(hasSchemaVarLenFields())
                // write length of all varlen attributes after fixed len attributes
                *((int64_t *) ((uint8_t *) ptr + bitmapSize + _fixedLenFields.size())) = _varLenFields.size();

            if (_varLenFields.size() > 0) {

                // copy varlenfields over
                std::memcpy((uint8_t *) ptr + bitmapSize + _fixedLenFields.size() + sizeof(int64_t),
                            _varLenFields.buffer(), _varLenFields.size());

                // set correct offsets in buffer
                int offset = 0;
                auto it = _varLenFieldOffsets.begin();
                int64_t curVarLenOffsetFromStart = static_cast<int64_t >(_fixedLenFields.size() + sizeof(int64_t));
                int iVarField = 0;
                int iLastVarField = numVarFields() - 1;

                assert(_isVarField.size() == numFields());
                for (int i = 0; i < numFields(); i++) {
                    if (_isVarField[i]) {
                        int64_t varLenOffset = curVarLenOffsetFromStart + static_cast<int64_t>(*it) - offset;

                        // compute varFieldSize from offsets.
                        size_t s =
                                iVarField == iLastVarField ? _varLenFields.size() - _varLenFieldOffsets[iLastVarField] :
                                _varLenFieldOffsets[iVarField + 1] - _varLenFieldOffsets[iVarField];
                        int64_t varFieldSize = static_cast<int64_t>(s);
                        assert(varFieldSize >= 0);

                        // lower 32bit are offset, higher 32bit size in bytes of this varfield.
                        int64_t info = varLenOffset | (varFieldSize << 32);
                        // write offset to ptr
                        *((int64_t *) ((uint8_t *) ptr + bitmapSize + offset)) = info;

                        // next var offset
                        ++it;
                        iVarField++;
                    }
                    offset += sizeof(int64_t);
                }
            }

#ifndef NDEBUG
            // important, make check that sizes match
            Deserializer ds(this->_schema);
            assert(ds.inferLength(ptr) == size);
#endif
            return size;
        }
    }


    size_t Serializer::length() {
        // just use the size calculations from above
        // if not done, fix schema
        fixSchema();

        // first compute if enough space is available
        // fixed len fields + var len fields + additional field for var len size

        // note: Kudos for Ben Givertz for discovering a nasty bug here. For the additional varlen field, do not
        // base the check on varlenFields.size being > 0, instead base it on the type.
        // Else, for option of varlen field, when everything is NULL, the size will be 0.
        auto varLenFieldSize = hasSchemaVarLenFields() ? sizeof(int64_t) : 0;
        return _fixedLenFields.size() + _varLenFields.size() + varLenFieldSize +
               calcBitmapSize(_requiresBitmap);
    }

    Deserializer::Deserializer(const Schema &schema) : _schema(schema), _buffer(nullptr), _numSerializedFields(0) {

        // get flattened type representation
        _flattenedRowType = flattenedType(_schema.getRowType());

        // determine from flattened Schema which fields are varlength
        auto params = _flattenedRowType.parameters();

        size_t curIdx = 0;
        for (int i = 0; i < params.size(); ++i) {
            auto el = params[i];

            auto type = el.isOptionType() ? el.getReturnType() : el;

            // types that do not get serialized, because they're constants
            if(!type.isSingleValued()) {
                _numSerializedFields++;
                _idxMap[i] = curIdx++;
            }

            // only option types require a bitmap entry!
            _requiresBitmap.push_back(el.isOptionType());

            // IMPORTANT: _isVarLenField has a value for every single object, so it is not the exact same as _isVarLenField in Serializer
            if(type.isSingleValued()) {
                _isVarLenField.push_back(false); // Opt[()], Opt[{}] are not varlenfields
            } else if (type == python::Type::BOOLEAN
                || type == python::Type::I64
                || type == python::Type::F64
                || (type.isListType() && type.elementType().isSingleValued())) {
                _isVarLenField.push_back(false); // logical
            } else if (type == python::Type::STRING ||
                       type == python::Type::PYOBJECT ||
                       type.isDictionaryType() ||
                       type == python::Type::GENERICDICT ||
                       (type.isListType() && !type.elementType().isSingleValued()) ||
                       (el.isOptionType() && type.isTupleType())) {
                _isVarLenField.push_back(true);
            } else {
                Logger::instance().logger("core").error("non deserializable type '" + el.desc() + "' detected");
            }
        }
    }

    bool Deserializer::hasSchemaVarLenFields() const {
        // from _isVarLenField, if any element is set to true return true
        return std::any_of(_isVarLenField.begin(), _isVarLenField.end(), [](bool b) { return b; });
    }

    size_t Deserializer::inferLength(const void *ptr) const {
        // it is sure that there 8 x _isVarLenField.size() bytes readable from ptr
        int fixedLenFieldsLength = sizeof(int64_t) * numSerializedFields();

        // compute bitmap size
        auto bitmapSize = calcBitmapSize(_requiresBitmap);
        fixedLenFieldsLength += bitmapSize;

        // check two versions:
        // 1st consistency?
        size_t altSize = 0;

        // is there a varfield? then fetch length!
        size_t varLenFieldsLength = hasSchemaVarLenFields() ? *((int64_t *) ((uint8_t *) ptr + fixedLenFieldsLength))
                                                            : 0;
        for (int i = 0; i < _isVarLenField.size(); ++i) {
            if (_isVarLenField[i]) {

                // NULL, empty dict, empty tuple should not be varlen fields...
                auto el_type = _flattenedRowType.parameters()[i];
                assert(el_type != python::Type::NULLVALUE &&
                el_type != python::Type::EMPTYDICT &&
                el_type != python::Type::EMPTYTUPLE &&
                el_type != python::Type::EMPTYLIST);

                auto phys_col = logicalToPhysicalIndex(i);
                // extract!
                int64_t offset = *((int64_t *) ((uint8_t *) ptr + sizeof(int64_t) * phys_col + bitmapSize));
                int64_t size = ((offset & (0xFFFFFFFFul << 32ul)) >> 32);
                altSize += size;
            }
        }

        // this seems to fails weirdly
#ifndef NDEBUG
        if (altSize != varLenFieldsLength) {
            std::stringstream ss;
            ss << "altSize != varLenFieldsLength:\naltSize: " << altSize << "  varLenFieldsLength: "
               << varLenFieldsLength
               << "\nschema: " << this->_schema.getRowType().desc();
            ss << "\nhexdump:\n";
            core::hexdump(ss, ptr, 256);
            ss << "\nasciidump:\n";
            core::asciidump(ss, ptr, 256);
            Logger::instance().defaultLogger().error(ss.str());
        }
#endif

        assert(altSize == varLenFieldsLength);

        // is any varlenfield contained?
        if (hasSchemaVarLenFields()) {
            // decode var len size
            return fixedLenFieldsLength + sizeof(int64_t) + varLenFieldsLength;
        } else {
            return fixedLenFieldsLength;
        }
    }

    Deserializer &Deserializer::deserialize(const void *ptr, const int capacityLeft) {

        assert(numFields() > 0);
        assert(capacityLeft >= sizeof(int64_t) * numSerializedFields());

        if (hasSchemaVarLenFields())
            assert(capacityLeft >= sizeof(int64_t) * numSerializedFields() + sizeof(int64_t));

        // get length
        auto size = inferLength(ptr);

        // make sure enough bytes are there
        assert(size <= capacityLeft);

        // copy buffer
        if (_buffer)
            free(_buffer);

        _buffer = malloc(size);
        std::memcpy(_buffer, ptr, size);

        return *this;
    }

    int64_t Deserializer::getInt(const int col) const {
        assert(_buffer);

        // assert col reflects type
        auto t = _flattenedRowType.parameters()[col]; if(t.isOptionType())t = t.getReturnType();
        assert(t == python::Type::I64);

        // fixed len type
        auto phys_col = logicalToPhysicalIndex(col);

        assert(phys_col >= 0);
        assert(phys_col < inferLength(_buffer) / sizeof(int64_t));
        return *((int64_t *) ((uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap)));
    }

    bool Deserializer::getBool(const int col) const {
        assert(_buffer);

        // assert col reflects type
        auto t = _flattenedRowType.parameters()[col]; if(t.isOptionType())t = t.getReturnType();
        assert(t == python::Type::BOOLEAN);

        // fixed len type
        auto phys_col = logicalToPhysicalIndex(col);
        assert(phys_col >= 0);
        assert(phys_col < inferLength(_buffer) / sizeof(int64_t));
        return *((int64_t *) ((uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap))) > 0;
    }

    double Deserializer::getDouble(const int col) const {
        assert(_buffer);

        // assert col reflects type
        auto t = _flattenedRowType.parameters()[col]; if(t.isOptionType())t = t.getReturnType();
        assert(t == python::Type::F64);

        // fixed len type
        auto phys_col = logicalToPhysicalIndex(col);
        assert(phys_col >= 0);
        assert(phys_col < inferLength(_buffer) / sizeof(int64_t));
        return *((double *) ((uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap)));
    }

    const uint8_t * Deserializer::getPtr(const int col) const {
        assert(_buffer);

        // get offset
        auto phys_col = logicalToPhysicalIndex(col);
        assert(phys_col >= 0);
        assert(phys_col < (inferLength(_buffer) - sizeof(int64_t)) / sizeof(int64_t)); // sharper bound because of varlen
        uint64_t offset = *((uint64_t *) ((uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap)));

        // new:
        // offset is in the lower 32bit, the upper are the size of the var entry
        int64_t len = ((offset & 0xFFFFFFFFul << 32) >> 32);

        assert(len >= 0);
        offset = offset & 0xFFFFFFFFul;

        // secure strlen estimate, to be sure it doesn't crash
        uint8_t *ptr = (uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap) + offset;

        return ptr;
    }

    size_t Deserializer::getSize(const int col) const {
        assert(_buffer);

        // get offset
        auto phys_col = logicalToPhysicalIndex(col);
        assert(phys_col >= 0);
        assert(phys_col < (inferLength(_buffer) - sizeof(int64_t)) / sizeof(int64_t)); // sharper bound because of varlen
        uint64_t offset = *((uint64_t *) ((uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap)));

        // new:
        // offset is in the lower 32bit, the upper are the size of the var entry
        int64_t len = ((offset & 0xFFFFFFFFul << 32) >> 32);

        assert(len >= 0);
        return len;
    }

    std::string Deserializer::getString(const int col) const {
        assert(_buffer);

        // get offset
        auto phys_col = logicalToPhysicalIndex(col);
        assert(phys_col >= 0);
        assert(phys_col < (inferLength(_buffer) - sizeof(int64_t)) / sizeof(int64_t)); // sharper bound because of varlen
        uint64_t offset = *((uint64_t *) ((uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap)));

        // new:
        // offset is in the lower 32bit, the upper are the size of the var entry
        int64_t len = ((offset & 0xFFFFFFFFul << 32) >> 32) - 1; // -1 because it actually is the size

        assert(len >= 0);
        offset = offset & 0xFFFFFFFFul;

        // secure strlen estimate, to be sure it doesn't crash
        uint8_t *ptr = (uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap) + offset;

        char *cstr = (char *) ptr;
        int i = sizeof(int64_t) * col + offset;

        // check that string is properly terminated with '\0'
        if (cstr[len] != '\0') {
            Logger::instance().logger("memory").error("corrupted memory found. Could not extract varlen string");

#ifndef NDEBUG
            core::hexdump(std::cout, _buffer, 64);
            std::cout<<std::endl;
            core::asciidump(std::cout, _buffer, 64);
            std::cout<<std::endl;
#endif

            return std::string("NULL");
        } else {
            return std::string((const char *) ptr);
        }
    }

    std::string Deserializer::getDictionary(const int col) const {
        assert(_buffer);

        // get offset
        auto phys_col = logicalToPhysicalIndex(col);
        assert(phys_col >= 0);
        assert(phys_col < (inferLength(_buffer) - sizeof(int64_t)) / sizeof(int64_t)); // sharper bound because of varlen
        int64_t offset = *((int64_t *) ((uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap)));

        // new:
        // offset is in the lower 32bit, the upper are the size of the var entry
        int64_t len = ((offset & (0xFFFFFFFFl << 32)) >> 32) - 1;

        assert(len > 0);
        offset = offset & 0xFFFFFFFF;

        // secure strlen estimate, to be sure it doesn't crash
        uint8_t *ptr = (uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap) + offset;

        char *dstr = (char *) ptr;
        int i = sizeof(int64_t) * col + offset;

        // check that string is properly terminated with '\0'
        if (dstr[len] != '\0') {
            Logger::instance().logger("memory").error("corrupted memory found. could not extract cJSON object");
            return std::string("NULL");
        } else {
            return std::string((char *) dstr);
        }
    }

    List Deserializer::getList(const int col) const {
        assert(_buffer);

        // check type
        auto list_type = _flattenedRowType.parameters()[col];
        if(list_type.isOptionType())
            list_type = list_type.getReturnType(); // get return type to account for options
        assert(list_type.isListType() && list_type != python::Type::EMPTYLIST);
        auto elType = list_type.elementType();

        // get physical column
        auto phys_col = logicalToPhysicalIndex(col);
        assert(phys_col >= 0);

        std::vector<Field> els;
        if(elType.isSingleValued()) {
            assert(phys_col < inferLength(_buffer) / sizeof(int64_t));
            auto num_elements = *((int64_t *) ((uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap)));
            for(int i=0; i<num_elements; i++) {
                if(elType == python::Type::NULLVALUE) els.push_back(Field::null());
                else if(elType == python::Type::EMPTYDICT) els.push_back(Field::empty_dict());
                else if(elType == python::Type::EMPTYTUPLE) els.push_back(Field::empty_tuple());
                else if(elType == python::Type::EMPTYLIST) els.push_back(Field::empty_list());
                else throw std::runtime_error("Unsupported list element type deserialized: " + elType.desc());
            }
            return List::from_vector(els);
        }

        assert(phys_col < (inferLength(_buffer) - sizeof(int64_t)) / sizeof(int64_t)); // sharper bound because of varlen
        // get offset: offset is in the lower 32bit, the upper are the size of the var entry
        int64_t offset = *((int64_t *) ((uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap)));
        int64_t len = ((offset & (0xFFFFFFFFl << 32)) >> 32) - 1;
        assert(len > 0);
        offset = offset & 0xFFFFFFFF;

        // pointer to varlen field
        uint8_t *ptr = (uint8_t *) _buffer + sizeof(int64_t) * phys_col + calcBitmapSize(_requiresBitmap) + offset;

        return getListHelper(list_type, ptr);
    }

    Tuple Deserializer::getTupleHelper(const python::Type &tupleType, const uint8_t *ptr) const {
        auto tree = TupleTree<Field>(tupleType);
        Field f;
        std::vector<bool> bitmapV;
        size_t bitmapIndex = 0;
        size_t numOptionalFields = python::numOptionalFields(tupleType);
        if(numOptionalFields) {
            auto numBitmapFields = core::ceilToMultiple(numOptionalFields, 64ul)/64;
            auto bitmapSize = numBitmapFields * sizeof(uint64_t);
            auto *bitmapAddr = (uint64_t *)ptr;
            ptr += bitmapSize;
            for (size_t i = 0; i < numOptionalFields; i++) {
                bool currBit = (bitmapAddr[i/64] >> (i % 64)) & 1;
                bitmapV.push_back(currBit);
            }
        }

        auto flattenedTupleType = flattenedType(tupleType);
        for (size_t i = 0; i < tree.numElements(); i++) {
            auto currFieldType = flattenedTupleType.parameters()[i];
            if(currFieldType == python::Type::BOOLEAN) {
                f = Field((bool)(*(uint64_t *)ptr));
                ptr += sizeof(uint64_t);
            } else if(currFieldType == python::Type::I64) {
                f = Field(*(int64_t *)ptr);
                ptr += sizeof(uint64_t);
            } else if(currFieldType == python::Type::F64) {
                f = Field(*(double *)ptr);
                ptr += sizeof(uint64_t);
            } else if(currFieldType == python::Type::STRING) {
                uint64_t sizeOffset = *(uint64_t *)ptr; // offset field for string is (size | offset)
                uint64_t strOffset = sizeOffset & 0xFFFFFFFF;
                f = Field((const char *)(ptr + strOffset));
                ptr += sizeof(uint64_t);
            } else if(currFieldType.isListType()) {
                auto listOffset = *(int64_t *)ptr;
                f = Field(getListHelper(currFieldType, ptr + listOffset));
                ptr += sizeof(uint64_t);
            } else if(currFieldType == python::Type::NULLVALUE) {
                f = Field::null();
            } else if(currFieldType == python::Type::EMPTYTUPLE) {
                f = Field::empty_tuple();
            } else if(currFieldType == python::Type::EMPTYLIST) {
                f = Field::empty_list();
            } else if(currFieldType == python::Type::EMPTYDICT) {
                f = Field::empty_dict();
            } else if(currFieldType.isOptionType()) {
                // need to check bitmapV
                auto underlyingType = currFieldType.getReturnType();
                if(underlyingType == python::Type::BOOLEAN) {
                    if(bitmapV[bitmapIndex]) {
                        // is None
                        f = Field(option<bool>::none);
                    } else {
                        f = Field(option<bool>((bool)(*(uint64_t *)ptr)));
                        ptr += sizeof(uint64_t);
                    }
                } else if(underlyingType == python::Type::I64) {
                    if(bitmapV[bitmapIndex]) {
                        // is None
                        f = Field(option<int64_t>::none);
                    } else {
                        f = Field(option<int64_t>(*(int64_t *)ptr));
                        ptr += sizeof(uint64_t);
                    }
                } else if(underlyingType == python::Type::F64) {
                    if(bitmapV[bitmapIndex]) {
                        // is None
                        f = Field(option<double>::none);
                    } else {
                        f = Field(option<double>(*(double *)ptr));
                        ptr += sizeof(uint64_t);
                    }
                } else if(underlyingType == python::Type::STRING) {
                    if(bitmapV[bitmapIndex]) {
                        // is None
                        f = Field(option<std::string>::none);
                    } else {
                        uint64_t sizeOffset = *(uint64_t *)ptr; // offset field for string is (size | offset)
                        uint64_t strOffset = sizeOffset & 0xFFFFFFFF;
                        f = Field(option<std::string>((const char *)(ptr + strOffset)));
                        ptr += sizeof(uint64_t);
                    }
                } else if(underlyingType.isListType()) {
                    if(bitmapV[bitmapIndex]) {
                        // is None
                        f = Field::null(currFieldType);
                    } else {
                        auto listOffset = *(int64_t *)ptr;
                        f = Field(option<List>(getListHelper(underlyingType, ptr + listOffset)));
                        ptr += sizeof(uint64_t);
                    }
                } else if(underlyingType.isTupleType()) {
                    if(bitmapV[bitmapIndex]) {
                        // is None
                        f = Field::null(currFieldType);
                    } else {
                        auto tupleOffset = *(int64_t *)ptr;
                        f = Field(option<Tuple>(getTupleHelper(underlyingType, ptr + tupleOffset)));
                        ptr += sizeof(uint64_t);
                    }
                } else {
                    Logger::instance().logger("deserializer").error(
                            "invalid element type in tuple: " + currFieldType.desc() + " encountered, can't deserialize.");
                }
                // increment bitmapIndex for optional fields only
                bitmapIndex++;
            } else {
                Logger::instance().logger("deserializer").error(
                        "invalid element type in tuple: " + currFieldType.desc() + " encountered, can't deserialize.");
            }
            tree.set(i, f);
        }
        return flattenToTuple(tree);
    }

    Tuple Deserializer::getOptionTuple(const int col) const {
        assert(_buffer);

        // check type
        auto tupleType = _flattenedRowType.parameters()[col].getReturnType();
        assert(tupleType.isTupleType() && tupleType != python::Type::EMPTYTUPLE);

        // get physical column
        auto phys_col = logicalToPhysicalIndex(col);
        assert(phys_col >= 0);
        assert(phys_col < (inferLength(_buffer) - sizeof(uint64_t)) / sizeof(uint64_t));

        uint64_t offset = *((uint64_t *) ((uint8_t *) _buffer + sizeof(uint64_t) * phys_col + calcBitmapSize(_requiresBitmap)));
        uint64_t len = ((offset & (0xFFFFFFFFl << 32)) >> 32) - 1;
        assert(len > 0);
        offset = offset & 0xFFFFFFFF;

        // get ptr to tuple data
        uint8_t *ptr = (uint8_t *) _buffer + sizeof(uint64_t) * phys_col + calcBitmapSize(_requiresBitmap) + offset;

        return getTupleHelper(tupleType, ptr);
    }

    List Deserializer::getListHelper(const python::Type &listType, const uint8_t *ptr) const {
        auto elType = listType.elementType();
        std::vector<Field> els;
        // get number of elements
        uint64_t numElements = *(uint64_t *)ptr;
        ptr += sizeof(uint64_t);

        if(listType.isSingleValued()) {
            // does not need memory data
            // fill els with numElements of the single value
            if(elType == python::Type::NULLVALUE) {
                els.insert(els.end(), numElements, Field::null());
            } else if(elType == python::Type::EMPTYTUPLE) {
                els.insert(els.end(), numElements, Field::empty_tuple());
            } else if(elType == python::Type::EMPTYLIST) {
                els.insert(els.end(), numElements, Field::empty_list());
            } else if(elType == python::Type::EMPTYDICT) {
                els.insert(els.end(), numElements, Field::empty_dict());
            } else {
                throw std::runtime_error("Unsupported list element type deserialized: " + elType.desc());
            }
            return List::from_vector(els);
        }

        std::vector<bool> bitmapV;
        if (elType.isOptionType()) {
            auto numBitmapFields = core::ceilToMultiple((unsigned long)numElements, 64ul)/64;
            auto bitmapSize = numBitmapFields * sizeof(uint64_t);
            auto *bitmapAddr = (uint64_t *)ptr;
            ptr += bitmapSize;
            for (size_t i = 0; i < numElements; i++) {
                bool currBit = (bitmapAddr[i/64] >> (i % 64)) & 1;
                bitmapV.push_back(currBit);
            }
        }

        if(elType == python::Type::I64) {
            for (size_t i = 0; i < numElements; i++) {
                els.emplace_back(Field(*(int64_t *)ptr));
                ptr += sizeof(uint64_t);
            }
        } else if(elType == python::Type::F64) {
            for (size_t i = 0; i < numElements; i++) {
                els.emplace_back(Field(*(double *)ptr));
                ptr += sizeof(uint64_t);
            }
        } else if(elType == python::Type::BOOLEAN) {
            for (size_t i = 0; i < numElements; i++) {
                els.emplace_back(Field(bool(*(uint64_t *)ptr)));
                ptr += sizeof(uint64_t);
            }
        } else if(elType == python::Type::STRING) {
            // read each string
            for (size_t i = 0; i < numElements; i++) {
                uint64_t strOffset = *(uint64_t *)ptr;
                els.emplace_back(Field((const char *)(ptr + strOffset)));
                ptr += sizeof(uint64_t);
            }
        } else if(elType.isTupleType()) {
            // read each tuple
            for (size_t i = 0; i < numElements; i++) {
                auto tupleOffset = *(uint64_t *) ptr;
                els.emplace_back(Field(getTupleHelper(elType, ptr + tupleOffset)));
                ptr += sizeof(uint64_t);
            }
        } else if(elType.isListType()) {
            // read each list
            for (size_t i = 0; i < numElements; i++) {
                auto listOffset = *(uint64_t *) ptr;
                els.emplace_back(Field(getListHelper(elType, ptr + listOffset)));
                ptr += sizeof(uint64_t);
            }
        } else if(elType.isOptionType()) {
            auto underlyingElType = elType.getReturnType();
            if(underlyingElType == python::Type::STRING) {
                // check for none then read each string
                for (size_t i = 0; i < numElements; i++) {
                    if(bitmapV[i]) {
                        // is None
                        els.emplace_back(Field(option<std::string>::none));
                    } else {
                        uint64_t strOffset = *(uint64_t *)ptr;
                        els.emplace_back(Field(option<std::string>((const char *)(ptr + strOffset))));
                        ptr += sizeof(uint64_t);
                    }
                }
            } else if(underlyingElType.isTupleType()) {
                // check for none then read each tuple
                for (size_t i = 0; i < numElements; i++) {
                    if(bitmapV[i]) {
                        // is None
                        els.emplace_back(Field::null(elType));
                    } else {
                        auto tupleOffset = *(uint64_t *) ptr;
                        els.emplace_back(Field(option<Tuple>(
                                getTupleHelper(underlyingElType, ptr + tupleOffset))));
                        ptr += sizeof(uint64_t);
                    }
                }
            } else if(underlyingElType.isListType()) {
                // check for none then read each list
                for (size_t i = 0; i < numElements; i++) {
                    if(bitmapV[i]) {
                        // is None
                        els.emplace_back(Field::null(elType));
                    } else {
                        auto listOffset = *(uint64_t *) ptr;
                        els.emplace_back(Field(option<List>(getListHelper(underlyingElType, ptr + listOffset))));
                        ptr += sizeof(uint64_t);
                    }
                }
            } else if(underlyingElType == python::Type::BOOLEAN) {
                // check for none then read each value
                for (size_t i = 0; i < numElements; i++) {
                    if(bitmapV[i]) {
                        // is None
                        els.emplace_back(Field(option<bool>::none));
                    } else {
                        els.emplace_back(Field(option<bool>((bool)(*(uint64_t *)ptr))));
                        ptr += sizeof(uint64_t);
                    }
                }
            } else if(underlyingElType == python::Type::I64) {
                // check for none then read each value
                for (size_t i = 0; i < numElements; i++) {
                    if(bitmapV[i]) {
                        // is None
                        els.emplace_back(Field(option<int64_t>::none));
                    } else {
                        els.emplace_back(Field(option<int64_t>(*(int64_t *)ptr)));
                        ptr += sizeof(uint64_t);
                    }
                }
            } else if(underlyingElType == python::Type::F64) {
                // check for none then read each value
                for (size_t i = 0; i < numElements; i++) {
                    if(bitmapV[i]) {
                        // is None
                        els.emplace_back(Field(option<double>::none));
                    } else {
                        els.emplace_back(Field(option<double>((double)(*(double *)ptr))));
                        ptr += sizeof(uint64_t);
                    }
                }
            } else {
                Logger::instance().logger("deserializer").error(
                        "invalid list type: " + listType.desc() + " encountered, can't deserialize.");
            }
        } else {
            Logger::instance().logger("deserializer").error(
                    "invalid list type: " + listType.desc() + " encountered, can't deserialize.");
        }
        return List::from_vector(els);
    }

    Tuple Deserializer::getTuple() const {

        // create tree structure to fill with field values
        // and then flatten tree to tuple
        auto tree = TupleTree<Field>(_schema.getRowType());

        // fill in elementsField f;
        Field f;
        for (int i = 0; i < tree.numElements(); ++i) {
            auto type = _flattenedRowType.parameters()[i];
            assert(tree.fieldType(i) == type);

            if (python::Type::BOOLEAN == type)
                f = Field(getBool(i));
            else if (python::Type::I64 == type)
                f = Field(getInt(i));
            else if (python::Type::F64 == type)
                f = Field(getDouble(i));
            else if (python::Type::STRING == type)
                f = Field(getString(i));
            else if (python::Type::EMPTYTUPLE == type)
                f = Field(Tuple());
            else if (python::Type::EMPTYDICT == type)
                f = Field::from_str_data("{}", python::Type::EMPTYDICT);
            else if (python::Type::NULLVALUE == type)
                f = Field::null();
            else if (python::Type::GENERICDICT == type)
                f = Field::from_str_data(getDictionary(i), python::Type::GENERICDICT);
            else if (type.isDictionaryType())
                f = Field::from_str_data(getDictionary(i), type);
            else if (type == python::Type::EMPTYLIST)
                f = Field(List());
            else if (type.isListType())
                f = Field(getList(i));
            else if(type == python::Type::PYOBJECT)
                f = Field::from_pickled_memory(getPtr(i), getSize(i));
            else if (type.isOptionType()) {
                // deserialize underlying type if option

                auto rt = type.getReturnType();

                if (python::Type::BOOLEAN == rt)
                    f = Field(isNull(i) ? option<bool>::none : option<bool>(getBool(i)));
                else if (python::Type::I64 == rt)
                    f = Field(isNull(i) ? option<int64_t>::none : option<int64_t>(getInt(i)));
                else if (python::Type::F64 == rt)
                    f = Field(isNull(i) ? option<double>::none : option<double>(getDouble(i)));
                else if (python::Type::STRING == rt)
                    f = Field(isNull(i) ? option<std::string>::none : option<std::string>(getString(i)));
                else if (python::Type::EMPTYTUPLE == rt)
                    f = Field(isNull(i) ? option<Tuple>::none : option<Tuple>(Tuple()));
                else if (python::Type::EMPTYDICT == rt)
                    f = Field::from_str_data(isNull(i) ? option<std::string>::none : option<std::string>("{}"), python::Type::EMPTYDICT);
                else if (python::Type::GENERICDICT == rt)
                    f = Field::from_str_data(
                            isNull(i) ? option<std::string>::none : option<std::string>(getDictionary(i)),
                            python::Type::GENERICDICT);
                else if (rt.isDictionaryType())
                    f = Field::from_str_data(
                            isNull(i) ? option<std::string>::none : option<std::string>(getDictionary(i)),
                            rt);
                else if(rt == python::Type::EMPTYLIST)
                    f = Field(isNull(i) ? option<List>::none : option<List>(List()));
                else if(rt.isListType())
                    f = isNull(i) ? Field::null(type) : Field(option<List>(getList(i)));
                else if(rt.isTupleType()) {
                    f = isNull(i) ? Field::null(type) : Field(option<Tuple>(getOptionTuple(i)));
                } else {
                    f = Field::null(); // default to NULL
                    Logger::instance().defaultLogger().error(
                            "unknown type '" + type.desc() + "' occurred when trying to attempt deserialization of field");
                }
            } else {
                f = Field::null(); // default to NULL
                Logger::instance().defaultLogger().error(
                        "unknown type '" + type.desc() + "' occurred when trying to attempt deserialization of field");
            }


            tree.set(i, f);
        }
        Tuple tuple = flattenToTuple(tree);
        return tuple;
    }


}