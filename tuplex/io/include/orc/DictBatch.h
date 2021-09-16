//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_DICTBATCH_H
#define TUPLEX_DICTBATCH_H

namespace tuplex { namespace orc {

/*!
 * Implementation of OrcBatch for tuplex Dictionary types.
 */
class DictBatch : public OrcBatch {
public:
    DictBatch(::orc::ColumnVectorBatch *orcBatch, OrcBatch *keyBatch, OrcBatch *valueBatch, python::Type keyType,
              python::Type valueType, size_t numRows, bool isOption) : _orcBatch(
            static_cast<::orc::MapVectorBatch *>(orcBatch)), _keyBatch(keyBatch), _valueBatch(valueBatch),
                                                                       _keyType(keyType), _valueType(valueType),
                                                                       _nextIndex(0) {
        _orcBatch->numElements = numRows;
        _orcBatch->hasNulls = isOption;
        _orcBatch->resize(numRows);
        _orcBatch->offsets[0] = 0;
    }

    ~DictBatch() override {
        delete _keyBatch;
        delete _valueBatch;
    }

    void setData(tuplex::Field field, uint64_t row) override {
        auto notNull = !field.isNull();

        assert(row <= _orcBatch->capacity);
        _orcBatch->notNull[row] = notNull;
        _orcBatch->offsets[row + 1] = _orcBatch->offsets[row];

        if (notNull) {
            assert(field.getPtr());
            auto dict = cJSON_Parse(reinterpret_cast<char *>(field.getPtr()));

            auto cur = dict->child;
            while (cur) {
                auto key = keyToField(cur, _keyType);
                _keyBatch->setData(keyToField(cur, _keyType), _nextIndex);
                auto val = valueToField(cur, _valueType);
                _valueBatch->setData(valueToField(cur, _valueType), _nextIndex);
                _nextIndex++;
                cur = cur->next;
                _orcBatch->offsets[row + 1]++;
            }
        }
    }

    void setBatch(::orc::ColumnVectorBatch *newBatch) override {
        auto mapBatch = static_cast<::orc::MapVectorBatch *>(newBatch);
        _orcBatch = mapBatch;
        _keyBatch->setBatch(mapBatch->keys.get());
        _valueBatch->setBatch(mapBatch->elements.get());
    }

    tuplex::Field getField(uint64_t row) override {
        auto numElements = _orcBatch->offsets[row + 1] - _orcBatch->offsets[row];
        std::stringstream ss;
        ss << "{";
        for (int i = 0; i < numElements; i++) {
            if (_keyType != python::Type::STRING) {
                ss << "\"" << fieldToStr(_keyType, _keyBatch->getField(_nextIndex)) << "\"";
            } else {
                ss << fieldToStr(_keyType, _keyBatch->getField(_nextIndex));
            }
            ss << ":";
            ss << fieldToStr(_valueType, _valueBatch->getField(_nextIndex));
            if (i != numElements - 1) {
                ss << ",";
            }
            _nextIndex++;
        }
        ss << "}";
        return tuplex::Field::from_str_data(ss.str(), python::Type::makeDictionaryType(_keyType, _valueType));
    }

private:
    ::orc::MapVectorBatch *_orcBatch;
    OrcBatch *_keyBatch;
    OrcBatch *_valueBatch;
    python::Type _keyType;
    python::Type _valueType;
    uint64_t _nextIndex;

    std::string fieldToStr(python::Type rowType, tuplex::Field field) {
        if (rowType == python::Type::I64) {
            return std::to_string(field.getInt());
        } else if (rowType == python::Type::F64) {
            return std::to_string(field.getDouble());
        } else if (rowType == python::Type::STRING) {
            return "\"" + std::string(reinterpret_cast<char*>(field.getPtr())) + "\"";
        } else if (rowType == python::Type::BOOLEAN) {
            return field.getInt() ? "true" : "false";
        }
        return "";
    }

    tuplex::Field keyToField(cJSON *entry, const python::Type &type) {
        using namespace tuplex;
        std::string str(entry->string);
        if (type == python::Type::I64) {
            return Field((int64_t) std::stoi(str));
        } else if (type == python::Type::F64) {
            return Field((double) std::stod(str));
        } else if (type == python::Type::STRING) {
            return Field(str);
        } else {
            if (str == "true") {
                return Field(true);
            } else {
                return Field(false);
            }
        }
    }

    tuplex::Field valueToField(cJSON *entry, const python::Type &type) {
        using namespace tuplex;
        if (type == python::Type::I64) {
            return Field((int64_t) entry->valueint);
        } else if (type == python::Type::F64) {
            return Field(entry->valuedouble);
        } else if (type == python::Type::BOOLEAN) {
            return Field((bool) entry->valueint);
        } else {
            return Field(entry->valuestring);
        }
    }
};

}}

#endif //TUPLEX_DICTBATCH_H
