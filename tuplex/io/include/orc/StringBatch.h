//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_STRINGBATCH_H
#define TUPLEX_STRINGBATCH_H

#ifdef BUILD_WITH_ORC

namespace tuplex { namespace orc {

/*!
 * Implementation of OrcBatch for tuplex STRING types.
 */
class StringBatch : public OrcBatch {
public:

    StringBatch() = delete;

    StringBatch(::orc::ColumnVectorBatch *orcBatch, uint64_t numRows, bool isOption) : _orcBatch(
            static_cast<::orc::StringVectorBatch *>(orcBatch)), _buffers({}) {
        _orcBatch->numElements = numRows;
        _orcBatch->hasNulls = isOption;
        _orcBatch->resize(numRows);
    }

    ~StringBatch() override {
        for (auto el : _buffers) {
            delete[] el;
        }
    }

    void setData(std::string value, uint64_t row) {
        char *buf = new char[value.size() + 1];
        value.copy(buf, value.size());
        _buffers.push_back(buf);
        _orcBatch->data[row] = buf;
        _orcBatch->length[row] = value.size();
    }

    void setData(tuplex::Field field, uint64_t row) override {
        if (row == _orcBatch->capacity) {
            _orcBatch->resize(_orcBatch->capacity * scaleFactor());
        }
        auto notNull = !field.isNull();
        _orcBatch->notNull[row] = notNull;
        if (notNull) {
            auto value = std::string(reinterpret_cast<char *>(field.getPtr()));
            setData(value, row);
        }
    }

    void setData(tuplex::Deserializer &ds, uint64_t col, uint64_t row) override {
        if (row == _orcBatch->capacity) {
            _orcBatch->resize(_orcBatch->capacity * scaleFactor());
        }
        auto notNull = !ds.isNull(col);
        _orcBatch->notNull[row] = notNull;
        if (notNull) {
            auto value = ds.getString(col);
            setData(value, row);
        }
    }

    void setBatch(::orc::ColumnVectorBatch *newBatch) override {
        _orcBatch = static_cast<::orc::StringVectorBatch *>(newBatch);
    }

    void getField(Serializer &serializer, uint64_t row) override {
        using namespace tuplex;
        if (_orcBatch->hasNulls) {
            if (_orcBatch->notNull[row]) {
                std::string str(_orcBatch->data[row], _orcBatch->data[row] + _orcBatch->length[row]);
                serializer.append(option<std::string>(str));
            } else {
                serializer.append(option<std::string>::none);
            }
        } else {
            std::string str(_orcBatch->data[row], _orcBatch->data[row] + _orcBatch->length[row]);
            serializer.append(str);
        }
    }

    tuplex::Field getField(uint64_t row) override {
        using namespace tuplex;
        if (_orcBatch->hasNulls) {
            if (_orcBatch->notNull[row]) {
                std::string str(_orcBatch->data[row], _orcBatch->data[row] + _orcBatch->length[row]);
                return Field(option<std::string>(str));
            } else {
                return Field(option<std::string>::none);
            }
        } else {
            std::string str(_orcBatch->data[row], _orcBatch->data[row] + _orcBatch->length[row]);
            return Field(str);
        }
    }

private:
    ::orc::StringVectorBatch *_orcBatch;
    std::vector<char *> _buffers;
};

}}

#endif

#endif //TUPLEX_STRINGBATCH_H
