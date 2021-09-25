//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TIMESTAMP_H
#define TUPLEX_TIMESTAMP_H

namespace tuplex { namespace orc {

/*!
 * Implementation of OrcBatch for orc Timestamp types.
 */
class TimestampBatch : public OrcBatch {
public:

    TimestampBatch(::orc::ColumnVectorBatch *orcBatch, uint64_t numRows, bool isOption) : _orcBatch(
            static_cast<::orc::TimestampVectorBatch *>(orcBatch)) {
        _orcBatch->numElements = numRows;
        _orcBatch->hasNulls = isOption;
    }

    void setData(int64_t value, uint64_t row) {
        _orcBatch->data[row] = value;
        auto nanos = value * 1000000000;
        if (nanos != 0 && nanos / value != 1000000000) {
            nanos = 2147483647;
        }
        _orcBatch->nanoseconds[row] = nanos;
    }

    void setData(tuplex::Deserializer &ds, uint64_t col, uint64_t row) override {
        if (row == _orcBatch->capacity) {
            _orcBatch->resize(_orcBatch->capacity * 2);
        }
        auto notNull = !ds.isNull(col);
        _orcBatch->notNull[row] = notNull;
        if (notNull) {
            auto value = ds.getInt(col);
            setData(value, row);
        }
    }

    void setData(tuplex::Field field, uint64_t row) override {
        if (row == _orcBatch->capacity) {
            _orcBatch->resize(_orcBatch->capacity * 2);
        }
        auto notNull = !field.isNull();
        _orcBatch->notNull[row] = notNull;
        if (notNull) {
            auto value = field.getInt();
            setData(value, row);
        }
    }

    void setBatch(::orc::ColumnVectorBatch *newBatch) override {
        _orcBatch = static_cast<::orc::TimestampVectorBatch *>(newBatch);
    }

    tuplex::Field getField(uint64_t row) override {
        using namespace tuplex;
        if (_orcBatch->hasNulls) {
            if (_orcBatch->notNull[row]) {
                return Field(option<int64_t>(_orcBatch->data[row]));
            } else {
                return Field(option<int64_t>::none);
            }
        } else {
            return Field(_orcBatch->data[row]);
        }
    }

private:
    ::orc::TimestampVectorBatch *_orcBatch;
};

}}

#endif //TUPLEX_TIMESTAMP_H
