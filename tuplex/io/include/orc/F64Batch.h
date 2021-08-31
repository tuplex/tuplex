//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_F64BATCH_H
#define TUPLEX_F64BATCH_H

namespace tuplex { namespace orc {

/*!
 * Implementation of OrcBatch for tuplex F64 types.
 */
        class F64Batch : public OrcBatch {
        public:

            F64Batch(::orc::ColumnVectorBatch *orcBatch, uint64_t numRows, bool isOption) : _orcBatch(
                    static_cast<::orc::DoubleVectorBatch *>(orcBatch)) {
                _orcBatch->numElements = numRows;
                _orcBatch->hasNulls = isOption;
            }

            void setData(tuplex::Field field, uint64_t row) override {
                if (row == _orcBatch->capacity) {
                    _orcBatch->resize(_orcBatch->capacity * 2);
                }
                auto notNull = !field.isNull();
                _orcBatch->notNull[row] = notNull;
                if (notNull) {
                    _orcBatch->data[row] = field.getDouble();
                }
            }

        private:
            ::orc::DoubleVectorBatch *_orcBatch;
        };

    }}

#endif //TUPLEX_F64BATCH_H
