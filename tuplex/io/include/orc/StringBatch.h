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

namespace tuplex { namespace orc {

/*!
 * Implementation of OrcBatch for tuplex STRING types.
 */
        class StringBatch : public OrcBatch {
        public:

            StringBatch(::orc::ColumnVectorBatch *orcBatch, uint64_t numRows, bool isOption) : _orcBatch(
                    static_cast<::orc::StringVectorBatch *>(orcBatch)), _buffers({}) {
                _orcBatch->numElements = numRows;
                _orcBatch->hasNulls = isOption;
            }

            ~StringBatch() override {
                for (auto el : _buffers) {
                    delete[] el;
                }
            }

            void setData(tuplex::Field field, uint64_t row) override {
                if (row == _orcBatch->capacity) {
                    _orcBatch->resize(_orcBatch->capacity * 2);
                }
                auto notNull = !field.isNull();
                _orcBatch->notNull[row] = notNull;
                if (notNull) {
                    auto str = std::string(reinterpret_cast<char *>(field.getPtr()));
                    char *buf = new char[str.size() + 1];
                    str.copy(buf, str.size());
                    _buffers.push_back(buf);
                    _orcBatch->data[row] = buf;
                    _orcBatch->length[row] = str.size();
                }
            }

        private:
            ::orc::StringVectorBatch *_orcBatch;
            std::vector<char *> _buffers;
        };

    }}

#endif //TUPLEX_STRINGBATCH_H
