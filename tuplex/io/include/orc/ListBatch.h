//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_LISTBATCH_H
#define TUPLEX_LISTBATCH_H

namespace tuplex { namespace orc {

        class ListBatch : public OrcBatch {
        public:
            ListBatch(::orc::ColumnVectorBatch *orcBatch, OrcBatch *child, size_t numRows, bool isOption) : _orcBatch(
                    static_cast<::orc::ListVectorBatch *>(orcBatch)), _nextIndex(0), _child(child) {
                _orcBatch->numElements = numRows;
                _orcBatch->hasNulls = isOption;
                _orcBatch->resize(numRows);
                _orcBatch->offsets[0] = 0;
            }

            ~ListBatch() override {
                delete _child;
            }

            void setData(tuplex::Field field, uint64_t row) override {
                auto notNull = !field.isNull();
                _orcBatch->notNull[row] = notNull;
                _orcBatch->offsets[row + 1] = _orcBatch->offsets[row];
                if (notNull) {
                    auto list = (tuplex::List *) field.getPtr();
                    auto numElements = list->numElements();
                    _orcBatch->offsets[row + 1] += numElements;

                    for (uint64_t i = 0; i < numElements; ++i) {
                        _child->setData(list->getField(i), _nextIndex);
                        _nextIndex++;
                    }
                }
            }

        private:
            ::orc::ListVectorBatch *_orcBatch;
            uint64_t _nextIndex;
            OrcBatch *_child;
        };

    }}

#endif //TUPLEX_LISTBATCH_H
