//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TUPLEBATCH_H
#define TUPLEX_TUPLEBATCH_H

namespace tuplex { namespace orc {

class TupleBatch : public OrcBatch {
public:

    TupleBatch() = delete;

    TupleBatch(::orc::ColumnVectorBatch *orcBatch, std::vector<OrcBatch *> children, size_t numRows, bool isOption): _orcBatch(static_cast<::orc::StructVectorBatch *>(orcBatch)), _children(children) {
        _orcBatch->numElements = numRows;
        _orcBatch->hasNulls = isOption;
        _orcBatch->resize(numRows);
    }

    ~TupleBatch() override {
        for (auto el : _children) {
            delete el;
        }
    }

    void setData(tuplex::Field field, uint64_t row) override {
        using namespace tuplex;
        if (row == _orcBatch->capacity) {
            _orcBatch->resize(_orcBatch->capacity * scaleFactor());
        }
        auto notNull = !field.isNull();
        _orcBatch->notNull[row] = notNull;
        if (notNull) {
            Tuple *tuple = (Tuple *) field.getPtr();
            for (uint64_t i = 0; i < tuple->numElements(); ++i) {
                _children.at(i)->setData(tuple->getField(i), row);
            }
        }
    }

    void setData(tuplex::Deserializer &ds, uint64_t col, uint64_t row) override {
        using namespace tuplex;
        if (row == _orcBatch->capacity) {
            _orcBatch->resize(_orcBatch->capacity * scaleFactor());
        }
        auto notNull = !ds.isNull(col);
        _orcBatch->notNull[row] = notNull;
        if (notNull) {
            auto tuple = ds.getTuple();
            for (uint64_t i = 0; i < tuple.numElements(); ++i) {
                _children.at(i)->setData(tuple.getField(i), row);
            }
        }
    }

    void setBatch(::orc::ColumnVectorBatch *newBatch) override {
        auto structBatch = static_cast<::orc::StructVectorBatch *>(newBatch);
        _orcBatch = structBatch;
        for (int i = 0; i < _children.size(); ++i) {
            _children.at(i)->setBatch(structBatch->fields[i]);
        }
    }

    void getField(Serializer &serializer, uint64_t row) override {
        using namespace tuplex;
        std::vector <Field> elements;
        for (auto child : _children) {
            elements.push_back(child->getField(row));
        }
        serializer.append(Tuple::from_vector(elements));
    }

    tuplex::Field getField(uint64_t row) override {
        using namespace tuplex;
        std::vector <Field> elements;
        for (auto child : _children) {
            elements.push_back(child->getField(row));
        }
        return Field(Tuple::from_vector(elements));
    }

private:
    ::orc::StructVectorBatch *_orcBatch;
    std::vector<OrcBatch *> _children;
};

}}

#endif //TUPLEX_TUPLEBATCH_H
