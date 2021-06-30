//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "Row.h"

namespace tuplex {

    Row Row::fromMemory(const Schema &schema, const void *ptr, size_t size) {
        Deserializer ds(schema);
        return fromMemory(ds, ptr, size);
    }

    Row Row::fromMemory(tuplex::Deserializer &ds, const void *ptr, size_t size) {
        assert(ptr);
        // size may be 0 when only zeros are deserialized.

        Row row;
        row._schema = ds.getSchema();
        ds.deserialize(ptr, size);
        // deserialize as Tuple structure & then unpack for easier access
        Tuple t = ds.getTuple();
        for(int i = 0; i < t.numElements(); i++)
            row._values.push_back(t.getField(i));
        // needs to be called after values are set!
        row._serializedLength = ds.rowSize();
        assert(ds.inferLength(ptr) == row._serializedLength);
        return row;
    }

    Row Row::fromExceptionMemory(const tuplex::Schema &schema, tuplex::ExceptionCode ec, const void *ptr,
                                 size_t size) {
        Deserializer ds(schema);
        return fromExceptionMemory(ds, ec, ptr, size);
    }

    Row Row::fromExceptionMemory(tuplex::Deserializer &ds, tuplex::ExceptionCode ec, const void *ptr, size_t size) {
        switch(ec) {
            case ExceptionCode::BADPARSE_STRING_INPUT: {

                // get string length
                int64_t size = *((const int64_t*)ptr);
                auto str = (const char*)ptr + 8;
                assert(size >= 1);
                return Row(fromCharPointers(str, str + size - 1));
            }
            // @TODO: add here special code for null-value opt violation, i.e. efficient in-memory format is saved!

            default:
                return fromMemory(ds, ptr, size);
        }
    }

    bool Row::getBoolean(const int col) const {
        return get(col).getInt() > 0;
    }

    int64_t Row::getInt(const int col) const {
        return get(col).getInt();
    }

    double Row::getDouble(const int col) const {
        return get(col).getDouble();
    }

    std::string Row::getString(const int col) const {
        return std::string((const char*)get(col).getPtr());
    }

    Tuple Row::getTuple(const int col) const {
        // copy constructor invoked to return deep copied tuple
        return Tuple(*((Tuple*)get(col).getPtr()));
    }

    Schema Row::getSchema() const {
        return _schema;
    }

    python::Type Row::getType(const int col) const {
        return _schema.getRowType().parameters()[col];
    }

    std::string Row::toPythonString() const {
        std::string s = "(";
        for(int i = 0; i < getNumColumns(); ++i) {
            s += _values[i].desc();

            if(i != getNumColumns() - 1)
                s += ",";
        }

        // special case: single element tuple, add , so it becomes a tuple
        // and not a expression in parentheses
        if(getNumColumns() == 1)
            s +=",";

        s += ")";

        return s;
    }

    python::Type Row::getRowType() const {
        if(_values.empty())
            return python::Type::EMPTYTUPLE; // bad case!

        // get types of rows & return then tuple type
        std::vector<python::Type> types;
        for(const auto& el: _values)
            types.push_back(el.getType());

        return python::Type::makeTupleType(types);
    }

    size_t Row::serializeToMemory(uint8_t *buffer, const size_t capacity) const {
        return getSerializer().serialize(buffer, capacity);
    }

    size_t Row::getSerializedLength() const {
        return getSerializer().length();
    }

    Serializer Row::getSerializer() const {
        Serializer serializer;

        if(_values.empty()) {
            // basically serialize an empty tuple here
            serializer.append(Tuple()); // use {} here to make empty tuple. default constructor yields unknown
            return serializer;
        }

        for(const auto& el : _values) {
            if(python::Type::BOOLEAN == el.getType())
                serializer.append(el.getInt() > 0);
            else if(python::Type::I64 == el.getType())
                serializer.append(el.getInt());
            else if(python::Type::F64 == el.getType())
                serializer.append(el.getDouble());
            else if(python::Type::STRING == el.getType())
                serializer.append(std::string((char*)el.getPtr()));
            else if(python::Type::GENERICDICT == el.getType())
                serializer.append(std::string((char*)el.getPtr()), python::Type::GENERICDICT);
            else if(el.getType().isDictionaryType())
                serializer.append(std::string((char*)el.getPtr()), el.getType());
            else if(el.getType() == python::Type::NULLVALUE) {
                serializer.appendNull();
            } else if(el.getType().isListType()) {
                serializer.append(*(List*)el.getPtr());
            }
            else {
                if(el.getType().isTupleType()) {
                    serializer.append(*((Tuple*)el.getPtr()));
                } else if(el.getType().isOptionType()) {

                    // quick hack, all types again
                    auto rt = el.getType().getReturnType();

                    if(python::Type::BOOLEAN == rt)
                        serializer.append(el.isNull() ? option<bool>::none : option<bool>(el.getInt() > 0));
                    else if(python::Type::I64 == rt)
                        serializer.append(el.isNull() ? option<int64_t>::none : option<int64_t>(el.getInt()));
                    else if(python::Type::F64 == rt)
                        serializer.append(el.isNull() ? option<double>::none : option<double>(el.getDouble()));
                    else if(python::Type::STRING == rt)
                        serializer.append(el.isNull() ? option<std::string>::none : option<std::string>(std::string((const char*)el.getPtr())));
                    else if(python::Type::EMPTYTUPLE == rt) {
                        serializer.appendEmptyTupleOption(el);
                    }
                    else if(python::Type::GENERICDICT == rt)
                        serializer.append(el.isNull() ? option<std::string>::none : option<std::string>(std::string((const char*)el.getPtr())), python::Type::GENERICDICT);
                    else if(rt.isDictionaryType())
                        serializer.append(el.isNull() ? option<std::string>::none : option<std::string>(std::string((const char*)el.getPtr())), el.getType());
                    else if(rt.isListType())
                        serializer.append(el.isNull() ? option<List>::none : option<List>(*(List*)el.getPtr()), el.getType());
                    else {
                        if(rt.isTupleType()) {

                            // needs specialized function, depending on tuple type...
                            throw std::runtime_error(std::string(__FILE__) + " + " + std::to_string(__LINE__) + std::string(": tuple serialization not yet implemented"));
                            //serializer.append(*((Tuple *)el.getPtr()));
                        } else throw std::runtime_error("option underlying type " + rt.desc() + " not known");
                    }
                } else if(el.getType() == python::Type::NULLVALUE) {
                    serializer.append(NullType());
                } else if(el.getType() == python::Type::PYOBJECT) {
                    serializer.appendObject(static_cast<const uint8_t *>(el.getPtr()), el.getPtrSize());
                } else {
                    throw std::runtime_error("unsupported type " + el.getType().desc() + " found");
                }
            }
        }
        return serializer;
    }

    bool operator == (const Row& lhs, const Row& rhs) {
        // first check if number of values match
        if(lhs._values.size() != rhs._values.size())
            return false;

        // special case: empty rows
        if(lhs._values.size() == 0)
            return true;

        // check whether type matches
        if(lhs.getRowType() != rhs.getRowType())
            return false;

        // types match, so do now check contents
        auto numElements = lhs._values.size();
        for(unsigned i = 0; i < numElements; ++i) {
            if(lhs.get(i) != rhs.get(i))
                return false;
        }


        return true;
    }


    Row Row::upcastedRow(const python::Type &targetType) const {
        using namespace std;
        assert(canUpcastToRowType(getRowType(), targetType));

        auto num_fields = _values.size();
        auto targetTypes = targetType.parameters();
        vector<Field> fields;
        for(unsigned i = 0; i < num_fields; ++i) {
            auto t = _values[i].getType();
            auto tt = targetTypes[i];
            if(t == tt) {
                fields.push_back(_values[i]);
            } else {
                // should be able to upcast!
                assert(canUpcastType(t, tt));
                fields.push_back(Field::upcastTo_unsafe(_values[i], tt));
            }
        }

        return Row::from_vector(fields);
    }

    Tuple Row::getAsTuple() const {
        return Tuple::from_vector(_values);
    }


    // print table based on rows and headers
    void printTable(std::ostream& os, const std::vector<std::string>& header,
                    const std::vector<Row>& rows, bool quoteStrings) {
        using namespace std;

        // retrieve max columns from headers and rows
        auto numColumns = header.size();
        for(const auto& row : rows)
            numColumns = std::max(numColumns, (size_t)row.getNumColumns());


        // get column width (maximum width within each column)
        std::vector<int> columnWidths(numColumns, 0);
        for (int i = 0; i < numColumns; ++i) {
            if (header[i].length() > columnWidths[i]) {
                columnWidths[i] = getMaxLineLength(replaceLineBreaks(header[i]));
            }
        }
        for (const auto &row: rows) {
            std::vector<std::string> strs = row.getAsStrings();

            // remove '...' quotes
            if(!quoteStrings) {
                for(int i = 0; i < row.getNumColumns(); ++i)
                    if(row.getType(i) == python::Type::STRING)
                        strs[i] = strs[i].substr(1, strs[i].length() - 2);
            }

            for (int i = 0; i < strs.size(); ++i) {
                if (strs[i].length() > columnWidths[i]) {
                    columnWidths[i] = getMaxLineLength(replaceLineBreaks(strs[i]));
                }
            }
        }

        // assert column widths are at least 1
        for (auto col_width : columnWidths)
            assert(col_width > 0);

        // print headers
        tuplex::helper::printSeparatingLine(os, columnWidths);
        tuplex::helper::printRow(os, columnWidths, header);
        tuplex::helper::printSeparatingLine(os, columnWidths);

        // print all rows
        for (const auto &row: rows) {
            std::vector<std::string> strs = row.getAsStrings();

            // remove '...' quotes
            if(!quoteStrings) {
                for(int i = 0; i < row.getNumColumns(); ++i)
                    if(row.getType(i) == python::Type::STRING)
                        strs[i] = strs[i].substr(1, strs[i].length() - 2);
            }

            tuplex::helper::printRow(os, columnWidths, strs);
            tuplex::helper::printSeparatingLine(os, columnWidths);
        }

        os.flush();
    }
}