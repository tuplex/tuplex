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
#include "JSONUtils.h"

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
            s += _values[i].toPythonString();

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

    std::string Row::toJsonString(const std::vector<std::string> &columns) const {
        std::stringstream ss;
        if(columns.empty()) {
            // use []
            ss<<"[";
            for(unsigned i = 0; i < getNumColumns(); ++i) {
                ss<<escape_for_json(columns[i])<<":"<<_values[i].toJsonString();
                if(i != getNumColumns() - 1)
                    ss<<",";
            }
            ss<<"]";
        } else {
            if(columns.size() != getNumColumns())
                throw std::runtime_error("can not convert row to Json string, number of columns given ("
                + std::to_string(columns.size()) + ") is different than stored number of fields ("
                + std::to_string(getNumColumns()) + ")");

            // use dictionary syntax
            ss<<"{";
            for(unsigned i = 0; i < getNumColumns(); ++i) {
                ss<<escape_for_json(columns[i])<<":"<<_values[i].toJsonString();
                if(i != getNumColumns() - 1)
                    ss<<",";
            }
            ss<<"}";
        }
        return ss.str();
    }

    python::Type Row::getRowType() const {
        if(_values.empty())
            return python::Type::EMPTYTUPLE; // bad case!

        // get types of rows & return then tuple type
        std::vector<python::Type> types;
        types.reserve(_values.size());
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
            return std::move(serializer);
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
                    else if (rt.isTupleType()) {
                        if (el.isNull()) {
                            serializer.append(option<Tuple>::none, el.getType());
                        } else {
                            auto ptrToTuple = (Tuple*)el.getPtr();
                            assert(ptrToTuple);
                            serializer.append(option<Tuple>(*ptrToTuple), el.getType());
                        }
                    } else {
                        throw std::runtime_error("option underlying type " + rt.desc() + " not known");
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
        return std::move(serializer);
    }

    bool operator == (const Row& lhs, const Row& rhs) {
        // first check if number of values match
        if(lhs._values.size() != rhs._values.size())
            return false;

        // special case: empty rows
        if(lhs._values.size() == 0) {
            assert(rhs._values.size() == 0);
            return true;
        }

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
    bool operator < (const Row& lhs, const Row& rhs) {
        // this is important !!!
        if(lhs == rhs)
            return false;

        if(lhs._values.size() != rhs._values.size())
            return lhs._values.size() < rhs._values.size();

        // same size
        auto num_elements = lhs._values.size();

        // element wise comparison (types!)
        for(unsigned i = 0; i < num_elements; ++i) {
            // compare values, if not equal then
            if(lhs._values[i] != rhs._values[i]) {
                // check whether lhs < rhs
                // types differ?
                // sort after type hash!
                if(lhs._values[i].getType() != rhs._values[i].getType())
                    return lhs._values[i].getType().hash() < rhs._values[i].getType().hash();

                auto r = rhs._values[i];
                auto l = lhs._values[i];
                // same type, so check values
                auto t = lhs._values[i].getType();
                if(t == python::Type::BOOLEAN || t == python::Type::I64) {
                    return l.getInt() < r.getInt();
                } else if(t == python::Type::F64) {
                    return l.getDouble() < r.getDouble();
                } else {
                    auto r_str = r.toPythonString();
                    auto l_str = l.toPythonString();
                    return lexicographical_compare(l_str.begin(), l_str.end(), r_str.begin(), r_str.end());
                }
            }
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

    python::Type detectMajorityRowType(const std::vector<Row>& rows,
                                       double threshold,
                                       bool independent_columns,
                                       bool use_nvo,
                                       const TypeUnificationPolicy& t_policy) {
        if(rows.empty())
            return python::Type::UNKNOWN;

        assert(0.0 <= threshold <= 1.0);

        if(independent_columns) {
            // count each column independently
            // hashmap is type, col -> count
            std::unordered_map<std::tuple<unsigned, unsigned>, unsigned> counts;
            for(const auto& row : rows) {
                auto t = row.getRowType();
                for(unsigned i = 0; i < t.parameters().size(); ++i) {
                    counts[std::make_tuple(t.parameters()[i].hash(), i)]++;
                }
            }

            // get column counts
            unsigned min_idx = std::numeric_limits<unsigned>::max();
            unsigned max_idx = 0;
            for(auto keyval : counts) {
                auto type_hash = std::get<0>(keyval.first);
                auto col_idx = std::get<1>(keyval.first);
                min_idx = std::min(min_idx, col_idx);
                max_idx = std::max(max_idx, col_idx);
            }

            // have counts, now for each column make a type decision --> i.e. based on majority case!
            std::vector<std::map<unsigned, unsigned, std::greater<unsigned>>> col_counts(max_idx + 1);
            for(auto keyval : counts) {
                auto type_hash = std::get<0>(keyval.first);
                auto col_idx = std::get<1>(keyval.first);
                col_counts[col_idx][keyval.second] = type_hash;
            }

            // compute majority, account for null-value prevalence!
            std::vector<python::Type> col_types(col_counts.size());
            for(unsigned i = 0; i < col_counts.size(); ++i) {
               // single type? -> trivial.
               // empty? pyobject
               if(col_counts[i].empty()) {
                   col_types[i] = python::Type::PYOBJECT;
               } else if(col_counts[i].size() == 1) {
                   col_types[i] = python::Type::fromHash(col_counts[i].begin()->second);
               } else {
                   // more than one count. Now it's getting tricky...
                   // is the first null and something else present? => use threshold to determine whether option type or not!
                   auto most_common_type = python::Type::fromHash(col_counts[i].begin()->second);
                   auto most_freq = col_counts[i].begin()->first;
                   unsigned total_freq = 0;
                   for(auto kv : col_counts[i])
                       total_freq += kv.first;
                   if(python::Type::NULLVALUE == most_common_type) {
                       // second one present?
                       auto it = std::next(col_counts[i].begin());
                       auto second_freq = it->first;
                       auto second_type = python::Type::fromHash(it->second);

                       // threshold?
                       if(most_freq >= threshold * total_freq && use_nvo) {
                           // null value
                           col_types[i] = python::Type::NULLVALUE;
                       } else {
                           // create opt type to cover other cases...
                           col_types[i] = python::Type::makeOptionType(second_type);
                       }
                   } else {
                       // is there null value somewhere so Opt covers most of the cases?

                       auto it = col_counts[i].begin();
                       while(it->second != python::Type::NULLVALUE.hash() && it != col_counts[i].end())
                           ++it;

                       // take original value
                       col_types[i] = most_common_type;

                       // null value found? --> form option type!
                       if(it != col_counts[i].end()) {
                           assert(it->second == python::Type::NULLVALUE.hash());
                           // check counts, in case of non-nvo always use Option type
                           auto nv_count = it->first;
                           // when most freq count >= threshold, use that. else use option type.
                           if(most_freq >= threshold * total_freq && use_nvo) {
                               // nothing
                           } else {
                               col_types[i] = python::Type::makeOptionType(most_common_type);
                           }
                       }
                   }
               }
            }

            // // debug print:
            // std::stringstream ss;
            // for(unsigned i = 0; i < col_counts.size(); ++i) {
            //     ss<<"col "<<i<<": ";
            //     for(auto entry : col_counts[i]) {
            //         python::Type t = python::Type::fromHash(entry.second);
            //         ss<<t.desc()<<" ("<<entry.first<<"), ";
            //     }
            //     ss<<"\n";
            // }
            // std::cout<<ss.str()<<std::endl;

            return python::Type::makeTupleType(col_types);
        } else {

            std::cerr<<"not yet implemented"<<std::endl;

            // joint aggregate
            return python::Type::UNKNOWN;
        }

        return python::Type::UNKNOWN;
    }
}