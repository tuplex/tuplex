//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ROW_H
#define TUPLEX_ROW_H

#include <Serializer.h>
#include <Field.h>
#include <ExceptionCodes.h>

namespace tuplex {
    /*!
     * expensive wrapper for a single column. For the actual computation, serialized versions are used.
     * This is merely a result type to be passed to the frontend.
     */
    class Row {
    private:

        // row is internally a vector of strings
        // right now only basic types supported.
        // serialize i.e. each element as string
        // ==> optimize later.

        Schema                      _schema;
        std::vector<Field>          _values;
        size_t                      _serializedLength;

        // get length from values
        Serializer getSerializer() const;
        size_t getSerializedLength() const;
    public:
        Row() : _serializedLength(0) {}

        Row(const Row& other) : _schema(other._schema), _values(other._values), _serializedLength(other._serializedLength) {}
        Row& operator = (const Row& other) {
            _schema = other._schema;
            _values = other._values;
            _serializedLength = other._serializedLength;
            return *this;
        }

        Row(Row&& other) : _schema(other._schema), _serializedLength(other._serializedLength), _values(std::move(other._values)) {
            other._values = {};
            other._serializedLength = 0;
            other._schema = Schema::UNKNOWN;
        }


        // new constructor using variadic templates
        template<typename... Targs> Row(Targs... Fargs) {
            vec_build(_values, Fargs...);
            _schema = Schema(Schema::MemoryLayout::ROW, getRowType());
            _serializedLength = getSerializedLength();
        }

        inline size_t   getNumColumns() const { return _values.size(); }
        inline Field    get(const int col) const {
            assert(!_values.empty());
            assert(0 <= col && col < _values.size());
            return _values[col];
        }

        inline void set(const unsigned col, const Field& f) {
#ifndef NDEBUG
            if(col >= _values.size())
                throw std::runtime_error("invalid column index in get specified");
#endif
            _values[col] = f;

            // need to update type of row!
            auto old_type = _schema.getRowType();
            auto types = old_type.parameters();
            if(types[col] != f.getType()) {
                types[col] = f.getType();
                _schema = Schema(_schema.getMemoryLayout(), python::Type::makeTupleType(types));
            }

            // update length, may change!
            _serializedLength = getSerializedLength();
        }


        bool            getBoolean(const int col) const;
        int64_t         getInt(const int col) const;
        double          getDouble(const int col) const;
        std::string     getString(const int col) const;
        Tuple           getTuple(const int col) const;
        List            getList(const int col) const;
        Schema          getSchema() const;

        Tuple getAsTuple() const;

        /*!
         * creates a row representation for easier output from internal memory storage format
         * @param schema Schema to use in order to deserialize from the memory ptr
         * @param ptr pointer to an address on where to start deserialization
         * @param size safe size of the memory region starting at ptr
         * @return Row with contents if deserialization succeeds. Empty row else.
         */
        static Row fromMemory(const Schema& schema, const void *ptr, size_t size);
        static Row fromMemory(Deserializer& ds, const void *ptr, size_t size);


        /*!
         * creates a row representation from memory where exceptions are stored. Because the exception type partially defines
         * the storage format, the normal fromMemory doesn't work always
         */
        static Row fromExceptionMemory(const Schema& schema, ExceptionCode ec, const void *ptr, size_t size);
        static Row fromExceptionMemory(Deserializer& ds, ExceptionCode ec, const void *ptr, size_t size);

        static size_t exceptionRowSize(Deserializer& ds, ExceptionCode ec, const void *ptr, size_t size);

        /*!
         * length contents would require in internal memory storage format
         * @return length in bytes
         */
        size_t serializedLength() const { return _serializedLength; }

        /*!
         * returns type of corresponding column
         * @param col column index
         * @return type of column col
         */
        python::Type getType(const int col) const;

        /*!
         * returns type of the row as tuple type
         * @return python tuple type corresponding to the type build from field types
         */
        python::Type getRowType() const;

        /*!
         * serializes row contents to memory starting at buffer. performs capacity check before serialization
         * @param buffer memory address where to serialize to
         * @param capacity secured capacity after memory address
         * @return serialized length of the row's contents
         */
        size_t serializeToMemory(uint8_t* buffer, const size_t capacity) const;

        /*!
         * creates valid python source representation of values as tuple.
         * @return string with pythonic data representation
         */
        std::string toPythonString() const;

        /*!
         * returns for each column a string representing its contents. Can be used for display.
         * @return vector of strings of the row contents.
         */
        inline std::vector<std::string> getAsStrings() const {
            std::vector<std::string> out;
            out.reserve(_values.size());
            for(const auto& el: _values)
                out.push_back(el.desc());
            return out;
        }

        friend bool operator == (const Row& lhs, const Row& rhs);

        static Row from_vector(const std::vector<Field>& fields) {
            Row row;
            row._values = fields;
            row._schema = Schema(Schema::MemoryLayout::ROW, row.getRowType());
            row._serializedLength = row.getSerializedLength();
            return row;
        }

        Row upcastedRow(const python::Type& targetType) const;
    };

    // used for tests
    extern bool operator == (const Row& lhs, const Row& rhs);

    struct ExceptionSample {
        std::string first_row_traceback;
        std::vector<Row> rows;
    };

    /*!
    * prints a nicely formatted table of rows
    * @param os
    * @param header
    * @param rows
    * @param quoteStrings whether to display with single quotes ' or not.
    */
    void printTable(std::ostream& os, const std::vector<std::string>& header,
                    const std::vector<Row>& rows, bool quoteStrings=true);
}
#endif //TUPLEX_ROW_H